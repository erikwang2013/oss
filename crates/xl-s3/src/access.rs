//! 授权核心（子账户 × 桶 三档权限：读/写/删）与 s3s 的 `S3Access` 实现。
//!
//! 三面共用同一份判权：S3 面经 `s3s::access::S3Access`（`check` 反序列化前统一入口）；
//! 控制台与 aetherupload 上传面（oss-server）直接调用 [`Acl`] 的 `can/is_owner/visible`
//! ——SQL 只有这一处，防两套实现漂移。
//!
//! 求值规则（fail-closed）：
//!   匿名 → 拒；`is_admin` → 全允许；`buckets.owner_id == uid`（桶主）→ 全允许；
//!   否则查 `permissions` 行，**无行 = 无权限**。
//! CreateBucket：任何已认证用户（建后 `record_bucket_owner` 记账）；DeleteBucket：桶主或管理员。

use std::sync::Arc;

use futures::StreamExt as _;
use sqlx::{Row, SqlitePool};

use xl_store::mesh::{ControlQuery, ControlRow, MeshTransport};

use s3s::access::{S3Access, S3AccessContext};
use s3s::dto::CopySource;
use s3s::header::X_AMZ_COPY_SOURCE;
use s3s::{S3Error, S3ErrorCode, S3Result};

/// 已认证用户身份：`check` 里解析 AK 后塞进请求扩展（s3s 官方机制
/// `S3AccessContext::extensions_mut`），后续 handler 用 `req.extensions.get::<AuthedUser>()` 读取。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthedUser {
    pub uid: i64,
    pub is_admin: bool,
}

/// 权限动作（与 `permissions` 表三列一一对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    Delete,
}

/// 控制面读扇出（Task 9）：本地未命中时向 peers 尽力扇出、**首答胜**（不缓存起步，
/// ponytail：加 5s TTL 缓存的升级点与撤销延迟权衡记档于计划）。
#[derive(Clone)]
pub struct ControlMesh {
    pub transport: Arc<dyn MeshTransport>,
    /// 本节点 id（仅日志/防呆用）
    pub self_id: String,
    /// 除本机外的节点 id
    pub peers: Vec<String>,
}

impl ControlMesh {
    /// 并发扇出，首个 Some 胜；全 miss / 全错 → None（调用方 fail-closed）。
    /// 失败落 warn（不许静默——排查现场全靠它）。
    pub async fn ask(&self, q: ControlQuery) -> Option<ControlRow> {
        let mut futs = futures::stream::FuturesUnordered::new();
        for p in &self.peers {
            let t = self.transport.clone();
            let n = p.clone();
            let q = q.clone();
            futs.push(async move {
                match t.control_query(&n, q).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(node = %n, error = %e, "控制面扇出失败");
                        None
                    }
                }
            });
        }
        while let Some(r) = futs.next().await {
            if let Some(row) = r {
                return Some(row);
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct Acl {
    pool: SqlitePool,
    /// 集群控制面扇出（None = 单节点：本地查无即拒，fail-closed 语义不变）
    mesh: Option<ControlMesh>,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Acl {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool, mesh: None }
    }

    /// 集群形态（Task 9）：读路径本地 miss → peers 扇出（写入仍只落本节点；强一致归 M9）。
    pub fn with_mesh(pool: SqlitePool, mesh: Option<ControlMesh>) -> Self {
        Self { pool, mesh }
    }

    /// 桶版本控制状态（M4 切片 3；`buckets.versioning`）：无行/旧库缺列值 → "Off"（fail-closed 兼容）。
    pub async fn bucket_versioning(&self, bucket: &str) -> Result<String, sqlx::Error> {
        let row = sqlx::query("SELECT versioning FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("versioning"))
            .unwrap_or_else(|| "Off".to_string()))
    }

    /// 写桶版本控制状态（PutBucketVersioning；授权门已确认桶主/管理员）。
    /// 返回 false = 桶无 DB 行（S3 层按 NoSuchBucket 处置）。
    pub async fn set_bucket_versioning(
        &self,
        bucket: &str,
        status: &str,
    ) -> Result<bool, sqlx::Error> {
        let res = sqlx::query("UPDATE buckets SET versioning = ? WHERE name = ?")
            .bind(status)
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// AK → (uid, is_admin)。status/expires 条件与 `SigV4Auth::get_secret_key` 同口径
    ///（认证面与授权面查的是同一行，任一处撤销即同时失效）；**账号停用（users.status）
    /// 同样使 AK 失效**——停用即全链路出局。
    pub async fn user_of_key(&self, access_key: &str) -> Result<Option<AuthedUser>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT ak.user_id AS user_id, u.is_admin AS is_admin \
             FROM access_keys ak JOIN users u ON u.id = ak.user_id \
             WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
               AND (ak.expires_at IS NULL OR ak.expires_at > ?)",
        )
        .bind(access_key)
        .bind(now())
        .fetch_optional(&self.pool)
        .await?;
        if let Some(r) = row {
            return Ok(Some(AuthedUser {
                uid: r.get("user_id"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
            }));
        }
        // 集群（Task 9）：本地查无 → 扇出（节点 B 建的号/签的 AK，节点 A 也认）
        if let Some(m) = &self.mesh
            && let Some(row) = m.ask(ControlQuery::AccessKey(access_key.to_string())).await
        {
            return Ok(Some(AuthedUser {
                uid: row.uid.unwrap_or(0),
                is_admin: row.is_admin.unwrap_or(false),
            }));
        }
        Ok(None)
    }

    /// 桶主？（`buckets.owner_id == uid`）
    pub async fn is_owner(&self, bucket: &str, uid: i64) -> Result<bool, sqlx::Error> {
        let row = sqlx::query("SELECT 1 FROM buckets WHERE name = ? AND owner_id = ?")
            .bind(bucket)
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        if row.is_some() {
            return Ok(true);
        }
        // 集群：归属记账可能在对端（本地无行）→ 扇出比对
        if let Some(m) = &self.mesh
            && let Some(row) = m.ask(ControlQuery::BucketOwner(bucket.to_string())).await
        {
            return Ok(row.uid == Some(uid));
        }
        Ok(false)
    }

    /// 主判权：admin / 桶主 全允许；否则查对应能力位（无行 = 拒）。
    pub async fn can(
        &self,
        user: AuthedUser,
        action: Action,
        bucket: &str,
    ) -> Result<bool, sqlx::Error> {
        if user.is_admin {
            return Ok(true);
        }
        if self.is_owner(bucket, user.uid).await? {
            return Ok(true);
        }
        // 三份静态字面量（sqlx 0.9 拒绝动态 SQL 字符串；字面量也更利于审计）
        let sql = match action {
            Action::Read => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_read = 1"
            }
            Action::Write => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_write = 1"
            }
            Action::Delete => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_delete = 1"
            }
        };
        let row = sqlx::query(sql)
            .bind(user.uid)
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        if row.is_some() {
            return Ok(true);
        }
        // 集群（Task 9）：本地无行 → 扇出（对端本地语义 = owner ∨ permission 位；
        // admin 已在发起端判定，对端无需重判）
        if let Some(m) = &self.mesh
            && let Some(r) = m
                .ask(ControlQuery::Permission {
                    uid: user.uid,
                    bucket: bucket.to_string(),
                    action: action as u8,
                })
                .await
        {
            return Ok(r.flag.unwrap_or(false));
        }
        Ok(false)
    }

    /// 可见桶集合（列举过滤）：自有桶 ∪ 在 `permissions` 任一能力位为 1 的桶。
    /// 与磁盘列表**求交**使用（磁盘是权威——历史桶可能没有 DB 行）。
    pub async fn visible(&self, uid: i64) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT name AS b FROM buckets WHERE owner_id = ? \
             UNION \
             SELECT bucket AS b FROM permissions WHERE user_id = ? \
               AND (can_read = 1 OR can_write = 1 OR can_delete = 1)",
        )
        .bind(uid)
        .bind(uid)
        .fetch_all(&self.pool)
        .await?;
        let mut names: Vec<String> = rows.into_iter().map(|r| r.get("b")).collect();
        // 集群：并集对端可见集（去重）
        if let Some(m) = &self.mesh
            && let Some(r) = m.ask(ControlQuery::VisibleBuckets(uid)).await
            && let Some(peers) = r.names
        {
            for n in peers {
                if !names.contains(&n) {
                    names.push(n);
                }
            }
        }
        Ok(names)
    }

    /// 桶归属登记（S3 面 CreateBucket 用；幂等——已存在（含历史 NULL owner）不覆盖）。
    pub async fn record_bucket_owner(&self, bucket: &str, uid: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT OR IGNORE INTO buckets (name, owner_id, creation_date) VALUES (?, ?, ?)",
        )
        .bind(bucket)
        .bind(uid)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 删桶后的记账清理（S3 与控制台两条删桶路径共用同一实现）：
    /// 清该桶的授权行（防重建同名桶继承旧授权）+ 清桶行。
    pub async fn forget_bucket(&self, bucket: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM permissions WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 带 `Option<bucket>` 判权的便捷形态（Root 路径的 op 直接拒）。
    async fn can_opt(
        &self,
        user: AuthedUser,
        action: Action,
        bucket: &Option<String>,
    ) -> Result<bool, S3Error> {
        match bucket {
            Some(b) => self.can(user, action, b).await.map_err(acl_err),
            None => Ok(false),
        }
    }
}

/// 判权查询失败的统一映射（详情进日志，客户端只见 500 泛化文案）。
pub(crate) fn acl_err(e: sqlx::Error) -> S3Error {
    tracing::error!(error = ?e, "acl lookup failed");
    S3Error::with_message(S3ErrorCode::InternalError, "internal error")
}

/// s3s 的授权入口：`check` 在反序列化**前**对每个请求执行（我们已 `set_auth` → 恒走；
/// per-op 方法默认 Ok 是叠加非替代，无需实现）。判据与三面共用的 [`Acl::can`] 同源；匿名一律拒。
#[async_trait::async_trait]
impl S3Access for Acl {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // 先克隆 AK（credentials() 借自 cx，与随后的 extensions_mut 冲突）
        let Some(ak) = cx.credentials().map(|c| c.access_key.clone()) else {
            return Err(s3s::s3_error!(AccessDenied, "Authentication required"));
        };
        let user = self
            .user_of_key(&ak)
            .await
            .map_err(acl_err)?
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?;
        // handler 读取：ListBuckets 过滤 / CreateBucket 记账（随请求搬进 S3Request.extensions）
        cx.extensions_mut().insert(user);
        if user.is_admin {
            return Ok(());
        }

        let op = cx.s3_op().name();
        // 桶名必须用 `get_bucket_name()`：`as_bucket()` 只匹配 `S3Path::Bucket` 变体，
        // 对象路径（`Object{bucket,key}`）会返回 None——用它会把一切对象级 op 误判成
        // 「无桶」而全拒（子账户实弹 403 的根因，管理员旁路曾掩盖）。
        let bucket: Option<String> = cx.s3_path().get_bucket_name().map(str::to_owned);
        let Some(gate) = classify(op) else {
            tracing::warn!(uid = user.uid, op, "授权拒绝（未映射 op，fail-closed）");
            return Err(s3s::s3_error!(AccessDenied, "access denied"));
        };
        let allow = match gate {
            // 列表由 handler 过滤；建桶任何已认证用户（建后记账 owner）
            Gate::Open => true,
            Gate::OwnerBucket => match &bucket {
                Some(b) => self.is_owner(b, user.uid).await.map_err(acl_err)?,
                None => false,
            },
            Gate::Need(action) => self.can_opt(user, action, &bucket).await?,
            Gate::Copy => {
                // 目标写 + 源读（源在 header；用 s3s 的解析器——header 是 URL 编码且有
                // / 前导与 ?versionId 形态，解析失败与反序列化同判，直接拒绝不误伤）
                let dst = self.can_opt(user, Action::Write, &bucket).await?;
                let src = match cx
                    .headers()
                    .get(X_AMZ_COPY_SOURCE)
                    .and_then(|v| v.to_str().ok())
                    .map(CopySource::parse)
                {
                    Some(Ok(CopySource::Bucket { bucket, .. })) => self
                        .can(user, Action::Read, &bucket)
                        .await
                        .map_err(acl_err)?,
                    _ => false,
                };
                dst && src
            }
        };
        if allow {
            Ok(())
        } else {
            tracing::warn!(uid = user.uid, op, bucket = ?bucket, "授权拒绝");
            Err(s3s::s3_error!(AccessDenied, "access denied"))
        }
    }
}

/// op → 门槛类型的纯映射（独立成函数以便表驱动单测；`None` = 未映射 op，fail-closed）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// 已认证即可（ListBuckets 由 handler 过滤；CreateBucket 建后记账 owner）
    Open,
    /// 桶主或管理员
    OwnerBucket,
    /// 桶级 读/写/删 三档
    Need(Action),
    /// 目标写 + 源读
    Copy,
}

fn classify(op: &str) -> Option<Gate> {
    Some(match op {
        "ListBuckets" | "CreateBucket" => Gate::Open,
        "DeleteBucket" => Gate::OwnerBucket,
        // M4 版本控制：桶级状态改成开版本控制 = 桶主/管理员；读状态与列版本归读
        "PutBucketVersioning" => Gate::OwnerBucket,
        "GetBucketVersioning" => Gate::Need(Action::Read),
        // 读（HeadBucket 归读：AWS 语义绑 ListBucket）
        "HeadBucket" | "ListObjectsV2" | "ListParts" | "GetObject" | "HeadObject"
        | "ListObjectVersions" => Gate::Need(Action::Read),
        // 写（Abort 归写——它是写路径的清理；PostObject 的 trait 默认实现委托
        // put_object，op 名看到的是 "PostObject"，不映射它就是浏览器表单直传的漏检点）
        "PutObject"
        | "PostObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => Gate::Need(Action::Write),
        // 删
        "DeleteObject" | "DeleteObjects" => Gate::Need(Action::Delete),
        "CopyObject" => Gate::Copy,
        // 未实现/未知 op：fail-closed（下游本来也是 501；这里统一 403）
        _ => return None,
    })
}

/// Task 9 测试支撑：假传输（只答 `control_query`；其余方法显式未实现）。
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use xl_store::error::{Error, Result};
    use xl_store::mesh::{ControlQuery, ControlRow, MeshTransport};

    /// 节点 → 应答（`None` = 该节点查无；不在表内 = RPC 失败——模拟不可达）。
    #[derive(Debug, Default)]
    pub struct FakeMesh {
        pub answers: Mutex<HashMap<String, Option<ControlRow>>>,
    }

    impl FakeMesh {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
        pub fn answer(&self, node: &str, row: Option<ControlRow>) {
            self.answers.lock().unwrap().insert(node.to_string(), row);
        }
    }

    fn unimpl(op: &str) -> Error {
        Error::Encode(format!("FakeMesh 未实现：{op}"))
    }

    #[async_trait::async_trait]
    impl MeshTransport for FakeMesh {
        async fn control_query(&self, node: &str, _q: ControlQuery) -> Result<Option<ControlRow>> {
            match self.answers.lock().unwrap().get(node) {
                Some(row) => Ok(row.clone()),
                None => Err(unimpl("control_query（该节点不可达）")),
            }
        }
        async fn write_shard(&self, _n: &str, _w: xl_store::mesh::ShardWrite) -> Result<()> {
            Err(unimpl("write_shard"))
        }
        async fn read_shard(
            &self,
            _n: &str,
            _r: xl_store::mesh::ShardRead,
        ) -> Result<Option<bytes::Bytes>> {
            Err(unimpl("read_shard"))
        }
        async fn delete_version(&self, _n: &str, _d: xl_store::mesh::VersionDirRef) -> Result<()> {
            Err(unimpl("delete_version"))
        }
        async fn recycle_version(&self, _n: &str, _d: xl_store::mesh::VersionDirRef) -> Result<()> {
            Err(unimpl("recycle_version"))
        }
        async fn delete_shard(&self, _n: &str, _d: xl_store::mesh::ShardDelete) -> Result<()> {
            Err(unimpl("delete_shard"))
        }
        async fn prepare_meta(
            &self,
            _n: &str,
            _m: xl_store::mesh::MetaWrite,
        ) -> Result<xl_store::mesh::MetaAck> {
            Err(unimpl("prepare_meta"))
        }
        async fn commit_meta(
            &self,
            _n: &str,
            _m: xl_store::mesh::MetaWrite,
        ) -> Result<xl_store::mesh::MetaAck> {
            Err(unimpl("commit_meta"))
        }
        async fn abort_meta(&self, _n: &str, _m: xl_store::mesh::MetaWrite) -> Result<()> {
            Err(unimpl("abort_meta"))
        }
        async fn read_meta(
            &self,
            _n: &str,
            _r: xl_store::mesh::MetaRead,
        ) -> Result<xl_store::mesh::MetaSnapshot> {
            Err(unimpl("read_meta"))
        }
        async fn delete_meta(&self, _n: &str, _r: xl_store::mesh::MetaRead) -> Result<()> {
            Err(unimpl("delete_meta"))
        }
        async fn ensure_bucket(&self, _n: &str, _b: &str) -> Result<()> {
            Err(unimpl("ensure_bucket"))
        }
        async fn delete_bucket_dir(&self, _n: &str, _b: &str) -> Result<()> {
            Err(unimpl("delete_bucket_dir"))
        }
        async fn list_bucket_dirs(&self, _n: &str) -> Result<Vec<String>> {
            Err(unimpl("list_bucket_dirs"))
        }
        async fn list_local(
            &self,
            _n: &str,
            _l: xl_store::mesh::ListLocal,
        ) -> Result<xl_store::mesh::ListLocalResp> {
            Err(unimpl("list_local"))
        }
        async fn health(&self, _n: &str) -> Result<xl_store::mesh::NodeHealth> {
            Err(unimpl("health"))
        }
        async fn lock(
            &self,
            _n: &str,
            _r: xl_store::mesh::LockReq,
        ) -> Result<xl_store::mesh::LockResp> {
            Err(unimpl("lock"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 最小 schema 的内联建库（列与 oss-server/migrations/0001_init.sql 对齐；
    /// xl-s3 不能反向依赖 oss-server，模式同 auth.rs 的测试 helper）。
    async fn acl() -> Acl {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, is_admin INTEGER NOT NULL DEFAULT 0, \
              status TEXT NOT NULL DEFAULT 'active');\
             CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL);\
             CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, \
              versioning TEXT NOT NULL DEFAULT 'Off');\
             CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
              can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
              PRIMARY KEY (user_id, bucket));",
        )
        .execute(&pool)
        .await
        .unwrap();
        Acl::new(pool)
    }

    async fn add_user(a: &Acl, id: i64, admin: bool) {
        sqlx::query("INSERT INTO users (id, username, is_admin) VALUES (?, ?, ?)")
            .bind(id)
            .bind(format!("u{id}"))
            .bind(admin as i64)
            .execute(&a.pool)
            .await
            .unwrap();
    }

    async fn add_key(a: &Acl, ak: &str, uid: i64, expires_at: Option<i64>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at) VALUES (?, 'sk', ?, ?, 0)",
        )
        .bind(ak)
        .bind(uid)
        .bind(expires_at)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    async fn add_bucket(a: &Acl, name: &str, owner: Option<i64>) {
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 0)")
            .bind(name)
            .bind(owner)
            .execute(&a.pool)
            .await
            .unwrap();
    }

    async fn grant(a: &Acl, uid: i64, bucket: &str, r: bool, w: bool, d: bool) {
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uid)
        .bind(bucket)
        .bind(r as i64)
        .bind(w as i64)
        .bind(d as i64)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    const SUB: AuthedUser = AuthedUser {
        uid: 2,
        is_admin: false,
    };
    const ADMIN: AuthedUser = AuthedUser {
        uid: 1,
        is_admin: true,
    };

    #[test]
    fn op_classification_table() {
        // 全量映射 + 三个易错点（PostObject 漏检、Abort 归写、HeadBucket 归读）
        // + fail-closed 默认臂。**S3 面 403 事故的回归**：对象级 op 必须命中这张表——
        // 曾经的 bug 在 check 的桶名解析（as_bucket 对 Object 路径返回 None），
        // 该段依赖 s3s 上下文、无法在此构造，由 e2e_s3.sh 的 15a 实弹钉住。
        assert_eq!(classify("ListBuckets"), Some(Gate::Open));
        assert_eq!(classify("CreateBucket"), Some(Gate::Open));
        assert_eq!(classify("DeleteBucket"), Some(Gate::OwnerBucket));
        // M4 切片 3：开版本控制 = 桶主/管理员；查状态归读
        assert_eq!(classify("PutBucketVersioning"), Some(Gate::OwnerBucket));
        assert_eq!(
            classify("GetBucketVersioning"),
            Some(Gate::Need(Action::Read))
        );
        for op in [
            "HeadBucket",
            "ListObjectsV2",
            "ListParts",
            "ListObjectVersions",
            "GetObject",
            "HeadObject",
        ] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Read)), "{op}");
        }
        for op in [
            "PutObject",
            "PostObject",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
        ] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Write)), "{op}");
        }
        for op in ["DeleteObject", "DeleteObjects"] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Delete)), "{op}");
        }
        assert_eq!(classify("CopyObject"), Some(Gate::Copy));
        // 未映射（含 s3s 存在但本服务未实现的桶策略类 op）→ fail-closed
        for op in ["PutBucketPolicy", "Frobnicate"] {
            assert_eq!(classify(op), None, "{op}");
        }
    }

    #[tokio::test]
    async fn admin_and_owner_allow_everything() {
        let a = acl().await;
        add_user(&a, 1, true).await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "b", Some(2)).await;
        for act in [Action::Read, Action::Write, Action::Delete] {
            assert!(a.can(ADMIN, act, "b").await.unwrap(), "admin 全允许");
            assert!(a.can(SUB, act, "b").await.unwrap(), "桶主全允许");
        }
    }

    #[tokio::test]
    async fn three_tiers_are_independent_and_fail_closed() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "b", None).await;
        grant(&a, 2, "b", true, false, false).await;
        assert!(a.can(SUB, Action::Read, "b").await.unwrap());
        assert!(!a.can(SUB, Action::Write, "b").await.unwrap());
        assert!(!a.can(SUB, Action::Delete, "b").await.unwrap());

        grant(&a, 2, "c", false, true, false).await;
        assert!(!a.can(SUB, Action::Read, "c").await.unwrap(), "写不含读");
        assert!(a.can(SUB, Action::Write, "c").await.unwrap());

        grant(&a, 2, "d", false, false, true).await;
        assert!(a.can(SUB, Action::Delete, "d").await.unwrap());

        // 无行 = 无权限（fail-closed）
        assert!(!a.can(SUB, Action::Read, "nope").await.unwrap());
    }

    #[tokio::test]
    async fn visible_is_owned_union_any_bit() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "owned", Some(2)).await;
        add_bucket(&a, "wr", None).await;
        add_bucket(&a, "none", None).await;
        grant(&a, 2, "wr", false, true, false).await;
        grant(&a, 2, "alloff", false, false, false).await;
        let mut v = a.visible(2).await.unwrap();
        v.sort();
        assert_eq!(v, vec!["owned".to_string(), "wr".to_string()]);
    }

    #[tokio::test]
    async fn user_of_key_respects_status_and_expiry() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 9, true).await;
        add_key(&a, "AK_OK", 2, None).await;
        add_key(&a, "AK_EXPIRED", 2, Some(1)).await; // 1970 → 已过期
        add_key(&a, "AK_ADMIN", 9, None).await;
        assert_eq!(
            a.user_of_key("AK_OK").await.unwrap(),
            Some(AuthedUser {
                uid: 2,
                is_admin: false
            })
        );
        assert_eq!(a.user_of_key("AK_EXPIRED").await.unwrap(), None);
        assert_eq!(
            a.user_of_key("AK_ADMIN").await.unwrap(),
            Some(AuthedUser {
                uid: 9,
                is_admin: true
            })
        );
        assert_eq!(a.user_of_key("AK_UNKNOWN").await.unwrap(), None);

        // 账号停用 → AK 立即失效（全链路出局）
        sqlx::query("UPDATE users SET status = 'disabled' WHERE id = 2")
            .execute(&a.pool)
            .await
            .unwrap();
        assert_eq!(a.user_of_key("AK_OK").await.unwrap(), None);
    }

    #[tokio::test]
    async fn record_bucket_owner_is_idempotent_and_preserves_existing() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        a.record_bucket_owner("b", 2).await.unwrap();
        a.record_bucket_owner("b", 3).await.unwrap(); // 二次不覆盖
        assert!(a.is_owner("b", 2).await.unwrap());
        assert!(!a.is_owner("b", 3).await.unwrap());
    }

    // ---------- Task 9：控制面读扇出 ----------

    fn mesh_with(peer_row: Option<ControlRow>) -> ControlMesh {
        let mesh = test_support::FakeMesh::new();
        mesh.answer("peer1", peer_row);
        ControlMesh {
            transport: mesh,
            self_id: "n1".into(),
            peers: vec!["peer1".into()],
        }
    }

    /// 权限位在对端：本地无行 → 扇出 granted。
    #[tokio::test]
    async fn permission_fanout_grants_from_peer() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        let a = Acl::with_mesh(
            base.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: Some(false),
                active: Some(true),
                secret: None,
                flag: Some(true),
                names: None,
            }))),
        );
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
        };
        assert!(a.can(u, Action::Read, "bkt").await.unwrap());
    }

    /// 对端不可达/查无 → fail-closed（拒），与单机同口径。
    #[tokio::test]
    async fn fanout_miss_or_peer_down_denies() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        let a = Acl::with_mesh(base.pool.clone(), Some(mesh_with(None)));
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
        };
        assert!(!a.can(u, Action::Read, "bkt").await.unwrap());
        assert_eq!(a.user_of_key("AK_PEER").await.unwrap(), None);
        assert!(!a.is_owner("bkt", 7).await.unwrap());
    }

    /// 归属与可见集的扇出：owner 比对 + 并集去重。
    #[tokio::test]
    async fn owner_and_visible_fanout() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        // 同一 FakeMesh 同一应答：owner=7。先测 owner。
        let a = Acl::with_mesh(
            base.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: None,
                active: None,
                secret: None,
                flag: None,
                names: None,
            }))),
        );
        assert!(a.is_owner("bkt", 7).await.unwrap());
        assert!(!a.is_owner("bkt", 8).await.unwrap());
        // 可见集：远端应答复用 names 字段（换 mesh）
        let base2 = acl().await;
        add_user(&base2, 7, false).await;
        let a2 = Acl::with_mesh(
            base2.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: None,
                is_admin: None,
                active: None,
                secret: None,
                flag: None,
                names: Some(vec!["beta".into(), "gamma".into()]),
            }))),
        );
        let v = a2.visible(7).await.unwrap();
        assert!(v.contains(&"beta".to_string()) && v.contains(&"gamma".to_string()));
    }
}
