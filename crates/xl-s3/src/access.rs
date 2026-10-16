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
/// 带 `ak`：handler 级逐键策略复评（DeleteObjects）需要与 check 同源的 Principal 匹配输入；
/// 控制台/aether 合成身份给空串（各自面不走桶策略）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthedUser {
    pub uid: i64,
    pub is_admin: bool,
    pub ak: String,
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

    /// M5 切片 4：桶对象锁状态 → `Some((enabled, 默认模式, 默认天数))`；**行不存在 = None**
    /// （N4 复验补：调用方可据此区分「桶不存在(404)」与「锁未开(400)」）。
    pub async fn bucket_lock(
        &self,
        bucket: &str,
    ) -> Result<Option<(bool, Option<String>, Option<i64>)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT object_lock, object_lock_default_mode, object_lock_default_days \
             FROM buckets WHERE name = ?",
        )
        .bind(bucket)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.get::<i64, _>("object_lock") != 0,
                r.get::<Option<String>, _>("object_lock_default_mode"),
                r.get::<Option<i64>, _>("object_lock_default_days"),
            )
        }))
    }

    /// M5 切片 4：开桶锁——`object_lock=1` **且自动开版本控制**（AWS：锁须版本化，建桶带
    /// `x-amz-bucket-object-lock-enabled` 时二者一并生效）。**Upsert**：建桶旗标可能先于记账
    /// 行到达（两条调用序都安全）；冲突时只改锁/版本列，不碰 owner/set/creation_date。
    pub async fn enable_bucket_lock(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, set_id, object_lock) \
             VALUES (?, NULL, ?, 'Enabled', 0, 1) \
             ON CONFLICT(name) DO UPDATE SET object_lock = 1, versioning = 'Enabled'",
        )
        .bind(bucket)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M5 切片 4：设/改默认保留规则（仅锁已开启桶；调用方先校验；None = 清空规则）。
    pub async fn set_bucket_lock_rule(
        &self,
        bucket: &str,
        mode: Option<&str>,
        days: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE buckets SET object_lock_default_mode = ?, object_lock_default_days = ? \
             WHERE name = ?",
        )
        .bind(mode)
        .bind(days)
        .bind(bucket)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 桶策略读取（M4 切片 7）：None = 未配；**解析失败 = Err**（check 里 fail-closed 拒）。
    pub async fn bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, sqlx::Error> {
        let row = sqlx::query("SELECT policy_json FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else { return Ok(None) };
        let json: String = r.get("policy_json");
        serde_json::from_str(&json)
            .map(Some)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))
    }

    /// 审计落行（判定后；允许与拒绝都记；best-effort 由调用方 warn）。
    pub async fn write_audit(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        sqlx::query(
            "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision, detail) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(now)
        .bind(uid)
        .bind(op)
        .bind(bucket)
        .bind(object_key)
        .bind(decision)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 清桶策略（删桶时调用——与生命周期规则同款 S6-1 语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 清桶生命周期规则（M4 切片 6：删桶时调用——桶名可复用，旧规则复活会误删新桶
    /// 对象；verifier S6-1）。表在共享 DB（migrations 0001），S3 面与控制面同口径。
    pub async fn clear_lifecycle_rules(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// DeleteObjects **逐键**授权（handler 用；verifier 阻断 1c 的修复点）。批量删的对象键在
    /// 请求体、check() 阶段不可见——若不逐键复评，「前缀 Deny」可被一条批量删请求整体绕过，
    /// 而「桶级 ARN Deny」又会误压整个请求。判定与单键删在 check() 里的融合完全同口径：
    /// 策略显式 Deny → 拒；策略 Allow → 放；无匹配 → 回落表三档。
    /// 返回 `(allowed, deny_detail)`：deny_detail 仅拒绝时有值（`policy:deny` = 策略拒，
    /// `None` = 表三档拒），供 handler 落审计。策略/表读取失败 → 调用方 fail-closed。
    pub async fn key_delete_decision(
        &self,
        user: &AuthedUser,
        bucket: &str,
        key: &str,
    ) -> Result<(bool, Option<&'static str>), sqlx::Error> {
        let decision = match self.bucket_policy(bucket).await? {
            Some(doc) => {
                let pr = crate::policy::PolicyRequest {
                    op: "DeleteObjects",
                    ak: &user.ak,
                    uid: user.uid,
                    bucket,
                    key: Some(key),
                };
                crate::policy::s3_action("DeleteObject")
                    .and_then(|a| crate::policy::evaluate(&doc, &pr, a))
            }
            None => None,
        };
        match decision {
            Some(true) => Ok((true, None)),
            Some(false) => Ok((false, Some("policy:deny"))),
            None => {
                let allowed = self.can(user.clone(), Action::Delete, bucket).await?;
                Ok((allowed, None))
            }
        }
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
                ak: access_key.to_string(),
            }));
        }
        // 集群（Task 9）：本地查无 → 扇出（节点 B 建的号/签的 AK，节点 A 也认）
        if let Some(m) = &self.mesh
            && let Some(row) = m.ask(ControlQuery::AccessKey(access_key.to_string())).await
        {
            return Ok(Some(AuthedUser {
                uid: row.uid.unwrap_or(0),
                is_admin: row.is_admin.unwrap_or(false),
                ak: access_key.to_string(),
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

    /// 桶归属登记（历史形态，无 set；幂等——已存在不覆盖，测试钉）。
    pub async fn record_bucket_owner(&self, bucket: &str, uid: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT OR IGNORE INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, ?, ?, 0)",
        )
        .bind(bucket)
        .bind(uid)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 带 set 的归属登记（C3：S3 面建桶缺省走自动指派；单 set 恒 0）。
    /// M5 切片 0：**覆盖**语义（原 INSERT OR IGNORE）——删桶记账曾被容忍失败留下的陈旧行
    /// 必须在重建时收敛 set 与 owner，否则本节点按旧 set 路由、按旧 owner 判权，与已收广播
    /// 的 peer 静默分叉（verifier 阻断 1）。
    pub async fn record_bucket_owner_set(
        &self,
        bucket: &str,
        uid: i64,
        set: u16,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, ?, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET owner_id = excluded.owner_id, set_id = excluded.set_id",
        )
        .bind(bucket)
        .bind(uid)
        .bind(now())
        .bind(set as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// C3：建桶自动指派——选**当前桶数最少**的 set（并列取小号）；无行数据按 0 计。
    /// 单 set/单机 k=1 恒 0（零查询）。指派只影响**新桶**；已有桶不迁（S-L 口径）。
    pub async fn pick_least_loaded_set(&self, k: u16) -> Result<u16, sqlx::Error> {
        if k <= 1 {
            return Ok(0);
        }
        let rows = sqlx::query("SELECT set_id, COUNT(*) AS c FROM buckets GROUP BY set_id")
            .fetch_all(&self.pool)
            .await?;
        let mut counts: std::collections::HashMap<u16, i64> = std::collections::HashMap::new();
        for r in &rows {
            let s = r.get::<i64, _>("set_id");
            if let Ok(s) = u16::try_from(s) {
                counts.insert(s, r.get::<i64, _>("c"));
            }
        }
        let mut best = 0u16;
        let mut best_c = i64::MAX;
        for i in 0..k {
            let c = counts.get(&i).copied().unwrap_or(0);
            if c < best_c {
                best = i;
                best_c = c;
            }
        }
        Ok(best)
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
        // handler 读取：ListBuckets 过滤 / CreateBucket 记账 / DeleteObjects 逐键复评
        //（随请求搬进 S3Request.extensions）
        cx.extensions_mut().insert(user.clone());
        // 管理员旁路**表级三档**，但不再提前 return：桶策略的显式 Deny 对管理员同样生效
        // （AWS 语义），且管理员请求照常落审计。旧实现此处直接 Ok，Deny 与审计双双落空
        // （verifier 阻断 2——两侧都要，admin 与子账户各自被检查项覆盖）。
        let is_admin = user.is_admin;

        let op = cx.s3_op().name();
        // 桶名必须用 `get_bucket_name()`：`as_bucket()` 只匹配 `S3Path::Bucket` 变体，
        // 对象路径（`Object{bucket,key}`）会返回 None——用它会把一切对象级 op 误判成
        // 「无桶」而全拒（子账户实弹 403 的根因，管理员旁路曾掩盖）。
        let bucket: Option<String> = cx.s3_path().get_bucket_name().map(str::to_owned);
        // DeleteObjects：对象键在请求体里，check 阶段拿不到 → 授权**整体下放 handler 逐键**
        //（service.rs::delete_objects 对每个键跑 policy×表三档，与单键删同口径；AWS 亦逐键
        // 授权）。此处只保身份与下面审计，避免「批量删绕过前缀 Deny」（verifier 阻断 1c）
        // 与反向「桶级 ARN Deny 误压整个批量请求」两种偏差。
        let delegated = op == "DeleteObjects";
        // 表级基线拆两轴：dst（目标/主体动作）与 src（仅 CopyObject 的源读）。两轴独立 =
        // 策略融合时不会互相抬升/塌缩（旧实现 src Allow 能抬掉 dst 表拒、dst Allow(Put) 能
        // 抬掉 src 表读——verifier 阻断 1b）；最终 allow = 两轴合取。
        let (mut allow_dst, mut allow_src) = if is_admin || delegated {
            (true, true)
        } else {
            // can()/can_opt() 取 by-value；user 后面还要用（策略 Principal / 审计）→ 克隆一份
            let gate_user = user.clone();
            match classify(op) {
                None => {
                    tracing::warn!(uid = user.uid, op, "授权拒绝（未映射 op，fail-closed）");
                    return Err(s3s::s3_error!(AccessDenied, "access denied"));
                }
                Some(gate) => match gate {
                    // 列表由 handler 过滤；建桶任何已认证用户（建后记账 owner）
                    Gate::Open => (true, true),
                    Gate::OwnerBucket => match &bucket {
                        Some(b) => (self.is_owner(b, user.uid).await.map_err(acl_err)?, true),
                        None => (false, true),
                    },
                    Gate::Need(action) => (self.can_opt(gate_user, action, &bucket).await?, true),
                    Gate::Copy => {
                        // 目标写 + 源读（源在 header；用 s3s 的解析器——header 是 URL 编码
                        // 且有 / 前导与 ?versionId 形态，解析失败与反序列化同判，直接拒绝）
                        let dst = self
                            .can_opt(gate_user.clone(), Action::Write, &bucket)
                            .await?;
                        let src = match cx
                            .headers()
                            .get(X_AMZ_COPY_SOURCE)
                            .and_then(|v| v.to_str().ok())
                            .map(CopySource::parse)
                        {
                            Some(Ok(CopySource::Bucket { bucket, .. })) => self
                                .can(gate_user, Action::Read, &bucket)
                                .await
                                .map_err(acl_err)?,
                            _ => false,
                        };
                        (dst, src)
                    }
                },
            }
        };
        // —— 桶策略融合（M4 切片 7）：显式 Deny 优先 → Allow 提升 → 无匹配回落三档 ——
        // 两轴**各自**融合：dst 轴所有 op；src 轴仅 CopyObject 且**始终**评估（与目标桶有无
        // 策略无关——旧实现把 src 评估嵌在 dst 有策略分支内，dst 无策略时 src Deny 被完全
        // 跳过：verifier 阻断 1a）。策略读/解析失败按轴 fail-closed 拒（不静默放行）。
        // 未映射 op（s3_action None）不参与。匿名放行未实现——记档于 policy.rs 头注。
        let key: Option<String> = cx.s3_path().as_object().map(|(_, k)| k.to_string());
        let (mut saw_deny, mut saw_error, mut saw_allow) = (false, false, false);
        if !delegated && let Some(b) = &bucket {
            match self.bucket_policy(b).await {
                Ok(Some(doc)) => {
                    let pr = crate::policy::PolicyRequest {
                        op,
                        ak: &ak,
                        uid: user.uid,
                        bucket: b,
                        key: key.as_deref(),
                    };
                    // CopyObject 的目标桶策略管目标写（不走 s3_action 通用映射）
                    let act = if op == "CopyObject" {
                        crate::policy::s3_action("PutObject")
                    } else {
                        crate::policy::s3_action(op)
                    };
                    if let Some(a) = act {
                        match crate::policy::evaluate(&doc, &pr, a) {
                            Some(false) => {
                                allow_dst = false;
                                saw_deny = true;
                            }
                            Some(true) => {
                                allow_dst = true;
                                saw_allow = true;
                            }
                            None => {} // 无匹配：回落既有判定
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, bucket = %b, "桶策略读取/解析失败——fail-closed 拒绝");
                    allow_dst = false;
                    saw_error = true;
                }
            }
        }
        if op == "CopyObject" {
            // 源桶策略管源读，**独立于目标桶策略**评估（Deny 优先：src Deny 压过 dst Allow
            // ——否则有目标桶写权者可绕过源桶 Deny 拷贝机密）。源桶策略读取失败同样 fail-closed。
            if let Some(Ok(CopySource::Bucket {
                bucket: src_bucket,
                key: src_key,
                ..
            })) = cx
                .headers()
                .get(X_AMZ_COPY_SOURCE)
                .and_then(|v| v.to_str().ok())
                .map(CopySource::parse)
            {
                match self.bucket_policy(&src_bucket).await {
                    Ok(Some(sdoc)) => {
                        let spr = crate::policy::PolicyRequest {
                            op,
                            ak: &ak,
                            uid: user.uid,
                            bucket: &src_bucket,
                            key: Some(&src_key),
                        };
                        if let Some(sa) = crate::policy::s3_action("GetObject") {
                            match crate::policy::evaluate(&sdoc, &spr, sa) {
                                Some(false) => {
                                    allow_src = false;
                                    saw_deny = true;
                                }
                                Some(true) => {
                                    allow_src = true;
                                    saw_allow = true;
                                }
                                None => {}
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!(error = %e, bucket = %src_bucket, "源桶策略读取失败——fail-closed");
                        allow_src = false;
                        saw_error = true;
                    }
                }
            }
        }
        let allow = allow_dst && allow_src;
        // 审计注解：只标「策略实际决定了方向」的判定；拒绝侧 Deny > error 优先归类
        let policy_detail: Option<&str> = if allow {
            if saw_allow {
                Some("policy:allow")
            } else {
                None
            }
        } else if saw_deny {
            Some("policy:deny")
        } else if saw_error {
            Some("policy:error")
        } else {
            None
        };

        // 审计（允许与拒绝都记；failed 只 warn——审计不可用时不得反向影响判定）
        if let Err(e) = self
            .write_audit(
                user.uid,
                op,
                bucket.as_deref(),
                key.as_deref(),
                if allow { "allow" } else { "deny" },
                policy_detail,
            )
            .await
        {
            tracing::warn!(error = %e, "审计落行失败");
        }

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
mod pick_set_tests {
    use super::*;

    /// C3：最少桶数优先；并列取小号；k=1 恒 0 且零查询。
    #[tokio::test]
    async fn pick_least_loaded_set() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, set_id INTEGER NOT NULL DEFAULT 0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let acl = Acl::new(pool.clone());
        assert_eq!(acl.pick_least_loaded_set(1).await.unwrap(), 0, "k=1 恒 0");
        // 空库 → 0（并列取小号）
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 0);
        for n in ["a", "b"] {
            sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES (?, 0, 0)")
                .bind(n)
                .execute(&pool)
                .await
                .unwrap();
        }
        // set0=2, set1=0, set2=0 → 取 1
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 1);
        sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES ('c', 0, 1)")
            .execute(&pool)
            .await
            .unwrap();
        // set0=2, set1=1, set2=0 → 取 2
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 2);
        sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES ('d', 0, 2)")
            .execute(&pool)
            .await
            .unwrap();
        // 2/1/1 → 取 1（并列取小号）
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 1);
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
              versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0);\
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
        ak: String::new(),
    };
    const ADMIN: AuthedUser = AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
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
                is_admin: false,
                ak: "AK_OK".to_string()
            })
        );
        assert_eq!(a.user_of_key("AK_EXPIRED").await.unwrap(), None);
        assert_eq!(
            a.user_of_key("AK_ADMIN").await.unwrap(),
            Some(AuthedUser {
                uid: 9,
                is_admin: true,
                ak: "AK_ADMIN".to_string()
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

    /// M5 切片 0（verifier 阻断 1 修复）：带 set 的建桶记账**覆盖**语义——删桶记账容忍失败
    /// 留下的陈旧行随重建收敛 owner/set（旧 INSERT OR IGNORE 整条 no-op → 旧 owner/set 分叉）。
    #[tokio::test]
    async fn record_bucket_owner_set_overrides_stale_row() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        a.record_bucket_owner_set("b", 2, 0).await.unwrap();
        a.record_bucket_owner_set("b", 3, 1).await.unwrap(); // 重建：覆盖
        assert!(!a.is_owner("b", 2).await.unwrap(), "旧 owner 不得继承");
        assert!(a.is_owner("b", 3).await.unwrap());
        let row = sqlx::query("SELECT set_id FROM buckets WHERE name = 'b'")
            .fetch_one(&a.pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("set_id"), 1, "set 应收敛到重建值");
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
                set: None,
            }))),
        );
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: String::new(),
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
            ak: String::new(),
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
                set: None,
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
                set: None,
            }))),
        );
        let v = a2.visible(7).await.unwrap();
        assert!(v.contains(&"beta".to_string()) && v.contains(&"gamma".to_string()));
    }
}
