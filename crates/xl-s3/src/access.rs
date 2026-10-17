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

    /// M6 切片 0：桶通知配置（None = 未配置；json 原文，解析在 notify 模块）。
    pub async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT config_json FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("config_json")))
    }

    /// M6 切片 0：写桶通知配置（全量替换）。
    pub async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(config_json)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M6 切片 0：删桶时清通知配置（防同名重建继承旧规则）。
    pub async fn clear_bucket_notification(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM event_configs WHERE bucket = ?")
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
        let (allowed, detail) = match decision {
            Some(true) => (true, None),
            Some(false) => (false, Some("policy:deny")),
            None => (self.can(user.clone(), Action::Delete, bucket).await?, None),
        };
        // M5 切片 5（S8）：会话策略在这里同步叠交集——批量删逐键判定与 check() 的单键
        // 口径必须同源，否则会话策略可被一条批量删整体绕过。会话 Deny/无匹配 → 拒。
        if allowed && let Some(sdoc) = self.session_policy_of(&user.ak).await? {
            let pr = crate::policy::PolicyRequest {
                op: "DeleteObjects",
                ak: &user.ak,
                uid: user.uid,
                bucket,
                key: Some(key),
            };
            match crate::policy::s3_action("DeleteObject")
                .and_then(|a| crate::policy::evaluate(&sdoc, &pr, a))
            {
                Some(true) => {}
                _ => return Ok((false, Some("session:deny"))),
            }
        }
        Ok((allowed, detail))
    }

    /// M5 切片 5（S8）：会话策略交集融合——在表三档 × 桶策略之后叠会话判定：
    /// 会话 Deny → 拒；**无匹配 → 拒**（未 Allow 即拒）；Allow → 保留基线（不能抬升）。
    /// CopyObject 双轴分别求值（dst=`PutObject` 映射 / src=`GetObject`），与桶策略融合同构。
    /// `s3_action` 未映射 op：会话不约束（词表缺项，同桶策略口径；定稿记档）。
    /// 返回 `((allow_dst, allow_src), detail)`；无会话策略 = 原样返回 `((..), None)`。
    /// 抽出独立方法以便单测直钉（S3AccessContext 不可外部构造，check() 本体无法在单测驱动）。
    async fn fuse_session(
        &self,
        pr: crate::policy::PolicyRequest<'_>,
        src: Option<crate::policy::PolicyRequest<'_>>,
        mut allow_dst: bool,
        mut allow_src: bool,
    ) -> Result<((bool, bool), Option<&'static str>), sqlx::Error> {
        let op = pr.op;
        let Some(sdoc) = self.session_policy_of(pr.ak).await? else {
            return Ok(((allow_dst, allow_src), None));
        };
        let dst_act = if op == "CopyObject" {
            crate::policy::s3_action("PutObject")
        } else {
            crate::policy::s3_action(op)
        };
        let mut denied = false;
        if let Some(a) = dst_act
            && crate::policy::evaluate(&sdoc, &pr, a) != Some(true)
        {
            allow_dst = false;
            denied = true;
        }
        // P2-1 复验修复：src 轴在**源桶/源键**的 pr 上求值（调用方自 CopySource 构造；
        // 解析失败 → None，此时 Gate::Copy 基线已置 allow_src=false，fail-closed 不塌）
        if let Some(spr) = src
            && let Some(a) = crate::policy::s3_action("GetObject")
            && crate::policy::evaluate(&sdoc, &spr, a) != Some(true)
        {
            allow_src = false;
            denied = true;
        }
        Ok((
            (allow_dst, allow_src),
            Some(if denied {
                "session:deny"
            } else {
                "session:allow"
            }),
        ))
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
        // M5 切片 5：与 SigV4Auth::get_secret_key 同口径（含会话父链条件——三处 SQL 逐字
        // 一致：auth.rs / 此处 / cluster_control AccessKey；P3-1 补父**用户**停用同样传播）。
        let row = sqlx::query(
            "SELECT ak.user_id AS user_id, u.is_admin AS is_admin \
             FROM access_keys ak JOIN users u ON u.id = ak.user_id \
             LEFT JOIN access_keys p ON p.access_key = ak.parent_ak \
             LEFT JOIN users pu ON pu.id = p.user_id \
             WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
               AND (ak.expires_at IS NULL OR ak.expires_at > ?) \
               AND (ak.parent_ak IS NULL OR (p.kind = 'long-term' \
                    AND p.status = 'active' AND (p.expires_at IS NULL OR p.expires_at > ?) \
                    AND pu.status = 'active'))",
        )
        .bind(access_key)
        .bind(now())
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

    /// M5 切片 5（S8）：AK 的会话策略（`kind='session'` 且 `session_policy` 非空时）。
    /// 本地查无 → 扇出（远端应答带 `session_policy`；NULL/空 = 无约束，与线上口径一致）。
    /// 解析失败 → Err（调用方 fail-closed 拒）。非会话键 → `None`。
    pub async fn session_policy_of(
        &self,
        access_key: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, sqlx::Error> {
        // 空串 = 控制台/aether 合成身份（AuthedUser 口径——各自面不走策略）→ 无会话
        if access_key.is_empty() {
            return Ok(None);
        }
        let local: Option<Option<String>> =
            sqlx::query_scalar("SELECT session_policy FROM access_keys WHERE access_key = ?")
                .bind(access_key)
                .fetch_optional(&self.pool)
                .await?;
        let raw = match local {
            Some(sp) => sp,
            None => {
                // 远端铸的会话键：扇出取策略原文（ControlRow.session_policy）。
                // P3-2 复验修复：本地无行 + 扇出**无答**（全不可达）→ Err（fail-closed）——
                // 「答了但无策略」与「没人答得上来」不可混同，后者静默放行即 fail-open
                // （user_of_key 此前已答过，本处再答失败 = 竞态窗口）。
                match &self.mesh {
                    Some(m) => match m.ask(ControlQuery::AccessKey(access_key.to_string())).await {
                        Some(row) => row.session_policy,
                        None => {
                            return Err(sqlx::Error::Protocol(
                                "会话策略扇出不可达（fail-closed）".into(),
                            ));
                        }
                    },
                    None => None,
                }
            }
        };
        let Some(raw) = raw.filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let doc: crate::policy::PolicyDoc =
            serde_json::from_str(&raw).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Some(doc))
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
        // —— 会话策略融合（M5 切片 5 / S8）：在表三档 × 桶策略融合之后叠**交集** ——
        // 语义本体在 `fuse_session`（抽出以便单测直钉——S3AccessContext 不可外部构造）。
        // **P2-1 复验修复**：CopyObject 的 src 轴必须用**源桶/源键**构造 PolicyRequest 求值
        // （与上面桶策略 src 轴同构）——曾复用 dst 的 pr，会话对源读的收窄可被 copy 绕过。
        let mut session_detail: Option<&'static str> = None;
        if !delegated && let Some(b) = &bucket {
            // CopyObject 源解析一次持有（String 落外层作用域，PolicyRequest 借之）
            let copy_src: Option<(String, String)> = if op == "CopyObject" {
                match cx
                    .headers()
                    .get(X_AMZ_COPY_SOURCE)
                    .and_then(|v| v.to_str().ok())
                    .map(CopySource::parse)
                {
                    Some(Ok(CopySource::Bucket { bucket, key, .. })) => {
                        Some((bucket.to_string(), key.to_string()))
                    }
                    _ => None,
                }
            } else {
                None
            };
            let src_pr =
                copy_src
                    .as_ref()
                    .map(|(src_bucket, src_key)| crate::policy::PolicyRequest {
                        op,
                        ak: &ak,
                        uid: user.uid,
                        bucket: src_bucket,
                        key: Some(src_key),
                    });
            let ((d, s), detail) = self
                .fuse_session(
                    crate::policy::PolicyRequest {
                        op,
                        ak: &ak,
                        uid: user.uid,
                        bucket: b,
                        key: key.as_deref(),
                    },
                    src_pr,
                    allow_dst,
                    allow_src,
                )
                .await
                .map_err(acl_err)?;
            allow_dst = d;
            allow_src = s;
            session_detail = detail;
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
        // 会话判定缀后（policy 在前、session 在后；「;」分隔供审计查询 LIKE 匹配）
        let detail: Option<String> = match (policy_detail, session_detail) {
            (Some(p), Some(s)) => Some(format!("{p};{s}")),
            (Some(p), None) => Some(p.to_string()),
            (None, Some(s)) => Some(s.to_string()),
            (None, None) => None,
        };

        // 审计（允许与拒绝都记；failed 只 warn——审计不可用时不得反向影响判定）
        if let Err(e) = self
            .write_audit(
                user.uid,
                op,
                bucket.as_deref(),
                key.as_deref(),
                if allow { "allow" } else { "deny" },
                detail.as_deref(),
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
        // M6 切片 0/4：桶通知与复制配置（写配置归桶主，读归读档）
        "PutBucketNotificationConfiguration" => Gate::OwnerBucket,
        "GetBucketNotificationConfiguration" => Gate::Need(Action::Read),
        "PutBucketReplication" => Gate::OwnerBucket,
        "GetBucketReplication" => Gate::Need(Action::Read),
        "DeleteBucketReplication" => Gate::OwnerBucket,
        // 读（HeadBucket 归读：AWS 语义绑 ListBucket）
        "HeadBucket"
        | "ListObjectsV2"
        | "ListParts"
        | "GetObject"
        | "HeadObject"
        | "ListObjectVersions"
        | "SelectObjectContent" => Gate::Need(Action::Read),
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
    pub(super) async fn acl() -> Acl {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, is_admin INTEGER NOT NULL DEFAULT 0, \
              status TEXT NOT NULL DEFAULT 'active');\
             CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
              parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');\
             CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, \
              versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
              can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
              PRIMARY KEY (user_id, bucket));\
             CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL);",
        )
        .execute(&pool)
        .await
        .unwrap();
        Acl::new(pool)
    }

    pub(super) async fn add_user(a: &Acl, id: i64, admin: bool) {
        sqlx::query("INSERT INTO users (id, username, is_admin) VALUES (?, ?, ?)")
            .bind(id)
            .bind(format!("u{id}"))
            .bind(admin as i64)
            .execute(&a.pool)
            .await
            .unwrap();
    }

    pub(super) async fn add_key(a: &Acl, ak: &str, uid: i64, expires_at: Option<i64>) {
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
            "SelectObjectContent",
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

    pub(super) fn mesh_with(peer_row: Option<ControlRow>) -> ControlMesh {
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
                session_policy: None,
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
                session_policy: None,
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
                session_policy: None,
            }))),
        );
        let v = a2.visible(7).await.unwrap();
        assert!(v.contains(&"beta".to_string()) && v.contains(&"gamma".to_string()));
    }
}

#[cfg(test)]
mod session_tests {
    use super::tests::{acl, add_key, add_user, mesh_with};
    use super::*;

    /// 会话键行（测试装置）：kind='session' + parent + 可选 policy。
    async fn add_session(a: &Acl, ak: &str, uid: i64, parent: &str, policy: Option<&str>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, parent_ak, session_policy, kind) \
             VALUES (?, 'SK_SESS', ?, 'active', 4000000000, 0, ?, ?, 'session')",
        )
        .bind(ak)
        .bind(uid)
        .bind(parent)
        .bind(policy)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    const GET_ONLY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#;

    /// session_policy_of：本地命中 / 非会话键 None / 坏 JSON Err / 扇出取原文。
    #[tokio::test]
    async fn session_policy_of_sources() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_S1", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        add_session(&a, "ASIA_S2", 7, "AKIA_LONG", None).await;
        assert!(a.session_policy_of("ASIA_S1").await.unwrap().is_some());
        assert!(a.session_policy_of("ASIA_S2").await.unwrap().is_none());
        assert!(a.session_policy_of("AKIA_LONG").await.unwrap().is_none());
        // 坏 JSON → Err（调用方 fail-closed）
        add_session(&a, "ASIA_BAD", 7, "AKIA_LONG", Some("{not-json")).await;
        assert!(a.session_policy_of("ASIA_BAD").await.is_err());
        // 扇出：本地无行 → 远端应答带 session_policy
        let b = Acl::with_mesh(
            acl().await.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: Some(false),
                active: Some(true),
                secret: Some("SK_PEER".into()),
                session_policy: Some(GET_ONLY.to_string()),
                ..Default::default()
            }))),
        );
        assert!(b.session_policy_of("ASIA_REMOTE").await.unwrap().is_some());
        // 远端无策略（None）= 无约束
        let c = Acl::with_mesh(
            acl().await.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                secret: Some("SK".into()),
                session_policy: None,
                ..Default::default()
            }))),
        );
        assert!(c.session_policy_of("ASIA_REMOTE2").await.unwrap().is_none());
    }

    /// fuse_session 调用包装（测试便利）：构造 dst PolicyRequest；`src` = CopyObject 源（桶, 键）。
    #[allow(clippy::too_many_arguments)] // 测试装置：平铺语义参（同 rebalance 先例）
    async fn fuse(
        a: &Acl,
        ak: &str,
        op: &str,
        bucket: &str,
        key: Option<&str>,
        src: Option<(&str, &str)>,
        d: bool,
        s: bool,
    ) -> ((bool, bool), Option<&'static str>) {
        a.fuse_session(
            crate::policy::PolicyRequest {
                op,
                ak,
                uid: 7,
                bucket,
                key,
            },
            src.map(|(sb, sk)| crate::policy::PolicyRequest {
                op,
                ak,
                uid: 7,
                bucket: sb,
                key: Some(sk),
            }),
            d,
            s,
        )
        .await
        .unwrap()
    }

    /// fuse_session 交集矩阵：Allow 保留 / Deny 拒 / 无匹配拒 / CopyObject 双轴 / 未映射 op 不约束 / 非会话无痕。
    #[tokio::test]
    async fn fuse_session_intersection_matrix() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        // 会话 Allow：基线保留
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "GetObject",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // 无匹配（PutObject 不在策略里）→ 交集拒
        let ((d, _), det) = fuse(
            &a,
            "ASIA_G",
            "PutObject",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
        // 资源不匹配（别的桶）→ 拒
        let ((d, _), det) = fuse(
            &a,
            "ASIA_G",
            "GetObject",
            "other",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
        // CopyObject：dst 轴（PutObject 映射）无匹配 → 拒；src 轴（GetObject）在策略内 → 保留
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("bkt", "k")),
            true,
            true,
        )
        .await;
        assert!(!d && s && det == Some("session:deny"));
        // 反向：策略只允许 PutObject 时，CopyObject 的 src 轴（GetObject）红、dst 轴绿
        add_session(
            &a,
            "ASIA_P",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let ((d, s), det) = fuse(
            &a,
            "ASIA_P",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("bkt", "k")),
            true,
            true,
        )
        .await;
        assert!(d && !s && det == Some("session:deny"));
        // 未映射 op（如 PutObjectLockConfiguration）：会话不约束（词表缺项口径）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "PutObjectLockConfiguration",
            "bkt",
            None,
            None,
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // 非会话键：原样返回、无 detail
        let ((d, s), det) = fuse(
            &a,
            "AKIA_LONG",
            "PutObject",
            "bkt",
            Some("k"),
            None,
            true,
            false,
        )
        .await;
        assert!((d, s, det) == (true, false, None));
        // 显式 Deny 语句 → 拒（Deny 优先于同策略 Allow）
        add_session(
            &a,
            "ASIA_D",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}]}"#),
        )
        .await;
        let ((d, _), det) = fuse(
            &a,
            "ASIA_D",
            "GetObject",
            "bkt",
            Some("secret.txt"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
    }

    /// 批量删逐键判定叠会话：会话只放 GetObject → 删键被会话拒（防批量删绕过）。
    #[tokio::test]
    async fn key_delete_decision_applies_session() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        // 表三档放行（can_delete=1）
        sqlx::query("INSERT INTO permissions (user_id, bucket, can_delete) VALUES (7, 'bkt', 1)")
            .execute(&a.pool)
            .await
            .unwrap();
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_G".into(),
        };
        assert_eq!(
            a.key_delete_decision(&u, "bkt", "k").await.unwrap(),
            (false, Some("session:deny"))
        );
        // 会话未覆盖 DeleteObject → 恒拒；换成放行 DeleteObject 的会话 → 表三档生效
        add_session(
            &a,
            "ASIA_DEL",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let u2 = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_DEL".into(),
        };
        assert_eq!(
            a.key_delete_decision(&u2, "bkt", "k").await.unwrap(),
            (true, None)
        );
    }

    /// P2-1 复验修复判别：CopyObject 的 src 轴用**源桶/源键**求值——曾复用 dst pr，
    /// 会话对源读的收窄可被 copy 绕过（src 轴在目标资源上误命中 Allow）。
    #[tokio::test]
    async fn fuse_session_copy_src_axis_uses_source_resource() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        // 会话收窄：bkt/* 可写、srcbkt/* 可读（「只许从 srcbkt 拷入 bkt」）
        add_session(
            &a,
            "ASIA_CP",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject"],"Resource":"arn:aws:s3:::bkt/*"},{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::srcbkt/*"}]}"#),
        )
        .await;
        // 合法拷贝：dst=bkt src=srcbkt → 双轴过
        let ((d, s), det) = fuse(
            &a,
            "ASIA_CP",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("srcbkt", "o")),
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // 未覆盖源：src=other → src 轴在源资源上无匹配 → 拒（本用例对旧码亦红/绿不可判别，
        // 判别核心在下一段「安全向」）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_CP",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("other", "o")),
            true,
            true,
        )
        .await;
        assert!(
            (d, s, det) == (true, false, Some("session:deny")),
            "src 轴必须用源桶求值（P2-1）"
        );
        // 安全向判别（verifier 复现口径）：会话在 **dst 资源**上放行 Get+Put 时，
        // 「从任意源桶拷入」必须仍被 src 轴拒——旧实现（src 轴复用 dst pr）会在此误放行。
        add_session(
            &a,
            "ASIA_OPEN",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let ((d, s), det) = fuse(
            &a,
            "ASIA_OPEN",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("secret", "o")),
            true,
            true,
        )
        .await;
        assert!(
            (d, s, det) == (true, false, Some("session:deny")),
            "src≠dst 桶时 src 轴不得在目标资源上误命中（P2-1 安全向）"
        );
        // 同桶拷贝（dst 即源）→ 双轴允许（策略覆盖 bkt/*）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_OPEN",
            "CopyObject",
            "bkt",
            Some("k2"),
            Some(("bkt", "k1")),
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
    }

    /// P3-2 复验修复判别：本地无行 + 扇出无答（全不可达）→ Err（fail-closed），
    /// 不得把「没人答得上来」静默当「无策略」放行。
    #[tokio::test]
    async fn session_policy_of_mesh_no_answer_is_error() {
        let d = Acl::with_mesh(acl().await.pool.clone(), Some(mesh_with(None)));
        assert!(d.session_policy_of("ASIA_REMOTE_X").await.is_err());
    }
}
