//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;

use crate::raft::FencingEpoch;
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::bucket::BucketTxn;
use xl_store::control_store::event_outbox::EventOutboxTxn;
use xl_store::control_store::iam::{IamTxn, PermissionRow as IamPermissionRow};
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryConfigView, InventoryCursorRow, InventoryIncrementalRow,
    InventoryTxn,
};
use xl_store::control_store::jobs::{JobTxn, NewJob as JobInsert, NewTask as JobTaskInsert};
use xl_store::control_store::lifecycle::LifecycleTxn;
use xl_store::control_store::repl_outbox::ReplOutboxTxn;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};
use xl_store::control_store::tier::{TierRestoreRow, TierTxn};
use xl_store::control_store::uploads::{NewUpload, UploadTxn};
use xl_store::control_store::{AccessPointEntry, ControlTxn, OlapConfigEntry, OwnerRef};

/// Backend-neutral control-plane error (H1: no SQLite error type survives).
#[derive(Debug)]
pub enum DbError {
    Backend(String),
    Protocol(String),
    Io(std::io::Error),
    RowNotFound,
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(m) => write!(f, "control store backend: {m}"),
            Self::Protocol(m) => write!(f, "invalid control data: {m}"),
            Self::Io(e) => write!(f, "io: {e}"),
            Self::RowNotFound => write!(f, "row not found"),
        }
    }
}

impl std::error::Error for DbError {}

pub type Result<T> = std::result::Result<T, DbError>;

/// `ControlError` → facade error. Text is preserved so unique-violation checks
/// and operator messages keep working.
pub fn to_db(e: xl_store::control_store::ControlError) -> DbError {
    use xl_store::control_store::ControlError;
    match e {
        ControlError::Backend(m) => DbError::Backend(m),
        ControlError::Invalid(m) => DbError::Protocol(m),
        ControlError::MissingOwner(_) => DbError::RowNotFound,
    }
}

/// Duplicate-key detection for console/API 409/400 mapping (object store emits
/// the same "UNIQUE constraint failed: ..." text as the old adapter).
pub fn is_unique_violation(e: &DbError) -> bool {
    match e {
        DbError::Backend(m) | DbError::Protocol(m) => m.contains("UNIQUE constraint failed"),
        _ => false,
    }
}

pub struct Db {
    /// Object control root, injected at startup before serving requests.
    object: std::sync::OnceLock<std::sync::Arc<crate::control_backend::ObjectControl>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UploadRecord {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub original_name: Option<String>,
    pub created_at: i64,
}

fn upload_record(r: xl_store::control_store::uploads::UploadRow) -> UploadRecord {
    UploadRecord {
        saved_path: r.saved_path,
        bucket: r.bucket,
        key: r.key,
        group_name: r.group_name,
        size: r.size,
        etag: r.etag,
        original_name: r.original_name,
        created_at: r.created_at,
    }
}

/// 本站在复制关系中的身份（`site_config` 单行；token 仅供控制面通信用，接口层负责脱敏）。
#[derive(Debug, Clone)]
pub struct SiteIdentity {
    pub id: String,
    pub name: String,
    pub region: String,
    pub priority: i64,
    pub token: String,
}

/// 对端站点行（`sites`；含出站凭据——接口层必须脱敏）。
#[derive(Debug, Clone)]
pub struct SitePeer {
    pub peer_id: String,
    /// 对端 `site_config.id`（配对时由 identity 校验取得；dead 表按它归属）
    pub remote_site_id: String,
    pub name: String,
    pub endpoint: String,
    pub region: String,
    pub priority: i64,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub peer_site_token: String,
    pub created_at: i64,
    pub last_seen: Option<i64>,
    pub last_error: Option<String>,
    /// M9：对端 Raft u64 节点 id（M7 行 NULL）。
    pub raft_node_id: Option<u64>,
    /// M9 S7.4：替换站点后，sync 修复完成前拒绝向该对端复制/服务。
    pub reconcile_required: bool,
}

/// 控制台用户行（子账户管理页用；`permissions` 全量随行返回，页面上直接展示摘要）。
#[derive(Debug, Clone, Serialize)]
pub struct UserRow {
    pub id: i64,
    pub username: String,
    pub is_admin: bool,
    /// `"active"` / `"disabled"`（停用即全链路失效）
    pub status: String,
    pub created_at: i64,
    pub permissions: Vec<PermissionRow>,
}

/// 账户行（AC-0；控制台 `/accounts` 直接序列化）。
#[derive(Debug, Clone, Serialize)]
pub struct AccountRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub admin_uid: Option<i64>,
    pub bucket_quota: Option<i64>,
    /// 软字节配额（NULL = 不限；只告警不阻写，AC-2）。
    pub soft_byte_quota: Option<i64>,
    /// 硬字节配额（NULL/负数 = 不限；写入原子预留，删除释放）。
    pub hard_byte_quota: Option<i64>,
    /// 硬对象数配额（当前版本键数；NULL/负数 = 不限）。
    pub object_quota: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub member_count: i64,
    pub bucket_count: i64,
}

/// 账户成员行（列表页用；不返回任何凭据）。
#[derive(Debug, Clone, Serialize)]
pub struct AccountMemberRow {
    pub uid: i64,
    pub username: String,
    pub role: String,
    pub is_admin: bool,
    pub status: String,
    pub created_at: i64,
}

/// 子账户 × 桶 的三档权限（与 `permissions` 表一一对应；序列化给前端用 read/write/delete）。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PermissionRow {
    pub bucket: String,
    pub read: bool,
    pub write: bool,
    pub delete: bool,
}

// ——— M8 S0：作业引擎数据结构 ———

/// 建作业时的一条任务。
#[derive(Debug, Clone)]
pub struct NewJobTask {
    pub action: String,
    pub payload: String,
    pub dedupe_key: Option<String>,
}

/// 建作业规格（S3/S4b/S5 共用）。
#[derive(Debug, Clone, Default)]
pub struct JobSpec {
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub requested_by: Option<i64>,
    pub priority: i64,
    pub parent_job_id: Option<i64>,
    pub report_bucket: Option<String>,
    pub report_prefix: Option<String>,
    pub spec_json: Option<String>,
    pub tasks: Vec<NewJobTask>,
}

/// `JobSpec` → 缝的整行输入（本地建作业 SQL 已收口到适配器）。
fn job_insert(spec: &JobSpec, now: i64) -> JobInsert {
    JobInsert {
        kind: spec.kind.clone(),
        bucket: spec.bucket.clone(),
        prefix: spec.prefix.clone(),
        state: "queued".into(),
        total: spec.tasks.len() as i64,
        priority: spec.priority,
        requested_by: spec.requested_by,
        parent_job_id: spec.parent_job_id,
        report_bucket: spec.report_bucket.clone(),
        report_prefix: spec.report_prefix.clone(),
        spec_json: spec.spec_json.clone(),
        created_at: now,
        updated_at: now,
        started_at: None,
    }
}

fn new_tasks(spec: &JobSpec, now: i64) -> Vec<JobTaskInsert> {
    spec.tasks
        .iter()
        .map(|t| JobTaskInsert {
            action: t.action.clone(),
            payload_json: t.payload.clone(),
            dedupe_key: t.dedupe_key.clone(),
            next_at: now,
        })
        .collect()
}

/// M8 S4b：Restore 行（`tier_restores`；reaper/对账的跨 crate 视图）。
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreRow {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub expires_at: i64,
    pub job_id: Option<i64>,
    pub state: String,
    pub created_at: i64,
}

fn restore_row_of(r: TierRestoreRow) -> RestoreRow {
    RestoreRow {
        bucket: r.bucket,
        key: r.key,
        version_id: r.version_id,
        expires_at: r.expires_at,
        job_id: r.job_id,
        state: r.state,
        created_at: r.created_at,
    }
}

/// M8 S5：Inventory 运行行（`started_at` = 实际开始执行的时刻）。
/// 类型收敛到 ControlStore 缝（含 fence 两列）；`id` 是行内不可变属性，不是身份键。
pub use xl_store::control_store::inventory::InventoryRunRow;

/// 领取到的任务（worker 执行输入）。
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub job_id: i64,
    pub task_no: i64,
    pub action: String,
    pub payload_json: String,
    pub attempt: i64,
}

/// 账户表已规范化（v2 回填/重写完成）。standalone 启动时写；Raft 模式由后续
/// canonicalization worker 在 level-3 写，快照读取以本 marker 为门。
pub const META_ACCOUNTS_CANONICAL: &str = "accounts_canonical_v1";

pub(crate) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Db {
    /// Object control backend: no SQLite pool and no schema replay. Control
    /// reads/writes go through the injected `ObjectControl` root.
    pub async fn connect_object() -> Result<Self> {
        Ok(Self {
            object: std::sync::OnceLock::new(),
        })
    }

    /// Object-root marker read.
    pub async fn schema_meta(&self, key: &str) -> Result<Option<String>> {
        let Some(object) = self.object.get() else {
            return Ok(None);
        };
        let pinned = object
            .control
            .pin()
            .await
            .map_err(|e| DbError::Backend(format!("对象控制面根读取失败：{e}")))?;
        Ok(pinned.root.meta.markers.get(key).cloned())
    }

    /// Object-root marker write (unconditional).
    pub async fn schema_meta_set(&self, key: &str, value: &str) -> Result<()> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.control_write("schema-meta", move |txn| {
            let key = key.clone();
            let value = value.clone();
            Box::pin(async move { txn.schema_meta_set(&key, &value).await })
        })
        .await
    }

    /// Object-root marker write-if-absent.
    pub async fn schema_meta_set_if_absent(&self, key: &str, value: &str) -> Result<bool> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.control_write("schema-meta", move |txn| {
            let key = key.clone();
            let value = value.clone();
            Box::pin(async move { txn.schema_meta_set_if_absent(&key, &value).await })
        })
        .await
    }

    /// Alias used by startup code; unconditional upsert.
    pub async fn upsert_schema_meta(&self, key: &str, value: &str) -> Result<()> {
        self.schema_meta_set(key, value).await
    }

    // ---------- M8（S1）：访问点 ----------

    /// 建 AP（同名冲突由调用方按 409 处理）。`false` = 同名已存在（不覆盖）。
    pub async fn create_access_point(
        &self,
        name: &str,
        bucket: &str,
        owner_id: i64,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        let owner = self.owner_ref(owner_id).await?;
        let name = name.to_owned();
        let bucket = bucket.to_owned();
        let policy_json = policy_json.map(str::to_owned);
        return self
            .control_write("access-points", move |txn| {
                let name = name.clone();
                let bucket = bucket.clone();
                let policy_json = policy_json.clone();
                let owner = owner.clone();
                Box::pin(async move {
                    txn.access_point_create(&AccessPointEntry {
                        name,
                        bucket,
                        owner,
                        policy_json,
                        created_at: now,
                        updated_at: now,
                    })
                    .await
                })
            })
            .await;
    }

    /// 账户命名空间 v1：桶主的 12 位账户 ID（NULL/管理员桶 = 系统账户）。
    /// 本节点无桶行（非 Raft 集群）时同样回落到系统账户。
    pub async fn bucket_account(&self, bucket: &str) -> Result<String> {
        Ok(
            match self
                .control_store()
                .bucket(bucket)
                .await
                .map_err(crate::db::to_db)?
            {
                Some(v) => xl_s3::access::bucket_account_id(v.owner_uid, v.owner_is_admin),
                None => xl_s3::access::LEGACY_ACCOUNT.to_string(),
            },
        )
    }

    /// 更新 AP 策略（None = 清策略）。
    pub async fn set_access_point_policy(
        &self,
        name: &str,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        let name = name.to_owned();
        let policy_json = policy_json.map(str::to_owned);
        return self
            .control_write("access-points", move |txn| {
                let name = name.clone();
                let policy_json = policy_json.clone();
                Box::pin(async move {
                    txn.access_point_set_policy(&name, policy_json.as_deref(), now)
                        .await
                })
            })
            .await;
    }

    pub async fn delete_access_point(&self, name: &str) -> Result<bool> {
        let name = name.to_owned();
        return self
            .control_write("access-points", move |txn| {
                let name = name.clone();
                Box::pin(async move { txn.access_point_delete(&name).await })
            })
            .await;
    }

    // ---------- M8（S2）：Object Lambda 变换器 ----------

    /// 每 AP 至多一条（PK upsert）；保存前已由调用方编译校验。
    #[allow(clippy::too_many_arguments)]
    pub async fn set_olap_config(
        &self,
        name: &str,
        kind: &str,
        endpoint: Option<&str>,
        module: Option<&[u8]>,
        module_sha256: Option<&str>,
        timeout_ms: i64,
        max_response_bytes: i64,
        now: i64,
    ) -> Result<()> {
        let row = OlapConfigEntry {
            access_point: name.to_string(),
            kind: kind.to_string(),
            endpoint: endpoint.map(str::to_string),
            module: module.map(<[u8]>::to_vec),
            module_sha256: module_sha256.map(str::to_string),
            timeout_ms,
            max_response_bytes,
            updated_at: now,
        };
        return self
            .control_write("olap", move |txn| {
                let row = row.clone();
                Box::pin(async move { txn.olap_config_upsert(&row).await })
            })
            .await;
    }

    // ---------- M8（S0）：桶删除的 M8 状态清理 ----------

    /// 删桶后清理 M8 表（jobs 按审计保留）。S3/console/cluster 路径走这里；
    /// site apply 在既有事务内走 [`Db::clear_bucket_m8_tx`]。
    pub async fn clear_bucket_m8(&self, bucket: &str) -> Result<()> {
        let bucket = bucket.to_owned();
        return self
            .control_write("bucket-m8", move |txn| {
                let bucket = bucket.clone();
                Box::pin(async move {
                    txn.delete_access_points_for_bucket(&bucket).await?;
                    txn.delete_lifecycle_inventory_tier_for_bucket(&bucket)
                        .await
                })
            })
            .await;
    }

    // ---------- M7：站点身份与关系 ----------

    pub async fn site_identity(&self) -> Result<SiteIdentity> {
        let r = self
            .control_store()
            .site_identity()
            .await
            .map_err(crate::db::to_db)?
            .ok_or_else(|| crate::db::DbError::RowNotFound)?;
        Ok(SiteIdentity {
            id: r.id,
            name: r.name,
            region: r.region,
            priority: r.priority,
            token: r.token,
        })
    }

    /// `[site]` 配置覆盖（load 在桶对账之前调用；None 字段保持种子值）。
    pub async fn apply_site_config(
        &self,
        name: Option<&str>,
        region: Option<&str>,
        priority: Option<i64>,
        token: Option<&str>,
    ) -> Result<()> {
        let name = name.map(str::to_string);
        let region = region.map(str::to_string);
        let token = token.map(str::to_string);
        self.control_write("startup", move |txn| {
            let name = name.clone();
            let region = region.clone();
            let token = token.clone();
            Box::pin(async move {
                txn.site_config_apply(
                    name.as_deref(),
                    region.as_deref(),
                    priority,
                    token.as_deref(),
                )
                .await
            })
        })
        .await
    }

    pub async fn list_sites(&self) -> Result<Vec<SitePeer>> {
        let rows = self
            .control_store()
            .sites()
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows.into_iter().map(site_peer).collect())
    }

    pub async fn get_site(&self, peer_id: &str) -> Result<Option<SitePeer>> {
        Ok(self
            .control_store()
            .site(peer_id)
            .await
            .map_err(crate::db::to_db)?
            .map(site_peer))
    }

    pub async fn count_sites(&self) -> Result<i64> {
        self.control_store()
            .site_count()
            .await
            .map_err(crate::db::to_db)
    }

    /// S7.4：Raft 配对行写入/更新；按 peer_id 或 remote_site_id 采用既有 M7 行，
    /// 不写 `site_cursors/changelog`（Raft 下 sites 是站点本地表，S6d 启动断言要求 site_* 为空）。
    pub async fn upsert_pair_peer(&self, p: &SitePeer) -> Result<()> {
        // 注意：UPDATE 不触碰 `reconcile_required`——该门只能由 repair 成功清除
        // （S7.4 review I2：重复/重试配对不得把 B′ 未修复状态打开）。
        let row = site_peer_row(p);
        return self
            .object_local_write("site-pair", move |txn| {
                Box::pin(async move { txn.site_pair_upsert(&row).await })
            })
            .await;
    }

    /// S7.4 B′：替换站点后、repair 完成前拒绝向该对端服务 sync。
    pub async fn set_site_reconcile_required(&self, peer_id: &str, required: bool) -> Result<()> {
        let peer = peer_id.to_owned();
        return self
            .object_local_write("site-reconcile", move |txn| {
                Box::pin(async move { txn.site_set_reconcile_required(&peer, required).await })
            })
            .await;
    }

    pub async fn insert_site(&self, p: &SitePeer) -> Result<()> {
        let row = site_peer_row(p);
        return self
            .object_local_write("site-pair", move |txn| {
                Box::pin(async move { txn.site_insert(&row, now()).await })
            })
            .await;
    }

    /// 删除 peer：同事务丢弃其 site 出站行与游标（对象复制不再尝试该目的地）。
    /// 删除 peer：同事务丢弃其 site 出站行与游标；返回被删掉的**非空版本**集合
    /// （调用方据此结算版本终态）。`DELETE ... RETURNING` 作为事务第一条写语句，
    /// 与解配并发入队的行之间不存在 TOCTOU。
    pub async fn delete_site(
        &self,
        peer_id: &str,
    ) -> Result<Option<Vec<(String, String, String)>>> {
        let peer = peer_id.to_owned();
        let out = self
            .object_local_write("site-unpair", move |txn| {
                Box::pin(async move {
                    let Some(remote) = txn.site_remote_id(&peer).await? else {
                        return Ok(None);
                    };
                    let affected = txn.repl_drop_site(&peer).await?;
                    txn.site_cursor_delete(&peer).await?;
                    txn.site_delete(&peer).await?;
                    Ok(Some((remote, affected)))
                })
            })
            .await?;
        let Some((remote, affected)) = out else {
            return Ok(None);
        };
        // shortcut: cursor/dead files are node-local and cannot join the root
        // CAS; cleanup is best-effort after the peer row is gone.
        if let Some(object) = self.object_control()
            && let Ok(local) = object.control.local_store()
        {
            if let Ok(cursor) = local.cursor(peer_id) {
                let _ = cursor.delete();
            }
            if let Ok(dead) = local.dead(&remote) {
                let _ = dead.delete();
            }
        }
        return Ok(Some(affected));
    }

    pub async fn update_site_ping(
        &self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<()> {
        let peer = peer_id.to_owned();
        let error = error.map(str::to_owned);
        return self
            .object_local_write("site-ping", move |txn| {
                Box::pin(async move { txn.site_update_ping(&peer, ts, error.as_deref()).await })
            })
            .await;
    }

    /// 启动装配注入对象控制面（S7：每个 `Db` 只能挂一次）。
    pub fn attach_object_control(
        &self,
        object: std::sync::Arc<crate::control_backend::ObjectControl>,
    ) -> Result<()> {
        self.object.set(object).map_err(|_| {
            crate::db::DbError::Protocol("对象控制面后端被重复注入（每个 Db 只能挂一次）".into())
        })
    }

    /// H1: the object control store is the only backend.
    pub fn control_backend(&self) -> crate::config::ControlBackend {
        crate::config::ControlBackend::Objects
    }

    /// 对象后端句柄（未选对象后端 = None；测试/诊断用）。
    pub fn object_control(&self) -> Option<&std::sync::Arc<crate::control_backend::ObjectControl>> {
        self.object.get()
    }

    fn control(&self) -> &std::sync::Arc<crate::control_store_obj::ObjectControlStore> {
        &self
            .object
            .get()
            .expect("object control backend not attached")
            .control
    }

    /// Control-plane read handle (object root is attached before serving).
    pub fn control_store(&self) -> std::sync::Arc<dyn xl_store::control_store::ControlStore> {
        self.object
            .get()
            .expect("object control backend not attached")
            .control
            .clone()
    }

    /// Write dispatch: `ObjectControlStore::transaction` (the whole closure is
    /// retried on root CAS conflict), origin `Local`. Local-store writes must
    /// stay outside this closure because they take effect immediately.
    pub async fn control_write<T, F>(&self, actor: &str, mut f: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a> FnMut(
            &'a mut dyn ControlTxn,
        ) -> futures::future::BoxFuture<
            'a,
            std::result::Result<T, xl_store::control_store::ControlError>,
        >,
    {
        let object = self
            .object
            .get()
            .ok_or_else(|| crate::db::DbError::Protocol("对象控制面未装配".into()))?;
        object
            .control
            .transaction(crate::control_store_obj::WriteOrigin::local(actor), |txn| {
                let fut = f(txn);
                Box::pin(async move { fut.await.map_err(crate::control_backend::store_error) })
            })
            .await
            .map_err(|e| crate::db::DbError::Protocol(format!("对象控制面事务失败：{e}")))
    }

    /// 对象后端「本地域」写入（sync intent 等 `LocalKv`）：单次 begin + commit，
    /// **不做根 CAS 重试**——本地写已在调用时生效，重放会重复副作用。
    /// Object-mode local upload store (D4); fails closed without the object backend.
    fn object_uploads(&self) -> Result<crate::local_store::UploadStore> {
        self.object
            .get()
            .ok_or_else(|| crate::db::DbError::Protocol("对象控制面未装配".into()))?
            .control
            .local_store()
            .map_err(|e| crate::db::DbError::Protocol(format!("对象本地目录失败：{e}")))?
            .uploads()
            .map_err(crate::db::DbError::Io)
    }

    pub async fn object_local_write<T, F>(&self, actor: &str, f: F) -> Result<T>
    where
        T: Send,
        F: for<'a> FnOnce(
            &'a mut crate::control_store_obj::ObjectControlTxn,
        ) -> futures::future::BoxFuture<
            'a,
            std::result::Result<T, xl_store::control_store::ControlError>,
        >,
    {
        let Some(object) = self.object.get() else {
            return Err(crate::db::DbError::Protocol("对象控制面未装配".into()));
        };
        let mut txn = object
            .control
            .begin(crate::control_store_obj::WriteOrigin::local(actor))
            .await
            .map_err(|e| crate::db::DbError::Protocol(format!("对象控制面事务失败：{e}")))?;
        let out = f(&mut txn).await;
        match out {
            Ok(value) => {
                txn.commit()
                    .await
                    .map_err(|e| crate::db::DbError::Protocol(format!("对象控制面提交失败：{e}")))?;
                Ok(value)
            }
            Err(e) => Err(crate::db::DbError::Protocol(format!("对象控制面本地写失败：{e}"))),
        }
    }

    /// 自然键属主 → 本节点数字 id（console JSON 边界用）：`Admin` → 首个管理员；
    /// `User(name)` → `users.id`；缺失（含无管理员）→ 0。
    pub async fn owner_uid(&self, owner: &OwnerRef) -> Result<i64> {
        let store = self.control_store();
        let id = match owner {
            OwnerRef::Admin => store
                .admin_id()
                .await
                .map_err(crate::db::to_db)?,
            OwnerRef::User(name) => store
                .user(name)
                .await
                .map_err(crate::db::to_db)?
                .map(|u| u.uid),
        };
        Ok(id.unwrap_or(0))
    }

    /// 本地数字 id → 自然键属主（写门面用）：0 → `Admin`；用户存在 → `User`；
    /// 其余 → `RowNotFound`（uid 必须存在，不静默降级）。
    pub async fn owner_ref(&self, owner_id: i64) -> Result<OwnerRef> {
        if owner_id == 0 {
            return Ok(OwnerRef::Admin);
        }
        self.control_store()
            .user_by_id(owner_id)
            .await
            .map_err(crate::db::to_db)?
            .map(|u| OwnerRef::User(u.username))
            .ok_or(crate::db::DbError::RowNotFound)
    }

    pub async fn create_user(&self, username: &str, password: &str, is_admin: bool) -> Result<i64> {
        let hash = crate::db::hash_password(password);
        // 对象后端：用户行 + 派生账户/成员同一根 CAS，重复用户名经
        // `is_unique_violation` 的文本匹配映射为 400。
        let username = username.to_owned();
        return self
            .control_write("console", move |txn| {
                let username = username.clone();
                let hash = hash.clone();
                Box::pin(async move {
                    let id = txn
                        .user_create_default_status(&username, &hash, is_admin, now())
                        .await?;
                    if !is_admin && let Some(acct) = xl_s3::access::account_id_of(id) {
                        txn.account_insert_derived_if_absent(
                            &acct,
                            &username,
                            id,
                            now(),
                            now(),
                        )
                        .await?;
                        txn.account_member_insert_if_absent(&acct, id, "owner", now())
                            .await?;
                    }
                    Ok(id)
                })
            })
            .await;
    }

    pub async fn user_exists(&self, username: &str) -> Result<bool> {
        self.control_store()
            .user_exists(username)
            .await
            .map_err(crate::db::to_db)
    }

    pub async fn verify_user(&self, username: &str, password: &str) -> Result<Option<i64>> {
        // M4 切片 7：Argon2id 恒时校验（password-hash 的 verify_password 内部恒时）；
        // 旧 M1 格式（sha256 单轮）仍可验证——**登录成功即透明重写升级**（一次迁移）。
        // status='active'：被停用的账号登录直接按「用户名或密码错误」处理（不问不答）。
        // AC-1：账户停用同样按凭据错误处理（成员行存在但账户行缺失 → 停用，fail-closed）。
        let Some(r) = self
            .control_store()
            .login_user(username)
            .await
            .map_err(crate::db::to_db)?
        else {
            // 用户存在性 oracle 收口（verifier 切片 7 观察 A）：Argon2 让「未命中早退」
            // 与「命中校验」的时延差从 µs 放大到几十 ms、网络可测——未命中路径先对一个
            // 固定 dummy 串跑一次同参数 verify 再返回，两侧时延拉平。
            // 与真实校验共用同一并发闸（C1：同价成本、同一上限）。
            let password = password.to_owned();
            crate::db::argon2_offload(move || crate::db::dummy_verify(&password)).await?;
            return Ok(None);
        };
        let stored = r.password_hash;
        let id = r.uid;
        let legacy = crate::db::is_legacy_hash(&stored);

        // Argon2 移出 tokio worker；行查询已在闸外完成，许可在返回前释放（不跨后续 DB I/O）。
        let password_for_verify = password.to_owned();
        if !crate::db::argon2_offload(move || {
            crate::db::verify_password(&password_for_verify, &stored)
        })
        .await?
        {
            return Ok(None);
        }
        // Raft 模式下各站点盐不同，禁止本地升级（对象根 `mode` 即后端权威）。
        let object_raft = match self.object.get() {
            Some(object) => object
                .control
                .pin()
                .await
                .map(|p| p.root.meta.mode == xl_store::control_state::root::Mode::Raft)
                .unwrap_or(false),
            None => false,
        };
        if legacy && !object_raft {
            // 透明升级（失败只 warn：本次登录已成功，下次再来）。
            // Raft 模式：随机盐会让各站点写出的 hash 不同 → 禁止本地写，只验通过。
            // 哈希同样走阻塞池 + 并发闸（旧哈希账户登录频率低，但不应占 tokio worker）。
            let password_for_hash = password.to_owned();
            let upgraded = match argon2_offload(move || hash_password(&password_for_hash)).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, user = %username, "旧口令哈希升级计算失败");
                    return Ok(Some(id));
                }
            };
            let upgrade = async {
                let upgraded = upgraded.clone();
                self.control_write("login-upgrade", move |txn| {
                    let upgraded = upgraded.clone();
                    Box::pin(async move { txn.user_set_password(id, &upgraded).await.map(|_| ()) })
                })
                .await
            }
            .await;
            if let Err(e) = upgrade {
                tracing::warn!(error = %e, user = %username, "旧口令哈希透明升级失败");
            }
        }
        Ok(Some(id))
    }

    pub async fn create_access_key(&self, ak: &str, sk: &str, user_id: i64) -> Result<()> {
        let ak = ak.to_owned();
        let sk = sk.to_owned();
        return self
            .control_write("console", move |txn| {
                let ak = ak.clone();
                let sk = sk.clone();
                Box::pin(
                    async move { txn.access_key_create_legacy(&ak, &sk, user_id, now()).await },
                )
            })
            .await;
    }

    // SigV4 的 AK → SK 查询归 xl-s3/src/auth.rs（SigV4Auth）；这里不再保留第二份，防 M5 双改。

    pub async fn list_access_keys(&self, user_id: i64) -> Result<Vec<String>> {
        // M5 切片 5：控制台只列长时键（会话行由 STS 签发、海量短寿，不进「访问密钥」页）
        self.control_store()
            .access_keys_for_user(user_id)
            .await
            .map_err(crate::db::to_db)
    }

    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket(&self, name: &str, owner_id: Option<i64>) -> Result<()> {
        self.insert_bucket_set(name, owner_id, 0).await
    }

    /// M4 切片 8：带 set 的建桶行（单 set 缺省 0 走上面）。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket_set(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
    ) -> Result<()> {
        self.insert_bucket_full(name, owner_id, set, "async").await
    }

    /// M9 全列建桶行（INSERT OR IGNORE；已存在的权威行不重置）。`set_id` 是本地路由列。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket_full(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
        replication_mode: &str,
    ) -> Result<()> {
        let name = name.to_owned();
        let replication_mode = replication_mode.to_owned();
        let creation_date = now();
        return self
            .control_write("bucket-row", move |txn| {
                let name = name.clone();
                let replication_mode = replication_mode.clone();
                Box::pin(async move {
                    txn.bucket_insert_ignore(
                        &name,
                        owner_id,
                        set as i64,
                        &replication_mode,
                        creation_date,
                    )
                    .await
                })
            })
            .await;
    }

    /// M5 切片 0：建桶行**覆盖**语义（console CreateBucket 用）——删桶记账容忍失败留下的
    /// 陈旧行（旧 owner/旧 set）必须在重建时收敛，否则本节点按旧 set 路由、按旧 owner 判权，
    /// 与已收广播的 peer 静默分叉（verifier 阻断 1）。
    pub async fn upsert_bucket_owner_set(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
    ) -> Result<BucketRowOutcome> {
        self.upsert_bucket_owner_set_mode(name, owner_id, set, "async")
            .await
    }

    /// M9：同 [`Self::upsert_bucket_owner_set`]，并写 `replication_mode`。
    /// ON CONFLICT 全列重置（set 随重建收敛，M5 阻断 1）；creation_date 保持首建。
    /// **新建行**在 `BEGIN IMMEDIATE` 写锁内 count+insert——并发建桶只有一个能越过账户
    /// 桶数配额；已存在行是重建收敛（桶数不变），不做配额判定。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn upsert_bucket_owner_set_mode(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
        replication_mode: &str,
    ) -> Result<BucketRowOutcome> {
        let name = name.to_owned();
        let replication_mode = replication_mode.to_owned();
        let creation_date = now();
        return self
            .control_write("console", move |txn| {
                let name = name.clone();
                let replication_mode = replication_mode.clone();
                Box::pin(async move {
                    // 新建行才判配额；已存在行是重建收敛（桶数不变）。
                    if !txn.bucket_exists(&name).await?
                        && txn.bucket_quota_reached(owner_id).await?
                    {
                        return Ok(BucketRowOutcome::QuotaExceeded);
                    }
                    txn.bucket_upsert_owner_set_mode(
                        &name,
                        owner_id,
                        set as i64,
                        &replication_mode,
                        creation_date,
                    )
                    .await?;
                    Ok(BucketRowOutcome::Written)
                })
            })
            .await;
    }

    /// 桶的复制模式；行不存在 = "async"（读取路径与缺行旧桶同口径）。
    pub async fn bucket_replication_mode(&self, name: &str) -> Result<String> {
        Ok(self
            .control_store()
            .bucket(name)
            .await
            .map_err(crate::db::to_db)?
            .map(|v| v.replication_mode)
            .unwrap_or_else(|| "async".into()))
    }

    /// M9：sync 写门所需桶状态 `(replication_mode, deleting, versioning)`；
    /// 行不存在 = ("async", false, "Off")（与缺行旧桶同口径）。
    pub async fn bucket_sync_state(&self, name: &str) -> Result<(String, bool, String)> {
        Ok(self
            .bucket_sync_state_row(name)
            .await?
            .unwrap_or_else(|| ("async".into(), false, "Off".into())))
    }

    /// M9 S3：同 [`Self::bucket_sync_state`]，但**行不存在返回 `None`**——删桶恢复据此
    /// 区分「陈旧 intent」与「同名桶仍存在」，避免误删重建的新桶。
    pub async fn bucket_sync_state_row(
        &self,
        name: &str,
    ) -> Result<Option<(String, bool, String)>> {
        Ok(self
            .control_store()
            .bucket(name)
            .await
            .map_err(crate::db::to_db)?
            .map(|v| (v.replication_mode, v.deleting, v.versioning)))
    }

    /// M9 S3：删桶 quiesce 标志（S6 起改由 Raft `SetBucketDeleting` 命令承载）。
    pub async fn set_bucket_deleting(&self, name: &str, deleting: bool) -> Result<()> {
        let name = name.to_owned();
        return self
            .control_write("console", move |txn| {
                let name = name.clone();
                Box::pin(
                    async move { txn.bucket_set_deleting(&name, deleting).await.map(|_| ()) },
                )
            })
            .await;
    }

    /// 全量桶名 → 模式（console 列表一次查完，避免逐桶查询）。
    pub async fn bucket_replication_modes(
        &self,
    ) -> Result<std::collections::HashMap<String, String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(|v| (v.name, v.replication_mode))
            .collect())
    }

    /// P3-3：回填对账——把**权威**解析到的 set 覆盖到已有行（仅 set_id 列；owner/versioning
    /// 不动）。场景：桶删除后在别节点重建为另一 set，本节点陈旧行（INSERT OR IGNORE 不会更新）
    /// 会在下次启动被此处收敛；行不存在则插入（owner NULL，与历史桶回填同权）。
    /// M5 切片 0：权威值须来自 `resolve_bucket_set_authoritative`（数据面本地先行解析对本地
    /// 陈旧行会把旧值原样写回，收敛不成立——verifier 高 2）。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn upsert_bucket_set(&self, name: &str, set: u16) -> Result<()> {
        let name = name.to_owned();
        let creation_date = now();
        return self
            .control_write("bucket-row", move |txn| {
                let name = name.clone();
                Box::pin(async move {
                    txn.bucket_upsert_set(&name, set as i64, creation_date)
                        .await
                })
            })
            .await;
    }

    pub async fn list_bucket_rows(&self) -> Result<Vec<String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(|v| v.name)
            .collect())
    }

    pub async fn instant_get(&self, k: &str) -> Result<Option<String>> {
        self.control()
            .instant_get(k, now())
            .map_err(crate::db::to_db)
    }

    pub async fn instant_set(&self, k: &str, v: &str, ttl_seconds: u64) -> Result<()> {
        // ttl = 0 视为「立刻过期」，与 aetherupload 的 SETEX 语义一致
        let expires = now() + ttl_seconds as i64;
        self.control()
            .instant_set(k, v, expires)
            .map_err(crate::db::to_db)
    }

    pub async fn instant_del(&self, k: &str) -> Result<()> {
        self.control().instant_del(k).map_err(crate::db::to_db)
    }

    /// 按前缀清空秒传索引（`InstantIndex::delete_all` 的落点）。
    pub async fn instant_delete_prefix(&self, prefix: &str) -> Result<()> {
        self.control()
            .instant_delete_prefix(prefix)
            .map_err(crate::db::to_db)
    }

    /// 清过期秒传索引行（有界批；过期行不影响命中，但会无限增长）。
    pub async fn instant_purge_expired(&self, limit: i64) -> Result<u64> {
        self.control()
            .instant_purge_expired(now(), limit)
            .map_err(crate::db::to_db)
    }

    pub async fn record_upload(
        &self,
        saved_path: &str,
        bucket: &str,
        key: &str,
        group: &str,
        size: i64,
        etag: &str,
    ) -> Result<()> {
        let uploads = self.object_uploads()?;
        return uploads
            .put_if_absent(&crate::local_store::UploadRecord {
                saved_path: saved_path.to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                group_name: group.to_string(),
                size,
                etag: etag.to_string(),
                original_name: None,
                owner_uid: None,
                created_at: now(),
                expires_at: None,
            })
            .map(|_| ())
            .map_err(crate::db::DbError::Io);
    }

    /// 登记上传的 `original_name` 与归属（规格 §5.4）。
    ///
    /// 归属防挂名：非管理员只能认领**无归属**的记录（`owner_id IS NULL`）或自己的
    /// （原实现无条件覆盖 owner 的洞在此收口）。返回是否命中（0 行 = 记录不存在 → 404）。
    pub async fn register_upload(
        &self,
        saved_path: &str,
        original_name: &str,
        owner_id: i64,
        is_admin: bool,
    ) -> Result<bool> {
        let uploads = self.object_uploads()?;
        let Some(mut record) = uploads.get(saved_path).map_err(crate::db::DbError::Io)? else {
            return Ok(false);
        };
        if !is_admin && record.owner_uid.is_some_and(|owner| owner != owner_id) {
            return Ok(false);
        }
        record.original_name = Some(original_name.to_string());
        record.owner_uid = Some(owner_id);
        uploads.put(&record).map_err(crate::db::DbError::Io)?;
        return Ok(true);
    }

    /// 记录对应的桶（登记前判权用；记录不存在 → None）。
    pub async fn upload_bucket(&self, saved_path: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .upload(saved_path)
            .await
            .map_err(crate::db::to_db)?
            .map(|r| r.bucket))
    }

    /// 桶版本控制状态（M4 切片 3；aether 上传面按此传 versioned；无行 → "Off"）。
    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<String> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::db::to_db)?
            .map(|v| v.versioning)
            .unwrap_or_else(|| "Off".to_string()))
    }

    /// M8 缺口 C：桶默认加密（"AES256"/"aws:kms"；None = 未配置）。
    pub async fn get_bucket_default_encryption(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::db::to_db)?
            .and_then(|v| v.default_encryption))
    }

    /// M8 缺口 C：写桶默认加密（桶不存在 → 0 行）。
    pub async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool> {
        let bucket = bucket.to_owned();
        let algorithm = algorithm.to_owned();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                let algorithm = algorithm.clone();
                Box::pin(
                    async move { txn.bucket_set_default_encryption(&bucket, &algorithm).await },
                )
            })
            .await;
    }

    /// M8 缺口 C：清桶默认加密（幂等）。
    pub async fn clear_bucket_default_encryption(&self, bucket: &str) -> Result<bool> {
        let bucket = bucket.to_owned();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                Box::pin(async move { txn.bucket_clear_default_encryption(&bucket).await })
            })
            .await;
    }

    /// 全部桶默认加密记录（桶名 + 库内存储值），按桶名排序；CMK 引用守卫用。
    pub async fn list_bucket_default_encryptions(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .filter_map(|v| v.default_encryption.map(|e| (v.name, e)))
            .collect())
    }

    /// 某用户拥有的桶（控制台桶列表能力位计算用）。
    pub async fn owned_buckets(&self, uid: i64) -> Result<Vec<String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .filter(|v| v.owner_uid == Some(uid))
            .map(|v| v.name)
            .collect())
    }

    pub async fn list_uploads(&self, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = self
            .control_store()
            .uploads(limit)
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows.into_iter().map(upload_record).collect())
    }

    // ---- 子账户与权限（M-权限体系）----

    /// 取用户（`/me`、admin 判定、停用拦截用）：(username, is_admin, is_active)。
    pub async fn get_user(&self, uid: i64) -> Result<Option<(String, bool, bool)>> {
        Ok(self
            .control_store()
            .user_by_id(uid)
            .await
            .map_err(crate::db::to_db)?
            .map(|u| (u.username, u.is_admin, u.status == "active")))
    }

    /// 启停用用户。返回是否生效（false = 用户不存在，或停用会留下 0 个在册管理员）。
    ///
    /// **防呆是原子条件**（评审 P2：先读 count 再 UPDATE 的 check-then-act 在并发互停
    /// 两个管理员时会双双放行 → 0 在册管理员锁死）。停用走单条条件 UPDATE：
    /// 非管理员恒可停；管理员仅在「当前在册管理员 > 1」时可停——子查询与写入同语句，
    /// SQLite 写锁串行化下天然防 TOCTOU。启用无锁死风险，走普通 UPDATE。
    pub async fn set_user_status(&self, uid: i64, active: bool) -> Result<bool> {
        return self
            .control_write("console", move |txn| {
                Box::pin(async move { txn.user_set_status(uid, active).await })
            })
            .await;
    }

    /// 在册（active）管理员数：保护「不能停用最后一个可用管理员」。
    pub async fn count_active_admins(&self) -> Result<i64> {
        Ok(self
            .control_store()
            .admin_counts()
            .await
            .map_err(crate::db::to_db)?
            .0)
    }

    /// 全部用户 + 各自权限行（子账户管理页；数量级 = 人手，两步查询足够）。
    pub async fn list_users(&self) -> Result<Vec<UserRow>> {
        let users = self
            .control_store()
            .users()
            .await
            .map_err(crate::db::to_db)?;
        let mut out = Vec::with_capacity(users.len());
        for u in users {
            out.push(UserRow {
                id: u.uid,
                username: u.username,
                is_admin: u.is_admin,
                status: u.status,
                created_at: u.created_at,
                permissions: self.get_permissions(u.uid).await?,
            });
        }
        Ok(out)
    }

    /// 管理员数量（保护「最后一个管理员」不被删）。
    pub async fn count_admins(&self) -> Result<i64> {
        Ok(self
            .control_store()
            .admin_counts()
            .await
            .map_err(crate::db::to_db)?
            .1)
    }

    /// 改口令。返回是否有该用户。
    pub async fn set_password(&self, uid: i64, password: &str) -> Result<bool> {
        let hash = crate::db::hash_password(password);
        return self
            .control_write("console", move |txn| {
                let hash = hash.clone();
                Box::pin(async move { txn.user_set_password(uid, &hash).await })
            })
            .await;
    }

    /// 删除用户（一个事务内处理全部外键：sqlx 默认 FK ON，裸删 users 会报错）：
    /// 级联删 access_keys 与 permissions；buckets/uploads 的 owner 置 NULL（记录保留作审计）。
    /// 返回是否删除（false = 用户不存在，或会留下 0 个在册管理员）。
    ///
    /// **防呆原子化**（评审 P2，同 set_user_status）：事务第一条是无副作用的守卫写
    /// （`UPDATE users SET id=id WHERE …`），既取写锁串行化并发删除/停用，又在同一语句里
    /// 完成判定——通过才继续级联；不通过直接返回 false，一个字节都不改。
    pub async fn delete_user(&self, uid: i64) -> Result<bool> {
        let deleted = self
            .control_write("console", move |txn| {
                Box::pin(async move {
                    if !txn.user_delete_guard(uid).await? {
                        return Ok(false);
                    }
                    txn.user_delete_access_keys_permissions(uid).await?;
                    txn.bucket_clear_owner(uid).await?;
                    txn.user_delete(uid).await?;
                    Ok(true)
                })
            })
            .await?;
        // 本地上传属主是节点本地域，不能进根 CAS（D3）：CAS 成功后清，失败下次对账。
        if deleted && let Ok(uploads) = self.object_uploads() {
            for mut record in uploads.all().unwrap_or_default() {
                if record.owner_uid == Some(uid) {
                    record.owner_uid = None;
                    if let Err(e) = uploads.put(&record) {
                        tracing::warn!(error = %e, "清本地上传属主失败（下次启动对账）");
                    }
                }
            }
        }
        return Ok(deleted);
    }

    // ---- AC-0：多租户账户（standalone 直写；Raft 经 ControlCommand apply）----

    /// 账户表是否已规范化（v2 回填/level-3 canonicalize 完成）。
    pub async fn accounts_canonical(&self) -> Result<bool> {
        Ok(self.schema_meta(META_ACCOUNTS_CANONICAL).await?.is_some())
    }

    /// 缺少账户成员行的非管理员用户（legacy 派生账户路径；worker 的修复对象）。
    pub async fn legacy_users_missing_accounts(&self) -> Result<Vec<(i64, String)>> {
        self.control_store()
            .legacy_users_missing_accounts()
            .await
            .map_err(crate::db::to_db)
    }

    /// standalone 幂等规范化（serve worker 与启动路径共用）：
    /// 1. 全局管理员移出成员表、删除其无成员派生账户（同 backfill v2）；
    /// 2. 为缺失成员行的非管理员补 uid 派生账户 + owner 成员（ARN 不漂移）；
    /// 3. 无 `accounts_canonical_v1` 时写 marker（仅本事务成功后）。
    ///
    /// 单事务：任一用户失败全部回滚，重启/重跑安全；返回本轮新增行数。
    /// Raft 模式不得调用（账户表经快照复制，由 leader 用既有 AccountCreate/
    /// AccountMemberAttach 命令修复）。
    pub async fn canonicalize_legacy_accounts_once(&self) -> Result<usize> {
        // 对象后端：同样三条修复在同一根 CAS 内；marker 省略（迁移已搬，
        // 函数本身幂等）。
        // shortcut: 对象模式不写 accounts_canonical marker，升级点=需要对象侧
        // accounts_canonical() 对未迁移库也为真时，用具体 txn 的 set_marker。
        return self
            .control_write("accounts-canonical", |txn| {
                Box::pin(async move {
                    txn.account_members_delete_admins().await?;
                    txn.accounts_delete_admin_derived().await?;
                    let users = txn.legacy_users_missing_accounts().await?;
                    let ts = now();
                    let mut changed = 0usize;
                    for (uid, username) in users {
                        let Some(acct) = xl_s3::access::account_id_of(uid) else {
                            continue;
                        };
                        changed += txn
                            .account_insert_derived_if_absent(&acct, &username, uid, ts, ts)
                            .await? as usize;
                        changed += txn
                            .account_member_insert_if_absent(&acct, uid, "owner", ts)
                            .await? as usize;
                    }
                    Ok(changed)
                })
            })
            .await;
    }

    /// standalone 设置硬配额（Raft 下无复制命令，路由拒绝；见 accounts_api）。
    /// 返回 false = 账户不存在。
    pub async fn set_account_hard_limits(
        &self,
        id: &str,
        hard_byte_quota: Option<i64>,
        object_quota: Option<i64>,
    ) -> Result<bool> {
        let id = id.to_owned();
        return self
            .control_write("console", move |txn| {
                let id = id.clone();
                Box::pin(async move {
                    txn.account_set_hard_limits(&id, hard_byte_quota, object_quota, now())
                        .await
                })
            })
            .await;
    }

    /// 用户当前账户：成员行优先，无行回落 uid 派生账户（legacy 兼容路径）。
    pub async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .account_member_account(uid)
            .await
            .map_err(crate::db::to_db)?
            .or_else(|| xl_s3::access::account_id_of(uid)))
    }

    /// 账户状态是否允许该用户继续使用（无成员行 = 派生账户语义 = 有效）。
    /// 成员行存在但账户行缺失 → false（fail-closed）。
    pub async fn account_active(&self, uid: i64) -> Result<bool> {
        self.control_store()
            .account_active(uid)
            .await
            .map_err(crate::db::to_db)
    }

    fn account_row_from_view(a: xl_store::control_store::account::AccountView) -> AccountRow {
        AccountRow {
            id: a.id,
            name: a.name,
            status: a.status,
            admin_uid: a.admin_uid,
            bucket_quota: a.bucket_quota,
            soft_byte_quota: a.soft_byte_quota,
            hard_byte_quota: a.hard_byte_quota,
            object_quota: a.object_quota,
            created_at: a.created_at,
            updated_at: a.updated_at,
            member_count: a.member_count,
            bucket_count: a.bucket_count,
        }
    }

    /// 账户名下桶名（软配额用量端点逐桶列举；口径见适配器 `account_buckets_from`）。
    pub async fn account_bucket_names(&self, account_id: &str) -> Result<Vec<String>> {
        self.control_store()
            .account_bucket_names(account_id)
            .await
            .map_err(crate::db::to_db)
    }

    /// 全部账户（管理员视角）或某用户的账户（账户成员视角）。
    pub async fn list_accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountRow>> {
        Ok(self
            .control_store()
            .accounts(member_uid)
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(Self::account_row_from_view)
            .collect())
    }

    pub async fn get_account(&self, id: &str) -> Result<Option<AccountRow>> {
        Ok(self
            .control_store()
            .account(id)
            .await
            .map_err(crate::db::to_db)?
            .map(Self::account_row_from_view))
    }

    /// 新建账户。id 为 12 位随机数（与派生账户空间重叠概率可忽略，冲突重试）。
    pub async fn create_account(&self, name: &str, bucket_quota: Option<i64>) -> Result<String> {
        let ts = now();
        let name = name.to_owned();
        for _ in 0..8 {
            let id = crate::accounts_support::new_account_id();
            let insert_id = id.clone();
            let name = name.clone();
            let inserted = self
                .control_write("console", move |txn| {
                    let id = insert_id.clone();
                    let name = name.clone();
                    Box::pin(async move {
                        txn.account_insert_if_absent(&id, &name, bucket_quota, ts, ts)
                            .await
                    })
                })
                .await?;
            if inserted {
                return Ok(id);
            }
        }
        return Err(crate::db::DbError::Protocol(
            "account id generation collision (retry)".into(),
        ));
    }

    pub async fn rename_account(&self, id: &str, name: &str) -> Result<bool> {
        let id = id.to_owned();
        let name = name.to_owned();
        return self
            .control_write("console", move |txn| {
                let id = id.clone();
                let name = name.clone();
                Box::pin(async move { txn.account_set_name(&id, &name, now()).await })
            })
            .await;
    }

    /// 启停账户。停用是原子条件更新：账户里还有在册全局管理员时拒绝
    /// （否则该管理员会随账户一起被锁死，且可能锁死最后一个管理员）。
    pub async fn set_account_status(&self, id: &str, status: &str) -> Result<bool> {
        let id = id.to_owned();
        let status = status.to_owned();
        return self
            .control_write("console", move |txn| {
                let id = id.clone();
                let status = status.clone();
                Box::pin(async move { txn.account_set_status(&id, &status, now()).await })
            })
            .await;
    }

    pub async fn set_account_quota(&self, id: &str, quota: Option<i64>) -> Result<bool> {
        let id = id.to_owned();
        return self
            .control_write("console", move |txn| {
                let id = id.clone();
                Box::pin(async move { txn.account_set_bucket_quota(&id, quota, now()).await })
            })
            .await;
    }

    /// 删除账户：要求无成员且无桶（LEGACY 系统账户永不可删）。
    /// 返回 Ok(true)；账户不存在 / 非空不删（由 handler 区分）。
    pub async fn delete_account(&self, id: &str) -> Result<bool> {
        if id == xl_s3::access::LEGACY_ACCOUNT {
            return Ok(false);
        }
        let id = id.to_owned();
        return self
            .control_write("console", move |txn| {
                let id = id.clone();
                Box::pin(async move {
                    let members = txn.account_member_count(&id).await?;
                    let buckets = txn.account_bucket_count(&id).await?;
                    if members != 0 || buckets != 0 {
                        return Ok(false);
                    }
                    txn.account_delete(&id).await
                })
            })
            .await;
    }

    pub async fn list_account_members(&self, id: &str) -> Result<Vec<AccountMemberRow>> {
        Ok(self
            .control_store()
            .account_members(id)
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(|m| AccountMemberRow {
                uid: m.uid,
                username: m.username,
                role: m.role,
                is_admin: m.is_admin,
                status: m.status,
                created_at: m.created_at,
            })
            .collect())
    }

    /// 某用户在该账户里的角色（不在账户 → None）。
    pub async fn member_role(&self, account_id: &str, uid: i64) -> Result<Option<String>> {
        self.control_store()
            .member_role(account_id, uid)
            .await
            .map_err(crate::db::to_db)
    }

    /// 把用户挂到账户（已有成员行则迁入并改角色）。账户/用户不存在 → false；
    /// 全局管理员不是任何账户的成员（AC-2 不变量），恒拒绝 → false。
    /// 账户 admin_uid 为空且角色为 admin 时顺带补上。
    pub async fn attach_account_member(
        &self,
        account_id: &str,
        uid: i64,
        role: &str,
    ) -> Result<bool> {
        let account_id = account_id.to_owned();
        let role = role.to_owned();
        return self
            .control_write("console", move |txn| {
                let account_id = account_id.clone();
                let role = role.clone();
                Box::pin(async move {
                    if !txn.user_non_admin_exists(uid).await? {
                        return Ok(false);
                    }
                    txn.account_member_upsert(&account_id, uid, &role, now())
                        .await?;
                    if role == "admin" {
                        txn.account_set_admin_if_unset(&account_id, uid).await?;
                    }
                    Ok(true)
                })
            })
            .await;
    }

    /// 在账户内建用户（is_admin 恒 0；角色由调用方限定 member）。
    /// 用户名冲突 → 数据库 unique violation（handler 映射 400）。
    pub async fn create_user_in_account(
        &self,
        account_id: &str,
        username: &str,
        password: &str,
        role: &str,
    ) -> Result<i64> {
        let ts = now();
        let account_id = account_id.to_owned();
        let username = username.to_owned();
        let role = role.to_owned();
        let hash = crate::db::hash_password(password);
        return self
            .control_write("console", move |txn| {
                let account_id = account_id.clone();
                let username = username.clone();
                let role = role.clone();
                let hash = hash.clone();
                Box::pin(async move {
                    let uid = txn
                        .user_create_default_status(&username, &hash, false, ts)
                        .await?;
                    txn.account_member_insert(&account_id, uid, &role, ts)
                        .await?;
                    Ok(uid)
                })
            })
            .await;
    }

    /// 某用户的权限行（全量）。
    pub async fn get_permissions(&self, uid: i64) -> Result<Vec<PermissionRow>> {
        Ok(self
            .control_store()
            .permissions_for_user(uid)
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(|p| PermissionRow {
                bucket: p.bucket,
                read: p.read,
                write: p.write,
                delete: p.delete,
            })
            .collect())
    }

    /// 全量替换某用户的权限行（管理员的 PUT 语义）。全 0 行跳过不写（无行 = 无权限，等价且省行）。
    pub async fn set_permissions(&self, uid: i64, rows: &[PermissionRow]) -> Result<()> {
        let rows: Vec<IamPermissionRow> = rows
            .iter()
            .filter(|p| p.read || p.write || p.delete)
            .map(|p| IamPermissionRow {
                bucket: p.bucket.clone(),
                read: p.read,
                write: p.write,
                delete: p.delete,
            })
            .collect();
        return self
            .control_write("iam", move |txn| {
                let rows = rows.clone();
                Box::pin(async move { txn.permissions_replace(uid, &rows).await })
            })
            .await;
    }

    /// 上传归属登记：仅当该记录尚无归属时写入（`AND owner_id IS NULL`）。
    /// 返回是否写入（已归属/记录不存在 → false，调用方 best-effort 忽略）。
    pub async fn set_upload_owner(&self, saved_path: &str, uid: i64) -> Result<bool> {
        let uploads = self.object_uploads()?;
        let Some(mut record) = uploads.get(saved_path).map_err(crate::db::DbError::Io)? else {
            return Ok(false);
        };
        if record.owner_uid.is_some() {
            return Ok(false);
        }
        record.owner_uid = Some(uid);
        uploads.put(&record).map_err(crate::db::DbError::Io)?;
        return Ok(true);
    }

    /// 某用户自己的上传记录（子账户视野）。
    pub async fn list_uploads_for(&self, uid: i64, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = self
            .control_store()
            .uploads_for(uid, limit)
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows.into_iter().map(upload_record).collect())
    }
}

/// 进程级 Argon2 并发闸（C1 DoS）：单次校验约 19 MiB，最多 8 路并行。
/// 校验一律走阻塞池，未认证登录洪水不再占满 tokio worker。
static ARGON2_PERMITS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(8));

/// 在阻塞池执行一次 Argon2 工作（校验/哑校验），并发受 [`ARGON2_PERMITS`] 限制。
/// JoinError 转成 DB 错误上抛（登录 handler 记日志后回 500，不 panic）。
async fn argon2_offload<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T> {
    let _permit = ARGON2_PERMITS
        .acquire()
        .await
        .map_err(|e| crate::db::DbError::Protocol(format!("Argon2 并发闸不可用: {e}")))?;
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| crate::db::DbError::Protocol(format!("Argon2 阻塞任务失败: {e}")))
}

/// 口令哈希（M4 切片 7 起）：Argon2id（salt 随机、串内自描述 `$argon2id$...`）。
pub fn hash_password(password: &str) -> String {
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 哈希（默认参数）不会失败")
        .to_string()
}

/// M1 旧格式（`sha256("oss-m1-salt:"+pw)` hex）——仅供 `verify_password` 的遗留分支使用。
fn legacy_hash(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}

/// 旧格式判定：非 `$argon2` 前缀的 64 位 hex（M1 单轮 sha256）。
pub fn is_legacy_hash(hash: &str) -> bool {
    !hash.starts_with("$argon2") && hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// 未命中路径的时延拉平（观察 A）：与真实校验同参数的 dummy verify。
/// 固定 dummy 串（公钥性质，无秘密）：仅用于消耗与真实路径同量的 CPU/内存。
/// 参数（m=19456,t=2,p=1）= `Argon2::default()`，与真实哈希同强度——测试钉住可解析性。
pub(crate) const DUMMY_ARGON2: &str = "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

pub fn dummy_verify(password: &str) {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    let parsed = PasswordHash::new(DUMMY_ARGON2)
        .expect("DUMMY_ARGON2 必须是合法 PHC 串（测试钉住；解析失败会让 oracle 收口失效）");
    let _ = argon2::Argon2::default().verify_password(password.as_bytes(), &parsed);
}

/// 恒时校验（Argon2 走 password-hash 的 verify；旧格式走定长 hex 比较）。
pub fn verify_password(password: &str, stored: &str) -> bool {
    if stored.starts_with("$argon2") {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        match PasswordHash::new(stored) {
            Ok(parsed) => argon2::Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        }
    } else {
        let calc = legacy_hash(password);
        // 定长比较（两者恒 64 字符）
        calc.as_bytes()
            .iter()
            .zip(stored.as_bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
            && stored.len() == calc.len()
    }
}

fn site_peer(r: SitePeerRow) -> SitePeer {
    SitePeer {
        peer_id: r.peer_id,
        remote_site_id: r.remote_site_id,
        name: r.name,
        endpoint: r.endpoint,
        region: r.region,
        priority: r.priority,
        s3_access_key: r.s3_access_key,
        s3_secret_key: r.s3_secret_key,
        peer_site_token: r.peer_site_token,
        created_at: r.created_at,
        last_seen: r.last_seen,
        last_error: r.last_error,
        raft_node_id: r.raft_node_id,
        reconcile_required: r.reconcile_required,
    }
}

pub(crate) fn site_peer_row(p: &SitePeer) -> SitePeerRow {
    SitePeerRow {
        peer_id: p.peer_id.clone(),
        remote_site_id: p.remote_site_id.clone(),
        name: p.name.clone(),
        endpoint: p.endpoint.clone(),
        region: p.region.clone(),
        priority: p.priority,
        s3_access_key: p.s3_access_key.clone(),
        s3_secret_key: p.s3_secret_key.clone(),
        peer_site_token: p.peer_site_token.clone(),
        created_at: p.created_at,
        last_seen: p.last_seen,
        last_error: p.last_error.clone(),
        raft_node_id: p.raft_node_id,
        reconcile_required: p.reconcile_required,
    }
}

// ——— M4 切片 6：生命周期规则与作业（jobs 表进度） ———

/// 生命周期规则行。M8 S0 扩为全字段（S4b 只做映射，不再改 db.rs）；
/// `expire_days` 为 M4 旧列（无 Expiration 的新规则写哨兵 -1），`expiration_days` 为新语义。
/// 类型收敛到 ControlStore 缝，行为字段不变。
pub use xl_store::control_store::LifecycleRuleEntry as LifecycleRuleRow;

/// CMK 删除/扫描的桶默认加密引用源（xl-store 不依赖 SQL，经 trait 注入）。
/// 存值形态 `aws:kms:<名或 ARN>`（S3/控制台写入口径）——按规范名归一后比对。
#[async_trait::async_trait]
impl xl_store::store::CmkConfigRefs for Db {
    async fn cmk_config_refs(
        &self,
        cmk: &str,
    ) -> xl_store::error::Result<Vec<xl_store::store::CmkBucketRef>> {
        let rows = self.list_bucket_default_encryptions().await.map_err(|e| {
            xl_store::error::Error::Unavailable(format!("读取桶默认加密配置失败: {e}"))
        })?;
        Ok(rows
            .into_iter()
            .filter_map(|(bucket, setting)| {
                let raw = setting.strip_prefix("aws:kms:")?;
                (xl_store::kms::parse_cmk_id(raw).ok() == Some(cmk))
                    .then_some(xl_store::store::CmkBucketRef { bucket, setting })
            })
            .collect())
    }
}

impl Db {
    /// 某桶的全部规则（控制台 GET 用）。
    pub async fn list_lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleRow>> {
        self.control_store()
            .lifecycle_rules(bucket)
            .await
            .map_err(crate::db::to_db)
    }

    /// 全量替换某桶规则（PUT 语义：先删后插，一个事务）。`now` 由调用方给（可测）。
    pub async fn set_lifecycle_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleRow],
        now: i64,
    ) -> Result<()> {
        let mut rows: Vec<LifecycleRuleRow> = rules.to_vec();
        for r in &mut rows {
            r.updated_at = now;
        }
        let bucket = bucket.to_owned();
        return self
            .control_write("lifecycle", move |txn| {
                let bucket = bucket.clone();
                let rows = rows.clone();
                Box::pin(async move { txn.replace_lifecycle_rules(&bucket, &rows).await })
            })
            .await;
    }

    /// 4.10：读取某规则某阶段的扫描游标（None = 从头扫）。
    pub async fn lifecycle_cursor_get(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<String>> {
        self.control_store()
            .lifecycle_cursor(bucket, rule_id, stage)
            .await
            .map_err(crate::db::to_db)
    }

    /// 4.10：持久化/清除扫描游标（`None` → 删除行，表示本阶段已扫完，下轮从头）。
    /// M8-1：游标写入同样受 fencing 保护——行已被更高 term 接管时旧 leader 的迟到游标
    /// 写入是 no-op（`fence_term <= token.term` 才接受并重标 token）。
    pub async fn lifecycle_cursor_set(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let rule_id = rule_id.to_owned();
        let stage = stage.to_owned();
        let cursor = cursor.map(str::to_owned);
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("lifecycle", move |txn| {
                let bucket = bucket.clone();
                let rule_id = rule_id.clone();
                let stage = stage.clone();
                let row = cursor.as_ref().filter(|c| !c.is_empty()).map(|c| {
                    xl_store::control_store::lifecycle::LifecycleCursorRow {
                        bucket: bucket.clone(),
                        rule_id: rule_id.clone(),
                        stage: stage.clone(),
                        cursor: c.clone(),
                        updated_at: now,
                        fence_term: term,
                        fence_leader: leader,
                    }
                });
                Box::pin(async move {
                    match row {
                        Some(row) => txn.lifecycle_cursor_upsert(&row).await.map(|_| ()),
                        None => {
                            txn.lifecycle_cursor_delete(&bucket, &rule_id, &stage, term)
                                .await
                        }
                    }
                })
            })
            .await;
    }

    /// 全部启用规则（worker 扫描用）。
    pub async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleRow>> {
        self.control_store()
            .enabled_lifecycle_rules()
            .await
            .map_err(crate::db::to_db)
    }

    /// 开一条作业（worker 每次执行一条）。
    pub async fn job_start(&self, kind: &str, bucket: &str, prefix: &str, now: i64) -> Result<i64> {
        let kind = kind.to_owned();
        let bucket = bucket.to_owned();
        let prefix = prefix.to_owned();
        return self
            .control_write("jobs", move |txn| {
                let kind = kind.clone();
                let bucket = bucket.clone();
                let prefix = prefix.clone();
                Box::pin(async move { txn.job_start(&kind, &bucket, &prefix, now).await })
            })
            .await;
    }

    /// 收尾一条作业（done/error + 计数 + 可选错误）。`error=None` 保留已有错误
    /// （与复制路径一致：错误一旦落库不可被收尾静默清空）。
    #[allow(clippy::too_many_arguments)]
    pub async fn job_finish(
        &self,
        id: i64,
        state: &str,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<&str>,
        now: i64,
    ) -> Result<()> {
        let state = state.to_owned();
        let error = error.map(str::to_owned);
        return self
            .control_write("jobs", move |txn| {
                let state = state.clone();
                let error = error.clone();
                Box::pin(async move {
                    txn.job_finish(id, &state, scanned, deleted, skipped, error.as_deref(), now)
                        .await
                })
            })
            .await;
    }

    /// 最近作业（控制台 GET /jobs；最新在前）。列投影与 `job_get` 同值。
    pub async fn list_jobs(&self, limit: i64) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .recent_jobs(limit)
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows
            .iter()
            .map(|j| {
                serde_json::json!({
                    "id": j.id,
                    "kind": j.kind,
                    "bucket": j.bucket,
                    "prefix": j.prefix,
                    "state": j.state,
                    "scanned": j.scanned,
                    "deleted": j.deleted,
                    "skipped": j.skipped,
                    "error": j.error,
                    "priority": j.priority,
                    "total": j.total,
                    "succeeded": j.succeeded,
                    "failed": j.failed,
                    "requested_by": j.requested_by,
                    "report_bucket": j.report_bucket,
                    "report_prefix": j.report_prefix,
                    "parent_job_id": j.parent_job_id,
                    "started_at": j.started_at,
                    "finished_at": j.finished_at,
                    "created_at": j.created_at,
                    "updated_at": j.updated_at,
                })
            })
            .collect())
    }

    // ——— M8 S0：作业引擎（jobs/job_tasks） ———

    /// 建作业 + 任务（同一事务）。任务 `task_no` 从 1 起；作业初始 `queued`。
    pub async fn create_job(&self, spec: &JobSpec, now: i64) -> Result<i64> {
        let insert = job_insert(spec, now);
        let tasks = new_tasks(spec, now);
        return self
            .control_write("jobs", move |txn| {
                let insert = insert.clone();
                let tasks = tasks.clone();
                Box::pin(async move { txn.job_create(&insert, &tasks).await })
            })
            .await;
    }

    /// 建带 dedupe 的作业：任务 `INSERT OR IGNORE`，一条都没插入（全被去重）→
    /// 整个事务回滚并返回 None（C7：不产生空作业）。
    pub async fn create_job_dedup(&self, spec: &JobSpec, now: i64) -> Result<Option<i64>> {
        let insert = job_insert(spec, now);
        let tasks = new_tasks(spec, now);
        return self
            .control_write("jobs", move |txn| {
                let insert = insert.clone();
                let tasks = tasks.clone();
                Box::pin(async move { txn.job_create_dedup(&insert, &tasks).await })
            })
            .await;
    }

    /// M8 S4b：Restore 受理（单事务）——建 kind=restore 单任务作业 + upsert `tier_restores`
    /// `state='pending', expires_at=0`。返回 `(job_id, 是否新建任务)`。
    /// M8-1：`fence` 是入队者 token。请求路径（API/用户）用 `UNFENCED`，行上的 fence
    /// 只升不降（`MAX`），避免新请求把已被高 term 接管的行降回 0；执行者的状态转换
    /// 仍按 `fence_term <= token.term` 门控。
    pub async fn create_restore_job(
        &self,
        spec: &JobSpec,
        bucket: &str,
        key: &str,
        version_id: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<(i64, bool)> {
        let insert = job_insert(spec, now);
        let tasks = new_tasks(spec, now);
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.to_owned();
        let fence_term = fence.term_i64();
        let fence_leader = fence.leader_i64();
        return self
            .control_write("jobs", move |txn| {
                let insert = insert.clone();
                let tasks = tasks.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                let version_id = version_id.clone();
                Box::pin(async move {
                    let (id, inserted) = txn.job_create_restore(&insert, &tasks).await?;
                    txn.tier_restore_upsert(&TierRestoreRow {
                        bucket,
                        key,
                        version_id,
                        expires_at: 0,
                        job_id: Some(id),
                        state: "pending".into(),
                        created_at: now,
                        fence_term,
                        fence_leader,
                    })
                    .await?;
                    Ok((id, inserted))
                })
            })
            .await;
    }

    /// 某状态的 Restore 行（reaper/对账）。
    pub async fn restore_rows(&self, state: &str) -> Result<Vec<RestoreRow>> {
        Ok(self
            .control_store()
            .tier_restores(state)
            .await
            .map_err(crate::db::to_db)?
            .into_iter()
            .map(restore_row_of)
            .collect())
    }

    pub async fn restore_row(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<RestoreRow>> {
        Ok(self
            .control_store()
            .tier_restore(bucket, key, version_id)
            .await
            .map_err(crate::db::to_db)?
            .map(restore_row_of))
    }

    /// M8-1：带 fencing token 的恢复状态转换。行已被更高 term 接管 → no-op。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::set_restore_state_fenced`]）。
    pub async fn set_restore_state(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
    ) -> Result<()> {
        self.set_restore_state_fenced(
            bucket,
            key,
            version_id,
            state,
            expires_at,
            FencingEpoch::UNFENCED,
        )
        .await
    }

    /// M8-1：行已被更高 term 接管（`fence_term > token.term`）→ no-op。
    pub async fn set_restore_state_fenced(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.to_owned();
        let state = state.to_owned();
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("tier-restore", move |txn| {
                let bucket = bucket.clone();
                let key = key.clone();
                let version_id = version_id.clone();
                let state = state.clone();
                Box::pin(async move {
                    txn.tier_restore_set_state(
                        &bucket,
                        &key,
                        &version_id,
                        &state,
                        expires_at,
                        term,
                        leader,
                    )
                    .await
                })
            })
            .await;
    }

    /// 单节点/M7 入口：不带 fencing token（见 [`Self::delete_restore_fenced`]）。
    pub async fn delete_restore(&self, bucket: &str, key: &str, version_id: &str) -> Result<()> {
        self.delete_restore_fenced(bucket, key, version_id, FencingEpoch::UNFENCED)
            .await
    }

    /// M8-1：行已被更高 term 接管（`fence_term > token.term`）→ 不删除（留给新 leader）。
    pub async fn delete_restore_fenced(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence: FencingEpoch,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.to_owned();
        let term = fence.term_i64();
        return self
            .control_write("tier-restore", move |txn| {
                let bucket = bucket.clone();
                let key = key.clone();
                let version_id = version_id.clone();
                Box::pin(async move {
                    txn.tier_restore_delete(&bucket, &key, &version_id, term)
                        .await
                })
            })
            .await;
    }

    /// 作业是否仍有 queued/running 任务（启动对账判断是否需要重排）。
    pub async fn job_task_active(&self, job_id: i64) -> Result<bool> {
        let n = self
            .control_store()
            .active_task_count(job_id, None)
            .await
            .map_err(crate::db::to_db)?;
        Ok(n > 0)
    }

    /// `(该用户, 全局)` 的 inflight 作业数（queued/running/paused/preempted）。
    pub async fn job_inflight_counts(&self, requested_by: Option<i64>) -> Result<(i64, i64)> {
        self.control_store()
            .job_inflight_counts(requested_by)
            .await
            .map_err(crate::db::to_db)
    }

    /// 原子领取一条任务（BEGIN IMMEDIATE）。先提升 queued 作业，再按优先级/时间取一条。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::claim_task_fenced`]）。
    pub async fn claim_task(&self, now: i64, timeout_secs: i64) -> Result<Option<ClaimedTask>> {
        self.claim_task_fenced(now, timeout_secs, FencingEpoch::UNFENCED)
            .await
    }

    /// M8-1：带 fencing token 的领取。只领 `fence_term <= token.term` 的排队任务
    /// （被更高 term 接管过的行不再归旧 leader），并把 token 落到行上作为租约归属。
    pub async fn claim_task_fenced(
        &self,
        now: i64,
        timeout_secs: i64,
        fence: FencingEpoch,
    ) -> Result<Option<ClaimedTask>> {
        // 对象后端：根 CAS 提供串行化；闭包可重跑（纯行读写）。
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move {
                    let claim = txn
                        .job_claim(now, timeout_secs, fence.term_i64(), fence.leader_i64())
                        .await?;
                    Ok(claim.map(|c| ClaimedTask {
                        job_id: c.job_id,
                        task_no: c.task_no,
                        action: c.action,
                        payload_json: c.payload_json,
                        attempt: c.attempt,
                    }))
                })
            })
            .await;
    }

    /// 领取后按动作类型收紧截止时间（claim 时动作未知，只能先写两类超时的较大值）。
    /// 只改 running 行：终态/已重排任务的 deadline 不被拉回。M8-1：只改本 token
    /// 领取的行（被更高 term 接管后旧 worker 的收尾是 no-op）。
    pub async fn set_task_deadline(
        &self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move {
                    txn.task_set_deadline(job_id, task_no, deadline_at, fence.term_i64())
                        .await
                })
            })
            .await;
    }

    /// 作业当前状态（None = 不存在）。
    pub async fn job_state(&self, id: i64) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .job(id)
            .await
            .map_err(crate::db::to_db)?
            .map(|j| j.state))
    }

    /// 抢占边界回退：把已领取但**尚未执行**的 running 任务放回 queued。
    /// 不消耗重试次数（attempt 不变）、不改结果列；已执行完的动作仍是 done。
    /// 只命中 running 行，重复调用幂等（返回是否命中）。
    pub async fn requeue_task(
        &self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<bool> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move {
                    txn.requeue_task(job_id, task_no, now, fence.term_i64())
                        .await
                })
            })
            .await;
    }

    /// 协作式抢占（动作边界，单节点）：高等优先级作业有**就绪**排队任务、且 worker
    /// 容量已满（running 任务数 ≥ `capacity`）时，把仍有排队任务的低优先级 running
    /// 作业标记为 `preempted`。只改作业状态——正在执行的动作不被中断，已领取未执行
    /// 的任务由 worker 在动作边界自行回退（[`Self::requeue_task`]）。返回被抢占数。
    ///
    /// M-2：抢占只在**同一 `requested_by`** 内生效（NULL 与 NULL 视为同一系统账户），
    /// 一个租户的高优先级作业不能抢占另一个租户的作业。
    /// 严格大于才抢占：同优先级按 FIFO（next_at/task_no），不互相打断。
    pub async fn preempt_lower_priority(&self, capacity: usize, now: i64) -> Result<usize> {
        if capacity == 0 {
            return Ok(0);
        }
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.preempt_lower_priority(capacity, now).await })
            })
            .await;
    }

    /// 释放抢占：同一 `requested_by` 内高等优先级的**就绪**排队任务已清空（都已领取
    /// 或完成）后，把 `preempted` 作业放回 `queued`（下一条领取按正常优先级顺序恢复）。
    /// 返回释放数。正在运行的高优先级任务不阻塞释放——空闲容量应回到低优先级作业，
    /// 领取顺序仍保证后续高优先级任务先被取走。
    pub async fn release_preempted(&self, now: i64) -> Result<usize> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.release_preempted(now).await })
            })
            .await;
    }

    /// 任务终态/重试。成功 → done；失败未达上限 → queued（指数退避）；达上限 → failed。
    /// 返回任务新状态；作业在无 queued/running 任务时收敛为 done/error。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::finish_task_fenced`]）。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_task(
        &self,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
    ) -> Result<&'static str> {
        self.finish_task_fenced(
            job_id,
            task_no,
            ok,
            code,
            message,
            now,
            max_attempts,
            FencingEpoch::UNFENCED,
        )
        .await
    }

    /// 任务失败后的重试退避（秒）：`10 * 2^attempt` 封顶 300，再加 0..9s 确定性抖动。
    /// `now` 是 Unix 秒；旧实现用 `now % 1000`，实际抖动是 0..999 **秒**，默认 3 次
    /// 重试可能拖到半小时。抖动只依赖 (job_id, task_no, now, attempt)，重放结果一致。
    pub(crate) fn retry_backoff_secs(attempt: i64, now: i64, job_id: i64, task_no: i64) -> i64 {
        let base = 10i64
            .saturating_mul(2i64.saturating_pow(attempt.min(30) as u32))
            .min(300);
        let mut h = (job_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (task_no as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (now as u64).wrapping_mul(0x1656_67B1_9E37_79F9)
            ^ (attempt as u64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        base + (h % 10) as i64
    }

    /// M8-1：带 fencing token 的收尾。行已被更高 term 接管（`fence_term > token.term`）
    /// → `Ok("fenced")` 且不做任何写入；调用方的动作结果丢弃，任务留 running 交给
    /// 新 leader 的过期/接管回收（at-least-once）。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_task_fenced(
        &self,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<&'static str> {
        let code = code.to_owned();
        let message = message.map(str::to_owned);
        return self
            .control_write("jobs", move |txn| {
                let code = code.clone();
                let message = message.clone();
                Box::pin(async move {
                    txn.finish_task(
                        job_id,
                        task_no,
                        ok,
                        &code,
                        message.as_deref(),
                        now,
                        max_attempts,
                        fence.term_i64(),
                    )
                    .await
                })
            })
            .await;
    }

    /// 超时回收：running 且 deadline 过期 → 按一次失败处理。返回回收条数。
    /// M8-1：接管时把行重标为当前 token，旧 holder 的迟到 finish 因此被拒。
    pub async fn recover_expired_tasks(
        &self,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize> {
        let rows = self
            .control_store()
            .recover_expired_task_ids(now, fence.term_i64())
            .await
            .map_err(crate::db::to_db)?;
        for (job_id, task_no) in &rows {
            self.take_over_task(*job_id, *task_no, None, now, fence)
                .await?;
            self.finish_task_fenced(
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("task deadline exceeded"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    /// M8-1：把一条 running 任务重标为当前 token（接管租约）。`started_before` 为 Some 时
    /// 只接管 `started_at <` 阈值的行（启动崩溃回收）；None = 只看 deadline 已过的行。
    async fn take_over_task(
        &self,
        job_id: i64,
        task_no: i64,
        started_before: Option<i64>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move {
                    txn.take_over_task(
                        job_id,
                        task_no,
                        started_before,
                        now,
                        fence.term_i64(),
                        fence.leader_i64(),
                    )
                    .await
                })
            })
            .await;
    }

    /// 4.11/M2：回收「state='running' 但没有任何 job_tasks」的作业行。
    /// lifecycle/inventory 等单条作业只写 jobs 行；进程在 job_start 之后崩溃会永久停在
    /// running。`cutoff` 限定 updated_at（长作业靠心跳续命，避免把仍在跑的扫描误标 error）。
    pub async fn recover_orphan_jobs(&self, cutoff: i64, now: i64) -> Result<usize> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.recover_orphan_jobs(cutoff, now).await })
            })
            .await;
    }

    /// M2：长作业心跳（只续 running 行），让 orphan 回收按 liveness 而非创建时间判断。
    pub async fn job_heartbeat(&self, job_id: i64, now: i64) -> Result<()> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.job_heartbeat(job_id, now).await })
            })
            .await;
    }

    /// 启动恢复：running 且 `started_at < cutoff`（进程崩溃遗留）按一次失败处理。
    /// M8-1：同样先接管 token 再收尾（旧进程若还活着，其 finish 会被拒）。
    pub async fn recover_stale_tasks(
        &self,
        cutoff: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize> {
        let rows = self
            .control_store()
            .recover_stale_task_ids(cutoff, fence.term_i64())
            .await
            .map_err(crate::db::to_db)?;
        let now = crate::db::now();
        for (job_id, task_no) in &rows {
            self.take_over_task(*job_id, *task_no, Some(cutoff), now, fence)
                .await?;
            self.finish_task_fenced(
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("process restart recovery"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    /// 作业详情（None = 不存在）。
    pub async fn job_get(&self, id: i64) -> Result<Option<serde_json::Value>> {
        let row = self
            .control_store()
            .job(id)
            .await
            .map_err(crate::db::to_db)?;
        Ok(row.map(|j| {
            serde_json::json!({
                "id": j.id,
                "kind": j.kind,
                "bucket": j.bucket,
                "prefix": j.prefix,
                "state": j.state,
                "spec_json": j.spec_json,
                "progress": j.progress,
                "total": j.total,
                "succeeded": j.succeeded,
                "failed": j.failed,
                "report_bucket": j.report_bucket,
                "report_prefix": j.report_prefix,
                "priority": j.priority,
                "requested_by": j.requested_by,
                "parent_job_id": j.parent_job_id,
                "error": j.error,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "finished_at": j.finished_at,
                "updated_at": j.updated_at,
            })
        }))
    }

    /// 作业级错误文案（报告写失败等；不改任务终态）。
    pub async fn job_set_error(&self, id: i64, msg: &str, now: i64) -> Result<()> {
        let msg = msg.to_owned();
        return self
            .control_write("jobs", move |txn| {
                let msg = msg.clone();
                Box::pin(async move { txn.job_set_error(id, &msg, now).await })
            })
            .await;
    }

    /// 作业归属/状态（API 判权用）。
    pub async fn job_owner(&self, id: i64) -> Result<Option<(Option<i64>, String)>> {
        Ok(self
            .control_store()
            .job(id)
            .await
            .map_err(crate::db::to_db)?
            .map(|j| (j.requested_by, j.state)))
    }

    /// 任务分页（task_no 升序）。
    pub async fn job_tasks(
        &self,
        id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .job_tasks_page(id, offset.max(0), limit.clamp(1, 500))
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_no": t.task_no,
                    "action": t.action,
                    "state": t.state,
                    "attempt": t.attempt,
                    "next_at": t.next_at,
                    "result_code": t.result_code,
                    "result_string": t.result_string,
                    "started_at": t.started_at,
                    "finished_at": t.finished_at,
                })
            })
            .collect())
    }

    /// 状态操作：`pause`/`resume`/`cancel`。返回是否有行被改（false = 不存在或状态不允许）。
    pub async fn job_set_state(&self, id: i64, action: &str, now: i64) -> Result<bool> {
        let action = action.to_owned();
        return self
            .control_write("jobs", move |txn| {
                let action = action.clone();
                Box::pin(async move { txn.job_set_state(id, &action, now).await })
            })
            .await;
    }

    /// 失败任务重试：建子作业（只含 failed 任务，原 task_no 保留为顺序），返回新 job id。
    pub async fn job_retry_failed(&self, id: i64, now: i64) -> Result<Option<i64>> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.job_retry_failed(id, now).await })
            })
            .await;
    }

    /// 报告行（含 `payload_json`）：S3 Batch 报告需要从载荷回读 bucket/key/versionId。
    /// 与 [`Self::job_tasks`] 分列——控制台分页不暴露内部载荷。
    pub async fn job_report_rows(&self, id: i64) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .job_tasks(id)
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_no": t.task_no,
                    "payload_json": t.payload_json,
                    "result_code": t.result_code,
                    "result_string": t.result_string,
                    "started_at": t.started_at,
                    "finished_at": t.finished_at,
                })
            })
            .collect())
    }

    /// 作业历史保留（feature 6）：`cutoff` 之前进入终态、按 id 升序的作业 id（最多
    /// `limit` 条）。只返回终态，调用方无需再筛 queued/running/preempted。
    pub async fn terminal_jobs_before(&self, cutoff: i64, limit: i64) -> Result<Vec<i64>> {
        self.control_store()
            .terminal_jobs_before(cutoff, limit)
            .await
            .map_err(crate::db::to_db)
    }

    /// 单机保留清理：一个事务内删除终态作业历史，返回删除的 jobs 行数。
    pub async fn purge_terminal_jobs_local(&self, ids: &[i64]) -> Result<u64> {
        let ids = ids.to_vec();
        return self
            .control_write("jobs", move |txn| {
                let ids = ids.clone();
                Box::pin(async move {
                    let (deleted, _) = txn.purge_terminal_jobs(&ids).await?;
                    Ok(deleted)
                })
            })
            .await;
    }

    // ——— M8 S5：Inventory 配置与运行行 ———

    pub async fn inventory_config_count(&self, bucket: &str) -> Result<i64> {
        self.control_store()
            .inventory_config_count(bucket)
            .await
            .map_err(crate::db::to_db)
    }

    pub async fn inventory_get_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfigView>> {
        self.control_store()
            .inventory_config(bucket, id)
            .await
            .map_err(crate::db::to_db)
    }

    pub async fn inventory_list_configs(&self, bucket: &str) -> Result<Vec<InventoryConfigView>> {
        self.control_store()
            .inventory_configs(bucket)
            .await
            .map_err(crate::db::to_db)
    }

    /// 生成器用：全部桶的配置（ORDER BY bucket, id 保证调度确定性）。
    pub async fn inventory_list_all_configs(&self) -> Result<Vec<InventoryConfigView>> {
        self.control_store()
            .inventory_configs_all()
            .await
            .map_err(crate::db::to_db)
    }

    /// 单机门面保留数字 uid 签名（决策 7）：0 → Admin；uid 必须存在，否则 RowNotFound。
    pub async fn inventory_upsert_config(
        &self,
        bucket: &str,
        id: &str,
        owner_id: i64,
        config_json: &str,
        now: i64,
    ) -> Result<()> {
        let row = InventoryConfigEntry {
            bucket: bucket.to_string(),
            id: id.to_string(),
            owner: self.owner_ref(owner_id).await?,
            config_json: config_json.to_string(),
            updated_at: now,
        };
        return self
            .control_write("inventory", move |txn| {
                let row = row.clone();
                Box::pin(async move { txn.inventory_config_upsert(&row).await })
            })
            .await;
    }

    pub async fn inventory_delete_config(&self, bucket: &str, id: &str) -> Result<bool> {
        let bucket = bucket.to_owned();
        let id = id.to_owned();
        return self
            .control_write("inventory", move |txn| {
                let bucket = bucket.clone();
                let id = id.clone();
                Box::pin(async move { txn.inventory_config_delete(&bucket, &id).await })
            })
            .await;
    }

    /// 3.6：Inventory 增量模式（默认全量；本地表，不进 Raft）。
    pub async fn inventory_incremental_get(&self, bucket: &str, config_id: &str) -> Result<bool> {
        Ok(self
            .control_store()
            .inventory_incremental(bucket, config_id)
            .await
            .map_err(crate::db::to_db)?
            .is_some_and(|r| r.enabled))
    }

    /// 3.6：设置增量模式；关闭时连旧水位一起清掉（重新开启先跑全量）。
    pub async fn inventory_incremental_set(
        &self,
        bucket: &str,
        config_id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let config_id = config_id.to_owned();
        return self
            .control_write("inventory", move |txn| {
                let bucket = bucket.clone();
                let config_id = config_id.clone();
                Box::pin(async move {
                    if enabled {
                        txn.inventory_incremental_upsert(&InventoryIncrementalRow {
                            bucket: bucket.clone(),
                            config_id: config_id.clone(),
                            enabled: true,
                            updated_at: now,
                        })
                        .await?;
                    } else {
                        txn.inventory_incremental_delete(&bucket, &config_id)
                            .await?;
                        txn.inventory_cursor_delete(&bucket, &config_id, None)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await;
    }

    /// 3.6：读取增量水位；`config_updated_at` 不匹配（配置已改）→ None，从头全量。
    pub async fn inventory_cursor_get(
        &self,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
    ) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .inventory_cursor(bucket, config_id)
            .await
            .map_err(crate::db::to_db)?
            .filter(|r| r.config_updated_at == config_updated_at)
            .map(|r| r.cursor))
    }

    /// 3.6：写入/清除增量水位（`None` → 删除，回到全量起点）。
    /// M8-1：行已被更高 term 接管时旧 leader 的迟到写入是 no-op。
    pub async fn inventory_cursor_set(
        &self,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let bucket = bucket.to_owned();
        let config_id = config_id.to_owned();
        let cursor = cursor.map(str::to_owned);
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("inventory", move |txn| {
                let bucket = bucket.clone();
                let config_id = config_id.clone();
                let cursor = cursor.clone();
                Box::pin(async move {
                    match cursor.as_deref().filter(|c| !c.is_empty()) {
                        Some(c) => txn
                            .inventory_cursor_upsert(&InventoryCursorRow {
                                bucket,
                                config_id,
                                cursor: c.to_string(),
                                config_updated_at,
                                updated_at: now,
                                fence_term: term,
                                fence_leader: leader,
                            })
                            .await
                            .map(|_| ()),
                        None => {
                            txn.inventory_cursor_delete(&bucket, &config_id, Some(term))
                                .await
                        }
                    }
                })
            })
            .await;
    }

    /// 建运行行；`(bucket, config_id, run_key)` 已存在 → None（同槽去重）。
    /// M8-1：带调度者 token（新行归当前 leader；旧 term 的执行器不能改其状态）。
    pub async fn inventory_create_run(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<Option<i64>> {
        let bucket = bucket.to_owned();
        let config_id = config_id.to_owned();
        let run_key = run_key.to_owned();
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("inventory", move |txn| {
                let bucket = bucket.clone();
                let config_id = config_id.clone();
                let run_key = run_key.clone();
                Box::pin(async move {
                    txn.inventory_run_create(&bucket, &config_id, &run_key, now, term, leader)
                        .await
                })
            })
            .await;
    }

    pub async fn inventory_run_set_job(
        &self,
        run_id: i64,
        job_id: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("inventory", move |txn| {
                Box::pin(async move {
                    txn.inventory_run_set_job(run_id, job_id, term, leader)
                        .await
                })
            })
            .await;
    }

    /// 执行器开始跑时写实际开始时刻（槽时间只体现在 run_key）。接管旧 term 的行时
    /// 重标 token，旧执行器的后续转换因此被拒。
    pub async fn inventory_run_started(
        &self,
        run_id: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        // `started_at` 只在首次执行时写；更高 term 的重试要重标 token，旧执行器的收尾
        // 才会被 `fence_term <= ?` 拒绝。
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("inventory", move |txn| {
                Box::pin(
                    async move { txn.inventory_run_started(run_id, now, term, leader).await },
                )
            })
            .await;
    }

    pub async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>> {
        self.control_store()
            .inventory_run_by_key(bucket, config_id, run_key)
            .await
            .map_err(crate::db::to_db)
    }

    /// M8-1：行已被更高 term 接管 → no-op（旧 leader 的迟到完成不覆盖新状态）。
    pub async fn inventory_run_finish(
        &self,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence: FencingEpoch,
    ) -> Result<()> {
        let state = state.to_owned();
        let manifest_key = manifest_key.map(str::to_owned);
        let error = error.map(str::to_owned);
        let (term, leader) = (fence.term_i64(), fence.leader_i64());
        return self
            .control_write("inventory", move |txn| {
                let state = state.clone();
                let manifest_key = manifest_key.clone();
                let error = error.clone();
                Box::pin(async move {
                    txn.inventory_run_finish(
                        run_id,
                        &state,
                        manifest_key.as_deref(),
                        error.as_deref(),
                        term,
                        leader,
                    )
                    .await
                })
            })
            .await;
    }

    /// Inventory 作业的 `progress` = 已扫描对象数（任务收尾时引擎会 +1，调用方存 n-1）。
    pub async fn job_set_progress(&self, job_id: i64, progress: i64, now: i64) -> Result<()> {
        return self
            .control_write("jobs", move |txn| {
                Box::pin(async move { txn.job_set_progress(job_id, progress, now).await })
            })
            .await;
    }

    // ——— M4 切片 7：桶策略与审计 ———

    /// 桶 DB 行存在性（控制面写前置校验：防「给不存在桶预置策略，同名桶日后创建即继承」
    /// ——S6-1 同类边角，verifier 非阻断 3）。盘上有、表里无的历史桶由启动回填补齐。
    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::db::to_db)?
            .is_some())
    }

    /// 桶策略原文（None = 未配）。
    pub async fn bucket_policy(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket_policy(bucket)
            .await
            .map_err(crate::db::to_db)?
            .map(|p| p.policy_json))
    }

    /// 桶事件通知配置原文（None = 未配）。
    pub async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket_notification(bucket)
            .await
            .map_err(crate::db::to_db)?
            .map(|e| e.config_json))
    }

    /// 写桶策略（全量替换）。
    pub async fn set_bucket_policy(&self, bucket: &str, json: &str, now: i64) -> Result<()> {
        let bucket = bucket.to_owned();
        let json = json.to_owned();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                let json = json.clone();
                Box::pin(async move { txn.bucket_policy_set(&bucket, &json, now).await })
            })
            .await;
    }

    /// 清桶策略（删桶时连同清理——桶名可复用，S6-1 同款语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<()> {
        let bucket = bucket.to_owned();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                Box::pin(async move { txn.bucket_policy_delete(&bucket).await })
            })
            .await;
    }

    /// 写桶事件通知配置（全量替换）。
    pub async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
        now: i64,
    ) -> Result<()> {
        let bucket = bucket.to_string();
        let config_json = config_json.to_string();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                let config_json = config_json.clone();
                Box::pin(async move {
                    txn.bucket_notification_set(&bucket, &config_json, now)
                        .await
                })
            })
            .await;
    }

    /// 清桶事件通知配置（幂等）。
    pub async fn clear_bucket_notification(&self, bucket: &str) -> Result<()> {
        let bucket = bucket.to_string();
        return self
            .control_write("console", move |txn| {
                let bucket = bucket.clone();
                Box::pin(async move { txn.bucket_notification_delete(&bucket).await })
            })
            .await;
    }

    /// 死信重投（dead → pending）；0 行 = 不存在或不是 dead。
    pub async fn event_redeliver(&self, id: i64, now: i64) -> Result<bool> {
        return self
            .object_local_write("console", move |txn| {
                Box::pin(async move { txn.event_redeliver(id, now).await })
            })
            .await;
    }

    /// C5：审计保留策略——删除 ts < cutoff 的行，返回删除数（worker 按 OSS_AUDIT_RETENTION_DAYS）。
    /// 每批最多 `limit` 行（id 子查询圈定），大积压时不让单条 DELETE 长时间占写锁。
    pub async fn delete_audit_older_than(&self, cutoff: i64, limit: i64) -> Result<u64> {
        use xl_store::control_store::audit::AuditTxn;

        return self
            .object_local_write("lifecycle", move |txn| {
                Box::pin(async move { txn.audit_delete_older_than(cutoff, limit).await })
            })
            .await;
    }

    /// 审计查询（控制台；可选 decision/时间窗/uid/op/bucket/key 前缀过滤；最新在前）。
    /// 动态 WHERE 只拼**固定片段**，用户值一律绑定参数。
    pub async fn list_audit(&self, f: &AuditFilter<'_>) -> Result<Vec<serde_json::Value>> {
        use xl_store::control_store::audit::AuditQuery;

        let query = AuditQuery {
            limit: f.limit,
            decision: f.decision.map(str::to_owned),
            from: f.from,
            to: f.to,
            uid: f.uid,
            op: f.op.map(str::to_owned),
            bucket: f.bucket.map(str::to_owned),
            key_prefix: f.key_prefix.map(str::to_owned),
        };
        let rows = self
            .control_store()
            .audit_page(&query)
            .await
            .map_err(crate::db::to_db)?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id,
                    "ts": r.ts,
                    "uid": r.uid,
                    "op": r.op,
                    "bucket": r.bucket,
                    "object_key": r.object_key,
                    "decision": r.decision,
                    "detail": r.detail,
                })
            })
            .collect())
    }
}

/// 建桶行写入结果（console 建桶的配额最终判定）。
pub enum BucketRowOutcome {
    Written,
    QuotaExceeded,
}

/// 审计查询的过滤条件（`Db::list_audit`）；`key_prefix` 为 object_key 前缀，其余精确匹配。
#[derive(Debug)]
pub struct AuditFilter<'a> {
    pub limit: i64,
    pub decision: Option<&'a str>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub uid: Option<i64>,
    pub op: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub key_prefix: Option<&'a str>,
}

