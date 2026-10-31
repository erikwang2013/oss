//! M9 控制面门面：`Raft` 是 M9 唯一 HTTP 写入口；`Local` 仅供测试/兼容。
//!
//! HTTP handler 必须先 `is_raft()`：只有 Raft 分支可接 M9 路由，M7/standalone 的桶
//! 生命周期（set 指派、物理优先、LWW 补偿、广播）继续走 console/S3 旧路径。`Raft`
//! 在 append 前做 24 MiB 尺寸闸、写超时有界（503），提交后幂等执行副作用（失败重试
//! 3 次并留给启动对账）。

use std::sync::Arc;
use std::time::Duration;

use xl_store::store::Store;

use super::apply::{self, SideEffect};
use super::command::{ControlCommand, ControlResponse, SizeError};
use super::{RaftHandle, WriteError};
use crate::db::Db;

/// 提交后副作用执行器（缓存/物理桶目录；each site 本地执行）。
pub struct PostCommit {
    db: Arc<Db>,
    store: Arc<Store>,
    ap_cache: Arc<xl_s3::ap::ApCache>,
    olap_cache: Arc<xl_s3::olap::OlapCache>,
    region: String,
}

impl PostCommit {
    pub fn new(
        db: Arc<Db>,
        store: Arc<Store>,
        ap_cache: Arc<xl_s3::ap::ApCache>,
        olap_cache: Arc<xl_s3::olap::OlapCache>,
        region: String,
    ) -> Self {
        Self {
            db,
            store,
            ap_cache,
            olap_cache,
            region,
        }
    }

    /// S7.2：快照安装提交后，按新控制表重建本节点缓存与桶目录（数据站点）。
    pub async fn after_snapshot_install(&self) {
        match self.db.list_access_points(None).await {
            Ok(rows) => {
                let aps: Vec<_> = rows
                    .into_iter()
                    .filter_map(|row| {
                        let account = row.account();
                        match xl_s3::ap::AccessPoint::from_row(
                            row.name,
                            row.bucket,
                            row.owner_id,
                            row.policy_json,
                            &self.region,
                            &account,
                        ) {
                            Ok(ap) => Some(ap),
                            Err(e) => {
                                tracing::warn!(error = %e, "快照安装后 AP 行无效（跳过缓存）");
                                None
                            }
                        }
                    })
                    .collect();
                self.ap_cache.replace_all(aps);
            }
            Err(e) => tracing::warn!(error = %e, "快照安装后读取 AP 失败"),
        }
        self.olap_cache.clear();
        crate::access_points_api::preload_olap(&self.db, &self.ap_cache, &self.olap_cache).await;
        self.store.invalidate_gate_all();
        match self.db.list_bucket_rows().await {
            Ok(rows) => self.store.reconcile_bucket_dirs(&rows).await,
            Err(e) => tracing::warn!(error = %e, "快照安装后列举桶行失败"),
        }
    }

    async fn run(&self, effects: Vec<SideEffect>) {
        for effect in effects {
            self.run_one(effect).await;
        }
    }

    async fn run_one(&self, effect: SideEffect) {
        match effect {
            SideEffect::BucketDirCreate(name) => {
                for _ in 0..3 {
                    match self.store.bucket_exists(&name).await {
                        Ok(true) => return,
                        Ok(false) => match self.store.create_bucket(&name).await {
                            Ok(()) | Err(xl_store::error::Error::BucketAlreadyExists(_)) => return,
                            Err(e) => {
                                tracing::warn!(error = %e, bucket = %name, "建桶目录失败（重试）")
                            }
                        },
                        Err(e) => {
                            tracing::warn!(error = %e, bucket = %name, "建桶目录前置查询失败（重试）")
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tracing::warn!(bucket = %name, "建桶目录重试耗尽，留待启动对账");
            }
            SideEffect::BucketDirTombstone(name) => {
                for _ in 0..3 {
                    match self.store.delete_bucket_sync(&name).await {
                        Ok(()) => return,
                        Err(e) => {
                            tracing::warn!(error = %e, bucket = %name, "桶目录 tombstone 失败（重试）")
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tracing::warn!(bucket = %name, "桶目录 tombstone 重试耗尽，留待启动清 trash");
            }
            SideEffect::BucketDirDelete(name) => {
                for _ in 0..3 {
                    match self.store.bucket_exists(&name).await {
                        Ok(false) => return,
                        Ok(true) => match self.store.delete_bucket(&name).await {
                            Ok(()) | Err(xl_store::error::Error::NoSuchBucket(_)) => return,
                            Err(e) => {
                                tracing::warn!(error = %e, bucket = %name, "删桶目录失败（重试）")
                            }
                        },
                        Err(e) => {
                            tracing::warn!(error = %e, bucket = %name, "删桶目录前置查询失败（重试）")
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tracing::warn!(bucket = %name, "删桶目录重试耗尽，留待启动对账");
            }
            SideEffect::ApReload(name) => match self.db.get_access_point(&name).await {
                Ok(Some(row)) => {
                    let account = row.account();
                    match xl_s3::ap::AccessPoint::from_row(
                        row.name,
                        row.bucket,
                        row.owner_id,
                        row.policy_json,
                        &self.region,
                        &account,
                    ) {
                        Ok(ap) => self.ap_cache.put(ap),
                        Err(e) => tracing::warn!(error = %e, ap = %name, "AP 缓存刷新失败"),
                    }
                }
                Ok(None) => self.ap_cache.remove(&name),
                Err(e) => tracing::warn!(error = %e, ap = %name, "AP 缓存刷新回查失败"),
            },
            SideEffect::ApRemove(name) => self.ap_cache.remove(&name),
            SideEffect::ApInvalidateBucket(bucket) => self.ap_cache.invalidate_bucket(&bucket),
            SideEffect::OlapRemove(name) => self.olap_cache.remove(&name),
            SideEffect::OlapInvalidateBucket(bucket) => self.olap_cache.invalidate_bucket(&bucket),
            SideEffect::OlapReload => {
                crate::access_points_api::preload_olap(&self.db, &self.ap_cache, &self.olap_cache)
                    .await;
            }
        }
    }
}

enum Inner {
    /// 测试/兼容门面。HTTP handler 必须先 `is_raft()` 再进 Raft 分支；
    /// M7/standalone 的桶生命周期（set 指派、物理优先、LWW 补偿、广播）继续走
    /// console/S3 旧路径，**不得**把 `Local` 接入 M9 路由（S6a review I1 裁决）。
    Local(Arc<Db>),
    Raft(Arc<RaftHandle>),
}

/// 控制写门面。`Local` 只用于 `cfg.raft.is_none()`；`Raft` 由 `state::load` 构造。
pub struct ControlPlane {
    inner: Inner,
    post: PostCommit,
    /// 属主解析与 sync 模式判定（append 前的本地读，不写库）。
    db: Arc<Db>,
}

/// 门面错误 → HTTP 状态（S6b-d 的 handler 只做映射）：400/404/409/413/500/503。
#[derive(Debug)]
pub enum ControlError {
    TooLarge {
        bytes: usize,
        max: usize,
    },
    Timeout(String),
    Unavailable(String),
    /// 本节点不是 leader 且转发未完成（M8-0）：带 membership 里的 leader 提示。
    NotLeader {
        leader: Option<u64>,
        endpoint: Option<String>,
    },
    /// 客户端输入非法（如未注册的 `Ext.type_id`）：400，不 append、不可重试。
    Invalid(String),
    /// 账户桶数硬配额已满（leader append 前拒绝）：403，不 append、不重试。
    QuotaExceeded,
    Internal(String),
}

impl ControlError {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::TooLarge { .. } => 413,
            Self::Timeout(_) | Self::Unavailable(_) | Self::NotLeader { .. } => 503,
            Self::Invalid(_) => 400,
            Self::QuotaExceeded => 403,
            Self::Internal(_) => 500,
        }
    }

    /// M8-0：leader 提示 `(endpoint, leader_id)`；仅 endpoint 来自成员表。
    /// 无 endpoint 时返回 `None`（调用方可回退到 `leader` id）。
    pub fn leader_hint(&self) -> Option<(&str, Option<u64>)> {
        match self {
            Self::NotLeader { leader, endpoint } => {
                endpoint.as_deref().map(|endpoint| (endpoint, *leader))
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge { bytes, max } => {
                write!(f, "command {bytes} bytes exceeds raft limit {max}")
            }
            Self::Timeout(m) => write!(f, "raft write timeout: {m}"),
            Self::Unavailable(m) => write!(f, "raft unavailable: {m}"),
            Self::NotLeader { leader, endpoint } => write!(
                f,
                "not leader (leader={leader:?}, endpoint={endpoint:?}); retry on the leader"
            ),
            Self::Invalid(m) => write!(f, "invalid control command: {m}"),
            Self::QuotaExceeded => write!(f, "account bucket quota exceeded"),
            Self::Internal(m) => write!(f, "control apply failed: {m}"),
        }
    }
}

impl std::error::Error for ControlError {}

/// `ControlResponse` → HTTP：成功由 handler 决定 200/204；这里给拒绝状态。
pub fn response_status(response: ControlResponse) -> u16 {
    match response {
        ControlResponse::Ok => 200,
        ControlResponse::BucketNotEmpty | ControlResponse::Conflict => 409,
        ControlResponse::NotFound => 404,
        ControlResponse::Invalid => 400,
    }
}

fn internal(e: impl std::fmt::Display) -> ControlError {
    ControlError::Internal(e.to_string())
}

impl ControlPlane {
    /// 测试/兼容构造：不复制 M7 的 set 指派/物理优先/LWW 补偿语义。生产 handler
    /// 只允许在 `is_raft()` 时路由本门面；`!is_raft()` 保持旧直写路径。
    pub fn local(
        db: Arc<Db>,
        store: Arc<Store>,
        ap_cache: Arc<xl_s3::ap::ApCache>,
        olap_cache: Arc<xl_s3::olap::OlapCache>,
        region: String,
    ) -> Self {
        let post = PostCommit::new(Arc::clone(&db), store, ap_cache, olap_cache, region);
        Self {
            inner: Inner::Local(db.clone()),
            post,
            db,
        }
    }

    pub fn raft(
        handle: Arc<RaftHandle>,
        db: Arc<Db>,
        store: Arc<Store>,
        ap_cache: Arc<xl_s3::ap::ApCache>,
        olap_cache: Arc<xl_s3::olap::OlapCache>,
        region: String,
    ) -> Self {
        let post = PostCommit::new(Arc::clone(&db), store, ap_cache, olap_cache, region);
        Self {
            inner: Inner::Raft(handle),
            post,
            db,
        }
    }

    pub fn is_raft(&self) -> bool {
        matches!(self.inner, Inner::Raft(_))
    }

    /// Raft 句柄（Local = None）；jobs 运行态复制决策用。
    pub fn raft_handle(&self) -> Option<&Arc<RaftHandle>> {
        match &self.inner {
            Inner::Raft(handle) => Some(handle),
            Inner::Local(_) => None,
        }
    }

    /// 本节点当前是否 leader；Local（单机/测试）恒 true。
    pub fn is_leader(&self) -> bool {
        match &self.inner {
            Inner::Raft(handle) => handle.is_leader(),
            Inner::Local(_) => true,
        }
    }

    /// `/api/v1/raft/status`：Raft 下报真实运行状态；M7/Local 保持 S0 骨架。
    /// `configured` = `cfg.raft.is_some()`，未配置 `[raft]` 时能力位全 false。
    /// S7.2：快照安装后的缓存/目录重建入口（state 的 install watcher 调用）。
    pub async fn after_snapshot_install(&self) {
        self.post.after_snapshot_install().await;
    }

    pub fn status_json(&self, configured: bool) -> serde_json::Value {
        match &self.inner {
            Inner::Raft(handle) => handle.status_json(configured),
            Inner::Local(_) => super::status_json(configured),
        }
    }

    /// 本机已 apply 的日志 index（M8-3 读一致性/响应头）；Local 无 Raft 日志 = 0。
    pub fn applied_index(&self) -> u64 {
        match &self.inner {
            Inner::Local(_) => 0,
            Inner::Raft(handle) => handle.applied_index(),
        }
    }

    /// Raft 下等本地状态机追上 leader 已应用点（有界）；Local 恒已一致。
    pub async fn wait_caught_up(&self, timeout: Duration) -> bool {
        match &self.inner {
            Inner::Local(_) => true,
            Inner::Raft(handle) => handle.wait_caught_up(timeout).await,
        }
    }

    /// xl-s3 注入用：仅 Raft 模式返回 sink（M7/standalone 保持 `None` 直写路径）。
    pub fn bucket_sink(
        self: &Arc<Self>,
    ) -> Option<Arc<dyn xl_s3::bucket_control::BucketControlSink>> {
        self.is_raft()
            .then(|| Arc::clone(self) as Arc<dyn xl_s3::bucket_control::BucketControlSink>)
    }

    /// 一次控制写：恰好一条命令、恰好一个 `ControlResponse`。
    pub async fn write(&self, cmd: ControlCommand) -> Result<ControlResponse, ControlError> {
        match &self.inner {
            // S6b-d 路由闸：Local 仅供测试/兼容；HTTP handler 不得把 M7
            // 桶生命周期接进来（review I1）。
            Inner::Local(db) => {
                // AC-2：BucketCreate 的配额 count+insert 必须原子——先取写锁再读计数，
                // 并发 Local 写只有一个能越过（Raft 路径的闸在 leader append 前）。
                let mut tx = db
                    .pool()
                    .begin_with("BEGIN IMMEDIATE")
                    .await
                    .map_err(internal)?;
                if let ControlCommand::BucketCreate { name, owner, .. } = &cmd
                    && Db::bucket_create_quota_reached_conn(&mut tx, name, owner.as_deref())
                        .await
                        .map_err(internal)?
                {
                    return Err(ControlError::QuotaExceeded);
                }
                let applied = {
                    let mut cw = apply::control_tx(&mut tx);
                    apply::apply_command(&mut cw, &cmd)
                        .await
                        .map_err(internal)?
                };
                tx.commit().await.map_err(internal)?;
                // 业务拒绝不得触发副作用（如 Invalid 的 BucketCreate 不能建目录）。
                if applied.response == ControlResponse::Ok {
                    self.post.run(applied.effects).await;
                }
                Ok(applied.response)
            }
            Inner::Raft(handle) => {
                cmd.check_size().map_err(|e| match e {
                    SizeError::TooLarge(bytes) => ControlError::TooLarge {
                        bytes,
                        max: super::COMMAND_MAX_BYTES,
                    },
                    SizeError::Encode(e) => ControlError::Internal(e.to_string()),
                })?;
                let mut effects = apply::effects_for(&cmd);
                // sync 桶物理删除用 tombstone（quiesce 后 rename，幂等；启动清 trash）。
                if let ControlCommand::BucketDelete { name } = &cmd {
                    match self.db.bucket_sync_state(name).await {
                        Ok((mode, _, _)) if mode == "sync" => {
                            effects = effects
                                .into_iter()
                                .map(|e| match e {
                                    SideEffect::BucketDirDelete(n) => {
                                        SideEffect::BucketDirTombstone(n)
                                    }
                                    other => other,
                                })
                                .collect();
                        }
                        Ok(_) => {}
                        Err(e) => {
                            // 读不到模式时用 tombstone 兜底（不物理直删 sync 目录）。
                            tracing::warn!(error = %e, bucket = %name, "读桶模式失败，删桶副作用按 tombstone 处理");
                            effects = effects
                                .into_iter()
                                .map(|e| match e {
                                    SideEffect::BucketDirDelete(n) => {
                                        SideEffect::BucketDirTombstone(n)
                                    }
                                    other => other,
                                })
                                .collect();
                        }
                    }
                }
                let response = match handle.client_write(cmd).await {
                    Ok(r) => r,
                    Err(WriteError::Timeout(d)) => {
                        return Err(ControlError::Timeout(format!("after {d:?}")));
                    }
                    Err(WriteError::ForwardToLeader { leader, endpoint }) => {
                        // 选举窗口内既无 leader id 也无 endpoint：没有可给的提示，
                        // 保持既有 Unavailable（可重试）语义，避免伪装成 307 目标。
                        if leader.is_none() && endpoint.is_none() {
                            return Err(ControlError::Unavailable(
                                "no leader known yet; retry".into(),
                            ));
                        }
                        return Err(ControlError::NotLeader { leader, endpoint });
                    }
                    Err(WriteError::NoQuorum) => {
                        return Err(ControlError::Unavailable(
                            "no leader/quorum; write may have committed, retry is idempotent"
                                .into(),
                        ));
                    }
                    Err(WriteError::FeatureUnavailable {
                        required,
                        effective,
                    }) => {
                        return Err(ControlError::Unavailable(format!(
                            "raft feature level {required} not active (effective {effective}): \
                             需要 feature {required}（finish rollout/enable [raft].feature_level）"
                        )));
                    }
                    Err(WriteError::UnregisteredExtType { type_id }) => {
                        return Err(ControlError::Invalid(format!(
                            "Ext type_id {type_id:?} is not registered; nothing was appended"
                        )));
                    }
                    Err(WriteError::ExtTypeUnavailable { type_id, missing }) => {
                        return Err(ControlError::Unavailable(format!(
                            "Ext type_id {type_id:?} is not advertised by every current \
                             voter/learner ({}); upgrade/wait for handshakes; nothing was appended",
                            missing.join("; ")
                        )));
                    }
                    Err(WriteError::QuotaExceeded) => return Err(ControlError::QuotaExceeded),
                    Err(WriteError::Raft(msg)) => return Err(ControlError::Unavailable(msg)),
                };
                if response == ControlResponse::Ok {
                    self.post.run(effects).await;
                }
                Ok(response)
            }
        }
    }
}

/// S6c：控制响应/门面错误 → xl-s3 sink 错误（handler 再映射 S3/console 状态码）。
fn sink_result(
    r: Result<ControlResponse, ControlError>,
) -> Result<(), xl_s3::bucket_control::BucketControlError> {
    use xl_s3::bucket_control::BucketControlError as E;
    match r {
        Ok(ControlResponse::Ok) => Ok(()),
        Ok(ControlResponse::NotFound) => Err(E::NotFound),
        Ok(ControlResponse::Conflict) => Err(E::Conflict),
        Ok(ControlResponse::BucketNotEmpty) => Err(E::NotEmpty),
        Ok(ControlResponse::Invalid) => Err(E::Invalid),
        Err(ControlError::TooLarge { .. }) => Err(E::TooLarge),
        Err(ControlError::Timeout(m) | ControlError::Unavailable(m)) => {
            tracing::warn!(error = %m, "control write unavailable");
            Err(E::Unavailable)
        }
        Err(ControlError::NotLeader { endpoint, .. }) => {
            tracing::warn!(?endpoint, "control write not on leader");
            Err(E::NotLeader { endpoint })
        }
        Err(ControlError::Invalid(m)) => {
            tracing::warn!(error = %m, "control write rejected");
            Err(E::Invalid)
        }
        Err(ControlError::QuotaExceeded) => Err(E::QuotaExceeded),
        Err(ControlError::Internal(m)) => {
            tracing::error!(error = %m, "control write failed");
            Err(E::Internal)
        }
    }
}

#[async_trait::async_trait]
impl xl_s3::bucket_control::BucketControlSink for ControlPlane {
    async fn create_bucket(
        &self,
        spec: xl_s3::bucket_control::BucketCreateSpec,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        use xl_s3::bucket_control::BucketControlError as E;
        // uid → username 在 append 前解析（apply 只做确定性 SQL）。
        let owner = match spec.owner_uid {
            None => None,
            Some(uid) => match self.db.get_user(uid).await {
                Ok(Some((name, false, _))) => Some(name),
                Ok(Some((_, true, _))) => None,
                Ok(None) => {
                    tracing::error!(uid, "建桶属主用户不存在（不变量破坏）");
                    return Err(E::Internal);
                }
                Err(e) => {
                    tracing::error!(error = %e, uid, "解析建桶属主失败");
                    return Err(E::Internal);
                }
            },
        };
        let cmd = ControlCommand::BucketCreate {
            name: spec.name,
            owner,
            created_at: spec.created_at,
            replication_mode: spec.replication_mode,
            versioning: spec.versioning,
            object_lock: spec.object_lock,
            object_lock_default_mode: spec.object_lock_default_mode,
            object_lock_default_days: spec.object_lock_default_days,
            default_encryption: spec.default_encryption,
        };
        sink_result(self.write(cmd).await)
    }

    async fn set_versioning(
        &self,
        bucket: &str,
        status: &str,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        sink_result(
            self.write(ControlCommand::BucketSetVersioning {
                name: bucket.to_string(),
                versioning: status.to_string(),
            })
            .await,
        )
    }

    async fn set_object_lock(
        &self,
        bucket: &str,
        enabled: bool,
        default_mode: Option<String>,
        default_days: Option<i64>,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        sink_result(
            self.write(ControlCommand::BucketSetObjectLock {
                name: bucket.to_string(),
                enabled,
                default_mode,
                default_days,
            })
            .await,
        )
    }

    async fn set_notification(
        &self,
        bucket: &str,
        config_json: Option<String>,
        updated_at: i64,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        let cmd = match config_json {
            Some(config_json) => ControlCommand::BucketSetNotification {
                bucket: bucket.to_string(),
                config_json,
                updated_at,
            },
            None => ControlCommand::BucketClearNotification {
                bucket: bucket.to_string(),
            },
        };
        sink_result(self.write(cmd).await)
    }

    async fn set_default_encryption(
        &self,
        bucket: &str,
        algorithm: Option<String>,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        let cmd = match algorithm {
            Some(algorithm) => ControlCommand::BucketSetDefaultEncryption {
                bucket: bucket.to_string(),
                algorithm,
            },
            None => ControlCommand::BucketClearDefaultEncryption {
                bucket: bucket.to_string(),
            },
        };
        sink_result(self.write(cmd).await)
    }

    async fn set_policy(
        &self,
        bucket: &str,
        policy_json: &str,
        updated_at: i64,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        sink_result(
            self.write(ControlCommand::BucketSetPolicy {
                bucket: bucket.to_string(),
                policy_json: policy_json.to_string(),
                updated_at,
            })
            .await,
        )
    }

    async fn delete_policy(
        &self,
        bucket: &str,
    ) -> Result<(), xl_s3::bucket_control::BucketControlError> {
        sink_result(
            self.write(ControlCommand::BucketDeletePolicy {
                bucket: bucket.to_string(),
            })
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_error_and_response_http_mapping() {
        assert_eq!(
            ControlError::TooLarge { bytes: 1, max: 2 }.http_status(),
            413
        );
        assert_eq!(ControlError::Timeout("t".into()).http_status(), 503);
        assert_eq!(ControlError::Unavailable("u".into()).http_status(), 503);
        assert_eq!(ControlError::Invalid("i".into()).http_status(), 400);
        assert_eq!(ControlError::QuotaExceeded.http_status(), 403);
        assert_eq!(ControlError::Internal("i".into()).http_status(), 500);

        // M8-0：NotLeader 是 503 且带成员表 endpoint + leader id 提示。
        let not_leader = ControlError::NotLeader {
            leader: Some(2),
            endpoint: Some("http://10.0.0.2:9000".into()),
        };
        assert_eq!(not_leader.http_status(), 503);
        assert_eq!(
            not_leader.leader_hint(),
            Some(("http://10.0.0.2:9000", Some(2)))
        );
        assert_eq!(
            ControlError::NotLeader {
                leader: Some(2),
                endpoint: None
            }
            .leader_hint(),
            None
        );
        assert_eq!(ControlError::Unavailable("u".into()).leader_hint(), None);

        assert_eq!(response_status(ControlResponse::Ok), 200);
        assert_eq!(response_status(ControlResponse::BucketNotEmpty), 409);
        assert_eq!(response_status(ControlResponse::Conflict), 409);
        assert_eq!(response_status(ControlResponse::NotFound), 404);
        assert_eq!(response_status(ControlResponse::Invalid), 400);
    }

    #[test]
    fn sink_result_keeps_conflict_and_not_empty_distinct() {
        use xl_s3::bucket_control::BucketControlError as E;
        assert_eq!(
            sink_result(Ok(ControlResponse::BucketNotEmpty)),
            Err(E::NotEmpty)
        );
        assert_eq!(sink_result(Ok(ControlResponse::Conflict)), Err(E::Conflict));
        assert_eq!(
            sink_result(Err(ControlError::TooLarge { bytes: 1, max: 2 })),
            Err(E::TooLarge)
        );
        assert_eq!(
            sink_result(Err(ControlError::Internal("x".into()))),
            Err(E::Internal)
        );
        assert_eq!(
            sink_result(Err(ControlError::QuotaExceeded)),
            Err(E::QuotaExceeded)
        );
        // M8-0：S3 控制写拿到 leader endpoint 提示（leader id 不跨 crate 传递）。
        assert_eq!(
            sink_result(Err(ControlError::NotLeader {
                leader: Some(2),
                endpoint: Some("http://10.0.0.2:9000".into()),
            })),
            Err(E::NotLeader {
                endpoint: Some("http://10.0.0.2:9000".into())
            })
        );
    }

    /// I-1 的副作用底座：重复执行幂等；物理失败 3 次后放弃（不 panic），
    /// 盘恢复后再次执行即修复（启动对账走同一入口）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn post_commit_effects_idempotent_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("m9.db").display());
        let db = Arc::new(Db::connect_raft(&url).await.unwrap());
        let store = Arc::new(
            Store::open(xl_store::store::StoreConfig::new(dir.path().join("data")))
                .await
                .unwrap(),
        );
        let post = PostCommit::new(
            db,
            Arc::clone(&store),
            Arc::new(xl_s3::ap::ApCache::new()),
            Arc::new(xl_s3::olap::OlapCache::new()),
            "us-east-1".into(),
        );

        post.run_one(SideEffect::BucketDirCreate("pcb".into()))
            .await;
        post.run_one(SideEffect::BucketDirCreate("pcb".into()))
            .await;
        assert!(store.bucket_exists("pcb").await.unwrap());
        post.run_one(SideEffect::BucketDirTombstone("pcb".into()))
            .await;
        post.run_one(SideEffect::BucketDirTombstone("pcb".into()))
            .await;
        assert!(!store.bucket_exists("pcb").await.unwrap());

        // 数据盘临时换成普通文件：建目录 3 次失败后返回（不 panic、不卡死）。
        let data = dir.path().join("data");
        let backup = dir.path().join("data.bak");
        tokio::fs::rename(&data, &backup).await.unwrap();
        tokio::fs::write(&data, b"not a dir").await.unwrap();
        post.run_one(SideEffect::BucketDirCreate("pcb2".into()))
            .await;
        assert!(!store.bucket_exists("pcb2").await.unwrap_or(false));
        tokio::fs::remove_file(&data).await.unwrap();
        tokio::fs::rename(&backup, &data).await.unwrap();
        // 重跑同一副作用 = 对账修复（幂等）。
        post.run_one(SideEffect::BucketDirCreate("pcb2".into()))
            .await;
        assert!(store.bucket_exists("pcb2").await.unwrap());
    }
}
