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
                Ok(Some(row)) => match xl_s3::ap::AccessPoint::from_row(
                    row.name,
                    row.bucket,
                    row.owner_id,
                    row.policy_json,
                    &self.region,
                ) {
                    Ok(ap) => self.ap_cache.put(ap),
                    Err(e) => tracing::warn!(error = %e, ap = %name, "AP 缓存刷新失败"),
                },
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
}

/// 门面错误 → HTTP 状态（S6b-d 的 handler 只做映射）：400/404/409/413/500/503。
#[derive(Debug)]
pub enum ControlError {
    TooLarge { bytes: usize, max: usize },
    Timeout(String),
    Unavailable(String),
    Internal(String),
}

impl ControlError {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::TooLarge { .. } => 413,
            Self::Timeout(_) | Self::Unavailable(_) => 503,
            Self::Internal(_) => 500,
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
            inner: Inner::Local(db),
            post,
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
        let post = PostCommit::new(db, store, ap_cache, olap_cache, region);
        Self {
            inner: Inner::Raft(handle),
            post,
        }
    }

    pub fn is_raft(&self) -> bool {
        matches!(self.inner, Inner::Raft(_))
    }

    /// 一次控制写：恰好一条命令、恰好一个 `ControlResponse`。
    pub async fn write(&self, cmd: ControlCommand) -> Result<ControlResponse, ControlError> {
        match &self.inner {
            // S6b-d 路由闸：Local 仅供测试/兼容；HTTP handler 不得把 M7
            // 桶生命周期接进来（review I1）。
            Inner::Local(db) => {
                let mut tx = db.pool().begin().await.map_err(internal)?;
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
                let effects = apply::effects_for(&cmd);
                let response = match handle.write(cmd).await {
                    Ok(r) => r,
                    Err(WriteError::Timeout(d)) => {
                        return Err(ControlError::Timeout(format!("after {d:?}")));
                    }
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
        assert_eq!(ControlError::Internal("i".into()).http_status(), 500);

        assert_eq!(response_status(ControlResponse::Ok), 200);
        assert_eq!(response_status(ControlResponse::BucketNotEmpty), 409);
        assert_eq!(response_status(ControlResponse::Conflict), 409);
        assert_eq!(response_status(ControlResponse::NotFound), 404);
        assert_eq!(response_status(ControlResponse::Invalid), 400);
    }
}
