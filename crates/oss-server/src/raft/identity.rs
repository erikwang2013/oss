//! S7.1：Raft 节点身份持久化（对象根 markers）。
//!
//! 首启写种子，后续启动逐项比对；任何一项变化都拒绝启动（节点 id/角色/站点身份不可漂移）。
//! 集群令牌**不落身份**（ruling 7：泄漏面，配对另走 site token）。

use std::sync::Arc;

use crate::config::RaftRole;
use crate::control_store_obj::{ObjectControlStore, WriteOrigin};

use super::RaftSettings;
use super::file_log::FileLogStore;

pub const META_NODE_ID: &str = "raft_node_id";
pub const META_ROLE: &str = "raft_role";
pub const META_BOOTSTRAP: &str = "raft_bootstrap";
pub const META_ADVERTISE: &str = "raft_advertise_endpoint";
pub const META_SITE_ID: &str = "raft_site_id";
pub const META_INITIALIZED: &str = "raft_initialized";

async fn seed_or_check_object(
    control: &Arc<ObjectControlStore>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let pinned = control
        .pin()
        .await
        .map_err(|e| format!("读取对象控制根失败: {e}"))?;
    match pinned.root.meta.markers.get(key) {
        Some(old) if old != value => Err(format!(
            "Raft 身份 {key} 已持久化为 {old:?}，配置为 {value:?}：节点身份不可变更\
             （丢失/迁移见 raft-force-recover runbook）"
        )),
        Some(_) => Ok(()),
        None => {
            let txn_key = key.to_string();
            let value = value.to_string();
            control
                .transaction(WriteOrigin::local("raft-identity"), move |txn| {
                    let key = txn_key.clone();
                    let value = value.clone();
                    Box::pin(async move {
                        if !txn.meta().markers.contains_key(&key) {
                            txn.set_marker(key, value);
                        }
                        Ok(())
                    })
                })
                .await
                .map_err(|e| format!("写入对象 Raft 身份 {key} 失败: {e}"))
        }
    }
}

/// 对象后端 spawn 前：在根 markers 中种子/校验身份，并以根 applied/membership 与文件日志
/// 判定已初始化节点是否状态丢失（对应 SQLite 的 [`verify_and_seed`]）。
pub async fn verify_and_seed_object(
    control: &Arc<ObjectControlStore>,
    settings: &RaftSettings,
) -> Result<(), String> {
    seed_or_check_object(control, META_NODE_ID, &settings.node_id.to_string()).await?;
    seed_or_check_object(
        control,
        META_ROLE,
        match settings.role {
            RaftRole::Voter => "voter",
            RaftRole::Witness => "witness",
        },
    )
    .await?;
    seed_or_check_object(
        control,
        META_BOOTSTRAP,
        if settings.bootstrap { "1" } else { "0" },
    )
    .await?;
    seed_or_check_object(
        control,
        META_ADVERTISE,
        settings.advertise_endpoint.trim_end_matches('/'),
    )
    .await?;
    if settings.role == RaftRole::Voter {
        let site = xl_store::control_store::sites::SiteStore::site_identity(&**control)
            .await
            .map_err(|e| format!("读取对象 site_config 失败: {e}"))?;
        if let Some(site) = site {
            seed_or_check_object(control, META_SITE_ID, &site.id).await?;
        }
    }
    let initialized = control
        .pin()
        .await
        .map_err(|e| format!("读取对象控制根失败: {e}"))?
        .root
        .meta
        .markers
        .get(META_INITIALIZED)
        .is_some_and(|v| v == "1");
    if initialized {
        let logs = FileLogStore::open(&settings.db_path).await.map_err(|e| {
            format!(
                "打开文件 Raft 日志失败（{}）: {e}",
                settings.db_path.display()
            )
        })?;
        let has_logs = logs.has_logs().await.map_err(|e| e.to_string())?;
        if !has_logs && membership_empty_object(control).await? {
            return Err(format!(
                "raft_initialized=1 但 {} 无日志且根成员为空：控制面状态已丢失；\
                 使用 raft-force-recover 重建单节点集群（见 runbook）",
                settings.db_path.display()
            ));
        }
    }
    Ok(())
}

async fn membership_empty_object(control: &Arc<ObjectControlStore>) -> Result<bool, String> {
    let pinned = control
        .pin()
        .await
        .map_err(|e| format!("读取对象控制根失败: {e}"))?;
    let Some(raw) = &pinned.root.meta.membership_json else {
        return Ok(true);
    };
    match serde_json::from_str::<openraft::StoredMembership<u64, openraft::BasicNode>>(raw) {
        Ok(m) => Ok(m.voter_ids().next().is_none()),
        Err(_) => Ok(false),
    }
}

/// 对象后端 spawn 后：同步根 `raft_initialized` marker（语义同 [`record_initialized`]）。
pub async fn record_initialized_object(
    control: &Arc<ObjectControlStore>,
    initialized: bool,
) -> Result<(), String> {
    let current = control
        .pin()
        .await
        .map_err(|e| format!("读取对象控制根失败: {e}"))?
        .root
        .meta
        .markers
        .get(META_INITIALIZED)
        .cloned();
    let next = match current.as_deref() {
        None => {
            if initialized {
                "1"
            } else {
                "0"
            }
        }
        Some("0") if initialized => "1",
        _ => return Ok(()),
    }
    .to_string();
    control
        .transaction(WriteOrigin::local("raft-identity"), move |txn| {
            let next = next.clone();
            Box::pin(async move {
                txn.set_marker(META_INITIALIZED, next);
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("写入对象 raft_initialized 失败: {e}"))
}

#[cfg(test)]
mod tests {
    use xl_store::control_state::root::Mode;
    use xl_store::store::{Store, StoreConfig};

    use super::*;
    use crate::control_store_obj::RootIo;

    async fn object_control(dir: &std::path::Path) -> Arc<ObjectControlStore> {
        let mut cfg = StoreConfig::new(dir);
        cfg.fsync = false;
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(store))));
        control.bootstrap(Mode::Raft, 100).await.unwrap();
        control
    }

    async fn marker(control: &Arc<ObjectControlStore>) -> Option<String> {
        control
            .pin()
            .await
            .unwrap()
            .root
            .meta
            .markers
            .get(META_INITIALIZED)
            .cloned()
    }

    /// 对象+raft 的初始化标志必须落在对象根 markers，standalone 占位池不参与。
    #[tokio::test]
    async fn record_initialized_object_writes_root_marker_and_never_downgrades() {
        let dir = tempfile::tempdir().unwrap();
        let control = object_control(dir.path()).await;
        record_initialized_object(&control, false).await.unwrap();
        assert_eq!(marker(&control).await.as_deref(), Some("0"));
        record_initialized_object(&control, true).await.unwrap();
        assert_eq!(marker(&control).await.as_deref(), Some("1"));
        // 已初始化的节点重启时成员可能尚未载入：不得把 1 降回 0。
        record_initialized_object(&control, false).await.unwrap();
        assert_eq!(marker(&control).await.as_deref(), Some("1"));
    }
}
