//! 对象根上的启动守卫：与 [`super::sql`] 的 `schema_meta` 守卫同语义，但只读
//! 对象根的 committed CAS（pin/事务），绝不构造 SQLite 连接池或状态机。
//!
//! 覆盖 R8 激活级别、`applied`/`membership` 可解码性与一致性、运行态/inventory
//! 激活标记（缺失时要求对应表为空且引用一致），以及最新文件快照的封套格式。

use std::collections::HashSet;

use openraft::{BasicNode, LogId, StoredMembership};
use xl_store::control_state::root::RootMeta;
use xl_store::control_store::ControlTxn;

use super::{
    FEATURE_LEVEL_4, FEATURE_LEVEL_5, META_INVENTORY_ACTIVATED, META_RUNSTATE_ACTIVATED,
    advertised_feature, guard_startup,
};
use crate::control_store_obj::{ObjectControlStore, WriteOrigin};
use crate::raft::data;
use crate::raft::file_snapshot::FileSnapshotStore;
use crate::raft::runstate::{self, RunState};

/// 全量 LogId（含 node id）marker；与 `object_sm` 写入的键一致。
const META_APPLIED_LOG_ID: &str = "raft_applied_log_id";
/// `runstate_meta` 在对象根 markers 里的前缀；与 `control_store_obj::jobs` 一致。
const RUNSTATE_META_PREFIX: &str = "@runstate/";

/// 对象后端 spawn 前的一次性守卫。`configured` = `[raft].feature_level`。
pub async fn guard_object_startup(
    control: &ObjectControlStore,
    snapshots: &FileSnapshotStore,
    configured: u8,
) -> Result<(), String> {
    let pinned = control
        .pin()
        .await
        .map_err(|e| format!("读取对象控制根失败: {e}"))?;
    let meta = &pinned.root.meta;
    let activation = u8::try_from(meta.feature_level)
        .map_err(|_| format!("对象根 feature_level={} 超出 u8 范围", meta.feature_level))?;
    guard_startup(activation, configured)?;
    guard_applied_membership(meta)?;
    let mut txn = control
        .begin(WriteOrigin::local("raft-startup-guard"))
        .await
        .map_err(|e| format!("读取对象根运行态失败: {e}"))?;
    let state = runstate::read_runstate_txn(&mut txn as &mut dyn ControlTxn)
        .await
        .map_err(|e| format!("读取对象根运行态失败: {e}"))?;
    drop(txn);
    guard_runstate_activation(meta, &state, configured)?;
    guard_inventory_activation(meta, &state, configured)?;
    guard_latest_snapshot(snapshots, configured).await
}

/// `applied` 的完整 LogId marker 与 `RootMeta::applied` 必须指向同一条日志；
/// `membership_json` 必须是 openraft 可解码的成员。损坏对象根在打开 Raft 前拒绝。
fn guard_applied_membership(meta: &RootMeta) -> Result<(), String> {
    if let Some(raw) = meta.markers.get(META_APPLIED_LOG_ID) {
        let log: LogId<u64> = serde_json::from_str(raw)
            .map_err(|e| format!("对象根 {META_APPLIED_LOG_ID} 非法: {e}"))?;
        if let Some(applied) = meta.applied
            && (log.leader_id.term, log.index) != (applied.term, applied.index)
        {
            return Err(format!(
                "对象根 {META_APPLIED_LOG_ID}={log:?} 与 applied(term={}, index={}) 不一致",
                applied.term, applied.index
            ));
        }
    }
    if let Some(raw) = &meta.membership_json {
        serde_json::from_str::<StoredMembership<u64, BasicNode>>(raw)
            .map_err(|e| format!("对象根 membership_json 非法: {e}"))?;
    }
    Ok(())
}

fn marker(meta: &RootMeta, key: &str) -> Result<bool, String> {
    match meta.markers.get(key).map(String::as_str) {
        None => Ok(false),
        Some("1") => Ok(true),
        Some(other) => Err(format!("对象根 marker {key}={other:?} 非法（只认 \"1\"）")),
    }
}

/// feature 4：缺 `m8_runstate_activated` 时六张运行态表必须为空且无悬空 job 引用。
/// 对象根没有「节点本地行」的概念；非空而未激活说明根来自旧/被改写的状态，拒绝启动。
fn guard_runstate_activation(
    meta: &RootMeta,
    state: &RunState,
    configured: u8,
) -> Result<(), String> {
    if advertised_feature(configured) < FEATURE_LEVEL_4 || marker(meta, META_RUNSTATE_ACTIVATED)? {
        return Ok(());
    }
    let job_ids: HashSet<i64> = state.jobs.iter().map(|j| j.id).collect();
    let inv_dangling = state
        .inventory_runs
        .iter()
        .filter(|r| r.job_id.is_some_and(|id| !job_ids.contains(&id)))
        .count();
    let tier_dangling = state
        .tier_restores
        .iter()
        .filter(|r| r.job_id.is_some_and(|id| !job_ids.contains(&id)))
        .count();
    let empty = state.jobs.is_empty()
        && state.job_tasks.is_empty()
        && state.inventory_runs.is_empty()
        && state.inventory_cursors.is_empty()
        && state.inventory_incremental.is_empty()
        && state.tier_restores.is_empty();
    if empty && inv_dangling == 0 && tier_dangling == 0 {
        return Ok(());
    }
    Err(format!(
        "[raft].feature_level={configured} 要求 feature 4 运行态已激活，但对象根未见 \
         marker {META_RUNSTATE_ACTIVATED} 且运行态非空：jobs={}、job_tasks={}、\
         inventory_runs={}、inventory_cursors={}、inventory_incremental={}、\
         tier_restores={}；悬空引用：inventory_runs.job_id={inv_dangling}、\
         tier_restores.job_id={tier_dangling}。对象根的全部行都应来自日志/快照；\
         请用 control-migrate 从带 marker 的 SQLite 源重灌，或在 feature_level<4 下启动并清理。",
        state.jobs.len(),
        state.job_tasks.len(),
        state.inventory_runs.len(),
        state.inventory_cursors.len(),
        state.inventory_incremental.len(),
        state.tier_restores.len(),
    ))
}

/// feature 5：缺 `m8_inventory_activated` 时三张 inventory 表必须为空、自然键唯一、
/// `next_run_id` 高于现有最大 id。对象根不保留节点本地 inventory 行，因此不清理。
fn guard_inventory_activation(
    meta: &RootMeta,
    state: &RunState,
    configured: u8,
) -> Result<(), String> {
    if advertised_feature(configured) < FEATURE_LEVEL_5 || marker(meta, META_INVENTORY_ACTIVATED)? {
        return Ok(());
    }
    let mut keys = HashSet::new();
    let duplicates = state
        .inventory_runs
        .iter()
        .filter(|r| !keys.insert((r.bucket.as_str(), r.config_id.as_str(), r.run_key.as_str())))
        .count();
    let max_run_id = state.inventory_runs.iter().map(|r| r.id).max().unwrap_or(0);
    let next_run_id = match meta
        .markers
        .get(&format!("{RUNSTATE_META_PREFIX}next_run_id"))
    {
        None => None,
        Some(raw) => Some(
            raw.parse::<i64>()
                .map_err(|_| format!("对象根 runstate_meta.next_run_id={raw:?} 非法"))?,
        ),
    };
    let next_ok = match next_run_id {
        None => max_run_id == 0,
        Some(next) => next > max_run_id,
    };
    if state.inventory_runs.is_empty()
        && state.inventory_cursors.is_empty()
        && state.inventory_incremental.is_empty()
        && duplicates == 0
        && next_ok
    {
        return Ok(());
    }
    Err(format!(
        "[raft].feature_level={configured} 要求 feature 5 inventory 运行态已激活，但对象根\
         未见 marker {META_INVENTORY_ACTIVATED} 且 inventory 表含非日志行/不一致：\
         inventory_runs={}、inventory_cursors={}、inventory_incremental={}、\
         重复 (bucket,config_id,run_key)={duplicates}、next_run_id={next_run_id:?}\
         （须 > MAX(id)={max_run_id}）。请用 control-migrate 从带 marker 的 SQLite 源重灌，\
         或在 feature_level<5 下启动并清理。",
        state.inventory_runs.len(),
        state.inventory_cursors.len(),
        state.inventory_incremental.len(),
    ))
}

/// 最新文件快照必须是本二进制可解码的 OSSCTL1/2/3 控制面快照，且其声明的
/// feature level 不高于本机配置（与安装路径同一 fail-closed 口径）。
async fn guard_latest_snapshot(
    snapshots: &FileSnapshotStore,
    configured: u8,
) -> Result<(), String> {
    let Some(snapshot) = snapshots
        .latest()
        .await
        .map_err(|e| format!("读取对象 Raft 最新快照失败: {e}"))?
    else {
        return Ok(());
    };
    let bytes = snapshot.snapshot.get_ref();
    if !data::is_control_snapshot(bytes) {
        return Err(format!(
            "对象 Raft 最新快照 {:?} 不是控制面快照（OSSCTL1/2/3）；拒绝启动",
            snapshot.meta.snapshot_id
        ));
    }
    let decoded =
        data::decode_snapshot(bytes).map_err(|e| format!("对象 Raft 最新快照封套非法: {e}"))?;
    guard_startup(decoded.feature_level, configured).map_err(|e| format!("对象 Raft 最新快照：{e}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use openraft::SnapshotMeta;
    use xl_store::control_state::root::{AppliedLog, Mode, RootMeta};

    use super::*;
    use crate::control_store_obj::RootIo;
    use crate::raft::data::{self, ControlData};

    fn meta(feature_level: u16, markers: &[(&str, &str)]) -> RootMeta {
        RootMeta {
            mode: Mode::Raft,
            markers: markers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
            next_uid: 1,
            next_job_id: 1,
            next_run_id: 1,
            next_site_seq: 1,
            applied: None,
            membership_json: None,
            feature_level,
            created_at: 100,
        }
    }

    fn job(id: i64) -> crate::raft::runstate::JobRow {
        crate::raft::runstate::JobRow {
            id,
            ..Default::default()
        }
    }

    #[test]
    fn runstate_marker_missing_rejects_non_log_rows_and_dangling_refs() {
        let mut state = RunState {
            jobs: vec![job(7)],
            ..Default::default()
        };
        let err = guard_runstate_activation(&meta(4, &[]), &state, 4).unwrap_err();
        assert!(
            err.contains("jobs=1") && err.contains(META_RUNSTATE_ACTIVATED),
            "{err}"
        );
        // 空表 + 无 marker 放行（不提前写 marker）。
        state.jobs.clear();
        assert!(guard_runstate_activation(&meta(4, &[]), &state, 4).is_ok());
        // 只有悬空 inventory 引用同样拒绝。
        state
            .inventory_runs
            .push(crate::raft::runstate::InventoryRunRow {
                id: 1,
                job_id: Some(99),
                ..Default::default()
            });
        let err = guard_runstate_activation(&meta(4, &[]), &state, 4).unwrap_err();
        assert!(err.contains("inventory_runs.job_id=1"), "{err}");
        // marker = "1" 时非空也放行（日志/快照/迁移写的激活事实）。
        assert!(
            guard_runstate_activation(&meta(4, &[(META_RUNSTATE_ACTIVATED, "1")]), &state, 4)
                .is_ok()
        );
        // 非法 marker 值 fail-closed。
        let err = guard_runstate_activation(&meta(4, &[(META_RUNSTATE_ACTIVATED, "2")]), &state, 4)
            .unwrap_err();
        assert!(err.contains("非法"), "{err}");
    }

    #[test]
    fn inventory_marker_missing_checks_rows_duplicates_and_next_run_id() {
        let mut state = RunState::default();
        assert!(guard_inventory_activation(&meta(5, &[]), &state, 5).is_ok());
        state
            .inventory_runs
            .push(crate::raft::runstate::InventoryRunRow {
                id: 3,
                bucket: "b".into(),
                config_id: "c".into(),
                run_key: "r".into(),
                ..Default::default()
            });
        let err = guard_inventory_activation(&meta(5, &[]), &state, 5).unwrap_err();
        assert!(
            err.contains("inventory_runs=1") && err.contains("next_run_id"),
            "{err}"
        );
        // 带 marker 放行；marker 与 runstate marker 独立。
        assert!(
            guard_inventory_activation(&meta(5, &[(META_INVENTORY_ACTIVATED, "1")]), &state, 5)
                .is_ok()
        );
        // 低配置不启用 inventory 复制，不检查也不写 marker。
        assert!(guard_inventory_activation(&meta(4, &[]), &state, 4).is_ok());
        // next_run_id 必须严格大于 MAX(id)。
        let markers = [("@runstate/next_run_id", "3")];
        let err = guard_inventory_activation(&meta(5, &markers), &state, 5).unwrap_err();
        assert!(err.contains("next_run_id"), "{err}");
    }

    #[test]
    fn applied_membership_marker_must_match_root_metadata() {
        let mut m = meta(
            1,
            &[(
                META_APPLIED_LOG_ID,
                "{\"leader_id\":{\"term\":2,\"node_id\":1},\"index\":9}",
            )],
        );
        m.applied = Some(AppliedLog { term: 2, index: 8 });
        let err = guard_applied_membership(&m).unwrap_err();
        assert!(err.contains("不一致"), "{err}");
        m.applied = Some(AppliedLog { term: 2, index: 9 });
        assert!(guard_applied_membership(&m).is_ok());
        m.membership_json = Some("not-json".into());
        let err = guard_applied_membership(&m).unwrap_err();
        assert!(err.contains("membership_json 非法"), "{err}");
    }

    /// 完整入口只读对象根：根 feature 高于配置、marker 非法都在打开状态机前拒绝。
    #[tokio::test]
    async fn startup_guard_reads_object_root_and_rejects_inconsistent_markers() {
        use std::sync::Arc;

        use xl_store::store::{Store, StoreConfig};

        let dir = tempfile::tempdir().unwrap();
        let mut store_cfg = StoreConfig::new(dir.path());
        store_cfg.fsync = false;
        let store = Arc::new(Store::open(store_cfg).await.unwrap());
        let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(store))));
        control.bootstrap(Mode::Raft, 100).await.unwrap();
        let snapshots = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
        guard_object_startup(&control, &snapshots, 1).await.unwrap();

        control
            .transaction(WriteOrigin::local("test-startup-guard"), |txn| {
                Box::pin(async move {
                    txn.meta_mut().feature_level = 5;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let err = guard_object_startup(&control, &snapshots, 4)
            .await
            .unwrap_err();
        assert!(
            err.contains("raft_feature_level=5") && err.contains("feature_level=4"),
            "{err}"
        );

        control
            .transaction(WriteOrigin::local("test-startup-guard"), |txn| {
                Box::pin(async move {
                    txn.meta_mut().feature_level = 4;
                    txn.set_marker(META_RUNSTATE_ACTIVATED, "2");
                    Ok(())
                })
            })
            .await
            .unwrap();
        let err = guard_object_startup(&control, &snapshots, 4)
            .await
            .unwrap_err();
        assert!(err.contains("非法"), "{err}");
    }

    #[tokio::test]
    async fn latest_snapshot_format_and_feature_are_gated() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
        // 无快照：放行。
        assert!(guard_latest_snapshot(&snapshots, 1).await.is_ok());
        let snapshot_meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "1".into(),
        };
        // 非控制面封套：拒绝。
        snapshots
            .write(&snapshot_meta, b"not-a-control-snapshot")
            .await
            .unwrap();
        let err = guard_latest_snapshot(&snapshots, 1).await.unwrap_err();
        assert!(err.contains("不是控制面快照"), "{err}");
        // 高 feature 快照不能用低配置启动。
        let bytes = data::encode_snapshot_at(&ControlData::default(), 5).unwrap();
        snapshots.write(&snapshot_meta, &bytes).await.unwrap();
        let err = guard_latest_snapshot(&snapshots, 4).await.unwrap_err();
        assert!(err.contains("feature_level=5"), "{err}");
        assert!(guard_latest_snapshot(&snapshots, 5).await.is_ok());
    }
}
