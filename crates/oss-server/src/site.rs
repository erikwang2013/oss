//! M7：站点控制面 changelog 应用（对端条目 → 本端控制面）与出货 worker（切片 1b）。
//!
//! 冲突模型：LWW 元组 `(ts, priority, origin_site_id, seq)`（ts 优先，priority 同秒裁决）。
//! 毒丸：永久坏条目写 `site_apply_dead` 并越过（cursor 可推进）；DB/IO 错误整批 500 让对端重试。

use std::sync::Arc;

use serde::Deserialize;

use crate::state::AppState;

mod object;

const MAX_ENTRIES: usize = 500;
/// 单条 payload 上限（策略 64KiB 经一层 JSON 转义后仍可容纳——L5）。
const MAX_PAYLOAD: usize = 256 * 1024;
const MAX_DEAD_PAYLOAD: usize = 4096;
/// 单批线上体积预算（与 30s 控制面超时匹配：1MiB 在 ~300kbps 链路也可完成，
/// 避免超大策略批在慢链路上被超时永久卡住——L5/低-5）。
const MAX_BATCH_BYTES: usize = 1024 * 1024;
const CLOCK_SKEW_SECS: i64 = 300;

#[derive(Debug, Clone, Deserialize)]
pub struct Entry {
    pub seq: i64,
    pub origin_site_id: String,
    pub entity: String,
    pub entity_id: String,
    pub op: String,
    pub payload: serde_json::Value,
    pub priority: i64,
    pub ts: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ApplyStats {
    pub applied: usize,
    pub skipped: usize,
    pub dead: usize,
}

fn bad(msg: impl Into<String>) -> String {
    msg.into()
}

/// LWW 裁决：`(ts, priority, origin_site_id, seq)` 严格大于本地 clock 才应用。
fn wins(e: &Entry, clock: Option<(i64, i64, String, i64)>) -> bool {
    match clock {
        None => true,
        Some(c) => (e.ts, e.priority, e.origin_site_id.clone(), e.seq) > c,
    }
}

/// 应用一批来自 peer 的 changelog 条目。错误 = 整批 500（重试幂等）。
pub async fn apply_changelog(state: &AppState, entries: Vec<Entry>) -> Result<ApplyStats, String> {
    let Some(object) = state.db.object_control() else {
        return Err(bad("对象控制面未装配"));
    };
    object::apply_changelog(
        &object.control,
        &state.store,
        &state.site.id,
        state.bucket_cleanup.as_ref(),
        &entries,
    )
    .await
}

/// `POST /sites` 的持久化：sites 行 + cursor + 控制面快照**同一事务**（P1）。
/// 快照只包含「本地 clock 无行或 origin=本站」的实体（不回显远端值）。
pub async fn insert_peer_with_snapshot(
    state: &AppState,
    peer: &crate::db::SitePeer,
) -> Result<(), String> {
    let Some(object) = state.db.object_control() else {
        return Err(bad("对象控制面未装配"));
    };
    object::insert_peer_with_snapshot(&object.control, state.control.is_raft(), peer).await
}

/// 一轮出货：逐 peer 推 cursor 之后的 changelog（≤100 条/批）。
pub async fn ship_once(db: &crate::db::Db) -> Result<(usize, usize), String> {
    ship_once_tls(db, None).await
}

/// 同 [`ship_once`]；`tls = Some` 时对端地址按 peer TLS 模式升级/校验。
pub async fn ship_once_tls(
    db: &crate::db::Db,
    tls: Option<&crate::tls::Tls>,
) -> Result<(usize, usize), String> {
    let Some(object) = db.object_control() else {
        return Err(bad("对象控制面未装配"));
    };
    object::ship_once(&object.control, tls).await
}

/// serve 挂载（`OSS_SITE_INTERVAL_SECS` 默认 1s；0=关）。
pub fn spawn_site_worker(state: AppState, tls: Arc<crate::tls::Tls>) {
    let secs: u64 = std::env::var("OSS_SITE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if secs == 0 {
        tracing::info!("站点 changelog worker 已关闭（OSS_SITE_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let _ = ship_once_tls(&state.db, Some(&tls)).await;
        }
    });
}
