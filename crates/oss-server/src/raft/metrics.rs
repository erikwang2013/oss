//! M9 post-release 观测计数：状态机与 Raft 句柄共享一份 `Arc`，`/metrics` 直读。
//!
//! 全部 `Relaxed`：单调计数，跨字段无顺序要求。

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    /// leader 变更次数（含首次当选；由 `spawn_with_control` 的采样任务累加）。
    pub leader_changes_total: AtomicU64,
    /// apply 批失败次数（openraft fail-stop 前每次失败 +1）。
    pub apply_errors_total: AtomicU64,
    /// follower 把控制写单跳转发到 leader 的次数。
    pub forward_total: AtomicU64,
}

impl Metrics {
    pub fn leader_changes(&self) -> u64 {
        self.leader_changes_total.load(Ordering::Relaxed)
    }

    pub fn apply_errors(&self) -> u64 {
        self.apply_errors_total.load(Ordering::Relaxed)
    }

    pub fn forwards(&self) -> u64 {
        self.forward_total.load(Ordering::Relaxed)
    }
}
