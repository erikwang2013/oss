//! M9 控制面能力开关与状态骨架。S0 只提供契约：Raft 运行时在 S5 接入。

/// 控制写（console/S3 配置写）是否已可走 Raft。S6 出口翻转。
pub const RAFT_READY: bool = false;
/// sync 桶创建/读写是否可用。S3 出口翻转。
pub const SYNC_READY: bool = false;
/// stretch 2×6 数据面（S4）已实现；控制面（Raft 桶创建/身份配对）就绪后随 S7/S9 翻转。
pub const STRETCH_READY: bool = false;

/// `GET /api/v1/raft/status` 的 S0 骨架（S5 补 leader/term/applied）。
/// capabilities 供 UI 置灰未就绪模式；`enabled` 在 S5 接上运行时前恒 false。
pub fn status_json() -> serde_json::Value {
    serde_json::json!({
        "enabled": false,
        "capabilities": {
            "raft": RAFT_READY,
            "sync": SYNC_READY,
            "stretch": STRETCH_READY,
        },
    })
}
