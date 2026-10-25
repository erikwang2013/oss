//! M8 多节点控制面运行态（level-3）：`m8.runstate` 扩展的模型与编解码。
//!
//! 运行态（jobs / job_tasks / inventory_runs / inventory_cursors /
//! inventory_incremental / tier_restores / lifecycle_cursors / schedule_lease /
//! runstate_meta）不进 11 表 `ControlData`，而是作为 OSSCTL3 封套中的一个命名扩展：
//! `SnapshotExtension { version: 1, payload: json(RunState) }`。
//!
//! 编解码都是 fail-closed：解码先查 64 MiB 上限，再按 `deny_unknown_fields` 解析，
//! 最后校验版本（0 或高于本机支持版本一律拒绝）；编码超限直接报错，绝不截断。
//! 非空运行态要求 feature 3（[`RunState::required_feature_level`]），由封套组装方
//! 与 `data::encode_snapshot_with_extensions` 的 level-3 门共同把关。

pub mod model;
mod store;

use serde::{Deserialize, Serialize};

use crate::raft::data::DataError;

pub use model::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RunState, ScheduleLeaseRow, TierRestoreRow,
};
pub use store::{install_runstate_tx, read_runstate, read_runstate_tx};

/// OSSCTL3 扩展区中的命名空间（`[a-z0-9._-]{1,64}`）。
pub const RUNSTATE_EXTENSION: &str = "m8.runstate";
/// 本二进制理解/写出的扩展版本；加删行字段必须升版本。
pub const RUNSTATE_EXTENSION_VERSION: u32 = 1;
/// 扩展载荷硬上限（解码前先看长度，避免 JSON 放大）。
pub const RUNSTATE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeRef<'a> {
    version: u32,
    runstate: &'a RunState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    runstate: RunState,
}

/// 编码为 `m8.runstate` v1 载荷（确定性 JSON；超 64 MiB 报错）。
pub fn encode_extension(runstate: &RunState) -> Result<Vec<u8>, DataError> {
    let bytes = serde_json::to_vec(&EnvelopeRef {
        version: RUNSTATE_EXTENSION_VERSION,
        runstate,
    })?;
    check_size(bytes.len())?;
    Ok(bytes)
}

/// 解码 `m8.runstate` 载荷；`max_version` = 本机理解的最高版本（当前为 1）。
pub fn decode_extension(bytes: &[u8], max_version: u32) -> Result<RunState, DataError> {
    check_size(bytes.len())?;
    let envelope: Envelope = serde_json::from_slice(bytes)?;
    if envelope.version == 0 || envelope.version > max_version {
        return Err(DataError::Format(format!(
            "m8.runstate version {} unsupported (binary supports up to {max_version})",
            envelope.version
        )));
    }
    Ok(envelope.runstate)
}

fn check_size(len: usize) -> Result<(), DataError> {
    if len > RUNSTATE_MAX_BYTES {
        return Err(DataError::Format(format!(
            "m8.runstate payload {len} bytes exceeds cap {RUNSTATE_MAX_BYTES}"
        )));
    }
    Ok(())
}
