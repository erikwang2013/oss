//! M8 多节点控制面运行态（level-3）：`m8.runstate` 扩展的模型与编解码。
//!
//! 运行态（jobs / job_tasks / inventory_runs / inventory_cursors /
//! inventory_incremental / tier_restores / lifecycle_cursors / schedule_lease /
//! runstate_meta）不进 11 表 `ControlData`，而是作为 OSSCTL3 封套中的一个命名扩展：
//! `SnapshotExtension { version: 1, payload_b64: base64(json({ version: 1, runstate })) }`。
//! 扩展条目版本与载荷内版本必须一致（[`from_snapshot_extension`]）；生产端只经
//! [`to_snapshot_extension`] 用同一常量写出，消费者断言两者相等，不一致即拒绝。
//!
//! 编解码都是 fail-closed：解码先查 64 MiB 上限，再按 `deny_unknown_fields` 解析，
//! 最后校验版本（0 或高于本机支持版本一律拒绝）；编码超限直接报错，绝不截断。
//! 非空运行态要求 feature 3（[`RunState::required_feature_level`]），由封套组装方
//! 与 `data::encode_snapshot_with_extensions` 的 level-3 门共同把关。

pub mod model;
mod store;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use crate::raft::data::{
    ControlData, DataError, SnapshotExtension, SnapshotExtensions, decode_snapshot,
    encode_snapshot_with_extensions,
};

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

/// 编码 `SnapshotExtension`：条目版本与载荷版本同源（[`RUNSTATE_EXTENSION_VERSION`]）。
pub fn to_snapshot_extension(runstate: &RunState) -> Result<SnapshotExtension, DataError> {
    Ok(SnapshotExtension {
        version: RUNSTATE_EXTENSION_VERSION,
        payload_b64: BASE64.encode(encode_extension(runstate)?),
    })
}

/// 解码 `m8.runstate` 载荷；`max_version` = 本机理解的最高版本（当前为 1）。
pub fn decode_extension(bytes: &[u8], max_version: u32) -> Result<RunState, DataError> {
    Ok(decode_envelope(bytes, max_version)?.1)
}

/// 从 OSSCTL3 扩展条目解码，并断言条目版本 == 载荷版本（任一不匹配即 fail-closed）。
pub fn from_snapshot_extension(
    ext: &SnapshotExtension,
    max_version: u32,
) -> Result<RunState, DataError> {
    let bytes = BASE64
        .decode(&ext.payload_b64)
        .map_err(|e| DataError::Format(format!("m8.runstate payload base64: {e}")))?;
    let (payload_version, runstate) = decode_envelope(&bytes, max_version)?;
    if payload_version != ext.version {
        return Err(DataError::Format(format!(
            "m8.runstate version mismatch: extension {} vs payload {payload_version}",
            ext.version
        )));
    }
    Ok(runstate)
}

fn decode_envelope(bytes: &[u8], max_version: u32) -> Result<(u32, RunState), DataError> {
    check_size(bytes.len())?;
    let envelope: Envelope = serde_json::from_slice(bytes)?;
    if envelope.version == 0 || envelope.version > max_version {
        return Err(DataError::Format(format!(
            "m8.runstate version {} unsupported (binary supports up to {max_version})",
            envelope.version
        )));
    }
    Ok((envelope.version, envelope.runstate))
}

/// 编码带 `m8.runstate` 扩展的 OSSCTL3 快照；非空运行态要求 `feature_level >= 3`。
pub fn encode_runstate_snapshot(
    data: &ControlData,
    runstate: &RunState,
    feature_level: u8,
) -> Result<Vec<u8>, DataError> {
    let mut extensions = SnapshotExtensions::new();
    extensions.insert(RUNSTATE_EXTENSION.into(), to_snapshot_extension(runstate)?);
    encode_snapshot_with_extensions(data, feature_level, extensions)
}

/// 解码 OSSCTL3 快照中的运行态；无扩展 = 空运行态（低级别快照不携带本地运行态）。
pub fn decode_runstate_snapshot(bytes: &[u8]) -> Result<(ControlData, RunState), DataError> {
    let snapshot = decode_snapshot(bytes)?;
    if let Some(name) = snapshot
        .extensions
        .keys()
        .find(|k| k.as_str() != RUNSTATE_EXTENSION)
    {
        return Err(DataError::Format(format!(
            "runstate snapshot carries unknown extension {name:?}; refusing to drop it"
        )));
    }
    let runstate = match snapshot.extensions.get(RUNSTATE_EXTENSION) {
        Some(ext) => from_snapshot_extension(ext, RUNSTATE_EXTENSION_VERSION)?,
        None => RunState::default(),
    };
    Ok((snapshot.data, runstate))
}

fn check_size(len: usize) -> Result<(), DataError> {
    if len > RUNSTATE_MAX_BYTES {
        return Err(DataError::Format(format!(
            "m8.runstate payload {len} bytes exceeds cap {RUNSTATE_MAX_BYTES}"
        )));
    }
    Ok(())
}
