//! 控制面导出文件：读取 11 张复制表，生成带校验和的 JSON（只读；不重放 schema/模式）。
//!
//! 格式 2 起可选携带 level-3 运行态（checksum 覆盖两者）；格式 1 旧文件仍可读。
//! 封套带来源 provenance（`snapshot_format`/`wire_version`/`feature_level`/
//! `activation_level`/`source_build`）。
//! [`verify_export`] 在 cutover（含 dry-run）与 force-recover 动数据之前拒绝本机无法
//! 解码/无法应用来源导出。LC-0 之前写出的旧导出缺这些字段：载荷仍按冻结的
//! `ControlData` 完整解码，按「未验证」接受并发警告（不静默放行伪造的显式字段）。

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::db::Db;
use crate::raft::compat;
use crate::raft::data::{self, ControlData};
use crate::raft::runstate::{self, RunState};

/// 本二进制写出的导出格式：2 起可选携带 `runstate`（OSSCTL3 来源）。
pub const EXPORT_FORMAT: u32 = 2;
/// 仍接受读取的最旧导出格式（1 = 无运行态）。
pub const MIN_EXPORT_FORMAT: u32 = 1;
pub const EXPORT_KIND: &str = "oss-raft-control-export";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFile {
    pub format: u32,
    pub kind: String,
    pub source_site: String,
    pub source_node_id: Option<u64>,
    pub source_role: String,
    pub version: String,
    /// 导出时来源节点的激活级别（`schema_meta.raft_feature_level`，缺省 1）；
    /// 来源数据自身要求的级别更高时取 max（与 `encode_snapshot_at` 一致）。
    #[serde(default)]
    pub feature_level: Option<u8>,
    /// 导出时来源节点的激活标记原值（不经数据要求取 max）。v1.16 及更早的封套没有
    /// 本字段：恢复时回落到 `feature_level`；激活过但无 level-2 数据的集群靠它保持 2。
    #[serde(default)]
    pub activation_level: Option<u8>,
    /// 导出时来源二进制会写出的快照封套号：1=OSSCTL1，2=OSSCTL2，3=OSSCTL3。
    #[serde(default)]
    pub snapshot_format: Option<u32>,
    /// 来源控制面 wire 版本（[`compat::WIRE_VERSION`]）。
    #[serde(default)]
    pub wire_version: Option<u32>,
    /// 构建标识（`OSS_BUILD_ID`，无则回落包版本）；纯诊断，不参与闸门。
    #[serde(default)]
    pub source_build: Option<String>,
    pub admin_username: Option<String>,
    pub has_vault: Option<bool>,
    pub exported_at: i64,
    pub sha256: String,
    /// 来源激活 level 3 时的运行态（format 2 起）。低于 3 不导出：那是节点本地状态。
    #[serde(default)]
    pub runstate: Option<RunState>,
    pub data: ControlData,
}

/// 读取 11 张复制表并生成带校验和的导出文件（只读；不重放 schema/模式）。
pub async fn build_export(
    db: &Db,
    source_site: String,
    source_node_id: Option<u64>,
    source_role: &str,
    admin_username: Option<String>,
    has_vault: Option<bool>,
) -> Result<ExportFile, String> {
    // 在线导出也必须在一个读事务里完成，避免并发控制写把 11 表导出成撕裂组合。
    let mut tx = db.pool().begin().await.map_err(|e| format!("begin: {e}"))?;
    let data = data::read_tx(&mut tx)
        .await
        .map_err(|e| format!("read control data: {e}"))?;
    let marker = compat::read_activation_conn(&mut tx).await?;
    // level 3：运行态必须与 11 表在同一次读事务里取（不能被并发 apply 撕裂）。
    let runstate = if marker >= compat::FEATURE_LEVEL_3 {
        Some(
            runstate::read_runstate_tx(&mut tx)
                .await
                .map_err(|e| format!("read runstate: {e}"))?,
        )
    } else {
        None
    };
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    let payload = export_payload(&data, runstate.as_ref())?;
    let sha256 = hex::encode(Sha256::digest(&payload));
    // 来源二进制写快照时取 max(marker, 数据要求)；封套号与实际编码共用 data.rs 选择器。
    let required = data::required_feature_level(&data);
    let feature_level = marker.max(required);
    let snapshot_format = data::snapshot_format_for(feature_level, required);
    Ok(ExportFile {
        format: EXPORT_FORMAT,
        kind: EXPORT_KIND.to_string(),
        source_site,
        source_node_id,
        source_role: source_role.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        feature_level: Some(feature_level),
        activation_level: Some(marker),
        snapshot_format: Some(snapshot_format),
        wire_version: Some(compat::WIRE_VERSION),
        source_build: Some(
            option_env!("OSS_BUILD_ID")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_string(),
        ),
        admin_username,
        has_vault,
        exported_at: crate::db::now(),
        sha256,
        runstate,
        data,
    })
}

#[derive(Serialize)]
struct ExportPayload<'a> {
    data: &'a ControlData,
    runstate: &'a RunState,
}

/// checksum 载荷：带运行态时两者一起哈希；旧格式（无运行态）保持只哈希 data，
/// 这样 v1 冻结 fixture 与旧文件的 sha256 不变。
fn export_payload(data: &ControlData, runstate: Option<&RunState>) -> Result<Vec<u8>, String> {
    match runstate {
        Some(runstate) => serde_json::to_vec(&ExportPayload { data, runstate }),
        None => serde_json::to_vec(data),
    }
    .map_err(|e| e.to_string())
}

/// 校验封套、校验和与来源兼容性；返回警告（当前仅缺 provenance 的旧导出）。
///
/// 任何显式 provenance 字段都必须通过闸门；缺失字段（LC-0 之前的导出）按未验证
/// 接受：载荷已由冻结的 `ControlData`（全部 `deny_unknown_fields`）完整解码，
/// 且数据自身要求的级别仍受本二进制上限约束，不存在静默丢行/丢字段的路径。
pub fn verify_export(export: &ExportFile) -> Result<Vec<String>, String> {
    if !(MIN_EXPORT_FORMAT..=EXPORT_FORMAT).contains(&export.format) || export.kind != EXPORT_KIND {
        return Err(format!(
            "unsupported export format/kind: {}/{} (this binary reads format {}..={})",
            export.format, export.kind, MIN_EXPORT_FORMAT, EXPORT_FORMAT
        ));
    }
    if export.format == MIN_EXPORT_FORMAT && export.runstate.is_some() {
        return Err(format!(
            "export format {MIN_EXPORT_FORMAT} (pre-runstate) cannot carry runstate"
        ));
    }
    let payload = export_payload(&export.data, export.runstate.as_ref())?;
    let want = hex::encode(Sha256::digest(&payload));
    if want != export.sha256 {
        return Err("export checksum mismatch".into());
    }
    verify_provenance(export)
}

/// 运行态只随 OSSCTL3/level-3 导出；低于 3 或 provenance 不一致一律拒绝。
fn verify_runstate(export: &ExportFile) -> Result<(), String> {
    let Some(runstate) = &export.runstate else {
        return Ok(());
    };
    if export.snapshot_format != Some(3) {
        return Err(format!(
            "runstate requires OSSCTL3 snapshot_format 3, export declares {:?}",
            export.snapshot_format
        ));
    }
    match export.feature_level {
        Some(level) if level >= compat::FEATURE_LEVEL_3 => {}
        Some(level) => {
            return Err(format!(
                "runstate requires feature_level >= {}, export declares {level}",
                compat::FEATURE_LEVEL_3
            ));
        }
        None => return Err("runstate requires feature_level provenance >= 3".into()),
    }
    runstate
        .validate()
        .map_err(|e| format!("invalid runstate in export: {e}"))
}

fn verify_provenance(export: &ExportFile) -> Result<Vec<String>, String> {
    verify_runstate(export)?;
    let required = data::required_feature_level(&export.data);
    if let Some(format) = export.snapshot_format {
        if !(1..=data::SNAPSHOT_FORMAT_MAX).contains(&format) {
            return Err(format!(
                "unsupported source snapshot_format {format}: this binary decodes 1..={}; \
                 upgrade oss-server before importing",
                data::SNAPSHOT_FORMAT_MAX
            ));
        }
        // OSSCTL1 没有 feature_level/结构化生命周期，携带 level-2 数据只能是伪造或损坏。
        if format == 1 && required > compat::MIN_FEATURE_LEVEL {
            return Err(format!(
                "source snapshot_format 1 (OSSCTL1) cannot carry level-{required} data"
            ));
        }
        // OSSCTL3 只在 level-3 激活下写出；低级别或缺级别声明格式 3 只能是伪造。
        if format == 3
            && !export
                .feature_level
                .is_some_and(|level| level >= compat::FEATURE_LEVEL_3)
        {
            return Err("source snapshot_format 3 (OSSCTL3) requires feature_level >= 3".into());
        }
    }
    if let Some(wire) = export.wire_version
        && !(compat::MIN_COMPAT_WIRE_VERSION..=compat::WIRE_VERSION).contains(&wire)
    {
        return Err(format!(
            "unsupported source wire_version {wire}: this binary reads {}..={}",
            compat::MIN_COMPAT_WIRE_VERSION,
            compat::WIRE_VERSION
        ));
    }
    if let Some(level) = export.feature_level {
        if !(compat::MIN_FEATURE_LEVEL..=compat::MAX_FEATURE_LEVEL).contains(&level) {
            return Err(format!(
                "source feature_level {level} outside supported {}..={}: the source cluster \
                 activated features this binary cannot apply",
                compat::MIN_FEATURE_LEVEL,
                compat::MAX_FEATURE_LEVEL
            ));
        }
        if level < required {
            return Err(format!(
                "source feature_level {level} is below the level-{required} data in the export"
            ));
        }
        // OSSCTL1 封套没有 feature/activation 概念：格式 1 带 level>1 只能是伪造或损坏。
        if export.snapshot_format == Some(1) && level > compat::MIN_FEATURE_LEVEL {
            return Err(format!(
                "source snapshot_format 1 (OSSCTL1) cannot record feature_level {level}"
            ));
        }
    }
    if let Some(activation) = export.activation_level {
        if !(compat::MIN_FEATURE_LEVEL..=compat::MAX_FEATURE_LEVEL).contains(&activation) {
            return Err(format!(
                "source activation_level {activation} outside supported {}..={}",
                compat::MIN_FEATURE_LEVEL,
                compat::MAX_FEATURE_LEVEL
            ));
        }
        // activation < 数据要求是「标记滞后」而非越权：安装级别取 max(activation, required)
        // （保守升到数据要求），不拒绝——build_export 对这类库仍须产出可自校验的导
        // 出（来源快照按 max 编码，见 encode_snapshot_at）。
        if export.snapshot_format == Some(1) && activation > compat::MIN_FEATURE_LEVEL {
            return Err(format!(
                "source snapshot_format 1 (OSSCTL1) cannot record activation_level {activation}"
            ));
        }
        // 两个 provenance 字段一起写；单独出现 = 不一致（fail closed，不猜）。
        let Some(level) = export.feature_level else {
            return Err(
                "export carries activation_level without feature_level (inconsistent provenance)"
                    .into(),
            );
        };
        if activation > level {
            return Err(format!(
                "source activation_level {activation} exceeds feature_level {level} \
                 (inconsistent provenance)"
            ));
        }
    }
    let mut missing = Vec::new();
    if export.snapshot_format.is_none() {
        missing.push("snapshot_format");
    }
    if export.wire_version.is_none() {
        missing.push("wire_version");
    }
    if export.feature_level.is_none() {
        missing.push("feature_level");
    }
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![format!(
        "legacy export without provenance ({}): accepting as unverified \
         (payload decodes fully; data requires level {required})",
        missing.join(", ")
    )])
}

/// 割接/恢复后目标节点必须保持的激活级别：新封套用 `activation_level`（来源 marker
/// 原值），v1.16 封套回落到 `feature_level`（= max(激活, 数据要求)），LC-0 前的旧导出
/// 两者都缺 → 数据要求。调用前必须已通过 [`verify_export`]。
pub fn install_activation_level(export: &ExportFile) -> u8 {
    let required = data::required_feature_level(&export.data).max(
        export
            .runstate
            .as_ref()
            .map_or(compat::MIN_FEATURE_LEVEL, |r| r.required_feature_level()),
    );
    export
        .activation_level
        .or(export.feature_level)
        .unwrap_or(required)
        .max(required)
}

pub fn write_export(path: &Path, export: &ExportFile) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(export)?;
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub fn read_export(path: &Path) -> Result<ExportFile, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let export: ExportFile =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse export: {e}"))?;
    for warning in verify_export(&export)? {
        eprintln!("warning: {warning}");
    }
    Ok(export)
}
