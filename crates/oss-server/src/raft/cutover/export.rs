//! 控制面导出文件：读取 11 张复制表，生成带校验和的 JSON（只读；不重放 schema/模式）。

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::db::Db;
use crate::raft::data::{self, ControlData};

pub const EXPORT_FORMAT: u32 = 1;
pub const EXPORT_KIND: &str = "oss-raft-control-export";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFile {
    pub format: u32,
    pub kind: String,
    pub source_site: String,
    pub source_node_id: Option<u64>,
    pub source_role: String,
    pub version: String,
    pub admin_username: Option<String>,
    pub has_vault: Option<bool>,
    pub exported_at: i64,
    pub sha256: String,
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
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    let payload = serde_json::to_vec(&data).map_err(|e| e.to_string())?;
    let sha256 = hex::encode(Sha256::digest(&payload));
    Ok(ExportFile {
        format: EXPORT_FORMAT,
        kind: EXPORT_KIND.to_string(),
        source_site,
        source_node_id,
        source_role: source_role.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        admin_username,
        has_vault,
        exported_at: crate::db::now(),
        sha256,
        data,
    })
}

pub fn verify_export(export: &ExportFile) -> Result<(), String> {
    if export.format != EXPORT_FORMAT || export.kind != EXPORT_KIND {
        return Err(format!(
            "unsupported export format/kind: {}/{}",
            export.format, export.kind
        ));
    }
    let payload = serde_json::to_vec(&export.data).map_err(|e| e.to_string())?;
    let want = hex::encode(Sha256::digest(&payload));
    if want != export.sha256 {
        return Err("export checksum mismatch".into());
    }
    Ok(())
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
    verify_export(&export)?;
    Ok(export)
}
