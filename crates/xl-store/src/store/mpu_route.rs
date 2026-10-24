//! M3 MPU 会话粘性：upload_id 的属主前缀编解码。
//!
//! 线上形态（不透明字符串，客户端只回传）：
//!
//! ```text
//! <base64url-nopad(node_id)>.<uuid>
//! ```
//!
//! - 单节点模式与升级前的存量 upload：**裸 UUID**（无前缀）——语义 = 本地属主；
//! - 集群模式 Create 生成带前缀的 id（暂存区仍在 Create 所在节点的 root 盘）；
//! - 目录名始终是规范 UUID（`<prefix>.<uuid>` 的右半），故盘上布局/列举口径不变。
//!
//! 解析是**信任边界**：右半必须能解析为 UUID（收口 `../`、大小写变体），左半必须
//! 是长度受限的合法 base64url + UTF-8。属主是否在拓扑内由 `Store::mpu_remote_owner`
//! 判定（未知属主 → 503，不是 404）。

use base64::Engine as _;

use crate::error::{Error, Result};

/// 节点 id 编码后的最大长度（base64 膨胀 4/3；防超长输入的无界解码）。
const MAX_NODE_B64_LEN: usize = 512;
/// 节点 id 原文上限（与配置层的一般约束同量级；超限视为非法 upload_id）。
const MAX_NODE_ID_LEN: usize = 256;

/// 生成客户端可见 upload_id。
pub(super) fn format_upload_id(node_id: &str, uuid: &str) -> String {
    let enc = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(node_id.as_bytes());
    format!("{enc}.{uuid}")
}

/// 拆分 upload_id → (属主, 规范 UUID)。裸 UUID → `(None, uuid)`。
/// 非法（含解码失败/超长/非 UUID 右半）→ `NoSuchUpload`（不泄漏解析细节）。
pub(super) fn split_upload_id(upload_id: &str) -> Result<(Option<String>, String)> {
    // 存量格式优先：裸 UUID（canonicalize，沿用旧路径收口语义）
    if let Ok(u) = uuid::Uuid::parse_str(upload_id) {
        return Ok((None, u.to_string()));
    }
    let bad = || Error::NoSuchUpload(upload_id.to_string());
    let (enc, rest) = upload_id.split_once('.').ok_or_else(bad)?;
    if enc.is_empty() || enc.len() > MAX_NODE_B64_LEN {
        return Err(bad());
    }
    let uuid = uuid::Uuid::parse_str(rest).map_err(|_| bad())?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(enc)
        .map_err(|_| bad())?;
    if raw.is_empty() || raw.len() > MAX_NODE_ID_LEN {
        return Err(bad());
    }
    let node = String::from_utf8(raw).map_err(|_| bad())?;
    Ok((Some(node), uuid.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_uuid_is_local_and_canonicalized() {
        let u = uuid::Uuid::new_v4();
        let (owner, id) = split_upload_id(&u.to_string()).unwrap();
        assert!(owner.is_none());
        assert_eq!(id, u.to_string());
        // 大小写变体收口到同一规范目录名
        let upper = u.to_string().to_uppercase();
        let (_, id2) = split_upload_id(&upper).unwrap();
        assert_eq!(id2, u.to_string());
    }

    #[test]
    fn prefixed_id_roundtrip() {
        let uuid = uuid::Uuid::new_v4().to_string();
        for node in ["n1", "A", "stretch-node-2", "节点"] {
            let id = format_upload_id(node, &uuid);
            let (owner, got) = split_upload_id(&id).unwrap();
            assert_eq!(owner.as_deref(), Some(node));
            assert_eq!(got, uuid);
        }
    }

    #[test]
    fn malformed_ids_are_no_such_upload() {
        let uuid = uuid::Uuid::new_v4().to_string();
        for bad in [
            "",
            "not-a-uuid",
            "n1",
            "./..",
            &format!("!!!.{uuid}"),
            &format!(".{uuid}"),
            &format!("{}.{}", "A".repeat(600), uuid),
            "n1.not-a-uuid",
            "n1.ffffffff-ffff-ffff-ffff-ffffffffffff.extra",
        ] {
            assert!(
                matches!(split_upload_id(bad), Err(Error::NoSuchUpload(_))),
                "应拒绝: {bad}"
            );
        }
        // UUID 右半大小写不敏感（与裸 UUID 旧路径同口径），统一规范化为小写
        let upper = format!("n1.{}", uuid.to_uppercase());
        let (owner, got) = split_upload_id(&upper).unwrap();
        assert_eq!(owner.as_deref(), Some("n1"));
        assert_eq!(got, uuid);
    }
}
