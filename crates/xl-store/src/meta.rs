//! xl.meta v1 —— 每个对象目录一份，是**唯一的提交点**。
//!
//! 编码：`[1 字节格式版本][msgpack 载荷]`。版本字节是前向兼容的锚点：
//! 后续阶段（版本控制/加密/对象锁/复制）只往 `Version` 里加 `Option` 字段，
//! 老解码器靠 serde 的默认「跳过未知字段」行为继续可读。
//! 非 Option 字段必须带 `#[serde(default)]`——新解码器读旧文件时，缺 default 会硬报
//! `missing field`（M4 的 is_latest/is_delete_marker 等要照此办）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const FORMAT_VERSION: u8 = 1;
/// xl.meta 文件名。
pub const META_FILE: &str = "xl.meta";
/// 覆盖写时的落盘中转名。
pub const META_NEW_FILE: &str = "xl.meta.new";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XlMeta {
    pub object_key: String,
    /// M1 恒为一条。M4 起多条（版本控制），解码器与读路径从第一天就按 Vec 处理。
    pub versions: Vec<Version>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Version {
    /// M1 无版本控制：`Uuid::nil()`（S3 协议层映射为字面量 `"null"`）；M4 起为每版本真实 uuid。
    pub version_id: uuid::Uuid,
    /// `>= 0` 是写入端不变量（`now_nanos` 钳 0）；下游转 SystemTime 前必须钳制
    /// （负数会回绕成天文数字）。
    pub mod_time_nanos: i64,
    pub size: u64,
    /// 单段 PUT = 整份 MD5（S3 的定义）；aetherupload 上传时白拿。
    pub etag: String,
    pub content_type: Option<String>,
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    pub part: PartRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartRef {
    /// 版本目录名（`<对象目录>/<uuid>/`）。
    pub uuid: String,
    pub part: u16,
    pub size: u64,
    pub sha256: String,
}

impl XlMeta {
    pub fn encode(&self) -> Result<Vec<u8>> {
        // to_vec_named：struct 编成「字段名 → 值」映射而非位置数组。
        // 前向兼容靠它 —— 数组编码下未来版本多一个字段，老解码器必报长度错。
        let body = rmp_serde::to_vec_named(self).map_err(|e| Error::Encode(e.to_string()))?;
        let mut out = Vec::with_capacity(body.len() + 1);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let m: Self = match bytes.split_first() {
            Some((&FORMAT_VERSION, rest)) => {
                rmp_serde::from_slice(rest).map_err(|e| Error::CorruptMeta(e.to_string()))?
            }
            Some((&v, _)) => return Err(Error::CorruptMeta(format!("不支持的格式版本 {v}"))),
            None => return Err(Error::CorruptMeta("空文件".into())),
        };
        // part.uuid 是版本目录名，读路径会把它拼进盘上路径：含 / \ .. 空或控制字符的 uuid
        // 能把读路径指到对象目录之外，解码时（唯一的信任边界）一律拒绝。
        // 宽松白名单：不按 UUID 格式收严，非 UUID 形态的历史数据仍可读。
        for v in &m.versions {
            if !part_uuid_is_safe(&v.part.uuid) {
                return Err(Error::CorruptMeta(format!(
                    "part.uuid 非法: {:?}",
                    v.part.uuid
                )));
            }
        }
        Ok(m)
    }

    /// M1：取唯一的一条。M4 起改为取 is_latest（格式已在 §5.1 预留该字段）。
    pub fn latest(&self) -> Option<&Version> {
        self.versions.last()
    }
}

/// 版本目录名的宽松白名单：非空、无路径分隔符、无 `..`、无控制字符。
fn part_uuid_is_safe(uuid: &str) -> bool {
    !uuid.is_empty()
        && !uuid.contains('/')
        && !uuid.contains('\\')
        && !uuid.contains("..")
        && !uuid.chars().any(|c| c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> XlMeta {
        XlMeta {
            object_key: "a/b.txt".into(),
            versions: vec![Version {
                version_id: uuid::Uuid::nil(),
                mod_time_nanos: 1_700_000_000_000_000_000,
                size: 11,
                etag: "5eb63bbbe01eeed093cb22bb8f5acdc3".into(),
                content_type: Some("text/plain".into()),
                user_metadata: [("k".to_string(), "v".to_string())].into_iter().collect(),
                part: PartRef {
                    uuid: "u".into(),
                    part: 1,
                    size: 11,
                    sha256: "aa".into(),
                },
            }],
        }
    }

    #[test]
    fn roundtrip() {
        let m = sample();
        let bytes = m.encode().unwrap();
        let back = XlMeta::decode(&bytes).unwrap();
        assert_eq!(back.versions.len(), 1);
        assert_eq!(back.versions[0].etag, m.versions[0].etag);
        assert_eq!(
            back.versions[0].user_metadata.get("k"),
            Some(&"v".to_string())
        );
    }

    #[test]
    fn first_byte_is_format_version() {
        let bytes = sample().encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
    }

    #[test]
    fn decode_rejects_unknown_version() {
        let mut bytes = sample().encode().unwrap();
        bytes[0] = 99;
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_rejects_empty() {
        assert!(matches!(XlMeta::decode(&[]), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn latest_returns_single_version() {
        let m = sample();
        assert_eq!(m.latest().unwrap().size, 11);
    }

    #[test]
    fn decode_skips_unknown_fields() {
        // 前向兼容：v1 解码器遇到未来版本新增的字段必须跳过而不是报错
        #[derive(serde::Serialize)]
        struct Future {
            object_key: String,
            versions: Vec<Version>,
            brand_new_field: u64, // v1 不认识的字段
        }
        let bytes = rmp_serde::to_vec_named(&Future {
            object_key: "a".into(),
            versions: vec![],
            brand_new_field: 7,
        })
        .unwrap();
        let mut framed = vec![FORMAT_VERSION];
        framed.extend(bytes);
        assert!(XlMeta::decode(&framed).is_ok());
    }

    #[test]
    fn nested_unknown_fields_are_skipped() {
        // 前向兼容的承诺粒度是嵌套字段：未来版本往 Version / PartRef 里加字段，
        // v1 解码器也必须能读，不只顶层。
        #[derive(serde::Serialize)]
        struct FutureVersion {
            version_id: uuid::Uuid,
            mod_time_nanos: i64,
            size: u64,
            etag: String,
            content_type: Option<String>,
            user_metadata: BTreeMap<String, String>,
            part: PartRef,
            brand_new_v_field: u64, // v1 不认识的嵌套字段
        }
        #[derive(serde::Serialize)]
        struct FutureMeta {
            object_key: String,
            versions: Vec<FutureVersion>,
        }
        let src = sample();
        let v = &src.versions[0];
        let bytes = rmp_serde::to_vec_named(&FutureMeta {
            object_key: src.object_key.clone(),
            versions: vec![FutureVersion {
                version_id: v.version_id,
                mod_time_nanos: v.mod_time_nanos,
                size: v.size,
                etag: v.etag.clone(),
                content_type: v.content_type.clone(),
                user_metadata: v.user_metadata.clone(),
                part: v.part.clone(),
                brand_new_v_field: 7,
            }],
        })
        .unwrap();
        let mut framed = vec![FORMAT_VERSION];
        framed.extend(bytes);
        let back = XlMeta::decode(&framed).unwrap();
        assert_eq!(back.versions[0].etag, v.etag);
        assert_eq!(back.versions[0].part.sha256, "aa");
    }

    #[test]
    fn latest_is_none_for_empty_versions() {
        let m = XlMeta {
            object_key: "a".into(),
            versions: vec![],
        };
        assert!(m.latest().is_none());
    }

    #[test]
    fn truncated_payload_is_corrupt() {
        let bytes = sample().encode().unwrap();
        let cut = &bytes[..bytes.len() - 3];
        assert!(matches!(XlMeta::decode(cut), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_rejects_unsafe_part_uuid() {
        // part.uuid 是版本目录名（读路径会拼进盘上路径）：含 / \ .. 空或控制字符的 uuid
        // 能把读路径指到对象目录之外，解码时一律拒绝。宽松白名单——不按 UUID 格式收严，
        // 非 UUID 形态的历史数据（如 fixture "u"）仍必须可读。
        for bad in ["a/../b", "../x", "a/b", "a\\b", "", "a\u{0}b"] {
            let mut m = sample();
            m.versions[0].part.uuid = bad.into();
            let bytes = m.encode().unwrap();
            assert!(
                matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))),
                "{bad:?} 应当被拒"
            );
        }
        assert_eq!(
            XlMeta::decode(&sample().encode().unwrap()).unwrap(),
            sample()
        );
    }

    #[test]
    fn golden_bytes_stable() {
        // 字段名与表示在 named 编码下就是磁盘契约：自读自写的 roundtrip 抓不到
        // 字段重命名或 Uuid 表示漂移（str vs bin8），黄金字节能。
        // 更新流程：确认改动是有意的 → cargo test -p xl-store golden -- --nocapture
        // 打印实际摘要 → 更新下常量。改格式 = 有意识地改，不是顺手改。
        use sha2::Digest;
        let bytes = sample().encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
        // 已人工核对：首字节 01；version_id 为 bin8（c4 10 + 16 字节全零）；
        // mod_time_nanos 为 uint64（cf）；顶层/嵌套均为字段名 map。
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        println!("{digest}");
        assert_eq!(
            digest,
            "da898c71e7d91dbc23826a6e84269cd2962a530ff364a9fc3c59eccc7ea2821f"
        );
    }
}
