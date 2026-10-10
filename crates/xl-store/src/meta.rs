//! xl.meta v1 —— 每个对象目录一份，是**唯一的提交点**。
//!
//! 编码：`[1 字节格式版本][msgpack 载荷]`。版本字节是前向兼容的锚点：
//! 后续阶段（版本控制/加密/对象锁/复制）只往 `Version` 里加 `Option` 字段，
//! 老解码器靠 serde 的默认「跳过未知字段」行为继续可读。

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
        match bytes.split_first() {
            Some((&FORMAT_VERSION, rest)) => {
                rmp_serde::from_slice(rest).map_err(|e| Error::CorruptMeta(e.to_string()))
            }
            Some((&v, _)) => Err(Error::CorruptMeta(format!("不支持的格式版本 {v}"))),
            None => Err(Error::CorruptMeta("空文件".into())),
        }
    }

    /// M1：取唯一的一条。M4 起改为取 is_latest（格式已在 §5.1 预留该字段）。
    pub fn latest(&self) -> Option<&Version> {
        self.versions.last()
    }
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
}
