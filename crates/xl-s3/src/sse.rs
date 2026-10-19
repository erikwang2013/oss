//! M5 切片 3：S3 面 SSE 头解析与响应装配（裁决 R1/R2/R6，见 M5 工作令）。
//! 解析只做「线格式 → 内核形态」：客户钥（base64 32B）→ `SseCKey`；SSE 头 → `EncSpec`。
//! 密钥对象带手工 Debug（不打印钥字节）；一切客户端可纠正形态 → 4xx（InvalidRequest/
//! InvalidArgument），服务端面维持 store 错误映射（500）。

use http::HeaderMap;
use s3s::S3Result;
use xl_store::crypto::{EncSpec, SseCKey};
use xl_store::meta::EncryptionRecord;

use crate::convert::to_s3_error;

/// 解析产物：写路径 `spec`（Some 才加密）+ 读路径客户钥（SSE-C 时与 spec 同源）。
#[derive(Default)]
pub struct SseReq {
    pub spec: Option<EncSpec>,
    pub key: Option<SseCKey>,
}

impl SseReq {
    /// SSE-C 的 key-MD5（MPU create/upload_part 一致性核对用；非 SSE-C 恒 None）。
    pub fn sse_c_md5(&self) -> Option<&str> {
        match &self.spec {
            Some(EncSpec::SseC(k)) => Some(k.md5_b64()),
            _ => None,
        }
    }

    /// 服务端模式的持久化标识（MPU create 记录用："sse-s3"/"sse-kms"；SSE-C/无 = None）。
    pub fn sse_kind(&self) -> Option<&'static str> {
        match &self.spec {
            Some(EncSpec::SseS3) => Some("sse-s3"),
            Some(EncSpec::SseKms) => Some("sse-kms"),
            _ => None,
        }
    }
}

/// M8 缺口 C：桶默认加密线格式 → `EncSpec`；未知值 → None（记录损坏，调用方失败关闭）。
pub fn enc_spec_from_default(value: &str) -> Option<EncSpec> {
    match value {
        "AES256" => Some(EncSpec::SseS3),
        "aws:kms" => Some(EncSpec::SseKms),
        _ => None,
    }
}

/// 通用解析（R1/R2）。各 handler 把 DTO 字段借出来传入：
/// - `algo/key/md5` = `sse_customer_*`；`sse` = `server_side_encryption`；`kms_key_id` = `ssekms_key_id`。
///
/// 规则：SSE-C 三件套与 `sse`/`ssekms_key_id` 互斥；`sse` 只认 "AES256"（SSE-S3）/ "aws:kms"
/// （文件型 SSE-KMS）；显式 CMK（kms key id）→ 400（R2：钉子「不做多 CMK 编排」）。
pub fn parse_sse(
    algo: Option<&str>,
    key_b64: Option<&str>,
    key_md5: Option<&str>,
    sse: Option<&str>,
    kms_key_id: Option<&str>,
) -> S3Result<SseReq> {
    let mut out = SseReq::default();
    if algo.is_some() || key_b64.is_some() || key_md5.is_some() {
        if sse.is_some() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "sse-c headers and server-side encryption are mutually exclusive"
            ));
        }
        if kms_key_id.is_some() {
            // P3-a 复验补：SSE-C 三件套与显式 CMK 互斥（R2 同一 fail-closed 口径）
            return Err(s3s::s3_error!(
                InvalidRequest,
                "kms key id is not valid with sse-c"
            ));
        }
        let algo = algo.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing sse-c algorithm"))?;
        if algo != "AES256" {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "unsupported sse-c algorithm"
            ));
        }
        let b64 = key_b64.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing sse-c key"))?;
        let k = SseCKey::from_b64(b64).map_err(to_s3_error)?;
        if let Some(m) = key_md5 {
            k.check_md5_header(m).map_err(to_s3_error)?;
        }
        out.key = Some(k.clone());
        out.spec = Some(EncSpec::SseC(k));
        return Ok(out);
    }
    if let Some(s) = sse {
        match s {
            "AES256" => {
                if kms_key_id.is_some() {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "kms key id requires aws:kms"
                    ));
                }
                out.spec = Some(EncSpec::SseS3);
            }
            "aws:kms" => {
                if kms_key_id.is_some() {
                    // R2：文件型 KMS 单主密钥（不做多 CMK 编排）；HTTP KMS 落地时再开
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "file-backed KMS does not support specifying a CMK"
                    ));
                }
                out.spec = Some(EncSpec::SseKms);
            }
            _ => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "unsupported server-side encryption"
                ));
            }
        }
    } else if kms_key_id.is_some() {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "kms key id requires aws:kms"
        ));
    }
    Ok(out)
}

/// 响应装配值（R6）：按对象加密记录推出各响应字段（"AES256"=SSE-S3；"aws:kms"=SSE-KMS；
/// SSE-C 只回 algorithm + key-MD5，不回 server_side_encryption——AWS 语义）。
pub struct SseOut {
    pub server_side_encryption: Option<&'static str>,
    pub kms_key_id: Option<String>,
    pub sse_c_algorithm: Option<&'static str>,
    pub sse_c_key_md5: Option<String>,
}

pub fn sse_out(rec: Option<&EncryptionRecord>) -> SseOut {
    match rec {
        None => SseOut {
            server_side_encryption: None,
            kms_key_id: None,
            sse_c_algorithm: None,
            sse_c_key_md5: None,
        },
        Some(r) if r.key_id == "sse-c" => SseOut {
            server_side_encryption: None,
            kms_key_id: None,
            sse_c_algorithm: Some("AES256"),
            sse_c_key_md5: r.sse_c_key_md5.clone(),
        },
        Some(r) if r.key_id == "file" => SseOut {
            server_side_encryption: Some("AES256"),
            kms_key_id: None,
            sse_c_algorithm: None,
            sse_c_key_md5: None,
        },
        Some(r) => SseOut {
            server_side_encryption: Some("aws:kms"),
            kms_key_id: Some(r.key_id.clone()),
            sse_c_algorithm: None,
            sse_c_key_md5: None,
        },
    }
}

/// 按请求侧 spec 推响应装配值（CreateMPU/UploadPart 尚无对象记录时用；KMS key_id 未知留空）。
pub fn sse_out_from_spec(r: &SseReq) -> SseOut {
    match &r.spec {
        None => sse_out(None),
        Some(EncSpec::SseS3) => SseOut {
            server_side_encryption: Some("AES256"),
            kms_key_id: None,
            sse_c_algorithm: None,
            sse_c_key_md5: None,
        },
        Some(EncSpec::SseKms) => SseOut {
            server_side_encryption: Some("aws:kms"),
            kms_key_id: None,
            sse_c_algorithm: None,
            sse_c_key_md5: None,
        },
        Some(EncSpec::SseC(k)) => SseOut {
            server_side_encryption: None,
            kms_key_id: None,
            sse_c_algorithm: Some("AES256"),
            sse_c_key_md5: Some(k.md5_b64().to_string()),
        },
    }
}

/// GET/HEAD 的 SSE-C key-MD5 响应头（s3s 这两个 DTO 无该字段——`S3Response::with_headers` 补）。
pub fn sse_c_key_md5_header(md5: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    if let Ok(v) = http::HeaderValue::from_str(md5) {
        h.insert(
            http::HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5"),
            v,
        );
    }
    h
}

/// SSE-C 的 algorithm + key-MD5 双头（CompleteMPU 输出 DTO 无 SSE-C 字段处补）。
pub fn sse_c_headers(md5: &str) -> HeaderMap {
    let mut h = sse_c_key_md5_header(md5);
    h.insert(
        http::HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
        http::HeaderValue::from_static("AES256"),
    );
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sse_matrix() {
        // SSE-S3 / SSE-KMS
        let r = parse_sse(None, None, None, Some("AES256"), None).unwrap();
        assert!(matches!(r.spec, Some(EncSpec::SseS3)));
        let r = parse_sse(None, None, None, Some("aws:kms"), None).unwrap();
        assert!(matches!(r.spec, Some(EncSpec::SseKms)));
        // 显式 CMK → 400（R2）；未知算法 → 400；kms key id 无 aws:kms → 400
        assert!(parse_sse(None, None, None, Some("aws:kms"), Some("arn:cmk")).is_err());
        assert!(parse_sse(None, None, None, Some("aws:kms:dsse"), None).is_err());
        assert!(parse_sse(None, None, None, Some("DES"), None).is_err());
        assert!(parse_sse(None, None, None, None, Some("k0")).is_err());

        // SSE-C：合法三件套
        use base64::Engine as _;
        let key = [42u8; 32];
        let k = SseCKey::from_key(key);
        let b64 = base64::engine::general_purpose::STANDARD.encode(key);
        let r = parse_sse(Some("AES256"), Some(&b64), Some(k.md5_b64()), None, None).unwrap();
        assert!(matches!(r.spec, Some(EncSpec::SseC(_))));
        assert!(r.key.is_some());
        // key-MD5 不符 → 400；缺 key/缺 algo → 400；与 server-side 互斥 → 400
        assert!(
            parse_sse(
                Some("AES256"),
                Some(&b64),
                Some("AAAAAAAAAAAAAAAAAAAAAAAA"),
                None,
                None
            )
            .is_err()
        );
        assert!(parse_sse(Some("AES256"), None, None, None, None).is_err());
        assert!(parse_sse(None, Some(&b64), None, None, None).is_err());
        assert!(parse_sse(Some("AES256"), Some(&b64), None, Some("AES256"), None).is_err());
        // SSE-C 三件套 + 显式 CMK 互斥（P3-a 复验补）→ 400
        assert!(parse_sse(Some("AES256"), Some(&b64), None, None, Some("k0")).is_err());
        // algo 非 AES256 → 400
        assert!(parse_sse(Some("AES128"), Some(&b64), None, None, None).is_err());
    }

    #[test]
    fn sse_out_mapping() {
        let rec = |kid: &str, md5: Option<&str>| EncryptionRecord {
            algorithm: xl_store::crypto::ALGO.to_string(),
            key_id: kid.to_string(),
            wrapped_dek_hex: String::new(),
            nonce_seed_hex: "00".repeat(8),
            sse_c_key_md5: md5.map(str::to_string),
        };
        assert!(sse_out(None).server_side_encryption.is_none());
        let o = sse_out(Some(&rec("file", None)));
        assert_eq!(o.server_side_encryption, Some("AES256"));
        let o = sse_out(Some(&rec("k3", None)));
        assert_eq!(o.server_side_encryption, Some("aws:kms"));
        assert_eq!(o.kms_key_id.as_deref(), Some("k3"));
        let o = sse_out(Some(&rec("sse-c", Some("AbCdEfGhIjKlMnOpQrStUv=="))));
        assert!(o.server_side_encryption.is_none(), "SSE-C 不回 SSE 头");
        assert_eq!(o.sse_c_algorithm, Some("AES256"));
        assert_eq!(o.sse_c_key_md5.as_deref(), Some("AbCdEfGhIjKlMnOpQrStUv=="));
        // key-MD5 头构造（单头 / 双头）
        let h = sse_c_key_md5_header("AAA=");
        assert_eq!(
            h.get("x-amz-server-side-encryption-customer-key-md5")
                .unwrap(),
            "AAA="
        );
        let h2 = sse_c_headers("AAA=");
        assert_eq!(
            h2.get("x-amz-server-side-encryption-customer-algorithm")
                .unwrap(),
            "AES256"
        );
        assert!(
            h2.get("x-amz-server-side-encryption-customer-key-md5")
                .is_some()
        );
    }
}
