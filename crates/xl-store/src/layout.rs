//! 盘布局与寻址：对象名展平、桶名/键校验、盘上路径推导。
//!
//! 为什么展平：对象的全部内容（xl.meta + 版本目录）必须落在**同一个父目录**下，
//! 这样「提交」永远是一次同盘 rename，原子性不依赖任何锁，也不需要跨目录事务。

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// 对象名里 `/` 的展平替身。键里出现这个字面量会让往返编码错位，一律拒绝。
pub const SEP: &str = "__XLDIR__";
/// 系统目录：临时区、回收站、锁。
pub const SYS_DIR: &str = ".oss.sys";

const MAX_KEY_LEN: usize = 1024;

pub fn validate_bucket(name: &str) -> Result<()> {
    // 与控制台/aether 上传等非 URL 来源对齐：错误消息会进 S3 响应体，escape_debug
    // 让控制字符不破坏 XML（validate_key 同款）。
    let reject = || Err(Error::InvalidBucketName(name.escape_debug().to_string()));

    let ok_len = (3..=63).contains(&name.len());
    let ok_chars = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
    let ok_edges = !name.starts_with(['-', '.']) && !name.ends_with(['-', '.']);
    let ok_no_dotdot = !name.contains("..");
    // 不得形如 IPv4 地址（规格 §2.1；S3 的既有规则，见 aws 文档 "must not be formatted as an IP address"）
    let parts: Vec<&str> = name.split('.').collect();
    let looks_like_ipv4 = parts.len() == 4
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.bytes().all(|b| b.is_ascii_digit())
                && p.parse::<u16>().is_ok_and(|n| n <= 255)
        });
    if looks_like_ipv4 {
        return reject();
    }
    if ok_len && ok_chars && ok_edges && ok_no_dotdot {
        Ok(())
    } else {
        reject()
    }
}

pub fn validate_key(key: &str) -> Result<()> {
    // 键是用户字节，会随错误消息进 S3 响应体；escape_debug 让控制字符不破坏 XML，
    // 同时保留「哪段不合规」的指导性。
    let reject = |why: &str| Err(Error::InvalidKey(format!("{}: {why}", key.escape_debug())));

    if key.is_empty() {
        return reject("空键");
    }
    if key.len() > MAX_KEY_LEN {
        return reject("超过 1024 字节");
    }
    if key.contains(SEP) {
        return reject("包含保留分隔符");
    }
    if key.starts_with('/') {
        return reject("以 / 开头");
    }
    if key.contains("//") {
        return reject("含空路径段");
    }
    if key.split('/').any(|seg| seg == ".." || seg == ".") {
        return reject("含 . 或 .. 路径段");
    }
    if key.ends_with('/') {
        return reject("以 / 结尾");
    }
    if key.chars().any(|c| c.is_control() || c == '\\') {
        return reject("含控制字符或反斜杠");
    }
    Ok(())
}

pub fn object_dir_name(key: &str) -> Result<String> {
    validate_key(key)?;
    Ok(key.replace('/', SEP))
}

/// 目录名 → 键。重建的键必须过同一套校验：桶目录里可能出现手工塞入或残留的杂物目录，
/// 无校验时 `..__XLDIR__..` 这类名字会解出 `../..` 之类非法键。
pub fn key_from_dir_name(dir: &str) -> Result<String> {
    let key = dir.replace(SEP, "/");
    validate_key(&key)?;
    Ok(key)
}

pub fn bucket_dir(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket)
}

/// 所有对象读写路径都汇合到这里，桶名校验在此收口；S3 入口虽由 s3s 的默认 AwsNameValidation 先拦，但控制台/aether 上传直达内核，不能依赖上游。
pub fn object_dir(root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
    validate_bucket(bucket)?;
    validate_key(key)?;
    Ok(bucket_dir(root, bucket).join(object_dir_name(key)?))
}

pub fn sys_dir(root: &Path, sub: &str) -> PathBuf {
    root.join(SYS_DIR).join(sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_dir_roundtrip() {
        for key in ["a.txt", "a/b/c.txt", "x/y", "deep/a/b/c/d.bin"] {
            let dir = object_dir_name(key).unwrap();
            assert_eq!(key_from_dir_name(&dir).unwrap(), key);
        }
    }

    #[test]
    fn object_dir_flattens_slashes() {
        assert_eq!(
            object_dir_name("a/b/c.txt").unwrap(),
            "a__XLDIR__b__XLDIR__c.txt"
        );
    }

    #[test]
    fn rejects_path_traversal() {
        for bad in [
            "../etc/passwd",
            "a/../../b",
            "/abs",
            "a//b",
            "a/",
            "",
            "a\u{0}b",
        ] {
            assert!(object_dir_name(bad).is_err(), "{bad} 应当被拒");
        }
        // 拼接路径时桶名参数同样收口校验
        assert!(object_dir(Path::new("/tmp/root"), "..", "k").is_err());
        assert!(object_dir(Path::new("/tmp/root"), "../../etc", "k").is_err());
    }

    #[test]
    fn rejects_keys_containing_separator() {
        // 键里出现 __XLDIR__ 会让往返编码错位，必须拒绝而不是静默损坏
        assert!(object_dir_name("a__XLDIR__b").is_err());
    }

    #[test]
    fn key_from_dir_name_rejects_invalid_rebuilt_keys() {
        // 桶目录里手工塞入的杂物目录名解出的键非法，重建时必须拒绝：
        // `..__XLDIR__..` → `../..`、`__XLDIR__etc` → `/etc`、双分隔符 → `a//b`
        for bad in ["..__XLDIR__..", "__XLDIR__etc", "a__XLDIR____XLDIR__b", ""] {
            assert!(key_from_dir_name(bad).is_err(), "{bad} 应当被拒");
        }
    }

    #[test]
    fn key_len_limit_is_bytes() {
        assert!(validate_key(&"汉".repeat(341)).is_ok());
        assert!(validate_key(&"汉".repeat(342)).is_err()); // 341×3=1023B ≤1024，342×3=1026B >1024
    }

    #[test]
    fn invalid_key_message_escapes_user_bytes() {
        let msg = validate_key("a\u{0}b").unwrap_err().to_string();
        assert!(!msg.contains('\u{0}'), "原始控制字符不得进消息: {msg:?}");
        assert!(msg.contains("\\0"), "转义形式保留「哪段不合规」: {msg:?}");
    }

    #[test]
    fn key_rejects_control_chars() {
        assert!(validate_key("a\u{7f}b").is_err());
        assert!(validate_key("a\u{80}b").is_err()); // DEL 与 C1
    }

    #[test]
    fn bucket_name_rules() {
        assert!(validate_bucket("my-bucket").is_ok());
        for bad in ["ab", "My-Bucket", "-lead", "trail-", "a..b", "a_b", ""] {
            assert!(validate_bucket(bad).is_err(), "{bad} 应当被拒");
        }
        // S3 规则：不得形如 IP 地址（规格 §2.1）
        assert!(validate_bucket("192.168.1.1").is_err());
        assert!(validate_bucket("1.2.3.4.5").is_ok()); // 五段不是 IPv4，合法
    }

    #[test]
    fn invalid_bucket_message_escapes_user_bytes() {
        let msg = validate_bucket("a\u{0}b").unwrap_err().to_string();
        assert!(!msg.contains('\u{0}'), "原始控制字符不得进消息: {msg:?}");
        assert!(
            msg.contains("\\0"),
            "转义形式保留「哪个名字不合规」: {msg:?}"
        );
    }
}
