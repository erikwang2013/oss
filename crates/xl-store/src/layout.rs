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
    let ok_len = (3..=63).contains(&name.len());
    let ok_chars = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
    let ok_edges = !name.starts_with(['-', '.']) && !name.ends_with(['-', '.']);
    let ok_no_dotdot = !name.contains("..");
    if ok_len && ok_chars && ok_edges && ok_no_dotdot {
        Ok(())
    } else {
        Err(Error::InvalidBucketName(name.to_string()))
    }
}

pub fn validate_key(key: &str) -> Result<()> {
    let reject = |why: &str| Err(Error::InvalidKey(format!("{key}: {why}")));

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

pub fn key_from_dir_name(dir: &str) -> Result<String> {
    Ok(dir.replace(SEP, "/"))
}

pub fn bucket_dir(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket)
}

pub fn object_dir(root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
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
    }

    #[test]
    fn rejects_keys_containing_separator() {
        // 键里出现 __XLDIR__ 会让往返编码错位，必须拒绝而不是静默损坏
        assert!(object_dir_name("a__XLDIR__b").is_err());
    }

    #[test]
    fn bucket_name_rules() {
        assert!(validate_bucket("my-bucket").is_ok());
        for bad in ["ab", "My-Bucket", "-lead", "trail-", "a..b", "a_b", ""] {
            assert!(validate_bucket(bad).is_err(), "{bad} 应当被拒");
        }
    }
}
