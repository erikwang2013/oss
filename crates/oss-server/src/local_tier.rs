//! M8：本地冷盘池 tier 后端。
//!
//! key 口径与远端一致：`<tier_id>/<bucket>/<version_id>/<raw key>`，落盘到配置的根目录。
//! 写入 = 同目录临时文件 + fsync + rename；拒绝绝对路径、`..` 与 NUL。

use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use md5::Digest as _;

use crate::tier::{TierBackend, TierObject};

pub struct LocalTierBackend {
    id: String,
    root: PathBuf,
}

impl LocalTierBackend {
    pub fn new(id: String, root: PathBuf) -> Self {
        Self { id, root }
    }

    /// 根目录 + 相对 key；任一路径段不是普通名字就拒绝（挡 `..`/绝对路径/NUL）。
    fn safe_path(&self, key: &str) -> Result<PathBuf, String> {
        if key.is_empty() || key.contains('\0') {
            return Err(format!("本地 tier key 非法：{key:?}"));
        }
        let rel = Path::new(key);
        if rel.is_absolute() {
            return Err(format!("本地 tier key 不能是绝对路径：{key:?}"));
        }
        for c in rel.components() {
            if !matches!(c, Component::Normal(_)) {
                return Err(format!("本地 tier key 含非法路径段：{key:?}"));
            }
        }
        Ok(self.root.join(rel))
    }

    fn tmp_path(path: &Path) -> PathBuf {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("object");
        path.with_file_name(format!(".{name}.tmp-{}", uuid::Uuid::new_v4()))
    }

    /// 清理**同一目标文件**的陈旧临时文件（崩溃残留）。
    ///
    /// 只删 >1h 的同名前缀临时文件：并发写不同 key/同一 key 的活跃临时文件
    /// 不能被另一路 put 误删。
    async fn clean_stale_tmp(parent: &Path, target: &Path) -> Result<(), String> {
        const STALE_TMP_AGE: std::time::Duration = std::time::Duration::from_secs(3600);
        let name = target
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("object");
        let prefix = format!(".{name}.tmp-");
        let mut rd = tokio::fs::read_dir(parent)
            .await
            .map_err(|e| format!("本地 tier 读目录失败：{e}"))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|e| format!("本地 tier 读目录失败：{e}"))?
        {
            let entry_name = ent.file_name();
            if !entry_name.to_string_lossy().starts_with(&prefix) {
                continue;
            }
            let stale = ent
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|m| m.elapsed().ok())
                .is_some_and(|age| age >= STALE_TMP_AGE);
            if stale {
                let _ = tokio::fs::remove_file(ent.path()).await;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TierBackend for LocalTierBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn put(&self, key: &str, body: Bytes, etag: &str) -> Result<String, String> {
        let path = self.safe_path(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| format!("本地 tier key 无父目录：{key:?}"))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("本地 tier 建目录失败：{e}"))?;
        let tmp = Self::tmp_path(&path);
        let result = async {
            let mut f = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| format!("本地 tier 建临时文件失败：{e}"))?;
            tokio::io::AsyncWriteExt::write_all(&mut f, &body)
                .await
                .map_err(|e| format!("本地 tier 写入失败：{e}"))?;
            f.sync_all()
                .await
                .map_err(|e| format!("本地 tier fsync 失败：{e}"))?;
            drop(f);
            if etag.len() == 32 && etag.bytes().all(|b| b.is_ascii_hexdigit()) {
                let md5 = hex::encode(md5::Md5::digest(&body));
                if !md5.eq_ignore_ascii_case(etag) {
                    return Err(format!("本地 tier MD5 不符（期望 {etag}，得到 {md5}）"));
                }
            }
            tokio::fs::rename(&tmp, &path)
                .await
                .map_err(|e| format!("本地 tier rename 失败：{e}"))?;
            let _ = Self::clean_stale_tmp(parent, &path).await;
            Ok(etag.to_string())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        result
    }

    async fn get(&self, key: &str) -> Result<Bytes, String> {
        let path = self.safe_path(key)?;
        match tokio::fs::read(&path).await {
            Ok(b) => Ok(Bytes::from(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(format!("本地 tier 对象不存在：{key}"))
            }
            Err(e) => Err(format!("本地 tier 读取失败：{e}")),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        // 控制台健康检查依赖：根目录缺失必须报错，而不是当成“空 tier 健康”。
        if !self.root.is_dir() {
            return Err(format!("本地 tier 根目录不存在：{}", self.root.display()));
        }
        let path = self.safe_path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(m) if m.is_file() => Ok(Some(m.len())),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("本地 tier HEAD 失败：{e}")),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        let path = self.safe_path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(format!("本地 tier 删除失败：{e}")),
        }
        // 自底向上删空目录，到根为止（best-effort）。
        let mut dir = path.parent();
        while let Some(d) = dir {
            if d == self.root || !d.starts_with(&self.root) {
                break;
            }
            if tokio::fs::remove_dir(d).await.is_err() {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<TierObject>, String> {
        let base = self.root.join(&self.id);
        if !tokio::fs::try_exists(&base)
            .await
            .map_err(|e| format!("本地 tier 读目录失败：{e}"))?
        {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut stack = vec![base];
        while let Some(dir) = stack.pop() {
            let mut rd = tokio::fs::read_dir(&dir)
                .await
                .map_err(|e| format!("本地 tier 读目录失败：{e}"))?;
            while let Some(ent) = rd
                .next_entry()
                .await
                .map_err(|e| format!("本地 tier 读目录失败：{e}"))?
            {
                let ft = ent
                    .file_type()
                    .await
                    .map_err(|e| format!("本地 tier 读目录失败：{e}"))?;
                if ft.is_dir() {
                    stack.push(ent.path());
                } else if ft.is_file() {
                    let name = ent.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with('.') && name.contains(".tmp-") {
                        continue;
                    }
                    let md = ent
                        .metadata()
                        .await
                        .map_err(|e| format!("本地 tier 读元数据失败：{e}"))?;
                    let mtime = md
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    let rel = ent
                        .path()
                        .strip_prefix(&self.root)
                        .map_err(|e| format!("本地 tier 路径越界：{e}"))?
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.push(TierObject {
                        key: rel,
                        last_modified: mtime,
                        size: md.len(),
                    });
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(root: &Path) -> LocalTierBackend {
        LocalTierBackend::new("local1".into(), root.to_path_buf())
    }

    fn etag(body: &[u8]) -> String {
        hex::encode(md5::Md5::digest(body))
    }

    #[tokio::test]
    async fn rejects_traversal_and_absolute_keys() {
        let d = tempfile::tempdir().unwrap();
        let b = backend(d.path());
        // 注意：`a/./b` 会被 Path 规范化，不构成越界；只拒绝真正改变父目录的段。
        for key in [
            "../x",
            "a/../../b",
            "a/../b",
            "/etc/passwd",
            "./a",
            "a\0b",
            "",
        ] {
            assert!(
                b.put(key, Bytes::from_static(b"x"), "0").await.is_err(),
                "{key}"
            );
            assert!(b.get(key).await.is_err(), "{key}");
            assert!(b.head(key).await.is_err(), "{key}");
            assert!(b.delete(key).await.is_err(), "{key}");
        }
    }

    #[tokio::test]
    async fn put_get_head_delete_roundtrip_is_atomic() {
        let d = tempfile::tempdir().unwrap();
        let b = backend(d.path());
        let key = "local1/bkt/v1/dir/obj.txt";
        let body = b"cold data";
        assert_eq!(
            b.put(key, Bytes::from_static(body), &etag(body))
                .await
                .unwrap(),
            etag(body)
        );
        assert_eq!(b.get(key).await.unwrap(), Bytes::from_static(body));
        assert_eq!(b.head(key).await.unwrap(), Some(body.len() as u64));
        // rename 后不残留临时文件
        let parent = d.path().join("local1/bkt/v1/dir");
        let mut rd = tokio::fs::read_dir(&parent).await.unwrap();
        while let Some(e) = rd.next_entry().await.unwrap() {
            assert!(!e.file_name().to_string_lossy().contains(".tmp-"));
        }
        b.delete(key).await.unwrap();
        assert_eq!(b.head(key).await.unwrap(), None);
        assert!(b.get(key).await.is_err());
        // 空目录已清理，但根目录保留
        assert!(!parent.exists());
        assert!(d.path().is_dir());
    }

    #[tokio::test]
    async fn etag_mismatch_leaves_no_file() {
        let d = tempfile::tempdir().unwrap();
        let b = backend(d.path());
        let key = "local1/bkt/v1/obj.txt";
        let err = b
            .put(key, Bytes::from_static(b"data"), &"0".repeat(32))
            .await
            .unwrap_err();
        assert!(err.contains("MD5"), "{err}");
        assert_eq!(b.head(key).await.unwrap(), None);
        let parent = d.path().join("local1/bkt/v1");
        let mut rd = tokio::fs::read_dir(&parent).await.unwrap();
        while let Some(e) = rd.next_entry().await.unwrap() {
            assert!(!e.file_name().to_string_lossy().contains(".tmp-"));
        }
    }

    #[tokio::test]
    async fn restart_recovery_reads_existing_object_and_cleans_stale_tmp() {
        let d = tempfile::tempdir().unwrap();
        let key = "local1/bkt/v1/obj.txt";
        let body = b"persisted";
        let dir = d.path().join("local1/bkt/v1");
        {
            let b = backend(d.path());
            b.put(key, Bytes::from_static(body), &etag(body))
                .await
                .unwrap();
            // 模拟崩溃残留（>1h，才会被下一次 put 清理）
            let stale = dir.join(".obj.txt.tmp-stale");
            tokio::fs::write(&stale, b"junk").await.unwrap();
            std::fs::File::options()
                .write(true)
                .open(&stale)
                .unwrap()
                .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(7200))
                .unwrap();
            // 同 key 的活跃临时文件（并发写）：无论新旧都不得被另一路 put 清理。
            tokio::fs::write(dir.join(".obj.txt.tmp-active"), b"inflight")
                .await
                .unwrap();
        }
        let b = backend(d.path());
        assert_eq!(b.get(key).await.unwrap(), Bytes::from_static(body));
        b.put(key, Bytes::from_static(b"new"), &etag(b"new"))
            .await
            .unwrap();
        assert!(!dir.join(".obj.txt.tmp-stale").exists());
        assert!(
            dir.join(".obj.txt.tmp-active").exists(),
            "并发写入的临时文件不得被清理"
        );
        assert_eq!(b.get(key).await.unwrap(), Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn missing_root_is_unhealthy_and_put_recreates_it() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("not-there");
        let b = backend(&missing);
        assert!(b.head(".console-healthcheck").await.is_err());
        let body = b"x";
        b.put("local1/bkt/v1/k", Bytes::from_static(body), &etag(body))
            .await
            .unwrap();
        assert_eq!(b.head(".console-healthcheck").await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_returns_relative_keys_and_skips_tmp() {
        let d = tempfile::tempdir().unwrap();
        let b = backend(d.path());
        let body = b"x";
        b.put("local1/bkt/v1/a", Bytes::from_static(body), &etag(body))
            .await
            .unwrap();
        b.put("local1/bkt/v2/b", Bytes::from_static(body), &etag(body))
            .await
            .unwrap();
        tokio::fs::write(d.path().join("local1/bkt/v1/.a.tmp-x"), b"junk")
            .await
            .unwrap();
        let mut keys: Vec<String> = b.list().await.unwrap().into_iter().map(|o| o.key).collect();
        keys.sort();
        assert_eq!(keys, vec!["local1/bkt/v1/a", "local1/bkt/v2/b"]);
    }

    #[tokio::test]
    async fn write_failure_returns_error_without_final_file() {
        let d = tempfile::tempdir().unwrap();
        // 根路径是普通文件 → create_dir_all 失败
        let root = d.path().join("root-file");
        std::fs::write(&root, b"x").unwrap();
        let b = backend(&root);
        let err = b
            .put("local1/bkt/v1/k", Bytes::from_static(b"x"), "0")
            .await
            .unwrap_err();
        assert!(err.contains("建目录失败"), "{err}");
    }
}
