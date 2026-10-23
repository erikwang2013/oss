//! M8：本地冷盘池 tier 后端。
//!
//! key 口径与远端一致：`<tier_id>/<bucket>/<version_id>/<raw key>`，落盘到配置的根目录。
//! 写入 = 同目录临时文件 + fsync + rename；拒绝绝对路径、`..` 与 NUL。
//!
//! 4.7 加固：根目录视为信任根，但**根内任何路径段不得是符号链接**（否则 `link ->
//! /etc/passwd` 或 `dir -> /tmp/outside` 会逃出根目录）。所有 open/mkdir/rename/unlink
//! 走 `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` + `*at` 家族，父目录 fd 一旦
//! 解析即固定，避免「检查后再替换成链接」的 TOCTOU。Linux <5.6（无 openat2）与非
//! Linux 回退为逐段 `openat(O_NOFOLLOW)`，仍拒绝任何链接组件。

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use futures::StreamExt as _;
use md5::Digest as _;
use rustix::fd::OwnedFd;
#[cfg(target_os = "linux")]
use rustix::fs::ResolveFlags;
use rustix::fs::{AtFlags, Mode, OFlags};
use rustix::io::Errno;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::tier::{TierBackend, TierByteStream, TierObject, TierPutResult};

#[cfg(target_os = "linux")]
fn resolve_flags() -> ResolveFlags {
    ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS
}

fn dir_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC
}

fn io_msg(what: &str, e: Errno) -> String {
    if e == Errno::LOOP {
        return format!("{what}：路径包含符号链接或非目录路径段，拒绝访问（fail closed）");
    }
    format!("{what}：{e}")
}

/// 相对根目录打开。Linux 有 openat2 时用 BENEATH|NO_SYMLINKS；老内核（ENOSYS，<5.6）
/// 与非 Linux 回退为逐段 openat(O_NOFOLLOW)——每段都不跟随链接，语义等价。
/// ENOENT/ENOTDIR 原样返回（回退路径的中间段 ENOTDIR 统一折成 ELOOP）。
fn open_beneath(dir: &OwnedFd, rel: &Path, flags: OFlags) -> Result<OwnedFd, Errno> {
    #[cfg(target_os = "linux")]
    match rustix::fs::openat2(
        dir,
        rel,
        flags | OFlags::CLOEXEC,
        Mode::empty(),
        resolve_flags(),
    ) {
        Err(Errno::NOSYS) => {} // 老内核：走便携回退
        other => return other,
    }
    open_walk(dir, rel, flags)
}

/// 便携回退：逐段 openat(O_NOFOLLOW|O_DIRECTORY)，父目录 fd 逐级固定。
fn open_walk(dir: &OwnedFd, rel: &Path, flags: OFlags) -> Result<OwnedFd, Errno> {
    let comps: Vec<&std::ffi::OsStr> = rel.components().map(|c| c.as_os_str()).collect();
    let (name, dirs) = comps.split_last().ok_or(Errno::INVAL)?;
    let mut cur: Option<OwnedFd> = None;
    for comp in dirs {
        let base = cur.as_ref().unwrap_or(dir);
        cur = Some(
            rustix::fs::openat(base, *comp, dir_flags() | OFlags::NOFOLLOW, Mode::empty())
                .map_err(|e| if e == Errno::NOTDIR { Errno::LOOP } else { e })?,
        );
    }
    rustix::fs::openat(
        cur.as_ref().unwrap_or(dir),
        *name,
        flags | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
}

fn open_root(root: &Path) -> Result<OwnedFd, String> {
    rustix::fs::open(root, dir_flags(), Mode::empty())
        .map_err(|e| format!("本地 tier 根目录不可用（{}）：{e}", root.display()))
}

/// 逐段打开父目录；`create` 时补建缺失目录。返回父目录 fd 与文件名。
/// 任一段是符号链接 → ELOOP（fail closed，绝不解引用）。
fn open_parent(root: &Path, rel: &Path, create: bool) -> Result<(OwnedFd, OsString), String> {
    let comps: Vec<OsString> = rel
        .components()
        .map(|c| c.as_os_str().to_os_string())
        .collect();
    let (name, dirs) = comps
        .split_last()
        .ok_or_else(|| format!("本地 tier key 无文件名：{rel:?}"))?;
    let mut cur = open_root(root)?;
    for comp in dirs {
        if create {
            // EEXIST 正常；其它失败留给 open_beneath 报出具体原因。
            let _ = rustix::fs::mkdirat(&cur, comp.as_os_str(), Mode::from_raw_mode(0o755));
        }
        cur = open_beneath(&cur, Path::new(comp), dir_flags())
            .map_err(|e| io_msg(&format!("本地 tier 打开目录 {comp:?} 失败"), e))?;
    }
    Ok((cur, name.clone()))
}

/// 清理**同一目标文件**的陈旧临时文件（崩溃残留）：只删 >1h 的同名前缀项，
/// 并发写同/不同 key 的活跃临时文件不得被另一路 put 误删。
fn clean_stale_tmp_at(parent: &OwnedFd, name: &str) {
    let prefix = format!(".{name}.tmp-");
    let Ok(mut dir) = rustix::fs::Dir::read_from(parent) else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    while let Some(Ok(ent)) = dir.read() {
        let ent_name = ent.file_name().to_string_lossy();
        if !ent_name.starts_with(&prefix) {
            continue;
        }
        // SYMLINK_NOFOLLOW：链接自身的 mtime，绝不解引用到根外。
        let Ok(st) = rustix::fs::statat(parent, ent.file_name(), AtFlags::SYMLINK_NOFOLLOW) else {
            continue;
        };
        if now.saturating_sub(st.st_mtime) >= 3600 {
            let _ = rustix::fs::unlinkat(parent, ent.file_name(), AtFlags::empty());
        }
    }
}

/// 流式 put 的临时文件状态；Drop 时清理未 rename 的临时文件（含 future 取消）。
struct LocalTmp {
    parent: OwnedFd,
    tmp: String,
    name: String,
    done: bool,
}

impl LocalTmp {
    fn create(root: &Path, rel: &Path) -> Result<(Self, std::fs::File), String> {
        std::fs::create_dir_all(root).map_err(|e| format!("本地 tier 建目录失败：{e}"))?;
        let (parent, name) = open_parent(root, rel, true)?;
        let name = name
            .to_str()
            .ok_or_else(|| format!("本地 tier 文件名非 UTF-8：{rel:?}"))?
            .to_string();
        let tmp = format!(".{name}.tmp-{}", uuid::Uuid::new_v4());
        let fd = rustix::fs::openat(
            &parent,
            tmp.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o644),
        )
        .map_err(|e| io_msg("本地 tier 建临时文件失败", e))?;
        Ok((
            Self {
                parent,
                tmp,
                name,
                done: false,
            },
            std::fs::File::from(fd),
        ))
    }

    /// fsync + rename；返回后临时文件成为最终对象。
    fn commit(mut self, f: std::fs::File) -> Result<(), String> {
        f.sync_all()
            .map_err(|e| format!("本地 tier 写入失败：{e}"))?;
        drop(f); // 先关 fd，再 rename
        // rename 替换的是目录项本身（不跟随最终符号链接），父目录 fd 已固定。
        rustix::fs::renameat(
            &self.parent,
            self.tmp.as_str(),
            &self.parent,
            self.name.as_str(),
        )
        .map_err(|e| format!("本地 tier rename 失败：{e}"))?;
        let _ = rustix::fs::fsync(&self.parent);
        clean_stale_tmp_at(&self.parent, &self.name);
        self.done = true;
        Ok(())
    }
}

impl Drop for LocalTmp {
    fn drop(&mut self) {
        if !self.done {
            let _ = rustix::fs::unlinkat(&self.parent, self.tmp.as_str(), AtFlags::empty());
        }
    }
}

fn open_read(root: &Path, rel: &Path) -> Result<std::fs::File, String> {
    let rootfd = open_root(root)?;
    let fd = open_beneath(&rootfd, rel, OFlags::RDONLY).map_err(|e| {
        if e == Errno::NOENT || e == Errno::NOTDIR {
            format!("本地 tier 对象不存在：{}", rel.display())
        } else {
            io_msg("本地 tier 读取失败", e)
        }
    })?;
    Ok(std::fs::File::from(fd))
}

fn head_blocking(root: &Path, rel: &Path) -> Result<Option<u64>, String> {
    let rootfd = open_root(root)?;
    // 用 RDONLY 而非 O_PATH：O_PATH 仅 Linux 有，便携回退/非 Linux 需要可移植 flag。
    let fd = match open_beneath(&rootfd, rel, OFlags::RDONLY) {
        Ok(fd) => fd,
        Err(e) if e == Errno::NOENT || e == Errno::NOTDIR => return Ok(None),
        Err(e) => return Err(io_msg("本地 tier HEAD 失败", e)),
    };
    let md = std::fs::File::from(fd)
        .metadata()
        .map_err(|e| format!("本地 tier HEAD 失败：{e}"))?;
    Ok(if md.is_file() { Some(md.len()) } else { None })
}

fn delete_blocking(root: &Path, rel: &Path) -> Result<(), String> {
    let comps: Vec<OsString> = rel
        .components()
        .map(|c| c.as_os_str().to_os_string())
        .collect();
    let (name, dirs) = comps
        .split_last()
        .ok_or_else(|| format!("本地 tier key 无文件名：{rel:?}"))?;
    // dirs[0] = 根目录；dirs[i] 的名字相对 dirs[i-1]，用于自底向上删空目录。
    let mut opened: Vec<(OwnedFd, OsString)> = vec![(open_root(root)?, OsString::new())];
    for comp in dirs {
        match open_beneath(&opened[opened.len() - 1].0, Path::new(comp), dir_flags()) {
            Ok(fd) => opened.push((fd, comp.clone())),
            Err(e) if e == Errno::NOENT || e == Errno::NOTDIR => return Ok(()),
            Err(e) => return Err(io_msg(&format!("本地 tier 打开目录 {comp:?} 失败"), e)),
        }
    }
    let parent = &opened[opened.len() - 1].0;
    match rustix::fs::unlinkat(parent, name.as_os_str(), AtFlags::empty()) {
        Ok(()) => {}
        Err(Errno::NOENT) => return Ok(()),
        Err(e) => return Err(format!("本地 tier 删除失败：{e}")),
    }
    // 删空目录：unlinkat(parent_fd, name, AT_REMOVEDIR)，失败（非空）即停。
    for i in (1..opened.len()).rev() {
        if rustix::fs::unlinkat(
            &opened[i - 1].0,
            opened[i].1.as_os_str(),
            AtFlags::REMOVEDIR,
        )
        .is_err()
        {
            break;
        }
    }
    Ok(())
}

pub struct LocalTierBackend {
    id: String,
    root: PathBuf,
}

impl LocalTierBackend {
    pub fn new(id: String, root: PathBuf) -> Self {
        Self { id, root }
    }

    /// 相对 key；任一路径段不是普通名字就拒绝（挡 `..`/绝对路径/NUL）。
    fn safe_rel(key: &str) -> Result<PathBuf, String> {
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
        Ok(rel.to_path_buf())
    }

    async fn blocking<T, F>(&self, f: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, String> + Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| format!("本地 tier 任务失败：{e}"))?
    }
}

#[async_trait::async_trait]
impl TierBackend for LocalTierBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn put(
        &self,
        key: &str,
        mut body: TierByteStream,
        size: u64,
    ) -> Result<TierPutResult, String> {
        let rel = Self::safe_rel(key)?;
        let root = self.root.clone();
        let (tmp, f) = self.blocking(move || LocalTmp::create(&root, &rel)).await?;
        let mut file = tokio::fs::File::from_std(f);
        let mut md5 = md5::Md5::new();
        let mut sha = sha2::Sha256::new();
        let mut n: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            if n > size {
                return Err(format!("本地 tier 写入流超过声明长度 {size} 字节"));
            }
            md5.update(&chunk);
            sha.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|e| format!("本地 tier 写入失败：{e}"))?;
        }
        file.flush()
            .await
            .map_err(|e| format!("本地 tier 写入失败：{e}"))?;
        if n != size {
            return Err(format!("本地 tier 写入长度 {n} ≠ 声明 {size} 字节"));
        }
        let md5 = hex::encode(md5.finalize());
        let sha256 = hex::encode(sha.finalize());
        let std_file = file.into_std().await;
        self.blocking(move || tmp.commit(std_file)).await?;
        Ok(TierPutResult {
            etag: md5.clone(),
            md5,
            sha256,
        })
    }

    async fn get(&self, key: &str) -> Result<TierByteStream, String> {
        let rel = Self::safe_rel(key)?;
        let root = self.root.clone();
        let f = self.blocking(move || open_read(&root, &rel)).await?;
        let f = tokio::fs::File::from_std(f);
        let stream = futures::stream::try_unfold(f, |mut f| async move {
            let mut buf = vec![0u8; 256 * 1024];
            match f.read(&mut buf).await {
                Ok(0) => Ok(None),
                Ok(n) => {
                    buf.truncate(n);
                    Ok(Some((Bytes::from(buf), f)))
                }
                Err(e) => Err(format!("本地 tier 读取失败：{e}")),
            }
        });
        Ok(Box::pin(stream))
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        let rel = Self::safe_rel(key)?;
        let root = self.root.clone();
        self.blocking(move || head_blocking(&root, &rel)).await
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        let rel = Self::safe_rel(key)?;
        let root = self.root.clone();
        self.blocking(move || delete_blocking(&root, &rel)).await
    }

    async fn list(&self) -> Result<Vec<TierObject>, String> {
        let base = self.root.join(&self.id);
        match std::fs::symlink_metadata(&base) {
            Ok(m) if m.file_type().is_symlink() => {
                // 4.7：tier 根不能是链接（否则 list/GC 口径与实际数据目录不一致）。
                return Err(format!(
                    "本地 tier 目录是符号链接，拒绝访问：{}",
                    base.display()
                ));
            }
            Ok(m) if m.is_dir() => {}
            Ok(_) => return Ok(Vec::new()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("本地 tier 读目录失败：{e}")),
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

    fn stream(body: &'static [u8]) -> TierByteStream {
        Box::pin(futures::stream::once(async move {
            Ok(Bytes::from_static(body))
        }))
    }

    async fn collect(mut s: TierByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(c) = s.next().await {
            out.extend_from_slice(&c.unwrap());
        }
        out
    }

    async fn put(
        b: &LocalTierBackend,
        key: &str,
        body: &'static [u8],
    ) -> Result<TierPutResult, String> {
        b.put(key, stream(body), body.len() as u64).await
    }

    /// M1：便携回退（无 openat2 的内核/非 Linux）同样按段拒绝链接，并打开正常路径。
    #[test]
    fn portable_walk_rejects_symlinks_and_opens_plain_path() {
        use std::io::Read as _;

        let d = tempfile::tempdir().unwrap();
        let rootfd = open_root(d.path()).unwrap();
        std::fs::create_dir_all(d.path().join("a/b")).unwrap();
        std::fs::write(d.path().join("a/b/ok"), b"x").unwrap();
        let fd = open_walk(&rootfd, Path::new("a/b/ok"), OFlags::RDONLY).unwrap();
        let mut s = String::new();
        std::fs::File::from(fd).read_to_string(&mut s).unwrap();
        assert_eq!(s, "x");

        let outside = d.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, d.path().join("a/linkdir")).unwrap();
        assert_eq!(
            open_walk(&rootfd, Path::new("a/linkdir/x"), OFlags::RDONLY).unwrap_err(),
            Errno::LOOP,
            "中间目录链接必须拒绝"
        );
        std::fs::write(outside.join("secret"), b"s").unwrap();
        std::os::unix::fs::symlink(outside.join("secret"), d.path().join("a/b/evil")).unwrap();
        assert_eq!(
            open_walk(&rootfd, Path::new("a/b/evil"), OFlags::RDONLY).unwrap_err(),
            Errno::LOOP,
            "最终文件链接必须拒绝"
        );
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
            assert!(b.put(key, stream(b"x"), 1).await.is_err(), "{key}");
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
        let r = put(&b, key, body).await.unwrap();
        assert_eq!(r.etag, etag(body));
        assert_eq!(r.md5, etag(body));
        assert_eq!(collect(b.get(key).await.unwrap()).await, body);
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
        // 声明 5 字节、流只给 4 字节：fail closed，不落最终对象也不留临时文件。
        let err = b.put(key, stream(b"data"), 5).await.unwrap_err();
        assert!(err.contains("长度"), "{err}");
        assert_eq!(b.head(key).await.unwrap(), None);
        // 流报错同样清理临时文件。
        let bad: TierByteStream = Box::pin(futures::stream::once(async {
            Err("模拟源流失败".to_string())
        }));
        assert!(b.put(key, bad, 4).await.is_err());
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
            put(&b, key, body).await.unwrap();
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
        assert_eq!(collect(b.get(key).await.unwrap()).await, body);
        put(&b, key, b"new").await.unwrap();
        assert!(!dir.join(".obj.txt.tmp-stale").exists());
        assert!(
            dir.join(".obj.txt.tmp-active").exists(),
            "并发写入的临时文件不得被清理"
        );
        assert_eq!(collect(b.get(key).await.unwrap()).await, b"new");
    }

    #[tokio::test]
    async fn missing_root_is_unhealthy_and_put_recreates_it() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("not-there");
        let b = backend(&missing);
        assert!(b.head(".console-healthcheck").await.is_err());
        put(&b, "local1/bkt/v1/k", b"x").await.unwrap();
        assert_eq!(b.head(".console-healthcheck").await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_returns_relative_keys_and_skips_tmp() {
        let d = tempfile::tempdir().unwrap();
        let b = backend(d.path());
        put(&b, "local1/bkt/v1/a", b"x").await.unwrap();
        put(&b, "local1/bkt/v2/b", b"x").await.unwrap();
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
        let err = b.put("local1/bkt/v1/k", stream(b"x"), 1).await.unwrap_err();
        assert!(err.contains("建目录失败"), "{err}");
    }

    /// 4.7：根内最终文件是符号链接 → get/head 拒绝，绝不读到根外内容。
    #[tokio::test]
    async fn rejects_symlinked_file_escape() {
        let d = tempfile::tempdir().unwrap();
        let outside = d.path().join("outside.txt");
        std::fs::write(&outside, b"secret").unwrap();
        let objdir = d.path().join("local1/bkt/v1");
        std::fs::create_dir_all(&objdir).unwrap();
        std::os::unix::fs::symlink(&outside, objdir.join("evil.txt")).unwrap();
        let b = backend(d.path());
        let err = b
            .get("local1/bkt/v1/evil.txt")
            .await
            .err()
            .expect("符号链接必须拒绝");
        assert!(
            err.contains("不允许") || err.contains("符号链接") || err.contains("LOOP"),
            "{err}"
        );
        assert!(b.head("local1/bkt/v1/evil.txt").await.is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"secret");
    }

    /// 4.7：中间目录是符号链接 → put/get/delete 一律拒绝，根外不落任何文件。
    #[tokio::test]
    async fn rejects_symlinked_directory_component() {
        let d = tempfile::tempdir().unwrap();
        let outside = d.path().join("outside-dir");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::create_dir_all(d.path().join("local1")).unwrap();
        std::os::unix::fs::symlink(&outside, d.path().join("local1/bkt")).unwrap();
        let b = backend(d.path());
        let key = "local1/bkt/v1/k";
        assert!(b.put(key, stream(b"x"), 1).await.is_err());
        assert!(b.get(key).await.is_err());
        assert!(b.head(key).await.is_err());
        assert!(b.delete(key).await.is_err());
        assert!(
            !outside.join("v1").exists(),
            "不得在根目录之外创建任何文件/目录"
        );
    }

    /// 4.7：put 目标位置已有指向根外的符号链接 → rename 替换链接本身，根外文件不变。
    #[tokio::test]
    async fn put_replaces_final_symlink_without_following() {
        let d = tempfile::tempdir().unwrap();
        let outside = d.path().join("outside.txt");
        std::fs::write(&outside, b"keep").unwrap();
        let objdir = d.path().join("local1/bkt/v1");
        std::fs::create_dir_all(&objdir).unwrap();
        std::os::unix::fs::symlink(&outside, objdir.join("obj")).unwrap();
        let b = backend(d.path());
        put(&b, "local1/bkt/v1/obj", b"new-data").await.unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
        assert_eq!(
            collect(b.get("local1/bkt/v1/obj").await.unwrap()).await,
            b"new-data"
        );
    }
}
