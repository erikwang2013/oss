//! Node-local operational files (design D4 / §7).
//!
//! Everything lives under a caller-provided state root:
//!
//! ```text
//! kv/<namespace>/<hex key>.rec        versioned framed values
//! queue/<name>.log                    append-only framed queue journal
//! audit/<namespace>/<utc day>.log     append-only daily audit files
//! site/cursors/<hex peer>.cursor      per-peer site shipping cursor
//! uploads/<hex saved_path>.rec        multipart upload records
//! ```
//!
//! Record framing is `magic "OSSL" | u16 major | u8 kind | u32 payload_len |
//! payload | u32 crc32c(payload)`; a reader that sees a newer major fails closed.
//!
//! Durability and torn writes: single-record files (KV, cursor) are written to an
//! exclusive temp file, fsynced, then renamed over the target; a reader therefore
//! sees the old record or the new one, never a mix. Append-only logs (queue, audit)
//! are replayed up to the last complete CRC-valid frame; a torn tail is truncated on
//! the next open before appending.
//!
//! Concurrency model: KV and cursor writes need no lock — concurrent writers use
//! atomic rename and the last rename wins (readers never observe corruption). A
//! queue takes an exclusive `flock` on its journal, so one process owns it at a
//! time; handles inside that process share one replayed in-memory state behind a
//! mutex, which makes claim races single-winner.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub mod audit;
pub mod kv;
pub mod queue;
pub mod site;
pub mod uploads;

pub use audit::{AuditLog, AuditRecord};
pub use kv::{KvEntry, LocalKv};
pub use queue::{Claim, LocalQueue, QueueEntry, QueueStatus};
pub use site::{Cursor, CursorFile};
pub use uploads::{UploadRecord, UploadStore};

/// On-disk format major. Bump when framing or payload encoding changes.
pub const FORMAT_MAJOR: u16 = 1;

const MAGIC: &[u8; 4] = b"OSSL";
const HEADER_LEN: usize = 11; // magic + major + kind + payload_len
const CRC_LEN: usize = 4;

pub(crate) const KIND_KV: u8 = 1;
pub(crate) const KIND_QUEUE: u8 = 2;
pub(crate) const KIND_AUDIT: u8 = 3;
pub(crate) const KIND_CURSOR: u8 = 4;

pub type Result<T> = io::Result<T>;

/// Root of all node-local operational state.
#[derive(Clone)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    /// Creates the root and its top-level directories with mode 0700.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        ensure_dir(&root)?;
        // Eager: namespace subdirectories created later inherit a private parent.
        for sub in ["kv", "queue", "audit", "site", "uploads"] {
            ensure_dir(&root.join(sub))?;
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Namespaced local KV: one file per key.
    pub fn kv(&self, namespace: &str) -> Result<LocalKv> {
        validate_name(namespace)?;
        LocalKv::open(self.root.join("kv").join(namespace))
    }

    /// Durable queue with the default dead-letter threshold.
    pub fn queue(&self, name: &str) -> Result<LocalQueue> {
        self.queue_with_max_attempts(name, queue::DEFAULT_MAX_ATTEMPTS)
    }

    pub fn queue_with_max_attempts(&self, name: &str, max_attempts: u32) -> Result<LocalQueue> {
        validate_name(name)?;
        LocalQueue::open(self.root.join("queue"), name, max_attempts)
    }

    /// Daily rotated audit files.
    pub fn audit(&self, namespace: &str) -> Result<AuditLog> {
        validate_name(namespace)?;
        AuditLog::open(self.root.join("audit").join(namespace))
    }

    /// Atomic per-peer cursor used by site shipping.
    pub fn cursor(&self, peer_id: &str) -> Result<CursorFile> {
        CursorFile::open(self.root.join("site").join("cursors"), peer_id)
    }

    /// Multipart upload records keyed by `saved_path`.
    pub fn uploads(&self) -> Result<UploadStore> {
        UploadStore::open(self.root.join("uploads"))
    }
}

pub(crate) fn ensure_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    set_mode(dir, 0o700)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

pub(crate) fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(invalid_input(format!("unsafe local store name {name:?}")))
    }
}

/// Maps a caller key to a filename-safe path. Hex keeps the mapping injective and
/// cannot escape the directory. Keys longer than 120 bytes would exceed NAME_MAX.
pub(crate) fn key_file(dir: &Path, key: &str, suffix: &str) -> Result<PathBuf> {
    if key.is_empty() || key.len() > 120 {
        return Err(invalid_input(format!(
            "local key length {} outside 1..=120",
            key.len()
        )));
    }
    Ok(dir.join(format!("{}{suffix}", hex::encode(key.as_bytes()))))
}

pub(crate) fn invalid_input(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg.into())
}

pub(crate) fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

pub(crate) fn unsupported(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, msg.into())
}

/// CRC32C (Castagnoli). Bitwise: local files are small and this avoids a new dep.
/// shortcut: ~8 shifts/byte, upgrade to a table if audit/queue throughput matters.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82F6_3B78 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}

pub(crate) fn encode_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT_MAJOR.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc32c(payload).to_le_bytes());
    out
}

/// Parses one frame at the start of `bytes`. `Ok(None)` means truncated tail.
fn frame_at(bytes: &[u8], kind: u8) -> Result<Option<(&[u8], usize)>> {
    if bytes.len() < HEADER_LEN {
        return Ok(None);
    }
    if &bytes[..4] != MAGIC {
        return Err(corrupt("bad local-store magic"));
    }
    let major = u16::from_le_bytes([bytes[4], bytes[5]]);
    if major != FORMAT_MAJOR {
        return Err(unsupported(format!(
            "local-store format major {major}, reader supports {FORMAT_MAJOR}"
        )));
    }
    if bytes[6] != kind {
        return Err(corrupt("record kind mismatch"));
    }
    let len = u32::from_le_bytes(bytes[7..11].try_into().expect("4 bytes")) as usize;
    let total = HEADER_LEN + len + CRC_LEN;
    if bytes.len() < total {
        return Ok(None);
    }
    let payload = &bytes[HEADER_LEN..HEADER_LEN + len];
    let stored = u32::from_le_bytes(bytes[HEADER_LEN + len..total].try_into().expect("4 bytes"));
    if stored != crc32c(payload) {
        return Err(corrupt("record crc mismatch"));
    }
    Ok(Some((payload, total)))
}

/// Strict decode of a file that must contain exactly one frame.
pub(crate) fn decode_frame(bytes: &[u8], kind: u8) -> Result<&[u8]> {
    match frame_at(bytes, kind)? {
        Some((payload, total)) if total == bytes.len() => Ok(payload),
        Some(_) => Err(corrupt("trailing bytes after record")),
        None => Err(corrupt("truncated record")),
    }
}

/// Parses as many complete frames as possible. A torn tail, CRC failure or foreign
/// kind stops the scan (append-only files can only be damaged at the end); a newer
/// format major is an error so a downgraded binary never silently ignores state.
pub(crate) fn scan_frames(bytes: &[u8], kind: u8) -> Result<(Vec<&[u8]>, usize)> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        match frame_at(&bytes[offset..], kind) {
            Ok(Some((payload, total))) => {
                out.push(payload);
                offset += total;
            }
            Ok(None) => break,
            Err(e) if e.kind() == io::ErrorKind::Unsupported => return Err(e),
            Err(_) => break,
        }
    }
    Ok((out, offset))
}

/// Reads a log and truncates a torn tail in place, returning the valid prefix.
pub(crate) fn read_repaired_tail(file: &mut File, kind: u8) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    let (_, valid) = scan_frames(&bytes, kind)?;
    if valid < bytes.len() {
        file.set_len(valid as u64)?;
        file.sync_all()?;
        bytes.truncate(valid);
    }
    Ok(bytes)
}

pub(crate) fn read_file(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn open_private(path: &Path, read: bool, append: bool) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create(true);
    if read {
        opts.read(true);
    }
    if append {
        opts.append(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
}

pub(crate) fn create_new_private(path: &Path) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
}

pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()
}

/// Writes `bytes` atomically: temp file in the same directory, fsync, rename.
///
/// Concurrency: two writers may interleave; rename makes exactly one full record
/// visible and the loser's bytes are discarded. The old value survives a crash
/// before the rename.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_do(path, bytes, true)
}

/// Test-only crash simulation: writes and fsyncs the temp file, then returns
/// without renaming (as a crash between fsync and rename would).
#[cfg(test)]
pub(crate) fn atomic_write_without_rename(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_do(path, bytes, false)
}

/// Creates `path` with `bytes` only if it does not exist yet; returns whether
/// this call created it. The temp file is fully written and fsynced before the
/// hard link, so a concurrent reader sees either nothing or a complete record.
/// Cross-process safe on filesystems that support hard links.
pub(crate) fn atomic_create_new(path: &Path, bytes: &[u8]) -> Result<bool> {
    let dir = path
        .parent()
        .ok_or_else(|| invalid_input("atomic create path without parent"))?;
    ensure_dir(dir)?;
    let tmp = temp_path(dir);
    if let Err(e) = write_tmp(&tmp, bytes) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    let outcome = match fs::hard_link(&tmp, path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    };
    let _ = fs::remove_file(&tmp);
    match outcome {
        Ok(true) => {
            sync_dir(dir)?;
            Ok(true)
        }
        Ok(false) => Ok(false),
        Err(e) => Err(e),
    }
}

fn temp_path(dir: &Path) -> PathBuf {
    static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
    dir.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        NEXT_TMP.fetch_add(1, Ordering::Relaxed)
    ))
}

fn atomic_write_do(path: &Path, bytes: &[u8], commit: bool) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| invalid_input("atomic write path without parent"))?;
    ensure_dir(dir)?;
    let tmp = temp_path(dir);
    if let Err(e) = write_tmp(&tmp, bytes) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if !commit {
        return Err(io::Error::other("simulated crash before rename"));
    }
    if let Err(e) = fs::rename(&tmp, path).and_then(|()| sync_dir(dir)) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

fn write_tmp(tmp: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = create_new_private(tmp)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_castagnoli_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn frame_roundtrip_and_tail_tolerance() {
        let frame = encode_frame(KIND_KV, b"payload");
        assert_eq!(decode_frame(&frame, KIND_KV).unwrap(), b"payload");
        assert!(decode_frame(&frame[..frame.len() - 1], KIND_KV).is_err());
        let mut log = frame.clone();
        log.extend_from_slice(b"torn");
        let (frames, valid) = scan_frames(&log, KIND_KV).unwrap();
        assert_eq!(frames, vec![&b"payload"[..]]);
        assert_eq!(valid, frame.len());
    }

    #[test]
    fn store_layout_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path().join("local")).unwrap();
        let kv = store.kv("accounts").unwrap();
        kv.put("k", b"v").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(store.root()), 0o700);
            assert_eq!(mode(&store.root().join("kv/accounts")), 0o700);
            assert_eq!(
                mode(
                    &store
                        .root()
                        .join("kv/accounts")
                        .join(hex::encode("k") + ".rec")
                ),
                0o600
            );
        }
    }

    #[test]
    fn unsafe_names_are_rejected() {
        assert!(validate_name("../x").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name("").is_err());
        assert!(validate_name("accounts.v1").is_ok());
    }
}
