//! One framed record per key under a namespace directory.
//!
//! Payload layout: `u64 version | value`. Version increments on every put and is
//! advisory under concurrent writers (rename makes the last full record win).

use std::fs;
use std::io;
use std::path::PathBuf;

use super::{
    KIND_KV, Result, atomic_create_new, atomic_write, corrupt, decode_frame, encode_frame,
    ensure_dir, invalid_input, key_file, read_file, sync_dir,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
}

pub struct LocalKv {
    dir: PathBuf,
}

impl LocalKv {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        ensure_dir(&dir)?;
        Ok(Self { dir })
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.get_versioned(key)?.map(|(_, value)| value))
    }

    pub fn get_versioned(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>> {
        let path = key_file(&self.dir, key, ".rec")?;
        match read_file(&path)? {
            None => Ok(None),
            Some(bytes) => {
                let (version, value) = decode_payload(&bytes)?;
                Ok(Some((version, value.to_vec())))
            }
        }
    }

    /// Atomically replaces `key`; returns the new version. A corrupt existing
    /// record is an error rather than silently overwritten (fail closed).
    pub fn put(&self, key: &str, value: &[u8]) -> Result<u64> {
        let path = key_file(&self.dir, key, ".rec")?;
        let version = match read_file(&path)? {
            None => 1,
            Some(bytes) => decode_payload(&bytes)?
                .0
                .checked_add(1)
                .ok_or_else(|| invalid_input("local kv version overflow"))?,
        };
        let mut payload = Vec::with_capacity(8 + value.len());
        payload.extend_from_slice(&version.to_le_bytes());
        payload.extend_from_slice(value);
        atomic_write(&path, &encode_frame(KIND_KV, &payload))?;
        Ok(version)
    }

    /// Inserts `key` only when no record exists; returns whether this call
    /// created it. Concurrent callers (including other processes) cannot both
    /// observe `true`, so this is the KV analogue of `INSERT ... DO NOTHING`.
    pub fn put_if_absent(&self, key: &str, value: &[u8]) -> Result<bool> {
        let path = key_file(&self.dir, key, ".rec")?;
        let mut payload = Vec::with_capacity(8 + value.len());
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(value);
        atomic_create_new(&path, &encode_frame(KIND_KV, &payload))
    }

    /// Removes the record; returns whether it existed (missing is not an error).
    pub fn delete(&self, key: &str) -> Result<bool> {
        let path = key_file(&self.dir, key, ".rec")?;
        match fs::remove_file(&path) {
            Ok(()) => {
                sync_dir(&self.dir)?;
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// All records ordered by key (bytewise). Foreign/temp files are ignored.
    pub fn scan(&self) -> Result<Vec<KvEntry>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(stem) = name.strip_suffix(".rec") else {
                continue;
            };
            let Ok(key_bytes) = hex::decode(stem) else {
                continue;
            };
            let Ok(key) = String::from_utf8(key_bytes) else {
                continue;
            };
            let bytes = fs::read(entry.path())?;
            let (version, value) = decode_payload(&bytes)?;
            out.push(KvEntry {
                key,
                value: value.to_vec(),
                version,
            });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

fn decode_payload(bytes: &[u8]) -> Result<(u64, &[u8])> {
    let payload = decode_frame(bytes, KIND_KV)?;
    if payload.len() < 8 {
        return Err(corrupt("local kv payload shorter than version"));
    }
    let (version, value) = payload.split_at(8);
    Ok((
        u64::from_le_bytes(version.try_into().expect("8 bytes")),
        value,
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::super::LocalStore;
    use super::super::atomic_write_without_rename;

    #[test]
    fn put_get_delete_and_versions() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let kv = store.kv("accounts").unwrap();

        assert_eq!(kv.get("k").unwrap(), None);
        assert_eq!(kv.put("k", b"v1").unwrap(), 1);
        assert_eq!(kv.get("k").unwrap().as_deref(), Some(&b"v1"[..]));
        assert_eq!(kv.put("k", b"v2").unwrap(), 2);
        assert_eq!(kv.get_versioned("k").unwrap().unwrap().0, 2);
        assert!(kv.delete("k").unwrap());
        assert_eq!(kv.get("k").unwrap(), None);
        assert!(!kv.delete("k").unwrap());
    }

    #[test]
    fn scan_is_ordered_by_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let kv = store.kv("ns").unwrap();
        for key in ["b", "a", "c"] {
            kv.put(key, key.as_bytes()).unwrap();
        }
        let keys: Vec<_> = kv.scan().unwrap().into_iter().map(|e| e.key).collect();
        assert_eq!(keys, ["a", "b", "c"]);
    }

    #[test]
    fn crash_before_rename_leaves_old_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let kv = store.kv("ns").unwrap();
        kv.put("k", b"old").unwrap();
        let path = store.root().join("kv/ns").join(hex::encode("k") + ".rec");
        let mut payload = 9u64.to_le_bytes().to_vec();
        payload.extend_from_slice(b"new");
        assert!(
            atomic_write_without_rename(&path, &super::encode_frame(super::KIND_KV, &payload))
                .is_err()
        );
        assert_eq!(kv.get("k").unwrap().as_deref(), Some(&b"old"[..]));
        assert_eq!(kv.put("k", b"new").unwrap(), 2);
        assert_eq!(kv.get("k").unwrap().as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn torn_target_and_foreign_files_are_reported_or_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let kv = store.kv("ns").unwrap();
        kv.put("k", b"v").unwrap();
        let ns = store.root().join("kv/ns");
        fs::write(
            ns.join(format!(".tmp-{}-crash", std::process::id())),
            b"garbage",
        )
        .unwrap();
        assert_eq!(kv.scan().unwrap().len(), 1);
        let path = ns.join(hex::encode("k") + ".rec");
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        assert_eq!(
            kv.get("k").unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn concurrent_writers_never_tear_a_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let root = store.root().to_path_buf();
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let root = root.clone();
                std::thread::spawn(move || {
                    let kv = LocalStore::open(root).unwrap().kv("race").unwrap();
                    for n in 0..10 {
                        kv.put("k", format!("{i}-{n}").as_bytes()).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let value = store.kv("race").unwrap().get("k").unwrap().unwrap();
        let text = String::from_utf8(value).unwrap();
        let (writer, n) = text.split_once('-').unwrap();
        assert!(writer.parse::<u8>().unwrap() < 8);
        assert!(n.parse::<u8>().unwrap() < 10);
    }
}
