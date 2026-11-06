//! Per-peer site shipping cursor (atomic rename) and dead-letter journal.

use std::io::Write as _;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{
    KIND_CURSOR, KIND_DEAD, Result, atomic_write, corrupt, decode_frame, encode_frame, ensure_dir,
    key_file, open_private, read_file, scan_frames,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub peer_id: String,
    pub last_seq: i64,
    pub updated_at: i64,
}

pub struct CursorFile {
    peer_id: String,
    path: PathBuf,
}

impl CursorFile {
    pub fn open(dir: impl Into<PathBuf>, peer_id: &str) -> Result<Self> {
        let dir = dir.into();
        ensure_dir(&dir)?;
        let path = key_file(&dir, peer_id, ".cursor")?;
        Ok(Self {
            peer_id: peer_id.to_string(),
            path,
        })
    }

    /// Missing file is `None`; a corrupt file is an error (fail closed).
    pub fn load(&self) -> Result<Option<Cursor>> {
        let Some(bytes) = read_file(&self.path)? else {
            return Ok(None);
        };
        let payload = decode_frame(&bytes, KIND_CURSOR)?;
        if payload.len() != 16 {
            return Err(corrupt("cursor payload is not 16 bytes"));
        }
        let last_seq = i64::from_le_bytes(payload[..8].try_into().expect("8 bytes"));
        let updated_at = i64::from_le_bytes(payload[8..].try_into().expect("8 bytes"));
        Ok(Some(Cursor {
            peer_id: self.peer_id.clone(),
            last_seq,
            updated_at,
        }))
    }

    /// Replaces the cursor atomically; a crash before rename keeps the old value.
    pub fn store(&self, last_seq: i64, updated_at: i64) -> Result<()> {
        let mut payload = [0u8; 16];
        payload[..8].copy_from_slice(&last_seq.to_le_bytes());
        payload[8..].copy_from_slice(&updated_at.to_le_bytes());
        atomic_write(&self.path, &encode_frame(KIND_CURSOR, &payload))
    }
}

/// 死信行（与 SQLite `site_apply_dead` 同列；payload 已按上限截断）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub peer_id: String,
    pub seq: i64,
    pub entity: String,
    pub entity_id: String,
    pub error: String,
    pub payload: String,
    pub created_at: i64,
}

/// Append-only dead-letter journal; `(peer_id, seq)` is unique on scan.
pub struct DeadFile {
    peer_id: String,
    path: PathBuf,
}

impl DeadFile {
    pub fn open(dir: impl Into<PathBuf>, peer_id: &str) -> Result<Self> {
        let dir = dir.into();
        ensure_dir(&dir)?;
        let path = key_file(&dir, peer_id, ".dead")?;
        Ok(Self {
            peer_id: peer_id.to_string(),
            path,
        })
    }

    /// `seq` ascending; a redelivered `(peer_id, seq)` keeps the first record.
    pub fn scan(&self) -> Result<Vec<DeadLetter>> {
        let Some(bytes) = read_file(&self.path)? else {
            return Ok(Vec::new());
        };
        let (frames, _) = scan_frames(&bytes, KIND_DEAD)?;
        let mut out: Vec<DeadLetter> = Vec::with_capacity(frames.len());
        for payload in frames {
            let row: DeadLetter = rmp_serde::from_slice(payload)
                .map_err(|e| corrupt(format!("bad dead-letter record: {e}")))?;
            if !out.iter().any(|have| have.seq == row.seq) {
                out.push(row);
            }
        }
        Ok(out)
    }

    pub fn count(&self) -> Result<usize> {
        Ok(self.scan()?.len())
    }

    /// No-op when `(peer_id, seq)` is already present; truncates a torn tail first.
    pub fn append(&self, row: &DeadLetter) -> Result<()> {
        if row.peer_id != self.peer_id {
            return Err(corrupt("dead-letter peer does not match its file"));
        }
        let bytes = read_file(&self.path)?.unwrap_or_default();
        let (frames, valid) = scan_frames(&bytes, KIND_DEAD)?;
        for payload in frames {
            let have: DeadLetter = rmp_serde::from_slice(payload)
                .map_err(|e| corrupt(format!("bad dead-letter record: {e}")))?;
            if have.seq == row.seq {
                return Ok(());
            }
        }
        if valid < bytes.len() {
            open_private(&self.path, false, false)?.set_len(valid as u64)?;
        }
        let payload = rmp_serde::to_vec_named(row)
            .map_err(|e| corrupt(format!("encode dead-letter record: {e}")))?;
        let mut file = open_private(&self.path, false, true)?;
        file.write_all(&encode_frame(KIND_DEAD, &payload))?;
        file.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::super::LocalStore;

    #[test]
    fn missing_file_loads_none_and_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = LocalStore::open(dir.path())
            .unwrap()
            .cursor("peer-a")
            .unwrap();
        assert_eq!(cursor.load().unwrap(), None);
        cursor.store(41, 1_000).unwrap();
        let loaded = cursor.load().unwrap().unwrap();
        assert_eq!(
            loaded,
            super::Cursor {
                peer_id: "peer-a".into(),
                last_seq: 41,
                updated_at: 1_000
            }
        );
        cursor.store(99, 2_000).unwrap();
        assert_eq!(cursor.load().unwrap().unwrap().last_seq, 99);
    }

    #[test]
    fn crash_before_rename_keeps_previous_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let cursor = store.cursor("peer-a").unwrap();
        cursor.store(10, 100).unwrap();
        let path = store
            .root()
            .join("site/cursors")
            .join(hex::encode("peer-a") + ".cursor");
        assert!(super::super::atomic_write_without_rename(&path, b"torn").is_err());
        assert_eq!(cursor.load().unwrap().unwrap().last_seq, 10);
    }

    #[test]
    fn dead_file_dedupes_by_seq_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let dead = store.dead("remote-b").unwrap();
        assert!(dead.scan().unwrap().is_empty());
        let row = super::DeadLetter {
            peer_id: "remote-b".into(),
            seq: 7,
            entity: "user".into(),
            entity_id: "u".into(),
            error: "未知 op".into(),
            payload: "{}".into(),
            created_at: 1,
        };
        dead.append(&row).unwrap();
        dead.append(&row).unwrap();
        assert_eq!(dead.count().unwrap(), 1);
        let reopened = LocalStore::open(dir.path())
            .unwrap()
            .dead("remote-b")
            .unwrap();
        assert_eq!(reopened.scan().unwrap(), vec![row]);
    }

    #[test]
    fn corrupt_cursor_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).unwrap();
        let cursor = store.cursor("peer-a").unwrap();
        cursor.store(1, 1).unwrap();
        let path = store
            .root()
            .join("site/cursors")
            .join(hex::encode("peer-a") + ".cursor");
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..5]).unwrap();
        assert_eq!(
            cursor.load().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
