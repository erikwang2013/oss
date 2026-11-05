//! Read the legacy SQLite Raft database (`raft_log`, `raft_meta`,
//! `raft_snapshot`) through the same decoders the live server uses.

use std::collections::BTreeMap;
use std::path::Path;

use openraft::{BasicNode, Entry, LogId, SnapshotMeta, Vote};
use sqlx::Row;

use super::source::{BoxError, checksum, error, open_readonly};
use crate::raft::TypeConfig;

pub(crate) struct RaftSource {
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub last_purged: Option<LogId<u64>>,
    pub entries: Vec<Entry<TypeConfig>>,
    pub entries_sha256: Option<String>,
    pub snapshot: Option<(SnapshotMeta<u64, BasicNode>, Vec<u8>)>,
    pub snapshot_sha256: Option<String>,
    pub checksum: Option<String>,
}

impl RaftSource {
    pub(crate) fn has_state(&self) -> bool {
        !self.entries.is_empty()
            || self.vote.is_some()
            || self.committed.is_some()
            || self.last_purged.is_some()
            || self.snapshot.is_some()
    }
}

pub(crate) async fn read_raft(path: &Path) -> Result<RaftSource, BoxError> {
    let mut out = RaftSource {
        vote: None,
        committed: None,
        last_purged: None,
        entries: Vec::new(),
        entries_sha256: None,
        snapshot: None,
        snapshot_sha256: None,
        checksum: None,
    };
    if !path.is_file() {
        return Ok(out);
    }
    let before = checksum(path)?;
    let mut conn = open_readonly(path).await?;
    sqlx::query("BEGIN").execute(&mut conn).await?;
    let mut by_index: BTreeMap<u64, Entry<TypeConfig>> = BTreeMap::new();
    for r in sqlx::query("SELECT start_idx, end_idx, data FROM raft_log_segment ORDER BY start_idx")
        .fetch_all(&mut conn)
        .await?
    {
        let start: i64 = r.get("start_idx");
        let end: i64 = r.get("end_idx");
        let data: Vec<u8> = r.get("data");
        let entries = crate::raft::store::decode_segment(&data, start, end)
            .map_err(|e| std::io::Error::other(format!("raft segment: {e}")))?;
        for entry in entries {
            let idx = entry.log_id.index;
            if by_index.insert(idx, entry).is_some() {
                return error(format!("duplicate raft segment entry {idx}"));
            }
        }
    }
    for r in sqlx::query("SELECT idx, entry FROM raft_log ORDER BY idx")
        .fetch_all(&mut conn)
        .await?
    {
        let idx: i64 = r.get("idx");
        let text: String = r.get("entry");
        let entry = crate::raft::store::decode_entry(&text)
            .map_err(|e| std::io::Error::other(format!("raft entry {idx}: {e}")))?;
        if entry.log_id.index != idx as u64 {
            return error(format!(
                "raft row index {idx} decodes as {}",
                entry.log_id.index
            ));
        }
        if by_index.insert(idx as u64, entry).is_some() {
            return error(format!("duplicate raft entry {idx}"));
        }
    }
    let mut expected: Option<u64> = None;
    for (idx, entry) in by_index {
        if let Some(prev) = expected
            && idx != prev
        {
            return error(format!("raft log gap: {prev} -> {idx}"));
        }
        expected = Some(idx + 1);
        out.entries.push(entry);
    }
    if !out.entries.is_empty() {
        out.entries_sha256 = Some(entries_digest(&out.entries)?);
    }
    for r in sqlx::query("SELECT key, value FROM raft_meta")
        .fetch_all(&mut conn)
        .await?
    {
        let key: String = r.get("key");
        let value: String = r.get("value");
        match key.as_str() {
            "vote" => out.vote = Some(decode_meta::<Vote<u64>>(&value, "vote")?),
            "committed" => out.committed = Some(decode_meta::<LogId<u64>>(&value, "committed")?),
            "last_purged" => {
                out.last_purged = Some(decode_meta::<LogId<u64>>(&value, "last_purged")?)
            }
            _ => {}
        }
    }
    if let Some(row) = sqlx::query("SELECT meta, data FROM raft_snapshot WHERE id = 1")
        .fetch_optional(&mut conn)
        .await?
    {
        let meta_text: String = row.get("meta");
        let meta: SnapshotMeta<u64, BasicNode> = serde_json::from_str(&meta_text)
            .map_err(|e| std::io::Error::other(format!("raft snapshot meta: {e}")))?;
        let stored: Vec<u8> = row.get("data");
        let bytes = crate::raft::store::read_snapshot_data(
            &mut conn,
            &meta.snapshot_id,
            stored,
            512 * 1024 * 1024,
        )
        .await
        .map_err(|e| std::io::Error::other(format!("raft snapshot: {e}")))?;
        out.snapshot_sha256 = Some(sha256(&bytes));
        out.snapshot = Some((meta, bytes));
    }
    sqlx::query("COMMIT").execute(&mut conn).await?;
    drop(conn);
    let after = checksum(path)?;
    if before != after {
        return error(format!(
            "raft source changed during migration: {before} -> {after}"
        ));
    }
    out.checksum = Some(before);
    Ok(out)
}

fn decode_meta<T: serde::de::DeserializeOwned>(value: &str, what: &str) -> Result<T, BoxError> {
    serde_json::from_str(value)
        .map_err(|e| std::io::Error::other(format!("raft_meta.{what}: {e}")).into())
}

fn entries_digest(entries: &[Entry<TypeConfig>]) -> Result<String, BoxError> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for entry in entries {
        let bytes = serde_json::to_vec(entry)?;
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}
