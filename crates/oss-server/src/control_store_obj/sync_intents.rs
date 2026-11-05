//! Local-file sync write/delete intent adapters (design D4 / §7).
//!
//! Each intent is a KV record keyed by its id. `mark_done` rewrites state and
//! `updated_at`, which a queue journal cannot do in place without replaying
//! payloads; KV keeps recovery scans and status updates crash-safe.
//!
//! shortcut: `delete_all` and the duplicate-id error are not atomic across
//! processes (SQL `DELETE`/PK are). Intents are written by one node's sync
//! worker; upgrade to a lock if intents can be shared by concurrent writers.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::sync_intents::{
    NewSyncDeleteIntent, NewSyncWriteIntent, SyncDeleteIntentRow, SyncIntentStore, SyncIntentTxn,
    SyncWriteIntentRow,
};

use super::local_ops::{SYNC_DELETE_KV, SYNC_WRITE_KV, backend, decode, encode, kv, limit_of};
use super::{ObjectControlStore, ObjectControlTxn};

#[derive(Debug, Serialize, Deserialize)]
struct StoredWriteIntent {
    id: String,
    bucket: String,
    key: String,
    local_version_id: String,
    remote_version_id: String,
    tee_path: String,
    commit_json: String,
    state: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredDeleteIntent {
    id: String,
    bucket: String,
    key: String,
    op: String,
    local_version_id: Option<String>,
    remote_version_id: Option<String>,
    marker_version_id: Option<String>,
    state: String,
    created_at: i64,
    updated_at: i64,
}

fn write_intents(store: &ObjectControlStore) -> Result<Vec<StoredWriteIntent>, ControlError> {
    kv(store.local_store()?, SYNC_WRITE_KV)?
        .scan()
        .map_err(backend)?
        .into_iter()
        .map(|entry| decode(&entry.value))
        .collect()
}

fn delete_intents(store: &ObjectControlStore) -> Result<Vec<StoredDeleteIntent>, ControlError> {
    kv(store.local_store()?, SYNC_DELETE_KV)?
        .scan()
        .map_err(backend)?
        .into_iter()
        .map(|entry| decode(&entry.value))
        .collect()
}

#[async_trait::async_trait]
impl SyncIntentStore for ObjectControlStore {
    async fn sync_write_intents_stale(
        &self,
        cutoff: i64,
    ) -> Result<Vec<SyncWriteIntentRow>, ControlError> {
        Ok(write_intents(self)?
            .into_iter()
            .filter(|row| row.updated_at < cutoff)
            .map(|row| SyncWriteIntentRow {
                id: row.id,
                bucket: row.bucket,
                key: row.key,
            })
            .collect())
    }

    async fn sync_write_intent_tee(&self, id: &str) -> Result<Option<String>, ControlError> {
        let Some(bytes) = kv(self.local_store()?, SYNC_WRITE_KV)?
            .get(id)
            .map_err(backend)?
        else {
            return Ok(None);
        };
        let row: StoredWriteIntent = decode(&bytes)?;
        Ok(Some(row.tee_path))
    }

    async fn sync_write_intent_tees(&self) -> Result<Vec<String>, ControlError> {
        Ok(write_intents(self)?
            .into_iter()
            .map(|row| row.tee_path)
            .collect())
    }

    async fn sync_delete_intents_stale(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<SyncDeleteIntentRow>, ControlError> {
        let mut rows: Vec<_> = delete_intents(self)?
            .into_iter()
            .filter(|row| row.updated_at < cutoff)
            .collect();
        // SQL `ORDER BY updated_at LIMIT ?`; id breaks ties deterministically.
        rows.sort_by(|a, b| (a.updated_at, &a.id).cmp(&(b.updated_at, &b.id)));
        if let Some(limit) = limit_of(limit) {
            rows.truncate(limit);
        }
        Ok(rows
            .into_iter()
            .map(|row| SyncDeleteIntentRow {
                id: row.id,
                bucket: row.bucket,
                key: row.key,
                op: row.op,
                local_version_id: row.local_version_id,
                remote_version_id: row.remote_version_id,
                marker_version_id: row.marker_version_id,
                state: row.state,
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl SyncIntentTxn for ObjectControlTxn {
    async fn sync_write_intent_insert(
        &mut self,
        new: &NewSyncWriteIntent,
    ) -> Result<(), ControlError> {
        let store = kv(self.local_store()?, SYNC_WRITE_KV)?;
        let bytes = encode(&StoredWriteIntent {
            id: new.id.clone(),
            bucket: new.bucket.clone(),
            key: new.key.clone(),
            local_version_id: new.local_version_id.clone(),
            remote_version_id: new.remote_version_id.clone(),
            tee_path: new.tee_path.clone(),
            commit_json: new.commit_json.clone(),
            state: new.state.clone(),
            created_at: new.created_at,
            updated_at: new.updated_at,
        })?;
        if !store.put_if_absent(&new.id, &bytes).map_err(backend)? {
            return Err(ControlError::Backend(format!(
                "duplicate sync_write_intents id {}",
                new.id
            )));
        }
        Ok(())
    }

    async fn sync_write_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError> {
        let store = kv(self.local_store()?, SYNC_WRITE_KV)?;
        let Some(bytes) = store.get(id).map_err(backend)? else {
            return Ok(0);
        };
        let mut row: StoredWriteIntent = decode(&bytes)?;
        row.state = crate::sync::INTENT_DONE.to_string();
        row.updated_at = now;
        store.put(id, &encode(&row)?).map_err(backend)?;
        Ok(1)
    }

    async fn sync_write_intent_delete(&mut self, id: &str) -> Result<u64, ControlError> {
        let deleted = kv(self.local_store()?, SYNC_WRITE_KV)?
            .delete(id)
            .map_err(backend)?;
        Ok(u64::from(deleted))
    }

    async fn sync_write_intent_count(&mut self) -> Result<i64, ControlError> {
        Ok(kv(self.local_store()?, SYNC_WRITE_KV)?
            .scan()
            .map_err(backend)?
            .len() as i64)
    }

    async fn sync_write_intent_delete_all(&mut self) -> Result<u64, ControlError> {
        let store = kv(self.local_store()?, SYNC_WRITE_KV)?;
        let ids: Vec<_> = store
            .scan()
            .map_err(backend)?
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        let mut deleted = 0;
        for id in ids {
            if store.delete(&id).map_err(backend)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    async fn sync_delete_intent_insert(
        &mut self,
        new: &NewSyncDeleteIntent,
    ) -> Result<(), ControlError> {
        let store = kv(self.local_store()?, SYNC_DELETE_KV)?;
        let bytes = encode(&StoredDeleteIntent {
            id: new.id.clone(),
            bucket: new.bucket.clone(),
            key: new.key.clone(),
            op: new.op.clone(),
            local_version_id: Some(new.local_version_id.clone()),
            remote_version_id: Some(new.remote_version_id.clone()),
            marker_version_id: Some(new.marker_version_id.clone()),
            state: new.state.clone(),
            created_at: new.created_at,
            updated_at: new.updated_at,
        })?;
        if !store.put_if_absent(&new.id, &bytes).map_err(backend)? {
            return Err(ControlError::Backend(format!(
                "duplicate sync_delete_intents id {}",
                new.id
            )));
        }
        Ok(())
    }

    async fn sync_delete_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError> {
        let store = kv(self.local_store()?, SYNC_DELETE_KV)?;
        let Some(bytes) = store.get(id).map_err(backend)? else {
            return Ok(0);
        };
        let mut row: StoredDeleteIntent = decode(&bytes)?;
        row.state = crate::sync::INTENT_DONE.to_string();
        row.updated_at = now;
        store.put(id, &encode(&row)?).map_err(backend)?;
        Ok(1)
    }

    async fn sync_delete_intent_delete(&mut self, id: &str) -> Result<u64, ControlError> {
        let deleted = kv(self.local_store()?, SYNC_DELETE_KV)?
            .delete(id)
            .map_err(backend)?;
        Ok(u64::from(deleted))
    }

    async fn sync_delete_intent_count(&mut self) -> Result<i64, ControlError> {
        Ok(kv(self.local_store()?, SYNC_DELETE_KV)?
            .scan()
            .map_err(backend)?
            .len() as i64)
    }

    async fn sync_delete_intent_delete_all(&mut self) -> Result<u64, ControlError> {
        let store = kv(self.local_store()?, SYNC_DELETE_KV)?;
        let ids: Vec<_> = store
            .scan()
            .map_err(backend)?
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        let mut deleted = 0;
        for id in ids {
            if store.delete(&id).map_err(backend)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}
