//! Site root tables: `site_config`, `sites`, `site_changelog`, `site_entity_clock`.
//!
//! The changelog/clock are part of the root because they must commit in the same
//! CAS as the entity write (design §5.2). Trigger replacement lives in
//! [`super::site_apply`]; this file is the storage adapter plus explicit clock
//! writes for `SiteApply`.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
pub use xl_store::control_store::sites::{SiteChangelogRow, SiteEntityClockRow};
use xl_store::control_store::sites::{
    SiteCredentialsRow, SiteIdentityRow, SitePeerRow, SiteStore, SiteTxn,
};

use super::jobs::{backend, decode, encode, scan_store, scan_txn};
use super::{ObjectControlStore, ObjectControlTxn, TableId};

pub(super) const SITE_CONFIG_KEY: [u8; 1] = [1];

/// Full `site_config` row (the seam's identity view drops `created_at`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SiteConfigRow {
    pub id: String,
    pub name: String,
    pub region: String,
    pub priority: i64,
    pub site_token: String,
    pub created_at: i64,
}

pub(super) fn site_key(peer_id: &str) -> &[u8] {
    peer_id.as_bytes()
}

pub(crate) fn changelog_key(seq: i64) -> [u8; 8] {
    seq.to_be_bytes()
}

/// `entity` is ASCII and never contains NUL, so a NUL separator is unambiguous.
pub(crate) fn clock_key(entity: &str, entity_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(entity.len() + 1 + entity_id.len());
    key.extend_from_slice(entity.as_bytes());
    key.push(0);
    key.extend_from_slice(entity_id.as_bytes());
    key
}

pub(super) async fn config_row(
    store: &ObjectControlStore,
) -> Result<Option<SiteConfigRow>, ControlError> {
    match super::jobs::get_store(store, TableId::SiteConfig, &SITE_CONFIG_KEY).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) async fn config_row_txn(
    txn: &ObjectControlTxn,
) -> Result<Option<SiteConfigRow>, ControlError> {
    match txn
        .get(TableId::SiteConfig, &SITE_CONFIG_KEY)
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

async fn put_config(
    txn: &mut ObjectControlTxn,
    config: &SiteConfigRow,
) -> Result<(), ControlError> {
    txn.put(TableId::SiteConfig, &SITE_CONFIG_KEY, &encode(config)?)
        .await
        .map_err(backend)
}

async fn all_sites(store: &ObjectControlStore) -> Result<Vec<SitePeerRow>, ControlError> {
    let mut rows = Vec::new();
    for (_, value) in scan_store(store, TableId::Sites).await? {
        rows.push(decode::<SitePeerRow>(&value)?);
    }
    rows.sort_by(|a, b| {
        (a.created_at, a.peer_id.as_str()).cmp(&(b.created_at, b.peer_id.as_str()))
    });
    Ok(rows)
}

#[async_trait::async_trait]
impl SiteStore for ObjectControlStore {
    async fn site_identity(&self) -> Result<Option<SiteIdentityRow>, ControlError> {
        Ok(config_row(self).await?.map(|c| SiteIdentityRow {
            id: c.id,
            name: c.name,
            region: c.region,
            priority: c.priority,
            token: c.site_token,
        }))
    }

    async fn site(&self, peer_id: &str) -> Result<Option<SitePeerRow>, ControlError> {
        match super::jobs::get_store(self, TableId::Sites, site_key(peer_id)).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn sites(&self) -> Result<Vec<SitePeerRow>, ControlError> {
        all_sites(self).await
    }

    async fn site_count(&self) -> Result<i64, ControlError> {
        Ok(scan_store(self, TableId::Sites).await?.len() as i64)
    }

    async fn site_remote_id(&self, peer_id: &str) -> Result<Option<String>, ControlError> {
        Ok(self.site(peer_id).await?.map(|p| p.remote_site_id))
    }

    async fn first_site_remote_id(&self) -> Result<Option<String>, ControlError> {
        // SQLite uses `LIMIT 1` without ORDER BY; key order is the closest
        // deterministic stand-in (the M7 schema allows one peer in practice).
        match scan_store(self, TableId::Sites).await?.into_iter().next() {
            Some((_, value)) => Ok(Some(decode::<SitePeerRow>(&value)?.remote_site_id)),
            None => Ok(None),
        }
    }

    async fn site_peer_ids(&self) -> Result<Vec<String>, ControlError> {
        let mut ids: Vec<String> = scan_store(self, TableId::Sites)
            .await?
            .into_iter()
            .map(|(key, _)| String::from_utf8_lossy(&key).into_owned())
            .collect();
        ids.sort();
        Ok(ids)
    }

    async fn site_credentials(
        &self,
        peer_id: &str,
    ) -> Result<Option<SiteCredentialsRow>, ControlError> {
        Ok(self.site(peer_id).await?.map(|p| SiteCredentialsRow {
            endpoint: p.endpoint,
            s3_access_key: p.s3_access_key,
            s3_secret_key: p.s3_secret_key,
            region: p.region,
            peer_site_token: p.peer_site_token,
        }))
    }

    async fn site_cursor(&self, peer_id: &str) -> Result<Option<i64>, ControlError> {
        let local = self.local_store()?;
        let cursor = local
            .cursor(peer_id)
            .map_err(|e| ControlError::Backend(e.to_string()))?;
        Ok(cursor
            .load()
            .map_err(|e| ControlError::Backend(e.to_string()))?
            .map(|cursor| cursor.last_seq))
    }

    async fn site_lag_rows(&self) -> Result<i64, ControlError> {
        let max = ObjectControlStore::site_changelog_max_seq(self).await?;
        if max == 0 {
            return Ok(0);
        }
        let mut lag = 0;
        for peer_id in self.site_peer_ids().await? {
            if self.site_cursor(&peer_id).await?.unwrap_or(0) < max {
                lag += 1;
            }
        }
        Ok(lag)
    }

    async fn site_changelog_after(&self, seq: i64) -> Result<Vec<SiteChangelogRow>, ControlError> {
        ObjectControlStore::site_changelog_after(self, seq).await
    }

    async fn site_changelog_max_seq(&self) -> Result<i64, ControlError> {
        ObjectControlStore::site_changelog_max_seq(self).await
    }

    async fn site_clock(
        &self,
        entity: &str,
        entity_id: &str,
    ) -> Result<Option<SiteEntityClockRow>, ControlError> {
        ObjectControlStore::site_clock(self, entity, entity_id).await
    }
}

impl ObjectControlStore {
    pub async fn site_changelog_after(
        &self,
        seq: i64,
    ) -> Result<Vec<SiteChangelogRow>, ControlError> {
        let mut rows = Vec::new();
        for (_, value) in scan_store(self, TableId::SiteChangelog).await? {
            let row = decode::<SiteChangelogRow>(&value)?;
            if row.seq > seq {
                rows.push(row);
            }
        }
        rows.sort_by_key(|r| r.seq);
        Ok(rows)
    }

    pub async fn site_changelog_max_seq(&self) -> Result<i64, ControlError> {
        Ok(self
            .site_changelog_after(0)
            .await?
            .into_iter()
            .map(|r| r.seq)
            .max()
            .unwrap_or(0))
    }

    pub async fn site_clock(
        &self,
        entity: &str,
        entity_id: &str,
    ) -> Result<Option<SiteEntityClockRow>, ControlError> {
        match super::jobs::get_store(
            self,
            TableId::SiteEntityClock,
            &clock_key(entity, entity_id),
        )
        .await?
        {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl SiteTxn for ObjectControlTxn {
    async fn site_config_trim(&mut self) -> Result<u64, ControlError> {
        // One fixed key: duplicate rows (the old multi-row defect) cannot exist.
        Ok(0)
    }

    async fn site_config_id(&mut self) -> Result<Option<String>, ControlError> {
        Ok(config_row_txn(self).await?.map(|c| c.id))
    }

    async fn site_config_seed(
        &mut self,
        id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        if config_row_txn(self).await?.is_some() {
            return Ok(());
        }
        put_config(
            self,
            &SiteConfigRow {
                id: id.into(),
                name: "site".into(),
                region: "us-east-1".into(),
                priority: 0,
                site_token: token.into(),
                created_at,
            },
        )
        .await
    }

    async fn site_config_apply(
        &mut self,
        name: Option<&str>,
        region: Option<&str>,
        priority: Option<i64>,
        token: Option<&str>,
    ) -> Result<(), ControlError> {
        let Some(mut config) = config_row_txn(self).await? else {
            return Ok(());
        };
        if let Some(v) = name {
            config.name = v.into();
        }
        if let Some(v) = region {
            config.region = v.into();
        }
        if let Some(v) = priority {
            config.priority = v;
        }
        if let Some(v) = token {
            config.site_token = v.into();
        }
        put_config(self, &config).await
    }

    async fn site_pair_upsert(&mut self, peer: &SitePeerRow) -> Result<(), ControlError> {
        let mut updated = false;
        for (key, value) in scan_txn(self, TableId::Sites).await? {
            let mut row = decode::<SitePeerRow>(&value)?;
            if row.peer_id == peer.peer_id || row.remote_site_id == peer.remote_site_id {
                // UPDATE never touches `created_at` or `reconcile_required`.
                row.remote_site_id = peer.remote_site_id.clone();
                row.name = peer.name.clone();
                row.endpoint = peer.endpoint.clone();
                row.region = peer.region.clone();
                row.priority = peer.priority;
                row.s3_access_key = peer.s3_access_key.clone();
                row.s3_secret_key = peer.s3_secret_key.clone();
                row.peer_site_token = peer.peer_site_token.clone();
                row.raft_node_id = peer.raft_node_id;
                self.put(TableId::Sites, &key, &encode(&row)?)
                    .await
                    .map_err(backend)?;
                updated = true;
            }
        }
        if !updated {
            self.put(TableId::Sites, site_key(&peer.peer_id), &encode(peer)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn site_insert(&mut self, peer: &SitePeerRow, _now: i64) -> Result<(), ControlError> {
        let key = site_key(&peer.peer_id);
        if self
            .get(TableId::Sites, key)
            .await
            .map_err(backend)?
            .is_some()
        {
            return Err(ControlError::Backend(
                "UNIQUE constraint failed: sites.peer_id".into(),
            ));
        }
        self.put(TableId::Sites, key, &encode(peer)?)
            .await
            .map_err(backend)?;
        // shortcut: the peer cursor is a local file (design §5.2/§7); the pair
        // flow writes it before this CAS and D3 wires that call path.
        Ok(())
    }

    async fn site_remote_id(&mut self, peer_id: &str) -> Result<Option<String>, ControlError> {
        match self
            .get(TableId::Sites, site_key(peer_id))
            .await
            .map_err(backend)?
        {
            Some(bytes) => Ok(Some(decode::<SitePeerRow>(&bytes)?.remote_site_id)),
            None => Ok(None),
        }
    }

    async fn site_set_reconcile_required(
        &mut self,
        peer_id: &str,
        required: bool,
    ) -> Result<(), ControlError> {
        let key = site_key(peer_id);
        if let Some(bytes) = self.get(TableId::Sites, key).await.map_err(backend)? {
            let mut row = decode::<SitePeerRow>(&bytes)?;
            row.reconcile_required = required;
            self.put(TableId::Sites, key, &encode(&row)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn site_update_ping(
        &mut self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<(), ControlError> {
        let key = site_key(peer_id);
        if let Some(bytes) = self.get(TableId::Sites, key).await.map_err(backend)? {
            let mut row = decode::<SitePeerRow>(&bytes)?;
            row.last_seen = Some(ts);
            row.last_error = error.map(str::to_owned);
            self.put(TableId::Sites, key, &encode(&row)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn site_cursor_delete(&mut self, _peer_id: &str) -> Result<u64, ControlError> {
        // shortcut: cursor files are local state; caller deletes them.
        Ok(0)
    }

    async fn site_set_last_error(
        &mut self,
        peer_id: &str,
        error: Option<&str>,
    ) -> Result<(), ControlError> {
        self.set_last_error_where(site_key(peer_id), error).await
    }

    async fn site_set_last_error_for_remote(
        &mut self,
        remote_site_id: &str,
        error: Option<&str>,
    ) -> Result<(), ControlError> {
        let key = scan_txn(self, TableId::Sites)
            .await?
            .into_iter()
            .find(|(_, value)| {
                decode::<SitePeerRow>(value).is_ok_and(|row| row.remote_site_id == remote_site_id)
            })
            .map(|(key, _)| key);
        match key {
            Some(key) => self.set_last_error_where(&key, error).await,
            None => Ok(()),
        }
    }

    async fn site_put_clock(
        &mut self,
        entity: &str,
        entity_id: &str,
        ts: i64,
        priority: i64,
        origin_site_id: &str,
        seq: i64,
    ) -> Result<(), ControlError> {
        ObjectControlTxn::site_put_clock(self, entity, entity_id, ts, priority, origin_site_id, seq)
            .await
    }

    async fn site_delete(&mut self, peer_id: &str) -> Result<u64, ControlError> {
        Ok(u64::from(
            self.delete(TableId::Sites, site_key(peer_id))
                .await
                .map_err(backend)?,
        ))
    }
}

impl ObjectControlTxn {
    async fn set_last_error_where(
        &mut self,
        key: &[u8],
        error: Option<&str>,
    ) -> Result<(), ControlError> {
        if let Some(bytes) = self.get(TableId::Sites, key).await.map_err(backend)? {
            let mut row = decode::<SitePeerRow>(&bytes)?;
            row.last_error = error.map(str::to_owned);
            self.put(TableId::Sites, key, &encode(&row)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}
