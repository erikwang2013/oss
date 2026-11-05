//! Object-backed access-point / OLAP adapters.
//!
//! The seam declares these methods on `ControlStore`/`ControlTxn` rather than a
//! domain trait, so B2 exposes matching local traits; B4 can dispatch through
//! them. Runtime reads are lenient (dangling/0 owner → `Admin`), snapshot export
//! is strict (`Invalid` on a dangling non-zero owner). OLAP modules above the
//! 16 KiB inline limit are automatically chunked by `ObjectControlTxn::put`.

use serde::{Deserialize, Serialize};
use xl_store::control_store::{AccessPointEntry, ControlError, OlapConfigEntry, OwnerRef};

use super::bucket::{
    CResult, UserLookup, admin_uid, backend, clear_table, decode_row, encode_row, key1,
    store_scan_all, txn_scan_all, user_by_name,
};
use super::{ObjectControlStore, ObjectControlTxn, TableId};

#[async_trait::async_trait]
pub trait AccessPointStore: Send + Sync {
    async fn access_point(&self, name: &str) -> CResult<Option<AccessPointEntry>>;
    /// `name` ascending.
    async fn access_points(&self) -> CResult<Vec<AccessPointEntry>>;
    async fn olap_config(&self, access_point: &str) -> CResult<Option<OlapConfigEntry>>;
    /// `access_point` ascending.
    async fn olap_configs(&self) -> CResult<Vec<OlapConfigEntry>>;
}

#[async_trait::async_trait]
pub trait AccessPointTxn: Send {
    async fn access_point_create(&mut self, row: &AccessPointEntry) -> CResult<bool>;
    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> CResult<bool>;
    async fn access_point_delete(&mut self, name: &str) -> CResult<bool>;
    async fn access_point_exists(&mut self, name: &str) -> CResult<bool>;
    async fn access_points(&mut self) -> CResult<Vec<AccessPointEntry>>;
    async fn replace_access_points(&mut self, rows: &[AccessPointEntry]) -> CResult<()>;
    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> CResult<()>;
    async fn olap_config_delete(&mut self, access_point: &str) -> CResult<bool>;
    async fn olap_configs(&mut self) -> CResult<Vec<OlapConfigEntry>>;
    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> CResult<()>;
    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> CResult<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApRow {
    name: String,
    bucket: String,
    owner_id: i64,
    policy_json: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl ApRow {
    fn entry(self, owner: OwnerRef) -> AccessPointEntry {
        AccessPointEntry {
            name: self.name,
            bucket: self.bucket,
            owner,
            policy_json: self.policy_json,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

async fn runtime_entry<L: UserLookup + ?Sized>(dir: &L, row: ApRow) -> CResult<AccessPointEntry> {
    let owner = if row.owner_id == 0 {
        OwnerRef::Admin
    } else {
        match dir.user_by_uid(row.owner_id).await? {
            Some(user) => OwnerRef::User(user.username),
            None => OwnerRef::Admin,
        }
    };
    Ok(row.entry(owner))
}

async fn strict_entry<L: UserLookup + ?Sized>(dir: &L, row: ApRow) -> CResult<AccessPointEntry> {
    let owner = if row.owner_id == 0 {
        OwnerRef::Admin
    } else {
        match dir.user_by_uid(row.owner_id).await? {
            Some(user) if user.is_admin => OwnerRef::Admin,
            Some(user) => OwnerRef::User(user.username),
            None => {
                return Err(ControlError::Invalid(format!(
                    "owner user id {} missing",
                    row.owner_id
                )));
            }
        }
    };
    Ok(row.entry(owner))
}

/// `Admin` → first admin uid or 0 (witness); `User` → any matching uid; missing
/// user with an admin present → `Invalid`; witness → 0.
async fn resolve_owner(txn: &ObjectControlTxn, owner: &OwnerRef) -> CResult<i64> {
    let admin = admin_uid(txn).await?;
    match owner {
        OwnerRef::Admin => Ok(admin.unwrap_or(0)),
        OwnerRef::User(username) => match user_by_name(txn, username).await? {
            Some((uid, _)) => Ok(uid),
            None if admin.is_some() => Err(ControlError::Invalid(format!(
                "owner user {username} not found"
            ))),
            None => Ok(0),
        },
    }
}

async fn get_ap(txn: &ObjectControlTxn, name: &str) -> CResult<Option<ApRow>> {
    let row = txn
        .get(TableId::AccessPoints, &key1(name))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

async fn put_ap(txn: &mut ObjectControlTxn, row: &ApRow) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(TableId::AccessPoints, &key1(&row.name), &bytes)
        .await
        .map_err(backend)
}

async fn put_olap(txn: &mut ObjectControlTxn, row: &OlapConfigEntry) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(TableId::OlapConfigs, &key1(&row.access_point), &bytes)
        .await
        .map_err(backend)
}

#[async_trait::async_trait]
impl AccessPointStore for ObjectControlStore {
    async fn access_point(&self, name: &str) -> CResult<Option<AccessPointEntry>> {
        let Some(bytes) = self
            .read_row(TableId::AccessPoints, &key1(name))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        runtime_entry(self, decode_row(&bytes)?).await.map(Some)
    }

    async fn access_points(&self) -> CResult<Vec<AccessPointEntry>> {
        let mut out = Vec::new();
        for (_, value) in store_scan_all(self, TableId::AccessPoints).await? {
            out.push(runtime_entry(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn olap_config(&self, access_point: &str) -> CResult<Option<OlapConfigEntry>> {
        let Some(bytes) = self
            .read_row(TableId::OlapConfigs, &key1(access_point))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        decode_row(&bytes).map(Some)
    }

    async fn olap_configs(&self) -> CResult<Vec<OlapConfigEntry>> {
        store_scan_all(self, TableId::OlapConfigs)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }
}

#[async_trait::async_trait]
impl AccessPointTxn for ObjectControlTxn {
    async fn access_point_create(&mut self, row: &AccessPointEntry) -> CResult<bool> {
        if self
            .get(TableId::AccessPoints, &key1(&row.name))
            .await
            .map_err(backend)?
            .is_some()
        {
            return Ok(false);
        }
        let stored = ApRow {
            name: row.name.clone(),
            bucket: row.bucket.clone(),
            owner_id: resolve_owner(self, &row.owner).await?,
            policy_json: row.policy_json.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        put_ap(self, &stored).await?;
        Ok(true)
    }

    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> CResult<bool> {
        let Some(mut row) = get_ap(self, name).await? else {
            return Ok(false);
        };
        row.policy_json = policy_json.map(Into::into);
        row.updated_at = updated_at;
        put_ap(self, &row).await?;
        Ok(true)
    }

    async fn access_point_delete(&mut self, name: &str) -> CResult<bool> {
        self.delete(TableId::OlapConfigs, &key1(name))
            .await
            .map_err(backend)?;
        self.delete(TableId::AccessPoints, &key1(name))
            .await
            .map_err(backend)
    }

    async fn access_point_exists(&mut self, name: &str) -> CResult<bool> {
        Ok(get_ap(self, name).await?.is_some())
    }

    async fn access_points(&mut self) -> CResult<Vec<AccessPointEntry>> {
        let mut out = Vec::new();
        for (_, value) in txn_scan_all(self, TableId::AccessPoints).await? {
            out.push(strict_entry(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn replace_access_points(&mut self, rows: &[AccessPointEntry]) -> CResult<()> {
        clear_table(self, TableId::OlapConfigs).await?;
        clear_table(self, TableId::AccessPoints).await?;
        for row in rows {
            let stored = ApRow {
                name: row.name.clone(),
                bucket: row.bucket.clone(),
                owner_id: resolve_owner(self, &row.owner).await?,
                policy_json: row.policy_json.clone(),
                created_at: row.created_at,
                updated_at: row.updated_at,
            };
            put_ap(self, &stored).await?;
        }
        Ok(())
    }

    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> CResult<()> {
        put_olap(self, row).await
    }

    async fn olap_config_delete(&mut self, access_point: &str) -> CResult<bool> {
        self.delete(TableId::OlapConfigs, &key1(access_point))
            .await
            .map_err(backend)
    }

    async fn olap_configs(&mut self) -> CResult<Vec<OlapConfigEntry>> {
        txn_scan_all(self, TableId::OlapConfigs)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> CResult<()> {
        clear_table(self, TableId::OlapConfigs).await?;
        for row in rows {
            put_olap(self, row).await?;
        }
        Ok(())
    }

    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> CResult<()> {
        let mut doomed = Vec::new();
        for (key, value) in txn_scan_all(self, TableId::AccessPoints).await? {
            let row: ApRow = decode_row(&value)?;
            if row.bucket == bucket {
                doomed.push((key, row.name));
            }
        }
        for (_, name) in &doomed {
            self.delete(TableId::OlapConfigs, &key1(name))
                .await
                .map_err(backend)?;
        }
        for (key, _) in doomed {
            self.delete(TableId::AccessPoints, &key)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}
