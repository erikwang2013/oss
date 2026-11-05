//! Generation-tagged read view over the object control root (design D7/§4).
//!
//! A [`HotView`] is immutable for one root generation and owns copy-on-write
//! maps of the hot auth/ACL/bucket domains. [`ViewCache::publish`] merges only
//! the tables a commit dirtied; snapshot install calls [`ViewCache::rebuild`].
//! Every request captures one [`RequestView`], so auth, ACL and quota decisions
//! inside that request observe a single generation even if a commit lands.
//!
//! Secrets stay in stored form: a sealed `enc:v1:` value is never decrypted
//! here, and the view's `Debug` output never prints key material.
//!
//! Memory: [`HotView::estimated_bytes`] approximates resident map bytes. Loads
//! fail closed above the cap passed to [`HotView::load_with_cap`]; the default
//! [`HotView::DEFAULT_MAX_BYTES`] is the design's 256 MiB at 100k users/AKs/buckets.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use xl_s3::access::account_id_of;
use xl_store::control_state::node::LeafValue;
use xl_store::control_state::tree::TableTree;
use xl_store::control_store::bucket::{BucketPolicyEntry, EventConfigEntry};
use xl_store::control_store::sites::SitePeerRow;
use xl_store::control_store::{
    AccessPointEntry, ControlError, OlapConfigEntry, OwnerRef, VpcEndpoint,
};

use crate::control_store_obj::{
    AccessKeyRow, AccountRow, BucketRow, LedgerRow, MemberRow, ObjectControlStore,
    PermissionStorageRow, PinnedRoot, TableId, UserRow,
};

mod acl;
mod auth;
mod cache;
mod mem;
#[cfg(test)]
mod tests;

pub use cache::{CacheDelta, ViewCache};

const ALL_END: [u8; 256] = [0xff; 256];

fn backend(e: impl fmt::Display) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// Decode every row of `table` from one pinned generation (blobs included).
async fn scan_at<T: serde::de::DeserializeOwned>(
    store: &ObjectControlStore,
    pinned: &PinnedRoot,
    table: TableId,
) -> Result<Vec<T>, ControlError> {
    let Some(root) = pinned.root.table(table.as_u16()) else {
        return Ok(Vec::new());
    };
    let tree = TableTree::from_root(root.hash, root.rows, root.bytes);
    let mut out = Vec::new();
    for (_, value) in tree
        .scan_leaf(&*store.root, &b""[..]..&ALL_END[..])
        .await
        .map_err(backend)?
    {
        let bytes = match value {
            LeafValue::Row(bytes) => bytes,
            LeafValue::Blob(blob) => store.root.get_blob(&blob).await.map_err(backend)?,
            LeafValue::Tombstone => continue,
        };
        out.push(rmp_serde::from_slice(&bytes).map_err(backend)?);
    }
    Ok(out)
}

/// Access-key row as held by the view. `stored_secret` is copied verbatim.
#[derive(Clone, PartialEq, Eq)]
struct ViewKey {
    user_id: i64,
    status: String,
    expires_at: Option<i64>,
    parent_ak: Option<String>,
    session_policy: Option<String>,
    kind: String,
    stored_secret: String,
}

impl From<AccessKeyRow> for ViewKey {
    fn from(row: AccessKeyRow) -> Self {
        Self {
            user_id: row.user_id,
            status: row.status,
            expires_at: row.expires_at,
            parent_ak: row.parent_ak,
            session_policy: row.session_policy,
            kind: row.kind,
            stored_secret: row.secret_key,
        }
    }
}

impl fmt::Debug for ViewKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewKey")
            .field("user_id", &self.user_id)
            .field("status", &self.status)
            .field("expires_at", &self.expires_at)
            .field("parent_ak", &self.parent_ak)
            .field("kind", &self.kind)
            .field("stored_secret", &"<redacted>")
            .finish()
    }
}

/// Stored access-point row (`owner_id` is resolved to a natural key on load).
#[derive(Deserialize, serde::Serialize)]
struct StoredAp {
    name: String,
    bucket: String,
    owner_id: i64,
    policy_json: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Clone, PartialEq, Eq, Deserialize, serde::Serialize, Debug)]
struct StoredSiteConfig {
    id: String,
    name: String,
    region: String,
    priority: i64,
    site_token: String,
}

/// Immutable maps for one root generation.
#[derive(Clone, PartialEq)]
pub struct HotView {
    generation: u64,
    users: Arc<HashMap<i64, UserRow>>,
    usernames: Arc<HashMap<String, i64>>,
    keys: Arc<HashMap<String, ViewKey>>,
    accounts: Arc<HashMap<String, AccountRow>>,
    members: Arc<HashMap<i64, MemberRow>>,
    ledger: Arc<HashMap<String, LedgerRow>>,
    buckets: Arc<HashMap<String, BucketRow>>,
    policies: Arc<HashMap<String, BucketPolicyEntry>>,
    events: Arc<HashMap<String, EventConfigEntry>>,
    permissions: Arc<HashMap<i64, HashMap<String, PermissionStorageRow>>>,
    aps: Arc<HashMap<String, AccessPointEntry>>,
    olap: Arc<HashMap<String, OlapConfigEntry>>,
    vpcs: Arc<HashMap<String, VpcEndpoint>>,
    site_config: Option<StoredSiteConfig>,
    sites: Arc<HashMap<String, SitePeerRow>>,
    /// `ACCOUNT_BUCKET_COUNT_SQL`: account → live non-admin owned bucket count.
    bucket_counts: Arc<HashMap<String, i64>>,
    admin_uid: Option<i64>,
}

impl fmt::Debug for HotView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotView")
            .field("generation", &self.generation)
            .field("users", &self.users.len())
            .field("keys", &format_args!("{}(redacted)", self.keys.len()))
            .field("accounts", &self.accounts.len())
            .field("members", &self.members.len())
            .field("buckets", &self.buckets.len())
            .field("permissions", &self.permissions.len())
            .field("aps", &self.aps.len())
            .field("sites", &self.sites.len())
            .finish()
    }
}

fn usernames(users: &HashMap<i64, UserRow>) -> HashMap<String, i64> {
    users
        .values()
        .map(|u| (u.username.clone(), u.uid))
        .collect()
}

fn admin_uid(users: &HashMap<i64, UserRow>) -> Option<i64> {
    users.values().filter(|u| u.is_admin).map(|u| u.uid).min()
}

fn bucket_counts(
    buckets: &HashMap<String, BucketRow>,
    users: &HashMap<i64, UserRow>,
    members: &HashMap<i64, MemberRow>,
) -> HashMap<String, i64> {
    let mut out = HashMap::new();
    for bucket in buckets.values() {
        let Some(uid) = bucket.owner_uid else {
            continue;
        };
        let Some(user) = users.get(&uid) else {
            continue;
        };
        if user.is_admin {
            continue;
        }
        let Some(account) = members
            .get(&uid)
            .map(|m| m.account_id.clone())
            .or_else(|| account_id_of(uid))
        else {
            continue;
        };
        *out.entry(account).or_insert(0) += 1;
    }
    out
}

fn ap_entry(row: StoredAp, users: &HashMap<i64, UserRow>) -> AccessPointEntry {
    let owner = if row.owner_id == 0 {
        OwnerRef::Admin
    } else {
        match users.get(&row.owner_id) {
            Some(user) => OwnerRef::User(user.username.clone()),
            None => OwnerRef::Admin,
        }
    };
    AccessPointEntry {
        name: row.name,
        bucket: row.bucket,
        owner,
        policy_json: row.policy_json,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

impl HotView {
    /// Design §4 budget: 100k users/AKs/buckets fit in 256 MiB.
    pub const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

    /// Load a generation-consistent view at the default memory cap.
    pub async fn load(store: &ObjectControlStore) -> Result<Arc<Self>, ControlError> {
        Self::load_with_cap(store, Self::DEFAULT_MAX_BYTES).await
    }

    /// Pin, scan, and retry until both pins agree so no generation mixes.
    pub async fn load_with_cap(
        store: &ObjectControlStore,
        max_bytes: usize,
    ) -> Result<Arc<Self>, ControlError> {
        loop {
            let before = store.pin().await.map_err(backend)?;
            let view = Self::load_at(store, &before).await?;
            let after = store.pin().await.map_err(backend)?;
            if after.version != before.version {
                continue;
            }
            let bytes = view.estimated_bytes();
            if bytes > max_bytes {
                return Err(ControlError::Invalid(format!(
                    "control view is {bytes} bytes, over the {max_bytes} byte cap"
                )));
            }
            return Ok(Arc::new(view));
        }
    }

    async fn load_at(
        store: &ObjectControlStore,
        pinned: &PinnedRoot,
    ) -> Result<Self, ControlError> {
        let users: HashMap<i64, UserRow> = scan_at::<UserRow>(store, pinned, TableId::Users)
            .await?
            .into_iter()
            .map(|u| (u.uid, u))
            .collect();
        let keys: HashMap<String, ViewKey> =
            scan_at::<AccessKeyRow>(store, pinned, TableId::AccessKeys)
                .await?
                .into_iter()
                .map(|k| (k.access_key.clone(), ViewKey::from(k)))
                .collect();
        let mut permissions: HashMap<i64, HashMap<String, PermissionStorageRow>> = HashMap::new();
        for row in scan_at::<PermissionStorageRow>(store, pinned, TableId::Permissions).await? {
            permissions
                .entry(row.user_id)
                .or_default()
                .insert(row.bucket.clone(), row);
        }
        let accounts: HashMap<String, AccountRow> =
            scan_at::<AccountRow>(store, pinned, TableId::Accounts)
                .await?
                .into_iter()
                .map(|a| (a.id.clone(), a))
                .collect();
        let members: HashMap<i64, MemberRow> =
            scan_at::<MemberRow>(store, pinned, TableId::AccountMembers)
                .await?
                .into_iter()
                .map(|m| (m.uid, m))
                .collect();
        let ledger: HashMap<String, LedgerRow> =
            scan_at::<LedgerRow>(store, pinned, TableId::AccountUsageLedger)
                .await?
                .into_iter()
                .map(|l| (l.account_id.clone(), l))
                .collect();
        let buckets: HashMap<String, BucketRow> =
            scan_at::<BucketRow>(store, pinned, TableId::Buckets)
                .await?
                .into_iter()
                .map(|b| (b.name.clone(), b))
                .collect();
        let policies = scan_at::<BucketPolicyEntry>(store, pinned, TableId::BucketPolicies)
            .await?
            .into_iter()
            .map(|p| (p.bucket.clone(), p))
            .collect();
        let events = scan_at::<EventConfigEntry>(store, pinned, TableId::EventConfigs)
            .await?
            .into_iter()
            .map(|e| (e.bucket.clone(), e))
            .collect();
        let aps = scan_at::<StoredAp>(store, pinned, TableId::AccessPoints)
            .await?
            .into_iter()
            .map(|r| ap_entry(r, &users))
            .map(|e| (e.name.clone(), e))
            .collect();
        let olap = scan_at::<OlapConfigEntry>(store, pinned, TableId::OlapConfigs)
            .await?
            .into_iter()
            .map(|c| (c.access_point.clone(), c))
            .collect();
        let vpcs = scan_at::<VpcEndpoint>(store, pinned, TableId::VpcEndpoints)
            .await?
            .into_iter()
            .map(|v| (v.vpce_id.clone(), v))
            .collect();
        let site_config = scan_at::<StoredSiteConfig>(store, pinned, TableId::SiteConfig)
            .await?
            .into_iter()
            .next();
        let sites = scan_at::<SitePeerRow>(store, pinned, TableId::Sites)
            .await?
            .into_iter()
            .map(|s| (s.peer_id.clone(), s))
            .collect();

        let bucket_counts = bucket_counts(&buckets, &users, &members);
        Ok(Self {
            generation: pinned.version,
            usernames: Arc::new(usernames(&users)),
            admin_uid: admin_uid(&users),
            users: Arc::new(users),
            keys: Arc::new(keys),
            accounts: Arc::new(accounts),
            members: Arc::new(members),
            ledger: Arc::new(ledger),
            buckets: Arc::new(buckets),
            policies: Arc::new(policies),
            events: Arc::new(events),
            permissions: Arc::new(permissions),
            aps: Arc::new(aps),
            olap: Arc::new(olap),
            vpcs: Arc::new(vpcs),
            site_config,
            sites: Arc::new(sites),
            bucket_counts: Arc::new(bucket_counts),
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn capture(self: &Arc<Self>) -> RequestView {
        RequestView {
            generation: self.generation,
            view: Arc::clone(self),
        }
    }

    pub fn access_point(&self, name: &str) -> Option<&AccessPointEntry> {
        self.aps.get(name)
    }

    /// `name` ascending.
    pub fn access_points(&self) -> Vec<AccessPointEntry> {
        let mut out: Vec<AccessPointEntry> = self.aps.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn olap_config(&self, access_point: &str) -> Option<&OlapConfigEntry> {
        self.olap.get(access_point)
    }

    pub fn vpc_endpoint(&self, vpce_id: &str) -> Option<&VpcEndpoint> {
        self.vpcs.get(vpce_id)
    }

    pub fn bucket_count(&self, account_id: &str) -> i64 {
        self.bucket_counts.get(account_id).copied().unwrap_or(0)
    }
}

/// One request's pinned view. Cheap to clone; decisions inside the request all
/// read the same generation.
#[derive(Clone, PartialEq, Debug)]
pub struct RequestView {
    pub generation: u64,
    pub view: Arc<HotView>,
}

impl RequestView {
    pub fn new(view: Arc<HotView>) -> Self {
        Self {
            generation: view.generation,
            view,
        }
    }
}
