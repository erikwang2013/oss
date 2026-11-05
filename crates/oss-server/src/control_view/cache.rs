//! Publication: merge only the tables a commit dirtied, or rebuild all.

use std::collections::HashSet;
use std::sync::RwLock;

use crate::control_store_obj::CommitStamp;

use super::*;

impl HotView {
    /// Rebuild only the maps whose tables are in `tables`; everything else is
    /// `Arc`-cloned. `pinned` must be the generation being published.
    async fn rebuild_domains(
        &self,
        store: &ObjectControlStore,
        pinned: &PinnedRoot,
        tables: &[u16],
    ) -> Result<Self, ControlError> {
        let dirty: HashSet<u16> = tables.iter().copied().collect();
        let dirty = |table: TableId| dirty.contains(&table.as_u16());
        let mut next = self.clone();
        next.generation = pinned.version;

        if dirty(TableId::Users) {
            let users: HashMap<i64, UserRow> = scan_at::<UserRow>(store, pinned, TableId::Users)
                .await?
                .into_iter()
                .map(|u| (u.uid, u))
                .collect();
            next.usernames = Arc::new(usernames(&users));
            next.admin_uid = admin_uid(&users);
            next.users = Arc::new(users);
        }
        if dirty(TableId::AccessKeys) {
            next.keys = Arc::new(
                scan_at::<AccessKeyRow>(store, pinned, TableId::AccessKeys)
                    .await?
                    .into_iter()
                    .map(|k| (k.access_key.clone(), ViewKey::from(k)))
                    .collect(),
            );
        }
        if dirty(TableId::Permissions) {
            let mut permissions: HashMap<i64, HashMap<String, PermissionStorageRow>> =
                HashMap::new();
            for row in scan_at::<PermissionStorageRow>(store, pinned, TableId::Permissions).await? {
                permissions
                    .entry(row.user_id)
                    .or_default()
                    .insert(row.bucket.clone(), row);
            }
            next.permissions = Arc::new(permissions);
        }
        if dirty(TableId::Accounts) {
            next.accounts = Arc::new(
                scan_at::<AccountRow>(store, pinned, TableId::Accounts)
                    .await?
                    .into_iter()
                    .map(|a| (a.id.clone(), a))
                    .collect(),
            );
        }
        if dirty(TableId::AccountMembers) {
            next.members = Arc::new(
                scan_at::<MemberRow>(store, pinned, TableId::AccountMembers)
                    .await?
                    .into_iter()
                    .map(|m| (m.uid, m))
                    .collect(),
            );
        }
        if dirty(TableId::AccountUsageLedger) {
            next.ledger = Arc::new(
                scan_at::<LedgerRow>(store, pinned, TableId::AccountUsageLedger)
                    .await?
                    .into_iter()
                    .map(|l| (l.account_id.clone(), l))
                    .collect(),
            );
        }
        if dirty(TableId::Buckets) {
            next.buckets = Arc::new(
                scan_at::<BucketRow>(store, pinned, TableId::Buckets)
                    .await?
                    .into_iter()
                    .map(|b| (b.name.clone(), b))
                    .collect(),
            );
        }
        if dirty(TableId::BucketPolicies) {
            next.policies = Arc::new(
                scan_at::<BucketPolicyEntry>(store, pinned, TableId::BucketPolicies)
                    .await?
                    .into_iter()
                    .map(|p| (p.bucket.clone(), p))
                    .collect(),
            );
        }
        if dirty(TableId::EventConfigs) {
            next.events = Arc::new(
                scan_at::<EventConfigEntry>(store, pinned, TableId::EventConfigs)
                    .await?
                    .into_iter()
                    .map(|e| (e.bucket.clone(), e))
                    .collect(),
            );
        }
        // AP owner is resolved to a natural key at load time, so a user
        // create/rename/delete must re-resolve the AP map too.
        if dirty(TableId::AccessPoints) || dirty(TableId::Users) {
            let users = &next.users;
            let aps = scan_at::<StoredAp>(store, pinned, TableId::AccessPoints)
                .await?
                .into_iter()
                .map(|r| ap_entry(r, users))
                .map(|e| (e.name.clone(), e))
                .collect();
            next.aps = Arc::new(aps);
        }
        if dirty(TableId::OlapConfigs) {
            next.olap = Arc::new(
                scan_at::<OlapConfigEntry>(store, pinned, TableId::OlapConfigs)
                    .await?
                    .into_iter()
                    .map(|c| (c.access_point.clone(), c))
                    .collect(),
            );
        }
        if dirty(TableId::VpcEndpoints) {
            next.vpcs = Arc::new(
                scan_at::<VpcEndpoint>(store, pinned, TableId::VpcEndpoints)
                    .await?
                    .into_iter()
                    .map(|v| (v.vpce_id.clone(), v))
                    .collect(),
            );
        }
        if dirty(TableId::SiteConfig) {
            next.site_config = scan_at::<StoredSiteConfig>(store, pinned, TableId::SiteConfig)
                .await?
                .into_iter()
                .next();
        }
        if dirty(TableId::Sites) {
            next.sites = Arc::new(
                scan_at::<SitePeerRow>(store, pinned, TableId::Sites)
                    .await?
                    .into_iter()
                    .map(|s| (s.peer_id.clone(), s))
                    .collect(),
            );
        }
        if dirty(TableId::Users) || dirty(TableId::AccountMembers) || dirty(TableId::Buckets) {
            next.bucket_counts = Arc::new(bucket_counts(&next.buckets, &next.users, &next.members));
        }
        Ok(next)
    }
}

/// Tables a commit changed, as reported by [`CommitStamp::dirty_tables`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDelta {
    pub generation: u64,
    pub tables: Vec<u16>,
    /// Rebuild every domain at the pinned generation.
    pub full: bool,
}

impl CacheDelta {
    /// A no-op commit yields an empty, non-full delta (publish is a no-op).
    pub fn from_stamp(stamp: &CommitStamp) -> Self {
        Self {
            generation: stamp.generation,
            tables: if stamp.committed {
                stamp.dirty_tables.clone()
            } else {
                Vec::new()
            },
            full: false,
        }
    }

    pub fn full(generation: u64) -> Self {
        Self {
            generation,
            tables: Vec::new(),
            full: true,
        }
    }
}

/// Publishes committed generations to request snapshots.
pub struct ViewCache {
    store: Arc<ObjectControlStore>,
    current: RwLock<Arc<HotView>>,
    /// One publisher at a time: a full rebuild and a merge must never interleave.
    publish_lock: tokio::sync::Mutex<()>,
    max_bytes: usize,
}

impl ViewCache {
    pub async fn load(store: Arc<ObjectControlStore>) -> Result<Arc<Self>, ControlError> {
        Self::load_with_cap(store, HotView::DEFAULT_MAX_BYTES).await
    }

    pub async fn load_with_cap(
        store: Arc<ObjectControlStore>,
        max_bytes: usize,
    ) -> Result<Arc<Self>, ControlError> {
        let view = HotView::load_with_cap(&store, max_bytes).await?;
        Ok(Arc::new(Self {
            store,
            current: RwLock::new(view),
            publish_lock: tokio::sync::Mutex::new(()),
            max_bytes,
        }))
    }

    pub fn capture(&self) -> RequestView {
        let view = self
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        RequestView::new(view)
    }

    /// Merge the committed tables into a new generation. A gap (a commit whose
    /// publish was missed) rebuilds everything, so a stale map is never tagged
    /// with a newer generation.
    pub async fn publish(&self, delta: CacheDelta) -> Result<(), ControlError> {
        if !delta.full && delta.tables.is_empty() && delta.generation == 0 {
            return Ok(());
        }
        let _publishing = self.publish_lock.lock().await;
        let current = self
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if !delta.full && delta.generation <= current.generation {
            if delta.tables.is_empty() {
                return Ok(());
            }
            // A data commit older than the published generation: a concurrent
            // metadata-only publisher may have skipped its tables, so rebuild
            // from the current root instead of trusting the delta.
            let next = HotView::load_with_cap(&self.store, self.max_bytes).await?;
            return self.store_view(next);
        }
        if !delta.full && delta.tables.is_empty() {
            // Metadata-only commits (applied id, membership, markers) advance the
            // root without touching a table. Adopt the generation when it is the
            // immediate successor so the next data commit still takes the merge
            // path; a real gap falls through to the full rebuild.
            if current.generation.checked_add(1) == Some(delta.generation) {
                let mut next = (*current).clone();
                next.generation = delta.generation;
                let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
                if next.generation > guard.generation {
                    *guard = Arc::new(next);
                }
                return Ok(());
            }
        }
        let pinned = self.store.pin().await.map_err(backend)?;
        let next = if !delta.full
            && pinned.version == delta.generation
            && current.generation.checked_add(1) == Some(delta.generation)
        {
            Arc::new(
                current
                    .rebuild_domains(&self.store, &pinned, &delta.tables)
                    .await?,
            )
        } else {
            HotView::load_with_cap(&self.store, self.max_bytes).await?
        };
        self.store_view(next)
    }

    fn store_view(&self, next: Arc<HotView>) -> Result<(), ControlError> {
        let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
        if next.generation > guard.generation {
            *guard = next;
        }
        Ok(())
    }

    /// Snapshot install / bulk replace: rebuild every map from the current root.
    pub async fn rebuild(&self) -> Result<(), ControlError> {
        let next = HotView::load_with_cap(&self.store, self.max_bytes).await?;
        let mut guard = self.current.write().unwrap_or_else(|e| e.into_inner());
        if next.generation >= guard.generation {
            *guard = next;
        }
        Ok(())
    }
}
