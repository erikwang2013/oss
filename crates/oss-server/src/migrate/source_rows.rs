//! Raw control-table rows read from SQLite. `data::read_tx` exports the
//! replicated natural-key subset; these rows keep the node-local attributes
//! (uids, `is_admin`, bucket `set_id`, account admin uid) that snapshot
//! installers would otherwise rewrite.

use sqlx::Row;
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use xl_store::control_store::sites::SitePeerRow;

use crate::control_store_obj::migrate_support::{
    SiteChangelogRow, SiteConfigRow, SiteEntityClockRow,
};
use crate::control_store_obj::{
    AccessKeyRow, AccountRow, BucketRow, LedgerRow, MemberRow, PermissionStorageRow, UserRow,
};

pub(crate) struct RawRows {
    pub users: Vec<UserRow>,
    pub access_keys: Vec<AccessKeyRow>,
    pub permissions: Vec<PermissionStorageRow>,
    pub buckets: Vec<BucketRow>,
    pub accounts: Vec<AccountRow>,
    pub members: Vec<MemberRow>,
    pub ledger: Vec<LedgerRow>,
    pub sites: Vec<SitePeerRow>,
    pub site_config: Option<SiteConfigRow>,
    pub changelog: Vec<SiteChangelogRow>,
    pub clocks: Vec<SiteEntityClockRow>,
}

fn get<'r, T>(row: &'r SqliteRow, column: &str, default: T) -> T
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(column).unwrap_or(default)
}

pub(crate) async fn read_raw(
    conn: &mut SqliteConnection,
    canonical: bool,
) -> Result<RawRows, super::source::BoxError> {
    let mut raw = RawRows {
        users: Vec::new(),
        access_keys: Vec::new(),
        permissions: Vec::new(),
        buckets: Vec::new(),
        accounts: Vec::new(),
        members: Vec::new(),
        ledger: Vec::new(),
        sites: Vec::new(),
        site_config: None,
        changelog: Vec::new(),
        clocks: Vec::new(),
    };
    for r in sqlx::query("SELECT * FROM users ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.users.push(UserRow {
            uid: r.get("id"),
            username: r.get("username"),
            password_hash: r.get("password_hash"),
            is_admin: get::<i64>(&r, "is_admin", 0) != 0,
            status: get(&r, "status", "active".to_string()),
            created_at: r.get("created_at"),
        });
    }
    for r in sqlx::query("SELECT * FROM access_keys ORDER BY access_key")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.access_keys.push(AccessKeyRow {
            access_key: r.get("access_key"),
            secret_key: r.get("secret_key"),
            user_id: r.get("user_id"),
            status: r.get("status"),
            expires_at: r.get("expires_at"),
            created_at: r.get("created_at"),
            parent_ak: get(&r, "parent_ak", None),
            session_policy: get(&r, "session_policy", None),
            kind: get(&r, "kind", "long-term".to_string()),
        });
    }
    for r in sqlx::query("SELECT * FROM permissions ORDER BY user_id, bucket")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.permissions.push(PermissionStorageRow {
            user_id: r.get("user_id"),
            bucket: r.get("bucket"),
            can_read: get::<i64>(&r, "can_read", 0) != 0,
            can_write: get::<i64>(&r, "can_write", 0) != 0,
            can_delete: get::<i64>(&r, "can_delete", 0) != 0,
        });
    }
    for r in sqlx::query("SELECT * FROM buckets ORDER BY name")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.buckets.push(BucketRow {
            name: r.get("name"),
            owner_uid: r.get("owner_id"),
            set_id: get(&r, "set_id", 0),
            creation_date: r.get("creation_date"),
            versioning: get(&r, "versioning", "Off".to_string()),
            object_lock: get::<i64>(&r, "object_lock", 0) != 0,
            default_encryption: get(&r, "default_encryption", None),
            object_lock_default_mode: get(&r, "object_lock_default_mode", None),
            object_lock_default_days: get(&r, "object_lock_default_days", None),
            replication_mode: get(&r, "replication_mode", "async".to_string()),
            deleting: get::<i64>(&r, "deleting", 0) != 0,
        });
    }
    if canonical {
        for r in sqlx::query("SELECT * FROM accounts ORDER BY id")
            .fetch_all(&mut *conn)
            .await?
        {
            raw.accounts.push(AccountRow {
                id: r.get("id"),
                name: r.get("name"),
                status: get(&r, "status", "active".to_string()),
                admin_uid: r.get("admin_uid"),
                bucket_quota: r.get("bucket_quota"),
                soft_byte_quota: get(&r, "soft_byte_quota", None),
                hard_byte_quota: get(&r, "hard_byte_quota", None),
                object_quota: get(&r, "object_quota", None),
                created_at: r.get("created_at"),
                updated_at: r.get("updated_at"),
            });
        }
        for r in sqlx::query("SELECT * FROM account_members ORDER BY account_id, uid")
            .fetch_all(&mut *conn)
            .await?
        {
            raw.members.push(MemberRow {
                account_id: r.get("account_id"),
                uid: r.get("uid"),
                role: r.get("role"),
                created_at: r.get("created_at"),
            });
        }
        for r in sqlx::query("SELECT * FROM account_usage_ledger ORDER BY account_id")
            .fetch_all(&mut *conn)
            .await?
        {
            raw.ledger.push(LedgerRow {
                account_id: r.get("account_id"),
                used_bytes: r.get("used_bytes"),
                object_count: r.get("object_count"),
            });
        }
    }
    for r in sqlx::query("SELECT * FROM site_config ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        if raw.site_config.is_none() {
            raw.site_config = Some(SiteConfigRow {
                id: r.get("id"),
                name: r.get("name"),
                region: r.get("region"),
                priority: get(&r, "priority", 0),
                site_token: r.get("site_token"),
                created_at: get(&r, "created_at", 0),
            });
        }
    }
    for r in sqlx::query("SELECT * FROM sites ORDER BY peer_id")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.sites.push(SitePeerRow {
            peer_id: r.get("peer_id"),
            remote_site_id: get(&r, "remote_site_id", String::new()),
            name: r.get("name"),
            endpoint: r.get("endpoint"),
            region: get(&r, "region", "us-east-1".to_string()),
            priority: get(&r, "priority", 0),
            s3_access_key: r.get("s3_access_key"),
            s3_secret_key: r.get("s3_secret_key"),
            peer_site_token: r.get("peer_site_token"),
            created_at: get(&r, "created_at", 0),
            last_seen: r.get("last_seen"),
            last_error: r.get("last_error"),
            raft_node_id: get(&r, "raft_node_id", None),
            reconcile_required: get::<i64>(&r, "reconcile_required", 0) != 0,
        });
    }
    for r in sqlx::query("SELECT * FROM site_changelog ORDER BY seq")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.changelog.push(SiteChangelogRow {
            seq: r.get("seq"),
            origin_site_id: r.get("origin_site_id"),
            entity: r.get("entity"),
            entity_id: r.get("entity_id"),
            op: r.get("op"),
            payload: r.get("payload"),
            priority: get(&r, "priority", 0),
            ts: get(&r, "ts", 0),
            created_at: get(&r, "created_at", 0),
        });
    }
    for r in sqlx::query("SELECT * FROM site_entity_clock ORDER BY entity, entity_id")
        .fetch_all(&mut *conn)
        .await?
    {
        raw.clocks.push(SiteEntityClockRow {
            entity: r.get("entity"),
            entity_id: r.get("entity_id"),
            ts: r.get("ts"),
            priority: get(&r, "priority", 0),
            origin_site_id: r.get("origin_site_id"),
            seq: r.get("seq"),
        });
    }
    Ok(raw)
}
