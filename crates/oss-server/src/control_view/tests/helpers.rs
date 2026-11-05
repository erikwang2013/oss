//! Shared fixtures: an empty object control store plus typed row builders.

use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::store::{Store, StoreConfig};

use crate::control_store_obj::{ObjectControlTxn, RootIo, WriteOrigin};
use crate::local_store::LocalStore;

use super::super::*;

pub(super) struct Fx {
    pub(super) _dir: TempDir,
    pub(super) control: Arc<ObjectControlStore>,
}

pub(super) async fn fixture() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let control =
        Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(store))).with_local_store(local));
    control.bootstrap(Mode::Standalone, 100).await.unwrap();
    Fx { _dir: dir, control }
}

pub(super) async fn txn(fx: &Fx) -> ObjectControlTxn {
    fx.control
        .begin(WriteOrigin::local("view-test"))
        .await
        .unwrap()
}

pub(super) fn uid_key(uid: i64) -> [u8; 8] {
    (uid as u64).to_be_bytes()
}

pub(super) async fn put<T: serde::Serialize>(
    txn: &mut ObjectControlTxn,
    table: TableId,
    key: &[u8],
    row: &T,
) {
    txn.put(table, key, &rmp_serde::to_vec_named(row).unwrap())
        .await
        .unwrap();
}

pub(super) fn user(uid: i64, name: &str, admin: bool, status: &str) -> UserRow {
    UserRow {
        uid,
        username: name.into(),
        password_hash: "h".into(),
        is_admin: admin,
        status: status.into(),
        created_at: 1,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn key(
    ak: &str,
    sk: &str,
    uid: i64,
    kind: &str,
    status: &str,
    expires_at: Option<i64>,
    parent_ak: Option<&str>,
    session_policy: Option<&str>,
) -> AccessKeyRow {
    AccessKeyRow {
        access_key: ak.into(),
        secret_key: sk.into(),
        user_id: uid,
        status: status.into(),
        expires_at,
        created_at: 1,
        parent_ak: parent_ak.map(Into::into),
        session_policy: session_policy.map(Into::into),
        kind: kind.into(),
    }
}

pub(super) fn account(
    id: &str,
    status: &str,
    bucket_quota: Option<i64>,
    hard: Option<i64>,
    objects: Option<i64>,
) -> AccountRow {
    AccountRow {
        id: id.into(),
        name: id.into(),
        status: status.into(),
        admin_uid: None,
        bucket_quota,
        soft_byte_quota: None,
        hard_byte_quota: hard,
        object_quota: objects,
        created_at: 1,
        updated_at: 1,
    }
}

pub(super) fn member(account_id: &str, uid: i64) -> MemberRow {
    MemberRow {
        account_id: account_id.into(),
        uid,
        role: "member".into(),
        created_at: 1,
    }
}

pub(super) fn ledger(account_id: &str, used_bytes: i64, object_count: i64) -> LedgerRow {
    LedgerRow {
        account_id: account_id.into(),
        used_bytes,
        object_count,
    }
}

pub(super) fn bucket(name: &str, owner: Option<i64>) -> BucketRow {
    BucketRow {
        name: name.into(),
        owner_uid: owner,
        set_id: 0,
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

pub(super) fn perm(
    uid: i64,
    bucket: &str,
    can_read: bool,
    can_write: bool,
    can_delete: bool,
) -> PermissionStorageRow {
    PermissionStorageRow {
        user_id: uid,
        bucket: bucket.into(),
        can_read,
        can_write,
        can_delete,
    }
}

pub(super) fn policy(bucket: &str, json: &str) -> BucketPolicyEntry {
    BucketPolicyEntry {
        bucket: bucket.into(),
        policy_json: json.into(),
        updated_at: 1,
    }
}

pub(super) fn event(bucket: &str, json: &str) -> EventConfigEntry {
    EventConfigEntry {
        bucket: bucket.into(),
        config_json: json.into(),
        updated_at: 1,
    }
}

pub(super) fn site(peer: &str) -> SitePeerRow {
    SitePeerRow {
        peer_id: peer.into(),
        remote_site_id: format!("r-{peer}"),
        name: peer.into(),
        endpoint: "http://peer".into(),
        region: "us-east-1".into(),
        priority: 1,
        s3_access_key: "pk".into(),
        s3_secret_key: "enc:v1:psk".into(),
        peer_site_token: "tok".into(),
        created_at: 1,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

/// Deterministic 64-bit LCG (no new dependency).
pub(super) fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}
