//! F3: object snapshot export/install parity with the SQLite state machine.
//!
//! Byte equality against the frozen A8 goldens, the SQLite install predicates
//! (admin/set_id preserved, non-admin state cleared), the root-CAS crash matrix
//! and the large OLAP module path. The feature-level gate matrix lives in
//! `snapshot_gate_tests`.

use std::io::Cursor;
use std::sync::Arc;

use base64::Engine as _;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, CommittedLeaderId, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::control_store::bucket::BucketTxn;
use xl_store::control_store::iam::{AccessKeyEntry, IamStore, IamTxn};
use xl_store::control_store::{AccessPointEntry, BucketEntry, OwnerRef};
use xl_store::store::{Store, StoreConfig};

use super::super::TypeConfig;
use super::super::data::{self, ControlData};
use super::ObjectStateMachine;
use crate::control_store_obj::{
    BucketRow as StoredBucketRow, ObjectControlStore, RootIo, TableId, WriteOrigin,
};
use crate::local_store::{LocalStore, UploadRecord};

#[path = "../../../tests/support/golden.rs"]
pub(super) mod golden;

pub(super) struct ObjectMachine {
    pub(super) _dir: TempDir,
    pub(super) sm: ObjectStateMachine,
}

pub(super) async fn object_machine(feature_level: u8, max_bytes: u64) -> ObjectMachine {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let control = ObjectControlStore::new(Arc::new(RootIo::new(store))).with_local_store(local);
    let control = Arc::new(control);
    control.bootstrap(Mode::Raft, 100).await.unwrap();
    let snapshots = Arc::new(
        crate::raft::file_snapshot::FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap(),
    );
    let sm = ObjectStateMachine::open(control, snapshots, max_bytes, None, feature_level)
        .await
        .unwrap();
    ObjectMachine { _dir: dir, sm }
}

pub(super) fn meta(
    last_log_id: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
) -> SnapshotMeta<u64, BasicNode> {
    SnapshotMeta {
        last_log_id,
        last_membership,
        snapshot_id: "f3-test".into(),
    }
}

pub(super) async fn install(
    sm: &mut ObjectStateMachine,
    meta: &SnapshotMeta<u64, BasicNode>,
    bytes: &[u8],
) -> Result<(), StorageError<u64>> {
    RaftStateMachine::<TypeConfig>::install_snapshot(
        sm,
        meta,
        Box::new(Cursor::new(bytes.to_vec())),
    )
    .await
}

pub(super) async fn build(sm: &mut ObjectStateMachine) -> Snapshot<TypeConfig> {
    RaftSnapshotBuilder::<TypeConfig>::build_snapshot(sm)
        .await
        .unwrap()
}

pub(super) async fn seed_admin(sm: &ObjectStateMachine) -> i64 {
    let mut txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    let uid = txn
        .user_create_default_status("golden-admin", "admin-hash", true, 1)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    uid
}

fn bucket_entry(name: &str) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner: OwnerRef::Admin,
        creation_date: 0,
        versioning: "Enabled".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

/// Install every frozen golden, then export: bytes must match the SQLite
/// install/export roundtrip and the in-memory snapshot slot.
#[tokio::test]
async fn goldens_install_and_export_byte_identically() {
    for label in ["ossctl1", "ossctl2", "ossctl3", "ossctl3_runstate"] {
        let bytes = golden::load(&format!("snapshots/install_{label}.bin"));
        // Production accepts a level-3 snapshot carrying runstate rows only while
        // the configured level is still below 4 (matching the capture harness).
        let level = data::decode_snapshot(&bytes).unwrap().feature_level;
        let mut om = object_machine(level, 4 << 20).await;
        seed_admin(&om.sm).await;
        let log_id = LogId::new(CommittedLeaderId::new(7, 1), 42);
        let snap_meta = meta(Some(log_id), StoredMembership::default());
        install(&mut om.sm, &snap_meta, &bytes)
            .await
            .unwrap_or_else(|e| panic!("install {label}: {e}"));

        let mut sm = om.sm;
        let snap = build(&mut sm).await;
        golden::assert_bytes(
            &format!("snapshots/export_{label}.bin"),
            snap.snapshot.get_ref(),
        );
        assert_eq!(snap.meta.last_log_id, Some(log_id), "{label}: applied id");
        assert_eq!(
            snap.meta.last_membership,
            StoredMembership::default(),
            "{label}: membership"
        );

        let current = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .expect("installed snapshot remembered");
        assert_eq!(current.meta.last_log_id, Some(log_id));
        assert_eq!(current.snapshot.get_ref(), &bytes, "{label}: snapshot slot");
    }
}

/// SQLite install predicates: local admin user/key and bucket `set_id` survive,
/// non-admin keys/permissions and non-admin upload owners are cleared.
#[tokio::test]
async fn install_preserves_admin_and_set_id_and_clears_non_admin_state() {
    let mut om = object_machine(3, 4 << 20).await;
    let admin = seed_admin(&om.sm).await;
    let bytes = golden::load("snapshots/export_ossctl1.bin");
    let wanted = data::decode_snapshot(&bytes).unwrap().data;
    let snap_user = wanted.users[0].username.clone();
    let snap_bucket = wanted.buckets[0].name.clone();

    let mut txn = om
        .sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    let bob = txn
        .user_create_default_status("bob", "h", false, 10)
        .await
        .unwrap();
    txn.access_key_create_legacy("BOB-LONG", "s", bob, 10)
        .await
        .unwrap();
    txn.access_key_create(&AccessKeyEntry {
        access_key: "BOB-SESSION".into(),
        secret_key: "s".into(),
        username: "bob".into(),
        status: "active".into(),
        expires_at: None,
        created_at: 10,
        parent_ak: Some("BOB-LONG".into()),
        session_policy: None,
        kind: "session".into(),
    })
    .await
    .unwrap();
    txn.access_key_create_legacy("ADMIN-AK", "s", admin, 10)
        .await
        .unwrap();
    txn.permission_upsert(bob, "local-bucket", true, true, true)
        .await
        .unwrap();
    txn.permission_upsert(admin, "admin-target", true, false, false)
        .await
        .unwrap();
    let mut bucket = bucket_entry(&snap_bucket);
    bucket.owner = OwnerRef::User("bob".into());
    txn.bucket_create(&bucket).await.unwrap();
    txn.commit().await.unwrap();

    // Give the surviving bucket a non-default set_id (SQLite install keeps it).
    let mut txn = om
        .sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    let start = Vec::new();
    let end = vec![0xffu8];
    let rows = txn
        .scan(TableId::Buckets, start.as_slice()..end.as_slice())
        .await
        .unwrap();
    let (key, value) = rows.first().expect("seeded bucket");
    let mut row: StoredBucketRow = rmp_serde::from_slice(value).unwrap();
    assert_eq!(row.name, snap_bucket);
    row.set_id = 7;
    txn.put(
        TableId::Buckets,
        key,
        &rmp_serde::to_vec_named(&row).unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let uploads = om.sm.store.local_store().unwrap().uploads().unwrap();
    for (path, owner) in [("admin-upload", Some(admin)), ("bob-upload", Some(bob))] {
        uploads
            .put(&UploadRecord {
                saved_path: path.into(),
                bucket: "b".into(),
                key: "k".into(),
                group_name: "g".into(),
                size: 1,
                etag: "e".into(),
                original_name: None,
                owner_uid: owner,
                created_at: 1,
                expires_at: None,
            })
            .unwrap();
    }

    install(&mut om.sm, &meta(None, StoredMembership::default()), &bytes)
        .await
        .unwrap();
    let sm = om.sm;

    assert!(
        sm.store
            .user("golden-admin")
            .await
            .unwrap()
            .unwrap()
            .is_admin
    );
    assert!(sm.store.user("bob").await.unwrap().is_none());
    assert!(
        sm.store
            .user(&snap_user)
            .await
            .unwrap()
            .is_some_and(|u| !u.is_admin)
    );
    let snap_uid = sm.store.user(&snap_user).await.unwrap().unwrap().uid;

    {
        let mut txn = sm
            .store
            .begin(WriteOrigin::snapshot_install())
            .await
            .unwrap();
        assert_eq!(
            txn.access_key_user_and_kind("ADMIN-AK").await.unwrap(),
            Some((admin, "long-term".into()))
        );
        assert_eq!(
            txn.access_key_user_and_kind("BOB-LONG").await.unwrap(),
            None
        );
        assert_eq!(
            txn.access_key_user_and_kind("BOB-SESSION").await.unwrap(),
            None
        );
    }

    assert!(sm.store.permissions_for_user(bob).await.unwrap().is_empty());
    assert!(
        sm.store
            .permissions_for_user(admin)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sm.store.permissions_for_user(snap_uid).await.unwrap().len(),
        1
    );

    let txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    let start = Vec::new();
    let end = vec![0xffu8];
    let rows = txn
        .scan(TableId::Buckets, start.as_slice()..end.as_slice())
        .await
        .unwrap();
    let (_, value) = rows.first().expect("installed bucket");
    let row: StoredBucketRow = rmp_serde::from_slice(value).unwrap();
    assert_eq!(row.name, snap_bucket);
    assert_eq!(row.set_id, 7, "install preserves the target bucket set_id");
    assert_eq!(row.owner_uid, Some(snap_uid));

    let owner_of = |path: &str| {
        uploads
            .all()
            .unwrap()
            .into_iter()
            .find(|r| r.saved_path == path)
            .unwrap()
            .owner_uid
    };
    assert_eq!(owner_of("admin-upload"), Some(admin));
    assert_eq!(owner_of("bob-upload"), None);
}

/// A fresh node seeds its env admin into the local root before Raft spawn; a
/// snapshot carries no admin rows, so install must leave that local admin (and
/// its password hash) login-capable. This is the node-joins-by-snapshot path.
#[tokio::test]
async fn admin_seeded_before_install_stays_login_capable() {
    let mut om = object_machine(3, 4 << 20).await;
    let hash = crate::db::hash_password("env-admin-pw");
    let mut txn = om
        .sm
        .store
        .begin(WriteOrigin::local("admin-bootstrap"))
        .await
        .unwrap();
    let uid = txn
        .user_create_default_status("env-admin", &hash, true, 1)
        .await
        .unwrap();
    // Written by the startup bootstrap; gates the one-shot seed on this node.
    txn.set_marker("admin_username", "env-admin");
    txn.commit().await.unwrap();

    let bytes = golden::load("snapshots/export_ossctl1.bin");
    install(&mut om.sm, &meta(None, StoredMembership::default()), &bytes)
        .await
        .unwrap();
    let sm = om.sm;
    let login = sm
        .store
        .login_user("env-admin")
        .await
        .unwrap()
        .expect("本地管理员必须在快照安装后仍在册");
    assert_eq!(login.uid, uid);
    assert!(
        crate::db::verify_password("env-admin-pw", &login.password_hash),
        "管理员口令哈希必须在快照安装后仍可校验"
    );
    let txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    assert_eq!(
        txn.meta().markers.get("admin_username").map(String::as_str),
        Some("env-admin"),
        "一次性引导标记必须随快照安装保留，否则已删管理员会在重启后复活"
    );
}

/// An OLAP module above the 128 KiB blob threshold must round-trip through the
/// chunked content-addressed object path byte-identically.
#[tokio::test]
async fn olap_module_over_128k_round_trips() {
    let module: Vec<u8> = (0..160 * 1024).map(|i| (i % 251) as u8).collect();
    let module_b64 = base64::engine::general_purpose::STANDARD.encode(&module);
    let data = ControlData {
        users: vec![data::UserRow {
            username: "alice".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        }],
        buckets: vec![BucketEntry {
            name: "olap-bucket".into(),
            owner: OwnerRef::User("alice".into()),
            creation_date: 1,
            versioning: "Enabled".into(),
            object_lock: false,
            default_encryption: None,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: "async".into(),
            deleting: false,
        }],
        access_points: vec![AccessPointEntry {
            name: "olap-ap".into(),
            bucket: "olap-bucket".into(),
            owner: OwnerRef::User("alice".into()),
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        }],
        olap_configs: vec![data::OlapConfigRow {
            access_point: "olap-ap".into(),
            kind: "http".into(),
            endpoint: None,
            module_b64: Some(module_b64.clone()),
            module_sha256: None,
            timeout_ms: 1,
            max_response_bytes: 1,
            updated_at: 1,
        }],
        ..ControlData::default()
    };
    let bytes = data::encode_snapshot(&data).unwrap();
    assert!(bytes.len() > 128 * 1024, "module must exercise blob chunks");

    let mut om = object_machine(1, 8 << 20).await;
    install(&mut om.sm, &meta(None, StoredMembership::default()), &bytes)
        .await
        .unwrap();
    let mut sm = om.sm;
    let snap = build(&mut sm).await;
    assert_eq!(snap.snapshot.get_ref(), &bytes);

    let decoded = data::decode_snapshot(snap.snapshot.get_ref()).unwrap();
    let stored = decoded.data.olap_configs[0].module_b64.as_ref().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(stored)
            .unwrap(),
        module
    );
}
