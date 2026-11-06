//! Full-schema SQLite fixture: control DB (all replicated + local tables) and a
//! SQLite Raft DB (log, vote, committed, snapshot) to migrate from.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::entry::{Entry, EntryPayload};
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, CommittedLeaderId, LogId, SnapshotMeta, StoredMembership, Vote};
use xl_store::control_store::account::{AccountEntry, AccountMemberEntry, AccountUsageLedgerEntry};
use xl_store::control_store::{
    AccessKeyEntry, BucketEntry, BucketPolicyEntry, EventConfigEntry, InventoryConfigEntry,
    LifecycleRuleEntry, OwnerRef, PermissionEntry, ReplicationRuleEntry, VpcEndpoint,
};
use xl_store::store::{Store, StoreConfig};

use super::super::source::{self, Source};
use crate::control_store_obj::{ObjectControlStore, RootIo};
use crate::raft::TypeConfig;
use crate::raft::data::{self, ControlData, InstallCtx};
use crate::raft::runstate::{
    self, InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RunState, ScheduleLeaseRow, TierRestoreRow,
};
use crate::raft::store::SqliteLogStore;

pub(super) struct Fixture {
    pub(super) _dir: tempfile::TempDir,
    pub control: PathBuf,
    pub raft: PathBuf,
    pub state: PathBuf,
}

pub(super) fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(2, 1), index)
}

fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    }
}

fn control_data() -> ControlData {
    ControlData {
        users: vec![
            data::UserRow {
                username: "alice".into(),
                password_hash: "h".into(),
                status: "active".into(),
                created_at: 10,
            },
            data::UserRow {
                username: "bob".into(),
                password_hash: "h".into(),
                status: "disabled".into(),
                created_at: 11,
            },
        ],
        access_keys: vec![AccessKeyEntry {
            access_key: "AKALICE".into(),
            secret_key: "secret".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: Some(99),
            created_at: 12,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        }],
        permissions: vec![PermissionEntry {
            username: "alice".into(),
            bucket: "b1".into(),
            can_read: true,
            can_write: true,
            can_delete: false,
        }],
        buckets: vec![
            BucketEntry {
                name: "b1".into(),
                owner: OwnerRef::User("alice".into()),
                creation_date: 13,
                versioning: "Enabled".into(),
                object_lock: true,
                default_encryption: Some("AES256".into()),
                object_lock_default_mode: Some("GOVERNANCE".into()),
                object_lock_default_days: Some(7),
                replication_mode: "sync".into(),
                deleting: false,
            },
            BucketEntry {
                name: "b2".into(),
                owner: OwnerRef::Admin,
                creation_date: 14,
                versioning: "Off".into(),
                object_lock: false,
                default_encryption: None,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                replication_mode: "async".into(),
                deleting: false,
            },
        ],
        bucket_policies: vec![BucketPolicyEntry {
            bucket: "b1".into(),
            policy_json: "{}".into(),
            updated_at: 15,
        }],
        lifecycle_rules: vec![LifecycleRuleEntry {
            bucket: "b1".into(),
            prefix: "logs/".into(),
            rule_id: Some("r1".into()),
            expire_days: 30,
            expiration_days: Some(30),
            transition_days: Some(7),
            transition_class: Some("GLACIER".into()),
            transition_tier: Some("cold".into()),
            enabled: true,
            updated_at: 16,
            config_json: None,
        }],
        event_configs: vec![EventConfigEntry {
            bucket: "b1".into(),
            config_json: "{\"q\":[]}".into(),
            updated_at: 17,
        }],
        replication_rules: vec![ReplicationRuleEntry {
            bucket: "b1".into(),
            rules_json: "[]".into(),
            updated_at: 18,
        }],
        access_points: vec![data::AccessPointRow {
            name: "ap1".into(),
            bucket: "b1".into(),
            owner: OwnerRef::User("alice".into()),
            policy_json: Some("{}".into()),
            created_at: 19,
            updated_at: 20,
        }],
        olap_configs: vec![data::OlapConfigRow {
            access_point: "ap1".into(),
            kind: "http".into(),
            endpoint: Some("https://example.com/t".into()),
            module_b64: Some("aGVsbG8=".into()),
            module_sha256: Some("abc".into()),
            timeout_ms: 5000,
            max_response_bytes: 1024,
            updated_at: 21,
        }],
        inventory_configs: vec![InventoryConfigEntry {
            bucket: "b1".into(),
            id: "inv1".into(),
            owner: OwnerRef::Admin,
            config_json: "{}".into(),
            updated_at: 22,
        }],
        accounts: vec![AccountEntry {
            id: "000000000007".into(),
            name: "acct".into(),
            status: "active".into(),
            admin: Some("bob".into()),
            bucket_quota: Some(2),
            soft_byte_quota: Some(1024),
            created_at: 30,
            updated_at: 31,
        }],
        account_members: vec![AccountMemberEntry {
            account_id: "000000000007".into(),
            username: "alice".into(),
            role: "owner".into(),
            created_at: 30,
        }],
        vpc_endpoints: vec![VpcEndpoint {
            vpce_id: "vpce-1".into(),
            vpc_id: "vpc-1".into(),
            account_id: "000000000007".into(),
            state: "active".into(),
            created_at: 31,
            updated_at: 32,
        }],
        account_usage_ledger: vec![AccountUsageLedgerEntry {
            account_id: "000000000007".into(),
            used_bytes: 10,
            object_count: 2,
        }],
    }
}

fn runstate() -> RunState {
    RunState {
        jobs: vec![JobRow {
            id: 1,
            kind: "lifecycle".into(),
            bucket: "b1".into(),
            prefix: "logs/".into(),
            state: "done".into(),
            created_at: 40,
            updated_at: 41,
            report_bucket: Some("b1".into()),
            ..JobRow::default()
        }],
        job_tasks: vec![JobTaskRow {
            job_id: 1,
            task_no: 1,
            action: "delete".into(),
            payload_json: "{}".into(),
            state: "done".into(),
            ..JobTaskRow::default()
        }],
        inventory_runs: vec![InventoryRunRow {
            id: 1,
            bucket: "b1".into(),
            config_id: "inv1".into(),
            run_key: "run-1".into(),
            started_at: 42,
            state: "done".into(),
            job_id: Some(1),
            created_at: 42,
            ..InventoryRunRow::default()
        }],
        inventory_cursors: vec![InventoryCursorRow {
            bucket: "b1".into(),
            config_id: "inv1".into(),
            cursor: "c1".into(),
            updated_at: 43,
            ..InventoryCursorRow::default()
        }],
        inventory_incremental: vec![InventoryIncrementalRow {
            bucket: "b1".into(),
            config_id: "inv1".into(),
            enabled: true,
            updated_at: 43,
        }],
        tier_restores: vec![TierRestoreRow {
            bucket: "b1".into(),
            key: "logs/a".into(),
            version_id: "v1".into(),
            expires_at: 50,
            job_id: Some(1),
            state: "done".into(),
            created_at: 44,
            ..TierRestoreRow::default()
        }],
        lifecycle_cursors: vec![LifecycleCursorRow {
            bucket: "b1".into(),
            rule_id: "r1".into(),
            stage: "expire".into(),
            cursor: "x".into(),
            updated_at: 45,
            ..LifecycleCursorRow::default()
        }],
        schedule_lease: Some(ScheduleLeaseRow {
            term: 2,
            node: 1,
            expires_at: 60,
            acquired_at: 55,
        }),
        next_job_id: 2,
        next_run_id: 2,
    }
}

pub(super) async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let control = dir.path().join("oss.db");
    let raft = dir.path().join("raft.db");
    let state = dir.path().join("state");
    let control_url = format!("sqlite://{}?mode=rwc", control.display());
    let db = crate::db::Db::connect_raft(&control_url).await.unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    for ctx in ["raft_apply_ctx", "apply_ctx"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {ctx} SET active = 1 WHERE id = 1"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    let admin: i64 = sqlx::query_scalar(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES ('admin', 'h', 1, 'active', 1) RETURNING id",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let data = control_data();
    let ctx = InstallCtx {
        admin_id: Some(admin),
        witness: false,
        canonical: true,
    };
    data::install_tx(&mut tx, &data, &ctx).await.unwrap();
    runstate::install_runstate_tx(&mut tx, &runstate())
        .await
        .unwrap();
    // `connect_raft` already seeds exactly one identity row; make it deterministic.
    sqlx::query(
        "UPDATE site_config SET id = 'site-a', name = 'A', region = 'us-east-1', \
         priority = 0, site_token = 'site-token-a', created_at = 69",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, s3_access_key, \
         s3_secret_key, peer_site_token, created_at) \
         VALUES ('peer-b', 'site-b', 'B', 'http://b:9000', 'ak', 'sk', 'tok', 70)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO site_changelog (seq, origin_site_id, entity, entity_id, op, payload, \
         priority, ts, created_at) VALUES (7, 'site-b', 'bucket', 'b1', 'upsert', '{}', 0, 71, 71)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO site_entity_clock (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('bucket', 'b1', 71, 0, 'site-b', 7)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO site_cursors (peer_id, last_seq, updated_at) VALUES ('peer-b', 7, 72)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO site_apply_dead (peer_id, seq, entity, entity_id, error, payload, created_at) \
         VALUES ('peer-b', 6, 'bucket', 'b9', 'boom', '{\"x\":1}', 73)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let alice: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, original_name, \
         owner_id, created_at) VALUES ('g/k', 'b1', 'k', 'g', 5, 'e', 'n', ?, 80)",
    )
    .bind(alice)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision, detail, \
         principal_account, target_account) \
         VALUES (81, ?, 'GetObject', 'b1', 'k', 'allow', 'd', 'a1', 'a2')",
    )
    .bind(alice)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, claimed_at, \
         next_retry_at, created_at, last_error) VALUES \
         ('b1', 't1', '{}', 'pending', 0, NULL, 0, 82, NULL), \
         ('b1', 't2', '{}', 'sending', 1, 83, 0, 83, NULL), \
         ('b1', 't3', '{}', 'dead', 4, NULL, 0, 84, 'bad')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, dest_bucket, \
         is_delete_marker, state, attempts, claimed_at, next_retry_at, created_at, last_error, \
         target_kind, is_version_delete) VALUES \
         ('b1', 'k', 'v1', 'e', 5, 'peer-b', 'b2', 0, 'pending', 0, NULL, 0, 85, NULL, 'site', 0), \
         ('b1', 'k2', 'v2', 'e', 6, 'peer-b', 'b2', 1, 'sending', 2, 86, 0, 86, 'slow', 'remote', 1)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO sync_write_intents (id, bucket, key, local_version_id, remote_version_id, \
         tee_path, commit_json, state, created_at, updated_at) \
         VALUES ('w1', 'b1', 'k', 'v1', 'v9', '/tmp/t', '{}', 'pending', 87, 87)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO sync_delete_intents (id, bucket, key, op, local_version_id, state, \
         created_at, updated_at) VALUES ('d1', 'b1', 'k', 'delete', 'v1', 'pending', 88, 88)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO account_usage (account_id, used_bytes, object_count, complete, computed_at) \
         VALUES ('000000000007', 10, 2, 1, 90)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("INSERT INTO instant_index (k, v, expires_at) VALUES ('ik', 'iv', 91)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let applied = serde_json::to_string(&log_id(3)).unwrap();
    sqlx::query(
        "INSERT OR REPLACE INTO raft_applied (id, applied_log_id, membership_json) VALUES (1, ?, NULL)",
    )
    .bind(applied)
    .execute(&mut *tx)
    .await
    .unwrap();
    for (key, value) in [
        (crate::raft::compat::META_FEATURE_LEVEL, "9"),
        // 非空运行态在 feature 9 下必须已激活，否则 SQLite 启动守卫本身会拒绝。
        (crate::raft::compat::META_RUNSTATE_ACTIVATED, "1"),
        (crate::raft::compat::META_INVENTORY_ACTIVATED, "1"),
        (crate::db::META_ACCOUNTS_CANONICAL, "done"),
    ] {
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db.pool().close().await;
    drop(db);
    checkpoint(&control_url).await;

    let raft_url = format!("sqlite://{}?mode=rwc", raft.display());
    let mut store = SqliteLogStore::open(&raft).await.unwrap();
    store.purge(log_id(1)).await.unwrap();
    store.append_entries(&[blank(2), blank(3)]).await.unwrap();
    store.save_vote(&Vote::new_committed(2, 1)).await.unwrap();
    store.save_committed(Some(log_id(2))).await.unwrap();
    drop(store);
    let snap_meta = SnapshotMeta::<u64, BasicNode> {
        last_log_id: Some(log_id(2)),
        last_membership: StoredMembership::default(),
        snapshot_id: "snap-1".into(),
    };
    let pool = sqlx::SqlitePool::connect(&raft_url).await.unwrap();
    sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
        .bind(serde_json::to_string(&snap_meta).unwrap())
        .bind(b"snapshot-bytes".to_vec())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    checkpoint(&raft_url).await;

    Fixture {
        _dir: dir,
        control,
        raft,
        state,
    }
}

pub(super) async fn checkpoint(url: &str) {
    let pool = sqlx::SqlitePool::connect(url).await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

pub(super) async fn open_store(root: &Path) -> Arc<Store> {
    std::fs::create_dir_all(root).unwrap();
    let mut cfg = StoreConfig::new(root);
    cfg.fsync = false;
    Arc::new(Store::open(cfg).await.unwrap())
}

pub(super) async fn read_source(f: &Fixture) -> Source {
    source::read_control(&f.control, &f.raft).await.unwrap()
}

pub(super) async fn read_control(f: &Fixture) -> ObjectControlStore {
    let store = open_store(&f.state).await;
    ObjectControlStore::new(Arc::new(RootIo::new(store)))
}

pub(super) fn source_checksum(f: &Fixture) -> String {
    source::checksum(&f.control).unwrap()
}

pub(super) fn raft_checksum(f: &Fixture) -> String {
    source::checksum(&f.raft).unwrap()
}
