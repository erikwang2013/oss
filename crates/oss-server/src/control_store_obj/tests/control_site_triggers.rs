//! E2: frozen site-trigger matrix. All 15 mutations run twice against the
//! goldens: a future clock makes the gate reject, then the same mutation logs
//! one exact payload + clock row. Extras live in sibling modules.

use futures::future::BoxFuture;
use serde_json::json;
use xl_store::control_state::root::{AppliedLog, Mode};
use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};

use super::golden;
use super::site_common::{now, se};
use super::support::{self, Obj};
use crate::control_store_obj::site::{SiteChangelogRow, SiteEntityClockRow, clock_key};
use crate::control_store_obj::site_apply::{
    AccessKeySiteRow, BucketSiteRow, PermissionSiteRow, PolicySiteRow, UserSiteRow,
};
use crate::control_store_obj::{ObjectControlTxn, TableId, WriteOrigin};

pub(super) const SITE_ID: &str = "site-golden";
pub(super) const USER: &str = "alice\"q\\b\u{1}";
pub(super) const PASSWORD_HASH: &str = "hash\"q\\b\u{1}";
const BUCKET: &str = "bucket\"q\\b\u{1}";
const POLICY_JSON: &str = "{\"k\":\"q\\\"\\\\\\u0001\"}";
const AK: &str = "AK\"q\\b\u{1}";
const SK: &str = "SK\"q\\b\u{1}";

#[derive(Clone, Copy)]
pub(super) enum Ent {
    Bucket,
    Policy,
    Permission,
    User,
    AccessKey,
}

#[derive(Clone, Copy)]
pub(super) enum Op {
    Insert,
    Update,
    Delete,
}

const CASES: [(&str, Ent, Op); 15] = [
    ("bucket_ins", Ent::Bucket, Op::Insert),
    ("bucket_upd", Ent::Bucket, Op::Update),
    ("bucket_del", Ent::Bucket, Op::Delete),
    ("policy_ins", Ent::Policy, Op::Insert),
    ("policy_upd", Ent::Policy, Op::Update),
    ("policy_del", Ent::Policy, Op::Delete),
    ("perm_ins", Ent::Permission, Op::Insert),
    ("perm_upd", Ent::Permission, Op::Update),
    ("perm_del", Ent::Permission, Op::Delete),
    ("user_ins", Ent::User, Op::Insert),
    ("user_upd", Ent::User, Op::Update),
    ("user_del", Ent::User, Op::Delete),
    ("ak_ins", Ent::AccessKey, Op::Insert),
    ("ak_upd", Ent::AccessKey, Op::Update),
    ("ak_del", Ent::AccessKey, Op::Delete),
];

fn entity(ent: Ent) -> &'static str {
    match ent {
        Ent::Bucket => "bucket",
        Ent::Policy => "bucket_policy",
        Ent::Permission => "permission",
        Ent::User => "user",
        Ent::AccessKey => "access_key",
    }
}

fn entity_id(ent: Ent) -> String {
    match ent {
        Ent::Bucket | Ent::Policy => BUCKET.into(),
        Ent::Permission => format!("{USER}\u{1f}{BUCKET}"),
        Ent::User => USER.into(),
        Ent::AccessKey => AK.into(),
    }
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
    }
}

pub(super) fn bucket(versioning: &str) -> BucketSiteRow {
    BucketSiteRow {
        name: BUCKET.into(),
        owner_uid: Some(10),
        owner_username: Some(USER.into()),
        creation_date: 222,
        versioning: versioning.into(),
        object_lock: false,
    }
}

pub(super) fn policy(updated_at: i64) -> PolicySiteRow {
    PolicySiteRow {
        bucket: BUCKET.into(),
        policy_json: POLICY_JSON.into(),
        updated_at,
    }
}

pub(super) fn permission(can_read: bool) -> PermissionSiteRow {
    PermissionSiteRow {
        username: Some(USER.into()),
        bucket: BUCKET.into(),
        can_read,
        can_write: false,
        can_delete: true,
        owner_is_admin: Some(false),
    }
}

pub(super) fn user(status: &str) -> UserSiteRow {
    UserSiteRow {
        username: USER.into(),
        password_hash: PASSWORD_HASH.into(),
        status: status.into(),
        created_at: 111,
        is_admin: false,
    }
}

pub(super) fn access_key(status: &str) -> AccessKeySiteRow {
    AccessKeySiteRow {
        access_key: AK.into(),
        secret_key: SK.into(),
        username: Some(USER.into()),
        status: status.into(),
        expires_at: Some(444),
        created_at: 555,
        kind: "long-term".into(),
        owner_is_admin: Some(false),
    }
}

pub(super) async fn call(
    txn: &mut ObjectControlTxn,
    ent: Ent,
    op: Op,
    ts: i64,
) -> Result<(), ControlError> {
    match (ent, op) {
        (Ent::Bucket, Op::Insert) => txn.site_hook_bucket(None, Some(&bucket("Off")), ts).await,
        (Ent::Bucket, Op::Update) => {
            txn.site_hook_bucket(Some(&bucket("Off")), Some(&bucket("Enabled")), ts)
                .await
        }
        (Ent::Bucket, Op::Delete) => txn.site_hook_bucket(Some(&bucket("Off")), None, ts).await,
        (Ent::Policy, Op::Insert) => txn.site_hook_policy(None, Some(&policy(333)), ts).await,
        (Ent::Policy, Op::Update) => {
            txn.site_hook_policy(Some(&policy(333)), Some(&policy(334)), ts)
                .await
        }
        (Ent::Policy, Op::Delete) => txn.site_hook_policy(Some(&policy(333)), None, ts).await,
        (Ent::Permission, Op::Insert) => {
            txn.site_hook_permission(None, Some(&permission(true)), ts)
                .await
        }
        (Ent::Permission, Op::Update) => {
            txn.site_hook_permission(Some(&permission(true)), Some(&permission(false)), ts)
                .await
        }
        (Ent::Permission, Op::Delete) => {
            txn.site_hook_permission(Some(&permission(true)), None, ts)
                .await
        }
        (Ent::User, Op::Insert) => txn.site_hook_user(None, Some(&user("active")), ts).await,
        (Ent::User, Op::Update) => {
            txn.site_hook_user(Some(&user("active")), Some(&user("disabled")), ts)
                .await
        }
        (Ent::User, Op::Delete) => txn.site_hook_user(Some(&user("active")), None, ts).await,
        (Ent::AccessKey, Op::Insert) => {
            txn.site_hook_access_key(None, Some(&access_key("active")), ts)
                .await
        }
        (Ent::AccessKey, Op::Update) => {
            txn.site_hook_access_key(
                Some(&access_key("active")),
                Some(&access_key("disabled")),
                ts,
            )
            .await
        }
        (Ent::AccessKey, Op::Delete) => {
            txn.site_hook_access_key(Some(&access_key("active")), None, ts)
                .await
        }
    }
}

fn peer() -> SitePeerRow {
    SitePeerRow {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "peer-b".into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: "tok".into(),
        created_at: 1,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

async fn seed_site(real: &Obj, origin: WriteOrigin) {
    real.control
        .transaction(origin, |txn| {
            Box::pin(async move {
                txn.site_config_seed(SITE_ID, "tok", 1).await.map_err(se)?;
                txn.site_config_apply(None, None, Some(7), None)
                    .await
                    .map_err(se)?;
                txn.site_insert(&peer(), 1).await.map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

pub(super) async fn standalone_site() -> Obj {
    let real = support::object_store(Mode::Standalone).await;
    seed_site(&real, WriteOrigin::local("seed")).await;
    real
}

pub(super) async fn raft_site() -> Obj {
    let real = support::object_store(Mode::Raft).await;
    seed_site(
        &real,
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 1 }),
    )
    .await;
    real
}

pub(super) async fn obj_rows(real: &Obj) -> Vec<SiteChangelogRow> {
    real.control.site_changelog_after(0).await.unwrap()
}

pub(super) async fn run_origin<F>(real: &Obj, origin: WriteOrigin, f: F) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    let mut txn = real
        .control
        .begin(origin)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    f(&mut txn).await?;
    txn.commit()
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    Ok(())
}

pub(super) async fn run_hook<F>(real: &Obj, f: F) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    run_origin(real, WriteOrigin::local("test"), f).await
}

pub(super) async fn clock_rows(real: &Obj) -> Vec<SiteEntityClockRow> {
    let txn = real
        .control
        .begin(WriteOrigin::local("read"))
        .await
        .unwrap();
    let start: &[u8] = b"";
    let end: &[u8] = &[0xff];
    let mut rows: Vec<SiteEntityClockRow> = txn
        .scan(TableId::SiteEntityClock, start..end)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, v)| rmp_serde::from_slice(&v).unwrap())
        .collect();
    rows.sort_by(|a, b| (&a.entity, &a.entity_id).cmp(&(&b.entity, &b.entity_id)));
    rows
}

async fn cell(case: &str, ent: Ent, op: Op) {
    let real = standalone_site().await;
    let id = entity_id(ent);
    let gate = golden::load_json(&format!("site_triggers/gate/{case}.json"));
    assert_eq!(gate["case"], case);
    assert_eq!(gate["entity"], entity(ent));
    assert_eq!(gate["op"], op_name(op));
    let probe_priority = gate["probe"]["priority"].as_i64().unwrap();
    let probe_origin = gate["probe"]["origin_site_id"].as_str().unwrap();
    assert_eq!((probe_priority, probe_origin), (9, "remote-z"));

    let future = now() + 3600;
    let (entity_name, clock_id) = (entity(ent), id.clone());
    run_hook(&real, move |txn| {
        Box::pin(async move {
            txn.site_put_clock(&entity_name, &clock_id, future, 9, "remote-z", 1)
                .await
        })
    })
    .await
    .unwrap();

    let err = run_hook(&real, move |txn| {
        Box::pin(async move { call(txn, ent, op, now()).await })
    })
    .await
    .unwrap_err();
    let expected = gate["rejected"]["sqlite_message"].as_str().unwrap();
    match err {
        ControlError::Invalid(msg) => assert_eq!(msg, expected, "{case} gate message"),
        other => panic!("{case} gate returned {other}"),
    }
    assert!(
        obj_rows(&real).await.is_empty(),
        "{case} gate wrote a log row"
    );
    let clock = real
        .control
        .site_clock(entity(ent), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (clock.ts, clock.priority, clock.origin_site_id.as_str()),
        (future, 9, "remote-z"),
        "{case} gate moved the clock"
    );

    let key = clock_key(entity(ent), &id);
    run_hook(&real, move |txn| {
        Box::pin(async move {
            txn.delete(TableId::SiteEntityClock, &key)
                .await
                .map_err(|e| ControlError::Backend(e.to_string()))?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let ts = now();
    run_hook(&real, move |txn| {
        Box::pin(async move { call(txn, ent, op, ts).await })
    })
    .await
    .unwrap();
    let rows = obj_rows(&real).await;
    assert_eq!(rows.len(), 1, "{case}: expected one changelog row");
    let row = &rows[0];
    let clock = real
        .control
        .site_clock(entity(ent), &id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.ts > 0 && row.ts == ts && row.created_at == ts,
        "{case} ts"
    );
    assert_eq!(clock.ts, ts, "{case} clock ts");
    assert_eq!(clock.seq, row.seq, "{case} clock seq");
    let actual = json!({
        "case": case,
        "entity": &row.entity,
        "entity_id": &row.entity_id,
        "op": &row.op,
        "payload": &row.payload,
        "changelog": {
            "seq": row.seq,
            "origin_site_id": &row.origin_site_id,
            "entity": &row.entity,
            "entity_id": &row.entity_id,
            "op": &row.op,
            "priority": row.priority,
            "ts": 0,
            "created_at": 0,
        },
        "clock": {
            "entity": &clock.entity,
            "entity_id": &clock.entity_id,
            "ts": 0,
            "priority": clock.priority,
            "origin_site_id": &clock.origin_site_id,
            "seq": clock.seq,
        },
    });
    golden::assert_json(&format!("site_triggers/log/{case}.json"), &actual);
}

#[tokio::test]
async fn site_trigger_cells_match_goldens() {
    let mut gate_cells = 0;
    let mut log_cells = 0;
    for (case, ent, op) in CASES {
        cell(case, ent, op).await;
        gate_cells += 1;
        log_cells += 1;
    }
    assert_eq!((gate_cells, log_cells), (15, 15), "30 trigger cells");
}
