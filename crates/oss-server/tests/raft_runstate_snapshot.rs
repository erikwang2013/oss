//! M8 Task 4：`m8.runstate` 随 OSSCTL3 快照与导出文件往返。
//!
//! 覆盖：扩展随快照编解码、导出同事务读取运行态（level 3 才导出）、
//! checksum 覆盖运行态、verify_export 拒绝低级别/畸形运行态。

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use serde::Serialize;
use sha2::{Digest, Sha256};

use oss_server::db::Db;
use oss_server::raft::cutover::{self, ExportFile};
use oss_server::raft::data::{self, ControlData};
use oss_server::raft::runstate::{
    JobRow, JobTaskRow, RunState, decode_runstate_snapshot, encode_runstate_snapshot,
    install_runstate_tx, read_runstate,
};
use oss_server::raft::store::SqliteStateMachine;
use oss_server::raft::{TypeConfig, compat};

fn db_url(dir: &tempfile::TempDir) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.db").display()
    )
}

async fn fresh_db() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::connect_raft(&db_url(&dir)).await.unwrap();
    (dir, db)
}

fn sample() -> RunState {
    RunState {
        jobs: vec![JobRow {
            id: 7,
            kind: "batch".into(),
            created_at: 10,
            updated_at: 11,
            ..JobRow::default()
        }],
        next_job_id: 8,
        ..RunState::default()
    }
}

async fn install(db: &Db, rs: &RunState) {
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, rs).await.unwrap();
    tx.commit().await.unwrap();
}

#[test]
fn runstate_snapshot_round_trip() {
    let rs = sample();
    let snap = encode_runstate_snapshot(&ControlData::default(), &rs, 3).unwrap();
    let (control, got) = decode_runstate_snapshot(&snap).unwrap();
    assert_eq!(control, ControlData::default());
    assert_eq!(got, rs);
    assert_eq!(
        data::decode_snapshot(&snap).unwrap().extensions["m8.runstate"].version,
        1
    );
    assert!(
        encode_runstate_snapshot(&ControlData::default(), &rs, 2).is_err(),
        "非空运行态不得写入 level-2 快照"
    );
}

#[test]
fn snapshot_without_extension_decodes_empty_runstate() {
    let snap = data::encode_snapshot_at(&ControlData::default(), 2).unwrap();
    let (_, rs) = decode_runstate_snapshot(&snap).unwrap();
    assert!(rs.is_empty());
}

/// 真实状态机路径：build 在同一读事务里取运行态，install 与 11 表同事务替换。
#[tokio::test]
async fn snapshot_build_and_install_carries_runstate() {
    let (_dir_a, db_a) = fresh_db().await;
    db_a.schema_meta_set("raft_feature_level", "3")
        .await
        .unwrap();
    install(&db_a, &sample()).await;
    let mut sm_a = SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 3)
        .await
        .unwrap();
    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
        .await
        .unwrap();
    let bytes = snap.snapshot.get_ref().clone();
    let (_, decoded) = decode_runstate_snapshot(&bytes).unwrap();
    assert_eq!(decoded, sample(), "快照必须携带 level-3 运行态");

    let (_dir_b, db_b) = fresh_db().await;
    let mut sm_b = SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 3)
        .await
        .unwrap();
    RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut sm_b,
        &snap.meta,
        Box::new(std::io::Cursor::new(bytes)),
    )
    .await
    .unwrap();
    assert_eq!(read_runstate(db_b.pool()).await.unwrap(), sample());
    assert_eq!(
        compat::read_activation(db_b.pool()).await.unwrap(),
        3,
        "安装 OSSCTL3 必须激活 marker"
    );
}

async fn export_at_level_three(db: &Db) -> ExportFile {
    db.schema_meta_set("raft_feature_level", "3").await.unwrap();
    install(db, &sample()).await;
    cutover::build_export(
        db,
        "site-a".into(),
        Some(1),
        "voter",
        Some("admin".into()),
        Some(false),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn export_at_level_three_carries_runstate_and_verifies() {
    let (_dir, db) = fresh_db().await;
    let export = export_at_level_three(&db).await;
    assert_eq!(export.format, 2);
    assert_eq!(export.runstate, Some(sample()));
    assert_eq!(export.feature_level, Some(3));
    assert_eq!(export.snapshot_format, Some(3));
    assert_eq!(cutover::install_activation_level(&export), 3);
    assert!(cutover::verify_export(&export).unwrap().is_empty());
}

#[tokio::test]
async fn export_below_level_three_omits_local_runstate() {
    let (_dir, db) = fresh_db().await;
    install(&db, &sample()).await;
    let export = cutover::build_export(
        &db,
        "site-a".into(),
        Some(1),
        "voter",
        Some("admin".into()),
        Some(false),
    )
    .await
    .unwrap();
    assert_eq!(export.format, 2);
    assert_eq!(export.runstate, None, "level-3 之前运行态是节点本地状态");
    assert_eq!(export.snapshot_format, Some(1));
    assert!(cutover::verify_export(&export).unwrap().is_empty());
}

#[derive(Serialize)]
struct Payload<'a> {
    data: &'a ControlData,
    runstate: &'a RunState,
}

fn reseal(export: &mut ExportFile) {
    let payload = serde_json::to_vec(&Payload {
        data: &export.data,
        runstate: export.runstate.as_ref().unwrap(),
    })
    .unwrap();
    export.sha256 = hex::encode(Sha256::digest(&payload));
}

#[tokio::test]
async fn export_checksum_covers_runstate() {
    let (_dir, db) = fresh_db().await;
    let mut export = export_at_level_three(&db).await;
    export.runstate.as_mut().unwrap().jobs[0].kind = "tampered".into();
    let err = cutover::verify_export(&export).unwrap_err();
    assert!(err.contains("checksum"), "{err}");
}

#[tokio::test]
async fn verify_export_rejects_runstate_below_level_three() {
    let (_dir, db) = fresh_db().await;
    let mut export = export_at_level_three(&db).await;
    export.feature_level = Some(2);
    export.snapshot_format = Some(2);
    reseal(&mut export);
    let err = cutover::verify_export(&export).unwrap_err();
    assert!(err.contains("runstate"), "{err}");

    let mut export = export_at_level_three(&db).await;
    export.format = 1;
    let err = cutover::verify_export(&export).unwrap_err();
    assert!(err.contains("cannot carry runstate"), "{err}");
}

#[tokio::test]
async fn verify_export_rejects_semantically_invalid_runstate() {
    let (_dir, db) = fresh_db().await;

    let mut export = export_at_level_three(&db).await;
    export.runstate.as_mut().unwrap().next_job_id = 7;
    reseal(&mut export);
    let err = cutover::verify_export(&export).unwrap_err();
    assert!(
        err.contains("invalid runstate") && err.contains("next_job_id"),
        "{err}"
    );

    let mut export = export_at_level_three(&db).await;
    export
        .runstate
        .as_mut()
        .unwrap()
        .job_tasks
        .push(JobTaskRow {
            job_id: 99,
            task_no: 1,
            action: "copy".into(),
            payload_json: "{}".into(),
            ..JobTaskRow::default()
        });
    reseal(&mut export);
    let err = cutover::verify_export(&export).unwrap_err();
    assert!(
        err.contains("invalid runstate") && err.contains("job_tasks"),
        "{err}"
    );
}
