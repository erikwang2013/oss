//! Raft 启动时的 canonical 账户守卫：空库补 marker，非 canonical 行拒绝启动。
//!
//! 这两条规则保证 fresh Raft 集群的账户行能进入 OSSCTL3 快照，同时不把节点本地
//! uid 派生行带进复制面。

mod common;

use openraft::storage::RaftSnapshotBuilder;
use oss_server::db::{self, Db};
use oss_server::raft::store::SqliteStateMachine;
use oss_server::raft::{ControlCommand, ControlResponse, TypeConfig};

const ACC: &str = "000000000001";

fn account_create(id: &str) -> ControlCommand {
    ControlCommand::AccountCreate {
        id: id.into(),
        name: "acme".into(),
        bucket_quota: Some(5),
        created_at: 10,
    }
}

fn db_url(dir: &std::path::Path, name: &str) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        dir.join(format!("{name}.db")).display()
    )
}

/// fresh Raft DB 启动即补 marker；命令建的账户随后必须出现在 OSSCTL3 快照里。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_raft_marks_canonical_and_snapshots_created_accounts() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(dir.path(), "control");
    let db = Db::connect_raft(&url).await.unwrap();
    assert!(
        db.accounts_canonical().await.unwrap(),
        "空账户表的 fresh Raft DB 必须补 canonical marker"
    );

    let mut settings = common::raft_harness::settings(dir.path(), 1, true);
    settings.feature_level = 3;
    let node = common::raft_harness::spawn_node_with_pool(settings, Some(db.pool().clone())).await;
    common::raft_harness::wait_leader(&node.handle).await;
    assert_eq!(
        node.handle.write(account_create(ACC)).await.unwrap(),
        ControlResponse::Ok
    );

    let mut sm = SqliteStateMachine::open_with_pool(db.pool().clone(), 1 << 20, false, None, 3)
        .await
        .unwrap();
    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm)
        .await
        .unwrap();
    let bytes = snap.snapshot.get_ref().clone();
    let decoded = oss_server::raft::data::decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 3);
    assert!(
        decoded.data.accounts.iter().any(|a| a.id == ACC),
        "fresh Raft 集群的命令账户必须进入 OSSCTL3 快照"
    );

    node.shutdown().await;
    drop(db);
    // 重启再走同一守卫：marker 已存在时必须幂等放行。
    let reopened = Db::connect_raft(&url).await.unwrap();
    assert!(reopened.accounts_canonical().await.unwrap());
}

/// 有账户行但没有 canonical marker：Raft 启动必须 fail-closed，不能静默跑回填。
#[tokio::test]
async fn raft_refuses_non_canonical_account_rows() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(dir.path(), "noncanonical");
    {
        // standalone 首次启动会回填系统账户并写 marker；删 marker 即模拟非 canonical 来源。
        let db = Db::connect(&url).await.unwrap();
        sqlx::query("DELETE FROM schema_meta WHERE key = ?")
            .bind(db::META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert!(rows > 0, "standalone 回填必须留下系统账户行");
    }

    let err = match Db::connect_raft(&url).await {
        Ok(_) => panic!("非 canonical 账户行不能以 Raft 模式启动"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains(db::META_ACCOUNTS_CANONICAL), "{msg}");
    assert!(msg.contains("快照"), "{msg}");
}
