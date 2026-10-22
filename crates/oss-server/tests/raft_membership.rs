//! M9 S7.3：3-voter 成员变更、leader 感知转发、follower 控制写与站点移除前置条件。

mod common;

use std::path::Path;
use std::time::Duration;

use common::raft_harness::{self, TestNode};
use oss_server::db::Db;
use oss_server::raft::membership::{self, MembershipError, MembershipOp};
use oss_server::raft::{ControlCommand, RPC_TARGET_HEADER, RPC_TOKEN_HEADER};
use serde_json::json;

async fn control_db(dir: &Path, name: &str) -> Db {
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.join(format!("{name}.db")).display()
    );
    Db::connect_raft(&url).await.unwrap()
}

async fn spawn(dir: &Path, node_id: u64, bootstrap: bool, db: &Db) -> TestNode {
    let mut s = raft_harness::settings(dir, node_id, bootstrap);
    s.cluster_token = "s73-token".into();
    raft_harness::spawn_node_with_pool(s, Some(db.pool().clone())).await
}

async fn wait_leader_id(handle: &oss_server::raft::RaftHandle) -> u64 {
    raft_harness::wait_leader(handle).await
}

async fn wait_leader_of(nodes: &[&TestNode], expected: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if nodes
            .iter()
            .any(|n| membership::view(&n.handle).leader == Some(expected))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader {expected} not observed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn three_voters(dir: &Path) -> (TestNode, TestNode, TestNode) {
    let a_db = control_db(dir, "a").await;
    let b_db = control_db(dir, "b").await;
    let c_db = control_db(dir, "c").await;
    let a = spawn(dir, 1, true, &a_db).await;
    let b = spawn(dir, 2, false, &b_db).await;
    let c = spawn(dir, 3, false, &c_db).await;
    assert_eq!(wait_leader_id(&a.handle).await, 1);
    membership::promote_voter(&a.handle, 2, b.base.clone())
        .await
        .unwrap();
    membership::promote_voter(&a.handle, 3, c.base.clone())
        .await
        .unwrap();
    let v = membership::view(&a.handle);
    assert_eq!(v.voters, vec![1, 2, 3]);
    (a, b, c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_follower_write_remove_and_forwarded_depth() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = three_voters(dir.path()).await;

    // follower 控制写：B 本地提交失败 → 单跳转交 leader，返回 leader 的响应并等本地 apply。
    wait_leader_of(&[&a, &b, &c], 1).await;
    let resp = b
        .handle
        .client_write(ControlCommand::UserCreate {
            username: "fw-user".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        })
        .await
        .unwrap();
    assert_eq!(resp, oss_server::raft::ControlResponse::Ok);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE username='fw-user'")
        .fetch_one(b.pool.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(n, 1, "follower 必须等到本地 apply");

    // 已转发标记的请求不得二次转发：对 follower 发带该头的写 → 409。
    let r = reqwest::Client::new()
        .post(format!("{}/api/v1/site/raft/write", b.base))
        .header(RPC_TOKEN_HEADER, "s73-token")
        .header(RPC_TARGET_HEADER, "2")
        .header("x-oss-raft-forwarded", "1")
        .json(&ControlCommand::UserCreate {
            username: "loop".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 409, "forwarded 深度必须为 1");

    // 错 token：403；缺 token：401。
    for (token, want) in [(None, 401), (Some("wrong"), 403)] {
        let mut req = reqwest::Client::new()
            .post(format!("{}/api/v1/site/raft/membership", a.base))
            .json(&json!({"op":"remove_node","node_id":99}));
        if let Some(t) = token {
            req = req.header(RPC_TOKEN_HEADER, t);
        }
        assert_eq!(req.send().await.unwrap().status().as_u16(), want);
    }

    // leader 移除 B → {1,3}；再移除 C 会只剩 1 voter → 409/Invalid，且不变更。
    let v = membership::apply_local(&a.handle, &MembershipOp::RemoveNode { node_id: 2 })
        .await
        .unwrap();
    assert_eq!(v.voters, vec![1, 3]);
    let err = membership::apply_local(&a.handle, &MembershipOp::RemoveNode { node_id: 3 })
        .await
        .unwrap_err();
    assert!(matches!(err, MembershipError::Invalid(_)), "{err}");
    assert_eq!(membership::view(&a.handle).voters, vec![1, 3]);

    for n in [a, b, c] {
        n.handle.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_voter_and_leader_aware_request() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = three_voters(dir.path()).await;
    let d_db = control_db(dir.path(), "d").await;
    let d = spawn(dir.path(), 4, false, &d_db).await;
    wait_leader_of(&[&a, &b, &c], 1).await;

    // follower B 发成员请求：路由转发到 leader 执行替换，旧 voter 3 消失。
    let v = membership::request(
        &b.base,
        "s73-token",
        &MembershipOp::ReplaceVoter {
            node_id: 4,
            endpoint: d.base.clone(),
            old_node_id: 3,
        },
    )
    .await
    .unwrap();
    assert_eq!(v.voters, vec![1, 2, 4]);
    assert!(!v.voters.contains(&3));
    assert_eq!(membership::view(&a.handle).voters, vec![1, 2, 4]);

    // 替换后的新节点可被移除；剩 2 voters；再移除一个会被拒绝。
    let v = membership::apply_local(&a.handle, &MembershipOp::RemoveNode { node_id: 2 })
        .await
        .unwrap();
    assert_eq!(v.voters, vec![1, 4]);
    assert!(matches!(
        membership::apply_local(&a.handle, &MembershipOp::RemoveNode { node_id: 4 })
            .await
            .unwrap_err(),
        MembershipError::Invalid(_)
    ));

    for n in [a, b, c, d] {
        n.handle.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_loss_and_forwarded_removal() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, c) = three_voters(dir.path()).await;
    a.handle.shutdown().await;
    drop(a);

    // B+C 仍是 quorum；等两端对同一新 leader 达成一致（避免把请求转给已死的 A），
    // 然后由 C（可能是 follower）发起移除 A；选举窗口内允许重试。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let v = loop {
        let vb = membership::view(&b.handle);
        let vc = membership::view(&c.handle);
        if vb.leader == vc.leader
            && matches!(vb.leader, Some(2) | Some(3))
            && let Ok(v) = membership::request(
                &c.base,
                "s73-token",
                &MembershipOp::RemoveNode { node_id: 1 },
            )
            .await
        {
            break v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no stable new leader / membership request"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(v.voters, vec![2, 3]);

    b.handle.shutdown().await;
    c.handle.shutdown().await;
}

/// 只应答 `/status` 的对端（模拟旧 wire / 已升级节点，不参与复制）。
async fn mock_status(wire: u32, feature: u8) -> String {
    let app = axum::Router::new().route(
        "/api/v1/site/raft/status",
        axum::routing::get(move || async move {
            axum::Json(serde_json::json!({"wire_version": wire, "feature_level": feature}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_prechecks_reject_legacy_and_unverifiable_peers() {
    let dir = tempfile::tempdir().unwrap();
    let db = control_db(dir.path(), "a").await;
    let a = spawn(dir.path(), 1, true, &db).await;
    assert_eq!(wait_leader_id(&a.handle).await, 1);
    let old = mock_status(1, 1).await;
    let new = mock_status(2, 2).await;

    // R5：目标仍是旧 wire → 拒绝加入。
    let err = membership::request(
        &a.base,
        &a.token,
        &MembershipOp::AddWitness {
            node_id: 2,
            endpoint: old.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, MembershipError::Incompatible(_)), "{err:?}");
    assert_eq!(membership::view(&a.handle).voters, vec![1]);

    // 旧 learner 已在成员表（滚动窗口内入列）→ R6 阻塞任何新成员变更。
    a.handle
        .raft()
        .add_learner(2, openraft::BasicNode::new(old), false)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !membership::view(&a.handle).learners.contains(&2) {
        assert!(tokio::time::Instant::now() < deadline, "learner not listed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let err = membership::request(
        &a.base,
        &a.token,
        &MembershipOp::AddWitness {
            node_id: 3,
            endpoint: new,
        },
    )
    .await
    .unwrap_err();
    match err {
        MembershipError::Incompatible(m) => assert!(m.contains("peer 2"), "{m}"),
        other => panic!("expected Incompatible, got {other:?}"),
    }
    assert_eq!(membership::view(&a.handle).voters, vec![1]);

    a.handle.shutdown().await;
}
