//! Level-3 账户/成员命令：Local apply、feature 门控与 OSSCTL3 快照往返。
//!
//! 账户命令只携带自然键（account id / username）与预计算值；apply 在本地解析 uid，
//! 因此这里同样按 username 断言，不跨节点比较 id。follower 转发见
//! `raft_accounts_follower.rs`。

mod common;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use oss_server::db::{self, Db};
use oss_server::raft::store::SqliteStateMachine;
use oss_server::raft::{ControlCommand, ControlResponse, TypeConfig, WriteError};
use oss_server::state::AppState;
use sqlx::SqlitePool;

const ACC: &str = "000000000001";
const EMPTY_ACC: &str = "000000000002";

fn create_account(id: &str, name: &str, quota: Option<i64>) -> ControlCommand {
    ControlCommand::AccountCreate {
        id: id.into(),
        name: name.into(),
        bucket_quota: quota,
        created_at: 10,
    }
}

fn update_account(name: Option<&str>, status: Option<&str>, updated_at: i64) -> ControlCommand {
    ControlCommand::AccountUpdate {
        id: ACC.into(),
        name: name.map(str::to_string),
        status: status.map(str::to_string),
        updated_at,
    }
}

fn set_quota(quota: Option<i64>, updated_at: i64) -> ControlCommand {
    ControlCommand::AccountSetQuota {
        id: ACC.into(),
        bucket_quota: quota,
        updated_at,
    }
}

fn set_soft_quota(quota: Option<i64>, updated_at: i64) -> ControlCommand {
    ControlCommand::AccountSetSoftQuota {
        id: ACC.into(),
        soft_byte_quota: quota,
        updated_at,
    }
}

fn attach(username: &str, role: &str, created_at: i64) -> ControlCommand {
    ControlCommand::AccountMemberAttach {
        account_id: ACC.into(),
        username: username.into(),
        role: role.into(),
        created_at,
    }
}

fn user_in_account(username: &str, created_at: i64) -> ControlCommand {
    ControlCommand::AccountUserCreate {
        account_id: ACC.into(),
        username: username.into(),
        password_hash: db::hash_password(&format!("{username}-password")),
        role: "member".into(),
        created_at,
    }
}

fn member_status(username: &str, active: bool) -> ControlCommand {
    ControlCommand::AccountMemberSetStatus {
        account_id: ACC.into(),
        username: username.into(),
        active,
    }
}

fn delete_account(id: &str) -> ControlCommand {
    ControlCommand::AccountDelete { id: id.into() }
}

fn user_create(username: &str) -> ControlCommand {
    ControlCommand::UserCreate {
        username: username.into(),
        password_hash: db::hash_password(&format!("{username}-password")),
        status: "active".into(),
        created_at: 1,
    }
}

async fn write(st: &AppState, cmd: ControlCommand) -> ControlResponse {
    st.control.write(cmd).await.unwrap()
}

async fn account_status(pool: &SqlitePool, id: &str) -> (String, Option<i64>, i64) {
    sqlx::query_as("SELECT name, bucket_quota, updated_at FROM accounts WHERE id = ?")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// 每个写路由对应的命令都能在 Local 门面（同一事务 apply）落库，业务拒绝走响应码。
#[tokio::test]
async fn local_control_applies_every_account_command() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;

    assert_eq!(write(&st, user_create("alice")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, create_account(ACC, "acme", Some(5))).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, create_account(ACC, "dup", None)).await,
        ControlResponse::Conflict
    );
    assert_eq!(
        account_status(st.db.pool(), ACC).await,
        ("acme".into(), Some(5), 10)
    );

    // rename + 清配额
    assert_eq!(
        write(&st, update_account(Some("acme-renamed"), None, 11)).await,
        ControlResponse::Ok
    );
    assert_eq!(write(&st, set_quota(None, 12)).await, ControlResponse::Ok);
    assert_eq!(
        account_status(st.db.pool(), ACC).await,
        ("acme-renamed".into(), None, 12)
    );

    // 软配额独立变体：只动 soft_byte_quota，不得清掉桶配额（反向亦然）。
    assert_eq!(
        write(&st, set_soft_quota(Some(1024), 12)).await,
        ControlResponse::Ok
    );
    let (soft, bucket): (Option<i64>, Option<i64>) =
        sqlx::query_as("SELECT soft_byte_quota, bucket_quota FROM accounts WHERE id = ?")
            .bind(ACC)
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(soft, Some(1024));
    assert_eq!(bucket, None);
    assert_eq!(
        write(&st, set_soft_quota(None, 12)).await,
        ControlResponse::Ok
    );
    let soft: Option<i64> = sqlx::query_scalar("SELECT soft_byte_quota FROM accounts WHERE id = ?")
        .bind(ACC)
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(soft, None);

    // 挂载已有用户为账户 admin：admin_uid 只在新填入时补齐
    assert_eq!(
        write(&st, attach("alice", "admin", 13)).await,
        ControlResponse::Ok
    );
    let (role, admin_uid): (String, Option<i64>) = sqlx::query_as(
        "SELECT am.role, a.admin_uid FROM account_members am \
         JOIN accounts a ON a.id = am.account_id JOIN users u ON u.id = am.uid \
         WHERE am.account_id = ? AND u.username = 'alice'",
    )
    .bind(ACC)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(role, "admin");
    assert!(admin_uid.is_some());

    // 账户内建用户：is_admin 恒 0、口令哈希原样落库
    assert_eq!(
        write(&st, user_in_account("bob", 14)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, user_in_account("bob", 15)).await,
        ControlResponse::Conflict
    );
    let (is_admin, hash, member_role): (bool, String, String) = sqlx::query_as(
        "SELECT u.is_admin, u.password_hash, am.role FROM users u \
         JOIN account_members am ON am.uid = u.id WHERE u.username = 'bob'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert!(!is_admin);
    assert!(hash.starts_with("$argon2"));
    assert_eq!(member_role, "member");

    // 成员启停
    for (active, want) in [(false, "disabled"), (true, "active")] {
        assert_eq!(
            write(&st, member_status("bob", active)).await,
            ControlResponse::Ok
        );
        let status: String = sqlx::query_scalar("SELECT status FROM users WHERE username = 'bob'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
        assert_eq!(status, want);
    }

    // 业务拒绝：系统账户、非法角色、缺失账户、含成员账户不可删
    assert_eq!(
        write(&st, delete_account(xl_s3::access::LEGACY_ACCOUNT)).await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(&st, attach("alice", "owner", 16)).await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::AccountUpdate {
                id: "000000000999".into(),
                name: None,
                status: Some("disabled".into()),
                updated_at: 17,
            },
        )
        .await,
        ControlResponse::NotFound
    );
    assert_eq!(
        write(&st, delete_account(ACC)).await,
        ControlResponse::Conflict
    );

    // 全局管理员不属任何账户
    assert!(st.db.create_user("root", "pw", true).await.unwrap() > 0);
    assert_eq!(
        write(&st, attach("root", "member", 18)).await,
        ControlResponse::NotFound
    );

    // 空账户可删
    assert_eq!(
        write(&st, create_account(EMPTY_ACC, "empty", None)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, delete_account(EMPTY_ACC)).await,
        ControlResponse::Ok
    );
    let gone: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts WHERE id = ?")
        .bind(EMPTY_ACC)
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(gone, 0);
}

fn bucket_create(name: &str, owner: Option<&str>) -> ControlCommand {
    ControlCommand::BucketCreate {
        name: name.into(),
        owner: owner.map(str::to_string),
        created_at: 100,
        replication_mode: "async".into(),
        versioning: "Off".into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

/// AC-2：Local 门面建桶的配额判定在写事务内（`BEGIN IMMEDIATE`）——
/// quota=N 时 N+1 个并发建桶恰好 N 个成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_quota_is_atomic_in_local_apply() {
    let dir = tempfile::tempdir().unwrap();
    // 文件库（WAL）：内存 shared-cache 库并发 BEGIN IMMEDIATE 会 SQLITE_LOCKED。
    let st = common::state_with_file_db(dir.path()).await;
    assert_eq!(write(&st, user_create("alice")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, create_account(ACC, "acme", Some(2))).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, attach("alice", "member", 2)).await,
        ControlResponse::Ok
    );

    let mut tasks = Vec::new();
    for i in 0..3 {
        let control = Arc::clone(&st.control);
        tasks.push(tokio::spawn(async move {
            control
                .write(bucket_create(&format!("local-{i}"), Some("alice")))
                .await
        }));
    }
    let (mut ok, mut quota) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            Ok(ControlResponse::Ok) => ok += 1,
            Err(oss_server::raft::control::ControlError::QuotaExceeded) => quota += 1,
            other => panic!("unexpected control result: {other:?}"),
        }
    }
    assert_eq!((ok, quota), (2, 1), "N+1 并发恰好 N 成功、1 拒绝");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 2, "落库桶数不得超过配额");
}

/// AC-2 修订：leader append 前配额闸（进程内只串行 BucketCreate）——
/// quota=N 时 N+1 个并发恰好 N 成功；重建已有桶与 NULL 配额不判。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_quota_is_atomic_on_raft_leader() {
    let dir = tempfile::tempdir().unwrap();
    let (handle, pool) = spawn_raft(dir.path(), 3).await;
    assert_eq!(
        handle
            .write(create_account(ACC, "acme", Some(2)))
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handle.write(user_create("alice")).await.unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handle.write(attach("alice", "member", 2)).await.unwrap(),
        ControlResponse::Ok
    );

    let mut tasks = Vec::new();
    for i in 0..3 {
        let handle = Arc::clone(&handle);
        tasks.push(tokio::spawn(async move {
            handle
                .client_write(bucket_create(&format!("r-{i}"), Some("alice")))
                .await
        }));
    }
    let (mut ok, mut quota) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            Ok(ControlResponse::Ok) => ok += 1,
            Err(WriteError::QuotaExceeded) => quota += 1,
            other => panic!("unexpected write result: {other:?}"),
        }
    }
    assert_eq!((ok, quota), (2, 1), "N+1 并发恰好 N 成功、1 拒绝");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 2, "落库桶数不得超过配额");

    // 重建已有桶不增加桶数：满配额下的收敛仍放行。
    assert_eq!(
        handle
            .write(bucket_create("r-0", Some("alice")))
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    // NULL = 不限，恢复放行。
    assert_eq!(
        handle.write(set_quota(None, 20)).await.unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handle
            .write(bucket_create("r-3", Some("alice")))
            .await
            .unwrap(),
        ControlResponse::Ok,
        "NULL = 不限"
    );
    handle.shutdown().await;
}

/// 回归：账户停用后 `who()`（经 HTTP 面）与 `account_active` 都拒绝该成员。
#[tokio::test]
async fn disabled_account_blocks_who_and_account_active() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("carol")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, create_account(ACC, "frozen", None)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, attach("carol", "member", 2)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, update_account(None, Some("disabled"), 3)).await,
        ControlResponse::Ok
    );

    let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'carol'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert!(!st.db.account_active(uid).await.unwrap());

    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(token(uid))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "停用账户的旧 JWT 必须立即失活");
}

/// level-2 集群必须在 append 前拒绝 level-3 账户命令；level 3 接受并激活 marker。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn account_commands_require_level_three() {
    let dir2 = tempfile::tempdir().unwrap();
    let (handle2, pool2) = spawn_raft(dir2.path(), 2).await;
    match handle2.write(create_account(ACC, "too-early", None)).await {
        Err(WriteError::FeatureUnavailable {
            required,
            effective,
        }) => assert_eq!((required, effective), (3, 2)),
        other => panic!("level 2 must reject account commands before append, got {other:?}"),
    }
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
        .fetch_one(&pool2)
        .await
        .unwrap();
    assert_eq!(rows, 0, "rejected command must not reach apply");
    assert_eq!(
        oss_server::raft::compat::read_activation(&pool2)
            .await
            .unwrap(),
        1,
        "rejected command must not activate the level-3 marker"
    );
    handle2.shutdown().await;

    let dir3 = tempfile::tempdir().unwrap();
    let (handle3, pool3) = spawn_raft(dir3.path(), 3).await;
    assert_eq!(
        handle3
            .write(create_account(ACC, "enabled", None))
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let name: String = sqlx::query_scalar("SELECT name FROM accounts WHERE id = ?")
        .bind(ACC)
        .fetch_one(&pool3)
        .await
        .unwrap();
    assert_eq!(name, "enabled");
    assert_eq!(
        oss_server::raft::compat::read_activation(&pool3)
            .await
            .unwrap(),
        3
    );
    handle3.shutdown().await;
}

/// 账户/成员经命令落库 → OSSCTL3 快照 → 全新控制库安装：按 username 还原。
#[tokio::test]
async fn ossctl3_snapshot_roundtrip_keeps_accounts_by_username() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("alice")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, create_account(ACC, "acme", Some(5))).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, attach("alice", "admin", 20)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, user_in_account("bob", 21)).await,
        ControlResponse::Ok
    );
    // 快照只在 canonical marker 之后携带账户行；level 3 才允许规范化账户。
    st.db
        .schema_meta_set(db::META_ACCOUNTS_CANONICAL, "done")
        .await
        .unwrap();
    st.db
        .schema_meta_set("raft_feature_level", "3")
        .await
        .unwrap();

    let mut sm_a =
        SqliteStateMachine::open_with_pool(st.db.pool().clone(), 1 << 20, false, None, 3)
            .await
            .unwrap();
    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
        .await
        .unwrap();
    let bytes = snap.snapshot.get_ref().clone();
    let decoded = oss_server::raft::data::decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 3);
    let acme = decoded
        .data
        .accounts
        .iter()
        .find(|a| a.id == ACC)
        .expect("账户行必须进 level-3 快照");
    assert_eq!(acme.admin.as_deref(), Some("alice"));
    assert!(
        decoded
            .data
            .account_members
            .iter()
            .any(|m| m.username == "bob" && m.role == "member"),
        "快照成员行必须是 username 自然键"
    );

    let target = connect_raft(dir.path(), "target").await;
    let mut sm_b =
        SqliteStateMachine::open_with_pool(target.pool().clone(), 1 << 20, false, None, 3)
            .await
            .unwrap();
    RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut sm_b,
        &snap.meta,
        Box::new(std::io::Cursor::new(bytes)),
    )
    .await
    .unwrap();

    let (name, admin, quota, status): (String, Option<String>, Option<i64>, String) =
        sqlx::query_as(
            "SELECT a.name, u.username, a.bucket_quota, a.status FROM accounts a \
         LEFT JOIN users u ON u.id = a.admin_uid WHERE a.id = ?",
        )
        .bind(ACC)
        .fetch_one(target.pool())
        .await
        .unwrap();
    assert_eq!(name, "acme");
    assert_eq!(admin.as_deref(), Some("alice"));
    assert_eq!(quota, Some(5));
    assert_eq!(status, "active");

    let members: Vec<(String, String)> = sqlx::query_as(
        "SELECT u.username, am.role FROM account_members am JOIN users u ON u.id = am.uid \
         ORDER BY u.username",
    )
    .fetch_all(target.pool())
    .await
    .unwrap();
    assert_eq!(
        members,
        vec![
            ("alice".to_string(), "admin".to_string()),
            ("bob".to_string(), "member".to_string()),
        ]
    );
    assert!(target.accounts_canonical().await.unwrap());
    assert_eq!(
        oss_server::raft::compat::read_activation(target.pool())
            .await
            .unwrap(),
        3
    );
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn token(uid: i64) -> String {
    #[derive(serde::Serialize)]
    struct Claims {
        sub: String,
        exp: usize,
    }
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 3600;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &Claims {
            sub: uid.to_string(),
            exp,
        },
        &jsonwebtoken::EncodingKey::from_secret(common::JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

async fn connect_raft(dir: &std::path::Path, name: &str) -> Db {
    Db::connect_raft(&format!(
        "sqlite://{}?mode=rwc",
        dir.join(format!("{name}.db")).display()
    ))
    .await
    .unwrap()
}

async fn spawn_raft(
    dir: &std::path::Path,
    feature_level: u8,
) -> (Arc<oss_server::raft::RaftHandle>, SqlitePool) {
    let db = connect_raft(dir, "control").await;
    let mut settings = common::raft_harness::settings(dir, 1, true);
    settings.feature_level = feature_level;
    let node = common::raft_harness::spawn_node_with_pool(settings, Some(db.pool().clone())).await;
    common::raft_harness::wait_leader(&node.handle).await;
    (node.handle, db.pool().clone())
}
