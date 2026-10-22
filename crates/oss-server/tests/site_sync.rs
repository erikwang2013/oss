//! M7 切片 1b：配对快照（单事务、入钟、只本地胜者）+ 出货 worker + status。

mod common;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::db::{PermissionRow, SitePeer};
use oss_server::site::Entry;
use oss_server::state::AppState;
use serde_json::json;
use sqlx::Row;
use xl_store::store::VersioningMode;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn site(name: &str, ch: char) -> SiteToml {
    SiteToml {
        id: None,
        name: Some(name.into()),
        region: "us-east-1".into(),
        priority: 0,
        token: Some(ch.to_string().repeat(32)),
    }
}

fn peer(peer_id: &str, remote_site_id: &str, endpoint: &str) -> SitePeer {
    SitePeer {
        peer_id: peer_id.into(),
        remote_site_id: remote_site_id.into(),
        name: peer_id.into(),
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "AKIATEST000000001".into(),
        s3_secret_key: "SKTEST000000000001".into(),
        peer_site_token: "b".repeat(32),
        created_at: now(),
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn cl_count(st: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog")
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

async fn cursor(st: &AppState, peer_id: &str) -> i64 {
    sqlx::query_scalar("SELECT last_seq FROM site_cursors WHERE peer_id = ?")
        .bind(peer_id)
        .fetch_optional(st.db.pool())
        .await
        .unwrap()
        .unwrap_or(0)
}

async fn scalar_i64(st: &AppState, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn snapshot_ship_apply_and_replay() {
    // A：源站；B：接收站（先起 HTTP 服务）。
    let da = tempfile::tempdir().unwrap();
    let db_dir = tempfile::tempdir().unwrap();
    let a = state_with_site(da.path(), Some(site("a", 'a'))).await;
    let b = state_with_site(db_dir.path(), Some(site("b", 'b'))).await;
    // B 配一个 peer 行（触发器的 EXISTS(sites) 激活条件），以验证 apply 不产生 changelog。
    b.db.insert_site(&peer("a", &a.site.id, "http://127.0.0.1:9"))
        .await
        .unwrap();
    let base_b = serve(oss_server::router::build_router(b.clone()).unwrap()).await;

    // A 的既有资源（配对前写入，不被捕获；快照负责携带）。
    a.db.create_user("root", "pw", true).await.unwrap();
    let alice = a.db.create_user("alice", "pw", false).await.unwrap();
    a.db.create_access_key("AKALICE000000000001", "SKALICE000000000001", alice)
        .await
        .unwrap();
    a.db.set_permissions(
        alice,
        &[PermissionRow {
            bucket: "bkt".into(),
            read: true,
            write: false,
            delete: false,
        }],
    )
    .await
    .unwrap();
    a.db.insert_bucket("bkt", Some(alice)).await.unwrap();
    a.store.create_bucket("bkt").await.unwrap();
    sqlx::query(
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('bkt', ?, ?)",
    )
    .bind(r#"{"Version":"2012-10-17","Statement":[]}"#)
    .bind(now())
    .execute(a.db.pool())
    .await
    .unwrap();
    // bob：本地时钟显示远端已胜出 → 快照不得回显（R4）。
    a.db.create_user("bob", "pw", false).await.unwrap();
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('user', 'bob', ?, 0, 'remote-x', 7)",
    )
    .bind(now())
    .execute(a.db.pool())
    .await
    .unwrap();

    // 配对：sites + cursor + 快照同一事务。
    oss_server::site::insert_peer_with_snapshot(&a, &peer("b", &b.site.id, &base_b))
        .await
        .unwrap();
    let n = cl_count(&a).await;
    assert_eq!(n, 5, "alice + AK + permission + bucket + policy");
    let entities: Vec<(String, String)> = {
        let rows = sqlx::query("SELECT entity, entity_id FROM site_changelog ORDER BY seq")
            .fetch_all(a.db.pool())
            .await
            .unwrap();
        rows.iter()
            .map(|r| (r.get("entity"), r.get("entity_id")))
            .collect()
    };
    assert!(
        !entities.iter().any(|(_, id)| id == "bob"),
        "远端胜者 bob 不得进快照：{entities:?}"
    );
    assert!(
        !entities.iter().any(|(e, id)| e == "user" && id == "root"),
        "管理员不得进快照"
    );
    // 快照实体入本地钟（origin=本站）。
    let origin: String = sqlx::query_scalar(
        "SELECT origin_site_id FROM site_entity_clock WHERE entity='user' AND entity_id='alice'",
    )
    .fetch_one(a.db.pool())
    .await
    .unwrap();
    assert_eq!(origin, a.site.id);

    // 出货 → B 应用。
    let (peers, entries) = oss_server::site::ship_once(a.db.pool()).await.unwrap();
    assert_eq!((peers, entries), (1, 5));
    assert_eq!(cursor(&a, "b").await, n);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT password_hash FROM users WHERE username='alice'")
            .fetch_one(b.db.pool())
            .await
            .unwrap(),
        sqlx::query_scalar::<_, String>("SELECT password_hash FROM users WHERE username='alice'")
            .fetch_one(a.db.pool())
            .await
            .unwrap(),
    );
    assert!(b.store.bucket_exists("bkt").await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM permissions p JOIN users u ON u.id=p.user_id \
             WHERE u.username='alice' AND p.bucket='bkt' AND p.can_read=1"
        )
        .fetch_one(b.db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM access_keys k JOIN users u ON u.id=k.user_id \
             WHERE u.username='alice' AND k.kind='long-term'"
        )
        .fetch_one(b.db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        scalar_i64(
            &b,
            "SELECT COUNT(*) FROM bucket_policies WHERE bucket='bkt'"
        )
        .await,
        1
    );
    // apply_ctx 复位；接收端不产生 changelog（触发器被抑制）。
    assert_eq!(
        scalar_i64(&b, "SELECT active FROM apply_ctx WHERE id=1").await,
        0
    );
    assert_eq!(cl_count(&b).await, 0, "apply 期不得捕获");

    // 增量：A 更新用户状态 → 触发器捕获 → 出货 → B 跟随。
    sqlx::query("UPDATE users SET status='disabled' WHERE username='alice'")
        .execute(a.db.pool())
        .await
        .unwrap();
    let (peers, entries) = oss_server::site::ship_once(a.db.pool()).await.unwrap();
    assert_eq!((peers, entries), (1, 1));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM users WHERE username='alice'")
            .fetch_one(b.db.pool())
            .await
            .unwrap(),
        "disabled"
    );

    // 2xx 丢失重投：cursor 回拨 → 全量重放被时钟跳过，不得报错/不得分叉。
    sqlx::query("UPDATE site_cursors SET last_seq = 0 WHERE peer_id='b'")
        .execute(a.db.pool())
        .await
        .unwrap();
    let (peers, entries) = oss_server::site::ship_once(a.db.pool()).await.unwrap();
    assert_eq!((peers, entries), (1, 6));
    assert_eq!(
        cursor(&a, "b").await,
        scalar_i64(&a, "SELECT MAX(seq) FROM site_changelog").await
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM users WHERE username='alice'")
            .fetch_one(b.db.pool())
            .await
            .unwrap(),
        "disabled"
    );

    // status 面（管理员）：lag=0 / dead=0 / outbox 计数。
    let base_a = serve(oss_server::router::build_router(a.clone()).unwrap()).await;
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base_a}/api/v1/login"))
        .json(&serde_json::json!({"username":"root","password":"pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tok = login["token"].as_str().unwrap();
    let status: serde_json::Value = reqwest::Client::new()
        .get(format!("{base_a}/api/v1/sites/b/status"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["control_lag"], 0);
    assert_eq!(status["control_dead"], 0);
    assert_eq!(status["object_pending"], 0);
    assert_eq!(status["remote_site_id"], b.site.id);

    // 不可达 peer：cursor 不推进 + last_error 记录（下轮重试）。
    a.db.insert_site(&peer("c", "remote-c", "http://127.0.0.1:1"))
        .await
        .unwrap();
    let (peers, _) = oss_server::site::ship_once(a.db.pool()).await.unwrap();
    assert_eq!(peers, 0);
    assert_eq!(cursor(&a, "c").await, 0);
    let err: Option<String> = sqlx::query_scalar("SELECT last_error FROM sites WHERE peer_id='c'")
        .fetch_one(a.db.pool())
        .await
        .unwrap();
    assert!(err.is_some(), "失败应记 last_error");

    // P1-3：解配丢弃 site 行后，仅剩该行的版本必须结算（不得永久 PENDING）。
    let vinfo = a
        .store
        .put_object_stream_with_meta(
            "bkt",
            "site-k",
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"v"))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let site_vid = vinfo.version_id.unwrap();
    sqlx::query(
        "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, dest_bucket, \
         target_kind, created_at) VALUES ('bkt','site-k',?,?,1,'c','bkt','site',?)",
    )
    .bind(&site_vid)
    .bind(&vinfo.etag)
    .bind(now())
    .execute(a.db.pool())
    .await
    .unwrap();
    // 解除配对（HTTP 面）：同事务丢弃该 peer 的 site 出站行与游标，随后结算受影响版本。
    let r = reqwest::Client::new()
        .delete(format!("{base_a}/api/v1/sites/c"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let h = a
        .store
        .head_object_version("bkt", "site-k", &site_vid)
        .await
        .unwrap();
    assert_eq!(
        h.replication_status, None,
        "解配断链的版本清除复制状态（不再有目的地，不谎报 COMPLETED）"
    );
    assert_eq!(
        scalar_i64(
            &a,
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id='c'"
        )
        .await,
        0
    );
    assert_eq!(cursor(&a, "c").await, 0); // 游标行不存在时 helper 返回 0

    // P1-3（worker 路径）：peer 已不存在 → 丢行并结算（清除复制状态，不永久 PENDING）。
    let drop_info = a
        .store
        .put_object_stream_with_meta(
            "bkt",
            "drop-k",
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"d"))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let drop_vid = drop_info.version_id.clone().unwrap();
    sqlx::query(
        "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, dest_bucket, \
         target_kind, created_at) VALUES ('bkt','drop-k',?,?,1,'gone','bkt','site',?)",
    )
    .bind(&drop_vid)
    .bind(&drop_info.etag)
    .bind(now())
    .execute(a.db.pool())
    .await
    .unwrap();
    let (_sent, _failed) = oss_server::replication::process_repl_once(
        a.db.pool(),
        &a.store,
        &oss_server::repl_client::ReplClient::new(&[]),
    )
    .await;
    let h = a
        .store
        .head_object_version("bkt", "drop-k", &drop_vid)
        .await
        .unwrap();
    assert_eq!(h.replication_status, None, "peer 消失后清除复制状态");
    assert_eq!(
        scalar_i64(
            &a,
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id='gone'"
        )
        .await,
        0
    );

    // L3：接收端死信原因写入 sites.last_error（status 可见）。
    b.db.create_user("badmin", "pw", true).await.unwrap();
    oss_server::site::apply_changelog(
        &b,
        vec![Entry {
            seq: 99,
            origin_site_id: a.site.id.clone(),
            entity: "user".into(),
            entity_id: "badmin".into(),
            op: "upsert".into(),
            payload: json!({"username":"badmin","password_hash":"evil","status":"active","created_at":1}),
            priority: 0,
            ts: now(),
        }],
    )
    .await
    .unwrap();
    let last_error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM sites WHERE peer_id='a'")
            .fetch_one(b.db.pool())
            .await
            .unwrap();
    assert!(
        last_error.unwrap_or_default().contains("管理员"),
        "dead 原因应进 last_error"
    );

    // L4：损坏的 changelog 行拒绝推进 cursor（返回 Err，下轮可见）。
    sqlx::query(
        "INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, \
         priority, ts, created_at) VALUES (?, 'user', 'x', 'upsert', 'not-json', 0, ?, ?)",
    )
    .bind(&a.site.id)
    .bind(now())
    .bind(now())
    .execute(a.db.pool())
    .await
    .unwrap();
    let err = oss_server::site::ship_once(a.db.pool()).await.unwrap_err();
    assert!(err.contains("非 JSON"), "{err}");
}
