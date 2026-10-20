//! M7 切片 0：站点身份与关系管理 API（两实例 in-process 配对）。

mod common;

use common::{state, state_with_site};
use oss_server::config::SiteToml;
use oss_server::router::build_router;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username":"erik","password":"pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

fn site(name: &str, token_char: char) -> SiteToml {
    SiteToml {
        name: Some(name.into()),
        region: "us-east-1".into(),
        priority: 10,
        token: Some(token_char.to_string().repeat(32)),
    }
}

async fn pair_state(
    dir: &std::path::Path,
    name: &str,
    token_char: char,
) -> (oss_server::state::AppState, String) {
    let st = state_with_site(dir, Some(site(name, token_char))).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    (st, base)
}

#[tokio::test]
async fn site_identity_and_admin_gating() {
    let dir = tempfile::tempdir().unwrap();
    let (st, base) = pair_state(dir.path(), "a", 'a').await;
    // 无 JWT → 401；admin 可见本站 token
    let r = reqwest::get(format!("{base}/api/v1/site")).await.unwrap();
    assert_eq!(r.status(), 401);
    let tok = login(&base).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/site"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["name"], "a");
    assert_eq!(v["site_token"], "a".repeat(32));
    assert_eq!(v["has_vault"], false);
    assert!(v["id"].as_str().unwrap().len() == 36);

    // identity：token 面（无 JWT）
    let c = reqwest::Client::new();
    let r = c
        .get(format!("{base}/api/v1/site/identity"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "缺 token");
    let r = c
        .get(format!("{base}/api/v1/site/identity"))
        .header("x-oss-site-token", "b".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "错 token");
    let v: serde_json::Value = c
        .get(format!("{base}/api/v1/site/identity"))
        .header("x-oss-site-token", "a".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["name"], "a");
    assert!(v.get("site_token").is_none(), "identity 不得回显 token");

    // 子账户（非管理员）→ 403
    st.db.create_user("sub", "pw2", false).await.unwrap();
    let sub: serde_json::Value = c
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username":"sub","password":"pw2"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub_tok = sub["token"].as_str().unwrap();
    let r = c
        .get(format!("{base}/api/v1/site"))
        .bearer_auth(sub_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
}

#[tokio::test]
async fn site_pairing_flow_and_validation() {
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let (_sta, base_a) = pair_state(da.path(), "a", 'a').await;
    let (_stb, base_b) = pair_state(db.path(), "b", 'b').await;
    let tok_a = login(&base_a).await;
    let c = reqwest::Client::new();
    let b_ident: serde_json::Value = c
        .get(format!("{base_b}/api/v1/site/identity"))
        .header("x-oss-site-token", "b".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b_id = b_ident["id"].as_str().unwrap().to_string();

    let mk = |token: &str, region: &str| {
        serde_json::json!({
            "peer_id": "b",
            "name": "b",
            "endpoint": base_b,
            "region": region,
            "priority": 10,
            "s3_access_key": "AKIATEST000000001",
            "s3_secret_key": "SKTEST000000000001",
            "peer_site_token": token,
        })
    };
    // 错 token → 400（对端 identity 401）
    let r = c
        .post(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .json(&mk(&"f".repeat(32), "us-east-1"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // region 不一致 → 400
    let r = c
        .post(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .json(&mk(&"b".repeat(32), "eu-west-1"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // 成功
    let r = c
        .post(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .json(&mk(&"b".repeat(32), "us-east-1"))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let body = r.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    // 列表脱敏
    let list: Vec<serde_json::Value> = c
        .get(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["peer_id"], "b");
    assert_eq!(
        list[0]["remote_site_id"], b_id,
        "配对必须落对端 identity id"
    );
    assert_eq!(list[0]["has_credentials"], true);
    let s = serde_json::to_string(&list[0]).unwrap();
    assert!(!s.contains("SKTEST"), "{s}");
    assert!(!s.contains(&"b".repeat(32)), "{s}");
    // L8：changelog 的 origin 必须等于已配对 remote_site_id（伪造 → 401）。
    let r = c
        .post(format!("{base_a}/api/v1/site/changelog"))
        .header("x-oss-site-token", "b".repeat(32))
        .json(&serde_json::json!({"entries":[{
            "seq":1,"origin_site_id":"forged","entity":"user","entity_id":"x",
            "op":"upsert","payload":{},"priority":0,"ts":0}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // 第二个 peer → 400（M7 两站点）
    let mut cfg2 = mk(&"b".repeat(32), "us-east-1");
    cfg2["peer_id"] = "c".into();
    let r = c
        .post(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .json(&cfg2)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // ping 绿
    let r = c
        .post(format!("{base_a}/api/v1/sites/b/ping"))
        .bearer_auth(&tok_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // 删除 → 204；再列空
    let r = c
        .delete(format!("{base_a}/api/v1/sites/b"))
        .bearer_auth(&tok_a)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let list: Vec<serde_json::Value> = c
        .get(format!("{base_a}/api/v1/sites"))
        .bearer_auth(&tok_a)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list.is_empty());
    assert_ne!(b_id, "", "对端 id 应可读");
}

#[tokio::test]
async fn site_disabled_returns_404() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let tok = login(&base).await;
    for path in ["/api/v1/site", "/api/v1/sites"] {
        let r = reqwest::Client::new()
            .get(format!("{base}{path}"))
            .bearer_auth(&tok)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404, "{path}");
    }
    let r = reqwest::Client::new()
        .get(format!("{base}/api/v1/site/identity"))
        .header("x-oss-site-token", "a".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// M7 H1：删桶的记账清理被门控 ABORT 时，console 必须恢复空桶并返回 409，
/// 不得“物理已删 + 行残留 + 无 delete changelog”。
#[tokio::test]
async fn bucket_delete_conflict_restores_physical_and_returns_409() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site("a", 'a'))).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db
        .insert_site(&oss_server::db::SitePeer {
            peer_id: "b".into(),
            remote_site_id: "remote-b".into(),
            name: "b".into(),
            endpoint: "http://127.0.0.1:9".into(),
            region: "us-east-1".into(),
            priority: 0,
            s3_access_key: "AK".into(),
            s3_secret_key: "SK".into(),
            peer_site_token: "b".repeat(32),
            created_at: now(),
            last_seen: None,
            last_error: None,
            raft_node_id: None,
        })
        .await
        .unwrap();
    st.store.create_bucket("conflict-bkt").await.unwrap();
    st.db.insert_bucket("conflict-bkt", None).await.unwrap();
    // 远端钟领先 → DELETE FROM buckets 会被 BEFORE 门控 ABORT。
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('bucket', 'conflict-bkt', ?, 0, 'remote-b', 9)",
    )
    .bind(now() + 120)
    .execute(st.db.pool())
    .await
    .unwrap();

    let base = serve(build_router(st.clone()).unwrap()).await;
    let tok = login(&base).await;
    let r = reqwest::Client::new()
        .delete(format!("{base}/api/v1/buckets/conflict-bkt"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    assert!(
        st.store.bucket_exists("conflict-bkt").await.unwrap(),
        "冲突时必须恢复物理桶"
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='conflict-bkt'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 1, "行/授权保持原样");
}
