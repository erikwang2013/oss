//! M4 切片 7：桶策略控制面 CRUD/校验 + 审计读写。
//! S3 面的策略执行与审计落行（真 AK/真签名）由 e2e_s3.sh 第 17 节实弹承担。

mod common;

use common::state;
use oss_server::router::build_router;

async fn admin_env(dir: &std::path::Path) -> (oss_server::state::AppState, String) {
    let st = state(dir).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = {
        let app = build_router(st.clone()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    };
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tok = login["token"].as_str().unwrap().to_string();
    (st, format!("{base}|{tok}"))
}

#[tokio::test]
async fn policy_crud_validation_and_audit_query() {
    let dir = tempfile::tempdir().unwrap();
    let (st, base_tok) = admin_env(dir.path()).await;
    let (base, tok) = base_tok.split_once('|').unwrap();
    let client = reqwest::Client::new();
    st.store.create_bucket("pbk").await.unwrap();
    st.db.insert_bucket("pbk", None).await.unwrap();

    // 未配 → null
    let v: serde_json::Value = client
        .get(format!("{base}/api/v1/buckets/pbk/policy"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(v.is_null());

    // 畸形策略 → 400（不落库）
    let resp = client
        .put(format!("{base}/api/v1/buckets/pbk/policy"))
        .bearer_auth(tok)
        .body(r#"{"Statement":[{"Action":"s3:*"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let resp = client
        .put(format!("{base}/api/v1/buckets/pbk/policy"))
        .bearer_auth(tok)
        .body("[]")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "裸数组必须 400（顶层须对象）");
    assert!(st.db.bucket_policy("pbk").await.unwrap().is_none());

    // 合法策略 → 204 且回读等价（Deny 优先样例）
    let policy = r#"{"Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::pbk/secret*"}]}"#;
    let resp = client
        .put(format!("{base}/api/v1/buckets/pbk/policy"))
        .bearer_auth(tok)
        .body(policy)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let got: serde_json::Value = client
        .get(format!("{base}/api/v1/buckets/pbk/policy"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["Statement"][0]["Effect"], "Deny");

    // 审计：经 Acl 落两行（allow/deny）→ 控制面按 decision 过滤可查
    let acl = st.acl();
    acl.write_audit(1, "GetObject", Some("pbk"), Some("a.txt"), "allow", None)
        .await
        .unwrap();
    acl.write_audit(
        1,
        "GetObject",
        Some("pbk"),
        Some("secret/x"),
        "deny",
        Some("policy:deny"),
    )
    .await
    .unwrap();
    let rows: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/audit?decision=deny"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["decision"], "deny");
    assert_eq!(rows[0]["object_key"], "secret/x");
    assert_eq!(rows[0]["detail"], "policy:deny");
    let all: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/audit"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    // 删桶清策略（S6-1 同款语义）
    let resp = client
        .delete(format!("{base}/api/v1/buckets/pbk"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert!(st.db.bucket_policy("pbk").await.unwrap().is_none());
}
