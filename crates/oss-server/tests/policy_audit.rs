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

async fn audit_rows(base: &str, tok: &str, qs: &str) -> Vec<serde_json::Value> {
    let r = reqwest::Client::new()
        .get(format!("{base}/api/v1/audit{qs}"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "{qs}");
    r.json().await.unwrap()
}

/// M8 S6 缺口 A：审计过滤（时间窗/uid/op/bucket/key 前缀）与非法参数、非 admin 403。
#[tokio::test]
async fn audit_filters_by_time_identity_resource_and_op() {
    let dir = tempfile::tempdir().unwrap();
    let (st, base_tok) = admin_env(dir.path()).await;
    let (base, tok) = base_tok.split_once('|').unwrap();
    st.db.create_user("bob", "pw2", false).await.unwrap();
    for (ts, uid, op, bucket, key, decision) in [
        (100i64, 1i64, "GetObject", "b1", "dir/a.txt", "allow"),
        (200, 2, "PutObject", "b1", "dir/b.txt", "deny"),
        (300, 1, "GetObject", "b2", "dir2/c.txt", "allow"),
        (400, 2, "GetObject", "b1", "other/d.txt", "allow"),
    ] {
        sqlx::query(
            "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(ts)
        .bind(uid)
        .bind(op)
        .bind(bucket)
        .bind(key)
        .bind(decision)
        .execute(st.db.pool())
        .await
        .unwrap();
    }

    // 行形状不变
    let all = audit_rows(base, tok, "").await;
    assert_eq!(all.len(), 4);
    assert_eq!(all[0]["ts"], 400, "最新在前");
    let mut keys: Vec<&str> = all[0]
        .as_object()
        .unwrap()
        .keys()
        .map(|s| s.as_str())
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "bucket",
            "decision",
            "detail",
            "id",
            "object_key",
            "op",
            "ts",
            "uid"
        ]
    );

    assert_eq!(audit_rows(base, tok, "?op=PutObject").await.len(), 1);
    assert_eq!(audit_rows(base, tok, "?bucket=b2").await.len(), 1);
    assert_eq!(audit_rows(base, tok, "?uid=2").await.len(), 2);
    assert_eq!(audit_rows(base, tok, "?from=200&to=300").await.len(), 2);
    // 前缀按字面量：dir/ 不匹配 dir2/
    let dirhits = audit_rows(base, tok, "?key=dir/").await;
    assert_eq!(dirhits.len(), 2, "{dirhits:?}");
    // 组合过滤
    let combo = audit_rows(base, tok, "?bucket=b1&op=GetObject&key=dir/").await;
    assert_eq!(combo.len(), 1);
    assert_eq!(combo[0]["object_key"], "dir/a.txt");
    // 空串过滤视为未给
    assert_eq!(audit_rows(base, tok, "?op=&bucket=&key=").await.len(), 4);

    // 非法参数 → 400；非管理员 → 403
    let client = reqwest::Client::new();
    for qs in ["?from=300&to=100", "?uid=-1", "?from=-1"] {
        let r = client
            .get(format!("{base}/api/v1/audit{qs}"))
            .bearer_auth(tok)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{qs}");
    }
    let login: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "bob", "password": "pw2"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let bob = login["token"].as_str().unwrap();
    let r = client
        .get(format!("{base}/api/v1/audit"))
        .bearer_auth(bob)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
}
