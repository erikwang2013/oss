//! 子账户与权限体系（Step 2）集成用例：/me、用户管理与保护约束、权限矩阵、
//! 子账户自有桶、上传登记防挂名。全部走真实监听端口的控制台 API。

mod common;

use common::state;
use oss_server::router::build_router;
use reqwest::StatusCode;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn login(base: &str, user: &str, pw: &str) -> Option<String> {
    let resp = client()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": user, "password": pw}))
        .send()
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        return None;
    }
    let v: serde_json::Value = resp.json().await.unwrap();
    Some(v["token"].as_str().unwrap().to_string())
}

/// 起服务 + 管理员 erik/pw；TempDir 须在调用方作用域持有。
async fn boot(dir: &tempfile::TempDir) -> (String, oss_server::state::AppState) {
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    (base, st)
}

async fn create_sub(base: &str, admin_token: &str, name: &str) -> i64 {
    let r = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(admin_token)
        .json(&serde_json::json!({"username": name, "password": "pw12345678"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "建子账户应成功");
    r.json::<serde_json::Value>().await.unwrap()["id"]
        .as_i64()
        .unwrap()
}

#[tokio::test]
async fn me_reports_role_and_users_crud_guards() {
    let dir = tempfile::tempdir().unwrap();
    let (base, _st) = boot(&dir).await;
    let root = login(&base, "erik", "pw").await.unwrap();

    let me: serde_json::Value = client()
        .get(format!("{base}/api/v1/me"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["username"], "erik");
    assert_eq!(me["is_admin"], true);
    let root_id = me["id"].as_i64().unwrap();

    // 口令过短 → 400；空用户名 → 400
    for body in [
        serde_json::json!({"username": "sub", "password": "short"}),
        serde_json::json!({"username": "  ", "password": "pw12345678"}),
    ] {
        let r = client()
            .post(format!("{base}/api/v1/users"))
            .bearer_auth(&root)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    }

    let sub_id = create_sub(&base, &root, "sub").await;

    // 重名 → 400
    let r = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"username": "sub", "password": "pw12345678"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    // 子账户登录：/me is_admin=false；访问用户管理 → 403；不能给自己加权限
    let sub = login(&base, "sub", "pw12345678").await.unwrap();
    let me: serde_json::Value = client()
        .get(format!("{base}/api/v1/me"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["is_admin"], false);
    for (method, path) in [
        (reqwest::Method::GET, format!("{base}/api/v1/users")),
        (
            reqwest::Method::GET,
            format!("{base}/api/v1/users/{sub_id}/permissions"),
        ),
        (
            reqwest::Method::PUT,
            format!("{base}/api/v1/users/{sub_id}/permissions"),
        ),
    ] {
        let r = client()
            .request(method.clone(), path)
            .bearer_auth(&sub)
            .json(&serde_json::json!([]))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "{method} 子账户应 403");
    }

    // 管理员列表：2 人，sub 权限摘要为空
    let users: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[1]["username"], "sub");
    assert_eq!(users[1]["permissions"].as_array().unwrap().len(), 0);

    // 保护约束：不能删自己、不能删最后一个管理员
    for id in [root_id] {
        let r = client()
            .delete(format!("{base}/api/v1/users/{id}"))
            .bearer_auth(&root)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r.status(),
            StatusCode::BAD_REQUEST,
            "删自己/最后管理员必须被拒"
        );
    }

    // 重置口令：旧口令失效、新口令可登录
    let r = client()
        .post(format!("{base}/api/v1/users/{sub_id}/password"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"password": "newpw1234"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert!(login(&base, "sub", "pw12345678").await.is_none());
    assert!(login(&base, "sub", "newpw1234").await.is_some());

    // 删子账户 → 204；仅剩管理员
    let r = client()
        .delete(format!("{base}/api/v1/users/{sub_id}"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let users: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn permission_matrix_gates_console_actions() {
    let dir = tempfile::tempdir().unwrap();
    let (base, _st) = boot(&dir).await;
    let root = login(&base, "erik", "pw").await.unwrap();
    // 管理员建两个桶（owner=erik）
    for name in ["alpha", "beta"] {
        let r = client()
            .post(format!("{base}/api/v1/buckets"))
            .bearer_auth(&root)
            .json(&serde_json::json!({"name": name}))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT, "建桶 {name}");
    }
    let sub_id = create_sub(&base, &root, "sub").await;
    let sub = login(&base, "sub", "pw12345678").await.unwrap();

    // 无授权：桶列表空；对象列举/删除 403
    let buckets: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(buckets.is_empty(), "无授权子账户看不到任何桶");
    for (method, path) in [
        (
            reqwest::Method::GET,
            format!("{base}/api/v1/buckets/alpha/objects"),
        ),
        (
            reqwest::Method::DELETE,
            format!("{base}/api/v1/buckets/alpha/objects/k.txt"),
        ),
    ] {
        let r = client()
            .request(method.clone(), path)
            .bearer_auth(&sub)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "{method} 未授权应 403");
    }

    // 授 alpha: read+write
    let r = client()
        .put(format!("{base}/api/v1/users/{sub_id}/permissions"))
        .bearer_auth(&root)
        .json(&serde_json::json!([
            {"bucket": "alpha", "read": true, "write": true, "delete": false}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    let buckets: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1, "只见 alpha");
    assert_eq!(buckets[0]["name"], "alpha");
    assert_eq!(buckets[0]["can_read"], true);
    assert_eq!(buckets[0]["can_write"], true);
    assert_eq!(buckets[0]["can_delete"], false);
    assert_eq!(buckets[0]["is_owner"], false);

    // 读放行（空桶列举 200）；删对象仍 403（无 delete 位）
    let r = client()
        .get(format!("{base}/api/v1/buckets/alpha/objects"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = client()
        .delete(format!("{base}/api/v1/buckets/alpha/objects/k.txt"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN, "write 不含 delete");

    // 补 delete 位 → 删放行（幂等 204）
    let r = client()
        .put(format!("{base}/api/v1/users/{sub_id}/permissions"))
        .bearer_auth(&root)
        .json(&serde_json::json!([
            {"bucket": "alpha", "read": true, "write": true, "delete": true}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let r = client()
        .delete(format!("{base}/api/v1/buckets/alpha/objects/k.txt"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);

    // 删桶：非桶主非管理员 → 403
    let r = client()
        .delete(format!("{base}/api/v1/buckets/alpha"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN, "删桶限桶主/管理员");

    // 管理员列表里能看到权限摘要
    let users: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub_row = users.iter().find(|u| u["username"] == "sub").unwrap();
    assert_eq!(sub_row["permissions"][0]["bucket"], "alpha");
    assert_eq!(sub_row["permissions"][0]["delete"], true);
}

#[tokio::test]
async fn sub_account_owns_own_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let (base, _st) = boot(&dir).await;
    let root = login(&base, "erik", "pw").await.unwrap();
    create_sub(&base, &root, "sub").await;
    create_sub(&base, &root, "sub2").await;
    let sub = login(&base, "sub", "pw12345678").await.unwrap();
    let sub2 = login(&base, "sub2", "pw12345678").await.unwrap();

    // 子账户建自有桶 → 自动全权
    let r = client()
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&sub)
        .json(&serde_json::json!({"name": "gamma"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let buckets: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["is_owner"], true);
    assert_eq!(buckets[0]["can_delete"], true);

    // 其他子账户看不到；管理员看得到
    let b2: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&sub2)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(b2.is_empty(), "gamma 对无关子账户不可见");
    let ba: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(ba.iter().any(|b| b["name"] == "gamma"), "管理员全量可见");

    // 桶主可删自己的空桶 → 204
    let r = client()
        .delete(format!("{base}/api/v1/buckets/gamma"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT, "桶主删自有空桶");
}

#[tokio::test]
async fn register_upload_is_guarded_and_claims_once() {
    let dir = tempfile::tempdir().unwrap();
    let (base, st) = boot(&dir).await;
    let root = login(&base, "erik", "pw").await.unwrap();
    let sub_id = create_sub(&base, &root, "sub").await;
    let sub2_id = create_sub(&base, &root, "sub2").await;
    let sub = login(&base, "sub", "pw12345678").await.unwrap();
    let sub2 = login(&base, "sub2", "pw12345678").await.unwrap();

    // 模拟 ingest 落下的记录（bucket=files，尚无归属）
    st.db
        .record_upload("g_202610_p.txt", "files", "p.txt", "file", 1, "e")
        .await
        .unwrap();

    // 无 files 写权限 → 403（修「任意挂名」洞）
    let reg = |token: String| {
        let base = base.clone();
        async move {
            client()
                .post(format!("{base}/api/v1/uploads/register"))
                .bearer_auth(token)
                .json(&serde_json::json!({"saved_path": "g_202610_p.txt", "original_name": "n"}))
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(reg(sub.clone()).await, StatusCode::FORBIDDEN);

    // 授 sub 对 files 的写权限 → 认领成功（204）
    client()
        .put(format!("{base}/api/v1/users/{sub_id}/permissions"))
        .bearer_auth(&root)
        .json(
            &serde_json::json!([{"bucket": "files", "read": true, "write": true, "delete": false}]),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(reg(sub.clone()).await, StatusCode::NO_CONTENT);

    // 已归属他人：sub2（即使有写权限）认领 → 404（0 行命中）
    client()
        .put(format!("{base}/api/v1/users/{sub2_id}/permissions"))
        .bearer_auth(&root)
        .json(
            &serde_json::json!([{"bucket": "files", "read": true, "write": true, "delete": false}]),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(reg(sub2.clone()).await, StatusCode::NOT_FOUND);

    // 上传记录视野：sub 见自己的 1 条；sub2 见 0 条；管理员全量
    let count = |token: String| {
        let base = base.clone();
        async move {
            client()
                .get(format!("{base}/api/v1/uploads"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .json::<Vec<serde_json::Value>>()
                .await
                .unwrap()
                .len()
        }
    };
    assert_eq!(count(sub).await, 1);
    assert_eq!(count(sub2).await, 0);
    assert_eq!(count(root.clone()).await, 1);

    // 管理员可改归属（覆盖）
    let r = client()
        .post(format!("{base}/api/v1/uploads/register"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"saved_path": "g_202610_p.txt", "original_name": "admin-name"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn self_password_change_and_disable_flow() {
    let dir = tempfile::tempdir().unwrap();
    let (base, _st) = boot(&dir).await;
    let root = login(&base, "erik", "pw").await.unwrap();
    let sub_id = create_sub(&base, &root, "sub").await;
    let sub = login(&base, "sub", "pw12345678").await.unwrap();

    // --- 自助改密 ---
    // 旧口令错 → 400；新口令过短 → 400
    let r = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&sub)
        .json(&serde_json::json!({"old_password": "wrong", "new_password": "newpw-1234"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let r = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&sub)
        .json(&serde_json::json!({"old_password": "pw12345678", "new_password": "short"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    // 正确改密 → 204；旧口令失效、新口令可登录
    let r = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&sub)
        .json(&serde_json::json!({"old_password": "pw12345678", "new_password": "newpw-1234"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert!(login(&base, "sub", "pw12345678").await.is_none());
    let sub = login(&base, "sub", "newpw-1234").await.unwrap();

    // --- 启停用 ---
    // 管理员凭停用前的旧 token 拉列表：body 里带 status
    let users: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub_row = users.iter().find(|u| u["username"] == "sub").unwrap();
    assert_eq!(sub_row["status"], "active");

    // 停用：不能停用自己；停用 sub 后——登录 401、旧 JWT 的 /me 401、/buckets 401
    let root_id: i64 = {
        let me: serde_json::Value = client()
            .get(format!("{base}/api/v1/me"))
            .bearer_auth(&root)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        me["id"].as_i64().unwrap()
    };
    let r = client()
        .put(format!("{base}/api/v1/users/{root_id}/status"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"active": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST, "不能停用自己");

    let r = client()
        .put(format!("{base}/api/v1/users/{sub_id}/status"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"active": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert!(
        login(&base, "sub", "newpw-1234").await.is_none(),
        "停用后登录 401"
    );
    let r = client()
        .get(format!("{base}/api/v1/me"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "停用后旧 JWT 立即失效"
    );
    // 评审 P1 回归：曾绕过 who() 的遮面端点也必须拦下（建桶/密钥、分组只读）
    for (method, path, body) in [
        (
            reqwest::Method::POST,
            "/api/v1/buckets",
            Some(serde_json::json!({"name": "should-not-exist"})),
        ),
        (reqwest::Method::GET, "/api/v1/access-keys", None),
        (
            reqwest::Method::POST,
            "/api/v1/access-keys",
            Some(serde_json::json!({})),
        ),
        (reqwest::Method::GET, "/api/v1/groups", None),
    ] {
        let mut req = client()
            .request(method.clone(), format!("{base}{path}"))
            .bearer_auth(&sub);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let r = req.send().await.unwrap();
        assert_eq!(
            r.status(),
            StatusCode::UNAUTHORIZED,
            "停用后 {method} {path} 必须 401"
        );
    }
    // 停用期间没铸出任何桶
    let buckets_admin: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !buckets_admin
            .iter()
            .any(|b| b["name"] == "should-not-exist"),
        "停用账号不得建出桶"
    );

    // 列表里状态可见；重新启用恢复
    let users: Vec<serde_json::Value> = client()
        .get(format!("{base}/api/v1/users"))
        .bearer_auth(&root)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub_row = users.iter().find(|u| u["username"] == "sub").unwrap();
    assert_eq!(sub_row["status"], "disabled");
    let r = client()
        .put(format!("{base}/api/v1/users/{sub_id}/status"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"active": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    assert!(login(&base, "sub", "newpw-1234").await.is_some());

    // 最后一个在册管理员不可停用（把 erik 降为唯一管理员的情形不在此构造——
    // 这里是「只读管理员自己」的守卫：停用自己已被上一条 400 拦住）
    let r = client()
        .put(format!("{base}/api/v1/users/999/status"))
        .bearer_auth(&root)
        .json(&serde_json::json!({"active": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND, "不存在的用户 404");
}
