//! M8 S1：访问点（Access Point）进程内签名 HTTP 验收。
//!
//! 覆盖：控制面 CRUD/权限、AP 策略三轴交集（身份/桶/AP × Deny/Allow/无匹配）、
//! Deny 优先、Copy 目标 `s3:PutObject`、桶级 AP ARN、跨 AP 隔离、全部支持的
//! 对象操作经 AP、canonical key 只解码一次、审计合并、事件/复制各一次、
//! fail-closed（未签名/坏签名/缺 AP/坏策略）、桶删除级联、N1 内层直调、N2 STS 回归。
//!
//! SigV4 用 s3s-sigv4 的规范原语在测试内最小实现（不引生产签名器）。

mod common;

use std::sync::Arc;

use common::state;
use oss_server::db::PermissionRow;
use oss_server::router::build_router;
use oss_server::state::AppState;
use reqwest::Method;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::{Digest, Sha256};
use xl_s3::ap::AccessPoint;
use xl_s3::replication::ReplicationStore as _;
use xl_store::meta::TierPhase;
use xl_store::store::ArchiveSpec;

const REGION: &str = "us-east-1";

struct Env {
    st: AppState,
    base: String,
    http: reqwest::Client,
}

async fn env(dir: &std::path::Path) -> Env {
    env_with_peer(dir, None).await
}

/// `peer` = 模拟连接对端（SourceIp 条件）；注入在最外层，等价于真实 listener 的
/// connect_info（ecat 内置 listener 不提供，生产由 router 的信任反代路径补）。
async fn env_with_peer(dir: &std::path::Path, peer: Option<std::net::IpAddr>) -> Env {
    let st = state(dir).await;
    let app = build_router(st.clone()).unwrap();
    let app = match peer {
        Some(ip) => app.layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
                req.extensions_mut().insert(xl_s3::ap::ApPeer(ip));
                next.run(req).await
            },
        )),
        None => app,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Env {
        st,
        base: format!("http://{addr}"),
        http: reqwest::Client::new(),
    }
}

/// >1 MiB 体经 AP 的回归用：测试默认 max_put_bytes=1 MiB，这里单独放大到 4 MiB。
async fn env_with_max_put(dir: &std::path::Path, max_put_bytes: usize) -> Env {
    let st = common::state_with_max_put(dir, max_put_bytes).await;
    let app = build_router(st.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Env {
        st,
        base: format!("http://{addr}"),
        http: reqwest::Client::new(),
    }
}

// ---------- 播种 ----------

async fn user(e: &Env, name: &str, admin: bool) -> (i64, String, String) {
    let uid = e.st.db.create_user(name, "pw", admin).await.unwrap();
    let ak = format!("AKIA{}", name.to_uppercase());
    let sk = format!("secret-{name}");
    e.st.db.create_access_key(&ak, &sk, uid).await.unwrap();
    (uid, ak, sk)
}

async fn bucket(e: &Env, name: &str, owner: Option<i64>) {
    e.st.store.create_bucket(name).await.unwrap();
    e.st.db.insert_bucket(name, owner).await.unwrap();
}

async fn perms(e: &Env, uid: i64, rows: &[PermissionRow]) {
    e.st.db.set_permissions(uid, rows).await.unwrap();
}

fn full(bucket: &str) -> PermissionRow {
    PermissionRow {
        bucket: bucket.into(),
        read: true,
        write: true,
        delete: true,
    }
}

fn read_only(bucket: &str) -> PermissionRow {
    PermissionRow {
        bucket: bucket.into(),
        read: true,
        write: false,
        delete: false,
    }
}

async fn direct_ap(e: &Env, name: &str, bucket: &str, owner: i64, policy: Option<&str>) {
    e.st.db
        .create_access_point(name, bucket, owner, policy, 0)
        .await
        .unwrap();
    e.st.ap_cache.put(
        AccessPoint::from_row(
            name.into(),
            bucket.into(),
            owner,
            policy.map(str::to_string),
            &e.st.ap_region,
        )
        .unwrap(),
    );
}

async fn set_ap_policy(e: &Env, name: &str, bucket: &str, owner: i64, policy: Option<&str>) {
    e.st.db
        .set_access_point_policy(name, policy, 0)
        .await
        .unwrap();
    e.st.ap_cache.put(
        AccessPoint::from_row(
            name.into(),
            bucket.into(),
            owner,
            policy.map(str::to_string),
            &e.st.ap_region,
        )
        .unwrap(),
    );
}

async fn set_bucket_policy(e: &Env, bucket: &str, policy: Option<&str>) {
    match policy {
        Some(p) => e.st.db.set_bucket_policy(bucket, p, 0).await.unwrap(),
        None => {
            sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
                .bind(bucket)
                .execute(e.st.db.pool())
                .await
                .unwrap();
        }
    }
}

async fn login(base: &str, username: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": username, "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

// ---------- 最小 SigV4 签名 ----------

/// 百分号解码（查询用 form 语义：`+` → 空格）。非法/截断转义按字面量保留。
fn pct_decode(s: &str, plus_as_space: bool) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hi = (b[i + 1] as char).to_digit(16);
                let lo = (b[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

/// 查询组件百分号编码（unreserved 原样，其余 `%XX`），防止 `+`/`&`/`=` 破坏查询。
fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[test]
fn pct_helpers_handle_malformed_escapes_and_reserved_chars() {
    assert_eq!(pct_decode("a%2Fb", false), "a/b");
    assert_eq!(pct_decode("%252F", false), "%2F");
    assert_eq!(pct_decode("a+b", true), "a b");
    assert_eq!(pct_decode("a+b", false), "a+b");
    // 非法/截断转义：`%` 与后续字符都按字面量保留，不吞字符
    assert_eq!(pct_decode("%zz", false), "%zz");
    assert_eq!(pct_decode("%2", false), "%2");
    assert_eq!(pct_decode("100%", false), "100%");
    assert_eq!(pct_encode("a+b=c&d/e f"), "a%2Bb%3Dc%26d%2Fe%20f");
    assert_eq!(pct_decode(&pct_encode("a+b=c&d/e f"), true), "a+b=c&d/e f");
}

fn query_pairs(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k, true), pct_decode(v, true))
        })
        .collect()
}

fn amz_now() -> AmzDate {
    let t = time::OffsetDateTime::now_utc();
    let s = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    AmzDate::parse(&s).unwrap()
}

/// 对 `path_q` 发一个 SigV4 签名请求（header 签名；payload 单块哈希）。
#[allow(clippy::too_many_arguments)]
async fn signed(
    e: &Env,
    method: Method,
    path_q: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    ak: &str,
    sk: &str,
    service: &str,
) -> reqwest::Response {
    let url = format!("{}{}", e.base, path_q);
    let mut rb = e.http.request(method.clone(), &url);
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    let mut req = rb.body(body.to_vec()).build().unwrap();

    let payload_hex = hex::encode(Sha256::digest(body));
    let amz = amz_now();
    // 显式 Host 头（别名 Host 形式）必须参与签名；否则退回 URL authority。
    let host = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("host"))
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| {
            url.split("://")
                .nth(1)
                .unwrap()
                .split('/')
                .next()
                .unwrap()
                .to_string()
        });

    let mut to_sign: Vec<(String, String)> = vec![
        ("host".into(), host),
        ("x-amz-content-sha256".into(), payload_hex.clone()),
        ("x-amz-date".into(), amz.fmt_iso8601().to_string()),
    ];
    for (k, v) in headers {
        let name = k.to_ascii_lowercase();
        if !to_sign.iter().any(|(n, _)| *n == name) {
            to_sign.push((name, v.to_string()));
        }
    }
    to_sign.sort_by(|a, b| a.0.cmp(&b.0));
    let refs: Vec<(&str, &str)> = to_sign
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let decoded_path = pct_decode(req.url().path(), false);
    let pairs = query_pairs(req.url().query().unwrap_or(""));
    let canonical = create_canonical_request(
        method.as_str(),
        &decoded_path,
        &pairs,
        &refs,
        Payload::SingleChunk(&payload_hex),
    );
    let sts = create_string_to_sign(&canonical, &amz, REGION, service);
    let sig = calculate_signature(&sts, sk, &amz, REGION, service);
    let names: Vec<&str> = to_sign.iter().map(|(k, _)| k.as_str()).collect();
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={ak}/{}/{REGION}/{service}/aws4_request, SignedHeaders={}, Signature={sig}",
        amz.fmt_date(),
        names.join(";")
    );

    if let Err(err) = s3s_sigv4::AuthorizationV4::parse(&auth) {
        panic!("authorization parse failed: {err}\n{auth}");
    }
    req.headers_mut()
        .insert("x-amz-date", amz.fmt_iso8601().parse().unwrap());
    req.headers_mut()
        .insert("x-amz-content-sha256", payload_hex.parse().unwrap());
    req.headers_mut()
        .insert(reqwest::header::AUTHORIZATION, auth.parse().unwrap());
    e.http.execute(req).await.expect("HTTP 请求发送失败")
}

#[allow(clippy::too_many_arguments)]
async fn send_status(
    e: &Env,
    method: Method,
    path_q: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    ak: &str,
    sk: &str,
    service: &str,
) -> u16 {
    signed(e, method, path_q, headers, body, ak, sk, service)
        .await
        .status()
        .as_u16()
}

async fn count(e: &Env, sql: &'static str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(e.st.db.pool())
        .await
        .unwrap()
}

fn bucket_policy(effect: &str) -> String {
    serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":effect,"Principal":{"AWS":"*"},"Action":"s3:GetObject",
        "Resource":"arn:aws:s3:::mxb/*"}]})
    .to_string()
}

fn ap_policy(effect: &str, action: &str, resource: &str) -> String {
    serde_json::json!({"Statement":[{
        "Effect":effect,"Principal":{"AWS":"*"},"Action":action,"Resource":resource}]})
    .to_string()
}

// ---------- 验收 ----------

/// 控制面 CRUD/权限 + 缓存同步（创建/策略更新/删除后签名流量立即生效）。
#[tokio::test]
async fn console_crud_permissions_and_cache() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (_, _, _) = user(&e, "admin", true).await;
    let (owner_id, owner_ak, owner_sk) = user(&e, "owner", false).await;
    let (_, _, _) = user(&e, "outsider", false).await;
    bucket(&e, "crudb", Some(owner_id)).await;
    let admin_tok = login(&e.base, "admin").await;
    let owner_tok = login(&e.base, "owner").await;
    let out_tok = login(&e.base, "outsider").await;

    let create = |tok: &str, body: serde_json::Value| {
        e.http
            .post(format!("{}/api/v1/access-points", e.base))
            .bearer_auth(tok.to_string())
            .json(&body)
            .send()
    };

    // 非桶主非 admin 不能建 AP
    assert_eq!(
        create(
            &out_tok,
            serde_json::json!({"name":"out-ap","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        403
    );
    // 桶主可建
    assert_eq!(
        create(
            &owner_tok,
            serde_json::json!({"name":"owner-ap","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        201
    );
    assert!(e.st.ap_cache.get("owner-ap").is_some(), "创建后缓存未同步");
    let (uid, decision, detail): (i64, String, Option<String>) = sqlx::query_as(
        "SELECT uid, decision, detail FROM audit_log WHERE op = 'control:ap-create' AND object_key = 'owner-ap'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!((uid, decision.as_str()), (owner_id, "allow"));
    assert_eq!(detail.as_deref(), Some("policy=none"));
    // 重名 409 / 非法名 400 / 桶不存在 404 / 坏策略 400
    assert_eq!(
        create(
            &owner_tok,
            serde_json::json!({"name":"owner-ap","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        409
    );
    assert_eq!(
        create(
            &owner_tok,
            serde_json::json!({"name":"Bad Name","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        400
    );
    assert_eq!(
        create(
            &admin_tok,
            serde_json::json!({"name":"mis-ap","bucket":"nope"})
        )
        .await
        .unwrap()
        .status(),
        404
    );
    assert_eq!(
        create(
            &owner_tok,
            serde_json::json!({"name":"bad-ap","bucket":"crudb","policy":{"Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:x","Condition":{}}]}})
        )
        .await
        .unwrap()
        .status(),
        400
    );
    // admin 可建
    assert_eq!(
        create(
            &admin_tok,
            serde_json::json!({"name":"adm-ap","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        201
    );

    let list = |tok: &str| {
        e.http
            .get(format!("{}/api/v1/access-points", e.base))
            .bearer_auth(tok.to_string())
            .send()
    };
    let rows: serde_json::Value = list(&owner_tok).await.unwrap().json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1, "owner 只应看到自己的 AP");
    let rows: serde_json::Value = list(&out_tok).await.unwrap().json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 0);
    let rows: serde_json::Value = list(&admin_tok).await.unwrap().json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2, "admin 应看到全部 AP");

    // 详情：非桶主 403，桶主 200
    assert_eq!(
        e.http
            .get(format!("{}/api/v1/access-points/owner-ap", e.base))
            .bearer_auth(&out_tok)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        e.http
            .get(format!("{}/api/v1/access-points/owner-ap", e.base))
            .bearer_auth(&owner_tok)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // 无策略时经 AP 可写
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/owner-ap/pre",
            &[],
            b"pre",
            &owner_ak,
            &owner_sk,
            "s3"
        )
        .await,
        200
    );

    // 策略更新（仅 GetObject）后缓存立即生效：PUT 拒、GET 放行
    let policy = serde_json::json!({"Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
        "Resource":"arn:aws:s3:::crudb/*"}]});
    assert_eq!(
        e.http
            .put(format!("{}/api/v1/access-points/owner-ap/policy", e.base))
            .bearer_auth(&out_tok)
            .json(&policy)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        e.http
            .put(format!("{}/api/v1/access-points/owner-ap/policy", e.base))
            .bearer_auth(&owner_tok)
            .json(&policy)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    let detail: Option<String> = sqlx::query_scalar(
        "SELECT detail FROM audit_log WHERE op = 'control:ap-policy' AND object_key = 'owner-ap'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(detail.as_deref(), Some("policy=set"));
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/owner-ap/post",
            &[],
            b"post",
            &owner_ak,
            &owner_sk,
            "s3"
        )
        .await,
        403,
        "AP 策略未匹配 PutObject 应拒"
    );
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/owner-ap/pre",
            &[],
            b"",
            &owner_ak,
            &owner_sk,
            "s3"
        )
        .await,
        200
    );

    // 删除：非桶主 403；桶主 204；缓存移除后经 AP 404
    assert_eq!(
        e.http
            .delete(format!("{}/api/v1/access-points/owner-ap", e.base))
            .bearer_auth(&out_tok)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        e.http
            .delete(format!("{}/api/v1/access-points/owner-ap", e.base))
            .bearer_auth(&owner_tok)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(e.st.ap_cache.get("owner-ap").is_none(), "删除后缓存未移除");
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/owner-ap/pre",
            &[],
            b"",
            &owner_ak,
            &owner_sk,
            "s3"
        )
        .await,
        404
    );
    let (decision, bucket): (String, Option<String>) = sqlx::query_as(
        "SELECT decision, bucket FROM audit_log WHERE op = 'control:ap-delete' AND object_key = 'owner-ap'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (decision.as_str(), bucket.as_deref()),
        ("allow", Some("crudb"))
    );

    // I3：审计落行失败只 warn，不阻断动作
    sqlx::query("DROP TABLE audit_log")
        .execute(e.st.db.pool())
        .await
        .unwrap();
    assert_eq!(
        create(
            &owner_tok,
            serde_json::json!({"name":"noaudit-ap","bucket":"crudb"})
        )
        .await
        .unwrap()
        .status(),
        201
    );
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Axis {
    None,
    Allow,
    Deny,
}

/// AP 策略三轴（身份/桶/AP）× Deny/Allow/无匹配，含 Deny 优先。
#[tokio::test]
async fn ap_policy_matrix_deny_wins() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (admin_id, admin_ak, admin_sk) = user(&e, "admin", true).await;
    let (alice_id, alice_ak, alice_sk) = user(&e, "alice", false).await;
    let (_bob_id, bob_ak, bob_sk) = user(&e, "bob", false).await;
    bucket(&e, "mxb", Some(admin_id)).await;
    perms(&e, alice_id, &[read_only("mxb")]).await;
    direct_ap(&e, "mx-ap", "mxb", admin_id, None).await;
    // 标准路径播种对象（admin）
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/mxb/obj",
            &[],
            b"hello",
            &admin_ak,
            &admin_sk,
            "s3"
        )
        .await,
        200
    );

    let b_allow = bucket_policy("Allow");
    let b_deny = bucket_policy("Deny");
    let obj_arn = "arn:aws:s3:us-east-1:000000000000:accesspoint/mx-ap/object/*";
    let a_allow = ap_policy("Allow", "s3:GetObject", obj_arn);
    let a_deny = ap_policy("Deny", "s3:GetObject", obj_arn);

    let identities = [
        ("alice", alice_ak.as_str(), alice_sk.as_str(), true),
        ("bob", bob_ak.as_str(), bob_sk.as_str(), false),
        ("admin", admin_ak.as_str(), admin_sk.as_str(), true),
    ];
    for (who, ak, sk, identity_allow) in identities {
        for bx in [Axis::None, Axis::Allow, Axis::Deny] {
            let bp = match bx {
                Axis::None => None,
                Axis::Allow => Some(b_allow.as_str()),
                Axis::Deny => Some(b_deny.as_str()),
            };
            set_bucket_policy(&e, "mxb", bp).await;
            for ax in [Axis::None, Axis::Allow, Axis::Deny] {
                let ap = match ax {
                    Axis::None => None,
                    Axis::Allow => Some(a_allow.as_str()),
                    Axis::Deny => Some(a_deny.as_str()),
                };
                set_ap_policy(&e, "mx-ap", "mxb", admin_id, ap).await;
                let got =
                    send_status(&e, Method::GET, "/ap/mx-ap/obj", &[], b"", ak, sk, "s3").await;
                // 桶 Allow 提升身份；桶 Deny 压过一切；AP 无策略不额外限制、无匹配/Deny 拒。
                let base = match bx {
                    Axis::Deny => false,
                    Axis::Allow => true,
                    Axis::None => identity_allow,
                };
                let want = if ax == Axis::Deny {
                    403
                } else if base {
                    200
                } else {
                    403
                };
                assert_eq!(got, want, "identity={who} bucket={bx:?} ap={ax:?}");
            }
        }
    }

    // Deny 优先：同策略内 Allow+Deny 同时命中 → 拒（桶策略 / AP 策略各自）
    set_ap_policy(&e, "mx-ap", "mxb", admin_id, Some(&a_allow)).await;
    let both_bucket = serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::mxb/*"},
        {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::mxb/obj"}
    ]})
    .to_string();
    set_bucket_policy(&e, "mxb", Some(&both_bucket)).await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/mx-ap/obj",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        403,
        "桶策略内 Deny 应压过 Allow"
    );
    set_bucket_policy(&e, "mxb", None).await;
    let both_ap = serde_json::json!({"Statement":[
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":obj_arn},
        {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":obj_arn}
    ]})
    .to_string();
    set_ap_policy(&e, "mx-ap", "mxb", admin_id, Some(&both_ap)).await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/mx-ap/obj",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        403,
        "AP 策略内 Deny 应压过 Allow"
    );

    // AP 策略存在但无匹配（仅 PutObject）→ 拒
    let put_only = ap_policy("Allow", "s3:PutObject", obj_arn);
    set_ap_policy(&e, "mx-ap", "mxb", admin_id, Some(&put_only)).await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/mx-ap/obj",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        403,
        "AP 策略无匹配应拒"
    );

    // 桶策略无匹配 → 回落身份表（alice 有读 200，bob 无读 403）
    set_ap_policy(&e, "mx-ap", "mxb", admin_id, None).await;
    let bucket_put_only = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject",
        "Resource":"arn:aws:s3:::mxb/*"}]})
    .to_string();
    set_bucket_policy(&e, "mxb", Some(&bucket_put_only)).await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/mx-ap/obj",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/mx-ap/obj",
            &[],
            b"",
            &bob_ak,
            &bob_sk,
            "s3"
        )
        .await,
        403
    );
}

/// Copy 目标走 `s3:PutObject`、桶级 AP ARN、跨 AP 隔离。
#[tokio::test]
async fn policy_actions_arns_and_cross_ap_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (admin_id, ak, sk) = user(&e, "admin", true).await;
    bucket(&e, "arnb", Some(admin_id)).await;
    direct_ap(&e, "arn-ap", "arnb", admin_id, None).await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/arnb/src",
            &[],
            b"payload",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );

    let obj_arn = "arn:aws:s3:us-east-1:000000000000:accesspoint/arn-ap/object/*";
    // 仅 PutObject：Copy 放行（目标轴是 s3:PutObject），读对象被拒
    set_ap_policy(
        &e,
        "arn-ap",
        "arnb",
        admin_id,
        Some(&ap_policy("Allow", "s3:PutObject", obj_arn)),
    )
    .await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/arn-ap/dst",
            &[("x-amz-copy-source", "/arnb/src")],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200,
        "Copy 目标轴应映射为 s3:PutObject"
    );
    assert_eq!(
        send_status(&e, Method::GET, "/ap/arn-ap/dst", &[], b"", &ak, &sk, "s3").await,
        403,
        "仅 PutObject 的策略不应放行 GetObject"
    );
    // 仅 GetObject：Copy 拒
    set_ap_policy(
        &e,
        "arn-ap",
        "arnb",
        admin_id,
        Some(&ap_policy("Allow", "s3:GetObject", obj_arn)),
    )
    .await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/arn-ap/denied",
            &[("x-amz-copy-source", "/arnb/src")],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        403,
        "Copy 目标缺少 s3:PutObject 应拒"
    );

    // 桶级 AP ARN：ListBucket 放行 ListV2/HeadBucket，对象 GET 无匹配被拒
    let bucket_arn = "arn:aws:s3:us-east-1:000000000000:accesspoint/arn-ap";
    set_ap_policy(
        &e,
        "arn-ap",
        "arnb",
        admin_id,
        Some(&ap_policy("Allow", "s3:ListBucket", bucket_arn)),
    )
    .await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/arn-ap?list-type=2",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        send_status(&e, Method::HEAD, "/ap/arn-ap", &[], b"", &ak, &sk, "s3").await,
        200
    );
    assert_eq!(
        send_status(&e, Method::GET, "/ap/arn-ap/src", &[], b"", &ak, &sk, "s3").await,
        403,
        "桶级 ARN 不应匹配对象请求"
    );

    // 跨 AP 隔离：other-ap 的策略只认 arn-ap 的 ARN → 经 other-ap 恒拒
    direct_ap(&e, "other-ap", "arnb", admin_id, None).await;
    set_ap_policy(
        &e,
        "other-ap",
        "arnb",
        admin_id,
        Some(&ap_policy("Allow", "s3:GetObject", obj_arn)),
    )
    .await;
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/other-ap/src",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        403,
        "AP 策略按自身名称的 ARN 求值，不得跨 AP 生效"
    );
    set_ap_policy(
        &e,
        "arn-ap",
        "arnb",
        admin_id,
        Some(&ap_policy("Allow", "s3:GetObject", obj_arn)),
    )
    .await;
    assert_eq!(
        send_status(&e, Method::GET, "/ap/arn-ap/src", &[], b"", &ak, &sk, "s3").await,
        200
    );
}

/// 全部支持的对象操作经 AP：PUT/GET/HEAD/ListV2/MPU/Copy/DELETE，以及 501 形状。
#[tokio::test]
async fn object_ops_through_ap() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (owner_id, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "opsb", Some(owner_id)).await;
    direct_ap(&e, "ops-ap", "opsb", owner_id, None).await;

    let r = signed(
        &e,
        Method::PUT,
        "/ap/ops-ap/dir/hello.txt",
        &[],
        b"hello",
        &ak,
        &sk,
        "s3",
    )
    .await;
    let put = r.status().as_u16();
    let put_body = r.text().await.unwrap();
    assert_eq!(put, 200, "{put_body}");

    let r = signed(
        &e,
        Method::GET,
        "/ap/ops-ap/dir/hello.txt",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "hello");

    let r = signed(
        &e,
        Method::HEAD,
        "/ap/ops-ap/dir/hello.txt",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.headers().get(reqwest::header::CONTENT_LENGTH).unwrap(),
        "5"
    );

    let r = signed(
        &e,
        Method::GET,
        "/ap/ops-ap?list-type=2&prefix=dir%2F",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("dir/hello.txt"));

    // Copy（源同桶）
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/ops-ap/dir/copy.txt",
            &[("x-amz-copy-source", "/opsb/dir/hello.txt")],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/ap/ops-ap/dir/copy.txt",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.text().await.unwrap(), "hello");

    // MPU：Create → UploadPart → Complete → Get
    let r = signed(
        &e,
        Method::POST,
        "/ap/ops-ap/dir/mpu?uploads",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    let xml = r.text().await.unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .expect("CreateMultipartUpload 缺 UploadId")
        .to_string();
    // ListMultipartUploads 经 AP：GET bucket ?uploads 可见进行中的 upload
    let r = signed(
        &e,
        Method::GET,
        "/ap/ops-ap?uploads",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains(&upload_id));
    let part_q = format!(
        "/ap/ops-ap/dir/mpu?partNumber=1&uploadId={}",
        pct_encode(&upload_id)
    );
    let r = signed(&e, Method::PUT, &part_q, &[], b"part-one", &ak, &sk, "s3").await;
    assert_eq!(r.status(), 200);
    let etag = r
        .headers()
        .get(reqwest::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete_q = format!("/ap/ops-ap/dir/mpu?uploadId={}", pct_encode(&upload_id));
    assert_eq!(
        send_status(
            &e,
            Method::POST,
            &complete_q,
            &[],
            complete.as_bytes(),
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/ap/ops-ap/dir/mpu",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.text().await.unwrap(), "part-one");

    // Abort 路径可用
    let r = signed(
        &e,
        Method::POST,
        "/ap/ops-ap/abortme?uploads",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    let xml = r.text().await.unwrap();
    let abort_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let abort_q = format!("/ap/ops-ap/abortme?uploadId={}", pct_encode(&abort_id));
    assert_eq!(
        send_status(&e, Method::DELETE, &abort_q, &[], b"", &ak, &sk, "s3").await,
        204
    );

    // DELETE 与删除后 GET
    assert_eq!(
        send_status(
            &e,
            Method::DELETE,
            "/ap/ops-ap/dir/hello.txt",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        204
    );
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/ops-ap/dir/hello.txt",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        404
    );

    // UploadPartCopy 经 AP：MPU 目标 + x-amz-copy-source → 200 CopyPartResult
    let r = signed(
        &e,
        Method::POST,
        "/ap/ops-ap/dir/copy-part?uploads",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    let xml = r.text().await.unwrap();
    let copy_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .expect("CreateMultipartUpload 缺 UploadId")
        .to_string();
    let copy_q = format!(
        "/ap/ops-ap/dir/copy-part?partNumber=1&uploadId={}",
        pct_encode(&copy_id)
    );
    let r = signed(
        &e,
        Method::PUT,
        &copy_q,
        &[("x-amz-copy-source", "/opsb/dir/copy.txt")],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("CopyPartResult"));

    // DeleteObjects 经 AP：逐键部分成功（幂等空删也进 Deleted）
    let delete_xml = b"<Delete><Object><Key>dir/copy.txt</Key></Object>\
                       <Object><Key>dir/nope</Key></Object></Delete>";
    let r = signed(
        &e,
        Method::POST,
        "/ap/ops-ap?delete",
        &[],
        delete_xml,
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    let xml = r.text().await.unwrap();
    assert!(xml.contains("<Deleted><Key>dir/copy.txt</Key>"), "{xml}");
    assert!(xml.contains("<Deleted><Key>dir/nope</Key>"), "{xml}");
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/ops-ap/dir/copy.txt",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        404
    );
}

/// s3s 默认 `custom_route_max_body_size = 1 MiB` 会误拒经 AP 的正常大对象；
/// router 已放宽到单 PUT 上限。这里固定 2 MiB PUT/GET 回归。
#[tokio::test]
async fn large_put_through_ap_not_capped_at_1mib() {
    let dir = tempfile::tempdir().unwrap();
    let e = env_with_max_put(dir.path(), 4 * 1024 * 1024).await;
    let (owner_id, ak, sk) = user(&e, "owner-large", false).await;
    bucket(&e, "largeb", Some(owner_id)).await;
    direct_ap(&e, "large-ap", "largeb", owner_id, None).await;
    let body = vec![b'x'; 2 * 1024 * 1024];
    let r = signed(
        &e,
        Method::PUT,
        "/ap/large-ap/big.bin",
        &[],
        &body,
        &ak,
        &sk,
        "s3",
    )
    .await;
    let status = r.status().as_u16();
    let err = r.text().await.unwrap();
    assert_eq!(status, 200, "{err}");
    let r = signed(
        &e,
        Method::GET,
        "/ap/large-ap/big.bin",
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.bytes().await.unwrap().len(), 2 * 1024 * 1024);
}

/// I2：`POST /ap/<name>/<key>?restore` 推导 RestoreObject（AP 策略 s3:RestoreObject），
/// 内层原样转发 `restore` 查询 → 202 + `x-amz-restore`，归档版本进入 Restoring。
#[tokio::test]
async fn restore_object_through_ap() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (owner_id, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "coldb", Some(owner_id)).await;
    direct_ap(&e, "cold-ap", "coldb", owner_id, None).await;

    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/cold-ap/arch",
            &[],
            b"cold",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    // 仅放行 RestoreObject 的 AP 策略：POST ?restore 走 s3:RestoreObject 维度
    set_ap_policy(
        &e,
        "cold-ap",
        "coldb",
        owner_id,
        Some(&ap_policy(
            "Allow",
            "s3:RestoreObject",
            "arn:aws:s3:::coldb/*",
        )),
    )
    .await;
    let info = e.st.store.head_object("coldb", "arch").await.unwrap();
    let vid = info.version_id.clone().unwrap_or_else(|| "null".into());
    e.st.store
        .archive_version(
            "coldb",
            "arch",
            &vid,
            ArchiveSpec {
                backend: "cold".into(),
                key: "tier-key".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "remote".into(),
                sha256: "00".into(),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();

    let r = signed(
        &e,
        Method::POST,
        "/ap/cold-ap/arch?restore",
        &[("x-amz-restore-request", "days=1")],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 202);
    assert_eq!(
        r.headers()
            .get("x-amz-restore")
            .and_then(|v| v.to_str().ok()),
        Some("ongoing-request=\"true\"")
    );
    let tier =
        e.st.store
            .tier_version("coldb", "arch", &vid)
            .await
            .unwrap()
            .unwrap()
            .tier
            .unwrap();
    assert_eq!(tier.phase, TierPhase::Restoring);
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE op = 'RestoreObject' AND object_key = 'arch' AND decision = 'allow'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1, "AP 路径的 RestoreObject 应留审计");
}

/// Copy 源/目标双授权 + canonical key 只解码一次（策略/审计同一 key）+ 合并审计一行。
#[tokio::test]
async fn copy_dual_auth_and_canonical_keys() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (alice_id, alice_ak, alice_sk) = user(&e, "alice", false).await;
    let (bob_id, bob_ak, bob_sk) = user(&e, "bob", false).await;
    bucket(&e, "dstb", Some(alice_id)).await;
    bucket(&e, "srcb", Some(bob_id)).await;
    direct_ap(&e, "keys-ap", "dstb", alice_id, None).await;
    // alice：目标写 + 源读
    perms(&e, alice_id, &[full("dstb"), read_only("srcb")]).await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/srcb/src",
            &[],
            b"secret",
            &bob_ak,
            &bob_sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/copied",
            &[("x-amz-copy-source", "/srcb/src")],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200,
        "双轴均授权时应允许 Copy"
    );
    // 撤销源读 → Copy 拒（目标写仍在）
    perms(&e, alice_id, &[full("dstb")]).await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/copied2",
            &[("x-amz-copy-source", "/srcb/src")],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        403,
        "Copy 必须同时具备源读"
    );

    // canonical key：%2F 只解码一次 → a/b
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/a%2Fb",
            &[],
            b"slash",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/ap/keys-ap/a%2Fb",
        &[],
        b"",
        &alice_ak,
        &alice_sk,
        "s3",
    )
    .await;
    assert_eq!(r.text().await.unwrap(), "slash");
    // %252F → 字面量 a%2Fb（不是 a/b）
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/a%252Fb",
            &[],
            b"literal",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/ap/keys-ap/a%252Fb",
        &[],
        b"",
        &alice_ak,
        &alice_sk,
        "s3",
    )
    .await;
    assert_eq!(r.text().await.unwrap(), "literal");
    // 非 ASCII
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/%E4%B8%AD.txt",
            &[],
            b"zh",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/ap/keys-ap/%E4%B8%AD.txt",
        &[],
        b"",
        &alice_ak,
        &alice_sk,
        "s3",
    )
    .await;
    assert_eq!(r.text().await.unwrap(), "zh");
    // %2e%2e 解码后是 `..` 段 → 400（解码只做一次且校验严格）
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/keys-ap/%2e%2e%2fevil",
            &[],
            b"x",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        400
    );

    // 合并审计：桶策略 + AP 策略各出一段 detail，且整个请求只有一行
    set_bucket_policy(
        &e,
        "dstb",
        Some(
            &serde_json::json!({"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:aws:s3:::dstb/*"}]})
            .to_string(),
        ),
    )
    .await;
    let ap_obj = "arn:aws:s3:us-east-1:000000000000:accesspoint/keys-ap/object/*";
    set_ap_policy(
        &e,
        "keys-ap",
        "dstb",
        alice_id,
        Some(&ap_policy("Allow", "s3:GetObject", ap_obj)),
    )
    .await;
    sqlx::query("DELETE FROM audit_log")
        .execute(e.st.db.pool())
        .await
        .unwrap();
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/keys-ap/a%2Fb",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        count(
            &e,
            "SELECT COUNT(*) FROM audit_log WHERE op='GetObject' AND object_key='a/b'"
        )
        .await,
        1,
        "经 AP 的请求必须只写一条审计（内层不得二次审计）"
    );
    let detail: Option<String> = sqlx::query_scalar(
        "SELECT detail FROM audit_log WHERE op='GetObject' AND object_key='a/b'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(detail.as_deref(), Some("policy:allow;ap:allow"));
    // %252F 的审计 key 是字面量 a%2Fb
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/keys-ap/a%252Fb",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        count(
            &e,
            "SELECT COUNT(*) FROM audit_log WHERE op='GetObject' AND object_key='a%2Fb'"
        )
        .await,
        1
    );

    // AP 显式 Deny（身份/桶均 Allow）仍写一行 deny 审计，detail 带 ap:deny
    set_ap_policy(
        &e,
        "keys-ap",
        "dstb",
        alice_id,
        Some(&ap_policy("Deny", "s3:GetObject", ap_obj)),
    )
    .await;
    sqlx::query("DELETE FROM audit_log")
        .execute(e.st.db.pool())
        .await
        .unwrap();
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/keys-ap/a%2Fb",
            &[],
            b"",
            &alice_ak,
            &alice_sk,
            "s3"
        )
        .await,
        403
    );
    let (decision, detail): (String, Option<String>) = sqlx::query_as(
        "SELECT decision, detail FROM audit_log WHERE op='GetObject' AND object_key='a/b'",
    )
    .fetch_one(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(decision, "deny");
    assert!(
        detail.as_deref().unwrap_or("").contains("ap:deny"),
        "deny 审计应含 ap:deny: {detail:?}"
    );
}

/// AP Put 触发事件/复制各一次（内层共享 sink，AP 不额外发）。
#[tokio::test]
async fn put_emits_event_and_replication_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (admin_id, ak, sk) = user(&e, "admin", true).await;
    bucket(&e, "evb", Some(admin_id)).await;
    e.st.acl()
        .set_bucket_versioning("evb", "Enabled")
        .await
        .unwrap();
    let cfg = serde_json::json!([{
        "id":"t1","target":"kafka1","events":["s3:ObjectCreated:Put"],
        "prefix":null,"suffix":null
    }])
    .to_string();
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES (?, ?)")
        .bind("evb")
        .bind(&cfg)
        .execute(e.st.db.pool())
        .await
        .unwrap();
    let rules = serde_json::json!([{
        "id":"r1","remote_id":"remote1","dest_bucket":"dest1",
        "prefix":null,"delete_marker":false,"enabled":true
    }])
    .to_string();
    e.st.repl_store.save_rules("evb", &rules).await.unwrap();
    direct_ap(&e, "ev-ap", "evb", admin_id, None).await;

    assert_eq!(
        send_status(&e, Method::PUT, "/ap/ev-ap/k", &[], b"v", &ak, &sk, "s3").await,
        200
    );
    assert_eq!(count(&e, "SELECT COUNT(*) FROM event_outbox").await, 1);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM repl_outbox").await, 1);
    // 读不产生额外事件/复制
    assert_eq!(
        send_status(&e, Method::GET, "/ap/ev-ap/k", &[], b"", &ak, &sk, "s3").await,
        200
    );
    assert_eq!(count(&e, "SELECT COUNT(*) FROM event_outbox").await, 1);
    assert_eq!(count(&e, "SELECT COUNT(*) FROM repl_outbox").await, 1);
}

/// fail-closed：未签名/坏签名/未知 AK 403；AP 缺失 404；坏策略 500；桶删除级联。
#[tokio::test]
async fn fail_closed_and_bucket_delete_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (admin_id, ak, sk) = user(&e, "admin", true).await;
    bucket(&e, "casb", Some(admin_id)).await;
    direct_ap(&e, "cas-ap", "casb", admin_id, None).await;

    // 未签名
    assert_eq!(
        e.http
            .get(format!("{}/ap/cas-ap/x", e.base))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    // 坏签名（错误 SK）
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/cas-ap/x",
            &[],
            b"",
            &ak,
            "wrong-secret",
            "s3"
        )
        .await,
        403
    );
    // 未知 AK（匿名等价面）
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/cas-ap/x",
            &[],
            b"",
            "AKIAUNKNOWN",
            "secret",
            "s3"
        )
        .await,
        403
    );

    // AP 删除后（含缓存移除）→ 404
    direct_ap(&e, "gone-ap", "casb", admin_id, None).await;
    e.st.db.delete_access_point("gone-ap").await.unwrap();
    e.st.ap_cache.remove("gone-ap");
    let r = signed(&e, Method::GET, "/ap/gone-ap/x", &[], b"", &ak, &sk, "s3").await;
    assert_eq!(r.status(), 404);
    let body = r.text().await.unwrap();
    assert!(
        body.contains("<Code>NoSuchAccessPoint</Code>"),
        "未知 AP 应是 NoSuchAccessPoint 而非标准路径回落: {body}"
    );

    // 坏策略：运行期加载失败 → 500（不得当作无策略）
    e.st.db
        .create_access_point("bad-ap", "casb", admin_id, Some("{not-json"), 0)
        .await
        .unwrap();
    assert_eq!(
        send_status(&e, Method::GET, "/ap/bad-ap/x", &[], b"", &ak, &sk, "s3").await,
        500
    );
    let bad_semantics = serde_json::json!({"Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
        "Resource":"arn:x","Condition":{}}]})
    .to_string();
    e.st.db
        .create_access_point("bad2-ap", "casb", admin_id, Some(&bad_semantics), 0)
        .await
        .unwrap();
    assert_eq!(
        send_status(&e, Method::GET, "/ap/bad2-ap/x", &[], b"", &ak, &sk, "s3").await,
        500
    );

    // 桶删除级联：S3 DeleteBucket → AP 行清除 + 缓存失效 + 经 AP 404
    assert_eq!(
        send_status(&e, Method::DELETE, "/casb", &[], b"", &ak, &sk, "s3").await,
        204
    );
    assert!(
        e.st.db.get_access_point("cas-ap").await.unwrap().is_none(),
        "删桶应级联删除 AP 行"
    );
    assert!(e.st.ap_cache.get("cas-ap").is_none(), "删桶应失效 AP 缓存");
    assert_eq!(
        send_status(&e, Method::GET, "/ap/cas-ap/x", &[], b"", &ak, &sk, "s3").await,
        404
    );
}

/// N1：内层 `S3Service::call` 无签名、无 access/route 直达 → 200；N2：AP 未命中回落 STS。
#[tokio::test]
async fn inner_direct_and_sts_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (_admin_id, ak, sk) = user(&e, "admin", true).await;
    bucket(&e, "innerb", Some(_admin_id)).await;
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/innerb/hello",
            &[],
            b"inner",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );

    // N1：内层无 auth/access/route，无签名直达 → 200（只能进程内构造）
    let inner = s3s::service::S3ServiceBuilder::new(xl_s3::service::OssS3::new(
        Arc::clone(&e.st.store),
        e.st.acl(),
        e.st.cfg.max_put_bytes,
    ))
    .build();
    let req = s3s::HttpRequest::builder()
        .method("GET")
        .uri("/innerb/hello")
        .body(s3s::Body::empty())
        .unwrap();
    let resp = inner.call(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    use http_body_util::BodyExt as _;
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"inner");

    // N2：POST / + form → AP 未命中，回落既有 STS
    let r = signed(
        &e,
        Method::POST,
        "/",
        &[("content-type", "application/x-www-form-urlencoded")],
        b"Action=GetSessionToken",
        &ak,
        &sk,
        "sts",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("GetSessionTokenResponse"));
}

// ---------- M8 缺口：DeleteObjects / `<name>-s3alias` / SourceIp ----------

/// DeleteObjects 经 AP：AP 策略与基线逐键取交集；被拒键部分失败，绝不删除。
#[tokio::test]
async fn delete_objects_through_ap_partial() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (uid, ak, sk) = user(&e, "admin", true).await;
    bucket(&e, "delb", Some(uid)).await;
    let policy = serde_json::json!({"Statement":[
        {"Effect":"Allow","Principal":{"AWS":"*"},
         "Action":["s3:PutObject","s3:GetObject","s3:ListBucket","s3:DeleteObject"],
         "Resource":["arn:aws:s3:::delb","arn:aws:s3:::delb/*"]},
        {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObject",
         "Resource":"arn:aws:s3:::delb/ap-denied/*"}
    ]})
    .to_string();
    direct_ap(&e, "del-ap", "delb", uid, Some(&policy)).await;
    // 基线兜底：桶策略 Deny 的键即使 AP Allow 也拒（交集，不放松）
    set_bucket_policy(
        &e,
        "delb",
        Some(
            &serde_json::json!({"Statement":[{
                "Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObject",
                "Resource":"arn:aws:s3:::delb/base-denied/*"}]})
            .to_string(),
        ),
    )
    .await;

    for key in ["ok/a", "ap-denied/b", "base-denied/c"] {
        assert_eq!(
            send_status(
                &e,
                Method::PUT,
                &format!("/ap/del-ap/{key}"),
                &[],
                b"x",
                &ak,
                &sk,
                "s3"
            )
            .await,
            200,
            "{key}"
        );
    }
    let xml = b"<Delete>\
        <Object><Key>ok/a</Key></Object>\
        <Object><Key>ap-denied/b</Key></Object>\
        <Object><Key>base-denied/c</Key></Object>\
        </Delete>";
    let r = signed(
        &e,
        Method::POST,
        "/ap/del-ap?delete",
        &[],
        xml,
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = r.text().await.unwrap();
    assert!(body.contains("<Deleted><Key>ok/a</Key>"), "{body}");
    assert!(!body.contains("<Deleted><Key>ap-denied/b</Key>"), "{body}");
    assert!(
        !body.contains("<Deleted><Key>base-denied/c</Key>"),
        "{body}"
    );
    assert_eq!(
        body.matches("<Code>AccessDenied</Code>").count(),
        2,
        "{body}"
    );
    // 被拒键仍可读，放行键已删
    for (key, want) in [("ok/a", 404), ("ap-denied/b", 200), ("base-denied/c", 200)] {
        assert_eq!(
            send_status(
                &e,
                Method::GET,
                &format!("/ap/del-ap/{key}"),
                &[],
                b"",
                &ak,
                &sk,
                "s3"
            )
            .await,
            want,
            "{key}"
        );
    }
    // 审计：AP 拒键带 ap:deny，与基线 policy:deny 可区分
    assert_eq!(
        count(
            &e,
            "SELECT COUNT(*) FROM audit_log WHERE op='DeleteObjects' \
             AND object_key='ap-denied/b' AND decision='deny' AND detail LIKE '%ap:deny%'"
        )
        .await,
        1
    );
}

/// DeleteObjects 经 AP 支持 VersionId；请求体畸形/超限在 check 阶段即拒。
#[tokio::test]
async fn delete_objects_through_ap_versioned_and_bad_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (uid, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "verb", Some(uid)).await;
    e.st.acl()
        .set_bucket_versioning("verb", "Enabled")
        .await
        .unwrap();
    direct_ap(&e, "ver-ap", "verb", uid, None).await;

    let mut versions = Vec::new();
    for content in ["one", "two"] {
        let r = signed(
            &e,
            Method::PUT,
            "/ap/ver-ap/k",
            &[],
            content.as_bytes(),
            &ak,
            &sk,
            "s3",
        )
        .await;
        assert_eq!(r.status(), 200);
        versions.push(
            r.headers()
                .get("x-amz-version-id")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    }
    let xml = format!(
        "<Delete><Object><Key>k</Key><VersionId>{}</VersionId></Object></Delete>",
        versions[0]
    );
    let r = signed(
        &e,
        Method::POST,
        "/ap/ver-ap?delete",
        &[],
        xml.as_bytes(),
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = r.text().await.unwrap();
    assert!(
        body.contains(&format!("<VersionId>{}</VersionId>", versions[0])),
        "{body}"
    );
    // 指定版本已删，当前版本（two）仍在
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            &format!("/ap/ver-ap/k?versionId={}", versions[0]),
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        404
    );
    let r = signed(&e, Method::GET, "/ap/ver-ap/k", &[], b"", &ak, &sk, "s3").await;
    assert_eq!(r.text().await.unwrap(), "two");

    // 空体 → MissingRequestBodyError；畸形/1001 键 → MalformedXML
    let cases: [(&[u8], &str); 2] = [
        (b"", "MissingRequestBodyError"),
        (b"<Delete><Object>", "MalformedXML"),
    ];
    for (body, code) in cases {
        let r = signed(
            &e,
            Method::POST,
            "/ap/ver-ap?delete",
            &[],
            body,
            &ak,
            &sk,
            "s3",
        )
        .await;
        assert_eq!(r.status(), 400, "{code}");
        let text = r.text().await.unwrap();
        assert!(text.contains(&format!("<Code>{code}</Code>")), "{text}");
    }
    let mut many = String::from("<Delete>");
    for i in 0..=1000 {
        many.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
    }
    many.push_str("</Delete>");
    let r = signed(
        &e,
        Method::POST,
        "/ap/ver-ap?delete",
        &[],
        many.as_bytes(),
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 400);
    // AWS：>1000 键同样回 MalformedXML（不是 InvalidRequest）
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>MalformedXML</Code>")
    );
    // 1 MiB 上限：超一个字节 → EntityTooLarge。AWS 错误表映射 400（S3 无 413 错误码）；
    // 带 Content-Length 的请求由 s3s 自定义路由预检在 check_access 前按同码拒绝。
    let oversize = vec![b'x'; 1024 * 1024 + 1];
    let r = signed(
        &e,
        Method::POST,
        "/ap/ver-ap?delete",
        &[],
        &oversize,
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 400);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>EntityTooLarge</Code>")
    );
}

/// 别名：路径首段与 Host 首标签两种形态；未知别名 404 不回落桶；建桶保留后缀。
#[tokio::test]
async fn alias_path_and_host_forms() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (uid, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "aliasb", Some(uid)).await;
    direct_ap(&e, "alias-ap", "aliasb", uid, None).await;
    let alias = "alias-ap-s3alias";

    // 路径形式；键含 `/` 原样可用
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            &format!("/{alias}/dir/a.txt"),
            &[],
            b"hello",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        &format!("/{alias}/dir/a.txt"),
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "hello");
    let r = signed(
        &e,
        Method::GET,
        &format!("/{alias}?list-type=2"),
        &[],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert!(r.text().await.unwrap().contains("dir/a.txt"));

    // Host 形式（带端口/大小写）：签名覆盖原始 Host
    let host = "ALIAS-AP-s3alias.example.com:19000";
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/dir/b.txt",
            &[("host", host)],
            b"world",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    let r = signed(
        &e,
        Method::GET,
        "/dir/b.txt",
        &[("host", host)],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "world");

    // DELETE 经别名
    assert_eq!(
        send_status(
            &e,
            Method::DELETE,
            &format!("/{alias}/dir/a.txt"),
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        204
    );

    // 未知别名（路径/Host）→ 404 NoSuchAccessPoint，绝不回落桶路由
    let r = signed(&e, Method::GET, "/nope-s3alias/x", &[], b"", &ak, &sk, "s3").await;
    assert_eq!(r.status(), 404);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>NoSuchAccessPoint</Code>")
    );
    let r = signed(
        &e,
        Method::GET,
        "/x",
        &[("host", "nope-s3alias.example.com")],
        b"",
        &ak,
        &sk,
        "s3",
    )
    .await;
    assert_eq!(r.status(), 404);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>NoSuchAccessPoint</Code>")
    );

    // 后缀保留：所有建桶路径共用 validate_bucket
    assert!(e.st.store.create_bucket("foo-s3alias").await.is_err());
}

/// SourceIp 条件：允许网段放行、NotIpAddress 拒；畸形 CIDR 保存即 400。
#[tokio::test]
async fn source_ip_condition_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    // 对端 10.8.8.8：在 Allow 的 10/8 内，不在 Deny 的 NotIpAddress 10.9/16 内
    let e = env_with_peer(dir.path(), Some("10.8.8.8".parse().unwrap())).await;
    let (uid, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "ipb", Some(uid)).await;
    let policy = serde_json::json!({"Statement":[
        {"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject","s3:GetObject"],
         "Resource":"arn:aws:s3:::ipb/*",
         "Condition":{"IpAddress":{"aws:SourceIp":["10.0.0.0/8","2001:db8::/32"]}}},
        {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject",
         "Resource":"arn:aws:s3:::ipb/secret*",
         "Condition":{"NotIpAddress":{"aws:SourceIp":"10.9.0.0/16"}}}
    ]})
    .to_string();
    direct_ap(&e, "ip-ap", "ipb", uid, Some(&policy)).await;

    assert_eq!(
        send_status(&e, Method::PUT, "/ap/ip-ap/open", &[], b"x", &ak, &sk, "s3").await,
        200
    );
    assert_eq!(
        send_status(
            &e,
            Method::PUT,
            "/ap/ip-ap/secret/s",
            &[],
            b"x",
            &ak,
            &sk,
            "s3"
        )
        .await,
        200
    );
    assert_eq!(
        send_status(&e, Method::GET, "/ap/ip-ap/open", &[], b"", &ak, &sk, "s3").await,
        200
    );
    assert_eq!(
        send_status(
            &e,
            Method::GET,
            "/ap/ip-ap/secret/s",
            &[],
            b"",
            &ak,
            &sk,
            "s3"
        )
        .await,
        403
    );

    // 畸形 CIDR：控制面保存 400（不落库，不会运行期 fail-open）
    let token = login(&e.base, "owner").await;
    let bad = serde_json::json!({"name":"bad-ap","bucket":"ipb","policy":{"Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
        "Resource":"arn:aws:s3:::ipb/*",
        "Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/33"}}}]}});
    let r = e
        .http
        .post(format!("{}/api/v1/access-points", e.base))
        .header("authorization", format!("Bearer {token}"))
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

/// 无连接对端：带 IP 条件的语句一律不匹配（fail-closed）。
#[tokio::test]
async fn source_ip_absent_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let e = env(dir.path()).await;
    let (uid, ak, sk) = user(&e, "owner", false).await;
    bucket(&e, "ipnb", Some(uid)).await;
    let policy = serde_json::json!({"Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject","s3:GetObject"],
        "Resource":"arn:aws:s3:::ipnb/*",
        "Condition":{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}}]})
    .to_string();
    direct_ap(&e, "ipn-ap", "ipnb", uid, Some(&policy)).await;
    assert_eq!(
        send_status(&e, Method::PUT, "/ap/ipn-ap/k", &[], b"x", &ak, &sk, "s3").await,
        403
    );
    assert_eq!(
        send_status(&e, Method::GET, "/ap/ipn-ap/k", &[], b"", &ak, &sk, "s3").await,
        403
    );
}
