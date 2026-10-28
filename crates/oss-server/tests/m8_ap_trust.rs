//! AP Slice A 信任链 E2E：真实 listener + `ConnectInfo` → 可信反代头
//! （`x-oss-source-vpc` / `x-oss-source-vpce`）→ `ApVpc`/`ApVpce` extension →
//! endpoint registry 门禁 → 200/403。
//!
//! 其余 m8_ap 用例直接注入 extension，绕过 `Attest` 的头解析与直连对端信任；
//! 这里覆盖那条未验证的 HTTP 边界：重复/畸形头、非可信 peer、缺 ConnectInfo、
//! 静态 ID 优先、registry 各拒绝分支，以及 DeleteObjects 请求级门禁与审计 detail。

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use oss_server::router::{ApTrustConfig, build_router_with_trust};
use oss_server::state::AppState;
use reqwest::Client;
use xl_s3::ap::{AccessPoint, ApEndpoint, ApEndpointSource};

const VPCE: &str = "vpce-0trust001";
const VPC: &str = "vpc-0trust001";
const BUCKET: &str = "trustb";
const AP: &str = "trust-ap";
const KEY: &str = "k";

#[derive(Default)]
struct Registry {
    rows: RwLock<Vec<ApEndpoint>>,
}

impl Registry {
    fn put(&self, vpce: &str, vpc: &str, account: &str, state: &str) {
        let mut w = self.rows.write().unwrap();
        w.retain(|e| e.vpce_id != vpce);
        w.push(ApEndpoint {
            vpce_id: vpce.into(),
            vpc_id: vpc.into(),
            account_id: account.into(),
            state: state.into(),
        });
    }

    fn clear(&self) {
        self.rows.write().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl ApEndpointSource for Registry {
    async fn lookup(&self, vpce: &str) -> Result<Option<ApEndpoint>, String> {
        Ok(self
            .rows
            .read()
            .unwrap()
            .iter()
            .find(|e| e.vpce_id == vpce)
            .cloned())
    }
}

struct Env {
    st: AppState,
    account: String,
    base: String,
    http: Client,
}

impl Env {
    async fn last_detail(&self) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT detail FROM audit_log ORDER BY id DESC LIMIT 1",
        )
        .fetch_one(self.st.db.pool())
        .await
        .unwrap()
    }

    async fn last_detail_for(&self, op: &str) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT detail FROM audit_log WHERE op = ? ORDER BY id DESC LIMIT 1",
        )
        .bind(op)
        .fetch_one(self.st.db.pool())
        .await
        .unwrap()
    }

    fn get(&self) -> reqwest::RequestBuilder {
        self.http.get(format!("{}/ap/{AP}/{KEY}", self.base))
    }

    async fn get_status(&self, headers: &[(&str, &str)]) -> u16 {
        let mut rb = self.get();
        for (k, v) in headers {
            rb = rb.header(*k, *v);
        }
        rb.send().await.unwrap().status().as_u16()
    }
}

fn bucket_policy() -> String {
    serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},
        "Action":["s3:GetObject","s3:DeleteObject"],
        "Resource":format!("arn:aws:s3:::{BUCKET}/*")}]})
    .to_string()
}

/// 两个条件 AND：VPC 或 VPCE 任一证明缺失 → Allow 不命中（fail-closed）。
fn ap_policy() -> String {
    serde_json::json!({"Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*",
        "Resource":format!("arn:aws:s3:::{BUCKET}/*"),
        "Condition":{"StringEquals":{"aws:SourceVpc":VPC,"aws:SourceVpce":VPCE}}}]})
    .to_string()
}

/// 起真实 listener（`connect_info=false` 模拟传输层不提供 `ConnectInfo`）。
async fn env(dir: &Path, cfg: ApTrustConfig, registry: Arc<Registry>, connect_info: bool) -> Env {
    let st = common::state_with_endpoint_source(dir, registry).await;
    let uid = st.db.create_user("owner", "pw", false).await.unwrap();
    st.store.create_bucket(BUCKET).await.unwrap();
    st.db.insert_bucket(BUCKET, Some(uid)).await.unwrap();
    for (key, body) in [(KEY, b"hello".as_slice()), ("del", b"x".as_slice())] {
        st.store
            .put_object(BUCKET, key, Bytes::copy_from_slice(body), None)
            .await
            .unwrap();
    }
    let account = st.db.bucket_account(BUCKET).await.unwrap();
    st.db
        .set_bucket_policy(BUCKET, &bucket_policy(), 0)
        .await
        .unwrap();
    let policy = ap_policy();
    st.db
        .create_access_point(AP, BUCKET, uid, Some(&policy), 0)
        .await
        .unwrap();
    st.ap_cache.put(
        AccessPoint::from_row(
            AP.into(),
            BUCKET.into(),
            uid,
            Some(policy),
            &st.ap_region,
            &account,
        )
        .unwrap(),
    );
    let app = build_router_with_trust(st.clone(), cfg).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    if connect_info {
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap()
        });
    } else {
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    }
    Env {
        st,
        account,
        base: format!("http://{addr}"),
        http: Client::new(),
    }
}

fn trusted(peer_cidrs: &str) -> ApTrustConfig {
    ApTrustConfig {
        trust_proxy: true,
        peer_cidrs: Some(peer_cidrs.into()),
        ..Default::default()
    }
}

/// 头 → extension → registry → 200；两证明缺一即 403；审计带端点/账户。
#[tokio::test]
async fn trusted_peer_headers_reach_registry_gate() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let e = env(dir.path(), trusted("127.0.0.1/32"), registry.clone(), true).await;
    registry.put(VPCE, VPC, &e.account, "active");
    let vpce = ("x-oss-source-vpce", VPCE);
    let vpc = ("x-oss-source-vpc", VPC);

    let r = e.get().header(vpce.0, vpce.1).header(vpc.0, vpc.1).send();
    let r = r.await.unwrap();
    assert_eq!(r.status(), 200, "可信 peer + 合法头应放行");
    assert_eq!(r.text().await.unwrap(), "hello");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains(&format!("endpoint={VPCE}/account={}", e.account)),
        "审计须记录端点/账户: {detail}"
    );

    // 两个证明独立且 AND：缺任一 → AP 策略条件不命中 → 403。
    assert_eq!(e.get_status(&[vpce]).await, 403, "只有 VPCE 证明");
    assert_eq!(e.get_status(&[vpc]).await, 403, "只有 VPC 证明");
    assert_eq!(e.get_status(&[]).await, 403, "无证明");
}

/// 重复头行、单行逗号拼接、畸形 ID（大小写/前缀/超长）一律不产生 extension。
#[tokio::test]
async fn duplicate_and_malformed_headers_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let e = env(dir.path(), trusted("127.0.0.1/32"), registry.clone(), true).await;
    registry.put(VPCE, VPC, &e.account, "active");

    // 重复同名头行（即使值相同）→ Attest 取第一条后仍有第二条 → 无证明。
    let dup = e
        .get()
        .header("x-oss-source-vpce", VPCE)
        .header("x-oss-source-vpce", VPCE)
        .header("x-oss-source-vpc", VPC)
        .send()
        .await
        .unwrap();
    assert_eq!(dup.status(), 403, "重复头行必须 fail-closed");
    let detail = e.last_detail().await.unwrap();
    assert!(
        !detail.contains("endpoint="),
        "重复头不得形成端点证明: {detail}"
    );

    let too_long = format!("vpce-{}", "a".repeat(65));
    let bad_vpce = [
        "vpce-ABC",      // 大写
        "vpc-0trust001", // 错误前缀
        "vpce-a,vpce-b", // 单行逗号拼接
        "",              // 空值
        too_long.as_str(),
    ];
    for bad in bad_vpce {
        let status = e
            .get_status(&[("x-oss-source-vpce", bad), ("x-oss-source-vpc", VPC)])
            .await;
        assert_eq!(status, 403, "畸形 VPCE 头: {bad:?}");
    }
    let detail = e.last_detail().await.unwrap();
    assert!(
        !detail.contains("endpoint="),
        "畸形 VPCE 头不得形成端点证明: {detail}"
    );

    // 合法 VPCE + 畸形 VPC：VPC 证明缺失 → AP 策略条件不命中（端点断言本身仍成立）。
    assert_eq!(
        e.get_status(&[
            ("x-oss-source-vpce", VPCE),
            ("x-oss-source-vpc", "VPC-0TRUST001"),
        ])
        .await,
        403
    );
}

/// 直连对端不在 `OSS_AP_TRUSTED_PEER_CIDRS`：头无效，退回真实对端（无 VPC 证明）。
#[tokio::test]
async fn untrusted_direct_peer_ignores_headers() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    // listener 在 127.0.0.1，但只信任 10/8：回环直连不可信。
    let e = env(dir.path(), trusted("10.0.0.0/8"), registry.clone(), true).await;
    registry.put(VPCE, VPC, &e.account, "active");
    assert_eq!(
        e.get_status(&[("x-oss-source-vpce", VPCE), ("x-oss-source-vpc", VPC),])
            .await,
        403
    );
    let detail = e.last_detail().await.unwrap();
    assert!(
        !detail.contains("endpoint="),
        "非可信 peer 不得形成端点证明: {detail}"
    );
}

/// `OSS_AP_TRUSTED_PROXY` 未开启：即使 peer CIDR 命中，头也完全无效。
#[tokio::test]
async fn trust_proxy_disabled_ignores_headers() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let cfg = ApTrustConfig {
        trust_proxy: false,
        peer_cidrs: Some("127.0.0.1/32".into()),
        ..Default::default()
    };
    let e = env(dir.path(), cfg, registry.clone(), true).await;
    registry.put(VPCE, VPC, &e.account, "active");
    assert_eq!(
        e.get_status(&[("x-oss-source-vpce", VPCE), ("x-oss-source-vpc", VPC),])
            .await,
        403
    );
}

/// 传输层不提供 `ConnectInfo`（ecat 明文 listener 形态）：头一律不采信。
#[tokio::test]
async fn missing_connect_info_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let e = env(dir.path(), trusted("127.0.0.1/32"), registry.clone(), false).await;
    registry.put(VPCE, VPC, &e.account, "active");
    assert_eq!(
        e.get_status(&[("x-oss-source-vpce", VPCE), ("x-oss-source-vpc", VPC),])
            .await,
        403
    );
    let detail = e.last_detail().await.unwrap();
    assert!(
        !detail.contains("endpoint="),
        "缺 ConnectInfo 不得形成端点证明: {detail}"
    );
}

/// registry 门禁：未登记 / 停用 / 跨账户 / VPC 不匹配均 403，审计可追溯；
/// active + 同账户 200。
#[tokio::test]
async fn registry_denials_are_fail_closed_and_audited() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let e = env(dir.path(), trusted("127.0.0.1/32"), registry.clone(), true).await;
    let headers = [("x-oss-source-vpce", VPCE), ("x-oss-source-vpc", VPC)];
    let get = || e.get_status(&headers);

    registry.clear();
    assert_eq!(get().await, 403, "unregistered");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains("endpoint:deny:unregistered"),
        "审计须记录未登记: {detail}"
    );

    registry.put(VPCE, VPC, "123456789012", "active");
    assert_eq!(get().await, 403, "cross-account");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains("endpoint=vpce-0trust001/account=123456789012")
            && detail.contains("endpoint:deny:account-mismatch"),
        "审计须记录跨账户拒绝: {detail}"
    );

    registry.put(VPCE, VPC, &e.account, "disabled");
    assert_eq!(get().await, 403, "disabled");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains("endpoint:deny:disabled"),
        "审计须记录停用: {detail}"
    );

    registry.put(VPCE, "vpc-0other", &e.account, "active");
    assert_eq!(get().await, 403, "vpc-mismatch");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains("endpoint:deny:vpc-mismatch"),
        "审计须记录 VPC 不匹配: {detail}"
    );

    registry.put(VPCE, VPC, &e.account, "active");
    assert_eq!(get().await, 200, "active + 同账户");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains(&format!("endpoint={VPCE}/account={}", e.account)),
        "审计须记录放行端点/账户: {detail}"
    );
}

/// 静态 `OSS_AP_VPC_ID`/`OSS_AP_VPCE_ID`：优先于任何头，伪造头不参与判定。
#[tokio::test]
async fn static_ids_take_precedence_over_headers() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let cfg = ApTrustConfig {
        static_vpc: Some(VPC.into()),
        static_vpce: Some(VPCE.into()),
        ..Default::default()
    };
    let e = env(dir.path(), cfg, registry.clone(), true).await;
    registry.put(VPCE, VPC, &e.account, "active");
    // 伪造端点若被采信会命中停用行 → 403；静态 ID 优先时必须仍 200。
    registry.put("vpce-attacker", VPC, "999999999999", "disabled");

    assert_eq!(e.get_status(&[]).await, 200, "静态证明无需头");
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains(&format!("endpoint={VPCE}/account={}", e.account)),
        "审计须记录静态端点: {detail}"
    );

    assert_eq!(
        e.get_status(&[
            ("x-oss-source-vpce", "vpce-attacker"),
            ("x-oss-source-vpc", VPC),
        ])
        .await,
        200,
        "伪造头不得覆盖静态证明"
    );
    let detail = e.last_detail().await.unwrap();
    assert!(
        detail.contains(&format!("endpoint={VPCE}/account={}", e.account)),
        "伪造头不得进入审计端点: {detail}"
    );
}

/// 静态 ID 配置非法 → 启动失败（不静默降级）。
#[tokio::test]
async fn invalid_static_id_rejected_at_build() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let err = match build_router_with_trust(
        st,
        ApTrustConfig {
            static_vpce: Some("not-a-vpce".into()),
            ..Default::default()
        },
    ) {
        Ok(_) => panic!("非法静态 VPCE 应启动失败"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("OSS_AP_VPCE_ID"), "{err}");
}

/// DeleteObjects 请求级门禁：端点拒绝在逐键处理前整体 403；registry 放行后请求
/// 正常执行（逐键策略另测于 m8_ap）。
#[tokio::test]
async fn delete_objects_request_gate_and_audit() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::default());
    let e = env(dir.path(), trusted("127.0.0.1/32"), registry.clone(), true).await;
    let headers = [("x-oss-source-vpce", VPCE), ("x-oss-source-vpc", VPC)];
    let post = || {
        let mut rb = e
            .http
            .post(format!("{}/ap/{AP}?delete", e.base))
            .body("<Delete><Object><Key>del</Key></Object></Delete>");
        for (k, v) in headers {
            rb = rb.header(k, v);
        }
        rb.send()
    };

    registry.put(VPCE, VPC, &e.account, "disabled");
    let r = post().await.unwrap();
    assert_eq!(r.status(), 403, "停用端点必须在请求级拒绝");
    let detail = e.last_detail_for("DeleteObjects").await.unwrap();
    assert!(
        detail.contains("endpoint:deny:disabled"),
        "审计须记录 DeleteObjects 请求级拒绝原因: {detail}"
    );

    registry.put(VPCE, VPC, &e.account, "active");
    let r = post().await.unwrap();
    assert_eq!(r.status(), 200, "端点门禁通过后进入逐键处理");
    let body = r.text().await.unwrap();
    assert!(body.contains("<Deleted><Key>del</Key>"), "{body}");
    // 删除确实执行（不是请求级放行后被静默跳过）。
    assert_eq!(e.get_status(&headers).await, 200, "k 仍在");
    let gone = e
        .http
        .get(format!("{}/ap/{AP}/del", e.base))
        .header(headers[0].0, headers[0].1)
        .header(headers[1].0, headers[1].1)
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404, "del 已删除");
}
