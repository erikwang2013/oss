//! M4 切片 1（观测面）：`/metrics` 与 `/api/v1/cluster/status`。
//! 单机口径下的形态与鉴权断言；集群口径由 M3 集群测试基座 + e2e 覆盖。

mod common;

use common::state;
use oss_server::router::build_router;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login_token(base: &str, user: &str, pass: &str) -> String {
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({ "username": user, "password": pass }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    login["token"].as_str().unwrap().to_string()
}

/// `/metrics`：公开、Prometheus 文本形态、含最小信号（单机：1 盘 1 槽、计数为 0）。
#[tokio::test]
async fn metrics_exposes_minimal_signal() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let base = serve(build_router(st).unwrap()).await;

    let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.starts_with("text/plain"), "content-type: {ct}");
    let body = resp.text().await.unwrap();
    // 形态与数值都断言（不是只跑通）
    assert!(
        body.contains("# TYPE oss_quorum_unmet_total counter"),
        "{body}"
    );
    assert!(
        body.contains("oss_quorum_unmet_total{op=\"write\"} 0"),
        "{body}"
    );
    assert!(
        body.contains("oss_quorum_unmet_total{op=\"lock\"} 0"),
        "{body}"
    );
    assert!(body.contains("oss_drive_count 1"), "{body}");
    assert!(body.contains("oss_slot_count 1"), "{body}");
    // DP-1：单机无可达不兼容/降级节点
    assert!(body.contains("oss_data_plane_compatible 1"), "{body}");
    assert!(body.contains("oss_data_plane_degraded_peers 0"), "{body}");
    assert!(
        body.lines()
            .any(|l| l.starts_with("oss_node_up{node=") && l.ends_with(" 1")),
        "缺本机 node_up=1 行：{body}"
    );
}

/// 审计队列丢弃计数经 `/metrics` 导出（队列保持 serve-only/best-effort）。
#[tokio::test]
async fn metrics_exposes_audit_dropped_counter() {
    use xl_s3::access::AuditSink as _;

    let dir = tempfile::tempdir().unwrap();
    let mut st = state(dir.path()).await;
    let q = oss_server::audit_queue::AuditQueue::new(1);
    assert!(q.enqueue(1, "GetObject", Some("b"), Some("k"), "allow", None));
    assert!(!q.enqueue(1, "GetObject", Some("b"), Some("k"), "allow", None));
    st.audit = Some(q);
    let base = serve(build_router(st).unwrap()).await;

    let body = reqwest::get(format!("{base}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("# TYPE oss_audit_dropped_total counter"),
        "{body}"
    );
    assert!(body.contains("oss_audit_dropped_total 1"), "{body}");
}

/// 集群状态：未带 token 401；非管理员 403；管理员单机 = mode:"single" 且形状完整。
#[tokio::test]
async fn cluster_status_auth_gate_and_single_mode() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("sub", "pw", false).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let url = format!("{base}/api/v1/cluster/status");

    // 匿名 → 401
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 401);

    // 非管理员 → 403（纯文本形态，与既有 403 一致）
    let sub_tok = login_token(&base, "sub", "pw").await;
    let resp = reqwest::Client::new()
        .get(&url)
        .bearer_auth(&sub_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);

    // 管理员 → 200，单机优雅降级
    let tok = login_token(&base, "erik", "pw").await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["mode"], "single");
    assert_eq!(v["node_id"], serde_json::Value::Null);
    assert_eq!(v["slot_count"], 1);
    assert_eq!(v["topo_fingerprint"], serde_json::Value::Null);
    assert_eq!(v["nodes"][0]["id"], "local");
    assert_eq!(v["nodes"][0]["drive_count"], 1);
    assert_eq!(v["nodes"][0]["up"], true);
    assert_eq!(v["quorum_unmet"]["write"], 0);
    assert_eq!(v["quorum_unmet"]["lock"], 0);
    // DP-1：单机 = 自身即全集群；运维可见数据面版本与降级/不兼容状态。
    assert_eq!(v["data_wire_version"], 2);
    assert_eq!(v["data_plane_compatible"], true);
    assert_eq!(v["degraded_peers"], serde_json::json!([]));
    assert_eq!(v["incompatible_peers"], serde_json::json!([]));
}

/// DP-0：公开健康视图同样带数据面握手字段（单机口径）。
#[tokio::test]
async fn health_cluster_exposes_data_plane_status() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let base = serve(build_router(st).unwrap()).await;

    let resp = reqwest::get(format!("{base}/health/cluster"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["data_wire_version"], 2);
    assert_eq!(v["data_plane_compatible"], true);
    assert_eq!(v["degraded_peers"], serde_json::json!([]));
    assert_eq!(v["incompatible_peers"], serde_json::json!([]));
    assert_eq!(v["nodes"][0]["data_wire_version"], 2);
}
