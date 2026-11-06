//! Object-backend shipping: HTTP gate, cursor file, byte budget, non-JSON.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use xl_store::control_store::sites::SiteChangelogRow;
use xl_store::control_store::sites::SiteStore;

use super::fixture;
use crate::control_store_obj::TableId;
use crate::control_store_obj::jobs::encode;
use crate::local_store::LocalStore;

#[derive(Clone)]
struct Cfg {
    status: u16,
    hits: Arc<Mutex<Vec<serde_json::Value>>>,
}

async fn capture(State(cfg): State<Cfg>, body: String) -> (StatusCode, &'static str) {
    if let Ok(value) = serde_json::from_str(&body) {
        cfg.hits.lock().unwrap().push(value);
    }
    (StatusCode::from_u16(cfg.status).unwrap(), "test")
}

async fn serve(status: u16) -> (String, Arc<Mutex<Vec<serde_json::Value>>>) {
    let cfg = Cfg {
        status,
        hits: Arc::default(),
    };
    let hits = Arc::clone(&cfg.hits);
    let app = Router::new()
        .route("/api/v1/site/changelog", post(capture))
        .with_state(cfg);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), hits)
}

async fn seed_row(fx: &super::Fx, seq: i64, payload: String) {
    let row = SiteChangelogRow {
        seq,
        origin_site_id: "site-a".into(),
        entity: "user".into(),
        entity_id: format!("u{seq}"),
        op: "upsert".into(),
        payload,
        priority: 0,
        ts: 100,
        created_at: 100,
    };
    let mut txn = fx
        .control
        .begin(crate::control_store_obj::WriteOrigin::site_apply("seed"))
        .await
        .unwrap();
    txn.put(
        TableId::SiteChangelog,
        &seq.to_be_bytes(),
        &encode(&row).unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
}

fn cursor(fx: &super::Fx) -> Option<i64> {
    fx.local
        .cursor("peer-b")
        .unwrap()
        .load()
        .unwrap()
        .map(|c| c.last_seq)
}

#[tokio::test]
async fn cursor_advances_after_200_and_survives_reopen() {
    let (base, hits) = serve(200).await;
    let fx = fixture(&base).await;
    seed_row(&fx, 1, r#"{"username":"u1"}"#.into()).await;
    seed_row(&fx, 2, r#"{"username":"u2"}"#.into()).await;

    let shipped = super::super::ship_once(&fx.control, None).await.unwrap();
    assert_eq!(shipped, (1, 2));
    assert_eq!(cursor(&fx), Some(2));
    assert_eq!(hits.lock().unwrap().len(), 1);
    assert_eq!(
        hits.lock().unwrap()[0]["entries"].as_array().unwrap().len(),
        2
    );
    assert!(
        fx.control
            .site("peer-b")
            .await
            .unwrap()
            .unwrap()
            .last_error
            .is_none()
    );

    // A second handle over the same directory sees the atomically renamed cursor.
    let reopened = LocalStore::open(fx.local.root()).unwrap();
    assert_eq!(
        reopened
            .cursor("peer-b")
            .unwrap()
            .load()
            .unwrap()
            .unwrap()
            .last_seq,
        2
    );
}

#[tokio::test]
async fn failure_leaves_cursor_and_sets_last_error() {
    for status in [400u16, 500] {
        let (base, hits) = serve(status).await;
        let fx = fixture(&base).await;
        seed_row(&fx, 1, r#"{"username":"u1"}"#.into()).await;

        let shipped = super::super::ship_once(&fx.control, None).await.unwrap();
        assert_eq!(shipped, (0, 0));
        assert_eq!(cursor(&fx), None);
        assert_eq!(hits.lock().unwrap().len(), 1);
        let peer = fx.control.site("peer-b").await.unwrap().unwrap();
        assert!(
            peer.last_error
                .as_deref()
                .unwrap()
                .contains(&status.to_string())
        );
    }
}

#[tokio::test]
async fn batch_byte_cap_stops_before_second_row() {
    let (base, hits) = serve(200).await;
    let fx = fixture(&base).await;
    let big = "x".repeat(600 * 1024);
    seed_row(&fx, 1, format!(r#"{{"password_hash":"{big}"}}"#)).await;
    seed_row(&fx, 2, format!(r#"{{"password_hash":"{big}"}}"#)).await;

    let shipped = super::super::ship_once(&fx.control, None).await.unwrap();
    assert_eq!(shipped, (1, 1));
    assert_eq!(cursor(&fx), Some(1));
    let entries = hits.lock().unwrap()[0]["entries"].as_array().unwrap().len();
    assert_eq!(entries, 1);
}

#[tokio::test]
async fn non_json_payload_refuses_to_advance() {
    let (base, hits) = serve(200).await;
    let fx = fixture(&base).await;
    seed_row(&fx, 1, "not-json".into()).await;

    let err = super::super::ship_once(&fx.control, None)
        .await
        .unwrap_err();
    assert!(err.contains("非 JSON"), "{err}");
    assert_eq!(cursor(&fx), None);
    assert!(hits.lock().unwrap().is_empty());
}
