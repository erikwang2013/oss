//! M8：tier 远端 GC/对账——dry-run 默认、grace、live 引用保护、删除失败、admin 路由。

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use oss_server::config::{TierKind, TierTarget};
use oss_server::jobs::TaskExecutor as _;
use oss_server::state::AppState;
use oss_server::tier::{S3TierGc, TierBackend, TierBackends, TierObject, tier_key};
use oss_server::tier_gc_sweep::{GcOptions, run_gc};
use oss_server::tier_jobs::M8TaskExecutor;

const DAY: i64 = 86_400;

#[derive(Default)]
struct MockTier {
    objs: Mutex<HashMap<String, (Bytes, i64)>>,
    deleted: Mutex<Vec<String>>,
    fail_delete: Mutex<std::collections::HashSet<String>>,
    list_fail: AtomicBool,
}

impl MockTier {
    fn add(&self, key: &str, mtime: i64) {
        self.objs
            .lock()
            .unwrap()
            .insert(key.to_string(), (Bytes::from_static(b"cold"), mtime));
    }

    fn set_mtime(&self, key: &str, mtime: i64) {
        self.objs.lock().unwrap().get_mut(key).unwrap().1 = mtime;
    }

    fn has(&self, key: &str) -> bool {
        self.objs.lock().unwrap().contains_key(key)
    }

    fn deleted(&self) -> Vec<String> {
        self.deleted.lock().unwrap().clone()
    }

    fn fail_delete(&self, key: &str) {
        self.fail_delete.lock().unwrap().insert(key.to_string());
    }

    fn fail_list(&self) {
        self.list_fail.store(true, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl TierBackend for MockTier {
    fn id(&self) -> &str {
        "cold"
    }

    async fn put(&self, key: &str, body: Bytes, etag: &str) -> Result<String, String> {
        self.objs
            .lock()
            .unwrap()
            .insert(key.to_string(), (body, now()));
        Ok(format!("mock-{etag}"))
    }

    async fn get(&self, key: &str) -> Result<Bytes, String> {
        self.objs
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, _)| b.clone())
            .ok_or_else(|| format!("missing {key}"))
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        Ok(self
            .objs
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, _)| b.len() as u64))
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        if self.fail_delete.lock().unwrap().contains(key) {
            return Err(format!("mock delete 失败：{key}"));
        }
        self.deleted.lock().unwrap().push(key.to_string());
        self.objs.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<TierObject>, String> {
        if self.list_fail.load(Ordering::Relaxed) {
            return Err("mock list 失败".to_string());
        }
        Ok(self
            .objs
            .lock()
            .unwrap()
            .iter()
            .map(|(key, (body, mtime))| TierObject {
                key: key.clone(),
                last_modified: *mtime,
                size: body.len() as u64,
            })
            .collect())
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn tier_target() -> TierTarget {
    TierTarget {
        id: "cold".into(),
        kind: TierKind::S3,
        path: None,
        endpoint: "http://127.0.0.1:9".into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: None,
    }
}

async fn setup() -> (AppState, Arc<MockTier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut st = common::state_with_tiers(dir.path(), vec![tier_target()]).await;
    let mock = Arc::new(MockTier::default());
    let backends = Arc::new(TierBackends::new(vec![
        Arc::clone(&mock) as Arc<dyn TierBackend>
    ]));
    st.tiers = Arc::clone(&backends);
    st.tier_gc = Arc::new(S3TierGc::new(backends));
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    (st, mock, dir)
}

fn executor(st: &AppState) -> M8TaskExecutor {
    M8TaskExecutor::new(
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.tiers),
        Arc::clone(&st.events),
    )
}

/// 归档一个版本并返回其 live tier key。
async fn archive_live(st: &AppState, key: &str) -> String {
    let info = st
        .store
        .put_object("bkt", key, Bytes::from_static(b"payload"), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    let payload = serde_json::json!({
        "bucket": "bkt", "key": key, "version_id": vid,
        "class": "GLACIER", "tier": "cold"
    })
    .to_string();
    let out = executor(st).execute("Transition", &payload).await.unwrap();
    assert_eq!(out, "archived");
    tier_key("cold", "bkt", &vid, key)
}

fn opts(dry_run: bool) -> GcOptions {
    GcOptions {
        tier: None,
        dry_run,
        grace_days: 7,
        max_deletes: 1000,
    }
}

#[tokio::test]
async fn dry_run_reports_stale_without_deleting() {
    let (st, mock, _dir) = setup().await;
    let live = archive_live(&st, "live.txt").await;
    let orphan = "cold/bkt/orphan-v1/orphan.bin";
    mock.add(orphan, now() - 10 * DAY);
    mock.set_mtime(&live, now() - 10 * DAY);
    mock.add("cold/bkt/young-v1/young.bin", now() - DAY);

    let resp = run_gc(&st.store, &st.tiers, &opts(true)).await.unwrap();
    assert!(resp.dry_run);
    assert_eq!(resp.reports.len(), 1);
    let r = &resp.reports[0];
    assert_eq!(r.tier, "cold");
    assert_eq!(r.listed, 3);
    assert_eq!(r.live_refs, 1);
    assert_eq!(r.kept_live, 1);
    assert_eq!(r.kept_grace, 1);
    assert_eq!(r.stale, 1);
    assert_eq!(r.would_delete, 1);
    assert_eq!(r.deleted, 0);
    assert_eq!(r.missing_live, 0);
    assert!(mock.deleted().is_empty(), "dry-run 不得删除任何对象");
    assert!(mock.has(&live) && mock.has(orphan));
}

#[tokio::test]
async fn real_run_deletes_only_old_orphans_and_keeps_live() {
    let (st, mock, _dir) = setup().await;
    let live = archive_live(&st, "live.txt").await;
    let old = "cold/bkt/old-v1/old.bin";
    let young = "cold/bkt/young-v1/young.bin";
    let unknown = "cold/bkt/unknown-v1/x.bin";
    let foreign = "other/x.bin";
    mock.add(old, now() - 10 * DAY);
    mock.add(young, now() - DAY);
    mock.add(unknown, 0);
    mock.add(foreign, now() - 30 * DAY);
    mock.set_mtime(&live, now() - 10 * DAY);

    let resp = run_gc(&st.store, &st.tiers, &opts(false)).await.unwrap();
    let r = &resp.reports[0];
    assert_eq!(r.kept_live, 1);
    assert_eq!(r.kept_grace, 1);
    assert_eq!(r.kept_unknown_age, 1);
    assert_eq!(r.kept_foreign, 1);
    assert_eq!(r.stale, 1);
    assert_eq!(r.deleted, 1);
    assert_eq!(r.failed, 0);
    assert_eq!(mock.deleted(), vec![old.to_string()]);
    assert!(mock.has(&live), "live 引用永不删除");
    assert!(mock.has(young) && mock.has(unknown) && mock.has(foreign));
}

#[tokio::test]
async fn max_deletes_caps_one_run() {
    let (st, mock, _dir) = setup().await;
    for i in 0..3 {
        mock.add(&format!("cold/bkt/v{i}/x"), now() - 10 * DAY);
    }
    let mut o = opts(false);
    o.max_deletes = 2;
    let resp = run_gc(&st.store, &st.tiers, &o).await.unwrap();
    let r = &resp.reports[0];
    assert_eq!(r.stale, 3);
    assert_eq!(r.would_delete, 2);
    assert_eq!(r.deleted, 2);
    assert!(r.truncated, "剩余候选应标记 truncated");
    assert_eq!(mock.deleted().len(), 2);
}

#[tokio::test]
async fn delete_failures_are_reported_and_do_not_stop_others() {
    let (st, mock, _dir) = setup().await;
    let bad = "cold/bkt/bad-v1/x";
    let good = "cold/bkt/good-v1/x";
    mock.add(bad, now() - 10 * DAY);
    mock.add(good, now() - 10 * DAY);
    mock.fail_delete(bad);

    let resp = run_gc(&st.store, &st.tiers, &opts(false)).await.unwrap();
    let r = &resp.reports[0];
    assert_eq!(r.deleted, 1);
    assert_eq!(r.failed, 1);
    assert_eq!(r.errors.len(), 1);
    assert_eq!(r.errors[0].key, bad);
    assert!(r.errors[0].error.contains("mock delete 失败"));
    assert!(mock.has(bad), "失败对象保留，等下一轮重试");
    assert!(!mock.has(good));
}

#[tokio::test]
async fn list_failure_aborts_without_deleting() {
    let (st, mock, _dir) = setup().await;
    mock.add("cold/bkt/old-v1/x", now() - 10 * DAY);
    mock.fail_list();
    let err = run_gc(&st.store, &st.tiers, &opts(false))
        .await
        .unwrap_err();
    assert!(err.contains("list 失败"), "{err}");
    assert!(mock.deleted().is_empty());
}

#[tokio::test]
async fn missing_live_is_reported_but_not_written() {
    let (st, mock, _dir) = setup().await;
    let live = archive_live(&st, "gone.txt").await;
    mock.objs.lock().unwrap().remove(&live);

    let resp = run_gc(&st.store, &st.tiers, &opts(false)).await.unwrap();
    let r = &resp.reports[0];
    assert_eq!(r.missing_live, 1);
    assert_eq!(r.missing_samples, vec![live]);
    assert_eq!(r.deleted, 0);
    assert!(mock.deleted().is_empty());
}

// ---------- admin 路由 ----------

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str, user: &str, pw: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": user, "password": pw}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn console_route_is_admin_only_and_defaults_to_dry_run() {
    let (st, mock, _dir) = setup().await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("sub", "pw", false).await.unwrap();
    let orphan = "cold/bkt/route-v1/x";
    mock.add(orphan, now() - 10 * DAY);

    let base = serve(oss_server::router::build_router(st.clone()).unwrap()).await;
    let http = reqwest::Client::new();
    let token = login(&base, "erik", "pw").await;
    let sub = login(&base, "sub", "pw").await;

    // 非管理员 → 403
    assert_eq!(
        http.post(format!("{base}/api/v1/tiers/gc"))
            .bearer_auth(&sub)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    // 空 body = 默认 dry-run
    let resp = http
        .post(format!("{base}/api/v1/tiers/gc"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["dry_run"], true);
    assert_eq!(v["grace_days"], 7);
    assert_eq!(v["reports"][0]["stale"], 1);
    assert_eq!(v["reports"][0]["deleted"], 0);
    assert!(mock.deleted().is_empty());

    // 非法参数 → 400
    for body in [
        serde_json::json!({"grace_days": 99_999}),
        serde_json::json!({"max_deletes": 0}),
        serde_json::json!({"tier": "nope"}),
    ] {
        assert_eq!(
            http.post(format!("{base}/api/v1/tiers/gc"))
                .bearer_auth(&token)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::BAD_REQUEST,
            "body={body}"
        );
    }

    // 显式 dry_run=false 才真删
    let v: serde_json::Value = http
        .post(format!("{base}/api/v1/tiers/gc"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"dry_run": false}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["reports"][0]["deleted"], 1);
    assert_eq!(mock.deleted(), vec![orphan.to_string()]);
}
