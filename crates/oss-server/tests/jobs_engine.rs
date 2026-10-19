//! M8 S0：作业引擎——领取/执行/收敛/报告/失败上限/超时。

use std::sync::Arc;

use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::jobs::{JobEnv, TaskExecutor, worker_once};
use xl_store::store::{Store, StoreConfig};

struct OkExec;

#[async_trait::async_trait]
impl TaskExecutor for OkExec {
    async fn execute(&self, action: &str, _payload: &str) -> Result<String, String> {
        Ok(format!("{action}-ok"))
    }
}

struct FailExec;

#[async_trait::async_trait]
impl TaskExecutor for FailExec {
    async fn execute(&self, _action: &str, _payload: &str) -> Result<String, String> {
        Err("boom".into())
    }
}

struct SlowExec;

#[async_trait::async_trait]
impl TaskExecutor for SlowExec {
    async fn execute(&self, _action: &str, _payload: &str) -> Result<String, String> {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Ok("late".into())
    }
}

async fn setup() -> (Db, Arc<Store>, tempfile::TempDir, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let store_dir = tempfile::tempdir().unwrap();
    let db = Db::connect(&format!(
        "sqlite://{}?mode=rwc",
        db_dir.path().join("jobs.db").display()
    ))
    .await
    .unwrap();
    let store = Arc::new(
        Store::open(StoreConfig::new(store_dir.path()))
            .await
            .unwrap(),
    );
    store.create_bucket("reports").await.unwrap();
    (db, store, db_dir, store_dir)
}

fn env(max_attempts: i64, timeout_secs: i64) -> JobEnv {
    JobEnv {
        workers: 1,
        interval_secs: 1,
        max_attempts,
        timeout_secs,
        tier_timeout_secs: timeout_secs,
    }
}

fn spec(tasks: Vec<&str>, report: bool) -> JobSpec {
    JobSpec {
        // 非 batch：走引擎通用报告列（batch 列格式由 m8_batch 覆盖）
        kind: "generic".into(),
        bucket: "reports".into(),
        requested_by: Some(7),
        tasks: tasks
            .into_iter()
            .map(|a| NewJobTask {
                action: a.into(),
                payload: "{}".into(),
                dedupe_key: None,
            })
            .collect(),
        report_bucket: report.then(|| "reports".into()),
        report_prefix: report.then(|| "out".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn worker_runs_tasks_and_writes_report() {
    let (db, store, _d1, _d2) = setup().await;
    let id = db
        .create_job(&spec(vec!["ReplaceTags", "Copy"], true), 1000)
        .await
        .unwrap();
    let e = env(3, 30);
    assert!(worker_once(&db, &store, &OkExec, e, 1000).await.unwrap());
    assert!(worker_once(&db, &store, &OkExec, e, 1001).await.unwrap());
    assert!(!worker_once(&db, &store, &OkExec, e, 1002).await.unwrap());
    let job = db.job_get(id).await.unwrap().unwrap();
    assert_eq!(job["state"], "done");
    assert_eq!(job["succeeded"], 2);
    assert_eq!(job["progress"], 2);

    let (info, bytes) = store
        .get_object("reports", &format!("out/{id}/results.csv"))
        .await
        .unwrap();
    assert_eq!(info.content_type.as_deref(), Some("text/csv"));
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("task_no,action,state,result_code,result_string")
    );
    assert!(
        lines
            .next()
            .unwrap()
            .starts_with("1,ReplaceTags,done,ReplaceTags-ok")
    );
    assert!(lines.next().unwrap().starts_with("2,Copy,done,Copy-ok"));
    assert_eq!(lines.next(), None);
}

#[tokio::test]
async fn worker_failure_reaches_permanent_and_job_error() {
    let (db, store, _d1, _d2) = setup().await;
    let id = db
        .create_job(&spec(vec!["Copy"], false), 1000)
        .await
        .unwrap();
    assert!(
        worker_once(&db, &store, &FailExec, env(1, 30), 1000)
            .await
            .unwrap()
    );
    let job = db.job_get(id).await.unwrap().unwrap();
    assert_eq!(job["state"], "error");
    assert_eq!(job["failed"], 1);
    let tasks = db.job_tasks(id, 0, 10).await.unwrap();
    assert_eq!(tasks[0]["state"], "failed");
    assert_eq!(tasks[0]["result_code"], "PermanentFailure");
    assert_eq!(tasks[0]["result_string"], "boom");
}

#[tokio::test]
async fn worker_timeout_is_failure() {
    let (db, store, _d1, _d2) = setup().await;
    let id = db
        .create_job(&spec(vec!["Copy"], false), 1000)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(
        worker_once(&db, &store, &SlowExec, env(1, 1), 1000)
            .await
            .unwrap()
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(4));
    let tasks = db.job_tasks(id, 0, 10).await.unwrap();
    assert_eq!(tasks[0]["state"], "failed");
    assert_eq!(tasks[0]["result_code"], "PermanentFailure");
}
