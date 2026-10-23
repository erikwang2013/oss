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
    db.insert_bucket("reports", None).await.unwrap();
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

/// 领取后收紧 deadline；只改 running 行（终态任务不被拉回）。
#[tokio::test]
async fn set_task_deadline_updates_running_row_only() {
    let (db, _store, _d1, _d2) = setup().await;
    let id = db
        .create_job(&spec(vec!["Copy"], false), 1000)
        .await
        .unwrap();
    let task = db.claim_task(1000, 3600).await.unwrap().unwrap();
    assert_eq!((task.job_id, task.task_no), (id, 1));
    db.set_task_deadline(id, 1, 1900).await.unwrap();
    let deadline: i64 =
        sqlx::query_scalar("SELECT deadline_at FROM job_tasks WHERE job_id = ? AND task_no = 1")
            .bind(id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(deadline, 1900);

    db.finish_task(id, 1, true, "OK", None, 1001, 3)
        .await
        .unwrap();
    db.set_task_deadline(id, 1, 9999).await.unwrap();
    let deadline: i64 =
        sqlx::query_scalar("SELECT deadline_at FROM job_tasks WHERE job_id = ? AND task_no = 1")
            .bind(id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(deadline, 1900, "终态任务不应再改 deadline");
}

/// worker 领到任务后按动作类型收紧 claim 写下的硬上限 deadline。
#[tokio::test]
async fn worker_tightens_deadline_by_action() {
    let (db, store, _d1, _d2) = setup().await;
    let e = JobEnv {
        workers: 1,
        interval_secs: 1,
        max_attempts: 3,
        timeout_secs: 30,
        tier_timeout_secs: 3600,
    };
    let id = db
        .create_job(&spec(vec!["Copy"], false), 1000)
        .await
        .unwrap();
    assert!(worker_once(&db, &store, &OkExec, e, 1000).await.unwrap());
    let deadline: i64 =
        sqlx::query_scalar("SELECT deadline_at FROM job_tasks WHERE job_id = ? AND task_no = 1")
            .bind(id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(deadline, 1030, "非 tier 动作按 timeout_secs 收紧");

    let id = db
        .create_job(&spec(vec!["Transition"], false), 2000)
        .await
        .unwrap();
    assert!(worker_once(&db, &store, &OkExec, e, 2000).await.unwrap());
    let deadline: i64 =
        sqlx::query_scalar("SELECT deadline_at FROM job_tasks WHERE job_id = ? AND task_no = 1")
            .bind(id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(deadline, 5600, "tier 动作按 tier_timeout_secs");
}

/// I5：报告对象必须套报告桶默认加密（AES256 → SSE-S3），且可解密读回。
#[tokio::test]
async fn report_applies_bucket_default_encryption() {
    let (db, store, _d1, store_dir) = setup().await;
    store
        .init_encryption(Some(store_dir.path().join("master.key")), None)
        .await
        .unwrap();
    db.set_bucket_default_encryption("reports", "AES256")
        .await
        .unwrap();
    let id = db
        .create_job(&spec(vec!["Copy"], true), 1000)
        .await
        .unwrap();
    assert!(
        worker_once(&db, &store, &OkExec, env(3, 30), 1000)
            .await
            .unwrap()
    );

    let key = format!("out/{id}/results.csv");
    let info = store.head_object("reports", &key).await.unwrap();
    let rec = info.encryption.expect("报告应套用桶默认加密");
    assert_eq!(rec.key_id, "file", "AES256 → SSE-S3（file provider）");
    let (_, body) = store.get_object("reports", &key).await.unwrap();
    assert!(
        String::from_utf8(body.to_vec())
            .unwrap()
            .contains("Copy-ok")
    );
}

/// 4.11：崩溃遗留的「无任务 running」行按截止时间回收；有任务行的作业不受影响。
#[tokio::test]
async fn recover_orphan_jobs_marks_crash_left_running_rows() {
    let (db, _store, _d1, _d2) = setup().await;
    let orphan = db
        .job_start("lifecycle", "reports", "", 1000)
        .await
        .unwrap();
    let live = db
        .create_job(&spec(vec!["Copy"], false), 1000)
        .await
        .unwrap();
    // 领取一次任务，让 live 成为真正 running 且有任务行的作业。
    assert!(db.claim_task(1000, 3600).await.unwrap().is_some());

    // 截止时间早于创建：都不回收
    assert_eq!(db.recover_orphan_jobs(500, 1000).await.unwrap(), 0);
    // 只有无任务行的 orphan 被标记 error
    assert_eq!(db.recover_orphan_jobs(1000, 2000).await.unwrap(), 1);
    let o = db.job_get(orphan).await.unwrap().unwrap();
    assert_eq!(o["state"], "error");
    assert_eq!(o["finished_at"], 2000);
    assert!(
        o["error"].as_str().unwrap_or("").contains("自动回收"),
        "{o:?}"
    );
    let l = db.job_get(live).await.unwrap().unwrap();
    assert_eq!(l["state"], "running", "有任务行的作业不归本回收器");
    // 幂等：再跑不重复计数
    assert_eq!(db.recover_orphan_jobs(1000, 3000).await.unwrap(), 0);
}
