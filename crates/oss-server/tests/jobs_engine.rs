//! M8 S0：作业引擎——领取/执行/收敛/报告/失败上限/超时。

use std::sync::{Arc, Mutex};

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

// ——— 协作式抢占（动作边界） ———

/// 可挂起的执行器：记录执行载荷（= 动作标识），执行中等待 gate 放行。
/// 用来把任务稳定停在「已领取、动作在执行中」的边界状态下断言抢占。
struct GatedExec {
    started: tokio::sync::mpsc::UnboundedSender<String>,
    gate: Arc<tokio::sync::Semaphore>,
    calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TaskExecutor for GatedExec {
    async fn execute(&self, _action: &str, payload: &str) -> Result<String, String> {
        self.calls.lock().unwrap().push(payload.to_string());
        let _ = self.started.send(payload.to_string());
        let permit = self.gate.acquire().await.map_err(|e| e.to_string())?;
        permit.forget();
        Ok("ok".into())
    }
}

/// 快速记录型执行器（抢占后的顺序/去重断言）。
#[derive(Default)]
struct RecordingExec {
    calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TaskExecutor for RecordingExec {
    async fn execute(&self, _action: &str, payload: &str) -> Result<String, String> {
        self.calls.lock().unwrap().push(payload.to_string());
        Ok("ok".into())
    }
}

fn spec_with(priority: i64, tasks: &[(&str, &str)]) -> JobSpec {
    JobSpec {
        kind: "generic".into(),
        bucket: "reports".into(),
        requested_by: Some(7),
        priority,
        tasks: tasks
            .iter()
            .map(|(a, p)| NewJobTask {
                action: (*a).into(),
                payload: (*p).into(),
                dedupe_key: None,
            })
            .collect(),
        ..Default::default()
    }
}

fn env_workers(workers: usize) -> JobEnv {
    JobEnv {
        workers,
        interval_secs: 1,
        max_attempts: 3,
        timeout_secs: 30,
        tier_timeout_secs: 30,
    }
}

/// 容量满 + 高优先级就绪任务排队 → 低优先级 running 作业在**动作边界**被抢占：
/// 在途动作完整落账（进度保留），未领取任务保持排队；高优先级先执行，之后释放恢复；
/// 已完成的动作绝不重跑（每个载荷恰好执行一次）。
#[tokio::test]
async fn preemption_orders_priority_and_never_duplicates_completed_actions() {
    let (db, store, _d1, _d2) = setup().await;
    let db = Arc::new(db);
    let low = db
        .create_job(
            &spec_with(1, &[("Low", "L1"), ("Low", "L2"), ("Low", "L3")]),
            1000,
        )
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let gated = Arc::new(GatedExec {
        started: tx,
        gate: gate.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let e2 = env_workers(2);
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (db2, store2, g2) = (db.clone(), store.clone(), gated.clone());
        handles.push(tokio::spawn(async move {
            worker_once(&db2, &store2, g2.as_ref(), e2, 1000).await
        }));
    }
    let mut started = Vec::new();
    for _ in 0..2 {
        started.push(
            tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
                .await
                .expect("worker 未在期限内领到任务")
                .unwrap(),
        );
    }
    started.sort();
    assert_eq!(started, ["L1", "L2"], "两条低优先级动作在执行中");
    assert_eq!(db.job_get(low).await.unwrap().unwrap()["state"], "running");

    // 容量未满（running=2 < capacity=3）不抢占。
    assert_eq!(db.preempt_lower_priority(3, 1000).await.unwrap(), 0);
    // 高优先级作业排队后，容量满（2）→ 抢占低优先级 running 作业。
    let high = db
        .create_job(&spec_with(9, &[("High", "H1")]), 1001)
        .await
        .unwrap();
    assert_eq!(db.preempt_lower_priority(2, 1001).await.unwrap(), 1);
    assert_eq!(
        db.job_get(low).await.unwrap().unwrap()["state"],
        "preempted"
    );
    assert_eq!(
        db.job_get(high).await.unwrap().unwrap()["state"],
        "queued",
        "抢占不打断/不提前执行高优先级任务"
    );
    // 高优先级就绪任务仍在排队 → 不得释放被抢占作业。
    assert_eq!(db.release_preempted(1001).await.unwrap(), 0);
    assert_eq!(
        db.job_get(low).await.unwrap().unwrap()["state"],
        "preempted"
    );

    // 放行在途动作：它们按安全边界正常完成（进度 2/3），未领取的 L3 仍在排队。
    gate.add_permits(2);
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let j = db.job_get(low).await.unwrap().unwrap();
    assert_eq!(j["state"], "preempted");
    assert_eq!(j["progress"], 2);
    assert_eq!(j["succeeded"], 2);
    let tasks = db.job_tasks(low, 0, 10).await.unwrap();
    assert_eq!(tasks[0]["state"], "done");
    assert_eq!(tasks[1]["state"], "done");
    assert_eq!(tasks[2]["state"], "queued");

    // 恢复：高优先级先执行；队列清空后释放低优先级作业并完成剩余任务。
    let rec = RecordingExec::default();
    let mut now = 2000;
    let mut guard = 0;
    while worker_once(&db, &store, &rec, e2, now).await.unwrap() {
        now += 1;
        guard += 1;
        assert!(guard < 20, "队列未收敛");
    }
    let after = rec.calls.lock().unwrap().clone();
    assert_eq!(
        after,
        ["H1", "L3"],
        "高优先级必须先于被抢占作业恢复；已完成动作不得重跑"
    );
    let mut all = gated.calls.lock().unwrap().clone();
    all.extend(after);
    all.sort();
    assert_eq!(
        all,
        ["H1", "L1", "L2", "L3"],
        "每个任务恰好执行一次（无重复副作用）"
    );
    assert_eq!(db.job_get(low).await.unwrap().unwrap()["state"], "done");
    assert_eq!(db.job_get(high).await.unwrap().unwrap()["state"], "done");
    assert_eq!(db.job_get(low).await.unwrap().unwrap()["succeeded"], 3);
}

/// 同优先级不互相抢占；没有更高优先级就绪工作时作业连续执行、不进入 preempted。
#[tokio::test]
async fn no_preemption_for_equal_priority_or_idle_capacity() {
    let (db, store, _d1, _d2) = setup().await;
    let low = db
        .create_job(&spec_with(5, &[("Low", "L1"), ("Low", "L2")]), 1000)
        .await
        .unwrap();
    // 两条在途（容量满），但队列里只有同优先级作业。
    assert_eq!(db.claim_task(1000, 900).await.unwrap().unwrap().job_id, low);
    assert_eq!(db.claim_task(1000, 900).await.unwrap().unwrap().job_id, low);
    let equal = db
        .create_job(&spec_with(5, &[("Eq", "E1")]), 1001)
        .await
        .unwrap();
    assert_eq!(db.preempt_lower_priority(2, 1001).await.unwrap(), 0);
    assert_eq!(
        db.job_get(low).await.unwrap().unwrap()["state"],
        "running",
        "严格更高优先级才抢占"
    );

    // 正常按动作边界完成（无抢占介入）。
    for (task_no, at) in [(1, 1002), (2, 1003)] {
        db.finish_task(low, task_no, true, "Succeeded", None, at, 3)
            .await
            .unwrap();
    }
    assert_eq!(db.job_get(low).await.unwrap().unwrap()["state"], "done");
    // 完成后同优先级作业照常领取并执行完毕（无抢占介入的常规路径）。
    assert!(
        worker_once(&db, &store, &OkExec, env_workers(1), 1004)
            .await
            .unwrap()
    );
    assert_eq!(db.job_get(equal).await.unwrap().unwrap()["state"], "done");
    assert!(
        !worker_once(&db, &store, &OkExec, env_workers(1), 1005)
            .await
            .unwrap()
    );
}

/// 已领取但未执行的任务在抢占边界回退：状态回 queued、attempt 不增（不消耗重试）、
/// start/deadline 清空；重复回退幂等。
#[tokio::test]
async fn preempted_claim_requeues_without_consuming_attempt() {
    let (db, _store, _d1, _d2) = setup().await;
    // 低优先级作业留一条排队任务，才有可恢复的剩余工作。
    let low = db
        .create_job(&spec_with(1, &[("Low", "L1"), ("Low", "L2")]), 1000)
        .await
        .unwrap();
    let t = db.claim_task(1000, 900).await.unwrap().unwrap();
    assert_eq!((t.job_id, t.task_no), (low, 1));
    let _high = db
        .create_job(&spec_with(9, &[("High", "H1")]), 1001)
        .await
        .unwrap();
    assert_eq!(db.preempt_lower_priority(1, 1001).await.unwrap(), 1);
    assert!(db.requeue_task(low, 1, 1001).await.unwrap());
    let tasks = db.job_tasks(low, 0, 10).await.unwrap();
    assert_eq!(tasks[0]["state"], "queued");
    assert_eq!(tasks[0]["attempt"], 0, "未执行不得消耗重试");
    assert!(tasks[0]["started_at"].is_null());
    assert!(
        !db.requeue_task(low, 1, 1002).await.unwrap(),
        "重复回退幂等"
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

    // M2：心跳续命的长作业不被误回收；心跳过期后才回收。
    let hb = db
        .job_start("lifecycle", "reports", "", 2000)
        .await
        .unwrap();
    db.job_heartbeat(hb, 5000).await.unwrap();
    assert_eq!(
        db.recover_orphan_jobs(3000, 6000).await.unwrap(),
        0,
        "updated_at 在截止之后：仍在跑"
    );
    assert_eq!(db.recover_orphan_jobs(5000, 7000).await.unwrap(), 1);
    let h = db.job_get(hb).await.unwrap().unwrap();
    assert_eq!(h["state"], "error");
}
