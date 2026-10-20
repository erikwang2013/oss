//! M8 S0：作业引擎（job_tasks 领取/重试/超时/报告）。
//!
//! 单节点 SQLite：`OSS_JOB_WORKERS` 个 worker 共享同一 WAL，`claim_task` 用
//! `BEGIN IMMEDIATE` 原子领取；多进程/多节点不支持（落档）。动作执行委托给
//! [`TaskExecutor`]——S3/S4b/S5 各自提供实现，S0 只有测试 executor。

use std::sync::Arc;

use crate::db::Db;
use xl_store::store::Store;

/// 动作执行器。`Ok(code)` = 成功；`Err(msg)` = 可重试失败（由引擎按上限转 PermanentFailure）。
#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(&self, action: &str, payload: &str) -> Result<String, String>;
}

/// S0 占位执行器：没有注册动作时任务按可重试失败处理（S3/S4b/S5 接入真实实现）。
pub struct NoopExecutor;

#[async_trait::async_trait]
impl TaskExecutor for NoopExecutor {
    async fn execute(&self, action: &str, _payload: &str) -> Result<String, String> {
        Err(format!("未注册的作业动作: {action}"))
    }
}

/// 引擎配置（env 解析一次）。
#[derive(Debug, Clone, Copy)]
pub struct JobEnv {
    pub workers: usize,
    pub interval_secs: u64,
    pub max_attempts: i64,
    pub timeout_secs: i64,
    pub tier_timeout_secs: i64,
}

impl JobEnv {
    pub fn from_env() -> Self {
        let get = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        Self {
            workers: get("OSS_JOB_WORKERS", 2) as usize,
            interval_secs: get("OSS_JOBS_INTERVAL_SECS", 2),
            max_attempts: get("OSS_JOB_MAX_ATTEMPTS", 3).max(1) as i64,
            timeout_secs: get("OSS_JOB_TASK_TIMEOUT_SECS", 900) as i64,
            tier_timeout_secs: get("OSS_TIER_TASK_TIMEOUT_SECS", 3600) as i64,
        }
    }
}

fn is_tier_action(action: &str) -> bool {
    matches!(action, "Transition" | "RestoreObject")
        || action.starts_with("Tier")
        || action.starts_with("Restore")
}

/// 启动 worker 池（`OSS_JOB_WORKERS=0` 关闭）。
pub fn spawn_workers(db: Arc<Db>, store: Arc<Store>, exec: Arc<dyn TaskExecutor>) {
    let env = JobEnv::from_env();
    if env.workers == 0 {
        tracing::info!("job worker 已关闭（OSS_JOB_WORKERS=0）");
        return;
    }
    let now = crate::db::now();
    let recover = db.clone();
    tokio::spawn(async move {
        // 崩溃遗留：running 超过 300s 或 deadline 已过 → 按一次失败回收
        match recover
            .recover_stale_tasks(now - 300, env.max_attempts)
            .await
        {
            Ok(n) if n > 0 => tracing::warn!(n, "job：启动回收崩溃遗留任务"),
            Err(e) => tracing::warn!(error = %e, "job：启动回收失败"),
            _ => {}
        }
        match recover.recover_expired_tasks(now, env.max_attempts).await {
            Ok(n) if n > 0 => tracing::warn!(n, "job：启动回收过期任务"),
            Err(e) => tracing::warn!(error = %e, "job：启动回收失败"),
            _ => {}
        }
    });
    for i in 0..env.workers {
        let db = db.clone();
        let store = store.clone();
        let exec = exec.clone();
        tokio::spawn(async move {
            let mut t =
                tokio::time::interval(std::time::Duration::from_secs(env.interval_secs.max(1)));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                let now = crate::db::now();
                match worker_once(&db, &store, exec.as_ref(), env, now).await {
                    Ok(true) => {} // 立刻尝试下一条
                    Ok(false) => {}
                    Err(e) => tracing::warn!(worker = i, error = %e, "job worker 本轮失败"),
                }
            }
        });
    }
    tracing::info!(workers = env.workers, "job worker 已启动");
}

/// 处理一条任务（测试直接调用）。返回是否领到任务。
pub async fn worker_once(
    db: &Db,
    store: &Store,
    exec: &dyn TaskExecutor,
    env: JobEnv,
    now: i64,
) -> Result<bool, String> {
    // 顺手回收超时任务（不需要每 tick 全表扫；这里与领取同频）
    db.recover_expired_tasks(now, env.max_attempts)
        .await
        .map_err(|e| e.to_string())?;
    // 领取时动作未知 → DB 截止时间取两类超时的较大值（硬上限）；
    // 领取后立即按动作类型收紧，让非 tier 任务超时后 900s（而非 3600s）即可回收。
    let deadline_secs = env.timeout_secs.max(env.tier_timeout_secs);
    let Some(task) = db
        .claim_task(now, deadline_secs)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(false);
    };
    let timeout = if is_tier_action(&task.action) {
        env.tier_timeout_secs
    } else {
        env.timeout_secs
    };
    // 收紧失败不致命：claim 已写下硬上限 deadline，任务仍会被过期回收。
    if let Err(e) = db
        .set_task_deadline(task.job_id, task.task_no, now + timeout)
        .await
    {
        tracing::warn!(job_id = task.job_id, task_no = task.task_no, error = %e,
            "job：收紧任务 deadline 失败（沿用 claim 硬上限）");
    }
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(timeout.max(1) as u64),
        exec.execute(&task.action, &task.payload_json),
    )
    .await;
    let (ok, code, msg) = match outcome {
        Ok(Ok(code)) => (true, code, None),
        Ok(Err(msg)) => (false, "InternalError".to_string(), Some(msg)),
        Err(_) => (
            false,
            "Timeout".to_string(),
            Some(format!("task exceeded {timeout}s")),
        ),
    };
    // 多 worker 共享 WAL 时 finish 偶发 SQLITE_BUSY：重试短退避。全部失败则上报错误，
    // 任务留 running 由过期回收兜底——绝不在 finish 未落库时继续领下一条。
    let mut finish_err = None;
    for attempt in 0..5 {
        match db
            .finish_task(
                task.job_id,
                task.task_no,
                ok,
                &code,
                msg.as_deref(),
                crate::db::now(),
                env.max_attempts,
            )
            .await
        {
            Ok(_) => {
                finish_err = None;
                break;
            }
            Err(e) => {
                finish_err = Some(e);
                if attempt < 4 {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }
    if let Some(e) = finish_err {
        tracing::error!(job_id = task.job_id, task_no = task.task_no, error = %e,
            "job：finish_task 重试 5 次仍失败，任务保持 running（等待超时回收）");
        return Err(format!(
            "finish_task 未落库（job {} task {}）: {e}",
            task.job_id, task.task_no
        ));
    }
    maybe_write_report(db, store, task.job_id).await?;
    Ok(true)
}

/// 作业终态且有 `report_prefix` 时写 `<prefix>/<job_id>/results.csv`（幂等覆盖）。
/// `kind=batch` 用 S3 报告列；其余作业保留引擎通用列。取消后也落一次报告（未执行任务不进报告）。
async fn maybe_write_report(db: &Db, store: &Store, job_id: i64) -> Result<(), String> {
    let Some(job) = db.job_get(job_id).await.map_err(|e| e.to_string())? else {
        return Ok(());
    };
    if !matches!(job["state"].as_str(), Some("done" | "error" | "cancelled")) {
        return Ok(());
    }
    let Some(bucket) = job["report_bucket"].as_str() else {
        return Ok(());
    };
    let Some(prefix) = job["report_prefix"].as_str() else {
        return Ok(());
    };
    let mut w = csv::Writer::from_writer(Vec::new());
    if job["kind"].as_str() == Some("batch") {
        w.write_record([
            "taskId",
            "bucket",
            "key",
            "versionId",
            "resultCode",
            "resultString",
            "startTime",
            "endTime",
        ])
        .map_err(|e| e.to_string())?;
        for r in db
            .job_report_rows(job_id)
            .await
            .map_err(|e| e.to_string())?
        {
            // 取消时未执行（从未 started）的任务不进报告
            let (Some(started), finished) = (r["started_at"].as_i64(), r["finished_at"].as_i64())
            else {
                continue;
            };
            let p: Option<crate::job_actions::BatchActionPayload> =
                serde_json::from_str(r["payload_json"].as_str().unwrap_or("")).ok();
            let (b, k, v) = p
                .map(|p| (p.bucket, p.key, p.version_id.unwrap_or_default()))
                .unwrap_or_default();
            let code = r["result_code"].as_str().unwrap_or("");
            let string = match r["result_string"].as_str() {
                Some(s) => s.to_string(),
                None if code == "Succeeded" => "Succeeded".to_string(),
                None => String::new(),
            };
            w.write_record([
                r["task_no"].to_string(),
                b,
                k,
                v,
                code.to_string(),
                string,
                iso8601_secs(started),
                finished.map(iso8601_secs).unwrap_or_default(),
            ])
            .map_err(|e| e.to_string())?;
        }
    } else {
        // 分页取全量任务（上限 10000，S3 创建时已限）
        let mut rows = Vec::new();
        let mut offset = 0i64;
        loop {
            let page = db
                .job_tasks(job_id, offset, 500)
                .await
                .map_err(|e| e.to_string())?;
            if page.is_empty() {
                break;
            }
            offset += page.len() as i64;
            rows.extend(page);
        }
        w.write_record(["task_no", "action", "state", "result_code", "result_string"])
            .map_err(|e| e.to_string())?;
        for r in &rows {
            w.write_record([
                r["task_no"].to_string(),
                r["action"].as_str().unwrap_or("").to_string(),
                r["state"].as_str().unwrap_or("").to_string(),
                r["result_code"].as_str().unwrap_or("").to_string(),
                r["result_string"].as_str().unwrap_or("").to_string(),
            ])
            .map_err(|e| e.to_string())?;
        }
    }
    let bytes = w.into_inner().map_err(|e| e.to_string())?;
    let prefix = prefix.trim_matches('/');
    let key = if prefix.is_empty() {
        format!("{job_id}/results.csv")
    } else {
        format!("{prefix}/{job_id}/results.csv")
    };
    let versioned = db
        .get_bucket_versioning(bucket)
        .await
        .map(|s| s == "Enabled")
        .unwrap_or(false);
    let written = store
        .put_object_stream_with_meta(
            bucket,
            &key,
            Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(bytes))
            })),
            Some("text/csv".to_string()),
            std::collections::BTreeMap::new(),
            None,
            versioned,
            None,
            None,
            None,
            None,
        )
        .await
        .map_err(|e| format!("报告写入失败: {e}"));
    if let Err(msg) = written {
        tracing::warn!(job_id, error = %msg, "job 报告写入失败");
        let _ = db.job_set_error(job_id, &msg, crate::db::now()).await;
    }
    Ok(())
}

/// ISO8601 UTC（秒精度；库内时间戳本身就是秒）。
fn iso8601_secs(t: i64) -> String {
    let dt =
        time::OffsetDateTime::from_unix_timestamp(t).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// 取消等状态变更后立即落一次报告（失败只记日志；下一次任务收敛会重写）。
pub(crate) async fn write_report_now(db: &Db, store: &Store, job_id: i64) {
    if let Err(e) = maybe_write_report(db, store, job_id).await {
        tracing::warn!(job_id, error = %e, "job 报告写入失败");
    }
}
