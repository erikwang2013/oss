//! Jobs 切片差分与源码审计：同一操作序列分别走 `Db`（ControlTxn 缝）与测试内
//! 旧 raw SQL，比较 jobs / job_tasks / schedule_lease / runstate_meta 全表；
//! 另测 dedupe 全回滚、fencing 零写入与生产源码不再直写这四表。

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::raft::FencingEpoch;
use sqlx::{Acquire, Row, SqliteConnection};
use xl_store::control_store::jobs::{JobTxn, ScheduleLeaseRow};

fn spec(kind: &str, bucket: &str, tasks: Vec<NewJobTask>, dedupe: Option<&str>) -> JobSpec {
    JobSpec {
        kind: kind.into(),
        bucket: bucket.into(),
        prefix: "p/".into(),
        requested_by: Some(7),
        priority: 5,
        parent_job_id: None,
        report_bucket: Some("rb".into()),
        report_prefix: Some("rp".into()),
        spec_json: Some("{}".into()),
        tasks: tasks
            .into_iter()
            .enumerate()
            .map(|(i, mut t)| {
                if i == 0 && t.dedupe_key.is_none() {
                    t.dedupe_key = dedupe.map(str::to_string);
                }
                t
            })
            .collect(),
    }
}

fn task(action: &str, payload: &str) -> NewJobTask {
    NewJobTask {
        action: action.into(),
        payload: payload.into(),
        dedupe_key: None,
    }
}

/// 旧退避公式（与 `Db::retry_backoff_secs` 逐位相同；后者是 crate-private）。
fn legacy_backoff(attempt: i64, now: i64, job_id: i64, task_no: i64) -> i64 {
    let base = 10i64
        .saturating_mul(2i64.saturating_pow(attempt.min(30) as u32))
        .min(300);
    let mut h = (job_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (task_no as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        ^ (now as u64).wrapping_mul(0x1656_67B1_9E37_79F9)
        ^ (attempt as u64);
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    h ^= h >> 33;
    base + (h % 10) as i64
}

// ---------- 旧 SQL 参考实现（仅测试） ----------

async fn legacy_create_job(conn: &mut SqliteConnection, spec: &JobSpec, now: i64) -> i64 {
    let mut tx = conn.begin().await.unwrap();
    let id = sqlx::query(
        "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
         parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
         VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&spec.kind)
    .bind(&spec.bucket)
    .bind(&spec.prefix)
    .bind(spec.tasks.len() as i64)
    .bind(spec.priority)
    .bind(spec.requested_by)
    .bind(spec.parent_job_id)
    .bind(&spec.report_bucket)
    .bind(&spec.report_prefix)
    .bind(&spec.spec_json)
    .bind(now)
    .bind(now)
    .execute(&mut *tx)
    .await
    .unwrap()
    .last_insert_rowid();
    for (i, t) in spec.tasks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
             VALUES (?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(id)
        .bind(i as i64 + 1)
        .bind(&t.action)
        .bind(&t.payload)
        .bind(now)
        .bind(&t.dedupe_key)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    id
}

async fn legacy_create_job_dedup(
    mut tx: sqlx::Transaction<'_, sqlx::Sqlite>,
    spec: &JobSpec,
    now: i64,
) -> Option<i64> {
    let id = sqlx::query(
        "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
         parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
         VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&spec.kind)
    .bind(&spec.bucket)
    .bind(&spec.prefix)
    .bind(spec.tasks.len() as i64)
    .bind(spec.priority)
    .bind(spec.requested_by)
    .bind(spec.parent_job_id)
    .bind(&spec.report_bucket)
    .bind(&spec.report_prefix)
    .bind(&spec.spec_json)
    .bind(now)
    .bind(now)
    .execute(&mut *tx)
    .await
    .unwrap()
    .last_insert_rowid();
    let mut inserted = 0i64;
    for (i, t) in spec.tasks.iter().enumerate() {
        inserted += sqlx::query(
            "INSERT OR IGNORE INTO job_tasks \
             (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
             VALUES (?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(id)
        .bind(i as i64 + 1)
        .bind(&t.action)
        .bind(&t.payload)
        .bind(now)
        .bind(&t.dedupe_key)
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected() as i64;
    }
    if inserted == 0 {
        tx.rollback().await.unwrap();
        return None;
    }
    if inserted != spec.tasks.len() as i64 {
        sqlx::query("UPDATE jobs SET total = ? WHERE id = ?")
            .bind(inserted)
            .bind(id)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    Some(id)
}

async fn legacy_claim(
    conn: &mut SqliteConnection,
    now: i64,
    timeout_secs: i64,
) -> Option<(i64, i64, String, String, i64)> {
    sqlx::raw_sql("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE jobs SET state = 'running', started_at = COALESCE(started_at, ?), updated_at = ? \
         WHERE state = 'queued' AND EXISTS (SELECT 1 FROM job_tasks t \
         WHERE t.job_id = jobs.id AND t.state IN ('queued','running'))",
    )
    .bind(now)
    .bind(now)
    .execute(&mut *conn)
    .await
    .unwrap();
    let row = sqlx::query(
        "SELECT t.job_id, t.task_no, t.action, t.payload_json, t.attempt \
         FROM job_tasks t JOIN jobs j ON j.id = t.job_id \
         WHERE t.state = 'queued' AND t.next_at <= ? AND j.state = 'running' \
           AND t.fence_term <= 0 \
         ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT 1",
    )
    .bind(now)
    .fetch_optional(&mut *conn)
    .await
    .unwrap();
    let out = match row {
        Some(r) => {
            let (job_id, task_no): (i64, i64) = (r.get("job_id"), r.get("task_no"));
            sqlx::query(
                "UPDATE job_tasks SET state = 'running', started_at = ?, deadline_at = ?, \
                 fence_term = 0, fence_leader = 0 \
                 WHERE job_id = ? AND task_no = ? AND state = 'queued' AND fence_term <= 0",
            )
            .bind(now)
            .bind(now + timeout_secs)
            .bind(job_id)
            .bind(task_no)
            .execute(&mut *conn)
            .await
            .unwrap();
            Some((
                job_id,
                task_no,
                r.get("action"),
                r.get("payload_json"),
                r.get("attempt"),
            ))
        }
        None => None,
    };
    sqlx::raw_sql("COMMIT").execute(&mut *conn).await.unwrap();
    out
}

/// 旧 finish_task：返回新状态并写库。
#[allow(clippy::too_many_arguments)]
async fn legacy_finish_task(
    conn: &mut SqliteConnection,
    job_id: i64,
    task_no: i64,
    ok: bool,
    code: &str,
    message: Option<&str>,
    now: i64,
    max_attempts: i64,
) -> &'static str {
    let mut tx = conn.begin().await.unwrap();
    let r = sqlx::query(
        "SELECT state, attempt, fence_term FROM job_tasks WHERE job_id = ? AND task_no = ?",
    )
    .bind(job_id)
    .bind(task_no)
    .fetch_optional(&mut *tx)
    .await
    .unwrap()
    .expect("task exists");
    assert_eq!(r.get::<String, _>("state"), "running");
    let attempt: i64 = r.get("attempt");
    if ok {
        sqlx::query(
            "UPDATE job_tasks SET state = 'done', result_code = ?, result_string = ?, \
             finished_at = ?, dedupe_key = NULL \
             WHERE job_id = ? AND task_no = ? AND fence_term <= 0",
        )
        .bind(code)
        .bind(message)
        .bind(now)
        .bind(job_id)
        .bind(task_no)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE jobs SET succeeded = succeeded + 1, progress = progress + 1, updated_at = ? \
             WHERE id = ?",
        )
        .bind(now)
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    } else if attempt + 1 < max_attempts {
        let backoff = legacy_backoff(attempt, now, job_id, task_no);
        sqlx::query(
            "UPDATE job_tasks SET state = 'queued', attempt = attempt + 1, next_at = ?, \
             result_code = ?, result_string = ?, started_at = NULL, deadline_at = NULL \
             WHERE job_id = ? AND task_no = ? AND fence_term <= 0",
        )
        .bind(now + backoff)
        .bind(code)
        .bind(message)
        .bind(job_id)
        .bind(task_no)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("UPDATE jobs SET updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .unwrap();
    } else {
        sqlx::query(
            "UPDATE job_tasks SET state = 'failed', attempt = attempt + 1, \
             result_code = 'PermanentFailure', result_string = ?, finished_at = ?, \
             dedupe_key = NULL WHERE job_id = ? AND task_no = ? AND fence_term <= 0",
        )
        .bind(message)
        .bind(now)
        .bind(job_id)
        .bind(task_no)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE jobs SET failed = failed + 1, progress = progress + 1, updated_at = ? \
             WHERE id = ?",
        )
        .bind(now)
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE jobs SET state = CASE WHEN failed > 0 THEN 'error' ELSE 'done' END, \
         finished_at = ?, updated_at = ? \
         WHERE id = ? AND state IN ('running','preempted') AND NOT EXISTS \
         (SELECT 1 FROM job_tasks WHERE job_id = ? AND state IN ('queued','running'))",
    )
    .bind(now)
    .bind(now)
    .bind(job_id)
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    if ok {
        "done"
    } else if attempt + 1 < max_attempts {
        "queued"
    } else {
        "failed"
    }
}

async fn legacy_cancel(conn: &mut SqliteConnection, id: i64, now: i64) -> bool {
    let mut tx = conn.begin().await.unwrap();
    let n = sqlx::query(
        "UPDATE jobs SET state = 'cancelled', updated_at = ? \
         WHERE id = ? AND state IN ('queued','running','paused','preempted')",
    )
    .bind(now)
    .bind(id)
    .execute(&mut *tx)
    .await
    .unwrap()
    .rows_affected();
    if n > 0 {
        sqlx::query(
            "UPDATE job_tasks SET state = 'cancelled', finished_at = ? \
             WHERE job_id = ? AND state = 'queued'",
        )
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    n > 0
}

// ---------- 全表 dump ----------

async fn dump(db: &Db) -> Vec<Vec<String>> {
    let jobs: Vec<String> = sqlx::query_scalar(
        "SELECT json_array(id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
         created_at, updated_at, spec_json, progress, total, succeeded, failed, report_bucket, \
         report_prefix, priority, requested_by, parent_job_id, started_at, finished_at) \
         FROM jobs ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let tasks: Vec<String> = sqlx::query_scalar(
        "SELECT json_array(job_id, task_no, action, payload_json, state, attempt, next_at, \
         dedupe_key, deadline_at, result_code, result_string, started_at, finished_at, \
         fence_term, fence_leader) FROM job_tasks ORDER BY job_id, task_no",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let leases: Vec<String> = sqlx::query_scalar(
        "SELECT json_array(singleton, term, node, expires_at, acquired_at) FROM schedule_lease",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let meta: Vec<String> =
        sqlx::query_scalar("SELECT json_array(key, value) FROM runstate_meta ORDER BY key")
            .fetch_all(db.pool())
            .await
            .unwrap();
    vec![jobs, tasks, leases, meta]
}

#[tokio::test]
async fn job_engine_differential_against_legacy_sql() {
    let a = Db::connect("sqlite::memory:").await.unwrap();
    let b = Db::connect("sqlite::memory:").await.unwrap();
    let s1 = spec(
        "lifecycle",
        "bucket-a",
        vec![task("scan", r#"{"k":1}"#), task("delete", "{}")],
        Some("dup"),
    );

    let id1 = a.create_job(&s1, 1000).await.unwrap();
    {
        let mut conn = b.pool().acquire().await.unwrap();
        assert_eq!(legacy_create_job(&mut conn, &s1, 1000).await, id1);
    }

    let c1 = a.claim_task(1005, 30).await.unwrap().unwrap();
    let l1 = {
        let mut conn = b.pool().acquire().await.unwrap();
        legacy_claim(&mut conn, 1005, 30).await.unwrap()
    };
    assert_eq!(
        (
            c1.job_id,
            c1.task_no,
            c1.action.clone(),
            c1.payload_json.clone(),
            c1.attempt
        ),
        l1
    );

    assert_eq!(
        a.finish_task(c1.job_id, c1.task_no, true, "OK", None, 1010, 3)
            .await
            .unwrap(),
        "done"
    );
    {
        let mut conn = b.pool().acquire().await.unwrap();
        assert_eq!(
            legacy_finish_task(&mut conn, c1.job_id, c1.task_no, true, "OK", None, 1010, 3).await,
            "done"
        );
    }

    let c2 = a.claim_task(1011, 30).await.unwrap().unwrap();
    let l2 = {
        let mut conn = b.pool().acquire().await.unwrap();
        legacy_claim(&mut conn, 1011, 30).await.unwrap()
    };
    assert_eq!((c2.job_id, c2.task_no), (l2.0, l2.1));
    assert_eq!(
        a.finish_task(
            c2.job_id,
            c2.task_no,
            false,
            "Timeout",
            Some("boom"),
            1020,
            3
        )
        .await
        .unwrap(),
        "queued"
    );
    {
        let mut conn = b.pool().acquire().await.unwrap();
        assert_eq!(
            legacy_finish_task(
                &mut conn,
                c2.job_id,
                c2.task_no,
                false,
                "Timeout",
                Some("boom"),
                1020,
                3
            )
            .await,
            "queued"
        );
    }

    // 保留清理用的终态作业。
    let id2 = a
        .job_start("retention", "bucket-a", "", 1100)
        .await
        .unwrap();
    a.job_finish(id2, "done", 1, 0, 0, None, 1101)
        .await
        .unwrap();
    {
        let mut conn = b.pool().acquire().await.unwrap();
        let id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, created_at, updated_at) \
             VALUES ('retention', 'bucket-a', '', 'running', 1100, 1100)",
        )
        .execute(&mut *conn)
        .await
        .unwrap()
        .last_insert_rowid();
        assert_eq!(id, id2);
        sqlx::query(
            "UPDATE jobs SET state = 'done', scanned = 1, deleted = 0, skipped = 0, updated_at = ? \
             WHERE id = ?",
        )
        .bind(1101)
        .bind(id)
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    // dedupe 全命中：事务回滚，不产生空作业。
    let sl = spec(
        "lifecycle",
        "bucket-a",
        vec![task("scan", "{}")],
        Some("lock"),
    );
    let id3 = a.create_job(&sl, 1200).await.unwrap();
    let dup = spec(
        "lifecycle",
        "bucket-a",
        vec![task("scan", "{}")],
        Some("lock"),
    );
    assert_eq!(a.create_job_dedup(&dup, 1201).await.unwrap(), None);
    {
        let tx = b.pool().begin().await.unwrap();
        assert_eq!(legacy_create_job_dedup(tx, &sl, 1200).await, Some(id3));
        let tx = b.pool().begin().await.unwrap();
        assert_eq!(legacy_create_job_dedup(tx, &dup, 1201).await, None);
    }

    // 状态操作与进度。
    assert!(a.job_set_state(id3, "cancel", 1300).await.unwrap());
    assert!(a.job_set_progress(id1, 1, 1301).await.is_ok());
    {
        let mut conn = b.pool().acquire().await.unwrap();
        assert!(legacy_cancel(&mut conn, id3, 1300).await);
        sqlx::query("UPDATE jobs SET progress = 1, updated_at = 1301 WHERE id = ?")
            .bind(id1)
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    // 保留清理：只删终态行，两库返回相同数量。
    let terminal = a.terminal_jobs_before(5000, 10).await.unwrap();
    assert_eq!(terminal, vec![id2, id3]);
    assert_eq!(a.purge_terminal_jobs_local(&terminal).await.unwrap(), 2);
    {
        let mut tx = b.pool().begin().await.unwrap();
        let n = sqlx::query(
            "DELETE FROM job_tasks WHERE job_id IN (SELECT id FROM jobs \
             WHERE state IN ('done','error','cancelled'))",
        )
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        assert_eq!(n, 1);
        let n = sqlx::query(
            "DELETE FROM jobs WHERE state IN ('done','error','cancelled') \
             AND id IN (?, ?)",
        )
        .bind(id2)
        .bind(id3)
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        assert_eq!(n, 2);
        tx.commit().await.unwrap();
    }

    // 租约与 next_run_id 分配器。
    let lease = ScheduleLeaseRow {
        term: 3,
        node: 2,
        expires_at: 2000,
        acquired_at: 1500,
    };
    let mut tx = a.pool().begin().await.unwrap();
    SqliteControlTxn::new(&mut tx)
        .schedule_lease_upsert(&lease)
        .await
        .unwrap();
    SqliteControlTxn::new(&mut tx)
        .runstate_meta_next_run_id(42)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    {
        let mut conn = b.pool().acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        sqlx::query(
            "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
             VALUES (1, ?, ?, ?, ?) \
             ON CONFLICT(singleton) DO UPDATE SET term = excluded.term, node = excluded.node, \
              expires_at = excluded.expires_at, acquired_at = excluded.acquired_at",
        )
        .bind(lease.term)
        .bind(lease.node)
        .bind(lease.expires_at)
        .bind(lease.acquired_at)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runstate_meta (key, value) VALUES ('next_run_id', ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value \
             WHERE CAST(runstate_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
        )
        .bind(42i64.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(dump(&a).await, dump(&b).await);
}

#[tokio::test]
async fn stale_fence_finish_writes_nothing_and_idempotent_finish_converges() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let s = spec("lifecycle", "b", vec![task("scan", "{}")], None);
    let id = db.create_job(&s, 100).await.unwrap();
    let claim = db.claim_task(101, 30).await.unwrap().unwrap();
    // 模拟新 leader 接管：行上的 fence 前进到 term 2。
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE job_tasks SET fence_term = 2, fence_leader = 2 \
         WHERE job_id = ? AND task_no = ?",
    )
    .bind(claim.job_id)
    .bind(claim.task_no)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let before = dump(&db).await;

    // 更高 term 接管后，旧持有者的收尾必须零写入。
    assert_eq!(
        db.finish_task_fenced(
            claim.job_id,
            claim.task_no,
            true,
            "OK",
            None,
            102,
            3,
            FencingEpoch { term: 1, leader: 1 },
        )
        .await
        .unwrap(),
        "fenced"
    );
    assert_eq!(before, dump(&db).await);

    // 当前 term 正常收尾 → 任务 done、作业收敛 done；重复收尾是 no-op。
    assert_eq!(
        db.finish_task_fenced(
            claim.job_id,
            claim.task_no,
            true,
            "OK",
            None,
            103,
            3,
            FencingEpoch { term: 2, leader: 2 },
        )
        .await
        .unwrap(),
        "done"
    );
    let after = dump(&db).await;
    assert_eq!(
        db.finish_task_fenced(
            claim.job_id,
            claim.task_no,
            true,
            "OK",
            None,
            104,
            3,
            FencingEpoch { term: 2, leader: 2 },
        )
        .await
        .unwrap(),
        "not-running"
    );
    assert_eq!(after, dump(&db).await);
    assert_eq!(db.job_state(id).await.unwrap().as_deref(), Some("done"));
}

// ---------- 源码审计 ----------

fn strip_test_modules(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() == "#[cfg(test)]" {
            let mut j = i + 1;
            while j < lines.len() && lines[j].trim().is_empty() {
                j += 1;
            }
            if j < lines.len()
                && lines[j].trim_start().starts_with("mod ")
                && lines[j].contains('{')
            {
                while j < lines.len() {
                    let close = lines[j].starts_with('}');
                    j += 1;
                    if close {
                        break;
                    }
                }
                i = j;
                continue;
            }
        }
        out.push_str(lines[i]);
        out.push('\n');
        i += 1;
    }
    out
}

#[test]
fn production_sources_have_no_raw_job_sql() {
    const ALLOWED: &[&str] = &[
        "jobs.rs",
        "jobs_write.rs",
        "jobs_snapshot.rs",
        // 兼容守卫只读跨表悬空引用（startup-only，不参与运行态写入）。
        "compat.rs",
    ];
    const TABLES: &[&str] = &["jobs", "job_tasks", "schedule_lease", "runstate_meta"];

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![src];
    let mut findings = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if ALLOWED.contains(&name.as_str()) {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            for (idx, line) in strip_test_modules(&text).lines().enumerate() {
                let lower = line.to_ascii_lowercase();
                let hit = TABLES.iter().any(|t| {
                    lower.contains(&format!("from {t} "))
                        || lower.contains(&format!("into {t} "))
                        || lower.contains(&format!("into {t}("))
                        || lower.contains(&format!("update {t} "))
                        || lower.contains(&format!("delete from {t} "))
                });
                if hit {
                    findings.push(format!("{name}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "生产源码仍直写 jobs 控制表（应经 ControlTxn 或更新 D7 白名单）：{findings:#?}"
    );
}
