//! M8-2 P1-A：feature ≤3 → feature 4 运行态激活边界（启动守卫）。
//!
//! feature ≤3 的 jobs/job_tasks 是节点本地行，不在复制日志中；以 feature_level=4
//! 启动前必须为空（或已安装 OSSCTL3 快照/cutover 留下激活标记），否则拒绝启动并
//! 给出排空/清空/重灌的运维步骤。真实 `AppState::load`，不再走 apply fail-stop。
//! feature 3 本身仍是本地作业语义，非空表不得误拒。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::JobSpec;
use oss_server::state::AppState;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn write_cfg(dir: &std::path::Path, feature_level: u8) -> std::path::PathBuf {
    let path = dir.join("oss.toml");
    let text = format!(
        r#"
listen = "127.0.0.1:0"
data_dir = "{data}"
staging_dir = "{staging}"
database_url = "sqlite://{db}?mode=rwc"
jwt_secret = "0123456789abcdef0123456789abcdef"

[site]
name = "m8jobs-activation"
region = "us-east-1"

[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:19001"
cluster_token = "m8jobs-activation-token"
db_path = "{raft_db}"
feature_level = {feature_level}
election_timeout_ms = 500
heartbeat_interval_ms = 100
write_timeout_ms = 5000
"#,
        data = dir.join("data").display(),
        staging = dir.join("staging").display(),
        db = dir.join("control.db").display(),
        raft_db = dir.join("raft.db").display(),
    );
    std::fs::write(&path, text).unwrap();
    path
}

async fn stop(state: AppState) {
    if let Some(raft) = &state.raft {
        raft.shutdown().await;
    }
    drop(state);
}

/// 旧本地作业（feature 2/3 建过）→ 切 feature_level=4 启动必须被拒（文案含计数与
/// 运维步骤）；feature 3 启动本身不得误拒。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_local_jobs_block_feature4_activation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), 2);
    let state = AppState::load(&cfg).await.unwrap();
    assert!(!state.jobs.replicated(), "feature 2 must stay local");
    let spec = JobSpec {
        kind: "batch".into(),
        bucket: "src".into(),
        prefix: "p/".into(),
        ..JobSpec::default()
    };
    state
        .jobs
        .create_job(&state.db, &spec, now())
        .await
        .unwrap();
    stop(state).await;

    // feature 3 仍是本地作业语义：非空表重启放行，且不写激活标记。
    let cfg = write_cfg(dir.path(), 3);
    let state = AppState::load(&cfg)
        .await
        .expect("feature 3 遇到旧本地作业行不得误拒");
    assert!(!state.jobs.replicated(), "feature 3 must stay local");
    assert_eq!(
        state.db.schema_meta("m8_runstate_activated").await.unwrap(),
        None,
        "feature 3 不启用复制，不得写激活标记"
    );
    stop(state).await;

    let cfg = write_cfg(dir.path(), 4);
    let err = AppState::load(&cfg)
        .await
        .map(|_| ())
        .expect_err("非空旧本地运行态切 feature 4 必须拒绝启动");
    let msg = err.to_string();
    assert!(msg.contains("m8_runstate_activated"), "{msg}");
    assert!(
        msg.contains("jobs=1") && msg.contains("job_tasks=0"),
        "{msg}"
    );
    assert!(msg.contains("feature_level=4"), "{msg}");
    assert!(msg.contains("排空") || msg.contains("清空"), "{msg}");
}

/// 空表首次激活成功并落标记；之后复制行非空的重启不再误拒；marker 4 不能被
/// 更低配置（模拟 v1.22 的 max 3）启动。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_tables_activate_and_feature4_restart_with_rows_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), 4);
    let state = AppState::load(&cfg).await.unwrap();
    assert_eq!(
        state.db.schema_meta("m8_runstate_activated").await.unwrap(),
        None,
        "空表启动不得提前写标记（effective 可能仍是 3）"
    );
    let handle = state.raft.clone().expect("raft handle");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(handle.is_leader() && handle.effective_feature() == Some(4)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single-node cluster never became feature-4 leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let spec = JobSpec {
        kind: "batch".into(),
        bucket: "src".into(),
        prefix: "p/".into(),
        ..JobSpec::default()
    };
    let id = state
        .jobs
        .create_job(&state.db, &spec, now())
        .await
        .unwrap();
    assert!(state.jobs.replicated(), "feature 4 must replicate");
    assert_eq!(
        state
            .db
            .schema_meta("raft_feature_level")
            .await
            .unwrap()
            .as_deref(),
        Some("4"),
        "首条 Job* 命令 apply 必须把激活 marker 提到 4"
    );
    assert_eq!(
        state
            .db
            .schema_meta("m8_runstate_activated")
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "已 apply 的 level-4 命令必须写运行态激活标记"
    );
    stop(state).await;

    // 重启：jobs 非空但已有激活标记（行全是日志事实）→ 放行。
    let state = AppState::load(&cfg)
        .await
        .expect("已激活的 feature-4 集群重启不得误拒");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1, "replicated job row must survive restart");
    stop(state).await;

    // 已激活 marker 4 后，配置 3 启动必须拒绝（v1.22 的 max 3 同理）。
    let cfg = write_cfg(dir.path(), 3);
    let err = AppState::load(&cfg)
        .await
        .map(|_| ())
        .expect_err("marker 4 不能用 feature_level=3 启动");
    assert!(err.to_string().contains("raft_feature_level=4"), "{err}");
}

/// P2：清掉 jobs/job_tasks 后 inventory_runs/tier_restores 仍引用旧 job → 悬空引用
/// 必须继续拒绝启动，并给出可执行的清理/重灌文案；清理后放行且不写 marker。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dangling_runstate_refs_block_feature4_activation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), 2);
    let state = AppState::load(&cfg).await.unwrap();
    let spec = JobSpec {
        kind: "batch".into(),
        bucket: "src".into(),
        prefix: "p/".into(),
        ..JobSpec::default()
    };
    let id = state
        .jobs
        .create_job(&state.db, &spec, now())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO inventory_runs \
         (bucket, config_id, run_key, started_at, state, job_id, created_at) \
         VALUES ('src', 'cfg', 'run-1', 1, 'done', ?, 1)",
    )
    .bind(id)
    .execute(state.db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tier_restores \
         (bucket, key, version_id, expires_at, job_id, state, created_at) \
         VALUES ('src', 'p/old', 'v1', 0, ?, 'pending', 1)",
    )
    .bind(id)
    .execute(state.db.pool())
    .await
    .unwrap();
    stop(state).await;

    let cfg = write_cfg(dir.path(), 4);
    let err = AppState::load(&cfg)
        .await
        .map(|_| ())
        .expect_err("非空运行态不得以 feature 4 启动")
        .to_string();
    assert!(
        err.contains("jobs=1")
            && err.contains("inventory_runs=1")
            && err.contains("tier_restores=1"),
        "{err}"
    );

    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.db").display()
    );
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("DELETE FROM jobs")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM job_tasks")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let err = AppState::load(&cfg)
        .await
        .map(|_| ())
        .expect_err("jobs 清空但引用悬空仍必须拒绝")
        .to_string();
    assert!(
        err.contains("inventory_runs.job_id=1")
            && err.contains("tier_restores.job_id=1")
            && err.contains("清空"),
        "{err}"
    );

    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query("DELETE FROM inventory_runs")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tier_restores")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let state = AppState::load(&cfg)
        .await
        .expect("四表清空后 feature 4 应可启动");
    assert_eq!(
        state.db.schema_meta("m8_runstate_activated").await.unwrap(),
        None,
        "清理后启动仍不得提前写 marker"
    );
    stop(state).await;
}
