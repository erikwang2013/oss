//! M8-2 P1-A：level-2 → level-3 运行态激活边界（启动守卫）。
//!
//! level-2 的 jobs/job_tasks 是节点本地行，不在复制日志中；以 feature_level=3 启动
//! 前必须为空（或已安装 level-3 快照/cutover 留下激活标记），否则拒绝启动并给出
//! 排空/清空/重灌的运维步骤。真实 `AppState::load`，不再走 apply fail-stop。

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

/// level 2 建过本地作业 → 切 feature_level=3 启动必须被拒（文案含计数与运维步骤）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level2_local_jobs_block_level3_activation() {
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

    let cfg = write_cfg(dir.path(), 3);
    let err = AppState::load(&cfg)
        .await
        .err()
        .expect("非空 level-2 运行态切 level 3 必须拒绝启动");
    let msg = err.to_string();
    assert!(msg.contains("m8_runstate_activated"), "{msg}");
    assert!(
        msg.contains("jobs=1") && msg.contains("job_tasks=0"),
        "{msg}"
    );
    assert!(msg.contains("feature_level=3"), "{msg}");
    assert!(msg.contains("排空") || msg.contains("清空"), "{msg}");
}

/// 空表首次激活成功并落标记；之后复制行非空的重启不再误拒。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_tables_activate_and_level3_restart_with_rows_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_cfg(dir.path(), 3);
    let state = AppState::load(&cfg).await.unwrap();
    assert_eq!(
        state
            .db
            .schema_meta("m8_runstate_activated")
            .await
            .unwrap()
            .as_deref(),
        Some("1"),
        "空表激活必须在启动时写标记"
    );
    let handle = state.raft.clone().expect("raft handle");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(handle.is_leader() && handle.effective_feature() == Some(3)) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single-node cluster never became level-3 leader"
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
    assert!(state.jobs.replicated(), "feature 3 must replicate");
    stop(state).await;

    // 重启：jobs 非空但已有激活标记（行全是日志事实）→ 放行。
    let state = AppState::load(&cfg)
        .await
        .expect("已激活的 level-3 集群重启不得误拒");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_one(state.db.pool())
        .await
        .unwrap();
    assert_eq!(rows, 1, "replicated job row must survive restart");
    stop(state).await;
}
