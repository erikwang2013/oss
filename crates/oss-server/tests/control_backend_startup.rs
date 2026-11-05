//! B4：`[server].control_backend` 启动选择（sqlite 默认 / objects）与 fail-closed 断言。

use std::path::{Path, PathBuf};

use oss_server::config::ControlBackend;
use oss_server::control_backend::ObjectControl;
use oss_server::state::AppState;
use xl_store::control_state::root::Mode;
use xl_store::store::{Store, StoreConfig};

fn base_config(dir: &Path, backend: Option<&str>, extra: &str) -> PathBuf {
    let server = backend
        .map(|b| format!("\n[server]\ncontrol_backend = \"{b}\"\n"))
        .unwrap_or_default();
    let text = format!(
        r#"
listen = "127.0.0.1:0"
data_dir = "{data}"
staging_dir = "{staging}"
database_url = "sqlite://{db}?mode=rwc"
jwt_secret = "0123456789abcdef0123456789abcdef"
{server}{extra}"#,
        data = dir.join("data").display(),
        staging = dir.join("staging").display(),
        db = dir.join("oss.db").display(),
    );
    let path = dir.join("oss.toml");
    std::fs::write(&path, text).unwrap();
    path
}

#[tokio::test]
async fn backend_select_object_startup_skips_sqlite_and_holds_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = base_config(
        dir.path(),
        Some("objects"),
        "\n[site]\nid = \"site-a\"\nregion = \"us-east-1\"\n",
    );
    let state = AppState::load(&cfg).await.unwrap();

    assert_eq!(state.db.control_backend(), ControlBackend::Objects);
    assert!(
        !dir.path().join("oss.db").exists(),
        "对象模式不得打开/创建 oss.db"
    );
    // 占位 pool 不跑迁移：未迁移的 SQLite 门面会以“表不存在”fail-closed。
    let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type = 'table'")
        .fetch_one(state.db.pool())
        .await
        .unwrap();
    assert_eq!(tables, 0, "对象模式不得执行 SQLite schema 迁移");

    let root = dir.path().join("data");
    assert!(
        root.join(".oss.sys").join("local").is_dir(),
        "节点本地运行态目录必须装配"
    );
    let control = state.db.object_control().expect("对象控制面必须已注入");
    assert_eq!(
        control.control.pin().await.unwrap().root.meta.mode,
        Mode::Standalone
    );
    assert_eq!(state.db.site_identity().await.unwrap().id, "site-a");

    // 第二个进程的启动锁：首个 AppState 存活期间必须失败。
    let err = AppState::load(&cfg)
        .await
        .err()
        .expect("第二个实例必须失败");
    assert!(err.to_string().contains("启动锁"), "实际错误：{err}");
}

#[tokio::test]
async fn backend_select_object_refuses_raft_root_mode_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut store_cfg = StoreConfig::new(&data);
    store_cfg.fsync = false;
    let store = std::sync::Arc::new(Store::open(store_cfg).await.unwrap());
    // 预置一个 raft 形态的对象根；随后以 standalone 配置启动必须 fail-closed。
    ObjectControl::open(std::sync::Arc::clone(&store), Mode::Raft, None)
        .await
        .unwrap();
    drop(store);

    let cfg = base_config(dir.path(), Some("objects"), "");
    let err = AppState::load(&cfg)
        .await
        .err()
        .expect("mode 不一致必须拒绝");
    assert!(err.to_string().contains("不一致"), "实际错误：{err}");
}

#[tokio::test]
async fn backend_select_object_refuses_raft_config_before_touching_root() {
    let dir = tempfile::tempdir().unwrap();
    let raft = format!(
        r#"
[site]
region = "us-east-1"

[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:19000"
cluster_token = "b4-cluster-token-0123456789"
db_path = "{}"
"#,
        dir.path().join("raft.db").display()
    );
    let cfg = base_config(dir.path(), Some("objects"), &raft);
    let err = AppState::load(&cfg)
        .await
        .err()
        .expect("objects+raft 必须拒绝");
    assert!(err.to_string().contains("暂不支持"), "实际错误：{err}");
    assert!(
        !dir.path().join("data").join(".oss.sys").exists(),
        "拒绝必须发生在引导对象根之前"
    );
}

#[tokio::test]
async fn backend_select_default_sqlite_creates_schema() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = base_config(dir.path(), None, "");
    let state = AppState::load(&cfg).await.unwrap();

    assert_eq!(state.db.control_backend(), ControlBackend::Sqlite);
    assert!(
        dir.path().join("oss.db").exists(),
        "默认 sqlite 必须创建 oss.db"
    );
    let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type = 'table'")
        .fetch_one(state.db.pool())
        .await
        .unwrap();
    assert!(tables > 0, "默认 sqlite 必须跑 schema 迁移");
}

#[tokio::test]
async fn backend_select_unknown_backend_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = base_config(dir.path(), Some("gcs"), "");
    let err = AppState::load(&cfg).await.err().expect("未知后端必须拒绝");
    assert!(
        err.to_string().contains("unknown variant") || err.to_string().contains("control_backend"),
        "实际错误：{err}"
    );
}
