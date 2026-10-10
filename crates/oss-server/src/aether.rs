//! aetherupload 接线：四条路由 + 秒传索引（落 SQL，不引 Redis）+ 上传完成后原生 ingest。
//!
//! 为什么用原生 ingest 而不是 aetherupload 的 S3 驱动：省一跳本机 HTTP，
//! 且 aetherupload 已经算完整份 MD5 —— 直接就是单段 PUT 的 ETag，白拿。

use std::sync::Arc;

use aetherupload::Runtime;
use aetherupload::config::{Config, GroupConfig, StorageDriver, SubdirRule};
use aetherupload::events::EventSink;
use aetherupload::instant::InstantStore;
use aetherupload::resource::Resource;

use crate::config::AppConfig;
use crate::db::Db;
use crate::state::AppState;
use xl_store::store::Store;

/// 同步 trait → async 运行时的桥。
///
/// aetherupload 的 `EventSink` 与 `InstantStore` 都是**同步契约**（对齐 PHP 语义），
/// 而我们的存取层是 async。`block_in_place` 需要 multi-thread runtime
/// （`#[tokio::main]` 默认即是；current_thread 下会 panic —— 测试里用
/// `#[tokio::test(flavor = "multi_thread")]`）。
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

/// 秒传索引落在已有的 SQLite 上 —— 少一个 Redis 依赖，TTL 语义与 SETEX 一致。
#[derive(Clone)]
pub struct SqlInstantStore {
    db: Arc<Db>,
}

impl SqlInstantStore {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

impl InstantStore for SqlInstantStore {
    fn get(&self, key: &str) -> aetherupload::Result<Option<String>> {
        block_on(self.db.instant_get(key))
            .map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }

    fn set_ex(&self, key: &str, value: &str, ttl_seconds: u64) -> aetherupload::Result<()> {
        block_on(self.db.instant_set(key, value, ttl_seconds))
            .map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }

    fn del(&self, key: &str) -> aetherupload::Result<()> {
        block_on(self.db.instant_del(key))
            .map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }
}

/// 上传完成 → 把成品文件装进存储内核。
#[derive(Clone)]
pub struct IngestSink {
    pub store: Arc<Store>,
    pub db: Arc<Db>,
    pub cfg: Arc<AppConfig>,
}

impl IngestSink {
    /// 分组 → 目标桶 → 对象键（`<子目录>/<md5>.<ext>`）。
    fn resolve(&self, resource: &Resource) -> Option<(String, String)> {
        let bucket = self.cfg.bucket_for_group(&resource.group)?.to_string();
        let key = if resource.group_sub_dir.is_empty() {
            resource.name.clone()
        } else {
            format!("{}/{}", resource.group_sub_dir, resource.name)
        };
        Some((bucket, key))
    }
}

impl EventSink for IngestSink {
    fn upload_complete(&self, resource: &Resource) {
        let Some((bucket, key)) = self.resolve(resource) else {
            tracing::warn!(group = %resource.group, "上传分组没有配置目标桶，跳过 ingest");
            return;
        };

        // 事件里抛错等同于上传失败 —— 这里必须把错误传播出去（panic 会被控制器接住并清理临时文件）
        let result = block_on(async {
            let bytes = tokio::fs::read(&resource.path).await?;
            let size = bytes.len() as u64;
            let info = self
                .store
                .put_object(&bucket, &key, bytes.into(), None)
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((size, info))
        });

        match result {
            Ok((size, info)) => {
                let saved_path = format!(
                    "{}_{}_{}",
                    resource.group, resource.group_sub_dir, resource.name
                );
                if let Err(e) = block_on(self.db.record_upload(
                    &saved_path,
                    &bucket,
                    &key,
                    &resource.group,
                    size as i64,
                    &info.etag,
                )) {
                    tracing::error!(error = %e, "记录 uploads 失败");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "ingest 失败");
                panic!("ingest 失败: {e}");
            }
        }
    }
}

/// 组装 aetherupload 的运行时：暂存盘、分组、秒传索引、事件出口。
pub fn build_runtime(state: &AppState) -> Arc<Runtime> {
    let mut cfg = Config::default();
    cfg.root_dir = state.cfg.staging_dir.to_string_lossy().into_owned();
    cfg.chunk_size = 1_000_000;
    cfg.resource_subdir_rule = SubdirRule::Month;
    cfg.instant_completion = true; // 秒传开启（索引落 SQL）
    cfg.storage.driver = StorageDriver::Local; // 暂存与成品都在本地；ingest 之后由内核负责

    for g in &state.cfg.groups {
        cfg.groups.insert(
            g.group.clone(),
            GroupConfig {
                group_dir: g.group.clone(),
                resource_maxsize: g.max_size,
                resource_extensions: g.extensions.clone(),
                event_before_upload_complete: false,
                event_upload_complete: true, // 必须为 true，否则不触发 ingest
            },
        );
    }

    // Runtime 的上传根是 `base_path.join(root_dir)`；base_path 给空串，
    // upload_root 便恰好等于 staging_dir —— 相对（oss.toml 的 `./staging`）与绝对
    // （集成测试的 tempdir）两种配置下都成立。
    let runtime = Runtime::new(cfg, "")
        .expect("aetherupload Runtime 构造失败")
        .with_instant(Arc::new(SqlInstantStore::new(Arc::clone(&state.db))))
        .with_events(Arc::new(IngestSink {
            store: Arc::clone(&state.store),
            db: Arc::clone(&state.db),
            cfg: Arc::clone(&state.cfg),
        }));

    // 分组目录必须先存在，否则预处理会报笼统的上传错误。aetherupload 自带 e2e 也是用 list_groups 建的。
    let _ = aetherupload::console::list_groups(&runtime, &mut |_| {});

    // `_header/`（断点文件目录）单独补建：list_groups 只在它自己新建根目录时顺带建 `_header/`，
    // 而启动自举早已建好 staging_dir —— 不补这一下，预处理会以 "fail to write head resource" 失败。
    std::fs::create_dir_all(state.cfg.staging_dir.join(aetherupload::header::HEADER_DIR))
        .expect("创建 aetherupload _header 目录失败");

    Arc::new(runtime)
}
