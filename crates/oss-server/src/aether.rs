//! aetherupload 接线：四条路由 + 秒传索引（落 SQL，不引 Redis）+ 上传完成后原生 ingest。
//!
//! 为什么用原生 ingest 而不是 aetherupload 的 S3 驱动：省一跳本机 HTTP，
//! 且 aetherupload 已经算完整份 MD5 —— 直接就是单段 PUT 的 ETag，白拿。

use std::sync::Arc;

use aetherupload::Runtime;
use aetherupload::config::{Config, GroupConfig, StorageDriver, SubdirRule};
use aetherupload::events::EventSink;
use aetherupload::instant::{InstantIndex, InstantStore, KEY_PREFIX};
use aetherupload::resource::Resource;
use aetherupload::saved_path::SavedPath;
use futures::StreamExt as _;
use tokio_util::io::ReaderStream;

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

/// SQL 错误 → aetherupload 错误。这里有且只有这一处观测点：内核把索引访问的失败
/// 一概当作「未命中」（`instant_lookup` 吞掉 Err），不 warn 就完全不可见。变体保持 Io 不换。
fn store_err(e: sqlx::Error) -> aetherupload::Error {
    tracing::warn!(error = %e, "秒传索引访问失败");
    aetherupload::Error::Io(std::io::Error::other(e.to_string()))
}

impl InstantStore for SqlInstantStore {
    fn get(&self, key: &str) -> aetherupload::Result<Option<String>> {
        block_on(self.db.instant_get(key)).map_err(store_err)
    }

    fn set_ex(&self, key: &str, value: &str, ttl_seconds: u64) -> aetherupload::Result<()> {
        block_on(self.db.instant_set(key, value, ttl_seconds)).map_err(store_err)
    }

    fn del(&self, key: &str) -> aetherupload::Result<()> {
        block_on(self.db.instant_del(key)).map_err(store_err)
    }

    fn delete_prefix(&self, prefix: &str) -> aetherupload::Result<()> {
        block_on(self.db.instant_delete_prefix(prefix)).map_err(store_err)
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

    /// 把这份内容从秒传索引里摘掉（best-effort，失败只告警）。
    ///
    /// 不清的后果是「钉死」：ingest 失败后索引仍在，同内容重传会命中秒传、
    /// 拿到一个根本没进桶的 savedPath（规格 §5.4：宁可让用户重传）。
    fn clear_instant_index(&self, resource: &Resource) {
        // 索引键 = `KEY_PREFIX + <group>_<整份 md5>`：md5 即成品名 `<md5>.<ext>` 的 file_stem
        // （控制器 `get_file_name(real_hash, ext)` → `InstantIndex::key(group, hash)`，upload.rs:390-398）。
        // 键重建成立的前提是 lax_mode 关闭（客户端声明 hash == 真 md5）；开启 lax_mode 后需同步改此处。
        let Some(stem) = std::path::Path::new(&resource.name)
            .file_stem()
            .and_then(|s| s.to_str())
        else {
            return;
        };
        let Ok(key) = InstantIndex::key(&resource.group, stem) else {
            return;
        };
        if let Err(e) = block_on(self.db.instant_del(&format!("{KEY_PREFIX}{key}"))) {
            tracing::warn!(error = %e, "清秒传索引失败（该内容可能仍被钉死）");
        }
    }
}

impl EventSink for IngestSink {
    fn upload_complete(&self, resource: &Resource) {
        let Some((bucket, key)) = self.resolve(resource) else {
            tracing::warn!(group = %resource.group, "上传分组没有配置目标桶，跳过 ingest");
            return;
        };

        // aetherupload 的 ecat controller 以 catch_unwind 吞掉回调 panic、响应仍为成功
        // （`emit_safely`，upload.rs:465）；本 sink 以 panic + tracing::error 保证服务端可见，
        // 并清索引防钉死。1.0.4 仍未修（`controller/upload.rs` 逐字节未变，events.rs 仅把该隔离
        // 文档化为「与 PHP 一致」的有意行为），补偿继续有效。
        let result = block_on(async {
            // 流式进内核：大文件不再整份驻留内存（tokio-util 的 ReaderStream 直接套文件句柄）
            let file = tokio::fs::File::open(&resource.path).await?;
            let size = file.metadata().await?.len();
            let stream = ReaderStream::new(file).map(|r| r.map_err(xl_store::error::Error::from));
            // 版本控制（M4 切片 3）：目标桶 Enabled 时上传也按版本追加（与 S3 面同口径）。
            // 查库失败上抛（fail-closed：状态不可知时不写数据——不得按非版本桶静默覆写链）
            let versioned = self.db.get_bucket_versioning(&bucket).await? == "Enabled";
            let info = self
                .store
                .put_object_stream_with_meta(
                    &bucket,
                    &key,
                    Box::pin(stream),
                    None,
                    Default::default(),
                    None,
                    versioned,
                )
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((size, info))
        });

        match result {
            Ok((size, info)) => {
                let saved_path =
                    SavedPath::encode(&resource.group, &resource.group_sub_dir, &resource.name);
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
                self.clear_instant_index(resource);
                panic!("ingest 失败: {e}");
            }
        }
    }
}

/// 组装 aetherupload 的运行时：暂存盘、分组、秒传索引、事件出口。
/// 失败（Runtime 构造 / `_header` 目录建不出）返回 Err：serve 与运维命令统一走
/// exit 1 + stderr，不再从启动路径 panic（101）。
pub fn build_runtime(
    state: &AppState,
) -> Result<Arc<Runtime>, Box<dyn std::error::Error + Send + Sync>> {
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
        .map_err(|e| format!("aetherupload Runtime 构造失败: {e}"))?
        .with_instant(Arc::new(SqlInstantStore::new(Arc::clone(&state.db))))
        // Arc<IngestSink> → Arc<dyn EventSink>：显式转换，否则 clone 的返回类型不做 unsize 推断
        .with_events(Arc::clone(&state.ingest) as Arc<dyn EventSink>); // 与 AppState 是同一个实例（测试打到生产那个）

    // 分组目录必须先存在，否则预处理会报笼统的上传错误。aetherupload 自带 e2e 也是用 list_groups 建的。
    let code = aetherupload::console::list_groups(
        &runtime,
        &mut |m| tracing::debug!(msg = %m, "aetherupload 分组目录"),
    );
    if code != 0 {
        tracing::error!(
            code,
            "aetherupload 分组目录准备失败（预处理会报笼统的上传错误）"
        );
    }

    // `_header/`（断点文件目录）单独补建：list_groups 只在它自己新建根目录时顺带建 `_header/`，
    // 而启动自举早已建好 staging_dir —— 不补这一下，预处理会以 "fail to write head resource" 失败。
    std::fs::create_dir_all(state.cfg.staging_dir.join(aetherupload::header::HEADER_DIR)).map_err(
        |e| {
            format!(
                "创建 aetherupload _header 目录（{}）失败: {e}",
                state.cfg.staging_dir.display()
            )
        },
    )?;

    Ok(Arc::new(runtime))
}
