//! M8 S5：Inventory 调度槽、生成 worker 与扫描/输出执行器。
//!
//! 调度只补当前槽（停机期间的旧槽不补）；同一 `(bucket,config_id,run_key)` 由唯一索引去重。
//! 扫描按前缀翻页（页大小 1000），输出先写 staging 临时文件（边写边算 MD5/大小）再流式 PUT。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use bytes::Bytes;
use flate2::write::GzEncoder;
use futures::StreamExt as _;
use md5::{Digest as _, Md5};
use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type as SchemaType;
use s3s::dto::{InventoryConfiguration, InventoryFormat};
use xl_s3::access::{Acl, AuthedUser};
use xl_s3::inventory::{
    config_from_json, csv_columns, destination_bucket_name, filter_prefix, includes_all_versions,
};
use xl_s3::sse::enc_spec_from_default;
use xl_store::meta::{ChecksumRecord, EncryptionRecord, ObjectLockRecord};
use xl_store::store::{ByteStream, ListRequest, ObjectInfo, Store, VersioningMode};

use crate::db::{Db, JobSpec, NewJobTask};
use crate::jobs::TaskExecutor;

/// 单数据文件上限（gzip 后仍超过 5 GiB → error）。
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InventoryPayload {
    pub bucket: String,
    pub config_id: String,
    pub run_key: String,
}

/// Daily 槽：UTC 日期。
pub fn daily_key(now: i64) -> String {
    let dt =
        time::OffsetDateTime::from_unix_timestamp(now).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    )
}

/// Weekly 槽：ISO 周（`YYYY-Www`；跨年周取 ISO 年）。
pub fn weekly_key(now: i64) -> String {
    let dt =
        time::OffsetDateTime::from_unix_timestamp(now).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let (year, week, _) = dt.to_iso_week_date();
    format!("{year:04}-W{week:02}")
}

fn slot_key(freq: &str, now: i64) -> Option<String> {
    if freq == s3s::dto::InventoryFrequency::DAILY {
        Some(daily_key(now))
    } else if freq == s3s::dto::InventoryFrequency::WEEKLY {
        Some(weekly_key(now))
    } else {
        None
    }
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 建运行行 + kind=inventory 单任务作业（返回 job id；同槽已存在 → None）。
pub async fn enqueue_run(
    db: &Db,
    bucket: &str,
    config_id: &str,
    run_key: &str,
    requested_by: Option<i64>,
    now: i64,
) -> Result<Option<i64>, String> {
    let Some(run_id) = db
        .inventory_create_run(bucket, config_id, run_key, now)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let payload = serde_json::to_string(&InventoryPayload {
        bucket: bucket.to_string(),
        config_id: config_id.to_string(),
        run_key: run_key.to_string(),
    })
    .map_err(|e| e.to_string())?;
    let spec = JobSpec {
        kind: "inventory".to_string(),
        bucket: bucket.to_string(),
        prefix: config_id.to_string(),
        requested_by,
        tasks: vec![NewJobTask {
            action: "Inventory".to_string(),
            payload,
            dedupe_key: None,
        }],
        ..Default::default()
    };
    match db.create_job(&spec, now).await {
        Ok(job_id) => {
            db.inventory_run_set_job(run_id, job_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Some(job_id))
        }
        Err(e) => {
            let _ = db
                .inventory_run_finish(run_id, "error", None, Some(&e.to_string()))
                .await;
            Err(e.to_string())
        }
    }
}

/// 本轮到期的配置：只建当前槽；已存在（含重复 tick）→ 去重。
pub async fn due_runs(db: &Db, now: i64) -> Result<usize, String> {
    let rows = db
        .inventory_list_all_configs()
        .await
        .map_err(|e| e.to_string())?;
    let mut queued = 0usize;
    for row in rows {
        let cfg: InventoryConfiguration = match serde_json::from_str(&row.config_json) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(bucket = %row.bucket, id = %row.id, error = %e, "inventory 配置非法");
                continue;
            }
        };
        if !cfg.is_enabled {
            continue;
        }
        let Some(run_key) = slot_key(cfg.schedule.frequency.as_str(), now) else {
            continue;
        };
        match enqueue_run(db, &row.bucket, &row.id, &run_key, Some(row.owner_id), now).await {
            Ok(Some(_)) => queued += 1,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(bucket = %row.bucket, id = %row.id, error = %e, "inventory 入队失败")
            }
        }
    }
    Ok(queued)
}

/// `OSS_INVENTORY_INTERVAL_SECS`（默认 60；0 = 关闭）。
pub fn spawn_worker(db: Arc<Db>) {
    let secs = env_secs("OSS_INVENTORY_INTERVAL_SECS", 60);
    if secs == 0 {
        tracing::info!("inventory worker 已关闭（OSS_INVENTORY_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            match due_runs(&db, crate::db::now()).await {
                Ok(n) if n > 0 => tracing::info!(n, "inventory：本轮入队"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "inventory 调度失败"),
            }
        }
    });
}

/// 3.6：增量扫描参数。`previous` = 上轮成功扫描开始时刻（nanos，None = 首轮全量）；
/// `cursor` = 本轮扫描开始时刻，成功后作为新水位。仅输出 `last_modified >= previous`。
#[derive(Clone, Copy)]
struct Incremental {
    previous: Option<i64>,
    cursor: i64,
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// 生成器（`TaskExecutor` 的 `Inventory` 动作）。
pub struct InventoryRunner {
    db: Arc<Db>,
    store: Arc<Store>,
    acl: Acl,
    staging_dir: PathBuf,
}

impl InventoryRunner {
    pub fn new(db: Arc<Db>, store: Arc<Store>, acl: Acl, staging_dir: PathBuf) -> Self {
        Self {
            db,
            store,
            acl,
            staging_dir,
        }
    }

    async fn fail(&self, run_id: i64, job_id: Option<i64>, msg: String) -> String {
        let now = crate::db::now();
        if let Some(j) = job_id {
            // 引擎在任务收尾时 progress+1；存 -1 让终值为 0。
            let _ = self.db.job_set_progress(j, -1, now).await;
        }
        let _ = self
            .db
            .inventory_run_finish(run_id, "error", None, Some(&msg))
            .await;
        msg
    }

    async fn run(&self, p: &InventoryPayload) -> Result<String, String> {
        let Some(run) = self
            .db
            .inventory_run_by_key(&p.bucket, &p.config_id, &p.run_key)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Err(format!(
                "inventory run 行不存在：{}/{}/{}",
                p.bucket, p.config_id, p.run_key
            ));
        };
        let Some(cfg_row) = self
            .db
            .inventory_get_config(&p.bucket, &p.config_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            // 入队后配置被删：run 行落 error，避免永久停在 running。
            let msg = format!("inventory 配置不存在：{}/{}", p.bucket, p.config_id);
            return Err(self.fail(run.id, run.job_id, msg).await);
        };
        let now = crate::db::now();
        self.db
            .inventory_run_started(run.id, now)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(j) = run.job_id {
            let _ = self.db.job_set_progress(j, 0, now).await;
        }
        let (cfg, recorded_source_account): (InventoryConfiguration, Option<String>) =
            match config_from_json(&cfg_row.config_json) {
                Ok(c) => c,
                Err(e) => {
                    let msg = format!("config_json 非法：{e}");
                    return Err(self.fail(run.id, run.job_id, msg).await);
                }
            };
        if !cfg.is_enabled {
            if let Some(j) = run.job_id {
                let _ = self.db.job_set_progress(j, -1, now).await;
            }
            let _ = self
                .db
                .inventory_run_finish(run.id, "skipped", None, None)
                .await;
            return Ok("skipped".to_string());
        }
        let Some(dest) = destination_bucket_name(&cfg.destination.s3_bucket_destination.bucket)
            .map(str::to_string)
        else {
            let msg = format!(
                "目标桶 ARN 非法：{}",
                cfg.destination.s3_bucket_destination.bucket
            );
            return Err(self.fail(run.id, run.job_id, msg).await);
        };
        // 运行期复核所有者仍存在且启用：停用/删除用户后配置不得继续产出。
        let (is_admin, active) = match self
            .db
            .get_user(cfg_row.owner_id)
            .await
            .map_err(|e| e.to_string())?
        {
            Some((_, is_admin, active)) => (is_admin, active),
            None => {
                let msg = format!("配置所有者 {} 不存在，拒绝运行清单", cfg_row.owner_id);
                return Err(self.fail(run.id, run.job_id, msg).await);
            }
        };
        if !active {
            let msg = format!("配置所有者 {} 已停用，拒绝运行清单", cfg_row.owner_id);
            return Err(self.fail(run.id, run.job_id, msg).await);
        }
        let owner = AuthedUser {
            uid: cfg_row.owner_id,
            is_admin,
            ak: String::new(),
        };
        // 3.7：配置期记录源桶账户；运行期复核源/目标桶账户，失配 fail-closed（不写输出）。
        let source_account = match self.db.bucket_account(&p.bucket).await {
            Ok(v) => v,
            Err(e) => return Err(self.fail(run.id, run.job_id, e.to_string()).await),
        };
        if let Some(recorded) = recorded_source_account.as_deref()
            && recorded != source_account.as_str()
        {
            let msg =
                format!("源桶账户已变化（配置 {recorded}，当前 {source_account}），拒绝写出清单");
            return Err(self.fail(run.id, run.job_id, msg).await);
        }
        let dest_account = match self.db.bucket_account(&dest).await {
            Ok(v) => v,
            Err(e) => return Err(self.fail(run.id, run.job_id, e.to_string()).await),
        };
        if let Some(want) = cfg.destination.s3_bucket_destination.account_id.as_deref()
            && want != dest_account.as_str()
        {
            let msg = format!("目标桶账户校验失败（配置 {want}，当前 {dest_account}）");
            return Err(self.fail(run.id, run.job_id, msg).await);
        }
        let base = join_key(&[
            cfg.destination
                .s3_bucket_destination
                .prefix
                .as_deref()
                .unwrap_or(""),
            &p.bucket,
            &p.config_id,
            &p.run_key,
        ]);
        let parquet =
            cfg.destination.s3_bucket_destination.format.as_str() == InventoryFormat::PARQUET;
        let ext = if parquet { "parquet" } else { "csv.gz" };
        let data_key = format!("{base}/data/{}.{ext}", uuid::Uuid::new_v4());
        let manifest_key = format!("{base}/manifest.json");
        // Hive 兼容清单与数据/清单同目录树，供 Athena SymlinkTextInputFormat 读取。
        // 4.9：分区取 run 行的 created_at（入队时刻），任务重试跨分钟也不会写出
        // 第二个 dt= 分区（run_key 不变，重试命中同一 run 行，分区必须稳定）。
        let hive_key = format!(
            "{base}/hive/dt={}/symlink.txt",
            hive_partition(run.created_at)
        );
        // I4：运行期以**实际键**复核 PutObject（融合桶策略，显式 Deny 优先）。
        for key in [&data_key, &manifest_key, &hive_key] {
            match self
                .acl
                .check_op(&owner, "", "PutObject", Some(&dest), Some(key), None)
                .await
            {
                Ok((true, _)) => {}
                Ok((false, detail)) => {
                    let suffix = detail.map(|d| format!("（{d}）")).unwrap_or_default();
                    let msg = format!("配置所有者没有目标桶 {dest} 的 PutObject 权限{suffix}");
                    return Err(self.fail(run.id, run.job_id, msg).await);
                }
                Err(e) => return Err(self.fail(run.id, run.job_id, e.to_string()).await),
            }
        }
        if !self
            .db
            .bucket_exists(&dest)
            .await
            .map_err(|e| e.to_string())?
        {
            let msg = format!("目标桶不存在：{dest}");
            return Err(self.fail(run.id, run.job_id, msg).await);
        }
        // 运行期复核源桶读权限：配置创建后权限被撤销 → 失败关闭，不扫描、不写输出。
        match self
            .acl
            .check_op(&owner, "", "ListObjectsV2", Some(&p.bucket), None, None)
            .await
        {
            Ok((true, _)) => {}
            Ok((false, detail)) => {
                let suffix = detail.map(|d| format!("（{d}）")).unwrap_or_default();
                let msg = format!("配置所有者没有源桶 {} 的 ListBucket 权限{suffix}", p.bucket);
                return Err(self.fail(run.id, run.job_id, msg).await);
            }
            Err(e) => return Err(self.fail(run.id, run.job_id, e.to_string()).await),
        }
        // 3.6：增量模式 per config（默认全量）。水位是「上次扫描开始时刻」：
        // 本轮输出 last_modified >= 水位 的对象/版本，扫描开始时刻作为新水位。
        // 增量清单是**非 AWS 扩展**：无版本控制桶的删除无法表现为条目（没有墓碑）。
        let incremental = match self
            .db
            .inventory_incremental_get(&p.bucket, &p.config_id)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("增量模式读取失败: {e}");
                return Err(self.fail(run.id, run.job_id, msg).await);
            }
        };
        let prev_mark = if incremental {
            match self
                .db
                .inventory_cursor_get(&p.bucket, &p.config_id, cfg_row.updated_at)
                .await
            {
                Ok(Some(raw)) => match raw.parse::<i64>() {
                    Ok(v) => Some(v),
                    Err(e) => {
                        // 坏水位退回全量（只会多写、不会漏写），成功后被新水位覆盖。
                        tracing::warn!(bucket = %p.bucket, config = %p.config_id, error = %e,
                            "inventory：增量水位损坏，本轮按全量扫描");
                        None
                    }
                },
                Ok(None) => None,
                Err(e) => {
                    let msg = format!("增量水位读取失败: {e}");
                    return Err(self.fail(run.id, run.job_id, msg).await);
                }
            }
        } else {
            None
        };
        // 水位必须在开始列举前取：扫描期间及之后的新写会 > 水位，留给下一轮。
        let inc = incremental.then_some(Incremental {
            previous: prev_mark,
            cursor: now_nanos(),
        });
        let tmp = self.staging_dir.join(format!(
            "inventory-{}-{}.tmp",
            p.config_id,
            uuid::Uuid::new_v4()
        ));
        let mut scanned = 0i64;
        let result = self
            .scan_and_upload(
                &p.bucket,
                &cfg,
                &dest,
                (&source_account, &dest_account),
                &data_key,
                &manifest_key,
                &hive_key,
                &tmp,
                run.job_id,
                inc,
                &mut scanned,
            )
            .await;
        let _ = std::fs::remove_file(&tmp);
        let now = crate::db::now();
        if let Some(j) = run.job_id {
            // 引擎在任务收尾时 progress+1；存 scanned-1 让终值 = 已扫描数。
            let _ = self.db.job_set_progress(j, scanned - 1, now).await;
        }
        match result {
            Ok(()) => {
                // H1：先把 run 落 done，再推进水位。done 失败时水位不动，任务重试会重放
                // 同一区间（重复条目安全）；反向顺序在崩溃时会永久漏掉本轮对象。
                self.db
                    .inventory_run_finish(run.id, "done", Some(&manifest_key), None)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(inc) = inc
                    && let Err(e) = self
                        .db
                        .inventory_cursor_set(
                            &p.bucket,
                            &p.config_id,
                            cfg_row.updated_at,
                            Some(&inc.cursor.to_string()),
                            now,
                        )
                        .await
                {
                    // run 已 done；水位保持旧值 → 下轮重复输出（覆盖完整），仅告警。
                    tracing::warn!(bucket = %p.bucket, config = %p.config_id, error = %e,
                        "inventory：增量水位写入失败，下轮将重复输出上一区间");
                }
                Ok("done".to_string())
            }
            Err(e) => {
                let _ = self
                    .db
                    .inventory_run_finish(run.id, "error", None, Some(&e))
                    .await;
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_and_upload(
        &self,
        source_bucket: &str,
        cfg: &InventoryConfiguration,
        dest: &str,
        accounts: (&str, &str),
        data_key: &str,
        manifest_key: &str,
        hive_key: &str,
        tmp: &Path,
        job_id: Option<i64>,
        inc: Option<Incremental>,
        scanned: &mut i64,
    ) -> Result<(), String> {
        let columns = csv_columns(cfg);
        let parquet =
            cfg.destination.s3_bucket_destination.format.as_str() == InventoryFormat::PARQUET;
        let filter = filter_prefix(cfg).to_string();
        let since = inc.and_then(|i| i.previous);
        let size = Arc::new(AtomicU64::new(0));
        let mut sink = DataSink::new(tmp, &columns, parquet, Arc::clone(&size))?;
        if includes_all_versions(cfg) {
            self.scan_versions(
                source_bucket,
                &columns,
                &filter,
                &mut sink,
                &size,
                job_id,
                since,
                scanned,
            )
            .await?;
        } else {
            self.scan_current(
                source_bucket,
                &filter,
                &mut sink,
                &size,
                job_id,
                since,
                scanned,
            )
            .await?;
        }
        let (md5, file_size) = sink.finish()?;
        if file_size > MAX_FILE_BYTES {
            return Err(format!("单文件 {file_size} 字节超过 5 GiB 上限"));
        }
        let mode = VersioningMode::from_status(
            &self
                .db
                .get_bucket_versioning(dest)
                .await
                .map_err(|e| e.to_string())?,
        );
        // I5：目标桶默认加密同样覆盖服务端生成对象（与 S3 写入路径同口径；坏记录 fail-closed）。
        let encryption = match self
            .acl
            .bucket_default_encryption(dest)
            .await
            .map_err(|e| e.to_string())?
            .as_deref()
        {
            Some(v) => Some(
                enc_spec_from_default(v).ok_or_else(|| format!("目标桶默认加密记录非法: {v}"))?,
            ),
            None => None,
        };
        let f = tokio::fs::File::open(tmp)
            .await
            .map_err(|e| format!("打开 staging 文件失败：{e}"))?;
        let body: ByteStream = Box::pin(
            tokio_util::io::ReaderStream::new(f).map(|r| r.map_err(xl_store::error::Error::from)),
        );
        let content_type = if parquet {
            "application/vnd.apache.parquet"
        } else {
            "application/gzip"
        };
        self.store
            .put_object_stream_with_meta(
                dest,
                data_key,
                body,
                Some(content_type.to_string()),
                BTreeMap::new(),
                None,
                mode,
                encryption.clone(),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| format!("数据文件写入目标桶失败：{e}"))?;
        // Hive 兼容清单：一行一个数据文件 S3 URI（Athena SymlinkTextInputFormat）。
        let symlink = format!("s3://{dest}/{data_key}\n");
        self.store
            .put_object_stream_with_meta(
                dest,
                hive_key,
                Box::pin(futures::stream::once(
                    async move { Ok(Bytes::from(symlink)) },
                )),
                Some("text/plain".to_string()),
                BTreeMap::new(),
                None,
                mode,
                encryption.clone(),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| format!("Hive symlink 写入目标桶失败：{e}"))?;
        let file_schema = if parquet {
            parquet_schema_string(&columns)?
        } else {
            columns.join(",")
        };
        // 3.7：账户字段是扩展（AWS manifest 无此键）；`accountId` 为源桶账户。
        let mut manifest = serde_json::json!({
            "sourceBucket": source_bucket,
            "destinationBucket": dest,
            "accountId": accounts.0,
            "destinationAccountId": accounts.1,
            "version": "2016-11-30",
            "creationTimestamp": iso8601_millis(crate::db::now(), 0),
            "fileFormat": if parquet { "Parquet" } else { "CSV" },
            "fileSchema": file_schema,
            "files": [{"key": data_key, "size": file_size, "md5Checksum": md5}],
        });
        // 3.6：manifest 必须记录本轮增量游标（上轮水位 + 新水位），便于核对清单覆盖区间。
        if let Some(inc) = inc {
            manifest["incremental"] = serde_json::json!({
                "enabled": true,
                "previousCursor": inc.previous.map(|v| v.to_string()),
                "cursor": inc.cursor.to_string(),
            });
        }
        let bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;
        // manifest 改走 stream+meta 入口以携带与 data 文件相同的默认加密。
        self.store
            .put_object_stream_with_meta(
                dest,
                manifest_key,
                Box::pin(futures::stream::once(async move { Ok(Bytes::from(bytes)) })),
                Some("application/json".to_string()),
                BTreeMap::new(),
                None,
                mode,
                encryption,
                None,
                None,
                None,
            )
            .await
            .map_err(|e| format!("manifest 写入目标桶失败：{e}"))?;
        Ok(())
    }

    /// 当前对象扫描（Current 模式）。
    #[allow(clippy::too_many_arguments)]
    async fn scan_current(
        &self,
        bucket: &str,
        filter: &str,
        sink: &mut DataSink,
        size: &AtomicU64,
        job_id: Option<i64>,
        since: Option<i64>,
        scanned: &mut i64,
    ) -> Result<(), String> {
        let mut token: Option<String> = None;
        loop {
            let page = self
                .store
                .list_objects(
                    bucket,
                    &ListRequest {
                        prefix: Some(filter.to_string()),
                        max_keys: Some(1000),
                        continuation_token: token,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            for info in &page.objects {
                // 时间未知（0）不在增量里静默丢弃：宁可多写一条。
                if since
                    .is_some_and(|m| info.last_modified_nanos > 0 && info.last_modified_nanos < m)
                {
                    continue;
                }
                sink.write(&current_fields(bucket, info))?;
            }
            *scanned += page.objects.len() as i64;
            if let Some(j) = job_id {
                let _ = self
                    .db
                    .job_set_progress(j, *scanned, crate::db::now())
                    .await;
            }
            check_size(size)?;
            if !page.is_truncated {
                return Ok(());
            }
            token = page.next_token;
        }
    }

    /// 版本扫描（All 模式；marker 行保留，IsLatest/DeleteMarker 如实输出）。
    #[allow(clippy::too_many_arguments)]
    async fn scan_versions(
        &self,
        bucket: &str,
        columns: &[&str],
        filter: &str,
        sink: &mut DataSink,
        size: &AtomicU64,
        job_id: Option<i64>,
        since: Option<i64>,
        scanned: &mut i64,
    ) -> Result<(), String> {
        let need_head = columns.iter().any(|c| {
            matches!(
                *c,
                "EncryptionStatus"
                    | "ObjectLockRetainUntilDate"
                    | "ObjectLockMode"
                    | "ObjectLockLegalHoldStatus"
                    | "ChecksumAlgorithm"
            )
        });
        let mut key_marker: Option<String> = None;
        let mut vid_marker: Option<String> = None;
        loop {
            let page = self
                .store
                .list_object_versions(
                    bucket,
                    filter,
                    1000,
                    key_marker.as_deref(),
                    vid_marker.as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
            for e in &page.entries {
                // 时间未知（0）不在增量里静默丢弃：宁可多写一条。
                if since.is_some_and(|m| e.mod_time_nanos > 0 && e.mod_time_nanos < m) {
                    continue;
                }
                // 不足字段按需 head（成本：每个版本一次 meta 读；仅在请求了这些列时发生）。
                let head = if need_head && !e.is_marker {
                    self.store
                        .head_object_version(bucket, &e.key, &e.version_id)
                        .await
                        .ok()
                } else {
                    None
                };
                sink.write(&version_fields(bucket, e, head.as_ref()))?;
            }
            *scanned += page.entries.len() as i64;
            if let Some(j) = job_id {
                let _ = self
                    .db
                    .job_set_progress(j, *scanned, crate::db::now())
                    .await;
            }
            check_size(size)?;
            if !page.is_truncated {
                return Ok(());
            }
            if page.next_key_marker.is_none() {
                return Ok(());
            }
            key_marker = page.next_key_marker;
            vid_marker = page.next_version_id_marker;
        }
    }
}

/// CSV / Parquet 两种输出；扫描循环只生产 [`Fields`]，格式差异都在这里。
/// ParquetSink 自带 writer 缓冲，枚举大小差大；每次运行只有一个 sink，不值得装箱。
#[allow(clippy::large_enum_variant)]
enum DataSink {
    Csv(CsvSink),
    Parquet(ParquetSink),
}

impl DataSink {
    fn new(
        path: &Path,
        columns: &[&'static str],
        parquet: bool,
        size: Arc<AtomicU64>,
    ) -> Result<Self, String> {
        let file = std::fs::File::create(path).map_err(|e| format!("建 staging 文件失败：{e}"))?;
        if parquet {
            return Ok(Self::Parquet(ParquetSink::new(file, columns, size)?));
        }
        let mut w = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(GzEncoder::new(
                HashingWriter::new(std::io::BufWriter::new(file), size),
                flate2::Compression::default(),
            ));
        w.write_record(columns)
            .map_err(|e| format!("CSV 头写入失败：{e}"))?;
        Ok(Self::Csv(CsvSink {
            w,
            columns: columns.to_vec(),
        }))
    }

    fn write(&mut self, f: &Fields) -> Result<(), String> {
        match self {
            Self::Csv(s) => s.write(f),
            Self::Parquet(p) => p.write(f),
        }
    }

    /// 返回 (base64 MD5, 落盘字节数)。
    fn finish(self) -> Result<(String, u64), String> {
        match self {
            Self::Csv(s) => s.finish(),
            Self::Parquet(p) => p.finish(),
        }
    }
}

type GzSink = GzEncoder<HashingWriter<std::io::BufWriter<std::fs::File>>>;

struct CsvSink {
    w: csv::Writer<GzSink>,
    columns: Vec<&'static str>,
}

impl CsvSink {
    fn write(&mut self, f: &Fields) -> Result<(), String> {
        let row: Vec<String> = self.columns.iter().map(|c| render(c, f)).collect();
        self.w
            .write_record(&row)
            .map_err(|e| format!("CSV 行写入失败：{e}"))
    }

    fn finish(mut self) -> Result<(String, u64), String> {
        self.w.flush().map_err(|e| format!("CSV 写入失败：{e}"))?;
        let gz = self
            .w
            .into_inner()
            .map_err(|e| format!("CSV 写入失败：{}", e.into_error()))?;
        let (_, md5, size) = gz
            .finish()
            .map_err(|e| format!("gzip 收尾失败：{e}"))?
            .finish()
            .map_err(|e| format!("staging 写入失败：{e}"))?;
        Ok((md5, size))
    }
}

/// 行组大小：限制 Parquet 写入的内存占用。
const PARQUET_ROW_GROUP_ROWS: usize = 8192;

#[derive(Clone, Copy)]
enum PKind {
    Utf8Req,
    Utf8Opt,
    BoolOpt,
    Int64Opt,
    TimestampOpt,
}

struct PCol {
    csv: &'static str,
    /// AWS Inventory Parquet 使用 lower_snake_case 字段名。
    name: &'static str,
    kind: PKind,
}

fn parquet_columns(columns: &[&'static str]) -> Result<Vec<PCol>, String> {
    columns
        .iter()
        .map(|c| {
            let (name, kind) = match *c {
                "Bucket" => ("bucket", PKind::Utf8Req),
                "Key" => ("key", PKind::Utf8Req),
                "VersionId" => ("version_id", PKind::Utf8Opt),
                "IsLatest" => ("is_latest", PKind::BoolOpt),
                "DeleteMarker" => ("is_delete_marker", PKind::BoolOpt),
                "Size" => ("size", PKind::Int64Opt),
                "LastModifiedDate" => ("last_modified_date", PKind::TimestampOpt),
                "ETag" => ("e_tag", PKind::Utf8Opt),
                "StorageClass" => ("storage_class", PKind::Utf8Opt),
                "ReplicationStatus" => ("replication_status", PKind::Utf8Opt),
                "EncryptionStatus" => ("encryption_status", PKind::Utf8Opt),
                "ObjectLockRetainUntilDate" => {
                    ("object_lock_retain_until_date", PKind::TimestampOpt)
                }
                "ObjectLockMode" => ("object_lock_mode", PKind::Utf8Opt),
                "ObjectLockLegalHoldStatus" => ("object_lock_legal_hold_status", PKind::Utf8Opt),
                "ChecksumAlgorithm" => ("checksum_algorithm", PKind::Utf8Opt),
                other => return Err(format!("Parquet 不支持的库存列：{other}")),
            };
            Ok(PCol { csv: c, name, kind })
        })
        .collect()
}

fn parquet_schema(cols: &[PCol]) -> Result<SchemaType, String> {
    let mut fields = Vec::with_capacity(cols.len());
    for c in cols {
        let (physical, repetition, logical) = match c.kind {
            PKind::Utf8Req => (
                PhysicalType::BYTE_ARRAY,
                Repetition::REQUIRED,
                Some(LogicalType::String),
            ),
            PKind::Utf8Opt => (
                PhysicalType::BYTE_ARRAY,
                Repetition::OPTIONAL,
                Some(LogicalType::String),
            ),
            PKind::BoolOpt => (PhysicalType::BOOLEAN, Repetition::OPTIONAL, None),
            PKind::Int64Opt => (PhysicalType::INT64, Repetition::OPTIONAL, None),
            PKind::TimestampOpt => (
                PhysicalType::INT64,
                Repetition::OPTIONAL,
                Some(LogicalType::timestamp(true, TimeUnit::MILLIS)),
            ),
        };
        let field = SchemaType::primitive_type_builder(c.name, physical)
            .with_repetition(repetition)
            .with_logical_type(logical)
            .build()
            .map_err(|e| format!("Parquet schema 构建失败：{e}"))?;
        fields.push(Arc::new(field));
    }
    SchemaType::group_type_builder("s3.inventory")
        .with_fields(fields)
        .build()
        .map_err(|e| format!("Parquet schema 构建失败：{e}"))
}

/// manifest 的 `fileSchema`：Hive/Athena 可直接读的 schema 文本。
fn parquet_schema_string(columns: &[&'static str]) -> Result<String, String> {
    let cols = parquet_columns(columns)?;
    let mut out = String::from("message s3.inventory {\n");
    for c in &cols {
        let field = match c.kind {
            PKind::Utf8Req => format!("required binary {} (UTF8);", c.name),
            PKind::Utf8Opt => format!("optional binary {} (UTF8);", c.name),
            PKind::BoolOpt => format!("optional boolean {};", c.name),
            PKind::Int64Opt => format!("optional int64 {};", c.name),
            PKind::TimestampOpt => format!("optional int64 {} (TIMESTAMP_MILLIS);", c.name),
        };
        out.push_str("  ");
        out.push_str(&field);
        out.push('\n');
    }
    out.push('}');
    Ok(out)
}

struct ParquetSink {
    writer: SerializedFileWriter<HashingWriter<std::io::BufWriter<std::fs::File>>>,
    cols: Vec<PCol>,
    rows: Vec<Fields>,
}

impl ParquetSink {
    fn new(
        file: std::fs::File,
        columns: &[&'static str],
        size: Arc<AtomicU64>,
    ) -> Result<Self, String> {
        let cols = parquet_columns(columns)?;
        let schema = Arc::new(parquet_schema(&cols)?);
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let writer = SerializedFileWriter::new(
            HashingWriter::new(std::io::BufWriter::new(file), size),
            schema,
            props,
        )
        .map_err(|e| format!("Parquet writer 初始化失败：{e}"))?;
        Ok(Self {
            writer,
            cols,
            rows: Vec::with_capacity(PARQUET_ROW_GROUP_ROWS),
        })
    }

    fn write(&mut self, f: &Fields) -> Result<(), String> {
        self.rows.push(f.clone());
        if self.rows.len() >= PARQUET_ROW_GROUP_ROWS {
            self.flush_group()?;
        }
        Ok(())
    }

    fn flush_group(&mut self) -> Result<(), String> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let mut rg = self
            .writer
            .next_row_group()
            .map_err(|e| format!("Parquet 行组写入失败：{e}"))?;
        for col in &self.cols {
            let Some(mut cw) = rg
                .next_column()
                .map_err(|e| format!("Parquet 列写入失败：{e}"))?
            else {
                return Err("Parquet 列数不匹配".to_string());
            };
            match col.kind {
                PKind::Utf8Req => {
                    let values: Vec<ByteArray> = rows
                        .iter()
                        .map(|f| {
                            ByteArray::from(parquet_string(col.csv, f).unwrap_or_default().as_str())
                        })
                        .collect();
                    cw.typed::<ByteArrayType>()
                        .write_batch(&values, None, None)
                        .map_err(|e| format!("Parquet 写入失败：{e}"))?;
                }
                PKind::Utf8Opt => {
                    let mut values = Vec::with_capacity(rows.len());
                    let mut defs = Vec::with_capacity(rows.len());
                    for f in &rows {
                        match parquet_string(col.csv, f) {
                            Some(s) => {
                                values.push(ByteArray::from(s.as_str()));
                                defs.push(1);
                            }
                            None => defs.push(0),
                        }
                    }
                    cw.typed::<ByteArrayType>()
                        .write_batch(&values, Some(&defs), None)
                        .map_err(|e| format!("Parquet 写入失败：{e}"))?;
                }
                PKind::BoolOpt => {
                    let mut values = Vec::with_capacity(rows.len());
                    let mut defs = Vec::with_capacity(rows.len());
                    for f in &rows {
                        match parquet_bool(col.csv, f) {
                            Some(v) => {
                                values.push(v);
                                defs.push(1);
                            }
                            None => defs.push(0),
                        }
                    }
                    cw.typed::<BoolType>()
                        .write_batch(&values, Some(&defs), None)
                        .map_err(|e| format!("Parquet 写入失败：{e}"))?;
                }
                PKind::Int64Opt | PKind::TimestampOpt => {
                    let mut values = Vec::with_capacity(rows.len());
                    let mut defs = Vec::with_capacity(rows.len());
                    for f in &rows {
                        match parquet_i64(col.csv, f) {
                            Some(v) => {
                                values.push(v);
                                defs.push(1);
                            }
                            None => defs.push(0),
                        }
                    }
                    cw.typed::<Int64Type>()
                        .write_batch(&values, Some(&defs), None)
                        .map_err(|e| format!("Parquet 写入失败：{e}"))?;
                }
            }
            cw.close().map_err(|e| format!("Parquet 列收尾失败：{e}"))?;
        }
        rg.close()
            .map_err(|e| format!("Parquet 行组收尾失败：{e}"))?;
        Ok(())
    }

    fn finish(mut self) -> Result<(String, u64), String> {
        self.flush_group()?;
        let w = self
            .writer
            .into_inner()
            .map_err(|e| format!("Parquet 收尾失败：{e}"))?;
        let (_, md5, size) = w.finish().map_err(|e| format!("staging 写入失败：{e}"))?;
        Ok((md5, size))
    }
}

fn parquet_string(col: &str, f: &Fields) -> Option<String> {
    match col {
        "Bucket" => Some(f.bucket.clone()),
        "Key" => Some(f.key.clone()),
        "VersionId" => (!f.version_id.is_empty()).then(|| f.version_id.clone()),
        "ETag" => f.etag.clone(),
        "StorageClass" if f.delete_marker => None,
        "StorageClass" => Some(
            f.storage_class
                .clone()
                .unwrap_or_else(|| "STANDARD".to_string()),
        ),
        "ReplicationStatus" => f.replication.clone(),
        "EncryptionStatus" if f.delete_marker => None,
        "EncryptionStatus" => f.encryption.clone(),
        "ObjectLockMode" => f.lock_mode.clone(),
        "ObjectLockLegalHoldStatus" if f.delete_marker => None,
        "ObjectLockLegalHoldStatus" => Some(if f.legal_hold { "ON" } else { "OFF" }.to_string()),
        "ChecksumAlgorithm" => f.checksum_algorithm.clone(),
        _ => None,
    }
}

fn parquet_bool(col: &str, f: &Fields) -> Option<bool> {
    match col {
        "IsLatest" => Some(f.is_latest),
        "DeleteMarker" => Some(f.delete_marker),
        _ => None,
    }
}

fn parquet_i64(col: &str, f: &Fields) -> Option<i64> {
    match col {
        "Size" => f.size.and_then(|v| i64::try_from(v).ok()),
        "LastModifiedDate" => Some(f.mod_time_nanos.div_euclid(1_000_000)),
        "ObjectLockRetainUntilDate" => f.lock_until_nanos.map(|n| n.div_euclid(1_000_000)),
        _ => None,
    }
}

/// Hive 分区时间戳（UTC，AWS `dt=YYYY-MM-DD-HH-MM`）。
fn hive_partition(now: i64) -> String {
    let dt =
        time::OffsetDateTime::from_unix_timestamp(now).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute()
    )
}

fn check_size(size: &AtomicU64) -> Result<(), String> {
    // gzip 内部缓冲可能滞后 ≤32KB；5 GiB 门槛不受影响。
    if size.load(Ordering::Relaxed) > MAX_FILE_BYTES {
        return Err("单文件超过 5 GiB 上限".to_string());
    }
    Ok(())
}

/// 一行 CSV 的元数据快照（与列顺序解耦）。
#[derive(Default, Clone)]
struct Fields {
    bucket: String,
    key: String,
    version_id: String,
    is_latest: bool,
    delete_marker: bool,
    size: Option<u64>,
    mod_time_nanos: i64,
    etag: Option<String>,
    storage_class: Option<String>,
    replication: Option<String>,
    encryption: Option<String>,
    lock_until_nanos: Option<i64>,
    lock_mode: Option<String>,
    legal_hold: bool,
    checksum_algorithm: Option<String>,
}

fn encryption_status(rec: Option<&EncryptionRecord>) -> String {
    match rec {
        None => "NOT-ENCRYPTED".to_string(),
        Some(r) if r.key_id == "sse-c" => "SSE-C".to_string(),
        Some(r) if r.key_id == "file" => "SSE-S3".to_string(),
        Some(_) => "SSE-KMS".to_string(),
    }
}

fn current_fields(bucket: &str, info: &ObjectInfo) -> Fields {
    Fields {
        bucket: bucket.to_string(),
        key: info.key.clone(),
        version_id: info.version_id.clone().unwrap_or_default(),
        is_latest: true,
        delete_marker: false,
        size: Some(info.size),
        mod_time_nanos: info.last_modified_nanos,
        etag: Some(info.etag.clone()),
        storage_class: info.storage_class.clone(),
        replication: info.replication_status.clone(),
        encryption: Some(encryption_status(info.encryption.as_ref())),
        lock_until_nanos: info.lock.as_ref().and_then(|l| l.retain_until_nanos),
        lock_mode: info.lock.as_ref().and_then(|l| l.mode.clone()),
        legal_hold: info.lock.as_ref().is_some_and(|l| l.legal_hold),
        checksum_algorithm: info.checksum.as_ref().map(|c| c.algorithm.clone()),
    }
}

fn version_fields(
    bucket: &str,
    e: &xl_store::store::VersionEntry,
    head: Option<&ObjectInfo>,
) -> Fields {
    let lock: Option<&ObjectLockRecord> = head.and_then(|h| h.lock.as_ref());
    let checksum: Option<&ChecksumRecord> = head.and_then(|h| h.checksum.as_ref());
    // 无客户钥的 head 只会因 SSE-C 失败；加密列由此仍可区分出 SSE-C。
    let encryption = if e.is_marker {
        None
    } else if !e.encrypted {
        Some("NOT-ENCRYPTED".to_string())
    } else {
        Some(match head {
            Some(h) => encryption_status(h.encryption.as_ref()),
            None => "SSE-C".to_string(),
        })
    };
    Fields {
        bucket: bucket.to_string(),
        key: e.key.clone(),
        version_id: e.version_id.clone(),
        is_latest: e.is_latest,
        delete_marker: e.is_marker,
        size: (!e.is_marker).then_some(e.size),
        mod_time_nanos: e.mod_time_nanos,
        etag: (!e.is_marker).then(|| e.etag.clone()),
        storage_class: (!e.is_marker).then(|| e.storage_class.clone()).flatten(),
        replication: e.replication_status.clone(),
        encryption,
        lock_until_nanos: lock.and_then(|l| l.retain_until_nanos),
        lock_mode: lock.and_then(|l| l.mode.clone()),
        legal_hold: lock.is_some_and(|l| l.legal_hold),
        checksum_algorithm: checksum.map(|c| c.algorithm.clone()),
    }
}

fn render(col: &str, f: &Fields) -> String {
    match col {
        "Bucket" => f.bucket.clone(),
        "Key" => f.key.clone(),
        "VersionId" => f.version_id.clone(),
        "IsLatest" => bool_csv(f.is_latest),
        "DeleteMarker" => bool_csv(f.delete_marker),
        "Size" => f.size.map(|s| s.to_string()).unwrap_or_default(),
        "LastModifiedDate" => iso8601_millis_nanos(f.mod_time_nanos),
        "ETag" => f.etag.clone().unwrap_or_default(),
        "StorageClass" if f.delete_marker => String::new(),
        "StorageClass" => f
            .storage_class
            .clone()
            .unwrap_or_else(|| "STANDARD".to_string()),
        "ReplicationStatus" => f.replication.clone().unwrap_or_default(),
        "EncryptionStatus" if f.delete_marker => String::new(),
        "EncryptionStatus" => f.encryption.clone().unwrap_or_default(),
        "ObjectLockRetainUntilDate" => f
            .lock_until_nanos
            .map(iso8601_millis_nanos)
            .unwrap_or_default(),
        "ObjectLockMode" => f.lock_mode.clone().unwrap_or_default(),
        "ObjectLockLegalHoldStatus" if f.delete_marker => String::new(),
        "ObjectLockLegalHoldStatus" => if f.legal_hold { "ON" } else { "OFF" }.to_string(),
        "ChecksumAlgorithm" => f.checksum_algorithm.clone().unwrap_or_default(),
        other => other.to_string(),
    }
}

fn bool_csv(v: bool) -> String {
    if v { "TRUE" } else { "FALSE" }.to_string()
}

fn iso8601_millis(secs: i64, ms: u32) -> String {
    let dt =
        time::OffsetDateTime::from_unix_timestamp(secs).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

fn iso8601_millis_nanos(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let ms = nanos.rem_euclid(1_000_000_000) / 1_000_000;
    iso8601_millis(secs, ms as u32)
}

fn join_key(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|s| s.trim_matches('/'))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// 写侧哈希/计数（gzip 输入侧，统计的是压缩后字节）。
struct HashingWriter<W> {
    inner: W,
    hasher: Md5,
    size: Arc<AtomicU64>,
}

impl<W: std::io::Write> HashingWriter<W> {
    fn new(inner: W, size: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            hasher: Md5::new(),
            size,
        }
    }

    fn finish(mut self) -> std::io::Result<(W, String, u64)> {
        self.inner.flush()?;
        Ok((
            self.inner,
            base64::engine::general_purpose::STANDARD.encode(self.hasher.finalize()),
            self.size.load(Ordering::Relaxed),
        ))
    }
}

impl<W: std::io::Write> std::io::Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.size.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[async_trait::async_trait]
impl TaskExecutor for InventoryRunner {
    async fn execute(&self, action: &str, payload: &str) -> Result<String, String> {
        if action != "Inventory" {
            return Err(format!("未注册的作业动作: {action}"));
        }
        let p: InventoryPayload =
            serde_json::from_str(payload).map_err(|e| format!("payload 非法: {e}"))?;
        self.run(&p).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_keys() {
        // 2021-01-01T00:00:00Z 属于 ISO 2020-W53
        assert_eq!(daily_key(1_609_459_200), "2021-01-01");
        assert_eq!(weekly_key(1_609_459_200), "2020-W53");
        assert_eq!(weekly_key(1_609_372_800), "2020-W53"); // 2020-12-31
        assert_eq!(weekly_key(1_609_718_400), "2021-W01"); // 2021-01-04
        assert_eq!(daily_key(0), "1970-01-01");
    }

    #[test]
    fn csv_rows_and_schema() {
        use s3s::dto::*;
        let cfg = InventoryConfiguration {
            id: "c1".into(),
            destination: InventoryDestination {
                s3_bucket_destination: InventoryS3BucketDestination {
                    bucket: "d".into(),
                    format: InventoryFormat::from_static(InventoryFormat::CSV),
                    ..Default::default()
                },
            },
            included_object_versions: InventoryIncludedObjectVersions::from_static(
                InventoryIncludedObjectVersions::ALL,
            ),
            is_enabled: true,
            optional_fields: Some(vec![
                InventoryOptionalField::from_static("EncryptionStatus"),
                InventoryOptionalField::from_static("ReplicationStatus"),
            ]),
            schedule: InventorySchedule {
                frequency: InventoryFrequency::from_static(InventoryFrequency::DAILY),
            },
            ..Default::default()
        };
        assert_eq!(
            csv_columns(&cfg),
            vec![
                "Bucket",
                "Key",
                "VersionId",
                "IsLatest",
                "DeleteMarker",
                "Size",
                "LastModifiedDate",
                "ETag",
                "StorageClass",
                "ReplicationStatus",
                "EncryptionStatus",
            ]
        );
        let f = Fields {
            bucket: "b".into(),
            key: "k,1".into(),
            version_id: "v".into(),
            is_latest: true,
            size: Some(3),
            etag: Some("e".into()),
            replication: Some("COMPLETED".into()),
            encryption: Some("NOT-ENCRYPTED".into()),
            ..Default::default()
        };
        assert_eq!(render("IsLatest", &f), "TRUE");
        assert_eq!(render("Size", &f), "3");
        assert_eq!(render("StorageClass", &f), "STANDARD");
        assert_eq!(render("EncryptionStatus", &f), "NOT-ENCRYPTED");
        let marker = Fields {
            delete_marker: true,
            ..Default::default()
        };
        assert_eq!(render("Size", &marker), "");
        assert_eq!(render("ETag", &marker), "");
        assert_eq!(render("StorageClass", &marker), "");
    }

    #[test]
    fn destination_key_join() {
        assert_eq!(join_key(&["", "b", "c", "2026-10-20"]), "b/c/2026-10-20");
        assert_eq!(join_key(&["inv/", "b", "c", "k"]), "inv/b/c/k");
        assert_eq!(join_key(&["/inv//", "b", "c", "k"]), "inv/b/c/k");
    }

    #[test]
    fn iso_format_has_millis() {
        assert_eq!(iso8601_millis(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            iso8601_millis_nanos(1_000_000_123),
            "1970-01-01T00:00:01.000Z"
        );
    }
}
