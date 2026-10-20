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
use s3s::dto::InventoryConfiguration;
use xl_s3::access::{Acl, AuthedUser};
use xl_s3::inventory::{csv_columns, filter_prefix, includes_all_versions};
use xl_s3::sse::enc_spec_from_default;
use xl_store::meta::{ChecksumRecord, EncryptionRecord, ObjectLockRecord};
use xl_store::store::{ByteStream, ListRequest, ObjectInfo, Store};

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
        let cfg: InventoryConfiguration = match serde_json::from_str(&cfg_row.config_json) {
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
        let dest = cfg.destination.s3_bucket_destination.bucket.clone();
        let is_admin = self
            .db
            .get_user(cfg_row.owner_id)
            .await
            .map_err(|e| e.to_string())?
            .map(|u| u.1)
            .unwrap_or(false);
        let owner = AuthedUser {
            uid: cfg_row.owner_id,
            is_admin,
            ak: String::new(),
        };
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
        let data_key = format!("{base}/data/{}.csv.gz", uuid::Uuid::new_v4());
        let manifest_key = format!("{base}/manifest.json");
        // I4：运行期以**实际键**复核 PutObject（融合桶策略，显式 Deny 优先）。
        for key in [&data_key, &manifest_key] {
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
                &data_key,
                &manifest_key,
                &tmp,
                run.job_id,
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
                self.db
                    .inventory_run_finish(run.id, "done", Some(&manifest_key), None)
                    .await
                    .map_err(|e| e.to_string())?;
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
        data_key: &str,
        manifest_key: &str,
        tmp: &Path,
        job_id: Option<i64>,
        scanned: &mut i64,
    ) -> Result<(), String> {
        let columns = csv_columns(cfg);
        let filter = filter_prefix(cfg).to_string();
        let size = Arc::new(AtomicU64::new(0));
        let file = std::fs::File::create(tmp).map_err(|e| format!("建 staging 文件失败：{e}"))?;
        let mut gz = GzEncoder::new(
            HashingWriter::new(std::io::BufWriter::new(file), Arc::clone(&size)),
            flate2::Compression::default(),
        );
        {
            let mut w = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(&mut gz);
            w.write_record(&columns)
                .map_err(|e| format!("CSV 头写入失败：{e}"))?;
            if includes_all_versions(cfg) {
                self.scan_versions(
                    source_bucket,
                    &columns,
                    &filter,
                    &mut w,
                    &size,
                    job_id,
                    scanned,
                )
                .await?;
            } else {
                self.scan_current(
                    source_bucket,
                    &columns,
                    &filter,
                    &mut w,
                    &size,
                    job_id,
                    scanned,
                )
                .await?;
            }
            w.flush().map_err(|e| format!("CSV 写入失败：{e}"))?;
        }
        let (_, md5, gz_size) = gz
            .finish()
            .map_err(|e| format!("gzip 收尾失败：{e}"))?
            .finish()
            .map_err(|e| format!("staging 写入失败：{e}"))?;
        if gz_size > MAX_FILE_BYTES {
            return Err(format!("单文件 {gz_size} 字节超过 5 GiB 上限"));
        }
        let versioned = self
            .db
            .get_bucket_versioning(dest)
            .await
            .map_err(|e| e.to_string())?
            == "Enabled";
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
        self.store
            .put_object_stream_with_meta(
                dest,
                data_key,
                body,
                Some("application/gzip".to_string()),
                BTreeMap::new(),
                None,
                versioned,
                encryption.clone(),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| format!("数据文件写入目标桶失败：{e}"))?;
        let manifest = serde_json::json!({
            "sourceBucket": source_bucket,
            "destinationBucket": dest,
            "version": "2016-11-30",
            "creationTimestamp": iso8601_millis(crate::db::now(), 0),
            "fileFormat": "CSV",
            "fileSchema": columns.join(","),
            "files": [{"key": data_key, "size": gz_size, "md5Checksum": md5}],
        });
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
                versioned,
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
        columns: &[&str],
        filter: &str,
        w: &mut CsvSink<'_>,
        size: &AtomicU64,
        job_id: Option<i64>,
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
                write_row(w, columns, &current_fields(bucket, info))?;
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
        w: &mut CsvSink<'_>,
        size: &AtomicU64,
        job_id: Option<i64>,
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
                // 不足字段按需 head（成本：每个版本一次 meta 读；仅在请求了这些列时发生）。
                let head = if need_head && !e.is_marker {
                    self.store
                        .head_object_version(bucket, &e.key, &e.version_id)
                        .await
                        .ok()
                } else {
                    None
                };
                write_row(w, columns, &version_fields(bucket, e, head.as_ref()))?;
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

type GzSink = GzEncoder<HashingWriter<std::io::BufWriter<std::fs::File>>>;
type CsvSink<'a> = csv::Writer<&'a mut GzSink>;

fn check_size(size: &AtomicU64) -> Result<(), String> {
    // gzip 内部缓冲可能滞后 ≤32KB；5 GiB 门槛不受影响。
    if size.load(Ordering::Relaxed) > MAX_FILE_BYTES {
        return Err("单文件超过 5 GiB 上限".to_string());
    }
    Ok(())
}

/// 一行 CSV 的元数据快照（与列顺序解耦）。
#[derive(Default)]
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

fn write_row(w: &mut CsvSink<'_>, columns: &[&str], f: &Fields) -> Result<(), String> {
    let row: Vec<String> = columns.iter().map(|c| render(c, f)).collect();
    w.write_record(&row)
        .map_err(|e| format!("CSV 行写入失败：{e}"))
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
