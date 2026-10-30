//! 在途 MPU 索引（LMU 加速；advisory）：本机各桶的 `(key, 可见 upload_id)` 表。
//!
//! 只索引**本机**暂存区（集群 LMU 各节点各自一页，见 uploads.rs 头注）。首次列举某桶时
//! 扫对象目录一次，之后 create/abort/complete 增量维护；构建失败回退逐目录扫描。
//! 加载期间的钩子记 delta，加载完成时在同一写锁内合并（并发 create/abort 不丢）。
//! `clean` 删除暂存目录时按 UUID 同步移除索引；加载期间删除记入 pending，合并扫描结果时
//! 应用（clean/加载竞态收敛）。索引另有 TTL（`OSS_MPU_INDEX_TTL_SECS`，默认 300s）整桶
//! 重建，纠正任何漏钩子的陈旧项。

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::layout;

use super::Store;
use super::key_cache::{drop_entry, map_lock, read_lock, write_lock};
use super::mpu::mpu_summary;
use super::mpu_route::{format_upload_id, split_upload_id};

#[derive(Clone)]
pub(in crate::store) struct UploadEntry {
    pub initiated_nanos: i64,
    pub storage_class: Option<String>,
}

#[derive(Default)]
struct UploadsData {
    entries: BTreeMap<(String, String), UploadEntry>,
    loaded: bool,
    /// 上次成功扫描时刻（TTL 重建用；None = 尚未成功加载）。
    loaded_at: Option<Instant>,
    loading: bool,
    disabled: bool,
    /// 加载期间钩子记下的增量 `((key, upload_id), Some=新增/None=删除)`。
    delta: Vec<((String, String), Option<UploadEntry>)>,
    /// 加载期间 `clean` 删除的暂存目录 `(key, uuid)`，扫描结果合并后应用。
    pending_removals: Vec<(String, String)>,
}

#[derive(Default)]
pub(in crate::store) struct BucketUploads {
    data: RwLock<UploadsData>,
    load_lock: tokio::sync::Mutex<()>,
}

pub(in crate::store) struct MpuIndex {
    buckets: Mutex<HashMap<String, Arc<BucketUploads>>>,
    /// 索引 TTL：到点整桶重建；0 = 不周期重建（仅靠钩子）。
    ttl: Duration,
}

impl MpuIndex {
    pub(in crate::store) fn new(ttl: Duration) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    fn entry(&self, bucket: &str) -> Arc<BucketUploads> {
        map_lock(&self.buckets)
            .entry(bucket.to_string())
            .or_default()
            .clone()
    }

    fn existing(&self, bucket: &str) -> Option<Arc<BucketUploads>> {
        map_lock(&self.buckets).get(bucket).cloned()
    }

    pub(in crate::store) fn invalidate(&self, bucket: &str) {
        map_lock(&self.buckets).remove(bucket);
    }

    /// `Some(Some(_))` = 已加载且未过 TTL；`Some(None)` = 禁用；`None` = 需要构建。
    fn ready(&self, bk: &Arc<BucketUploads>) -> Option<Option<Arc<BucketUploads>>> {
        let d = read_lock(&bk.data);
        if d.disabled {
            return Some(None);
        }
        let fresh = d
            .loaded_at
            .is_some_and(|t| self.ttl.is_zero() || t.elapsed() < self.ttl);
        if d.loaded && fresh {
            return Some(Some(bk.clone()));
        }
        None
    }

    /// 列举前取可用索引（必要时一次性构建）。`None` = 回退逐目录扫描。
    pub(in crate::store) async fn for_listing(
        &self,
        drives: &[PathBuf],
        bucket: &str,
    ) -> Option<Arc<BucketUploads>> {
        let bk = self.entry(bucket);
        if let Some(out) = self.ready(&bk) {
            return out;
        }
        let _load = bk.load_lock.lock().await;
        if let Some(out) = self.ready(&bk) {
            return out;
        }
        write_lock(&bk.data).loading = true;
        let scanned = scan_uploads(drives, bucket).await;
        let mut d = write_lock(&bk.data);
        d.loading = false;
        match scanned {
            Ok(entries) => {
                merge_scan(&mut d, entries);
                d.loaded = true;
                d.loaded_at = Some(Instant::now());
                Some(bk.clone())
            }
            Err(e) => {
                d.delta.clear();
                d.pending_removals.clear();
                if matches!(e, Error::NoSuchBucket(_)) {
                    // 本机缺桶目录（集群常态/已删桶）：丢弃条目，下次列举重试（lazy-heal）
                    drop(d);
                    drop_entry(&self.buckets, bucket, &bk);
                    None
                } else if d.loaded_at.is_some() {
                    // 已加载过：旧索引继续服务，按 TTL 节流重试（不把 LMU 打挂）
                    tracing::warn!(bucket, error = %e, "MPU 索引重建失败，沿用旧索引并按 TTL 重试");
                    d.loaded_at = Some(Instant::now());
                    Some(bk.clone())
                } else {
                    d.disabled = true;
                    tracing::warn!(bucket, error = %e, "MPU 索引构建失败，回退逐目录扫描");
                    None
                }
            }
        }
    }

    /// 提交钩子（未加载/禁用 → no-op：首次扫描读盘真相；加载中记 delta）。
    fn update(&self, bucket: &str, id: (String, String), entry: Option<UploadEntry>) {
        let Some(bk) = self.existing(bucket) else {
            return;
        };
        let mut d = write_lock(&bk.data);
        if d.disabled {
            return;
        }
        if d.loading {
            d.delta.push((id, entry));
        } else if d.loaded {
            match entry {
                Some(e) => {
                    d.entries.insert(id, e);
                }
                None => {
                    d.entries.remove(&id);
                }
            }
        }
    }

    pub(in crate::store) fn add(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        initiated_nanos: i64,
        storage_class: Option<String>,
    ) {
        let id = (key.to_string(), upload_id.to_string());
        let entry = UploadEntry {
            initiated_nanos,
            storage_class,
        };
        self.update(bucket, id, Some(entry));
    }

    pub(in crate::store) fn remove(&self, bucket: &str, key: &str, upload_id: &str) {
        self.update(bucket, (key.to_string(), upload_id.to_string()), None);
    }

    /// `clean` 按 mtime 删除暂存目录后的索引钩子（只按目录 UUID 匹配，属主前缀未知）。
    /// 加载中记入 pending，扫描结果合并时再删（否则扫描与删除并发会把已删目录重新收录）。
    pub(in crate::store) fn remove_dir(&self, bucket: &str, key: &str, uuid: &str) {
        let Some(bk) = self.existing(bucket) else {
            return;
        };
        let mut d = write_lock(&bk.data);
        if d.disabled {
            return;
        }
        if d.loading {
            d.pending_removals.push((key.to_string(), uuid.to_string()));
        } else if d.loaded {
            remove_uuid(&mut d, key, uuid);
        }
    }

    /// 测试用：把已加载桶的 TTL 推到过期，避免依赖真实 sleep。
    #[cfg(test)]
    pub(in crate::store) fn expire_for_test(&self, bucket: &str) {
        let Some(bk) = self.existing(bucket) else {
            return;
        };
        let mut d = write_lock(&bk.data);
        if let Some(t) = d.loaded_at {
            d.loaded_at = Some(t - self.ttl - Duration::from_secs(1));
        }
    }
}

/// 扫描结果合并：先应用加载期间钩子的 delta，再应用 `clean` 的 pending 删除
/// （扫描可能读到随后被删的目录，pending 必须最后应用）。
fn merge_scan(d: &mut UploadsData, entries: BTreeMap<(String, String), UploadEntry>) {
    d.entries = entries;
    for (id, entry) in std::mem::take(&mut d.delta) {
        match entry {
            Some(e) => {
                d.entries.insert(id, e);
            }
            None => {
                d.entries.remove(&id);
            }
        }
    }
    for (key, uuid) in std::mem::take(&mut d.pending_removals) {
        remove_uuid(d, &key, &uuid);
    }
}

/// 按暂存目录 UUID 删除索引项（upload_id 可能带属主前缀）。
fn remove_uuid(d: &mut UploadsData, key: &str, uuid: &str) {
    let suffix = format!(".{uuid}");
    d.entries
        .retain(|(k, id), _| !(k == key && (id == uuid || id.ends_with(&suffix))));
}

/// 扫本地各盘的对象目录，收集活暂存区（含 `mpu.meta` 的 UUID 子目录）。
async fn scan_uploads(
    drives: &[PathBuf],
    bucket: &str,
) -> Result<BTreeMap<(String, String), UploadEntry>> {
    let mut out = BTreeMap::new();
    let mut seen = false;
    for drive in drives {
        let mut entries = match tokio::fs::read_dir(layout::bucket_dir(drive, bucket)).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        seen = true;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = match layout::key_from_dir_name(&name) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(dir = ?name, error = %e, "跳过非法对象目录");
                    continue;
                }
            };
            let mut subs = match tokio::fs::read_dir(entry.path()).await {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(sub) = subs.next_entry().await? {
                if !sub.file_type().await?.is_dir() {
                    continue;
                }
                let sub_name = sub.file_name().to_string_lossy().into_owned();
                // 暂存区目录名 = 规范 UUID；版本目录（xl.meta）在这一步被排除
                let Ok(id) = uuid::Uuid::parse_str(&sub_name) else {
                    continue;
                };
                match mpu_summary(&sub.path()).await {
                    Ok(Some(sum)) => {
                        // 集群：客户端可见 upload_id 带属主前缀（目录名仍是 UUID）
                        let id_str = id.to_string();
                        let upload_id = match sum.owner.as_deref() {
                            Some(owner) => format_upload_id(owner, &id_str),
                            None => id_str,
                        };
                        out.entry((key.clone(), upload_id)).or_insert(UploadEntry {
                            initiated_nanos: sum.created_nanos,
                            storage_class: sum.storage_class,
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(dir = ?sub_name, error = %e, "跳过损坏的 MPU 暂存区")
                    }
                }
            }
        }
    }
    if !seen {
        return Err(Error::NoSuchBucket(bucket.to_string()));
    }
    Ok(out)
}

// ---------- Store 接线 ----------

impl Store {
    /// LMU 索引快照（必要时构建）。`None` = 回退逐目录扫描。
    pub(in crate::store) async fn mpu_index_snapshot(
        &self,
        bucket: &str,
    ) -> Option<BTreeMap<(String, String), UploadEntry>> {
        let bk = self.mpu_index.for_listing(&self.cfg.drives, bucket).await?;
        let d = read_lock(&bk.data);
        if d.disabled || !d.loaded {
            return None;
        }
        Some(d.entries.clone())
    }

    pub(in crate::store) fn mpu_index_add(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        initiated_nanos: i64,
        storage_class: Option<String>,
    ) {
        self.mpu_index
            .add(bucket, key, upload_id, initiated_nanos, storage_class);
    }

    pub(in crate::store) fn mpu_index_remove(&self, bucket: &str, key: &str, upload_id: &str) {
        self.mpu_index.remove(bucket, key, upload_id);
    }

    pub(in crate::store) fn mpu_index_remove_dir(&self, bucket: &str, key: &str, uuid: &str) {
        self.mpu_index.remove_dir(bucket, key, uuid);
    }

    /// 测试用：强制 MPU 索引 TTL 过期，下一次列举触发重建。
    #[cfg(test)]
    pub(in crate::store) fn mpu_index_expire(&self, bucket: &str) {
        self.mpu_index.expire_for_test(bucket);
    }
}

/// `upload_id`（客户端可见形态，可带属主前缀）→ 索引键用的规范可见形态。
pub(in crate::store) fn visible_upload_id(upload_id: &str) -> Option<String> {
    let (owner, uuid) = split_upload_id(upload_id).ok()?;
    Some(match owner {
        Some(owner) => format_upload_id(&owner, &uuid),
        None => uuid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> UploadEntry {
        UploadEntry {
            initiated_nanos: 1,
            storage_class: None,
        }
    }

    /// clean 在加载期间删目录：pending 删除必须在扫描结果合并后生效（不重新收录）。
    #[test]
    fn clean_during_load_is_applied_after_scan_merge() {
        let idx = MpuIndex::new(Duration::from_secs(300));
        let bk = idx.entry("alpha");
        write_lock(&bk.data).loading = true; // 扫描进行中
        idx.remove_dir("alpha", "k", "u1");
        let mut scanned = BTreeMap::new();
        scanned.insert(("k".into(), "owner.u1".into()), entry());
        scanned.insert(("k".into(), "u2".into()), entry());
        let mut d = write_lock(&bk.data);
        merge_scan(&mut d, scanned);
        assert_eq!(
            d.entries.keys().cloned().collect::<Vec<_>>(),
            vec![("k".to_string(), "u2".to_string())],
            "clean 期间删除的暂存区不得被加载结果重新收录"
        );
        assert!(d.pending_removals.is_empty());
    }

    /// TTL 到点后 ready() 要求重建；未到点直接可用。
    #[test]
    fn ttl_expiry_requires_rebuild() {
        let idx = MpuIndex::new(Duration::from_secs(300));
        let bk = idx.entry("alpha");
        {
            let mut d = write_lock(&bk.data);
            d.loaded = true;
            d.loaded_at = Some(Instant::now());
        }
        assert!(idx.ready(&bk).is_some(), "未过 TTL 应直接可用");
        idx.expire_for_test("alpha");
        assert!(idx.ready(&bk).is_none(), "过 TTL 必须触发重建");
    }
}
