//! 每节点命名空间键缓存（列举加速；advisory）。
//!
//! 首次列举某桶时扫本地各盘的对象目录一次，之后按批取键，不再每页全桶扫盘。
//! 一致性靠提交钩子（note/forget/invalidate）+ TTL 整桶重建：**最终一致**——漏掉的钩子
//! 由 TTL 重建纠正；运行期绕过 Store API 手工改盘不支持。
//! 加载期间的钩子记进 delta，加载完成时在同一写锁内合并（put 与一次性加载并发也收敛）。
//! 键数超过 `max_keys`（或 `max_keys=0`）→ 该桶禁用缓存，回退 `collect_keys_filtered`
//! 的逐页扫描（含 `OSS_LIST_MAX_KEYS` 上限）。std 锁从不跨 await 持有。

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::layout;

use super::Store;

/// 缓存键批大小：列举按批取键，绝不整桶物化。
const KEY_BATCH: usize = 1024;

pub(in crate::store) fn read_lock<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|e| e.into_inner())
}

pub(in crate::store) fn write_lock<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|e| e.into_inner())
}

pub(in crate::store) fn map_lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// 仅移除仍指向同一 Arc 的条目（并发 invalidate/重建时不误删新条目）。
pub(in crate::store) fn drop_entry<T>(
    m: &Mutex<HashMap<String, Arc<T>>>,
    bucket: &str,
    bk: &Arc<T>,
) {
    let mut m = map_lock(m);
    if m.get(bucket).is_some_and(|cur| Arc::ptr_eq(cur, bk)) {
        m.remove(bucket);
    }
}

pub(in crate::store) struct KeyCache {
    max_keys: usize,
    /// 全进程键缓存总上限（多桶累加；0 = 不限）。
    max_total_keys: usize,
    ttl: Duration,
    buckets: Mutex<HashMap<String, Arc<BucketKeys>>>,
    total_keys: Mutex<usize>,
}

#[derive(Default)]
struct BucketData {
    keys: BTreeSet<String>,
    loaded_at: Option<Instant>,
    loading: bool,
    disabled: bool,
    /// 加载期间钩子记下的增量 `(key, 是否新增)`，加载完成时合并。
    delta: Vec<(String, bool)>,
}

#[derive(Default)]
pub(in crate::store) struct BucketKeys {
    data: RwLock<BucketData>,
    load_lock: tokio::sync::Mutex<()>,
}

impl KeyCache {
    pub(in crate::store) fn new(max_keys: usize, max_total_keys: usize, ttl: Duration) -> Self {
        Self {
            max_keys,
            max_total_keys,
            ttl,
            buckets: Mutex::new(HashMap::new()),
            total_keys: Mutex::new(0),
        }
    }

    fn adjust_total(&self, delta: i64) {
        let mut total = map_lock(&self.total_keys);
        *total = if delta >= 0 {
            total.saturating_add(delta as usize)
        } else {
            total.saturating_sub(delta.unsigned_abs() as usize)
        };
    }

    fn entry(&self, bucket: &str) -> Arc<BucketKeys> {
        map_lock(&self.buckets)
            .entry(bucket.to_string())
            .or_default()
            .clone()
    }

    fn existing(&self, bucket: &str) -> Option<Arc<BucketKeys>> {
        map_lock(&self.buckets).get(bucket).cloned()
    }

    fn invalidate(&self, bucket: &str) {
        if let Some(bk) = map_lock(&self.buckets).remove(bucket) {
            let len = {
                let d = read_lock(&bk.data);
                if d.disabled { 0 } else { d.keys.len() }
            };
            if len > 0 {
                self.adjust_total(-(len as i64));
            }
        }
    }

    /// `Some(Some(_))` = 已加载且未过 TTL；`Some(None)` = 禁用；`None` = 需要（重）加载。
    fn ready(&self, bk: &Arc<BucketKeys>) -> Option<Option<Arc<BucketKeys>>> {
        let d = read_lock(&bk.data);
        if d.disabled {
            return Some(None);
        }
        // TTL=0：不周期重建（只靠钩子；首次加载仍扫盘）
        let fresh = d
            .loaded_at
            .is_some_and(|t| self.ttl.is_zero() || t.elapsed() < self.ttl);
        if fresh {
            return Some(Some(bk.clone()));
        }
        None
    }

    /// 列举前取可用键集（必要时加载/重建）。`None` = 回退逐页扫描。
    pub(in crate::store) async fn for_listing(
        &self,
        drives: &[PathBuf],
        bucket: &str,
    ) -> Option<Arc<BucketKeys>> {
        if self.max_keys == 0 {
            return None;
        }
        let bk = self.entry(bucket);
        if let Some(out) = self.ready(&bk) {
            return out;
        }
        // 重建串行化：并发 listers 等同一把锁；加载中钩子记 delta，不丢更新。
        let _load = bk.load_lock.lock().await;
        if let Some(out) = self.ready(&bk) {
            return out;
        }
        write_lock(&bk.data).loading = true;
        let scanned = scan_keys(drives, bucket, self.max_keys).await;
        let mut d = write_lock(&bk.data);
        d.loading = false;
        match scanned {
            Ok(Some(keys)) => {
                let old_len = d.keys.len();
                let mut keys = keys;
                for (k, add) in std::mem::take(&mut d.delta) {
                    if add {
                        keys.insert(k);
                    } else {
                        keys.remove(&k);
                    }
                }
                let new_len = keys.len();
                if self.max_total_keys != 0 {
                    let mut total = map_lock(&self.total_keys);
                    let projected = total.saturating_sub(old_len).saturating_add(new_len);
                    if projected > self.max_total_keys {
                        d.disabled = true;
                        d.keys.clear();
                        if old_len > 0 {
                            *total = total.saturating_sub(old_len);
                        }
                        tracing::warn!(
                            bucket,
                            total = projected,
                            max_total = self.max_total_keys,
                            "全进程键缓存总量超上限，该桶列举回退逐页扫描"
                        );
                        return None;
                    }
                    *total = projected;
                }
                d.keys = keys;
                d.loaded_at = Some(Instant::now());
                Some(bk.clone())
            }
            Ok(None) => {
                if !d.keys.is_empty() {
                    let old = d.keys.len();
                    d.keys.clear();
                    self.adjust_total(-(old as i64));
                }
                d.disabled = true;
                d.delta.clear();
                tracing::warn!(
                    bucket,
                    max = self.max_keys,
                    "桶键数超过缓存上限，该桶列举回退逐页扫描"
                );
                None
            }
            Err(e) => {
                d.delta.clear();
                if d.loaded_at.is_some() {
                    // 已加载过：旧集合继续服务，按 TTL 节流重试（不把列举打挂）
                    tracing::warn!(bucket, error = %e, "键缓存重建失败，沿用旧集合并按 TTL 重试");
                    d.loaded_at = Some(Instant::now());
                    Some(bk.clone())
                } else if matches!(e, Error::NoSuchBucket(_)) {
                    // 集群里本机缺桶目录是常态（懒自愈）——不永久禁用，下次访问重试
                    drop(d);
                    drop_entry(&self.buckets, bucket, &bk);
                    None
                } else {
                    d.disabled = true;
                    tracing::warn!(bucket, error = %e, "键缓存初次加载失败，回退逐页扫描");
                    None
                }
            }
        }
    }

    #[cfg(test)]
    fn contains(&self, bucket: &str, key: &str) -> bool {
        let Some(bk) = self.existing(bucket) else {
            return false;
        };
        let d = read_lock(&bk.data);
        !d.disabled && d.loaded_at.is_some() && d.keys.contains(key)
    }

    /// 测试用：把已加载桶的 TTL 推到过期，避免依赖真实 sleep（慢机/flaky）。
    #[cfg(test)]
    fn expire_for_test(&self, bucket: &str) {
        let Some(bk) = self.existing(bucket) else {
            return;
        };
        let mut d = write_lock(&bk.data);
        if let Some(t) = d.loaded_at {
            d.loaded_at = Some(t - self.ttl - Duration::from_secs(1));
        }
    }
}

impl BucketKeys {
    /// 提交钩子：新增/删除一个键。桶未加载 → no-op（首次加载扫盘会看到磁盘真相）；
    /// 加载中 → 记 delta，加载完成合并。
    fn note(&self, key: &str, add: bool) -> i64 {
        let mut d = write_lock(&self.data);
        if d.disabled || (d.loaded_at.is_none() && !d.loading) {
            return 0;
        }
        if d.loading {
            d.delta.push((key.to_string(), add));
            0
        } else if add {
            if d.keys.insert(key.to_string()) { 1 } else { 0 }
        } else if d.keys.remove(key) {
            -1
        } else {
            0
        }
    }

    /// 取前缀内、`after` 之后的至多 `count` 个键（`inclusive` = 包含 after 本身）。
    fn batch(
        &self,
        prefix: &str,
        after: Option<&str>,
        inclusive: bool,
        count: usize,
    ) -> Vec<String> {
        use std::ops::Bound;
        let d = read_lock(&self.data);
        if d.disabled || d.loaded_at.is_none() {
            return Vec::new();
        }
        let start = match after {
            Some(a) if a >= prefix && inclusive => Bound::Included(a.to_string()),
            Some(a) if a >= prefix => Bound::Excluded(a.to_string()),
            _ => Bound::Included(prefix.to_string()),
        };
        let mut out = Vec::new();
        for k in d.keys.range((start, Bound::Unbounded)) {
            // 命中前缀的键在有序集里是连续段：首个不命中即可停
            if !k.starts_with(prefix) {
                break;
            }
            out.push(k.clone());
            if out.len() >= count {
                break;
            }
        }
        out
    }
}

/// 键流：缓存可用 → 按批取（内存 O(批)）；否则回退一次扫描（旧路径，含上限）。
pub(in crate::store) enum PageKeys {
    Cached {
        bk: Arc<BucketKeys>,
        prefix: String,
        cursor: Option<String>,
        inclusive: bool,
        buf: VecDeque<String>,
        exhausted: bool,
    },
    Scanned(std::vec::IntoIter<String>),
}

impl PageKeys {
    pub(in crate::store) fn next(&mut self) -> Option<String> {
        match self {
            PageKeys::Scanned(it) => it.next(),
            PageKeys::Cached {
                bk,
                prefix,
                cursor,
                inclusive,
                buf,
                exhausted,
            } => {
                if buf.is_empty() {
                    if *exhausted {
                        return None;
                    }
                    let batch = bk.batch(prefix, cursor.as_deref(), *inclusive, KEY_BATCH);
                    // 首批包含 start_after 本身（V2 token 包含语义），其后严格大于游标
                    *inclusive = false;
                    if batch.is_empty() {
                        *exhausted = true;
                        return None;
                    }
                    buf.extend(batch);
                }
                let k = buf.pop_front();
                if let Some(k) = &k {
                    *cursor = Some(k.clone());
                }
                k
            }
        }
    }
}

/// 扫全部本地盘的对象目录 → 键集。键数超上限 → `Ok(None)`（该桶不缓存）。
async fn scan_keys(
    drives: &[PathBuf],
    bucket: &str,
    max_keys: usize,
) -> Result<Option<BTreeSet<String>>> {
    let mut keys = BTreeSet::new();
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
            match layout::key_from_dir_name(&name) {
                Ok(k) => {
                    keys.insert(k);
                    if keys.len() > max_keys {
                        return Ok(None);
                    }
                }
                Err(e) => tracing::warn!(dir = ?name, error = %e, "跳过非法对象目录"),
            }
        }
    }
    if !seen {
        return Err(Error::NoSuchBucket(bucket.to_string()));
    }
    Ok(Some(keys))
}

// ---------- Store 接线 ----------

impl Store {
    /// 列举键流（对象列表 / 版本列表 / LMU 的键集共用）。
    pub(in crate::store) async fn page_keys(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        exclusive: bool,
    ) -> Result<PageKeys> {
        if let Some(bk) = self.key_cache.for_listing(&self.cfg.drives, bucket).await {
            return Ok(PageKeys::Cached {
                bk,
                prefix: prefix.to_string(),
                cursor: start_after.map(str::to_string),
                inclusive: !exclusive,
                buf: VecDeque::new(),
                exhausted: false,
            });
        }
        let keys = self
            .collect_keys_filtered(bucket, prefix, start_after, exclusive, true)
            .await?;
        Ok(PageKeys::Scanned(keys.into_iter()))
    }

    /// meta 提交成功后的键缓存钩子（桶未加载 → no-op）。
    pub(in crate::store) fn note_cached_key(&self, bucket: &str, key: &str) {
        if let Some(bk) = self.key_cache.existing(bucket) {
            self.key_cache.adjust_total(bk.note(key, true));
        }
    }

    /// meta 删除后的键缓存钩子。
    pub(in crate::store) fn forget_cached_key(&self, bucket: &str, key: &str) {
        if let Some(bk) = self.key_cache.existing(bucket) {
            self.key_cache.adjust_total(bk.note(key, false));
        }
    }

    /// 删桶：键集与 MPU 索引整体失效。
    pub(in crate::store) fn invalidate_bucket_caches(&self, bucket: &str) {
        self.key_cache.invalidate(bucket);
        self.mpu_index.invalidate(bucket);
    }

    /// 测试用：缓存是否已加载且含该键。
    #[cfg(test)]
    pub(in crate::store) fn key_cache_contains(&self, bucket: &str, key: &str) -> bool {
        self.key_cache.contains(bucket, key)
    }

    /// 测试用：强制键缓存 TTL 过期，下一次列举触发重建。
    #[cfg(test)]
    pub(in crate::store) fn key_cache_expire(&self, bucket: &str) {
        self.key_cache.expire_for_test(bucket);
    }
}
