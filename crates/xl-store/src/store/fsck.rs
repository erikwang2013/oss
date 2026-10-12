//! fsck：上盘巡检（M2b-2 Task 10 起步版；设计 §4.1 预留的 `.oss.sys/fsck/` 报告目录在此启用）。
//!
//! 巡检三类对象（只读报告；`--repair` 才写）：
//!   a) tmp/trash 条目计数（回收交给 `clean`，fsck 只报数）；
//!   b) 孤儿版本目录：对象目录下未被任何版本引用的子目录（崩溃残骸 / 未完成 MPU）；
//!   c) EC 分片缺/坏计数：逐对象按 `DataLayout.shards` 校验文件存在性与 CRC32C。
//! `--repair`：删孤儿目录 + 对缺/坏片的对象走一次 `get_object`（触发读时修回写）。
//!
//! **裁决**：独立的 `scrub` 全量深度巡检仍是 M4（设计验收顺延）；本命令是「找出问题 + 最小修复」，
//! 报告 JSON 由调用方（oss-server cli）序列化落盘——xl-store 不引 serde_json。

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use serde::Serialize;

use crate::error::Result;
use crate::layout;
use crate::store::{Store, drives, ec};

/// 巡检报告（JSON 序列化形态由调用方决定）。
#[derive(Debug, Default, Serialize)]
pub struct FsckReport {
    pub drives: usize,
    pub objects_scanned: usize,
    /// xl.meta 全部副本都读不出（损坏）的对象数
    pub corrupt_meta: usize,
    /// tmp 目录条目数（每盘合计；清理由 clean/启动自检负责）
    pub tmp_entries: usize,
    /// trash 目录条目数（同上）
    pub trash_entries: usize,
    /// 孤儿版本目录（不可见残骸）：`<盘下标>#<bucket>/<key>/<uuid>`
    pub orphan_dirs: Vec<String>,
    /// 缺失的 EC 分片数（文件不存在或盘不在册）
    pub missing_shards: usize,
    /// CRC32C 不符的 EC 分片数
    pub bad_shards: usize,
    /// 是否执行了修复（删孤儿 + 读时修）
    pub repaired: bool,
}

impl Store {
    /// 全盘巡检。`repair=true` 时执行最小修复（删孤儿 + 触发读时修）。
    pub async fn fsck(&self, repair: bool) -> Result<FsckReport> {
        let mut r = FsckReport {
            drives: self.cfg.drives.len(),
            repaired: repair,
            ..Default::default()
        };

        // a) tmp/trash 计数（每盘各自的 sys 目录）
        for drive in &self.cfg.drives {
            for sub in ["tmp", "trash"] {
                let n = count_entries(&layout::sys_dir(drive, sub)).await?;
                match sub {
                    "tmp" => r.tmp_entries += n,
                    _ => r.trash_entries += n,
                }
            }
        }

        // b)+c) 命名空间：从所有盘收集对象键（容错——掉线盘跳过）
        let mut keys: BTreeSet<(String, String)> = BTreeSet::new();
        for drive in &self.cfg.drives {
            let mut buckets = match tokio::fs::read_dir(drive).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(bucket) = buckets.next_entry().await? {
                if !bucket.file_type().await?.is_dir() {
                    continue;
                }
                let bucket_name = bucket.file_name().to_string_lossy().into_owned();
                if bucket_name == layout::SYS_DIR {
                    continue;
                }
                let mut objects = tokio::fs::read_dir(bucket.path()).await?;
                while let Some(object) = objects.next_entry().await? {
                    if !object.file_type().await?.is_dir() {
                        continue;
                    }
                    let dir_name = object.file_name().to_string_lossy().into_owned();
                    if let Ok(key) = layout::key_from_dir_name(&dir_name) {
                        keys.insert((bucket_name.clone(), key));
                    }
                }
            }
        }

        for (bucket, key) in keys {
            r.objects_scanned += 1;
            // 引用集：所有版本（含旧 parts 兼容位）的 uuid
            let referenced: HashSet<String> = match self.read_meta(&bucket, &key).await {
                Ok(Some(m)) => m
                    .versions
                    .iter()
                    .flat_map(|v| v.effective_parts())
                    .map(|p| p.uuid)
                    .collect(),
                Ok(None) => HashSet::new(), // 无 meta = 崩在提交点前：全部子目录都是孤儿
                Err(e) => {
                    r.corrupt_meta += 1;
                    tracing::warn!(bucket = %bucket, key = %key, error = %e, "fsck：meta 读取失败");
                    continue; // 保守：损坏 meta 的对象整体跳过（与 reap_stale 同口径）
                }
            };

            // 孤儿扫描（逐盘）+ 分片巡检（按 meta 里的 DataLayout）
            for (d, drive) in self.cfg.drives.iter().enumerate() {
                let Ok(obj_dir) = layout::object_dir(drive, &bucket, &key) else {
                    continue;
                };
                let mut subs = match tokio::fs::read_dir(&obj_dir).await {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                while let Some(sub) = subs.next_entry().await? {
                    if !sub.file_type().await?.is_dir() {
                        continue;
                    }
                    let name = sub.file_name().to_string_lossy().into_owned();
                    if !referenced.contains(&name) {
                        r.orphan_dirs.push(format!("{d}#{bucket}/{key}/{name}"));
                        if repair {
                            let _ = tokio::fs::remove_dir_all(sub.path()).await;
                        }
                    }
                }
            }

            // c) 分片巡检：取任一盘的 meta 里最新版本（uuid + DataLayout）
            let layout_of = self
                .read_meta(&bucket, &key)
                .await
                .ok()
                .flatten()
                .and_then(|m| {
                    m.latest()
                        .and_then(|v| v.data_layout.clone().map(|l| (v.part.uuid.clone(), l)))
                });
            if let Some((uuid, l)) = layout_of {
                let mut miss = 0usize;
                let mut bad = 0usize;
                for s in &l.shards {
                    let Some(drive) = drives::drive_root(&self.cfg.drives, s.drive) else {
                        miss += 1;
                        continue;
                    };
                    let Ok(path) = drives::shard_path(drive, &bucket, &key, &uuid, s) else {
                        miss += 1;
                        continue;
                    };
                    match tokio::fs::read(&path).await {
                        Ok(bytes) if ec::crc32c_of(&bytes) == s.crc32c => {}
                        Ok(_) => bad += 1,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => miss += 1,
                        Err(e) => return Err(e.into()),
                    }
                }
                r.missing_shards += miss;
                r.bad_shards += bad;
                if repair && (miss > 0 || bad > 0) {
                    // 读时修：get_object 重建成功后 best-effort 回写缺/坏片
                    let _ = self.get_object(&bucket, &key).await;
                }
            }
        }
        Ok(r)
    }
}

/// 目录条目计数（不存在视为 0）。
async fn count_entries(dir: &Path) -> Result<usize> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let mut n = 0;
    while entries.next_entry().await?.is_some() {
        n += 1;
    }
    Ok(n)
}
