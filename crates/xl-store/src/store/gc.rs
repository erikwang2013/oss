//! 垃圾回收：启动瞬时回收（tmp/trash）与 `clean` 的按龄回收（trash/tmp + 对象目录下未引用子目录）。
//!
//! 与「读路径只信 xl.meta」是同一个不变量的两面：没有任何 meta 引用的盘上残骸都已不可见，
//! 这里只是把不可见且足够旧的残骸真正删掉。

use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use crate::error::Result;
use crate::layout;
use crate::store::Store;

impl Store {
    /// 启动自检：清空 `.oss.sys/tmp/` 与 `.oss.sys/trash/`。
    ///
    /// 为什么启动时敢全清：进程刚起，内核内无在途写入——tmp 里只可能是崩溃残留，
    /// trash 里都是已被覆写、无人引用的旧版本目录。
    /// 为什么**不碰对象目录**：`<对象目录>/<uuid>/` 可能是客户端跨重启续传的 MPU 暂存区
    /// （S3 客户端允许跨服务重启续传），启动即删会让在途上传无故失败；那些交给
    /// [`Store::reap_stale`] 按 mtime 判定。
    pub(crate) async fn reap_transient(&self) -> Result<usize> {
        let mut removed = 0;
        // 多盘（M2b-2）：每盘各自的 tmp/trash；启动时无在途写入，全部可清
        for drive in &self.cfg.drives {
            for sub in ["tmp", "trash"] {
                let dir = layout::sys_dir(drive, sub);
                let mut entries = match tokio::fs::read_dir(&dir).await {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if entry.file_type().await?.is_dir() {
                        tokio::fs::remove_dir_all(&path).await?;
                    } else {
                        tokio::fs::remove_file(&path).await?;
                    }
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// `clean <days>` 的按龄回收，返回删除条目数（三类合计）：
    ///   a) `.oss.sys/trash/` 下 mtime 早于 `due` 的条目（覆写后的旧版本）；
    ///   b) `.oss.sys/tmp/` 下 mtime 早于 `due` 的条目（崩溃残留）；
    ///   c) 每个对象目录下**未被任何版本引用**、且 mtime 早于 `due` 的子目录——
    ///      含崩溃留下的孤儿版本目录与未完成的 MPU 暂存区
    ///      （规格：未完成上传占用磁盘，clean 按 mtime 回收，默认 7 天）。
    ///
    /// 读 `xl.meta` 失败（损坏/键不一致）的对象**整个跳过**：保守优先，宁可漏收也不误删活引用。
    /// 无 meta 的对象目录按「零引用」处理（崩在提交点前的残骸）；在途 MPU 靠它自己的 mtime 窗口保护。
    pub async fn reap_stale(&self, due: SystemTime) -> Result<usize> {
        // 多盘（M2b-2）：每盘各自的 tmp/trash 与命名空间都要扫
        let mut removed = 0;
        for drive in &self.cfg.drives {
            removed += self.reap_stale_sys_dir(drive, "trash", due).await?
                + self.reap_stale_sys_dir(drive, "tmp", due).await?;
        }

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
                    // 目录名重建键失败 = 杂物目录（与列举同口径：跳过）
                    let Ok(key) = layout::key_from_dir_name(&dir_name) else {
                        continue;
                    };
                    // 引用集取任意可用盘的 meta（多盘最新者胜）；单盘即既有语义
                    let referenced: HashSet<String> = match self.read_meta(&bucket_name, &key).await
                    {
                        // effective_parts：单段 → part；多段 → parts 全列表（各段 uuid 均不得回收）
                        Ok(Some(m)) => m
                            .versions
                            .iter()
                            .flat_map(|v| v.effective_parts())
                            .map(|p| p.uuid)
                            .collect(),
                        Ok(None) => HashSet::new(),
                        Err(e) => {
                            tracing::warn!(bucket = %bucket_name, key = %key, error = %e, "回收跳过：读取 xl.meta 失败");
                            continue;
                        }
                    };
                    let mut subs = tokio::fs::read_dir(object.path()).await?;
                    while let Some(sub) = subs.next_entry().await? {
                        if !sub.file_type().await?.is_dir() {
                            // R7：对象目录内的 `xl.meta.new` 残骸（翻转失败遗留）按同 mtime 窗口回收
                            if sub.file_name().to_string_lossy() == crate::meta::META_NEW_FILE {
                                let stale = sub
                                    .metadata()
                                    .await
                                    .ok()
                                    .and_then(|m| m.modified().ok())
                                    .is_some_and(|t| t < due);
                                if stale {
                                    let _ = tokio::fs::remove_file(sub.path()).await;
                                    removed += 1;
                                }
                            }
                            continue;
                        }
                        let sub_name = sub.file_name().to_string_lossy().into_owned();
                        if referenced.contains(&sub_name) {
                            continue;
                        }
                        let stale = sub
                            .metadata()
                            .await
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .is_some_and(|t| t < due);
                        if stale {
                            tokio::fs::remove_dir_all(sub.path()).await?;
                            removed += 1;
                        }
                    }
                }
            }
        }
        Ok(removed)
    }

    /// 系统目录（tmp/trash）下按 mtime 回收条目。目录不存在视为空（返回 0）。
    async fn reap_stale_sys_dir(&self, drive: &Path, sub: &str, due: SystemTime) -> Result<usize> {
        let dir = layout::sys_dir(drive, sub);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        let mut removed = 0;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let stale = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .is_some_and(|t| t < due);
            if !stale {
                continue;
            }
            if entry.file_type().await?.is_dir() {
                tokio::fs::remove_dir_all(&path).await?;
            } else {
                tokio::fs::remove_file(&path).await?;
            }
            removed += 1;
        }
        Ok(removed)
    }
}
