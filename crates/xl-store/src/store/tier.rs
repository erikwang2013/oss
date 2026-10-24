//! M8 S4b：分层元数据状态机——归档（Archived）/恢复中（Restoring）/已恢复（Restored）。
//!
//! 全部迁移都在对象锁内重写 meta（提交点）；本地数据文件的写入发生在 meta 之外
//! （prepare 阶段对读路径不可见），保证「先数据、后 row、再 meta」的恢复顺序。

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt as _;

use super::Store;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, TierChunks, TierPhase, TierState};

/// 恢复下载暂存文件名（不以 `part.` 开头：fsck 的 stray 扫描不碰它）。
const RESTORE_STAGE: &str = "restore-stage";

/// 归档提交参数（避免长参数列表）。
pub struct ArchiveSpec {
    pub backend: String,
    pub key: String,
    pub size: u64,
    /// 作业开始时源版本 ETag（并发改写检测；不与远端 ETag 混用）。
    pub source_etag: String,
    /// 远端 PUT 响应 ETag（存入 `tier.etag`）。
    pub etag: String,
    pub sha256: String,
    /// 归档存储类（GLACIER / GLACIER_IR / DEEP_ARCHIVE）。
    pub class: String,
    /// 直读分块哈希（Transition 流上增量计算；None = 不提供直读，保持 403）。
    pub chunks: Option<TierChunks>,
}

impl Store {
    /// 读某版本的完整 meta 记录（job/reaper 用；delete marker/未知版本 → None）。
    pub async fn tier_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<meta::Version>> {
        self.ensure_bucket(bucket).await?;
        let Some(m) = self.read_meta(bucket, key).await? else {
            return Ok(None);
        };
        Ok(m.find_version(version_id)
            .filter(|v| !v.delete_marker())
            .cloned())
    }

    /// 在对象锁内重写某版本；`f(old_version, new_version)` 返回 Err 时不动 meta。
    async fn mutate_version<F>(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        f: F,
    ) -> Result<meta::Version>
    where
        F: FnOnce(&meta::Version, &mut meta::Version) -> Result<()>,
    {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let Some(idx) = old
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
        else {
            return Err(Error::NoSuchKey(format!("{key}@{version_id}")));
        };
        let mut m = old.clone();
        if m.versions[idx].delete_marker() {
            return Err(Error::NoSuchKey(format!("{key}@{version_id}")));
        }
        f(&old.versions[idx], &mut m.versions[idx])?;
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        let out = m.versions[idx].clone();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        Ok(out)
    }

    /// 非归档 Transition：只改存储类元数据（不搬数据、不建 tier 记录）。
    pub async fn set_storage_class(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        class: &str,
    ) -> Result<()> {
        if meta::is_archive_class(class) {
            return Err(Error::InvalidArgument(format!(
                "归档类 {class} 必须走 archive_version"
            )));
        }
        self.mutate_version(bucket, key, version_id, |_old, v| {
            if v.tier.is_some() {
                return Err(Error::InvalidObjectState(format!(
                    "{bucket}/{key}@{version_id}: 已分层对象不得改用 {class}"
                )));
            }
            if meta::normalize_storage_class(v.storage_class.as_deref()).as_deref() != Some(class) {
                v.storage_class = Some(class.to_string());
            }
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// 归档：同 class 幂等成功；不同 class → InvalidArgument。提交后回收本地分片
    /// （其它版本仍引用同 uuid 时保留）。
    pub async fn archive_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        spec: ArchiveSpec,
        now_secs: i64,
    ) -> Result<()> {
        if !meta::is_archive_class(&spec.class) {
            return Err(Error::InvalidArgument(format!(
                "非归档类 {} 不得调用 archive_version",
                spec.class
            )));
        }
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let Some(idx) = old
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
        else {
            return Err(Error::NoSuchKey(format!("{key}@{version_id}")));
        };
        let src = &old.versions[idx];
        if src.delete_marker() {
            return Err(Error::NoSuchKey(format!("{key}@{version_id}")));
        }
        if src.tier.is_some() {
            // C7：同 class 重复 sweep 幂等；不同 class 拒绝。
            if src.storage_class.as_deref() == Some(spec.class.as_str()) {
                return Ok(());
            }
            return Err(Error::InvalidArgument(format!(
                "{bucket}/{key}@{version_id}: 已归档为 {:?}，不得改为 {}",
                src.storage_class, spec.class
            )));
        }
        if src.encryption.is_some() {
            return Err(Error::InvalidObjectState(format!(
                "{bucket}/{key}@{version_id}: SSE 对象不参与 Transition"
            )));
        }
        if src.size != spec.size || src.etag != spec.source_etag {
            return Err(Error::CorruptMeta(format!(
                "{bucket}/{key}@{version_id}: 源对象在归档期间被改写（size/etag 不符）"
            )));
        }
        // 直读分块表是信任边界：写前自洽（作业流上算出；损坏则拒绝归档，绝不落半套表）。
        if let Some(c) = &spec.chunks {
            c.validate(spec.size)
                .map_err(|e| Error::InvalidArgument(format!("分块哈希表非法：{e}")))?;
        }
        let uuid = src.part.uuid.clone();
        let mut m = old.clone();
        let v = &mut m.versions[idx];
        v.storage_class = Some(spec.class.clone());
        v.tier = Some(TierState {
            backend: spec.backend,
            key: spec.key,
            size: spec.size,
            etag: spec.etag,
            sha256: spec.sha256,
            phase: TierPhase::Archived,
            archived_at: now_secs,
            restore_expires_at: None,
            chunks: spec.chunks,
        });
        // 本地数据不再可读：清数据描述（保留 part 兼容位）。
        v.data_layout = None;
        v.inline = None;
        v.parts = None;
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        // 提交后回收本地分片；meta 其它版本仍引用同 uuid → 保留。
        if m.versions
            .iter()
            .filter(|v| !v.delete_marker() && v.part.uuid == uuid)
            .count()
            <= 1
        {
            self.recycle_version_dir(bucket, key, &uuid).await;
        } else {
            tracing::warn!(bucket, key, uuid, "归档版本目录仍被其它版本引用，跳过回收");
        }
        Ok(())
    }

    /// Restore 受理：Archived → Restoring（sink 入队前）。已 Restoring/Restored → 409。
    pub async fn mark_restore_pending(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        self.mutate_version(bucket, key, version_id, |_old, v| {
            let Some(t) = &mut v.tier else {
                return Err(Error::InvalidObjectState(format!(
                    "{bucket}/{key}@{version_id}: 非归档对象"
                )));
            };
            match t.phase {
                TierPhase::Archived => {
                    t.phase = TierPhase::Restoring;
                    t.restore_expires_at = None;
                    Ok(())
                }
                TierPhase::Restoring | TierPhase::Restored => Err(Error::RestoreAlreadyInProgress(
                    format!("{bucket}/{key}@{version_id}"),
                )),
            }
        })
        .await
        .map(|_| ())
    }

    /// 写恢复副本到本地（对读路径不可见；meta 仍 Restoring）。返回实际写入的 SHA-256。
    pub async fn restore_prepare(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        bytes: &[u8],
    ) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(m) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let v = m
            .find_version(version_id)
            .filter(|v| !v.delete_marker())
            .ok_or_else(|| Error::NoSuchKey(format!("{key}@{version_id}")))?;
        if v.tier.as_ref().map(|t| t.phase) != Some(TierPhase::Restoring) {
            return Err(Error::RestoreAlreadyInProgress(format!(
                "{bucket}/{key}@{version_id}: 不在 Restoring 阶段"
            )));
        }
        if v.size != bytes.len() as u64 {
            return Err(Error::CorruptMeta(format!(
                "{bucket}/{key}@{version_id}: 恢复数据长度 {} ≠ {}",
                bytes.len(),
                v.size
            )));
        }
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?.join(&v.part.uuid);
        tokio::fs::create_dir_all(&dir).await?;
        let path = dir.join("part.1");
        let tmp = dir.join("part.1.restore-tmp");
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
        }
        drop(f);
        tokio::fs::rename(&tmp, &path).await?;
        Ok(())
    }

    /// 恢复下载的暂存路径（版本目录内固定名，一个任务一份；drop/abort 即清理）。
    pub fn restore_stage_path(&self, bucket: &str, key: &str, uuid: &str) -> Result<PathBuf> {
        if uuid.is_empty() || uuid.contains(['/', '\\', '\0']) || uuid == "." || uuid == ".." {
            return Err(Error::InvalidKey(format!("版本 uuid 非法：{uuid:?}")));
        }
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?.join(uuid);
        Ok(dir.join(RESTORE_STAGE))
    }

    /// 用流式下载好的暂存文件提交恢复副本：校验长度 → fsync → rename（对读路径不可见）。
    /// 调用方在 Restoring 阶段、持锁前完成下载与 SHA-256 校验。
    pub async fn restore_prepare_file(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        staged: &Path,
    ) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(m) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let v = m
            .find_version(version_id)
            .filter(|v| !v.delete_marker())
            .ok_or_else(|| Error::NoSuchKey(format!("{key}@{version_id}")))?;
        if v.tier.as_ref().map(|t| t.phase) != Some(TierPhase::Restoring) {
            return Err(Error::RestoreAlreadyInProgress(format!(
                "{bucket}/{key}@{version_id}: 不在 Restoring 阶段"
            )));
        }
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?.join(&v.part.uuid);
        if staged != dir.join(RESTORE_STAGE) {
            return Err(Error::InvalidArgument(format!(
                "恢复暂存路径不在版本目录内：{}",
                staged.display()
            )));
        }
        let len = tokio::fs::metadata(staged)
            .await
            .map(|m| m.len())
            .map_err(|e| {
                Error::CorruptMeta(format!("{bucket}/{key}@{version_id}: 恢复暂存不可读：{e}"))
            })?;
        if len != v.size {
            return Err(Error::CorruptMeta(format!(
                "{bucket}/{key}@{version_id}: 恢复数据长度 {len} ≠ {}",
                v.size
            )));
        }
        let path = dir.join("part.1");
        let tmp = dir.join("part.1.restore-tmp");
        if let Err(e) = tokio::fs::rename(staged, &tmp).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e.into());
        }
        let out = async {
            let f = tokio::fs::OpenOptions::new().write(true).open(&tmp).await?;
            if self.cfg.fsync {
                f.sync_all().await?;
            }
            drop(f);
            tokio::fs::rename(&tmp, &path).await
        }
        .await;
        if let Err(e) = out {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e.into());
        }
        if self.cfg.fsync {
            tokio::fs::File::open(&dir).await?.sync_all().await?;
        }
        Ok(())
    }

    /// 恢复提交：Restoring → Restored + 到期时刻；本地副本自此可读。
    /// `chunks`：恢复流上重算的分块哈希（旧对象直读 backfill）；None = 保留已有表
    /// （启动对账/无数据可算的路径不得清掉原表）。
    pub async fn restore_commit(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        expires_at: i64,
        chunks: Option<TierChunks>,
    ) -> Result<()> {
        self.mutate_version(bucket, key, version_id, |_old, v| {
            let Some(t) = &mut v.tier else {
                return Err(Error::InvalidObjectState(format!(
                    "{bucket}/{key}@{version_id}: 无 tier 记录"
                )));
            };
            if t.phase != TierPhase::Restoring {
                return Err(Error::RestoreAlreadyInProgress(format!(
                    "{bucket}/{key}@{version_id}: 不在 Restoring 阶段"
                )));
            }
            if let Some(c) = &chunks {
                c.validate(t.size)
                    .map_err(|e| Error::InvalidArgument(format!("分块哈希表非法：{e}")))?;
                t.chunks = Some(c.clone());
            }
            v.part = PartRef {
                uuid: v.part.uuid.clone(),
                part: 1,
                size: t.size,
                sha256: t.sha256.clone(),
            };
            v.parts = None;
            v.data_layout = None;
            v.inline = None;
            t.phase = TierPhase::Restored;
            t.restore_expires_at = Some(expires_at);
            Ok(())
        })
        .await
        .map(|_| ())
    }

    /// 恢复失败回滚：Restoring → Archived（已 Restored 不动，避免毁掉可见副本）。
    /// prepare 写下的 part.1 对读路径不可见，回滚时一并回收（共享 uuid 则保留）。
    pub async fn abort_restore(&self, bucket: &str, key: &str, version_id: &str) -> Result<()> {
        let out = self
            .mutate_version(bucket, key, version_id, |_old, v| {
                if let Some(t) = &mut v.tier
                    && t.phase == TierPhase::Restoring
                {
                    t.phase = TierPhase::Archived;
                    t.restore_expires_at = None;
                }
                Ok(())
            })
            .await?;
        if out.tier.as_ref().map(|t| t.phase) != Some(TierPhase::Archived) {
            return Ok(());
        }
        let Ok(Some(m)) = self.read_meta(bucket, key).await else {
            return Ok(());
        };
        let uuid = &out.part.uuid;
        if m.versions
            .iter()
            .filter(|v| !v.delete_marker() && &v.part.uuid == uuid)
            .count()
            <= 1
        {
            self.recycle_version_dir(bucket, key, uuid).await;
        }
        Ok(())
    }

    /// 过期回收：仅当版本处于 Restored 且到期（`<= now`）时执行；返回是否回收。
    pub async fn expire_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        now_secs: i64,
    ) -> Result<bool> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Ok(false);
        };
        let Some(idx) = old
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
        else {
            return Ok(false);
        };
        let v = &old.versions[idx];
        let Some(t) = &v.tier else { return Ok(false) };
        if t.phase != TierPhase::Restored || t.restore_expires_at.is_none_or(|e| e > now_secs) {
            return Ok(false);
        }
        let uuid = v.part.uuid.clone();
        let mut m = old.clone();
        let v = &mut m.versions[idx];
        let t = v.tier.as_mut().expect("上面已确认");
        t.phase = TierPhase::Archived;
        t.restore_expires_at = None;
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        if m.versions
            .iter()
            .filter(|v| !v.delete_marker() && v.part.uuid == uuid)
            .count()
            <= 1
        {
            self.recycle_version_dir(bucket, key, &uuid).await;
        }
        Ok(true)
    }

    /// 启动对账扫描：全部 Restoring 版本（无 row/job 的由调用方重置并重排）。
    pub async fn scan_tier_pending(&self) -> Result<Vec<(String, String, String)>> {
        let mut out = Vec::new();
        for b in self.list_buckets().await? {
            let mut key_marker: Option<String> = None;
            let mut vid_marker: Option<String> = None;
            loop {
                let page = self
                    .list_object_versions(
                        &b.name,
                        "",
                        1000,
                        key_marker.as_deref(),
                        vid_marker.as_deref(),
                    )
                    .await?;
                for e in &page.entries {
                    if !e.is_marker && e.tier_phase == Some(TierPhase::Restoring) {
                        out.push((b.name.clone(), e.key.clone(), e.version_id.clone()));
                    }
                }
                if !page.is_truncated {
                    break;
                }
                key_marker = page.next_key_marker;
                vid_marker = page.next_version_id_marker;
            }
        }
        Ok(out)
    }
}
