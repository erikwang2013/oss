//! rebalance（M4 切片 5）：追加式扩容后把存量对象迁到展开放置（v2）。
//!
//! 协议（子设计 v2 / 设计评审 A–J）：逐对象「copy 到新槽 → **锁内** meta 翻转（提交点）
//! → 老片 best-effort 清理」。翻转前读者走老槽、翻转后走新槽、清理在翻转后——单读无窗口。
//! 边界：只搬「链尾向前第一个非 marker 数据版本」（历史版本留老槽，自描述可读）；
//! inline / 旧 parts / 未完成 MPU 不在范围；逐对象 best-effort（目标不可达等 → 入
//! skipped，不中断整轮）；**同刻只跑一个 rebalance**；幂等（已达标对象跳过，重跑可续）。
//! 残窗记档：翻转后立即删老片 vs 在途旧 meta 读者 → 瞬缺片走 EC 重建（极端重叠可能单次失败）。

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, DataLayout, Version};

use super::{Store, drives, ec};

/// 搬迁报告（JSON 落盘口径同 fsck）。
#[derive(Debug, Default, serde::Serialize)]
pub struct RebalanceReport {
    pub dry_run: bool,
    /// v2 未启用（无可搬目标——显式开关未开或槽位未超分片数）
    pub placement_v2: bool,
    pub objects_scanned: usize,
    /// 需搬迁（dry-run = 计划数；apply = 成功数）
    pub objects_moved: usize,
    pub shards_moved: usize,
    pub bytes_moved: u64,
    /// 逐对象 best-effort 跳过（并发变更/源片缺失/目标不可达…），下轮重跑可续
    pub skipped: Vec<String>,
}

impl Store {
    /// 全量搬迁（dry_run 只扫描报告，不写任何东西）。
    pub async fn rebalance(&self, dry_run: bool) -> Result<RebalanceReport> {
        let shard_count = (self.cfg.ec.data + self.cfg.ec.parity) as usize;
        let v2 = self.placement_v2();
        let mut rep = RebalanceReport {
            dry_run,
            placement_v2: v2,
            ..Default::default()
        };
        if !v2 {
            return Ok(rep);
        }
        let (keys, _buckets) = self.collect_namespace().await?;
        // M4 切片 8：per-bucket set 缓存（未扩容的 set 新旧组内槽相等 → need 空 → 天然 0 移动）
        let mut cur_bucket: Option<String> = None;
        let mut cur_set: u16 = 0;
        let mut cur_failed = false;
        for (bucket, key) in keys {
            if cur_bucket.as_deref() != Some(bucket.as_str()) {
                cur_bucket = Some(bucket.clone());
                match self.set_of_bucket(&bucket).await {
                    Ok(s) => {
                        cur_set = s;
                        cur_failed = false;
                    }
                    Err(e) => {
                        // P3-1：失败也缓存（同 fsck 主循环）
                        rep.skipped.push(format!("{bucket}: set 解析失败 {e}"));
                        cur_failed = true;
                    }
                }
            }
            if cur_failed {
                continue;
            }
            rep.objects_scanned += 1;
            // 锁外粗读：定位「链尾向前第一个非 marker 数据版本」
            let m0 = match self.read_meta(&bucket, &key).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(e) => {
                    rep.skipped.push(format!("{bucket}/{key}: meta {e}"));
                    continue;
                }
            };
            let Some(idx) = m0
                .versions
                .iter()
                .rposition(|v| !v.delete_marker() && v.data_layout.is_some())
            else {
                continue; // inline / 旧 parts / 纯 marker 链：不在范围（E）
            };
            let v = m0.versions[idx].clone();
            let l = v.data_layout.clone().expect("rposition 已过滤");
            let hash = Store::placement_hash(&bucket, &key);
            let need: Vec<usize> = {
                let mut n = Vec::new();
                for i in 0..shard_count {
                    if l.shards[i].drive != self.slot_of_shard(cur_set, hash, i as u8)? as u8 {
                        n.push(i);
                    }
                }
                n
            };
            if need.is_empty() {
                continue;
            }
            if dry_run {
                rep.objects_moved += 1;
                rep.shards_moved += need.len();
                rep.bytes_moved += need.len() as u64 * l.shard_size;
                continue;
            }
            match self
                .rebalance_one(cur_set, &bucket, &key, &v, &l, &need, hash)
                .await
            {
                Ok(n) => {
                    rep.objects_moved += 1;
                    rep.shards_moved += n;
                    rep.bytes_moved += n as u64 * l.shard_size;
                }
                Err(e) => rep.skipped.push(format!("{bucket}/{key}: {e}")),
            }
        }
        Ok(rep)
    }

    /// 单对象搬迁：copy → 锁内翻转（提交点）→ 清老片。
    #[allow(clippy::too_many_arguments)]
    async fn rebalance_one(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &DataLayout,
        need: &[usize],
        hash: u64,
    ) -> Result<usize> {
        let uuid = &v.part.uuid;
        let ss = l.shard_size;
        let blocks = ss.div_ceil(ec::CHUNK_W as u64);
        // 1) copy：逐需搬片、逐块（常量内存；源缺失 → 放弃该对象下轮重试）
        for &i in need {
            let s = &l.shards[i];
            let new_slot = self.slot_of_shard(set, hash, i as u8)?;
            // C4 快路径：本地→本地且同设备 → hardlink（免整片重拷）。崩溃安全：翻前新旧同
            // inode，翻后删旧名只是减 link 计数。任何失败（EXDEV/权限/EEXIST）→ 回落分块 copy。
            if self
                .try_link_shard(set, bucket, key, uuid, s, new_slot)
                .await
            {
                continue;
            }
            for t in 0..blocks {
                let Some(chunk) = self
                    .read_shard_block(set, bucket, key, v, s, i, t, ss)
                    .await?
                else {
                    return Err(Error::Encode(format!(
                        "源片 {} 块 {t} 缺失（下轮重试）",
                        s.shard
                    )));
                };
                let is_final = t + 1 == blocks;
                self.write_shard_chunk(
                    set,
                    bucket,
                    key,
                    uuid,
                    s.part,
                    i,
                    new_slot,
                    t * ec::CHUNK_W as u64,
                    &chunk,
                    is_final,
                )
                .await?;
            }
        }
        // 2) 锁内翻转（设计 C：重读 + 应用更新 + 写 meta 全程持对象锁）
        {
            let _guard = self.lock_object(bucket, key).await?;
            let Some(mut m_now) = self.read_meta(bucket, key).await? else {
                return Err(Error::Encode("并发删除（下轮重试）".into()));
            };
            let Some(v_now) = m_now
                .versions
                .iter_mut()
                .find(|x| x.part.uuid == *uuid && !x.delete_marker())
            else {
                return Err(Error::Encode("并发版本变更（下轮重试）".into()));
            };
            let Some(l_now) = v_now.data_layout.as_mut() else {
                return Err(Error::Encode("并发形态变更（下轮重试）".into()));
            };
            for &i in need {
                l_now.shards[i].drive = self.slot_of_shard(set, hash, i as u8)? as u8;
            }
            // 链重写不变量（切片 4）：链尾 revision epoch 严格 +1
            let pre_max = m_now.max_epoch();
            let m_old = m_now.clone(); // 补偿回滚用（verifier P2：None 会在提交失败时删翻转槽 meta）
            m_now.refresh_latest_flags();
            if let Some(last) = m_now.versions.last_mut() {
                last.epoch = Some(pre_max + 1);
            }
            self.write_meta(bucket, key, &m_now, Some(&m_old)).await?;
        }
        // 3) 清老片（best-effort：翻转后老片已不可见，残留 = fsck stray 扫描的清理对象）
        for &i in need {
            let old_slot = l.shards[i].drive as u16;
            if let Err(e) = self
                .delete_shard_at(set, bucket, key, uuid, l.shards[i].part, i, old_slot)
                .await
            {
                tracing::warn!(bucket, key, shard = i, error = %e, "rebalance：老片清理失败（fsck stray 兜底）");
            }
        }
        Ok(need.len())
    }

    /// C4：本地→本地 hardlink 快路径（同设备才成；跨设备 EXDEV → false 回落分块 copy）。
    /// 目标目录按 (set, new_slot) 的物理盘拼接——与 write_shard_chunk 的 Local 分支同式。
    async fn try_link_shard(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        uuid: &str,
        s: &meta::ShardRef,
        new_slot: u16,
    ) -> bool {
        let old_slot = s.drive as u16;
        let (
            Some(drives::ShardTarget::Local { drive: src, .. }),
            Some(drives::ShardTarget::Local { drive: dst, .. }),
        ) = (
            self.slot_target(set, old_slot),
            self.slot_target(set, new_slot),
        )
        else {
            return false; // 任一端远端：走 RPC 分块路径
        };
        if src == dst {
            return false; // 同盘（need 理论上不含）：防御
        }
        if !drives::drive_up(src).await || !drives::drive_up(dst).await {
            return false;
        }
        let Ok(old_path) = drives::shard_path(src, bucket, key, uuid, s) else {
            return false;
        };
        let Ok(new_dir) = layout::object_dir(dst, bucket, key).map(|d| d.join(uuid)) else {
            return false;
        };
        let new_path = new_dir.join(format!("part.{}.{}", s.part, s.shard));
        if tokio::fs::create_dir_all(&new_dir).await.is_err() {
            return false;
        }
        match tokio::fs::hard_link(&old_path, &new_path).await {
            Ok(()) => true,
            Err(e) => {
                tracing::debug!(old = %old_path.display(), error = %e, "hardlink 快路径不可用，回落分块 copy");
                false
            }
        }
    }

    /// 分块写指定槽（本地定位写 / 远端 chunked RPC）——搬迁目标写。
    #[allow(clippy::too_many_arguments)]
    async fn write_shard_chunk(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        uuid: &str,
        part: u16,
        shard: usize,
        slot: u16,
        offset: u64,
        chunk: &[u8],
        is_final: bool,
    ) -> Result<()> {
        let crc = ec::crc32c_of(chunk);
        match self.slot_target(set, slot) {
            Some(drives::ShardTarget::Local { drive, .. }) => {
                if !drives::drive_up(drive).await {
                    return Err(Error::QuorumUnmet(format!("目标槽 {slot} 不在册")));
                }
                let dir = layout::object_dir(drive, bucket, key)?.join(uuid);
                tokio::fs::create_dir_all(&dir).await?;
                let path = dir.join(format!("part.{part}.{shard}"));
                let mut f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&path)
                    .await?;
                if offset == 0 {
                    f.set_len(0).await?;
                }
                tokio::io::AsyncSeekExt::seek(&mut f, std::io::SeekFrom::Start(offset)).await?;
                tokio::io::AsyncWriteExt::write_all(&mut f, chunk).await?;
                tokio::io::AsyncWriteExt::flush(&mut f).await?;
                if is_final && self.cfg.fsync {
                    tokio::io::AsyncWriteExt::flush(&mut f).await?;
                    f.sync_all().await?;
                    Store::sync_dir(&dir).await;
                }
                Ok(())
            }
            Some(drives::ShardTarget::Remote { node, .. }) => match self.mesh_transport() {
                Some(t) => {
                    t.write_shard(
                        node,
                        crate::mesh::ShardWrite {
                            bucket: bucket.into(),
                            key: key.into(),
                            uuid: uuid.into(),
                            part: part as u32,
                            shard: shard as u32,
                            crc32c: 0,
                            data: bytes::Bytes::copy_from_slice(chunk),
                            chunked: true,
                            offset,
                            chunk_crc32c: crc,
                            is_final,
                            slot: slot as u32,
                            set: set as u32,
                        },
                    )
                    .await
                }
                None => Err(Error::Encode("集群模式未注入 transport".into())),
            },
            None => Err(Error::Encode(format!("目标槽 {slot} 越界"))),
        }
    }

    /// 删指定槽上的单片（本地 remove / 远端 delete_shard RPC）。
    #[allow(clippy::too_many_arguments)]
    async fn delete_shard_at(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        uuid: &str,
        part: u16,
        shard: usize,
        slot: u16,
    ) -> Result<()> {
        match self.slot_target(set, slot) {
            Some(drives::ShardTarget::Local { drive, .. }) => {
                let path = layout::object_dir(drive, bucket, key)?
                    .join(uuid)
                    .join(format!("part.{part}.{shard}"));
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(e.into()),
                }
            }
            Some(drives::ShardTarget::Remote { node, .. }) => match self.mesh_transport() {
                Some(t) => {
                    t.delete_shard(
                        node,
                        crate::mesh::ShardDelete {
                            bucket: bucket.into(),
                            key: key.into(),
                            uuid: uuid.into(),
                            part: part as u32,
                            shard: shard as u32,
                            slot: slot as u32,
                            set: set as u32,
                        },
                    )
                    .await
                }
                None => Err(Error::Encode("集群模式未注入 transport".into())),
            },
            None => Err(Error::Encode(format!("老槽 {slot} 越界"))),
        }
    }
}
