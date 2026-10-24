//! 服务端 mesh 面（M3 Task 3 骨架）：**pub 入口**——in-process 实现与 xl-client 的
//! tonic handler 共用这一层。可见性约定：外部（xl-client）只能经 `Store::mesh_*`；
//! 其余内部件（stage_payload / write_meta_on / object_dir）保持 `pub(in crate::store)`。
//!
//! 语义学分工（与 mesh.rs 头注一致）：**一个节点收到的请求只操作它自己的本地槽**；
//! 扇出永远由协调者（收到客户端请求的节点）做——in-process 实现若在对端再扇出会循环 await 死锁。

use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::NodeHealth;
use crate::meta;

use super::Store;
use super::drives;
use super::ec;

impl Store {
    /// 本节点健康：拓扑视角的槽位在册状态 + 桶计数（/health/cluster 与 fixture 共用）。
    /// 单节点形态（未配 cluster）node_id = "single"、槽位从 0 起 = 本机盘。
    pub async fn mesh_health(&self) -> NodeHealth {
        let (node_id, base) = match self.cfg.cluster.as_deref() {
            None => ("single".to_string(), 0u16),
            Some(cl) => (
                cl.sets.flat.self_node().id.clone(),
                cl.sets.flat.local_slot_base(),
            ),
        };
        let mut slots = Vec::with_capacity(self.cfg.drives.len());
        for (i, drive) in self.cfg.drives.iter().enumerate() {
            slots.push((base + i as u16, drives::drive_up(drive).await));
        }
        // 桶计数 = 本机各盘桶目录并集（与 list_buckets 同口径：磁盘权威）
        let mut buckets = std::collections::BTreeSet::new();
        for drive in &self.cfg.drives {
            let Ok(mut rd) = tokio::fs::read_dir(drive).await else {
                continue;
            };
            while let Ok(Some(e)) = rd.next_entry().await {
                let name = e.file_name().to_string_lossy().into_owned();
                if name != layout::SYS_DIR
                    && e.file_type().await.map(|t| t.is_dir()).unwrap_or(false)
                {
                    buckets.insert(name);
                }
            }
        }
        NodeHealth {
            node_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
            slots,
            buckets: buckets.len() as u64,
        }
    }
}

// ---------- 数据面（Task 5a/5b）：服务端入口——只操作本节点本地槽，绝不再扇出 ----------

impl Store {
    /// 集群传输（None = 单节点）。写/读路径的 Remote 分派用它。
    pub(in crate::store) fn mesh_transport(
        &self,
    ) -> Option<&std::sync::Arc<dyn crate::mesh::MeshTransport>> {
        self.cfg.cluster.as_deref().map(|c| &c.transport)
    }

    /// 槽位归属校验（M4 切片 5；切片 8 加 set）：槽位由请求方携带，服务端**只校验归属并按
    /// (set, 槽) 拼路径**——不再重算放置公式（新旧公式并存会让存量对象远端面全错，评审 A）。
    /// 洞 1 修法 (a)：组内序经 `flat_of` 映射到 cfg.drives 扁平下标（不可直接当盘下标用）。
    fn local_drive_of_request_slot(&self, set: u32, slot: u32) -> Result<&std::path::Path> {
        let cl = self
            .cfg
            .cluster
            .as_deref()
            .ok_or_else(|| Error::Encode("非集群模式没有 mesh 面".into()))?;
        let set = u16::try_from(set).map_err(|_| Error::Encode(format!("set 越界: {set}")))?;
        let slot = u16::try_from(slot).map_err(|_| Error::Encode(format!("槽位越界: {slot}")))?;
        let topo = cl
            .sets
            .set_topo(set)
            .ok_or_else(|| Error::Encode(format!("set {set} 不在拓扑内")))?;
        let ord = topo.local_drive_of_slot(slot).ok_or_else(|| {
            Error::Encode(format!("槽位 {slot} 不属本节点 set {set}（请求误导？）"))
        })?;
        let flat = cl
            .sets
            .flat_of(set, ord)
            .ok_or_else(|| Error::Encode(format!("set {set} 组内序 {ord} 无物理盘映射")))?;
        self.cfg
            .drives
            .get(flat)
            .map(std::path::PathBuf::as_path)
            .ok_or_else(|| Error::Encode(format!("set {set} 槽 {slot} 的扁平盘下标 {flat} 越界")))
    }

    /// 控制面白名单写的服务端执行（M5 切片 0）：调本节点注入的 ControlBackend。
    pub async fn mesh_control_write(&self, w: crate::mesh::ControlWrite) -> Result<()> {
        match self.cfg.control.as_deref() {
            Some(b) => b.control_write(w).await,
            None => Err(Error::Encode("本节点未注入控制面后端".into())),
        }
    }

    /// 桶行变更广播（M5 切片 0 / P4-1）：向全部 peer best-effort 发 `ControlWrite`。
    /// 失败只 warn——广播是**第一**收敛机制（关删桶重建的陈旧行窗口）；重启时的权威扇出
    /// 对账（state.rs 双对账面）兜底。**未重启的漏播节点在重启前仍按本地陈旧行路由**（数据
    /// 面本地先行短路），这是 best-effort 设计的固有残窗。
    pub async fn broadcast_bucket_row(&self, bucket: &str, set: Option<u16>) {
        let Some(cl) = self.cfg.cluster.as_deref() else {
            return;
        };
        let self_id = cl.sets.flat.self_node().id.clone();
        let w = crate::mesh::ControlWrite::BucketRowSet {
            bucket: bucket.to_string(),
            set,
        };
        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            if let Err(e) = cl.transport.control_write(&n.id, w.clone()).await {
                tracing::warn!(node = %n.id, bucket, error = %e, "桶行广播未达（回填对账兜底）");
            }
        }
    }

    /// 幂等分片写（服务端）：**先验 CRC32C 再落盘**（等同本地落盘强度，防腐坏入库）。
    /// 整片语义（`chunked=false`）行为与 M3 完全一致；分块语义（`chunked=true`）按
    /// `(offset, chunk_crc32c, is_final)` 定位写、逐块校验、末块 fsync。
    pub async fn mesh_write_shard(&self, w: crate::mesh::ShardWrite) -> Result<()> {
        let drive = self.local_drive_of_request_slot(w.set, w.slot)?;
        let dir = layout::object_dir(drive, &w.bucket, &w.key)?.join(&w.uuid);
        tokio::fs::create_dir_all(&dir).await?;
        // 与 drives::shard_path 同式（part.<part>.<shard>）
        let path = dir.join(format!("part.{}.{}", w.part, w.shard));
        if w.chunked {
            if ec::crc32c_of(&w.data) != w.chunk_crc32c {
                return Err(Error::Encode(format!(
                    "分块 CRC32C 不符（{}:{} shard {} @{}）——拒收",
                    w.bucket, w.key, w.shard, w.offset
                )));
            }
            // 定位写（重试从头幂等：offset=0 先截断，避免旧尾巴残留）
            let mut f = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await?;
            if w.offset == 0 {
                f.set_len(0).await?;
            }
            tokio::io::AsyncSeekExt::seek(&mut f, std::io::SeekFrom::Start(w.offset)).await?;
            f.write_all(&w.data).await?;
            f.flush().await?;
            if w.is_final && self.cfg.fsync {
                f.sync_all().await?;
                // 版本目录 fsync：分片目录项先于 meta 翻转持久（与本机落片同序）
                Store::sync_dir(&dir).await;
            }
            return Ok(());
        }
        if ec::crc32c_of(&w.data) != w.crc32c {
            return Err(Error::Encode(format!(
                "分片 CRC32C 不符（{}:{} shard {}）——拒收",
                w.bucket, w.key, w.shard
            )));
        }
        let mut f = tokio::fs::File::create(&path).await?;
        f.write_all(&w.data).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
            // 版本目录 fsync：分片目录项先于 meta 翻转持久（与本机落片同序）
            Store::sync_dir(&dir).await;
        }
        Ok(())
    }

    /// 分片读（服务端）：缺失 → Ok(None)（调用方视同该片缺失）；本机 IO 错误 → Err。
    /// `length=0` = 整片（旧语义）；分块读 `[offset, offset+length)`，短读（半截件）视同缺失。
    pub async fn mesh_read_shard(&self, r: crate::mesh::ShardRead) -> Result<Option<bytes::Bytes>> {
        let drive = self.local_drive_of_request_slot(r.set, r.slot)?;
        let path = layout::object_dir(drive, &r.bucket, &r.key)?
            .join(&r.uuid)
            .join(format!("part.{}.{}", r.part, r.shard));
        if r.length == 0 {
            return match tokio::fs::read(&path).await {
                Ok(b) => Ok(Some(bytes::Bytes::from(b))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            };
        }
        // 分块读上限防呆（信任边界收口）：length 是调用方字段，无界分配 = 1 字节文件 + 巨
        // length 即可 OOM abort——正常客户端只请求 CHUNK_W，上限给 16 MiB 留未来放宽余地
        const MAX_BLOCK_READ: u64 = 16 * 1024 * 1024;
        if r.length > MAX_BLOCK_READ {
            return Err(Error::Encode(format!(
                "分块读长度越界：{} > {MAX_BLOCK_READ}",
                r.length
            )));
        }
        let mut f = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        tokio::io::AsyncSeekExt::seek(&mut f, std::io::SeekFrom::Start(r.offset)).await?;
        let mut buf = vec![0u8; r.length as usize];
        use tokio::io::AsyncReadExt as _;
        match f.read_exact(&mut buf).await {
            Ok(_) => Ok(Some(bytes::Bytes::from(buf))),
            // 短读：文件比预期短（崩溃半截/并发回收）——视同该块缺失，交 EC 重建
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 单片删除（M4 切片 5 rebalance  cleanup；幂等）：槽位显式携带（flip 后 meta 已不记老槽）。
    pub async fn mesh_delete_shard(&self, d: crate::mesh::ShardDelete) -> Result<()> {
        let drive = self.local_drive_of_request_slot(d.set, d.slot)?;
        let path = layout::object_dir(drive, &d.bucket, &d.key)?
            .join(&d.uuid)
            .join(format!("part.{}.{}", d.part, d.shard));
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// 删除版本目录（幂等）：本节点全部本地盘。**`uuid` 为空 = 删整个对象目录**
    /// （delete_object 的远端数据清理；残留本就不可见，失败由 GC 按龄回收）。
    pub async fn mesh_delete_version(&self, d: crate::mesh::VersionDirRef) -> Result<()> {
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, &d.bucket, &d.key) {
                let target = if d.uuid.is_empty() {
                    dir
                } else {
                    dir.join(&d.uuid)
                };
                let _ = tokio::fs::remove_dir_all(target).await;
            }
        }
        Ok(())
    }

    /// 旧版本回收（幂等）：本节点各盘 `trash/` —— 与 recycle_old 的盘侧动作同式。
    pub async fn mesh_recycle_version(&self, d: crate::mesh::VersionDirRef) -> Result<()> {
        for drive in &self.cfg.drives {
            let Ok(obj_dir) = layout::object_dir(drive, &d.bucket, &d.key) else {
                continue;
            };
            let old_dir = obj_dir.join(&d.uuid);
            if !matches!(tokio::fs::try_exists(&old_dir).await, Ok(true)) {
                continue;
            }
            let trash_dir = layout::sys_dir(drive, "trash");
            let _ = tokio::fs::create_dir_all(&trash_dir).await;
            let trash = trash_dir.join(&d.uuid);
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            if let Err(e) = tokio::fs::rename(&old_dir, &trash).await {
                tracing::warn!(error = %e, path = %old_dir.display(), "mesh 回收旧版本失败");
            }
        }
        Ok(())
    }

    /// 本节点首个全局槽位（单机 = 0；mesh 服务端 ack 的槽号用）。
    pub(in crate::store) fn slot_base(&self) -> u16 {
        match self.cfg.cluster.as_deref() {
            None => 0,
            Some(cl) => cl.sets.flat.local_slot_base(),
        }
    }

    /// 服务端建桶目录（幂等）：本节点各盘 create_dir_all。
    pub async fn mesh_ensure_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        for drive in &self.cfg.drives {
            tokio::fs::create_dir_all(layout::bucket_dir(drive, bucket)).await?;
        }
        Ok(())
    }

    /// 服务端删桶目录（空检查 + 幂等删除）：任一本地盘上非空 → Err(BucketNotEmpty)。
    pub async fn mesh_delete_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket_data(bucket)?;
        for drive in &self.cfg.drives {
            let dir = layout::bucket_dir(drive, bucket);
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if entries.next_entry().await?.is_some() {
                return Err(Error::BucketNotEmpty(bucket.to_string()));
            }
            drop(entries);
            tokio::fs::remove_dir(&dir).await.ok();
        }
        Ok(())
    }

    /// 服务端桶名列表（本节点各盘并集；磁盘权威，系统目录排除）。
    pub async fn mesh_list_buckets(&self) -> Result<Vec<String>> {
        let mut names = std::collections::BTreeSet::new();
        for drive in &self.cfg.drives {
            let Ok(mut rd) = tokio::fs::read_dir(drive).await else {
                continue;
            };
            while let Ok(Some(e)) = rd.next_entry().await {
                let name = e.file_name().to_string_lossy().into_owned();
                if name != layout::SYS_DIR
                    && e.file_type().await.map(|t| t.is_dir()).unwrap_or(false)
                {
                    names.insert(name);
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    /// 服务端列举页（ListLocal）：本节点一页（键 + cps，`limit` 为输出上限）。
    /// 本机无此桶目录 → 空页（并集语义：缺者空贡献，不 404）。
    pub async fn mesh_list_local(
        &self,
        l: crate::mesh::ListLocal,
    ) -> Result<crate::mesh::ListLocalResp> {
        let (objs, cps) = match self
            .list_page_local(
                &l.bucket,
                &l.prefix,
                l.marker.as_deref(),
                l.marker_exclusive,
                l.delimiter.as_deref(),
                l.include_deleted,
                l.limit,
            )
            .await
        {
            Ok(x) => x,
            Err(Error::NoSuchBucket(_)) => (Vec::new(), Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(crate::mesh::ListLocalResp {
            entries: objs.into_iter().map(|(e, _)| e).collect(),
            prefixes: cps,
            slots_ok: self.cfg.drives.len() as u32,
        })
    }

    /// 服务端 LMU 页（ListUploadsLocal）：本节点一页（upload + cps）。
    /// 本机无此桶目录 → 空页（并集语义：缺者空贡献，不 404）。
    pub async fn mesh_list_uploads_local(
        &self,
        l: crate::mesh::ListUploadsLocal,
    ) -> Result<crate::mesh::ListUploadsLocalResp> {
        let (ups, cps) = match self
            .list_uploads_page_local(
                &l.bucket,
                &l.prefix,
                l.delimiter.as_deref(),
                l.key_marker.as_deref(),
                l.upload_id_marker.as_deref(),
                l.limit,
            )
            .await
        {
            Ok(x) => x,
            Err(Error::NoSuchBucket(_)) => (Vec::new(), Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(crate::mesh::ListUploadsLocalResp {
            uploads: ups
                .into_iter()
                .map(|u| crate::mesh::UploadEntry {
                    key: u.key,
                    upload_id: u.upload_id,
                    initiated_nanos: u.initiated_nanos,
                    storage_class: u.storage_class.unwrap_or_default(),
                })
                .collect(),
            prefixes: cps,
            slots_ok: self.cfg.drives.len() as u32,
        })
    }

    /// best-effort 远程回滚（错误路径专用）：广播各 peer 删版本目录（幂等），1s 短超时
    /// 防拖慢错误路径；失败留给该节点 clean/fsck 按龄回收。
    pub(in crate::store) async fn mesh_rollback_remote(&self, bucket: &str, key: &str, uuid: &str) {
        let Some(cl) = self.cfg.cluster.as_deref() else {
            return;
        };
        let self_id = cl.sets.flat.self_node().id.clone();
        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            let d = crate::mesh::VersionDirRef {
                bucket: bucket.into(),
                key: key.into(),
                uuid: uuid.into(),
            };
            match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                cl.transport.delete_version(&n.id, d),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(node = %n.id, error = %e, "远程回滚失败（孤儿由该节点 clean 回收）")
                }
                Err(_) => {
                    tracing::warn!(node = %n.id, "远程回滚超时（孤儿由该节点 clean 回收）")
                }
            }
        }
    }

    // ---------- meta 面（Task 6）：服务端入口 ----------

    /// 写本机某盘槽的 `xl.meta.new`（prepare 的盘侧动作；内容由调用方给定）。
    pub(in crate::store) async fn write_meta_new_on(
        &self,
        drive: &std::path::Path,
        bucket: &str,
        key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        let dir = layout::object_dir(drive, bucket, key)?;
        tokio::fs::create_dir_all(&dir).await?;
        let mut f = tokio::fs::File::create(dir.join(meta::META_NEW_FILE)).await?;
        f.write_all(bytes).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
        }
        Ok(())
    }

    /// 翻本机某盘槽：**读回 `.new` 算内容 hash** → rename 覆盖 `xl.meta`（该槽提交点）。
    /// 返回 (是否翻转, 所翻内容 hash)——hash 供写者只计「内容是自己的」的翻转（防伪达成）。
    pub(in crate::store) async fn commit_meta_on(
        &self,
        drive: &std::path::Path,
        bucket: &str,
        key: &str,
    ) -> Result<(bool, [u8; 32])> {
        let dir = layout::object_dir(drive, bucket, key)?;
        let new_path = dir.join(meta::META_NEW_FILE);
        let bytes = match tokio::fs::read(&new_path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((false, [0u8; 32])),
            Err(e) => return Err(e.into()),
        };
        let hash = crate::mesh::content_hash(&bytes);
        tokio::fs::rename(&new_path, dir.join(meta::META_FILE)).await?;
        if self.cfg.fsync {
            Store::sync_dir(&dir).await;
        }
        Ok((true, hash))
    }

    /// 服务端 prepare：本机全部在册盘写 `.new`；逐槽 ack（hash = 内容 hash）。
    pub async fn mesh_prepare_meta(
        &self,
        m: crate::mesh::MetaWrite,
    ) -> Result<crate::mesh::MetaAck> {
        let self_hash = crate::mesh::content_hash(&m.meta);
        let mut slots = Vec::new();
        for (d, drive) in self.cfg.drives.iter().enumerate() {
            if !drives::drive_up(drive).await {
                tracing::warn!(drive = %drive.display(), "盘不在册，跳过 meta 准备");
                continue;
            }
            let slot = self.slot_base() + d as u16;
            match self
                .write_meta_new_on(drive, &m.bucket, &m.key, &m.meta)
                .await
            {
                Ok(()) => slots.push(crate::mesh::SlotAck {
                    slot,
                    ok: true,
                    hash: self_hash,
                }),
                Err(e) => {
                    tracing::warn!(slot, error = %e, "meta 准备失败");
                    slots.push(crate::mesh::SlotAck {
                        slot,
                        ok: false,
                        hash: [0u8; 32],
                    });
                }
            }
        }
        let ok = slots.iter().any(|s| s.ok);
        let slots_ok = slots.iter().filter(|s| s.ok).count() as u32;
        Ok(crate::mesh::MetaAck {
            ok,
            msg: String::new(),
            slots,
            slots_ok,
        })
    }

    /// 服务端 commit：逐本地槽「读回 `.new` 算 hash → rename」；幂等（无 `.new` → ok=false）。
    pub async fn mesh_commit_meta(
        &self,
        m: crate::mesh::MetaWrite,
    ) -> Result<crate::mesh::MetaAck> {
        let mut slots = Vec::new();
        for (d, drive) in self.cfg.drives.iter().enumerate() {
            let slot = self.slot_base() + d as u16;
            match self.commit_meta_on(drive, &m.bucket, &m.key).await {
                Ok((ok, hash)) => slots.push(crate::mesh::SlotAck { slot, ok, hash }),
                Err(e) => {
                    tracing::warn!(slot, error = %e, "meta 翻转失败");
                    slots.push(crate::mesh::SlotAck {
                        slot,
                        ok: false,
                        hash: [0u8; 32],
                    });
                }
            }
        }
        let ok = slots.iter().any(|s| s.ok);
        let slots_ok = slots.iter().filter(|s| s.ok).count() as u32;
        Ok(crate::mesh::MetaAck {
            ok,
            msg: String::new(),
            slots,
            slots_ok,
        })
    }

    /// 服务端 abort：清本机全部槽的 `.new` 残骸（幂等）。
    pub async fn mesh_abort_meta(&self, m: crate::mesh::MetaWrite) -> Result<()> {
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, &m.bucket, &m.key) {
                let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
            }
        }
        Ok(())
    }

    /// 服务端删除 meta（delete_object 分派与提交补偿共用）：清本机全部槽的 `xl.meta`
    /// 与 `.new`（幂等）。meta 删除是可见性提交点——失败必须由调用方上抛，不得吞。
    pub async fn mesh_delete_meta(&self, r: crate::mesh::MetaRead) -> Result<()> {
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, &r.bucket, &r.key) {
                let _ = tokio::fs::remove_file(dir.join(meta::META_FILE)).await;
                let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
            }
        }
        Ok(())
    }

    // ---------- M3 MPU 会话粘性：服务端 handler（只执行本地暂存区操作，不再转发） ----------

    /// 服务端 UploadPart：把转发流交给本地暂存区（流式落盘；失败清理 `.new`）。
    /// 属主校验在 `upload_part_local → mpu_dir` 内：前缀不是本节点 → NoSuchUpload。
    pub async fn mesh_mpu_upload_part(&self, r: crate::mesh::MpuPartUpload) -> Result<String> {
        self.upload_part_local(
            &r.bucket,
            &r.key,
            &r.upload_id,
            r.part_number,
            r.body,
            r.sse_c_md5.as_deref(),
        )
        .await
    }

    /// 服务端 Complete：属主本地提交（版本形态/加密/锁意图由协调者透传）。
    pub async fn mesh_mpu_complete(
        &self,
        r: crate::mesh::MpuComplete,
    ) -> Result<crate::store::ObjectInfo> {
        let encryption = r.encryption.map(|e| e.to_spec());
        self.complete_multipart_upload_versioned_local(
            &r.bucket,
            &r.key,
            &r.upload_id,
            &r.parts,
            r.mode,
            encryption,
            r.lock,
        )
        .await
    }

    /// 服务端 Abort：只删本节点暂存区（幂等失败面由本地实现决定）。
    pub async fn mesh_mpu_abort(&self, r: crate::mesh::MpuRef) -> Result<()> {
        self.abort_multipart_upload_local(&r.bucket, &r.key, &r.upload_id)
            .await
    }

    /// 服务端 ListParts：只读本节点暂存区。
    pub async fn mesh_mpu_list_parts(
        &self,
        r: crate::mesh::MpuRef,
    ) -> Result<Vec<crate::mesh::MpuPartRow>> {
        let parts = self
            .list_parts_local(&r.bucket, &r.key, &r.upload_id)
            .await?;
        Ok(parts
            .into_iter()
            .map(|p| crate::mesh::MpuPartRow {
                part_number: p.part_number,
                size: p.size,
                etag: p.etag,
            })
            .collect())
    }

    /// 服务端控制面查询（Task 9）：本地后端执行（未注入 → 显式错误，不静默）。
    pub async fn mesh_control_query(
        &self,
        q: crate::mesh::ControlQuery,
    ) -> Result<Option<crate::mesh::ControlRow>> {
        match &self.cfg.control {
            Some(b) => b.control_query(q).await,
            None => Err(Error::Encode(
                "未注入控制面后端（StoreConfig.control）".into(),
            )),
        }
    }

    /// 服务端租约锁（Task 8 / 节点域）：本机锁表按授予规则处理（见 lock.rs 头注）——
    /// 仅空闲/过期可授、活租约不抢占、仅持有者 Renew/Release 有效。
    pub async fn mesh_lock(&self, req: crate::mesh::LockReq) -> Result<crate::mesh::LockResp> {
        let key = format!("{}/{}", req.bucket, req.key);
        let ttl = std::time::Duration::from_millis(req.ttl_ms.max(1));
        Ok(crate::mesh::LockResp {
            granted: self.lock_table().apply(&key, &req, ttl),
        })
    }

    /// 服务端读 meta：**就地本地扫描**（latest 胜；**绝不调 self.read_meta**——集群下
    /// 那是扇出入口，服务端再扇出会无限递归）。`slots_ok` = 本机确定性读到结论的槽数
    /// （命中或 NotFound；IO/解码错误不计——覆盖不足由协调者 fail-closed）。
    /// **未命中恒返回快照（meta=None）**——覆盖率快路径需要「缺席也计数」。
    pub async fn mesh_read_meta(
        &self,
        r: crate::mesh::MetaRead,
    ) -> Result<crate::mesh::MetaSnapshot> {
        use crate::meta::XlMeta;
        let mut best: Option<((u64, i64), XlMeta)> = None;
        let mut corrupt: Option<String> = None;
        let mut covered = 0u32;
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                continue;
            }
            let path = layout::object_dir(drive, &r.bucket, &r.key)?.join(meta::META_FILE);
            match tokio::fs::read(&path).await {
                Ok(bytes) => match XlMeta::decode(&bytes) {
                    Ok(m) if m.object_key == r.key => {
                        covered += 1;
                        // 与 store.rs 的 read_meta 选举同口径（rank = (epoch, 链尾 mod_time)）：
                        // 服务端 RPC 是独立选举点，回拨场景漏改会把低 epoch 旧链当最优返回
                        let t = m.rank();
                        if best.as_ref().is_none_or(|(bt, _)| t > *bt) {
                            best = Some((t, m));
                        }
                    }
                    Ok(m) => {
                        corrupt = Some(format!(
                            "键不一致：请求 {:?}，实为 {:?}",
                            r.key, m.object_key
                        ));
                    }
                    Err(e) => corrupt = Some(e.to_string()),
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => covered += 1,
                Err(e) => corrupt = Some(e.to_string()),
            }
        }
        match best {
            Some((t, m)) => Ok(crate::mesh::MetaSnapshot {
                meta: Some(bytes::Bytes::from(m.encode()?)),
                // 协调者对解码后的 meta 自行算 rank()（选举口径的唯一出处）；
                // 此字段只带墙钟分量供诊断，不参与判定
                mod_time_nanos: t.1,
                slots_ok: covered,
            }),
            None => {
                if let Some(why) = corrupt {
                    // 全无可读副本且见过损坏 → 上抛（fail-closed，协调者不计入覆盖）
                    return Err(Error::CorruptMeta(why));
                }
                Ok(crate::mesh::MetaSnapshot {
                    meta: None,
                    mod_time_nanos: i64::MIN,
                    slots_ok: covered,
                })
            }
        }
    }
}
