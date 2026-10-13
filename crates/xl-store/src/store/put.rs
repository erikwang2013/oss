//! 写路径：整份/流式 PUT（提交协议）与候选 meta 落盘。
//! 提交协议（顺序不可变，见 `put_object_stream_with_meta` 的文档注释）。

use std::collections::BTreeMap;

use bytes::Bytes;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, ChecksumRecord, PartRef, Version, XlMeta};

use super::{ByteStream, ObjectInfo, Store, drives, ec, now_nanos};

impl Store {
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_with_meta(bucket, key, body, content_type, Default::default())
            .await
    }

    /// 带用户元数据的整份 PUT。metadata 是**全量替换**（S3 语义，不是合并）。
    pub async fn put_object_with_meta(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(
            bucket,
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
            content_type,
            user_metadata,
            None,
        )
        .await
    }

    /// 提交协议（顺序不可变）：
    ///   1. 数据**流式**写 `.oss.sys/tmp/<uuid>/part.1`，边写边算 MD5+SHA256+size；
    ///      收全后转形态——`< inline_threshold`：读回内联进 meta；否则：EC 编码为
    ///      `part.1.<shard>`（每片 fsync）。两种形态都删掉 part.1
    ///   2. 候选 `xl.meta` 写进同一个 tmp 目录
    ///   3. **同盘 rename** tmp/<uuid> → <bucket>/<obj>/<uuid>     ← 数据就位
    ///   4. 写 `<obj>/xl.meta.new` 并 fsync，再 rename 覆盖 `xl.meta` ← **提交点**
    ///   5. 老版本目录 → `.oss.sys/trash/`（移入 trash，由 GC 回收——M1 未实现回收器）
    ///
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，M1 无回收器，残留只占空间）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，同上）。读路径只信 xl.meta。
    ///
    /// M1 持久化假设：`fsync=true` 时数据文件与 `xl.meta.new` 都先做 file fsync；
    /// ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可。**没有目录 fsync**，
    /// 换文件系统（或要求断电零丢失）需复核这两条假设。
    pub async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(bucket, key, body, content_type, Default::default(), None)
            .await
    }

    /// 前段（单段 PUT / MPU complete / 多盘分发共用）：流式收 `body` 到 `tmp/part.1`
    /// 并做增量哈希。返回 `(size, 整份 MD5 etag, 整份 SHA256, part.1 路径)`。
    pub(in crate::store) async fn harvest_body(
        &self,
        tmp: &std::path::Path,
        mut body: ByteStream,
    ) -> Result<(u64, String, String, std::path::PathBuf)> {
        tokio::fs::create_dir_all(tmp).await?;
        let part_path = tmp.join("part.1");
        let mut file = tokio::fs::File::create(&part_path).await?;
        let mut md5 = Md5::new();
        let mut sha = Sha256::new();
        let mut size: u64 = 0;
        while let Some(chunk) = futures::StreamExt::next(&mut body).await {
            let chunk = chunk?;
            md5.update(&chunk);
            sha.update(&chunk);
            size += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        if self.cfg.fsync {
            file.sync_all().await?;
        }
        drop(file);
        Ok((
            size,
            hex::encode(md5.finalize()),
            hex::encode(sha.finalize()),
            part_path,
        ))
    }

    /// 写形态核心（单段 PUT 与 MPU complete 共用）：流式收 `body` 到 `tmp/part.1`，
    /// 收全后按 `inline_threshold` 转形态，返回
    /// `(size, 整份 MD5 etag, 整份 SHA256, data_layout, inline)`。
    /// 调用方负责候选 meta 与提交协议（rename → write_meta）。单盘形态专用；
    /// 多盘分发走 [`Store::stage_payload_multi`]。
    pub(in crate::store) async fn stage_payload(
        &self,
        tmp: &std::path::Path,
        body: ByteStream,
    ) -> Result<(
        u64,
        String,
        String,
        Option<meta::DataLayout>,
        Option<meta::InlineData>,
    )> {
        let (size, etag, sha256, part_path) = self.harvest_body(tmp, body).await?;

        // 2) 收尾转形态。为什么不像任务书最初设想的「收全再分叉」：那会失去流式落盘
        // 语义（M2a 的握手测试与 README 承诺）。保持「流式写 part.1 → 收全后转」，
        // 峰值只在 encode 一步（payload + 12 片），S3 路径受 max_put_bytes 约束；
        // ponytail: aether ingest 大文件同受此峰值，ShardByShard 流式编/解是 M2b-3 之后的升级路径。
        // 0 字节恒内联：EC 对空对象无意义（rs 拒收空片），阈值配成 0 时也不能把空对象送进 EC
        let (data_layout, inline) = if !self.cfg.ec.enabled {
            // enabled=false：M1 旧形态——part.1 原样保留（随 tmp rename 一起就位），无内联无分片
            (None, None)
        } else if size == 0 || size < self.cfg.inline_threshold {
            // 内联：小文件读回进 meta，删 part.1（版本目录只剩 xl.meta）
            let bytes = tokio::fs::read(&part_path).await?;
            tokio::fs::remove_file(&part_path).await?;
            (None, Some(meta::InlineData { bytes }))
        } else {
            // EC：读回整份 → 编码 → 逐片写 part.1.<shard>（每片 fsync），删 part.1
            let payload = tokio::fs::read(&part_path).await?;
            let shards = ec::encode(&payload, self.cfg.ec)?;
            drop(payload);
            let mut shard_refs = Vec::with_capacity(shards.len());
            for (i, s) in shards.iter().enumerate() {
                let path = tmp.join(format!("part.1.{i}"));
                let mut f = tokio::fs::File::create(&path).await?;
                f.write_all(s).await?;
                f.flush().await?;
                if self.cfg.fsync {
                    f.sync_all().await?;
                }
                drop(f);
                shard_refs.push(meta::ShardRef {
                    drive: 0,
                    part: 1,
                    shard: i as u8,
                    size: s.len() as u64,
                    crc32c: ec::crc32c_of(s),
                });
            }
            tokio::fs::remove_file(&part_path).await?;
            let shard_size = shards.first().map_or(0, Vec::len) as u64;
            (
                Some(meta::DataLayout {
                    data: self.cfg.ec.data,
                    parity: self.cfg.ec.parity,
                    shard_size,
                    shards: shard_refs,
                }),
                None,
            )
        };
        if self.cfg.fsync {
            // tmp 目录 fsync：分片目录项先于「tmp→对象目录」rename 持久
            Store::sync_dir(tmp).await;
        }
        Ok((size, etag, sha256, data_layout, inline))
    }

    /// 多盘落盘（M2b-2）：与前段共用 `harvest_body`（仍在 drives[0] 的 tmp 里收全 + 哈希），
    /// 转换阶段**分片直写目标盘的版本目录**——`<drives[i%n]>/<bucket>/<展键>/<uuid>/part.1.<i>`，
    /// 每片 fsync。数据先于 meta 落位，未提交（meta 未翻转）前不可见；无需跨盘 rename。
    ///
    /// quorum（设计 §4.3）：成功落盘分片数 ≥ data+1 才继续；不足 → 回滚已写目录 →
    /// `Err(QuorumUnmet)`。失败片仍进 `DataLayout.shards`（drive=目标盘、盘上无文件）——
    /// shards 表须按 shard 下标 12 项齐全，读路径把无文件视同缺失并用 EC 重建。
    /// 内联（< 阈值）不落数据文件：数据进 meta，冗余由 meta 多盘复制提供（M2b-1 记录的边界在此收口）。
    ///
    /// 返回 `(size, etag, sha256, data_layout, inline, 已触碰槽位)`；`touched` 供调用方回滚。
    pub(in crate::store) async fn stage_payload_multi(
        &self,
        bucket: &str,
        key: &str,
        uuid: &str,
        tmp: &std::path::Path,
        body: ByteStream,
    ) -> Result<(
        u64,
        String,
        String,
        Option<meta::DataLayout>,
        Option<meta::InlineData>,
        Vec<u16>,
    )> {
        let (size, etag, sha256, part_path) = self.harvest_body(tmp, body).await?;

        if size == 0 || size < self.cfg.inline_threshold {
            let bytes = tokio::fs::read(&part_path).await?;
            tokio::fs::remove_file(&part_path).await?;
            return Ok((
                size,
                etag,
                sha256,
                None,
                Some(meta::InlineData { bytes }),
                Vec::new(),
            ));
        }

        let payload = tokio::fs::read(&part_path).await?;
        let shards = ec::encode(&payload, self.cfg.ec)?;
        drop(payload);
        tokio::fs::remove_file(&part_path).await?;

        let quorum = self.cfg.ec.data as usize + 1;
        // 测试专用断电注入（Task 9）：写到第 N 片后 abort()（SIGABRT），复现「提交点前猝死」。
        // 仅环境变量在场时生效；生产部署不设此变量 = 每次循环多一次 Option 比较，零行为影响。
        let abort_after: Option<usize> = std::env::var("OSS_TEST_ABORT_AFTER_SHARD")
            .ok()
            .and_then(|v| v.parse().ok());
        let mut shard_refs = Vec::with_capacity(shards.len());
        let mut touched: Vec<u16> = Vec::new();
        let mut landed = 0usize;
        for (i, s) in shards.iter().enumerate() {
            let slot = self.slot_of_shard(i as u8);
            let res: Result<()> = async {
                match self.slot_target(slot) {
                    Some(drives::ShardTarget::Local { drive, .. }) => {
                        // 存活探测（掉线盘的挂载点目录仍在父 fs 上，盲写会填满根盘且误报成功）
                        if !drives::drive_up(drive).await {
                            return Err(Error::QuorumUnmet(format!("槽位 {slot} 不在册（跳过）")));
                        }
                        touched.push(slot);
                        let dir = layout::object_dir(drive, bucket, key)?.join(uuid);
                        tokio::fs::create_dir_all(&dir).await?;
                        let mut f =
                            tokio::fs::File::create(dir.join(format!("part.1.{i}"))).await?;
                        f.write_all(s).await?;
                        f.flush().await?;
                        if self.cfg.fsync {
                            f.sync_all().await?;
                            // 版本目录 fsync：分片目录项先于 meta 翻转持久（否则断电后 meta 存在而目录项丢失）
                            Store::sync_dir(&dir).await;
                        }
                        Ok(())
                    }
                    Some(drives::ShardTarget::Remote { node, .. }) => {
                        // M3 Task 5a：远程落片（write_shard RPC，幂等）
                        Err(Error::Encode(format!(
                            "远程落片未实现（node {node}，M3 Task 5a）"
                        )))
                    }
                    None => Err(Error::Encode(format!("槽位 {slot} 越界"))),
                }
            }
            .await;
            match res {
                Ok(()) => landed += 1,
                Err(e) => {
                    tracing::warn!(slot, shard = i, error = %e, "分片落盘失败（quorum 计数中）");
                }
            }
            shard_refs.push(meta::ShardRef {
                drive: slot as u8,
                part: 1,
                shard: i as u8,
                size: s.len() as u64,
                crc32c: ec::crc32c_of(s),
            });
            if abort_after == Some(i) {
                std::process::abort();
            }
        }
        if landed < quorum {
            for &slot in &touched {
                match self.slot_target(slot) {
                    Some(drives::ShardTarget::Local { drive, .. }) => {
                        if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                            let _ = tokio::fs::remove_dir_all(dir.join(uuid)).await;
                        }
                    }
                    Some(drives::ShardTarget::Remote { node, .. }) => {
                        // M3 Task 5a：delete_version RPC（幂等）
                        tracing::warn!(node, slot, "远程回滚未实现（M3 Task 5a）");
                    }
                    None => {}
                }
            }
            self.bump_quorum_unmet_write();
            return Err(Error::QuorumUnmet(format!(
                "分片存活 {landed} < {}（已回滚 {touched:?}）",
                quorum
            )));
        }
        let shard_size = shards.first().map_or(0, Vec::len) as u64;
        Ok((
            size,
            etag,
            sha256,
            Some(meta::DataLayout {
                data: self.cfg.ec.data,
                parity: self.cfg.ec.parity,
                shard_size,
                shards: shard_refs,
            }),
            None,
            touched,
        ))
    }

    /// 多盘回滚：删除本对象在各盘的 `<uuid>` 版本目录（半截写入 / 未提交残骸）。
    pub(in crate::store) async fn rollback_versions(&self, bucket: &str, key: &str, uuid: &str) {
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                let _ = tokio::fs::remove_dir_all(dir.join(uuid)).await;
            }
        }
    }

    /// 老版本回收（跨盘）：每盘的 `<old_uuid>` 目录 → 该盘 trash（best-effort：提交点已过，
    /// 任何错误都不该让已提交的 PUT 报失败）。
    pub(in crate::store) async fn recycle_old(
        &self,
        bucket: &str,
        key: &str,
        old_meta: Option<&XlMeta>,
    ) {
        let Some(old) = old_meta.and_then(|m| m.latest()) else {
            return;
        };
        for drive in &self.cfg.drives {
            let Ok(obj_dir) = layout::object_dir(drive, bucket, key) else {
                continue;
            };
            let old_dir = obj_dir.join(&old.part.uuid);
            let trash = layout::sys_dir(drive, "trash").join(&old.part.uuid);
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }
    }

    /// 流式 PUT 的完整形态（含用户元数据）；`put_object_stream` 是空元数据的薄壳。
    pub async fn put_object_stream_with_meta(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
        checksum: Option<ChecksumRecord>,
    ) -> Result<ObjectInfo> {
        layout::validate_key(key)?;
        // 桶窗口（桶锁内，M2b-2 Task 10）：ensure + 对象目录先落盘——此后 delete_bucket
        // 见到非空必拒，桶「复活」窗口关闭。失败清理时清掉可能新建的空目录（空目录会卡住删桶）
        let obj_dir = {
            let _bguard = self.lock_bucket_owned(bucket).await;
            self.ensure_bucket(bucket).await?;
            let dir = layout::object_dir(self.cfg.root(), bucket, key)?;
            tokio::fs::create_dir_all(&dir).await?;
            dir
        };
        let _guard = self.lock_object(bucket, key).await?;

        // M-6 裁决：损坏的 meta 让 PUT fail-closed（报错），不静默当作空对象覆盖——盘上可能还有旧数据，宁可失败也不毁
        let old_meta = self.read_meta(bucket, key).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(self.cfg.root(), "tmp").join(&uuid);
        let now = now_nanos();
        // 集群 + 单盘节点也必须走槽位路径（否则分片全落本地 tmp，槽位模型失效）
        let multi = self.cfg.cluster.is_some() || self.cfg.multi_drive();

        // 1)+2) 数据与候选 meta 落位；任何写失败/流错误都回滚本轮
        //（未提交前无任何 meta 引用，失败即弃；残留只占空间，由 `oss-server clean` 按 mtime 回收）
        let staged: Result<(u64, String, XlMeta)> = async {
            let (size, etag, sha256, data_layout, inline) = if multi {
                // 多盘：分片直写各盘版本目录（数据先于 meta；无需候选 meta 落 tmp）
                let (s, e, sh, l, i, _touched) = self
                    .stage_payload_multi(bucket, key, &uuid, &tmp, body)
                    .await?;
                (s, e, sh, l, i)
            } else {
                self.stage_payload(&tmp, body).await?
            };

            let version = Version {
                version_id: uuid::Uuid::nil(), // M1 无版本控制：nil = S3 的 "null" 版本
                mod_time_nanos: now,
                size,
                etag: etag.clone(),
                content_type: content_type.clone(),
                user_metadata: user_metadata.clone(),
                // 兼容位（旧读方依赖；EC 版的真实文件是 part.1.<shard>，旧读方读不到——「新解旧 ✓ 旧解新 ✗」已受裁定）
                part: PartRef {
                    uuid: uuid.clone(),
                    part: 1,
                    size,
                    sha256,
                },
                // 单段 PUT 的 parts 恒 None（多段只出现在 M2a 旧对象的只读兼容位）
                parts: None,
                checksum: checksum.clone(),
                data_layout,
                inline,
            };
            let meta = XlMeta {
                object_key: key.to_string(),
                versions: vec![version],
            };

            // 单盘：候选 meta 落 tmp（与数据同目录，随 rename 一起就位）
            if !multi {
                tokio::fs::write(tmp.join(meta::META_FILE), meta.encode()?).await?;
            }
            Ok((size, etag, meta))
        }
        .await;
        let (size, etag, meta) = match staged {
            Ok(v) => v,
            Err(e) => {
                if multi {
                    self.rollback_versions(bucket, key, &uuid).await;
                }
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                let _ = tokio::fs::remove_dir(&obj_dir).await; // 仅当空（窗口新建的）才删得掉
                return Err(e);
            }
        };

        if multi {
            // 多盘：两阶段 meta 提交（单盘协议是 tmp→rename→write_meta，多盘没有跨盘 rename，
            // 数据已在各盘版本目录就位，meta 翻转即各盘提交点）
            let quorum = self.cfg.ec.data as usize + 1;
            let flipped = match self.write_meta(bucket, key, &meta).await {
                Ok(n) => n,
                Err(e) => {
                    // 阶段一失败 = 一盘未翻转：数据目录干净回滚
                    self.rollback_versions(bucket, key, &uuid).await;
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    let _ = tokio::fs::remove_dir(&obj_dir).await;
                    return Err(e);
                }
            };
            if flipped == 0 {
                // 已准备 ≥quorum 但翻转全失败（病态 IO）：仍属未提交，回滚
                self.rollback_versions(bucket, key, &uuid).await;
                self.clear_meta_new(bucket, key).await; // R7：清各盘遗留的 .new 暂存 meta
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                let _ = tokio::fs::remove_dir(&obj_dir).await;
                self.bump_quorum_unmet_write();
                return Err(Error::QuorumUnmet("meta 翻转全失败，未提交".into()));
            }
            if flipped < quorum {
                tracing::warn!(
                    flipped,
                    quorum,
                    "meta 翻转不足 quorum（已提交，M3 heal 补副本）"
                );
            }
            let _ = tokio::fs::remove_dir_all(&tmp).await; // tmp 仅用于流式收全与哈希
            self.recycle_old(bucket, key, old_meta.as_ref()).await;
        } else {
            // 3) 同盘 rename：数据就位（此时对读路径仍不可见）；目录 fsync 是 rename 的持久化屏障。
            // 对象目录在桶窗口已建，但「窗口 → 取到对象锁」之间可能被删（delete 抢先），
            // 此处幂等重建（delete 走对象锁，此后不会再拆）
            tokio::fs::create_dir_all(&obj_dir).await?;
            tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;
            if self.cfg.fsync {
                Store::sync_dir(&obj_dir).await;
            }

            // 4) 提交点：写 xl.meta.new 再原子覆盖
            self.write_meta(bucket, key, &meta).await?;

            // 5) 老版本进回收站
            self.recycle_old(bucket, key, old_meta.as_ref()).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now,
            user_metadata,
            checksum,
        })
    }
}
