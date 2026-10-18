//! 写路径：整份/流式 PUT（提交协议）与候选 meta 落盘。
//! 提交协议（顺序不可变，见 `put_object_stream_with_meta` 的文档注释）。

use std::collections::BTreeMap;

use bytes::Bytes;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt};

use crate::crypto;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, ChecksumRecord, PartRef, Version, XlMeta};

use super::{ByteStream, ObjectInfo, Store, drives, ec, now_nanos};

/// 散读一块（M4 切片 2）：段式布局下第 i 片第 t 块的输入 = payload `[i·ss + t·W, +W)`
/// （越界补零）。单盘/多盘两条分块写路径共用；内存 = data×W。
async fn gather_block(
    src: &mut tokio::fs::File,
    size: u64,
    ss: u64,
    t: u64,
    data: usize,
) -> Result<Vec<Vec<u8>>> {
    let mut ins = Vec::with_capacity(data);
    for i in 0..data {
        let mut c = vec![0u8; ec::CHUNK_W];
        let start = i as u64 * ss + t * ec::CHUNK_W as u64;
        if start < size {
            let take = (size - start).min(ec::CHUNK_W as u64) as usize;
            src.seek(std::io::SeekFrom::Start(start)).await?;
            src.read_exact(&mut c[..take]).await?;
        }
        ins.push(c);
    }
    Ok(ins)
}

/// 散读一块（M5 切片 2 加密版）：第 i 片第 t 块的输入 = 单元 `u = i·blocks_ec + t` 的密文
/// （短单元右补零至块宽；`u ≥ nb` 的条带恒零）。P 侧按 `[65520·u, +len)` 散读。
async fn gather_block_enc(
    src: &mut tokio::fs::File,
    geo: &crypto::Geometry,
    blocks_ec: u64,
    t: u64,
    data: usize,
    ctx: &crypto::UnitCtx,
) -> Result<Vec<Vec<u8>>> {
    let mut ins = Vec::with_capacity(data);
    for i in 0..data {
        let u = i as u64 * blocks_ec + t;
        if u >= geo.units {
            ins.push(vec![0u8; ec::CHUNK_W]);
            continue;
        }
        let start = geo.unit_plain_start(u);
        let len = geo.unit_plain_len(u) as usize;
        let mut p = vec![0u8; len];
        src.seek(std::io::SeekFrom::Start(start)).await?;
        src.read_exact(&mut p).await?;
        let mut c = ctx.cipher.seal(&ctx.seed, u as u32, &p)?;
        c.resize(ec::CHUNK_W, 0); // 末单元短 → 右补零至块宽（读侧按 unit_cipher_len trim）
        ins.push(c);
    }
    Ok(ins)
}

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
            false, // 兼容壳：无版本控制（S3 层显式传 state）
            None,  // 兼容壳：无加密（切片 3 由 S3 头映射）
            None,  // 兼容壳：无对象锁
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
        self.put_object_stream_with_meta(
            bucket,
            key,
            body,
            content_type,
            Default::default(),
            None,
            false,
            None, // 兼容壳：无加密
            None, // 兼容壳：无对象锁
        )
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
        enc: Option<&crypto::UnitCtx>,
    ) -> Result<(
        u64,
        String,
        String,
        Option<meta::DataLayout>,
        Option<meta::InlineData>,
    )> {
        let (size, etag, sha256, part_path) = self.harvest_body(tmp, body).await?;

        // 2) 收尾转形态。为什么不像任务书最初设想的「收全再分叉」：那会失去流式落盘
        // 语义（M2a 的握手测试与 README 承诺）。M4 切片 2 起转形态本身也流式：
        // part.1 散读逐块编（块缓冲 ≈1.3 MiB，**与对象大小无关**），aether ingest 大文件
        // 同路径同收益。单段 PUT / MPU complete 共用本函数。
        // 0 字节恒内联：EC 对空对象无意义（rs 拒收空片），阈值配成 0 时也不能把空对象送进 EC
        let (data_layout, inline) = if !self.cfg.ec.enabled {
            // enabled=false：M1 旧形态——part.1 原样保留（随 tmp rename 一起就位），无内联无分片
            (None, None)
        } else if size == 0 || size < self.cfg.inline_threshold {
            // 内联：小文件读回进 meta（加密时按单元切成密文 C = c_len(P)；P=0 ⇒ C 空），
            // 删 part.1（版本目录只剩 xl.meta）
            let bytes = tokio::fs::read(&part_path).await?;
            tokio::fs::remove_file(&part_path).await?;
            let stored = match enc {
                None => bytes,
                Some(ctx) => crypto::seal_all(ctx, &bytes)?,
            };
            (None, Some(meta::InlineData { bytes: stored }))
        } else {
            // EC 分块编码（M4 切片 2）：常量内存（块缓冲 ≈1.3 MiB，与对象大小无关）——
            // part.1 散读逐块编 → 逐片写 part.1.<shard>（整片 fsync 在末块后一次），删 part.1。
            // M5 切片 2：加密时 EC 载荷 = 密文流 C（几何/分块全按 c_len；单元↔(片,块) 双射）。
            let data = self.cfg.ec.data as usize;
            let total = data + self.cfg.ec.parity as usize;
            let geo = enc.map(|_| crypto::Geometry::new(size)).transpose()?;
            let payload_len = geo.map_or(size, |g| g.cipher_len);
            let blocks = payload_len.div_ceil((data * ec::CHUNK_W) as u64);
            let ss = blocks * ec::CHUNK_W as u64;
            let mut w = ec::ChunkWriter::new(self.cfg.ec)?;
            let mut src = tokio::fs::File::open(&part_path).await?;
            let mut files = Vec::with_capacity(total);
            for i in 0..total {
                files.push(tokio::fs::File::create(tmp.join(format!("part.1.{i}"))).await?);
            }
            for t in 0..blocks {
                let ins = match (enc, &geo) {
                    (Some(ctx), Some(g)) => {
                        gather_block_enc(&mut src, g, blocks, t, data, ctx).await?
                    }
                    _ => gather_block(&mut src, size, ss, t, data).await?,
                };
                let out = w.encode_block(&ins)?;
                for (i, chunk) in out.iter().enumerate() {
                    files[i].write_all(chunk).await?;
                }
            }
            for f in &mut files {
                f.flush().await?;
                if self.cfg.fsync {
                    f.sync_all().await?;
                }
            }
            drop(files);
            tokio::fs::remove_file(&part_path).await?;
            let shard_refs = (0..total)
                .map(|i| meta::ShardRef {
                    drive: 0,
                    part: 1,
                    shard: i as u8,
                    size: ss,
                    crc32c: w.shard_crc(i),
                })
                .collect();
            (
                Some(meta::DataLayout {
                    data: self.cfg.ec.data,
                    parity: self.cfg.ec.parity,
                    shard_size: ss,
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
        enc: Option<&crypto::UnitCtx>,
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
            let stored = match enc {
                None => bytes,
                Some(ctx) => crypto::seal_all(ctx, &bytes)?,
            };
            return Ok((
                size,
                etag,
                sha256,
                None,
                Some(meta::InlineData { bytes: stored }),
                Vec::new(),
            ));
        }

        // M4 切片 2：分块编码（常量内存），分片直写目标槽位。
        // 每片一条落盘通道；某片任一块失败即该片作废（ok[i]=false），quorum 按「完整片数」计。
        // M5 切片 2：加密时 EC 载荷 = 密文流 C（几何/分块全按 c_len；单元↔(片,块) 双射）。
        let data = self.cfg.ec.data as usize;
        let total = data + self.cfg.ec.parity as usize;
        let geo = enc.map(|_| crypto::Geometry::new(size)).transpose()?;
        let payload_len = geo.map_or(size, |g| g.cipher_len);
        let blocks = payload_len.div_ceil((data * ec::CHUNK_W) as u64);
        let ss = blocks * ec::CHUNK_W as u64;
        let mut w = ec::ChunkWriter::new(self.cfg.ec)?;
        let mut src = tokio::fs::File::open(&part_path).await?;

        enum Sink {
            Local {
                f: tokio::fs::File,
                dir: std::path::PathBuf,
            },
            Remote(String),
            Dead,
        }
        let quorum = self.cfg.ec.data as usize + 1;
        // 测试专用断电注入（Task 9）：第 N 片写完最后一块后 abort()（SIGABRT），复现「提交点前猝死」。
        // 仅环境变量在场时生效；生产部署不设此变量 = 每块多一次 Option 比较，零行为影响。
        let abort_after: Option<usize> = std::env::var("OSS_TEST_ABORT_AFTER_SHARD")
            .ok()
            .and_then(|v| v.parse().ok());
        // M4 切片 5：放置哈希（sha256(bucket/key) 前 8B）——v2 开关下全域分散
        let place_hash = Store::placement_hash(bucket, key);
        // M4 切片 8：桶所属 set（多 set 一次解析、全线透传；fail-closed 见 Store::set_of_bucket）
        let set = self.set_of_bucket(bucket).await?;
        let mut sinks: Vec<Sink> = Vec::with_capacity(total);
        let mut touched: Vec<u16> = Vec::new();
        let mut slots: Vec<u16> = Vec::with_capacity(total);
        // ok[i] = 该片当前仍完整（任一块失败即 false）
        let mut ok: Vec<bool> = Vec::with_capacity(total);
        for i in 0..total {
            let slot = self.slot_of_shard(set, place_hash, i as u8)?;
            slots.push(slot);
            let alive = match self.slot_target(set, slot) {
                Some(drives::ShardTarget::Local { drive, .. }) => {
                    // 存活探测（掉线盘的挂载点目录仍在父 fs 上，盲写会填满根盘且误报成功）
                    if !drives::drive_up(drive).await {
                        tracing::warn!(slot, shard = i, "槽位不在册（跳过）");
                        sinks.push(Sink::Dead);
                        false
                    } else {
                        touched.push(slot);
                        let dir = layout::object_dir(drive, bucket, key)?.join(uuid);
                        let opened: Result<tokio::fs::File> = async {
                            tokio::fs::create_dir_all(&dir).await?;
                            Ok(tokio::fs::File::create(dir.join(format!("part.1.{i}"))).await?)
                        }
                        .await;
                        match opened {
                            Ok(f) => {
                                sinks.push(Sink::Local { f, dir });
                                true
                            }
                            Err(e) => {
                                tracing::warn!(slot, shard = i, error = %e, "本地分片文件打开失败（该片作废）");
                                sinks.push(Sink::Dead);
                                false
                            }
                        }
                    }
                }
                Some(drives::ShardTarget::Remote { node, .. }) => {
                    if self.mesh_transport().is_none() {
                        return Err(Error::Encode("集群模式未注入 transport".into()));
                    }
                    touched.push(slot);
                    sinks.push(Sink::Remote(node.to_string()));
                    true
                }
                None => {
                    tracing::warn!(slot, "槽位越界（跳过）");
                    sinks.push(Sink::Dead);
                    false
                }
            };
            ok.push(alive);
        }

        for t in 0..blocks {
            let ins = match (enc, &geo) {
                (Some(ctx), Some(g)) => gather_block_enc(&mut src, g, blocks, t, data, ctx).await?,
                _ => gather_block(&mut src, size, ss, t, data).await?,
            };
            let out = w.encode_block(&ins)?;
            let is_final = t + 1 == blocks;
            for (i, chunk) in out.iter().enumerate() {
                if !ok[i] {
                    continue;
                }
                let res: Result<()> = match &mut sinks[i] {
                    Sink::Dead => Err(Error::QuorumUnmet("槽位不在册（跳过）".into())),
                    Sink::Local { f, dir } => {
                        async {
                            f.write_all(chunk).await?;
                            if is_final {
                                f.flush().await?;
                                if self.cfg.fsync {
                                    f.sync_all().await?;
                                    // 版本目录 fsync：分片目录项先于 meta 翻转持久（否则断电后 meta 存在而目录项丢失）
                                    Store::sync_dir(dir).await;
                                }
                            }
                            Ok(())
                        }
                        .await
                    }
                    Sink::Remote(node) => {
                        // 远程落块（幂等）：服务端逐块验 CRC 再定位写；失败按 quorum 计数消化
                        match self.mesh_transport() {
                            Some(tp) => {
                                tp.write_shard(
                                    node,
                                    crate::mesh::ShardWrite {
                                        bucket: bucket.into(),
                                        key: key.into(),
                                        uuid: uuid.into(),
                                        part: 1,
                                        shard: i as u32,
                                        crc32c: 0,
                                        data: bytes::Bytes::copy_from_slice(chunk),
                                        chunked: true,
                                        offset: t * ec::CHUNK_W as u64,
                                        chunk_crc32c: ec::crc32c_of(chunk),
                                        is_final,
                                        // M4 切片 5：携带槽位（服务端只校验归属，不重算放置）
                                        slot: slots[i] as u32,
                                        // M4 切片 8：携带 set（服务端按 sets[set] 校验 + flat_of 拼路径）
                                        set: set as u32,
                                    },
                                )
                                .await
                            }
                            None => Err(Error::Encode("集群模式未注入 transport".into())),
                        }
                    }
                };
                if let Err(e) = res {
                    tracing::warn!(slot = slots[i], shard = i, block = t, error = %e, "分块落盘失败（该片作废，quorum 计数中）");
                    ok[i] = false;
                }
                if abort_after == Some(i) && is_final {
                    std::process::abort();
                }
            }
        }
        tokio::fs::remove_file(&part_path).await?;

        let landed = ok.iter().filter(|v| **v).count();
        if landed < quorum {
            for &slot in &touched {
                match self.slot_target(set, slot) {
                    Some(drives::ShardTarget::Local { drive, .. }) => {
                        if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                            let _ = tokio::fs::remove_dir_all(dir.join(uuid)).await;
                        }
                    }
                    // 远端统一走 mesh_rollback_remote（幂等广播，含 1s 短超时）
                    Some(drives::ShardTarget::Remote { .. }) => {}
                    None => {}
                }
            }
            self.mesh_rollback_remote(bucket, key, uuid).await;
            self.bump_quorum_unmet_write();
            return Err(Error::QuorumUnmet(format!(
                "分片存活 {landed} < {quorum}（已回滚 {touched:?}）"
            )));
        }
        let shard_refs = (0..total)
            .map(|i| meta::ShardRef {
                drive: slots[i] as u8,
                part: 1,
                shard: i as u8,
                size: ss,
                crc32c: w.shard_crc(i),
            })
            .collect();
        Ok((
            size,
            etag,
            sha256,
            Some(meta::DataLayout {
                data: self.cfg.ec.data,
                parity: self.cfg.ec.parity,
                shard_size: ss,
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
        // 集群：远端槽位的半截目录（幂等广播；失败 warn，孤儿由该节点 clean 按龄回收）
        self.mesh_rollback_remote(bucket, key, uuid).await;
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
        // 集群：远端槽位的旧版本 → 该节点 trash（best-effort；先决链=提交 quorum 成立后
        // 旧版本才无读者，回收才安全——Task 6 的集群提交同批落地）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let d = crate::mesh::VersionDirRef {
                    bucket: bucket.into(),
                    key: key.into(),
                    uuid: old.part.uuid.clone(),
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    cl.transport.recycle_version(&n.id, d),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(node = %n.id, error = %e, "远程旧版本回收失败（孤儿由该节点 clean 回收）")
                    }
                    Err(_) => {
                        tracing::warn!(node = %n.id, "远程旧版本回收超时（孤儿由该节点 clean 回收）")
                    }
                }
            }
        }
    }

    /// 流式 PUT 的完整形态（含用户元数据）；`put_object_stream` 是空元数据的薄壳。
    /// `versioned`（M4 切片 3）：桶版本控制 Enabled 时**追加**新版本（保留旧版本目录，
    /// 新版本 version_id = 版本目录 uuid）；否则维持单版本 + 老版本回收的 M2b 语义。
    /// 状态由 S3 层从 `buckets.versioning` 读出后传入（store 不查库）。
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_stream_with_meta(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
        checksum: Option<ChecksumRecord>,
        versioned: bool,
        encryption: Option<crypto::EncSpec>,
        lock: Option<meta::ObjectLockRecord>,
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
        let guard = self.lock_object(bucket, key).await?;
        // 集群锁（Task 8）：获取后即时自检（acquire 已会在分区下失败；此处收窄残余窗口）
        if guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }

        // M5 切片 2：加密上下文（起点；wrap 一次/对象 + 8B 随机 seed）。ec.enabled=false 下
        // 加密数据面无形态（连 inline 分支都不存在）→ 写前 fail-closed。
        let enc_ctx = match encryption {
            Some(spec) => {
                if !self.cfg.ec.enabled {
                    return Err(Error::Encode(
                        "encryption requires ec.enabled（加密数据面需要 EC 管线）".into(),
                    ));
                }
                Some(crypto::UnitCtx::for_write(spec, self.maybe_enc_state()).await?)
            }
            None => None,
        };
        let enc_record = enc_ctx.as_ref().map(|(_, r)| r.clone());
        // M5 切片 4：对象锁（空记录归一为 None，不落字段）
        let lock_record = lock.filter(|l| !l.is_empty());

        // M-6 裁决：损坏的 meta 让 PUT fail-closed（报错），不静默当作空对象覆盖——盘上可能还有旧数据，宁可失败也不毁
        let old_meta = self.read_meta(bucket, key).await?;
        // M5 切片 4（B2 兜底）：非版本化覆盖写会回收旧版本——链上带锁 → fail-closed
        // （陈旧节点行未同步 versioned 时也不得借 recycle 销毁锁定版本）
        if !versioned
            && let Some(old) = old_meta.as_ref()
            && let Some(why) = super::version::chain_active_lock(old)
        {
            return Err(Error::ObjectLocked(format!("覆盖写将回收锁定版本：{why}")));
        }
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(self.cfg.root(), "tmp").join(&uuid);
        // per-key 单调 mod_time（C4）：latest-wins 裁决与墙钟脱钩（M2b R8 的正解在 M3 兑现）
        let now = match old_meta.as_ref().and_then(|m| m.latest()) {
            Some(v) => now_nanos().max(v.mod_time_nanos + 1),
            None => now_nanos(),
        };
        // 集群 + 单盘节点也必须走槽位路径（否则分片全落本地 tmp，槽位模型失效）
        let multi = self.cfg.cluster.is_some() || self.cfg.multi_drive();

        // 1)+2) 数据与候选 meta 落位；任何写失败/流错误都回滚本轮
        //（未提交前无任何 meta 引用，失败即弃；残留只占空间，由 `oss-server clean` 按 mtime 回收）
        let staged: Result<(u64, String, XlMeta)> = async {
            let (size, etag, sha256, data_layout, inline) = if multi {
                // 多盘：分片直写各盘版本目录（数据先于 meta；无需候选 meta 落 tmp）
                let (s, e, sh, l, i, _touched) = self
                    .stage_payload_multi(
                        bucket,
                        key,
                        &uuid,
                        &tmp,
                        body,
                        enc_ctx.as_ref().map(|(c, _)| c),
                    )
                    .await?;
                (s, e, sh, l, i)
            } else {
                self.stage_payload(&tmp, body, enc_ctx.as_ref().map(|(c, _)| c))
                    .await?
            };

            let version = Version {
                // 版本控制 Enabled：真实版本号（= 版本目录 uuid）；否则 nil（S3 的 "null" 版本）
                version_id: if versioned {
                    uuid::Uuid::parse_str(&uuid).map_err(|e| {
                        Error::Encode(format!("版本目录 uuid 解析失败（{uuid}）: {e}"))
                    })?
                } else {
                    uuid::Uuid::nil()
                },
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
                is_delete_marker: None,
                is_latest: None,
                // NTP 回拨收口：epoch = 旧链 max+1（单调写序号，副本择新优先用它）
                epoch: Some(old_meta.as_ref().map_or(0, |m| m.max_epoch()) + 1),
                // M5 切片 2：加密记录随版本落 meta（旧版本各留各的记录 → 换主密钥后仍可读）
                encryption: enc_record.clone(),
                // M5 切片 4：对象锁随版本落 meta（守卫在删除路径）
                lock: lock_record.clone(),
                replication_status: None,
            };
            // 版本链：versioned 且已有链 → 追加（旧版本目录保留）；否则单版本（M2b 语义）
            let mut meta = match (versioned, old_meta.as_ref()) {
                (true, Some(old)) => {
                    let mut m = old.clone();
                    m.versions.push(version);
                    m
                }
                _ => XlMeta {
                    object_key: key.to_string(),
                    versions: vec![version],
                },
            };
            meta.refresh_latest_flags();

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
            // 提交点前复查租约（Task 8：无 fencing 窗口收窄到两次检查之间）
            if guard.lease_lost() {
                self.rollback_versions(bucket, key, &uuid).await;
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(Error::QuorumUnmet("集群锁租约丢失，写入中止".into()));
            }
            let quorum = self.cfg.ec.data as usize + 1;
            let flipped = match self.write_meta(bucket, key, &meta, old_meta.as_ref()).await {
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
                    "meta 翻转不足 quorum（已提交，M4 heal 补副本）"
                );
            }
            let _ = tokio::fs::remove_dir_all(&tmp).await; // tmp 仅用于流式收全与哈希
            if !versioned {
                // 版本控制桶：旧版本目录**保留**（链上仍有引用）；非版本桶进回收站
                self.recycle_old(bucket, key, old_meta.as_ref()).await;
            }
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
            self.write_meta(bucket, key, &meta, old_meta.as_ref())
                .await?;

            // 5) 老版本进回收站（版本控制桶跳过：旧版本目录仍被链引用）
            if !versioned {
                self.recycle_old(bucket, key, old_meta.as_ref()).await;
            }
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now,
            user_metadata,
            checksum,
            // 与 version_id_str() 同式：versioned 用目录 uuid，否则 S3 的 "null"
            version_id: Some(if versioned {
                uuid.clone()
            } else {
                "null".to_string()
            }),
            encryption: enc_record,
            lock: lock_record,
            replication_status: None,
        })
    }
}
