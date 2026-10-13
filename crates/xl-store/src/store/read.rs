//! 读路径：HEAD / 整读 / Range 有界读（含并发覆写时的 meta 重读重试）。
//! M2b 形态分派：`inline` → meta 内嵌字节；`data_layout` → EC 分片重建；否则旧 part 文件。

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, Version, XlMeta};

use super::{ObjectInfo, Store, drives, ec};

impl Store {
    pub(in crate::store) fn load_object_info(
        &self,
        bucket: &str,
        key: &str,
        m: &XlMeta,
    ) -> Result<ObjectInfo> {
        let v = m
            .latest()
            .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: v.size,
            etag: v.etag.clone(),
            content_type: v.content_type.clone(),
            last_modified_nanos: v.mod_time_nanos,
            user_metadata: v.user_metadata.clone(),
            checksum: v.checksum.clone(),
        })
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        self.load_object_info(bucket, key, &m)
    }

    /// 读对象：一次 meta 读取定 info 与数据文件位置。
    ///
    /// 读路径不加锁，meta 与数据文件之间隔着一次 open：覆写者在提交点之后才把老版本目录移进
    /// trash，所以先读到的 meta 可能指向刚被移走的版本目录（数据文件 ENOENT）。此时重读一次
    /// meta 拿新版本再试；两轮都读不到 = 对象确实不在（或被并发删除）→ NoSuchKey。
    /// ponytail: 固定重试一次；持续覆写风暴下不保证收敛（M2 快照读解决）。
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?;

        for round in 0..2 {
            // 回退一次（Task 6/D1）：快路径集合的数据轮失败 → 全量扇出取最新再试
            let m = if round == 0 {
                self.read_meta(bucket, key).await?
            } else {
                self.read_meta_full(bucket, key).await?
            }
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
            let info = self.load_object_info(bucket, key, &m)?;

            // 形态分派：内联 → EC 布局 → 旧 parts/part
            if let Some(inline) = &v.inline {
                if inline.bytes.len() as u64 != v.size {
                    return Err(Error::CorruptMeta(format!(
                        "{bucket}/{key}: 内联长度 {} ≠ 对象长度 {}",
                        inline.bytes.len(),
                        v.size
                    )));
                }
                return Ok((info, Bytes::from(inline.bytes.clone())));
            }
            if let Some(l) = &v.data_layout {
                match self.read_ec(bucket, key, v, l, round > 0).await? {
                    Some(bytes) => return Ok((info, bytes)),
                    // 版本目录被并发覆写者移走/清除：重读 meta 拿新版本
                    None => continue,
                }
            }

            let parts = v.effective_parts();
            // ponytail: M2a 整份读进内存（跨 part 拼接，与既有限制同口径）；M2b 换流式。
            let mut buf = Vec::with_capacity(v.size as usize);
            let mut missing = false;
            for p in &parts {
                let path = dir.join(&p.uuid).join(format!("part.{}", p.part));
                match tokio::fs::read(&path).await {
                    Ok(bytes) => buf.extend_from_slice(&bytes),
                    // 旧版本目录已被并发覆写者移走：下一轮重读 meta 拿新版本
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        missing = true;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            if missing {
                continue;
            }
            return Ok((info, Bytes::from(buf)));
        }
        // 两轮 meta → 数据文件都 ENOENT：按 S3 语义报 NoSuchKey，而不是把裸 IO 错误抛给上层
        Err(Error::NoSuchKey(key.to_string()))
    }

    /// 有界读：`range` 为半开区间 `[start, end)`，须满足 `start <= end <= size`
    ///（越界由调用方——s3s 的 `Range::check`——先拦；内核仍防呆，防呆错误是内部错误）。
    /// 重试语义与 `get_object` 相同（并发覆写时重读 meta 一轮）。
    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?;

        for round in 0..2 {
            // 回退一次（Task 6/D1）：快路径集合的数据轮失败 → 全量扇出取最新再试
            let m = if round == 0 {
                self.read_meta(bucket, key).await?
            } else {
                self.read_meta_full(bucket, key).await?
            }
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;

            if range.start > range.end || range.end > v.size {
                return Err(Error::InvalidKey(format!(
                    "range {range:?} 越界（对象 {} 字节）",
                    v.size
                )));
            }

            // 形态分派（同 get_object；EC 按整份重建后切片——ponytail: 分片级按需重建是 M2b-3 之后的升级路径）
            if let Some(inline) = &v.inline {
                if inline.bytes.len() as u64 != v.size {
                    return Err(Error::CorruptMeta(format!(
                        "{bucket}/{key}: 内联长度 {} ≠ 对象长度 {}",
                        inline.bytes.len(),
                        v.size
                    )));
                }
                return Ok(Bytes::copy_from_slice(
                    &inline.bytes[range.start as usize..range.end as usize],
                ));
            }
            if let Some(l) = &v.data_layout {
                match self.read_ec(bucket, key, v, l, round > 0).await? {
                    Some(bytes) => {
                        return Ok(bytes.slice(range.start as usize..range.end as usize));
                    }
                    None => continue,
                }
            }

            // 跨 part 映射：按 parts 序列累计偏移，只读命中段（首尾 part 各读一段）。
            // ponytail: M2a 单盘无分片；M2b 起 part 内部再映射到分片。
            let parts = v.effective_parts();
            let mut buf = Vec::with_capacity((range.end - range.start) as usize);
            let mut offset: u64 = 0;
            let mut missing = false;
            for p in &parts {
                let part_end = offset + p.size;
                // 本 part 与请求区间的交集（半开）；空交集跳过
                let s = range.start.max(offset);
                let e = range.end.min(part_end);
                if s < e {
                    let path = dir.join(&p.uuid).join(format!("part.{}", p.part));
                    let mut file = match tokio::fs::File::open(&path).await {
                        Ok(f) => f,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            missing = true;
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    };
                    file.seek(std::io::SeekFrom::Start(s - offset)).await?;
                    let mut chunk = vec![0u8; (e - s) as usize];
                    match file.read_exact(&mut chunk).await {
                        // tokio 的 read_exact 返回 io::Result<usize>（与 std 的 Result<()> 不同）
                        Ok(_) => buf.extend_from_slice(&chunk),
                        // 版本目录被并发覆写者移走/截断：下一轮重读 meta 拿新版本
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            missing = true;
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                offset = part_end;
                if offset >= range.end {
                    break;
                }
            }
            if missing {
                continue;
            }
            return Ok(Bytes::from(buf));
        }
        Err(Error::NoSuchKey(key.to_string()))
    }

    /// EC 读：逐片读取 + CRC32C/长度校验（坏片视同缺失）→ 重建 → 截断到对象长度 → SHA256 终验。
    ///
    /// 分片位置按拓扑下标解引用：`drives[s.drive] /<bucket>/<展键>/<uuid>/part.<part>.<shard>`。
    /// 分片是逐个读的，并发删除/覆写的中间态会让读者看到「前缀片在、后缀片没了」的半截视图，
    /// 与真实损坏（盘丢片）不可在单轮内区分——因此 `strict`（末轮）之前，「存活不足」返回
    /// `Ok(None)` 让调用方重读 meta 一轮：
    ///   - 非末轮：任何拿不到数据的视图都 `Ok(None)`（可能是并发窗口）；
    ///   - 末轮：全片 ENOENT 且未读到任何字节 = 目录确实不在 → `Ok(None)`（调用方报 NoSuchKey）；
    ///     只要读到过坏片字节（CRC/长度不符）或存活片不全但不为空 → `Err(CorruptMeta)`
    ///     （有字节落盘的损坏不得伪装成 404——fail-closed，不吐坏数据）。
    ///
    /// 重建后 SHA256 与提交记录不符同样 `Err`。重建成功且存在缺失/坏片时做**读时修**
    /// （best-effort 回写，ponytail: 无后台 heal；M4 的 heal 任务补全）。
    async fn read_ec(
        &self,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &meta::DataLayout,
        strict: bool,
    ) -> Result<Option<Bytes>> {
        let cfg = ec::EcConfig {
            data: l.data,
            parity: l.parity,
            ..Default::default()
        };
        let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(l.shards.len());
        // 缺失/坏片的下标（读时修的输入）
        let mut missing: Vec<usize> = Vec::new();
        // 读到过 CRC32C/长度不符的字节：区分「全片消失」（可能并发窗口/真删）与「片坏掉」（真损坏）
        let mut bad_bytes = false;
        for (i, s) in l.shards.iter().enumerate() {
            // 落点解析：单机 = 本机盘下标（越界视同缺失，防呆）；集群 = 本机盘 / 远端槽位
            let drive = match self.shard_target(s) {
                Some(drives::ShardTarget::Local { drive, .. }) => drive,
                Some(drives::ShardTarget::Remote { node, .. }) => {
                    // read_shard RPC：对端不可达/超时 ≡ 该片缺失（EC 重建兜底）
                    let got = match self.mesh_transport() {
                        Some(t) => {
                            let r = crate::mesh::ShardRead {
                                bucket: bucket.into(),
                                key: key.into(),
                                uuid: v.part.uuid.clone(),
                                part: s.part as u32,
                                shard: i as u32,
                            };
                            match t.read_shard(node, r).await {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(
                                        node,
                                        shard = i,
                                        error = %e,
                                        "远端分片读取失败，视同缺失"
                                    );
                                    None
                                }
                            }
                        }
                        None => None,
                    };
                    match got {
                        Some(b) if ec::crc32c_of(&b) == s.crc32c && b.len() as u64 == s.size => {
                            shards.push(Some(b.to_vec()));
                        }
                        Some(_) => {
                            tracing::warn!(
                                node,
                                shard = i,
                                "远端分片校验不符（CRC32C/长度），视同缺失"
                            );
                            bad_bytes = true;
                            missing.push(i);
                            shards.push(None);
                        }
                        None => {
                            missing.push(i);
                            shards.push(None);
                        }
                    }
                    continue;
                }
                None => {
                    tracing::warn!(drive = s.drive, "EC 分片槽位越界，视同缺失");
                    missing.push(i);
                    shards.push(None);
                    continue;
                }
            };
            let path = drives::shard_path(drive, bucket, key, &v.part.uuid, s)?;
            match tokio::fs::read(&path).await {
                Ok(bytes) => {
                    if ec::crc32c_of(&bytes) == s.crc32c && bytes.len() as u64 == s.size {
                        shards.push(Some(bytes));
                    } else {
                        tracing::warn!(path = %path.display(), "EC 分片校验不符（CRC32C/长度），视同缺失");
                        bad_bytes = true;
                        missing.push(i);
                        shards.push(None);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(i);
                    shards.push(None);
                }
                Err(e) => return Err(e.into()),
            }
        }
        let present = shards.iter().filter(|s| s.is_some()).count();
        if present < cfg.data as usize {
            if !strict {
                return Ok(None);
            }
            if present == 0 && !bad_bytes {
                return Ok(None);
            }
            return Err(Error::CorruptMeta(format!(
                "分片不足：存活 {present} < data {}（读到过坏片字节：{bad_bytes}）",
                cfg.data
            )));
        }
        let padded = ec::reconstruct(&shards, cfg)?;
        if (padded.len() as u64) < v.size {
            return Err(Error::CorruptMeta(format!(
                "EC 重建长度 {} < 对象长度 {}",
                padded.len(),
                v.size
            )));
        }
        // 截断到对象长度（move 复用缓冲，不再整份复制）
        let mut payload = padded;
        payload.truncate(v.size as usize);
        if hex::encode(Sha256::digest(&payload)) != v.part.sha256 {
            return Err(Error::CorruptMeta(format!(
                "EC 重建后 SHA256 与提交记录不符（对象 {} 字节）",
                v.size
            )));
        }
        if !missing.is_empty() {
            self.repair_shards(bucket, key, v, l, &payload, &missing)
                .await;
        }
        Ok(Some(Bytes::from(payload)))
    }

    /// 读时修（M2b-2 最小版）：把重建出的数据重新编码，回写缺失/坏片的那个分片文件。
    /// best-effort——修复失败只 warn（读已成功，绝不因修复失败把读变成错误）。
    /// ponytail: 无后台 heal / 无 quorum 校验（盘全掉时修复静默失败）；M4 的 heal 任务补全。
    async fn repair_shards(
        &self,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &meta::DataLayout,
        payload: &[u8],
        missing: &[usize],
    ) {
        let cfg = ec::EcConfig {
            data: l.data,
            parity: l.parity,
            ..Default::default()
        };
        let Ok(shards) = ec::encode(payload, cfg) else {
            return;
        };
        for &i in missing {
            let Some(s) = l.shards.get(i) else { continue };
            let drive = match self.shard_target(s) {
                Some(drives::ShardTarget::Local { drive, .. }) => drive,
                Some(drives::ShardTarget::Remote { node, .. }) => {
                    // write_shard RPC 回写（探活 = RPC 本身；读已成功，绝不因修复失败报错）
                    if let Some(t) = self.mesh_transport() {
                        let w = crate::mesh::ShardWrite {
                            bucket: bucket.into(),
                            key: key.into(),
                            uuid: v.part.uuid.clone(),
                            part: s.part as u32,
                            shard: s.shard as u32,
                            crc32c: s.crc32c,
                            data: bytes::Bytes::copy_from_slice(&shards[i]),
                        };
                        if let Err(e) = t.write_shard(node, w).await {
                            tracing::warn!(node, shard = i, error = %e, "远端读时修回写失败");
                        }
                    }
                    continue;
                }
                None => continue,
            };
            // 存活探测：不得对掉线盘「修复」——挂载点目录仍在父 fs 上，盲写会填满根盘，
            // 测试里更会直接把「拔走」的盘原地重建（探针语义见 drives::drive_up）
            if !drives::drive_up(drive).await {
                continue;
            }
            let Ok(path) = drives::shard_path(drive, bucket, key, &v.part.uuid, s) else {
                continue;
            };
            let res: Result<()> = async {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let mut f = tokio::fs::File::create(&path).await?;
                f.write_all(&shards[i]).await?;
                f.flush().await?;
                if self.cfg.fsync {
                    f.sync_all().await?;
                }
                Ok(())
            }
            .await;
            if let Err(e) = res {
                tracing::warn!(path = %path.display(), error = %e, "读时修回写失败");
            }
        }
    }
}
