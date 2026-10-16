//! 读路径：HEAD / 整读 / Range 有界读（含并发覆写时的 meta 重读重试）。
//! M2b 形态分派：`inline` → meta 内嵌字节；`data_layout` → EC 分片重建；否则旧 part 文件。

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::crypto;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, Version, XlMeta};

use super::{ObjectInfo, Store, drives, ec};

/// M5 切片 3（R1/R6）：读面前置校验——SSE-C 记录必带客户钥且 key-MD5 符；非 SSE-C 记录
/// 不得携带客户钥（否则 400，AWS 语义）。HEAD/GET/版本点名读共用（不解密，仅元数据校验）。
pub(in crate::store) fn check_read_sse_c(
    v: &Version,
    sse_c: Option<&crypto::SseCKey>,
) -> Result<()> {
    match (&v.encryption, sse_c) {
        (Some(rec), _) if rec.key_id == "sse-c" => {
            let k = sse_c.ok_or_else(|| {
                Error::InvalidArgument(
                    "SSE-C 对象需要客户端密钥（x-amz-server-side-encryption-customer-key）".into(),
                )
            })?;
            match rec.sse_c_key_md5.as_deref() {
                Some(exp) if exp == k.md5_b64() => Ok(()),
                Some(_) => Err(Error::InvalidArgument(
                    "SSE-C 密钥与对象记录的 key-MD5 不匹配".into(),
                )),
                None => Err(Error::CorruptMeta("SSE-C 记录缺 sse_c_key_md5".into())),
            }
        }
        (_, Some(_)) => Err(Error::InvalidArgument(
            "对象非 SSE-C 加密，不应携带客户端密钥".into(),
        )),
        _ => Ok(()),
    }
}

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
            version_id: Some(v.version_id_str()),
            encryption: v.encryption.clone(),
            lock: v.lock.clone(),
        })
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.head_object_ext(bucket, key, None).await
    }

    /// HEAD（可带 SSE-C 客户钥；M5 切片 3）：SSE-C 对象必须携带**同一把**钥（校验 key-MD5，
    /// 不解密——HEAD 只回元数据）；非 SSE-C 对象携带钥 → 400（AWS 语义）。
    pub async fn head_object_ext(
        &self,
        bucket: &str,
        key: &str,
        sse_c: Option<&crypto::SseCKey>,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        // 版本控制：链尾为 delete marker = 对象「已删除」（S3 404；marker 头在 S3 层按需补，
        // v1 从简只回状态码——记档边界）
        if m.latest_is_marker() {
            return Err(Error::NoSuchKey(key.to_string()));
        }
        if let Some(v) = m.latest() {
            check_read_sse_c(v, sse_c)?;
        }
        self.load_object_info(bucket, key, &m)
    }

    /// 读对象：一次 meta 读取定 info 与数据文件位置。
    ///
    /// 读路径不加锁，meta 与数据文件之间隔着一次 open：覆写者在提交点之后才把老版本目录移进
    /// trash，所以先读到的 meta 可能指向刚被移走的版本目录（数据文件 ENOENT）。此时重读一次
    /// meta 拿新版本再试；两轮都读不到 = 对象确实不在（或被并发删除）→ NoSuchKey。
    /// ponytail: 固定重试一次；持续覆写风暴下不保证收敛（M2 快照读解决）。
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.get_object_ext(bucket, key, None).await
    }

    /// 读对象（可带 SSE-C 客户钥；M5 切片 3）。旧签名委托 None，零 churn。
    pub async fn get_object_ext(
        &self,
        bucket: &str,
        key: &str,
        sse_c: Option<&crypto::SseCKey>,
    ) -> Result<(ObjectInfo, Bytes)> {
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
            // 版本控制：链尾为 delete marker = 对象「已删除」（S3 404，不带内容）
            if v.delete_marker() {
                return Err(Error::NoSuchKey(key.to_string()));
            }
            check_read_sse_c(v, sse_c)?;
            let info = self.load_object_info(bucket, key, &m)?;

            // 形态分派：内联 → EC 布局 → 旧 parts/part
            if let Some(inline) = &v.inline {
                // M5 切片 2：加密 inline 落盘态 = 密文 C = c_len(P)；明文对象逐字节原口径
                let expected = match &v.encryption {
                    Some(_) => crypto::Geometry::new(v.size)?.cipher_len,
                    None => v.size,
                };
                if inline.bytes.len() as u64 != expected {
                    return Err(Error::CorruptMeta(format!(
                        "{bucket}/{key}: 内联长度 {} ≠ {expected}",
                        inline.bytes.len()
                    )));
                }
                let bytes = match &v.encryption {
                    None => inline.bytes.clone(),
                    Some(rec) => {
                        let ctx =
                            crypto::UnitCtx::for_read(rec, self.maybe_enc_state(), sse_c).await?;
                        crypto::open_all(&ctx, v.size, &inline.bytes)?
                    }
                };
                return Ok((info, Bytes::from(bytes)));
            }
            if let Some(l) = &v.data_layout {
                match self
                    .read_ec_range(bucket, key, v, l, None, round > 0, sse_c)
                    .await?
                {
                    Some(bytes) => return Ok((info, bytes)),
                    // 版本目录被并发覆写者移走/清除：重读 meta 拿新版本
                    None => continue,
                }
            }

            // M5 切片 2：加密对象不会以旧 parts 形态落盘（写前 fail-closed）——见到即损坏/伪造
            if v.encryption.is_some() {
                return Err(Error::CorruptMeta(format!(
                    "{bucket}/{key}: 加密版本出现在旧 parts 形态（写路径不可能产出）"
                )));
            }
            let parts = v.effective_parts();
            // 旧形态（ec.enabled=false）parts 路径：整份读进内存——遗留形态按设计保留（非 EC
            // 对象本就无分片可流式）；EC 形态走上方 read_ec_range 分块路径。
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
        self.get_object_range_ext(bucket, key, range, None).await
    }

    /// 有界读（可带 SSE-C 客户钥；M5 切片 3）。旧签名委托 None，零 churn。
    pub async fn get_object_range_ext(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
        sse_c: Option<&crypto::SseCKey>,
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
            // 版本控制：链尾为 delete marker = 对象「已删除」
            if v.delete_marker() {
                return Err(Error::NoSuchKey(key.to_string()));
            }
            check_read_sse_c(v, sse_c)?;

            if range.start > range.end || range.end > v.size {
                return Err(Error::InvalidKey(format!(
                    "range {range:?} 越界（对象 {} 字节）",
                    v.size
                )));
            }

            // 形态分派（同 get_object；M4 切片 2 起 EC 区间读只取命中块 + 逐块重建）
            if let Some(inline) = &v.inline {
                // M5 切片 2：加密 inline 落盘态 = 密文 C = c_len(P)，先解密再切片
                let expected = match &v.encryption {
                    Some(_) => crypto::Geometry::new(v.size)?.cipher_len,
                    None => v.size,
                };
                if inline.bytes.len() as u64 != expected {
                    return Err(Error::CorruptMeta(format!(
                        "{bucket}/{key}: 内联长度 {} ≠ {expected}",
                        inline.bytes.len()
                    )));
                }
                let plain = match &v.encryption {
                    None => inline.bytes.clone(),
                    Some(rec) => {
                        let ctx =
                            crypto::UnitCtx::for_read(rec, self.maybe_enc_state(), sse_c).await?;
                        crypto::open_all(&ctx, v.size, &inline.bytes)?
                    }
                };
                return Ok(Bytes::copy_from_slice(
                    &plain[range.start as usize..range.end as usize],
                ));
            }
            if let Some(l) = &v.data_layout {
                match self
                    .read_ec_range(bucket, key, v, l, Some(range.clone()), round > 0, sse_c)
                    .await?
                {
                    Some(bytes) => return Ok(bytes),
                    None => continue,
                }
            }

            // M5 切片 2：加密对象不会以旧 parts 形态落盘（写前 fail-closed）——见到即损坏/伪造
            if v.encryption.is_some() {
                return Err(Error::CorruptMeta(format!(
                    "{bucket}/{key}: 加密版本出现在旧 parts 形态（写路径不可能产出）"
                )));
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

    /// 单块读取（M4 切片 2 分块路径）：第 i 片第 t 块，长度 = `min(W, ss − t·W)`
    /// （兼容非整块宽的旧对象——整片长度从 meta 的 `shard_size` 取，不假设 W 对齐）。
    /// 位置按拓扑下标解引用：`drives[s.drive] /<bucket>/<展键>/<uuid>/part.<part>.<shard>`。
    /// 缺失/短读/对端不可达 ≡ `None`（EC 重建兜底）；本机 IO 错误 → `Err`。
    #[allow(clippy::too_many_arguments)]
    pub(in crate::store) async fn read_shard_block(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        v: &Version,
        s: &meta::ShardRef,
        i: usize,
        t: u64,
        ss: u64,
    ) -> Result<Option<Vec<u8>>> {
        let off = t * ec::CHUNK_W as u64;
        if off >= ss {
            return Ok(None);
        }
        let len = (ss - off).min(ec::CHUNK_W as u64) as usize;
        match self.shard_target(set, s) {
            Some(drives::ShardTarget::Local { drive, .. }) => {
                let path = drives::shard_path(drive, bucket, key, &v.part.uuid, s)?;
                let mut f = match tokio::fs::File::open(&path).await {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                f.seek(std::io::SeekFrom::Start(off)).await?;
                let mut buf = vec![0u8; len];
                match f.read_exact(&mut buf).await {
                    Ok(_) => Ok(Some(buf)),
                    // 短读：文件比 meta 记录短（崩溃半截件）——视同缺失，交 EC 重建
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Some(drives::ShardTarget::Remote { node, .. }) => {
                // read_shard RPC（offset/length 定位读）：对端不可达/超时 ≡ 该块缺失
                let Some(tp) = self.mesh_transport() else {
                    return Ok(None);
                };
                let r = crate::mesh::ShardRead {
                    bucket: bucket.into(),
                    key: key.into(),
                    uuid: v.part.uuid.clone(),
                    part: s.part as u32,
                    shard: i as u32,
                    offset: off,
                    length: len as u64,
                    // M4 切片 5：槽位 = meta 的 ShardRef.drive（服务端只校验归属）
                    slot: s.drive as u32,
                    // M4 切片 8：桶所属 set
                    set: set as u32,
                };
                match tp.read_shard(node, r).await {
                    Ok(Some(b)) if b.len() == len => Ok(Some(b.to_vec())),
                    Ok(Some(b)) => {
                        tracing::warn!(
                            node,
                            shard = i,
                            got = b.len(),
                            want = len,
                            "远端分块长度不符，视同缺失"
                        );
                        Ok(None)
                    }
                    Ok(None) => Ok(None),
                    Err(e) => {
                        tracing::warn!(node, shard = i, error = %e, "远端分块读取失败，视同缺失");
                        Ok(None)
                    }
                }
            }
            None => {
                tracing::warn!(drive = s.drive, "EC 分片槽位越界，视同缺失");
                Ok(None)
            }
        }
    }

    /// 读齐一块（12 片）并解码：返回补齐后的 12 个块；`Ok(None)` = 本块按轮次语义
    /// 应重试/判缺（存活 < data 且非末轮；或末轮但全对象无任何分块可读）。
    /// 副作用：`any_bytes` 置位（见到过任何块字节）、`missing` 记录缺失（片,块）。
    #[allow(clippy::too_many_arguments)]
    async fn read_decode_block(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &meta::DataLayout,
        reader: &mut ec::ChunkReader,
        t: u64,
        ss: u64,
        strict: bool,
        any_bytes: &mut bool,
        missing: &mut Vec<(usize, u64)>,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>> {
        let total = l.shards.len();
        let data = l.data as usize;
        let mut chunks: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        for (i, s) in l.shards.iter().enumerate() {
            match self
                .read_shard_block(set, bucket, key, v, s, i, t, ss)
                .await?
            {
                Some(b) => {
                    *any_bytes = true;
                    chunks.push(Some(b));
                }
                None => {
                    missing.push((i, t));
                    chunks.push(None);
                }
            }
        }
        let present = chunks.iter().filter(|c| c.is_some()).count();
        if present < data {
            if !strict || !*any_bytes {
                return Ok(None);
            }
            return Err(Error::CorruptMeta(format!(
                "分块存活不足：块 {t} 存活 {present} < data {data}"
            )));
        }
        match reader.decode_block(&mut chunks) {
            // 定位到的坏片也进读时修：读到了正确数据，盘上也要重写回正确内容
            Ok(Some(j)) => missing.push((j, t)),
            Ok(None) => {}
            Err(e) => {
                // 坏片定位失败（多片同时损坏）：非末轮按并发窗口重试，末轮 fail-closed
                if !strict {
                    return Ok(None);
                }
                return Err(e);
            }
        }
        Ok(Some(chunks))
    }

    /// EC 分块读（M4 切片 2）：`range=None` 整读、`Some` 只读命中块（常量内存 O(块)），
    /// 逐块重建 + 12×W 对偶校验（坏片由 ChunkReader 留一定位后剔除重建）。
    /// 单轮语义与旧整片路径对齐——分块读的中间态与真损坏不可区分，故：
    ///   - 非末轮（`strict=false`）：任何取数失败 → `Ok(None)`（并发覆写窗口，调用方重读 meta）；
    ///   - 末轮：全对象无任何分块可读 → `Ok(None)`（目录确实不在 → 调用方报 NoSuchKey）；
    ///     有字节落盘的不足/损坏（含多片同时损坏、定位失败）→ `Err(CorruptMeta)` fail-closed。
    ///
    /// 整读做 SHA256 终验 + 读时修（按「片」整片回写，见 `repair_shards_chunked`）；
    /// 区间读只经逐块对偶校验、不做修复（修复归整读/`heal`）。
    #[allow(clippy::too_many_arguments)]
    pub(in crate::store) async fn read_ec_range(
        &self,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &meta::DataLayout,
        range: Option<std::ops::Range<u64>>,
        strict: bool,
        sse_c: Option<&crypto::SseCKey>,
    ) -> Result<Option<Bytes>> {
        // M4 切片 8：桶所属 set（一次解析、本对象读全线透传）
        let set = self.set_of_bucket(bucket).await?;
        let cfg = ec::EcConfig {
            data: l.data,
            parity: l.parity,
            ..Default::default()
        };
        let data = cfg.data as usize;
        let total = (cfg.data + cfg.parity) as usize;
        let ss = l.shard_size;
        if ss == 0 || l.shards.len() != total {
            return Err(Error::CorruptMeta(format!(
                "EC 布局非法：shard_size={ss} 片数={}（期望 {total}）",
                l.shards.len()
            )));
        }
        // M5 切片 2/3：加密读上下文（一次/读调用，N6 口径）——几何按明文长单源换算
        let enc = match &v.encryption {
            Some(rec) => Some(crypto::UnitCtx::for_read(rec, self.maybe_enc_state(), sse_c).await?),
            None => None,
        };
        let geo = match enc {
            Some(_) => Some(crypto::Geometry::new(v.size)?),
            None => None,
        };
        // EC 载荷长 = 密文流长（无加密 = 明文长，逐字节原口径）
        let payload_len = geo.map_or(v.size, |g| g.cipher_len);
        let blocks = ss.div_ceil(ec::CHUNK_W as u64);
        let out_len = match &range {
            Some(r) => (r.end - r.start) as usize,
            None => payload_len as usize, // 整读先重建 C（加密时）再解密
        };
        let mut out = vec![0u8; out_len];
        let mut reader = ec::ChunkReader::new(cfg)?;
        // 缺失的（片,块）——整读完成后按片整片回写
        let mut missing: Vec<(usize, u64)> = Vec::new();
        let mut any_bytes = false;

        match &range {
            None => {
                // 整读：逐块解码，数据片全部落位
                for t in 0..blocks {
                    let Some(chunks) = self
                        .read_decode_block(
                            set,
                            bucket,
                            key,
                            v,
                            l,
                            &mut reader,
                            t,
                            ss,
                            strict,
                            &mut any_bytes,
                            &mut missing,
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    for (i, c) in chunks.iter().enumerate().take(data) {
                        let Some(c) = c else {
                            return Err(Error::CorruptMeta("分块解码未补齐数据片".into()));
                        };
                        let obj_start = i as u64 * ss + t * ec::CHUNK_W as u64;
                        if obj_start >= payload_len {
                            continue;
                        }
                        // 该片该块的**真实长度**（旧布局末块短于 W；新布局恒为 W）——
                        // 不裁剪会把补零尾巴覆盖到相邻片段的数据上（旧布局多块实测踩过）。
                        // 加密时 payload_len = c_len：重建产物 = 密文流 C（恒等式 obj_start ≡ u·W）。
                        let real_len = (ss - t * ec::CHUNK_W as u64).min(ec::CHUNK_W as u64);
                        let obj_end = (obj_start + real_len).min(payload_len);
                        out[obj_start as usize..obj_end as usize]
                            .copy_from_slice(&c[..(obj_end - obj_start) as usize]);
                    }
                }
            }
            Some(r) => {
                if let (Some(ctx), Some(g)) = (&enc, &geo) {
                    // ===== M5 切片 2：加密区间读（u 驱动精确读；B2 空区间先行短路）=====
                    if r.start == r.end {
                        return Ok(Some(Bytes::new()));
                    }
                    let u0 = g.unit_of_plain(r.start);
                    let u1 = g.unit_of_plain(r.end - 1);
                    debug_assert!(u1 < g.units, "区间读单元必 < nb（r.end ≤ P 约束）");
                    let mut prev_t: Option<u64> = None;
                    let mut block_chunks: Vec<Option<Vec<u8>>> = Vec::new();
                    for u in u0..=u1 {
                        // 单元 u 的密文在（片 s = u/blocks, 块 t = u%blocks）——块解码按 t 去重
                        // （一次 12 片读服务所有同 t 单元；t 跨 s 层回绕时重解一次）
                        let t = u % blocks;
                        let s = (u / blocks) as usize;
                        if prev_t != Some(t) {
                            let Some(c) = self
                                .read_decode_block(
                                    set,
                                    bucket,
                                    key,
                                    v,
                                    l,
                                    &mut reader,
                                    t,
                                    ss,
                                    strict,
                                    &mut any_bytes,
                                    &mut missing,
                                )
                                .await?
                            else {
                                return Ok(None);
                            };
                            block_chunks = c;
                            prev_t = Some(t);
                        }
                        let clen = g.unit_cipher_len(u) as usize;
                        let ct = block_chunks[s]
                            .as_ref()
                            .ok_or_else(|| Error::CorruptMeta("分块解码未补齐数据片".into()))?;
                        let plain = ctx.cipher.open(&ctx.seed, u as u32, &ct[..clen])?;
                        let pstart = g.unit_plain_start(u);
                        let a = r.start.max(pstart);
                        let b = r.end.min(pstart + plain.len() as u64);
                        if a < b {
                            out[(a - r.start) as usize..(b - r.start) as usize].copy_from_slice(
                                &plain[(a - pstart) as usize..(b - pstart) as usize],
                            );
                        }
                    }
                    return Ok(Some(Bytes::from(out)));
                }
                // 区间读：「(片段 s, 块 t) 对」迭代——对象偏移 o 的落位 = (o/ss, (o%ss)/W)。
                // s0..s1 为区间覆盖的片段；块解码按 t 去重（一块 12 片读一次，服务所有片段）。
                // 代价注记：**跨片段大区间**会解码 [0, blocks) 全部块（IO ≈ 整对象，内存仍
                // O(块)）；单片段区间是精确命中读——常见 Range 都在单片段内。
                let s0 = r.start / ss;
                let s1 = r.end.saturating_sub(1) / ss;
                let w = ec::CHUNK_W as u64;
                let ta0 = (r.start - s0 * ss) / w;
                let tb1 = (r.end.saturating_sub(1) - s1 * ss) / w;
                let (t_min, t_max) = if s1 > s0 {
                    (0, blocks.saturating_sub(1))
                } else {
                    (ta0, tb1)
                };
                for t in t_min..=t_max {
                    let Some(chunks) = self
                        .read_decode_block(
                            set,
                            bucket,
                            key,
                            v,
                            l,
                            &mut reader,
                            t,
                            ss,
                            strict,
                            &mut any_bytes,
                            &mut missing,
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    for s in s0..=s1 {
                        // 首段从 ta0 起、末段到 tb1 止；中间段全覆
                        if !(s > s0 || t >= ta0) || !(s < s1 || t <= tb1) {
                            continue;
                        }
                        if s as usize >= data {
                            continue; // 区间在补零尾段（r.end ≤ size 时不会发生，防呆）
                        }
                        let obj_start = s * ss + t * w;
                        if obj_start >= v.size {
                            continue;
                        }
                        // 同整读分支：该片该块真实长度（旧布局末块可短于 W）
                        let real_len = (ss - t * w).min(w);
                        let obj_end = (obj_start + real_len).min(v.size);
                        let a = r.start.max(obj_start);
                        let b = r.end.min(obj_end);
                        if a < b {
                            let c = chunks[s as usize]
                                .as_ref()
                                .ok_or_else(|| Error::CorruptMeta("分块解码未补齐数据片".into()))?;
                            out[(a - r.start) as usize..(b - r.start) as usize].copy_from_slice(
                                &c[(a - obj_start) as usize..(b - obj_start) as usize],
                            );
                        }
                    }
                }
            }
        }

        if range.is_none() {
            // 整读：SHA256 终验（区间读没有整份可校验——逐块对偶已在 decode_block 里承担）
            if let Some(ctx) = &enc {
                // out 现为密文流 C：解密（逐单元 trim；条带补零由 c_len 截断兜住）→ P 域终验。
                // 读时修须从 **C** 重编码（从 P 重编码会写出错误分片内容）。
                let c = std::mem::take(&mut out);
                let plain = crypto::open_all(ctx, v.size, &c)?;
                if hex::encode(Sha256::digest(&plain)) != v.part.sha256 {
                    return Err(Error::CorruptMeta(format!(
                        "EC 重建后 SHA256 与提交记录不符（对象 {} 字节）",
                        v.size
                    )));
                }
                if !missing.is_empty() {
                    self.repair_shards_chunked(set, bucket, key, v, l, &c, &missing)
                        .await;
                }
                return Ok(Some(Bytes::from(plain)));
            }
            if hex::encode(Sha256::digest(&out)) != v.part.sha256 {
                return Err(Error::CorruptMeta(format!(
                    "EC 重建后 SHA256 与提交记录不符（对象 {} 字节）",
                    v.size
                )));
            }
            if !missing.is_empty() {
                self.repair_shards_chunked(set, bucket, key, v, l, &out, &missing)
                    .await;
            }
        }
        Ok(Some(Bytes::from(out)))
    }

    /// 读时修（M4 切片 2 分块版）：对这次整读里缺失过的片**整片重写**（数据来自本次
    /// 重建的 payload，逐块重编）。best-effort——修复失败只 warn（读已成功，绝不因修复
    /// 失败把读变成错误）。本地先探活再写（不得对掉线盘「修复」：挂载点目录仍在父 fs 上，
    /// 盲写会填满根盘）；远端走 write_shard 分块 RPC。
    /// M4 切片 4 已落地批量面：`heal`（fsck --repair + 桶目录补建）与 `scrub`（逐块深度
    /// 对偶校验）；本函数仍是**读时兜底**（盘全掉时修复静默失败，报告由 scrub 承担）。
    #[allow(clippy::too_many_arguments)]
    async fn repair_shards_chunked(
        &self,
        set: u16,
        bucket: &str,
        key: &str,
        v: &Version,
        l: &meta::DataLayout,
        payload: &[u8],
        missing: &[(usize, u64)],
    ) {
        let cfg = ec::EcConfig {
            data: l.data,
            parity: l.parity,
            ..Default::default()
        };
        let data = cfg.data as usize;
        let ss = l.shard_size;
        let blocks = ss.div_ceil(ec::CHUNK_W as u64);
        let mut bad: Vec<usize> = missing.iter().map(|(i, _)| *i).collect();
        bad.sort_unstable();
        bad.dedup();
        if bad.is_empty() {
            return;
        }
        enum Sink {
            Local {
                f: tokio::fs::File,
                path: std::path::PathBuf,
            },
            Remote(String),
            Skip,
        }
        let transport = self.mesh_transport();
        let mut sinks: Vec<(usize, Sink)> = Vec::new();
        for &i in &bad {
            let Some(s) = l.shards.get(i) else { continue };
            let sink = match self.shard_target(set, s) {
                Some(drives::ShardTarget::Local { drive, .. }) => {
                    if !drives::drive_up(drive).await {
                        Sink::Skip
                    } else {
                        let path = match drives::shard_path(drive, bucket, key, &v.part.uuid, s) {
                            Ok(p) => p,
                            Err(_) => {
                                sinks.push((i, Sink::Skip));
                                continue;
                            }
                        };
                        let res: Result<tokio::fs::File> = async {
                            if let Some(parent) = path.parent() {
                                tokio::fs::create_dir_all(parent).await?;
                            }
                            Ok(tokio::fs::File::create(&path).await?)
                        }
                        .await;
                        match res {
                            Ok(f) => Sink::Local { f, path },
                            Err(e) => {
                                tracing::warn!(path = %path.display(), error = %e, "读时修打开失败");
                                Sink::Skip
                            }
                        }
                    }
                }
                Some(drives::ShardTarget::Remote { node, .. }) => {
                    if transport.is_some() {
                        Sink::Remote(node.to_string())
                    } else {
                        Sink::Skip
                    }
                }
                None => Sink::Skip,
            };
            sinks.push((i, sink));
        }
        let Ok(mut w) = ec::ChunkWriter::new(cfg) else {
            return;
        };
        for t in 0..blocks {
            // 从内存 payload 散读一块（越界补零——片尾补零即真实片尾内容）
            let mut ins: Vec<Vec<u8>> = Vec::with_capacity(data);
            for i in 0..data {
                let mut c = vec![0u8; ec::CHUNK_W];
                let start = i as u64 * ss + t * ec::CHUNK_W as u64;
                if start < payload.len() as u64 {
                    let take = ((payload.len() as u64 - start).min(ec::CHUNK_W as u64)) as usize;
                    c[..take].copy_from_slice(&payload[start as usize..start as usize + take]);
                }
                ins.push(c);
            }
            let Ok(out) = w.encode_block(&ins) else {
                return;
            };
            let is_final = t + 1 == blocks;
            // 块的真实长度（旧对象末块可短于 W；新对象恒为 W）
            let real_len = (ss - t * ec::CHUNK_W as u64).min(ec::CHUNK_W as u64) as usize;
            for (i, sink) in sinks.iter_mut() {
                let chunk = match out.get(*i) {
                    Some(c) => &c[..real_len],
                    None => continue,
                };
                match sink {
                    Sink::Skip => {}
                    Sink::Local { f, path } => {
                        let res: Result<()> = async {
                            f.write_all(chunk).await?;
                            if is_final {
                                f.flush().await?;
                                if self.cfg.fsync {
                                    f.sync_all().await?;
                                }
                            }
                            Ok(())
                        }
                        .await;
                        if let Err(e) = res {
                            tracing::warn!(path = %path.display(), error = %e, "读时修回写失败");
                            *sink = Sink::Skip;
                        }
                    }
                    Sink::Remote(node) => {
                        let mut failed = None;
                        if let Some(tp) = self.mesh_transport() {
                            let req = crate::mesh::ShardWrite {
                                bucket: bucket.into(),
                                key: key.into(),
                                uuid: v.part.uuid.clone(),
                                part: l.shards[*i].part as u32,
                                shard: *i as u32,
                                crc32c: 0,
                                data: bytes::Bytes::copy_from_slice(chunk),
                                chunked: true,
                                offset: t * ec::CHUNK_W as u64,
                                chunk_crc32c: ec::crc32c_of(chunk),
                                is_final,
                                slot: l.shards[*i].drive as u32,
                                set: set as u32,
                            };
                            if let Err(e) = tp.write_shard(node, req).await {
                                failed = Some(e);
                            }
                        }
                        if let Some(e) = failed {
                            tracing::warn!(node, shard = *i, error = %e, "远端读时修回写失败");
                            *sink = Sink::Skip;
                        }
                    }
                }
            }
        }
    }
}
