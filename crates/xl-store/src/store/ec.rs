//! EC 纠删码编解码与分片校验（galois_8 / 8+4 默认）。
//!
//! 两条路径同格式：整份 `encode/reconstruct`（小对象与旧调用方）与**分块**编解码
//! （M4 切片 2：大对象流式，常量内存——EC 按列独立，[`ChunkWriter`]/[`ChunkReader`] 逐块
//! 喂 `data×CHUNK_W` 跨片散读、产出 `12×CHUNK_W`）。分片布局两者一致：第 i 片 =
//! payload 第 i 段（段长 `shard_size = 块数×W`，末段尾部补零），新旧读写互操作成立。
//!
//! 校验分两层：`crc32c_of` 对单片（与 meta 里存的 ShardRef.crc32c 比对，坏片视同缺失），
//! `shards_consistent` 用 rs 的 `verify` 做跨片奇偶一致性（抓 CRC 之后仍可能混入的错配）；
//! 分块路径用逐块 12×W 降维 `verify` 承担同一职责（range 读没有整片可校验）。

use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::error::{Error, Result};

/// EC 配置：data + parity 片数（默认 8+4；可配 4+2 / 6+3 / 12+4 等）+ 总开关。
/// `enabled=false` = M1 旧形态（单文件 part.1、无内联、无分片）——只支持单盘（`open` 校验）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EcConfig {
    pub data: u8,
    pub parity: u8,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl Default for EcConfig {
    fn default() -> Self {
        Self {
            data: 8,
            parity: 4,
            enabled: true,
        }
    }
}

/// 分片长度：payload 按 data 片均分，末片补零对齐。
fn shard_size(len: usize, data: u8) -> usize {
    len.div_ceil(data as usize)
}

fn codec(cfg: EcConfig) -> Result<ReedSolomon> {
    ReedSolomon::new(cfg.data as usize, cfg.parity as usize)
        .map_err(|e| Error::Encode(format!("EC 配置非法（{} + {}）: {e}", cfg.data, cfg.parity)))
}

/// 编码：返回 `data + parity` 片，前 data 片为数据片（末片尾部补零）。
pub fn encode(payload: &[u8], cfg: EcConfig) -> Result<Vec<Vec<u8>>> {
    let rs = codec(cfg)?;
    let size = shard_size(payload.len(), cfg.data);
    let mut shards: Vec<Vec<u8>> = vec![vec![0u8; size]; (cfg.data + cfg.parity) as usize];
    for (i, chunk) in payload.chunks(size.max(1)).enumerate() {
        shards[i][..chunk.len()].copy_from_slice(chunk);
    }
    rs.encode(&mut shards)
        .map_err(|e| Error::Encode(format!("EC 编码失败: {e}")))?;
    Ok(shards)
}

/// 重建全部片：缺失片（`None` 或长度不符）以 EC 补齐，返回**补齐后的 data+parity 片**。
/// shrink apply 需要把「源片缺失」的字节重建到**目标槽**（不只是数据段），故开放全片结果。
pub fn reconstruct_shards(shards: &[Option<Vec<u8>>], cfg: EcConfig) -> Result<Vec<Vec<u8>>> {
    let total = (cfg.data + cfg.parity) as usize;
    if shards.len() != total {
        return Err(Error::CorruptMeta(format!(
            "分片数不符：{} 片（期望 {total}）",
            shards.len()
        )));
    }
    let size = shards.iter().flatten().map(Vec::len).max().unwrap_or(0);
    if size == 0 {
        return Err(Error::CorruptMeta("全部分片缺失".into()));
    }
    let present = shards.iter().filter(|s| s.is_some()).count();
    if present < cfg.data as usize {
        return Err(Error::CorruptMeta(format!(
            "分片不足：存活 {present} < data {}",
            cfg.data
        )));
    }

    let mut buf: Vec<(Vec<u8>, bool)> = shards
        .iter()
        .map(|s| match s {
            Some(v) if v.len() == size => (v.clone(), true),
            _ => (vec![0u8; size], false),
        })
        .collect();
    let rs = codec(cfg)?;
    rs.reconstruct(&mut buf)
        .map_err(|e| Error::CorruptMeta(format!("EC 重建失败: {e}")))?;
    Ok(buf.into_iter().map(|(v, _)| v).collect())
}

/// 重建：缺失片（`None` 或长度不符）以 EC 补齐，返回**补齐后的**数据段（前 data 片拼接）。
/// 尾部补零的截断由调用方按 meta 里的对象长度完成（EC 层不知道原始长度）。
pub fn reconstruct(shards: &[Option<Vec<u8>>], cfg: EcConfig) -> Result<Vec<u8>> {
    let all = reconstruct_shards(shards, cfg)?;
    let mut out = Vec::with_capacity(all.first().map_or(0, Vec::len) * cfg.data as usize);
    for v in all.iter().take(cfg.data as usize) {
        out.extend_from_slice(v);
    }
    Ok(out)
}

/// 单片 CRC32C（与 meta 里 ShardRef.crc32c 比对用；不符 = 坏片，读路径视同缺失）。
pub fn crc32c_of(shard: &[u8]) -> u32 {
    crc32c::crc32c(shard)
}

/// 跨片奇偶一致性（rs 的 verify）：parity 与 data 不符即为 false。
/// 读路径用它做重建前的预检；单点 CRC 抓到的坏片在重建前应先被剔除为缺失。
pub fn shards_consistent(shards: &[Vec<u8>], cfg: EcConfig) -> Result<bool> {
    if shards.len() != (cfg.data + cfg.parity) as usize {
        return Err(Error::CorruptMeta(format!(
            "分片数不符：{} 片",
            shards.len()
        )));
    }
    let rs = codec(cfg)?;
    rs.verify(shards)
        .map_err(|e| Error::CorruptMeta(format!("EC 一致性校验失败: {e}")))
}

/// 流式块宽：每片每块输出字节数（一个输入块的跨片散读跨度 = data×W）。
pub const CHUNK_W: usize = 64 * 1024;

/// 分块写端（M4 切片 2）：列独立逐块编码，跨块累计每片 CRC32C。
/// 块语义：第 t 块 = 各片文件偏移 `[t·W, (t+1)·W)`；分片总长 = 块数×W（末块补零对齐）。
pub struct ChunkWriter {
    rs: ReedSolomon,
    data: u8,
    parity: u8,
    crcs: Vec<u32>,
    blocks: u64,
}

impl ChunkWriter {
    pub fn new(cfg: EcConfig) -> Result<Self> {
        Ok(Self {
            rs: codec(cfg)?,
            data: cfg.data,
            parity: cfg.parity,
            crcs: vec![0; (cfg.data + cfg.parity) as usize],
            blocks: 0,
        })
    }

    /// 编码一块：`data_chunks` 恰 `data` 个、各长 ≤ W（右补零在编码前完成）。
    /// 返回 `data+parity` 个长度恰 W 的块；同时增量累计各片 CRC32C。
    pub fn encode_block(&mut self, data_chunks: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        if data_chunks.len() != self.data as usize {
            return Err(Error::Encode(format!(
                "块输入片数 {} ≠ data {}",
                data_chunks.len(),
                self.data
            )));
        }
        let total = (self.data + self.parity) as usize;
        let mut blocks: Vec<Vec<u8>> = vec![vec![0u8; CHUNK_W]; total];
        for (i, c) in data_chunks.iter().enumerate() {
            if c.len() > CHUNK_W {
                return Err(Error::Encode(format!(
                    "块输入片 {i} 超宽（{} > {CHUNK_W}）",
                    c.len()
                )));
            }
            blocks[i][..c.len()].copy_from_slice(c);
        }
        self.rs
            .encode(&mut blocks)
            .map_err(|e| Error::Encode(format!("EC 分块编码失败: {e}")))?;
        for (i, b) in blocks.iter().enumerate() {
            self.crcs[i] = crc32c::crc32c_append(self.crcs[i], b);
        }
        self.blocks += 1;
        Ok(blocks)
    }

    /// 第 i 片的累计 CRC32C（读端按整片重算比对；与整份编码的 `crc32c_of` 同值）。
    pub fn shard_crc(&self, i: usize) -> u32 {
        self.crcs[i]
    }

    /// 分片总长（块数×W；写入 meta 的 `shard_size`）。
    pub fn shard_size(&self) -> u64 {
        self.blocks * CHUNK_W as u64
    }
}

/// 分块读端：逐块重建（缺失片 EC 补齐）+ 逐块 12×W 降维对偶校验。
pub struct ChunkReader {
    rs: ReedSolomon,
    data: u8,
    parity: u8,
}

impl ChunkReader {
    pub fn new(cfg: EcConfig) -> Result<Self> {
        Ok(Self {
            rs: codec(cfg)?,
            data: cfg.data,
            parity: cfg.parity,
        })
    }

    /// 重建一块：`chunks` 恰 `data+parity` 个（`Some` = 该片本块字节、长 ≤W；`None` = 缺失）。
    /// 存活 < data → `Err(CorruptMeta)`（调用方按轮次语义处置，与整份路径同口径）；
    /// 否则补齐缺失块并做对偶校验：**重建后全体**不一致（含「在场但损坏」的片）→
    /// 留一定位坏片（其余 11 片自洽者即坏）→ 剔除重建；≥2 片同坏无法唯一定位 →
    /// `Err(CorruptMeta)` fail-closed——分块路径没有整片 CRC，这是损坏的唯一闸。
    /// 成功时 `chunks` 回写为 12 个补齐块（各 W 字节）；返回值 = 本块定位到的坏片下标
    /// （`Some(j)`，调用方据此把该片加入读时修——盘上重写回正确内容）。
    pub fn decode_block(&mut self, chunks: &mut [Option<Vec<u8>>]) -> Result<Option<usize>> {
        let total = (self.data + self.parity) as usize;
        if chunks.len() != total {
            return Err(Error::CorruptMeta(format!(
                "分块片数不符：{} 片（期望 {total}）",
                chunks.len()
            )));
        }
        let present = chunks.iter().filter(|c| c.is_some()).count();
        if present < self.data as usize {
            return Err(Error::CorruptMeta(format!(
                "分块存活不足：{present} < data {}",
                self.data
            )));
        }
        let mut buf: Vec<(Vec<u8>, bool)> = Vec::with_capacity(total);
        for (i, c) in chunks.iter().enumerate() {
            match c {
                Some(v) if v.len() <= CHUNK_W => {
                    let mut p = v.clone();
                    p.resize(CHUNK_W, 0); // 短块右补零（重建要求各片等长）
                    buf.push((p, true));
                }
                Some(v) => {
                    return Err(Error::CorruptMeta(format!(
                        "分块 {i} 超宽（{} > {CHUNK_W}）",
                        v.len()
                    )));
                }
                None => buf.push((vec![0u8; CHUNK_W], false)),
            }
        }
        if present < total {
            self.rs
                .reconstruct(&mut buf)
                .map_err(|e| Error::CorruptMeta(format!("EC 分块重建失败: {e}")))?;
        }
        let slices: Vec<&[u8]> = buf.iter().map(|(v, _)| v.as_slice()).collect();
        let ok = self
            .rs
            .verify(&slices)
            .map_err(|e| Error::CorruptMeta(format!("EC 分块校验失败: {e}")))?;
        let mut bad: Option<usize> = None;
        if !ok {
            // 坏片定位（整份路径靠整片 CRC；逐块没有 CRC，用「留一重建」等价物化）：
            // 逐个假设 j 离场，若其余 11 片自洽（重建 j 后 verify 过）→ j 就是坏片。
            // 恰好一片坏定位成立；≥2 片同时坏无法唯一定位 → fail-closed。
            for j in 0..total {
                if !buf[j].1 {
                    continue;
                }
                let mut probe: Vec<(Vec<u8>, bool)> = buf
                    .iter()
                    .enumerate()
                    .map(|(i, (v, p))| (v.clone(), *p && i != j))
                    .collect();
                if self.rs.reconstruct(&mut probe).is_err() {
                    continue;
                }
                let ps: Vec<&[u8]> = probe.iter().map(|(v, _)| v.as_slice()).collect();
                if self
                    .rs
                    .verify(&ps)
                    .map_err(|e| Error::CorruptMeta(format!("EC 分块校验失败: {e}")))?
                {
                    bad = Some(j);
                    break;
                }
            }
            match bad {
                Some(j) => {
                    // 剔除坏片 → 以（≥data 的）在场片重建
                    buf[j].1 = false;
                    self.rs
                        .reconstruct(&mut buf)
                        .map_err(|e| Error::CorruptMeta(format!("EC 分块重建失败: {e}")))?;
                }
                None => {
                    return Err(Error::CorruptMeta(
                        "EC 分块对偶校验不过且无法定位坏片（≥2 片损坏？）".into(),
                    ));
                }
            }
        }
        for (i, (v, _)) in buf.into_iter().enumerate() {
            chunks[i] = Some(v);
        }
        Ok(bad)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> EcConfig {
        EcConfig {
            data: 8,
            parity: 4,
            ..Default::default()
        }
    }

    #[test]
    fn ec_roundtrip_8_4_survives_up_to_4_losses() {
        let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let shards = encode(&payload, cfg()).unwrap();
        assert_eq!(shards.len(), 12);

        // 丢任意 ≤4 片可重建（300_000 可被 8 整除，无补零）
        for lost in [[0usize, 3, 7, 11], [1, 2, 9, 10]] {
            let mut view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
            for &i in &lost {
                view[i] = None;
            }
            assert_eq!(reconstruct(&view, cfg()).unwrap(), payload, "丢 {lost:?}");
        }

        // 丢 5 片必败
        let mut view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        for i in [0, 1, 2, 3, 4] {
            view[i] = None;
        }
        assert!(reconstruct(&view, cfg()).is_err());
    }

    #[test]
    fn padded_last_shard_roundtrips_and_caller_truncates() {
        // 长度非 8 整除：末片补零，重建返回补齐长度，调用方按对象长度截断
        let payload: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let shards = encode(&payload, cfg()).unwrap();
        let mut view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        view[5] = None;
        let rebuilt = reconstruct(&view, cfg()).unwrap();
        assert_eq!(&rebuilt[..payload.len()], &payload[..]);
        assert!(rebuilt.len() >= payload.len());
    }

    #[test]
    fn flipped_shard_byte_is_detected_by_crc32c_and_verify() {
        let cfg = cfg();
        let payload = vec![7u8; 1024];
        let mut shards = encode(&payload, cfg).unwrap();
        let crc_before = crc32c_of(&shards[2]);
        // 单层一：CRC32C 对单片（与落盘存值比对即为检测手段）
        shards[2][0] ^= 0x01;
        assert_ne!(
            crc32c_of(&shards[2]),
            crc_before,
            "翻转字节必须被 CRC32C 抓到"
        );
        // 单层二：跨片奇偶一致性也能抓（数据片被翻，parity 对不上）
        assert!(!shards_consistent(&shards, cfg).unwrap());
        // 未翻转时一致
        let clean = encode(&payload, cfg).unwrap();
        assert!(shards_consistent(&clean, cfg).unwrap());
    }

    #[test]
    fn invalid_config_is_rejected() {
        let payload = vec![0u8; 16];
        // data + parity > 255（GF(2^8) 上限）→ codec 构造失败
        assert!(
            encode(
                &payload,
                EcConfig {
                    data: 200,
                    parity: 100,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }

    // ——— M4 切片 2：分块编/解码 ———

    /// 测试助手：按「段长 ss=块数×W」把 payload 切成 12 片整片（与磁盘布局同构）。
    /// 走 ChunkWriter 逐块编（块 t 的输入 = 各数据片段内 [t·W,(t+1)·W)），
    /// 返回 (12 片, ss, 各片累计 CRC)。
    fn to_shards_chunked(payload: &[u8], cfg: EcConfig) -> (Vec<Vec<u8>>, u64, Vec<u32>) {
        let data = cfg.data as usize;
        let blocks = payload.len().div_ceil(data * CHUNK_W).max(1) as u64;
        let ss = blocks as usize * CHUNK_W;
        let mut w = ChunkWriter::new(cfg).unwrap();
        let mut shards: Vec<Vec<u8>> = vec![Vec::new(); (cfg.data + cfg.parity) as usize];
        for t in 0..blocks as usize {
            let mut ins: Vec<Vec<u8>> = Vec::with_capacity(data);
            for i in 0..data {
                let start = i * ss + t * CHUNK_W;
                let end = (start + CHUNK_W).min(payload.len());
                let mut c = if start < payload.len() {
                    payload[start..end].to_vec()
                } else {
                    Vec::new()
                };
                c.resize(CHUNK_W, 0);
                ins.push(c);
            }
            for (i, b) in w.encode_block(&ins).unwrap().into_iter().enumerate() {
                shards[i].extend_from_slice(&b);
            }
        }
        let crcs: Vec<u32> = (0..shards.len()).map(|i| w.shard_crc(i)).collect();
        (shards, ss as u64, crcs)
    }

    /// 分块编的产物必须与整份语义互操作：旧 `reconstruct` 直读拼段 → 原 payload；
    /// 且块累计 CRC 与整片重算一致（meta 里的 ShardRef.crc32c 口径）。
    #[test]
    fn chunked_shards_interop_with_whole_path() {
        let cfg = cfg();
        for len in [
            1usize,
            8 * CHUNK_W - 1,
            8 * CHUNK_W,
            8 * CHUNK_W + 1,
            300_000,
        ] {
            let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let (shards, ss, crcs) = to_shards_chunked(&payload, cfg);
            assert!(shards.iter().all(|s| s.len() as u64 == ss), "len={len}");
            for (i, s) in shards.iter().enumerate() {
                assert_eq!(
                    crcs[i],
                    crc32c_of(s),
                    "片 {i} 增量 CRC ≠ 整片重算（len={len}）"
                );
            }
            // 旧路径（整份 reconstruct）直读
            let view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
            let rebuilt = reconstruct(&view, cfg).unwrap();
            assert_eq!(&rebuilt[..len], &payload[..], "len={len}");
        }
    }

    /// 分块读端：逐块解码拼回 = 原 payload；丢 ≤parity 块可重建；坏片必须 fail-closed。
    #[test]
    fn chunk_reader_roundtrip_loss_and_corruption() {
        let cfg = cfg();
        let len = 8 * CHUNK_W + 12_345; // 两块，末块带补零
        let payload: Vec<u8> = (0..len).map(|i| (i % 199) as u8).collect();
        let (shards, ss, _crcs) = to_shards_chunked(&payload, cfg);
        let blocks = (ss / CHUNK_W as u64) as usize;

        // 无损坏：逐块解码拼段 → 与 payload 相等
        let mut r = ChunkReader::new(cfg).unwrap();
        let mut out = vec![0u8; ss as usize * cfg.data as usize];
        for t in 0..blocks {
            let mut chunks: Vec<Option<Vec<u8>>> = (0..(cfg.data + cfg.parity) as usize)
                .map(|i| Some(shards[i][t * CHUNK_W..(t + 1) * CHUNK_W].to_vec()))
                .collect();
            assert!(
                r.decode_block(&mut chunks).unwrap().is_none(),
                "无损坏不应定位出坏片"
            );
            for i in 0..cfg.data as usize {
                out[i * ss as usize + t * CHUNK_W..i * ss as usize + (t + 1) * CHUNK_W]
                    .copy_from_slice(chunks[i].as_ref().unwrap());
            }
        }
        assert_eq!(&out[..len], &payload[..]);

        // 丢 4 片（=parity）任一块：重建后取数据段仍等于原片
        let mut r = ChunkReader::new(cfg).unwrap();
        let orig: Vec<Vec<u8>> = (0..12)
            .map(|i| shards[i][CHUNK_W..2 * CHUNK_W].to_vec())
            .collect();
        let mut chunks: Vec<Option<Vec<u8>>> = orig.iter().cloned().map(Some).collect();
        for i in [0, 3, 7, 11] {
            chunks[i] = None;
        }
        r.decode_block(&mut chunks).unwrap();
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.as_ref().unwrap(), &orig[i], "片 {i} 重建不符");
        }

        // 在场但坏掉一字节：留一定位 → 剔除坏片 + parity 重建（EC 自愈，等价旧整片路径的
        // CRC 剔除）；分块路径没有整片 CRC，对偶校验+定位是唯一闸
        let mut r = ChunkReader::new(cfg).unwrap();
        let mut chunks: Vec<Option<Vec<u8>>> = orig.iter().cloned().map(Some).collect();
        chunks[2].as_mut().unwrap()[0] ^= 0x01;
        assert_eq!(
            r.decode_block(&mut chunks).unwrap(),
            Some(2),
            "坏片 2 应被定位（供读时修）"
        );
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.as_ref().unwrap(), &orig[i], "坏片 {i} 应被定位并重建");
        }

        // 两片同时坏：无法唯一定位 → fail-closed
        let mut r = ChunkReader::new(cfg).unwrap();
        let mut chunks: Vec<Option<Vec<u8>>> = orig.iter().cloned().map(Some).collect();
        chunks[2].as_mut().unwrap()[0] ^= 0x01;
        chunks[5].as_mut().unwrap()[0] ^= 0x01;
        let e = r.decode_block(&mut chunks).unwrap_err();
        assert!(format!("{e}").contains("无法定位"), "{e}");

        // 存活 7 < data 8 → 拒绝
        let mut r = ChunkReader::new(cfg).unwrap();
        let mut chunks: Vec<Option<Vec<u8>>> = orig.iter().cloned().map(Some).collect();
        for i in [0, 1, 2, 3, 4] {
            chunks[i] = None;
        }
        assert!(r.decode_block(&mut chunks).is_err());
    }
}
