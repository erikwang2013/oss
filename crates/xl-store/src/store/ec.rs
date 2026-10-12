//! EC 纠删码编解码与分片校验（galois_8 / 8+4 默认）。
//!
//! ponytail: M2b 先整份进内存（写：payload + 12 片；读：重建整份）；rs-erasure 提供
//! `ShardByShard` 增量编码——大对象分块编/解与 range 按需重建是 M2b-3 之后的升级路径。
//!
//! 校验分两层：`crc32c_of` 对单片（与 meta 里存的 ShardRef.crc32c 比对，坏片视同缺失），
//! `shards_consistent` 用 rs 的 `verify` 做跨片奇偶一致性（抓 CRC 之后仍可能混入的错配）。

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

/// 重建：缺失片（`None` 或长度不符）以 EC 补齐，返回**补齐后的**数据段（前 data 片拼接）。
/// 尾部补零的截断由调用方按 meta 里的对象长度完成（EC 层不知道原始长度）。
pub fn reconstruct(shards: &[Option<Vec<u8>>], cfg: EcConfig) -> Result<Vec<u8>> {
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

    // 重建会同时修回 parity 片；数据段 = 前 data 片拼接
    let mut out = Vec::with_capacity(size * cfg.data as usize);
    for (v, _) in buf.iter().take(cfg.data as usize) {
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
}
