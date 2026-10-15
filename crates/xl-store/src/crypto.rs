//! M5 切片 2：加密数据面——块对齐信封加密（DARE 式）。
//!
//! 明文流按 `PLAIN_UNIT = CHUNK_W − TAG_LEN = 65520B` 分单元；单元 u 独立 AES-256-GCM
//! （nonce = 8B 随机 seed ‖ BE32(u)；AAD = `b"oss-m5|" ‖ key_id`）→ **密文单元恒 65536B**
//! （= [`crate::store::ec::CHUNK_W`]），末单元短（明文尾 + tag）。于是「（EC 片 s, 块 t）↔ 单元 u」
//! 成双射：`u = s·blocks_ec + t`（∵ `ss = blocks_ec·W`）——tag 永不跨块、每单元加解密恰一次。
//!
//! 几何换算单源（写读共用，全部经 [`Geometry`]）；上限 `P ≤ 65520·2³²`（≈281.4 TB）读写两侧
//! 一并 fail-closed（nonce 取 BE32(u) 的不变量在读侧同样强制）。方案标识写入 meta 的
//! [`crate::meta::EncryptionRecord::algorithm`]——格式一回写即锁死，演进靠新标识、读侧白名单。
//!
//! 本模块只做「单元级 AEAD + 几何 + provider 组合」；落盘/读盘的管线分别在 `store/put.rs`、
//! `store/read.rs`（子设计 D1-D12，计划 `2026-10-16-m5-security-lock.md`）。

use std::sync::Arc;

use aes_gcm::aead::{Aead as _, OsRng, Payload};
use aes_gcm::{Aes256Gcm, KeyInit as _, Nonce};

use crate::error::{Error, Result};
use crate::kms::{self, FileKeyProvider, KEY_LEN, KeyProvider, KmsFileProvider};
use crate::meta::EncryptionRecord;

/// 密文单元宽 = EC 分片块宽（对齐全等；下方 `unit_width_matches_ec_block` 测试钉死）。
pub const UNIT_W: usize = crate::store::ec::CHUNK_W;
/// GCM tag 长度。
pub const TAG_LEN: usize = 16;
/// 每单元明文长度（末单元可短）。
pub const PLAIN_UNIT: usize = UNIT_W - TAG_LEN; // 65520
/// 方案标识（meta `algorithm` 字段唯一值；自描述几何，演进靠新标识）。
pub const ALGO: &str = "AES256-GCM-DARE-65520-v1";
/// 单元 AAD 前缀（`前缀 ‖ key_id`）。
const AAD_PREFIX: &[u8] = b"oss-m5|";
/// 明文上限：`PLAIN_UNIT·2³²` ≈ 281.4 TB（u 以 BE32 进 nonce，超限即不可安全寻址）。
pub const MAX_PLAINTEXT: u64 = PLAIN_UNIT as u64 * (1u64 << 32);
/// `MAX_INLINE` 明文对应的密文长（meta 解码对加密 inline 的上限；≈131120）。
pub const MAX_INLINE_CIPHERTEXT: u64 = ciphertext_len_unchecked(crate::meta::MAX_INLINE);

/// 无上限校验的长度换算（const 可用；写入/读取路径请走 [`Geometry::new`]）。
pub const fn ciphertext_len_unchecked(plain_len: u64) -> u64 {
    let unit = PLAIN_UNIT as u64;
    let units = plain_len.div_ceil(unit);
    if units == 0 {
        0
    } else {
        (units - 1) * UNIT_W as u64 + (plain_len - (units - 1) * unit) + TAG_LEN as u64
    }
}

/// 对象几何（写读共用的单源换算；构造即完成上限校验）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    /// 明文长度 P（meta `size`）。
    pub plain_len: u64,
    /// 单元数 nb = ceil(P/65520)；P=0 ⇒ 0。
    pub units: u64,
    /// 落盘密文流长 c_len（= P + 16·nb）；P=0 ⇒ 0。
    pub cipher_len: u64,
}

impl Geometry {
    pub fn new(plain_len: u64) -> Result<Self> {
        if plain_len > MAX_PLAINTEXT {
            return Err(Error::Crypto(format!(
                "对象超加密上限：{plain_len} > {MAX_PLAINTEXT}（≈281.4 TB）"
            )));
        }
        let units = plain_len.div_ceil(PLAIN_UNIT as u64);
        // 等价于 P + 16·nb：末单元恒 ≥17B ⇒ c_len > (nb−1)·W ⇒ data·blocks_ec ≥ nb
        let cipher_len = if units == 0 {
            0
        } else {
            (units - 1) * UNIT_W as u64
                + (plain_len - (units - 1) * PLAIN_UNIT as u64)
                + TAG_LEN as u64
        };
        Ok(Self {
            plain_len,
            units,
            cipher_len,
        })
    }

    /// 单元 u（u < units）的明文起点。
    pub fn unit_plain_start(&self, u: u64) -> u64 {
        u * PLAIN_UNIT as u64
    }

    /// 单元 u 的明文长度（末单元可短；**调用方须保证 u < units**——下方断言防脚枪）。
    pub fn unit_plain_len(&self, u: u64) -> u64 {
        debug_assert!(
            u < self.units,
            "unit_plain_len: u={u} 越界（units={}）",
            self.units
        );
        (self.plain_len - self.unit_plain_start(u)).min(PLAIN_UNIT as u64)
    }

    /// 单元 u 的密文长度（落盘态；末单元可短）。调用方解密前按此 trim（尾部是 EC 补零）。
    pub fn unit_cipher_len(&self, u: u64) -> u64 {
        self.unit_plain_len(u) + TAG_LEN as u64
    }

    /// 明文偏移 off（< P）落在的单元号。
    pub fn unit_of_plain(&self, off: u64) -> u64 {
        off / PLAIN_UNIT as u64
    }
}

/// 单元级 AEAD：固定 (DEK, key_id) 下的 seal/open（AAD 含 key_id）。
pub struct UnitCipher {
    cipher: Aes256Gcm,
    aad: Vec<u8>,
}

impl UnitCipher {
    pub fn new(dek: &[u8; KEY_LEN], key_id: &str) -> Self {
        let mut aad = Vec::with_capacity(AAD_PREFIX.len() + key_id.len());
        aad.extend_from_slice(AAD_PREFIX);
        aad.extend_from_slice(key_id.as_bytes());
        Self {
            cipher: Aes256Gcm::new(dek.into()),
            aad,
        }
    }

    fn nonce_of(seed: &[u8; 8], u: u32) -> [u8; 12] {
        let mut n = [0u8; 12];
        n[..8].copy_from_slice(seed);
        n[8..].copy_from_slice(&u.to_be_bytes());
        n
    }

    /// 加密封一个单元（明文 1..=PLAIN_UNIT）→ 密文 = 明文 + 16B tag。
    pub fn seal(&self, seed: &[u8; 8], u: u32, plain: &[u8]) -> Result<Vec<u8>> {
        if plain.is_empty() || plain.len() > PLAIN_UNIT {
            return Err(Error::Crypto(format!(
                "单元明文长度非法：{}（应 1..={PLAIN_UNIT}）",
                plain.len()
            )));
        }
        let nonce = Self::nonce_of(seed, u);
        self.cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plain,
                    aad: &self.aad,
                },
            )
            .map_err(|_| Error::Crypto(format!("单元 {u} 加密失败")))
    }

    /// 解密一个单元（`ct` 须已按 [`Geometry::unit_cipher_len`] trim）→ 明文。
    /// 篡改/错钥/错 AAD/长度不符 → `Error::Crypto`（fail-closed）。
    pub fn open(&self, seed: &[u8; 8], u: u32, ct: &[u8]) -> Result<Vec<u8>> {
        if ct.len() <= TAG_LEN || ct.len() > UNIT_W {
            return Err(Error::Crypto(format!(
                "单元 {u} 密文长度非法：{}（应 {}..={UNIT_W}）",
                ct.len(),
                TAG_LEN + 1
            )));
        }
        let nonce = Self::nonce_of(seed, u);
        self.cipher
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: ct,
                    aad: &self.aad,
                },
            )
            .map_err(|_| Error::Crypto(format!("单元 {u} 解密失败（错钥/篡改/长度不符）")))
    }
}

/// 请求侧加密模式（切片 3 扩展 SSE-C 变体与请求参数）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncSpec {
    /// SSE-S3：文件主密钥 provider（key_id "file"）。
    SseS3,
    /// SSE-KMS（文件型实现）：多主密钥目录 provider（key_id "k<n>"）。
    SseKms,
}

/// 密钥 provider 组合（切片 3 的双 provider 路由面提前落定）：
/// wrap 按 spec 选路；unwrap 按记录里的 key_id 路由（"file" → 文件、前缀 "k" → KMS 目录）。
pub struct EncState {
    pub file: Option<Arc<FileKeyProvider>>,
    pub kms: Option<Arc<KmsFileProvider>>,
}

impl EncState {
    pub fn provider_for_spec(&self, spec: EncSpec) -> Result<&dyn KeyProvider> {
        match spec {
            EncSpec::SseS3 => self
                .file
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| Error::Crypto("SSE-S3 未配置文件主密钥 provider".into())),
            EncSpec::SseKms => self
                .kms
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| Error::Crypto("SSE-KMS 未配置 KMS 目录 provider".into())),
        }
    }

    pub fn provider_for_key_id(&self, key_id: &str) -> Result<&dyn KeyProvider> {
        if key_id == "file" {
            self.file
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| {
                    Error::Crypto("记录 key_id=\"file\" 但未配置文件主密钥 provider".into())
                })
        } else if key_id.starts_with('k') {
            self.kms
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| {
                    Error::Crypto(format!("记录 key_id={key_id:?} 但未配置 KMS 目录 provider"))
                })
        } else {
            Err(Error::Crypto(format!("key_id 无法路由：{key_id:?}")))
        }
    }
}

/// 单元处理上下文（写读共用）：cipher + seed + key_id。
pub struct UnitCtx {
    pub cipher: UnitCipher,
    pub seed: [u8; 8],
    pub key_id: String,
}

fn new_seed() -> [u8; 8] {
    use aes_gcm::aead::rand_core::RngCore as _;
    let mut s = [0u8; 8];
    OsRng.fill_bytes(&mut s);
    s
}

/// 整段加密（inline 专用）：按 D2 单元切分（P=0 ⇒ C 空）→ C = c_len(P)。
pub fn seal_all(ctx: &UnitCtx, plain: &[u8]) -> Result<Vec<u8>> {
    let geo = Geometry::new(plain.len() as u64)?;
    let mut out = Vec::with_capacity(geo.cipher_len as usize);
    for u in 0..geo.units {
        let s = geo.unit_plain_start(u) as usize;
        let e = s + geo.unit_plain_len(u) as usize;
        out.extend_from_slice(&ctx.cipher.seal(&ctx.seed, u as u32, &plain[s..e])?);
    }
    Ok(out)
}

/// 整段解密（inline / 整读重建缓冲）：先验密文长 == c_len(plain_len)，再逐单元解
/// （每单元 trim 到 unit_cipher_len——缓冲尾部可能是 EC 条带补零）。
pub fn open_all(ctx: &UnitCtx, plain_len: u64, cipher: &[u8]) -> Result<Vec<u8>> {
    let geo = Geometry::new(plain_len)?;
    if cipher.len() as u64 != geo.cipher_len {
        return Err(Error::Crypto(format!(
            "密文长度不符：{} ≠ c_len({plain_len})={}",
            cipher.len(),
            geo.cipher_len
        )));
    }
    let mut out = Vec::with_capacity(plain_len as usize);
    for u in 0..geo.units {
        let s = (u * UNIT_W as u64) as usize;
        let l = geo.unit_cipher_len(u) as usize;
        out.extend_from_slice(&ctx.cipher.open(&ctx.seed, u as u32, &cipher[s..s + l])?);
    }
    Ok(out)
}

fn hex_fixed<const N: usize>(s: &str, what: &str) -> Result<[u8; N]> {
    let v = hex::decode(s).map_err(|e| Error::CorruptMeta(format!("{what} 非 hex: {e}")))?;
    v.try_into()
        .map_err(|v: Vec<u8>| Error::CorruptMeta(format!("{what} 长度非法：{}（应 {N}）", v.len())))
}

impl UnitCtx {
    /// 写侧：按 spec 选 provider → 新 DEK wrap 一次 → 新 seed → （上下文, meta 记录）。
    pub async fn for_write(spec: EncSpec, state: &EncState) -> Result<(Self, EncryptionRecord)> {
        let provider = state.provider_for_spec(spec)?;
        let dek = kms::new_dek();
        let (wrapped, key_id) = provider.wrap_dek(&dek).await?;
        let seed = new_seed();
        let rec = EncryptionRecord {
            algorithm: ALGO.to_string(),
            key_id: key_id.clone(),
            wrapped_dek_hex: hex::encode(&wrapped),
            nonce_seed_hex: hex::encode(seed),
        };
        Ok((
            Self {
                cipher: UnitCipher::new(&dek, &key_id),
                seed,
                key_id,
            },
            rec,
        ))
    }

    /// 读侧：校验方案白名单 → 按 key_id 路由 provider → unwrap DEK → 上下文。
    /// 未知 algorithm / 空 wrapped（SSE-C 预留形态）/ unwrap 失败 → fail-closed。
    pub async fn for_read(rec: &EncryptionRecord, state: &EncState) -> Result<Self> {
        if rec.algorithm != ALGO {
            return Err(Error::CorruptMeta(format!(
                "未知加密方案：{:?}（本版仅 {:?}）",
                rec.algorithm, ALGO
            )));
        }
        let seed = hex_fixed::<8>(&rec.nonce_seed_hex, "nonce_seed")?;
        if rec.wrapped_dek_hex.is_empty() {
            return Err(Error::Crypto(
                "记录为 SSE-C 形态（wrapped 为空），本版不可解（切片 3 接线）".into(),
            ));
        }
        let wrapped = hex::decode(&rec.wrapped_dek_hex)
            .map_err(|e| Error::Crypto(format!("wrapped_dek 非 hex: {e}")))?;
        let provider = state.provider_for_key_id(&rec.key_id)?;
        let dek = provider.unwrap_dek(&wrapped, &rec.key_id).await?;
        Ok(Self {
            cipher: UnitCipher::new(&dek, &rec.key_id),
            seed,
            key_id: rec.key_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_width_matches_ec_block() {
        // 对齐不变量：密文单元宽恒等于 EC 分片块宽（D1 双射的根基）
        assert_eq!(UNIT_W, crate::store::ec::CHUNK_W);
        assert_eq!(PLAIN_UNIT, UNIT_W - TAG_LEN);
    }

    #[test]
    fn geometry_boundaries() {
        // P=0：零单元零密文（B1 短路，不得下溢）
        let g = Geometry::new(0).unwrap();
        assert_eq!((g.units, g.cipher_len), (0, 0));

        for (p, units, clen) in [
            (1u64, 1u64, 17u64),
            (PLAIN_UNIT as u64 - 1, 1, PLAIN_UNIT as u64 - 1 + 16),
            (PLAIN_UNIT as u64, 1, UNIT_W as u64), // 恰好整单元：65536
            (PLAIN_UNIT as u64 + 1, 2, UNIT_W as u64 + 17),
            (2 * PLAIN_UNIT as u64, 2, 2 * UNIT_W as u64),
            (2 * PLAIN_UNIT as u64 + 1, 3, 2 * UNIT_W as u64 + 17),
            (3 * PLAIN_UNIT as u64, 3, 3 * UNIT_W as u64),
        ] {
            let g = Geometry::new(p).unwrap();
            assert_eq!((g.units, g.cipher_len), (units, clen), "P={p}");
            // 末单元 = 剩余明文 + tag；其余单元满
            assert_eq!(
                g.unit_cipher_len(units - 1),
                p - (units - 1) * PLAIN_UNIT as u64 + 16
            );
            if units > 1 {
                assert_eq!(g.unit_cipher_len(0), UNIT_W as u64);
            }
        }

        // c_len = P + 16·nb（等价式）
        for p in [
            1u64, 65519, 65520, 65521, 131040, 131041, 300_000, 1_000_000,
        ] {
            let g = Geometry::new(p).unwrap();
            assert_eq!(g.cipher_len, p + 16 * g.units, "P={p}");
        }

        // 上限：恰好在界内/界外（写读共用 fail-closed）
        assert!(Geometry::new(MAX_PLAINTEXT).is_ok());
        assert!(matches!(
            Geometry::new(MAX_PLAINTEXT + 1),
            Err(Error::Crypto(_))
        ));
    }

    #[test]
    fn unit_span_helpers() {
        let g = Geometry::new(2 * PLAIN_UNIT as u64 + 10).unwrap();
        assert_eq!(g.unit_of_plain(0), 0);
        assert_eq!(g.unit_of_plain(PLAIN_UNIT as u64 - 1), 0);
        assert_eq!(g.unit_of_plain(PLAIN_UNIT as u64), 1);
        assert_eq!(g.unit_plain_start(1), PLAIN_UNIT as u64);
        assert_eq!(g.unit_plain_len(0), PLAIN_UNIT as u64);
        assert_eq!(g.unit_plain_len(2), 10);
        assert_eq!(g.unit_cipher_len(2), 26);
    }

    #[test]
    fn seal_open_roundtrip_and_nonce_discipline() {
        let dek = [7u8; KEY_LEN];
        let c = UnitCipher::new(&dek, "file");
        let seed = [3u8; 8];
        let plain: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let ct = c.seal(&seed, 0, &plain).unwrap();
        assert_eq!(ct.len(), plain.len() + TAG_LEN);
        assert_eq!(c.open(&seed, 0, &ct).unwrap(), plain);

        // 同明文同 seed 不同单元号 → 密文不同（nonce 位置相关）
        let ct2 = c.seal(&seed, 1, &plain).unwrap();
        assert_ne!(ct, ct2);
        // 同明文同单元不同 seed → 密文不同
        let ct3 = c.seal(&[9u8; 8], 0, &plain).unwrap();
        assert_ne!(ct, ct3);
        // 单元号错位解密 → 拒绝
        assert!(matches!(c.open(&seed, 1, &ct), Err(Error::Crypto(_))));
        // 篡改 1 字节 → 拒绝
        let mut bad = ct.clone();
        bad[0] ^= 1;
        assert!(matches!(c.open(&seed, 0, &bad), Err(Error::Crypto(_))));
        // 错 key → 拒绝
        let c2 = UnitCipher::new(&[8u8; KEY_LEN], "file");
        assert!(matches!(c2.open(&seed, 0, &ct), Err(Error::Crypto(_))));
        // AAD 绑定：key_id 不同 → 解密拒绝（同 DEK 同 seed 同单元）
        let c3 = UnitCipher::new(&dek, "k0");
        assert!(matches!(c3.open(&seed, 0, &ct), Err(Error::Crypto(_))));
        // 长度边界：空明文拒封、超宽拒封、短密文拒解
        assert!(c.seal(&seed, 0, &[]).is_err());
        assert!(c.seal(&seed, 0, &vec![0u8; PLAIN_UNIT + 1]).is_err());
        assert!(c.open(&seed, 0, &[0u8; TAG_LEN]).is_err());
    }

    #[test]
    fn read_path_validation_is_fail_closed() {
        // 非法 hex / 长度 → CorruptMeta；空 wrapped → Crypto（SSE-C 预留形态）
        let mk = |algo: &str, kid: &str, w: &str, s: &str| EncryptionRecord {
            algorithm: algo.into(),
            key_id: kid.into(),
            wrapped_dek_hex: w.into(),
            nonce_seed_hex: s.into(),
        };
        let f = |rec: EncryptionRecord| {
            futures::executor::block_on(UnitCtx::for_read(
                &rec,
                &EncState {
                    file: None,
                    kms: None,
                },
            ))
        };
        assert!(matches!(
            f(mk("AES-OTHER", "file", "", "00")),
            Err(Error::CorruptMeta(_))
        ));
        let good_seed = hex::encode([1u8; 8]);
        assert!(matches!(
            f(mk(ALGO, "file", "", &good_seed)),
            Err(Error::Crypto(_))
        ));
        assert!(matches!(
            f(mk(ALGO, "file", "zz", &good_seed)),
            Err(Error::Crypto(_))
        ));
        assert!(matches!(
            f(mk(ALGO, "file", "aabb", "nothex")),
            Err(Error::CorruptMeta(_))
        ));
        // 合法形态但 provider 未配置 → Crypto（路由面 fail-closed）
        let wrapped60 = hex::encode([0u8; 60]);
        assert!(matches!(
            f(mk(ALGO, "file", &wrapped60, &good_seed)),
            Err(Error::Crypto(_))
        ));
    }
}
