//! xl.meta v1 —— 每个对象目录一份，是**唯一的提交点**。
//!
//! 编码：`[1 字节格式版本][msgpack 载荷]`。版本字节是前向兼容的锚点：
//! 后续阶段（版本控制/加密/对象锁/复制/对象标签）只往 `Version` 里加 `Option` 字段，
//! 老解码器靠 serde 的默认「跳过未知字段」行为继续可读。
//! 非 Option 字段必须带 `#[serde(default)]`——新解码器读旧文件时，缺 default 会硬报
//! `missing field`（M4 的 is_latest/is_delete_marker 等要照此办）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const FORMAT_VERSION: u8 = 1;
/// xl.meta 文件名。
pub const META_FILE: &str = "xl.meta";
/// 覆盖写时的落盘中转名。
pub const META_NEW_FILE: &str = "xl.meta.new";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XlMeta {
    pub object_key: String,
    /// M1 恒为一条。M4 起多条（版本控制），解码器与读路径从第一天就按 Vec 处理。
    pub versions: Vec<Version>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Version {
    /// M1 无版本控制：`Uuid::nil()`（S3 协议层映射为字面量 `"null"`）；M4 起为每版本真实 uuid。
    pub version_id: uuid::Uuid,
    /// `>= 0` 是写入端不变量（`now_nanos` 钳 0）；下游转 SystemTime 前必须钳制
    /// （负数会回绕成天文数字）。
    pub mod_time_nanos: i64,
    pub size: u64,
    /// 单段 PUT = 整份 MD5（S3 的定义）；aetherupload 上传时白拿。
    pub etag: String,
    pub content_type: Option<String>,
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    pub part: PartRef,
    /// Multipart：`Some(parts)` 时以本列表为准（按 part 号升序），`part` 字段退为兼容位（首段副本）；
    /// `None` = M1 单段语义（读 `part`）。只加 `Option` 字段 = 前向兼容契约（见文件头注释）。
    #[serde(default)]
    pub parts: Option<Vec<PartRef>>,
    /// 客户端声明的校验和（Content-MD5 / `x-amz-checksum-*`；服务端在提交点前已验证）。
    /// multipart 的组合校验和语义留 M2b。
    #[serde(default)]
    pub checksum: Option<ChecksumRecord>,
    /// M2b：Some = EC 布局（内容以分片为准）；None = M1/M2a 旧形态（读 part/parts）。
    #[serde(default)]
    pub data_layout: Option<DataLayout>,
    /// M2b：小对象内联（读取端按 `MAX_INLINE` 复核）。
    #[serde(default)]
    pub inline: Option<InlineData>,
    /// M4 版本控制：delete marker（标记版本无数据；GET 最新见标记 → 404 带 marker 语义）。
    /// 文件头契约：只加 `Option` 字段，老解码器跳过未知字段、新解码器靠 `serde(default)` 读旧文件。
    #[serde(default)]
    pub is_delete_marker: Option<bool>,
    /// M4 版本控制：是否当前最新（写端每次重写 meta 时全链重算；**读判以链尾为准**——
    /// 该字段是给 S3 协议响应/未来集群合并用的冗余信息，不参与内部读逻辑）。
    #[serde(default)]
    pub is_latest: Option<bool>,
    /// M4 切片 4：单调写序号（NTP 回拨收口）。写端取旧链 `max_epoch()+1`；副本择新与集群
    /// 读按 `(epoch, mod_time)` 排秩——墙钟回拨时 epoch 仍单调，不再有「旧副本凭大钟胜出」。
    /// 老 meta 无此字段 → None（按 0 参与排秩）。
    #[serde(default)]
    pub epoch: Option<u64>,
    /// M5 切片 2：数据面加密记录（Some = 对象数据为块对齐 AEAD 密文；见 `crypto` 模块）。
    /// 前向兼容契约：只加 Option 字段（老解码器跳过未知字段）。
    #[serde(default)]
    pub encryption: Option<EncryptionRecord>,
    /// M5 切片 4：对象锁（retention 与 legal hold 独立；见 [`ObjectLockRecord`]）。
    #[serde(default)]
    pub lock: Option<ObjectLockRecord>,
    /// M6 切片 3：桶复制状态（PENDING/COMPLETED/FAILED/REPLICA；None = 非复制对象）。
    /// decode 白名单校验（信任边界）；老解码器跳过未知字段 = 前向兼容。
    #[serde(default)]
    pub replication_status: Option<String>,
    /// M8 前置：对象标签（键值对，按客户端顺序；None/空 = 无标签）。
    /// 只加 Option 字段 = 前向兼容契约；decode 校验长度/字符集/重复键（信任边界）。
    #[serde(default)]
    pub tags: Option<Vec<ObjectTag>>,
}

/// 对象标签（S3 Tag）。键唯一、最多 [`MAX_TAGS`] 条。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectTag {
    pub key: String,
    pub value: String,
}

/// S3 允许的标签上限（每对象）。
pub const MAX_TAGS: usize = 10;
/// 标签键的 Unicode 标量上限。
pub const MAX_TAG_KEY_CHARS: usize = 128;
/// 标签值的 Unicode 标量上限。
pub const MAX_TAG_VALUE_CHARS: usize = 256;

/// 校验标签集合（S3 口径）：≤10 条、键 1..=128、值 0..=256（Unicode 标量）、键唯一、
/// 仅 Unicode 字母/数字 + 空格 + `+ - = . _ : / @`（控制字符/换行天然排除）。
pub fn validate_tags(tags: &[ObjectTag]) -> std::result::Result<(), String> {
    if tags.len() > MAX_TAGS {
        return Err(format!("标签数量超过上限 {}", MAX_TAGS));
    }
    let mut seen = std::collections::HashSet::with_capacity(tags.len());
    for t in tags {
        if t.key.is_empty() {
            return Err("标签键不能为空".into());
        }
        if t.key.chars().count() > MAX_TAG_KEY_CHARS {
            return Err(format!("标签键超过 {} 字符", MAX_TAG_KEY_CHARS));
        }
        if t.value.chars().count() > MAX_TAG_VALUE_CHARS {
            return Err(format!("标签值超过 {} 字符", MAX_TAG_VALUE_CHARS));
        }
        let ok = |s: &str| {
            s.chars().all(|c| {
                c.is_alphanumeric()
                    || c == ' '
                    || matches!(c, '+' | '-' | '=' | '.' | '_' | ':' | '/' | '@')
            })
        };
        if !ok(&t.key) {
            return Err(format!("标签键含非法字符：{:?}", t.key));
        }
        if !ok(&t.value) {
            return Err(format!("标签值含非法字符：{:?}", t.value));
        }
        if !seen.insert(t.key.as_str()) {
            return Err(format!("标签键重复：{:?}", t.key));
        }
    }
    Ok(())
}

impl Version {
    /// 统一的 parts 视图：单段 → `vec![part]`，多段 → 原列表（克隆）。
    pub fn effective_parts(&self) -> Vec<PartRef> {
        match &self.parts {
            Some(parts) => parts.clone(),
            None => vec![self.part.clone()],
        }
    }

    /// 是否 delete marker（版本控制）。
    pub fn delete_marker(&self) -> bool {
        self.is_delete_marker == Some(true)
    }

    /// S3 线格式版本号：nil（无版本控制的"null 版本"）→ 字面量 `"null"`。
    pub fn version_id_str(&self) -> String {
        if self.version_id.is_nil() {
            "null".to_string()
        } else {
            self.version_id.to_string()
        }
    }
}

/// 内联字节上限（解码信任边界：防手工构造/损坏的 meta 触发超大分配）。
pub const MAX_INLINE: u64 = 128 * 1024;

/// 客户端声明的校验和记录（落盘仅作数据；**校验发生在提交点之前**，由 xl-s3 层完成）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChecksumRecord {
    /// s3s 的算法名（`"CRC32C"` / `"SHA256"` / `"MD5"`（Content-MD5）等）
    pub algorithm: String,
    /// base64 编码的摘要（S3 线格式）
    pub value_base64: String,
}

/// M2b：EC 布局（`Version.data_layout` 为 Some 时，本版本内容以分片为准）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataLayout {
    pub data: u8,
    pub parity: u8,
    /// 每片定长（含末片补零对齐）
    pub shard_size: u64,
    /// 分片引用表（按 shard 下标排列；只存拓扑下标，绝不存绝对路径——铁律 ⑤）
    pub shards: Vec<ShardRef>,
}

/// 单片引用：位置 = （盘下标, part 号, 分片下标）三元组。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardRef {
    /// 拓扑盘下标（单盘恒 0）
    pub drive: u8,
    /// M2b 恒 1（与 parts 的多 part 升级位对齐）
    pub part: u16,
    /// 0..data+parity
    pub shard: u8,
    pub size: u64,
    /// 单片 CRC32C（读路径坏片检测）
    pub crc32c: u32,
}

/// M2b：小对象内联（< inline_threshold 时数据就在 meta 里，不经 EC）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InlineData {
    pub bytes: Vec<u8>,
}

/// M5 切片 2：对象数据加密记录（meta 不含秘密：wrapped DEK 是 GCM 包裹的密文，可公开）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EncryptionRecord {
    /// 方案标识（白名单唯一值；自描述几何——格式一回写即锁死，演进靠新标识）。
    pub algorithm: String,
    /// 主密钥 id（`"file"` = 单钥文件；`"k<n>"` = KMS 目录；SSE-C 预留）。
    pub key_id: String,
    /// 信封 wrap 的 DEK（60B hex = nonce‖ct‖tag）；SSE-C 恒空串（钥在客户端，绝不落盘）。
    pub wrapped_dek_hex: String,
    /// 每存储流 8B 随机 nonce seed（hex 16 字符）；单元 u 的 nonce = seed ‖ BE32(u)。
    pub nonce_seed_hex: String,
    /// M5 切片 3（SSE-C）：base64(MD5(客户钥))——AWS 同款 key-MD5 校验值（24 字符）。
    /// `key_id="sse-c"` 时必在、其余恒无（decode 强制三态一致，见下方校验）。
    #[serde(default)]
    pub sse_c_key_md5: Option<String>,
}

/// M5 切片 4：对象锁记录（retention 与 legal hold **相互独立**；meta 无秘密）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectLockRecord {
    /// `"GOVERNANCE"` / `"COMPLIANCE"`；与 `retain_until_nanos` **成对**（无 retention 则两者皆 None）。
    #[serde(default)]
    pub mode: Option<String>,
    /// 保留截止（unix 纳秒；`直到该时刻为止禁删`——`until <= now` 即可删）。
    #[serde(default)]
    pub retain_until_nanos: Option<i64>,
    /// Legal hold：独立于 retention，开启期间禁删（含 admin）；关闭需显式调用。
    #[serde(default)]
    pub legal_hold: bool,
}

impl ObjectLockRecord {
    /// 是否为「空记录」（无 retention 无 hold）——空则不落字段。
    pub fn is_empty(&self) -> bool {
        self.mode.is_none() && self.retain_until_nanos.is_none() && !self.legal_hold
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PartRef {
    /// 版本目录名（`<对象目录>/<uuid>/`）。
    pub uuid: String,
    pub part: u16,
    pub size: u64,
    pub sha256: String,
}

impl XlMeta {
    pub fn encode(&self) -> Result<Vec<u8>> {
        // to_vec_named：struct 编成「字段名 → 值」映射而非位置数组。
        // 前向兼容靠它 —— 数组编码下未来版本多一个字段，老解码器必报长度错。
        let body = rmp_serde::to_vec_named(self).map_err(|e| Error::Encode(e.to_string()))?;
        let mut out = Vec::with_capacity(body.len() + 1);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let m: Self = match bytes.split_first() {
            Some((&FORMAT_VERSION, rest)) => {
                rmp_serde::from_slice(rest).map_err(|e| Error::CorruptMeta(e.to_string()))?
            }
            Some((&v, _)) => return Err(Error::CorruptMeta(format!("不支持的格式版本 {v}"))),
            None => return Err(Error::CorruptMeta("空文件".into())),
        };
        // part.uuid 是版本目录名，读路径会把它拼进盘上路径：含 / \ .. 空或控制字符的 uuid
        // 能把读路径指到对象目录之外，解码时（唯一的信任边界）一律拒绝。
        // 宽松白名单：不按 UUID 格式收严，非 UUID 形态的历史数据仍可读。
        for v in &m.versions {
            if !part_uuid_is_safe(&v.part.uuid) {
                return Err(Error::CorruptMeta(format!(
                    "part.uuid 非法: {:?}",
                    v.part.uuid
                )));
            }
            // M6 切片 3：复制状态白名单（信任边界）
            if let Some(rs) = &v.replication_status
                && !matches!(rs.as_str(), "PENDING" | "COMPLETED" | "FAILED" | "REPLICA")
            {
                return Err(Error::CorruptMeta(format!("非法复制状态：{rs:?}")));
            }
            // M8 前置：标签集合（信任边界——长度/字符集/重复键在解码时收口）
            if let Some(tags) = &v.tags
                && let Err(e) = validate_tags(tags)
            {
                return Err(Error::CorruptMeta(format!("标签非法：{e}")));
            }
            // 内联字节是解码信任边界：超上限一律拒（防手工构造/损坏文件触发超大分配）。
            // M5 切片 2：加密 inline 的落盘态是密文（C = c_len(P)），上限随方案放宽。
            let inline_limit = if v.encryption.is_some() {
                crate::crypto::MAX_INLINE_CIPHERTEXT
            } else {
                MAX_INLINE
            };
            if let Some(inline) = &v.inline
                && inline.bytes.len() as u64 > inline_limit
            {
                return Err(Error::CorruptMeta(format!(
                    "inline 超上限：{} > {inline_limit}",
                    inline.bytes.len()
                )));
            }
            // M5 切片 2/3：加密记录白名单校验（信任边界；存量对象无此字段 = 明文，零回归）。
            // 三态一致性（切片 3）：key_id="sse-c" ⟺ wrapped 空 ⟺ sse_c_key_md5 在；其余形态 ⟺
            // wrapped 120hex ⟺ sse_c_key_md5 无（切片 2 的「空 wrapped 预留」形态在 SSE-C 定义后收编）。
            if let Some(enc) = &v.encryption {
                if enc.algorithm != crate::crypto::ALGO {
                    return Err(Error::CorruptMeta(format!(
                        "未知加密方案：{:?}",
                        enc.algorithm
                    )));
                }
                let kid_ok = !enc.key_id.is_empty()
                    && enc.key_id.len() <= 64
                    && enc.key_id.bytes().all(|b| (0x20..=0x7e).contains(&b));
                let is_ssec = enc.key_id == "sse-c";
                let wrapped_ok = if is_ssec {
                    enc.wrapped_dek_hex.is_empty()
                } else {
                    enc.wrapped_dek_hex.len() == 120
                        && enc.wrapped_dek_hex.bytes().all(|b| b.is_ascii_hexdigit())
                };
                let md5_ok = match &enc.sse_c_key_md5 {
                    Some(m) if is_ssec => {
                        m.len() == 24
                            && m.bytes().all(|b| {
                                b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'='
                            })
                    }
                    None if !is_ssec => true,
                    _ => false,
                };
                let seed_ok = enc.nonce_seed_hex.len() == 16
                    && enc.nonce_seed_hex.bytes().all(|b| b.is_ascii_hexdigit());
                if !kid_ok || !wrapped_ok || !seed_ok || !md5_ok {
                    return Err(Error::CorruptMeta(format!(
                        "加密记录字段非法：key_id={:?} wrapped_len={} seed_len={} sse_c_md5={}",
                        enc.key_id,
                        enc.wrapped_dek_hex.len(),
                        enc.nonce_seed_hex.len(),
                        if enc.sse_c_key_md5.is_some() {
                            "set"
                        } else {
                            "-"
                        },
                    )));
                }
            }
            // M5 切片 4：对象锁记录（信任边界）——mode 白名单、与 until 成对、until 非负
            if let Some(l) = &v.lock {
                let mode_ok = matches!(l.mode.as_deref(), None | Some("GOVERNANCE" | "COMPLIANCE"));
                let pair_ok = l.mode.is_some() == l.retain_until_nanos.is_some();
                let until_ok = l.retain_until_nanos.is_none_or(|t| t >= 0);
                if !mode_ok || !pair_ok || !until_ok {
                    return Err(Error::CorruptMeta(format!(
                        "对象锁记录非法：mode={:?} until={:?}",
                        l.mode, l.retain_until_nanos
                    )));
                }
            }
            // EC 布局基本自洽：分片数必须等于 data+parity（下标型字段，无路径穿越面）
            if let Some(l) = &v.data_layout
                && l.shards.len() != (l.data as usize + l.parity as usize)
            {
                return Err(Error::CorruptMeta(format!(
                    "data_layout 分片数不符：{} ≠ {} + {}",
                    l.shards.len(),
                    l.data,
                    l.parity
                )));
            }
            // checksum 会作为响应头原样回显：磁盘上的 meta 也要天然安全（防御纵深——
            // 入网侧值必须等于实算摘要才可能落盘，此为手工构造/损坏文件的兜底）。
            if let Some(c) = &v.checksum {
                let ok = |s: &str, max: usize| {
                    !s.is_empty() && s.len() <= max && s.bytes().all(|b| (0x20..=0x7e).contains(&b))
                };
                if !ok(&c.algorithm, 32) || !ok(&c.value_base64, 128) {
                    return Err(Error::CorruptMeta(format!(
                        "checksum 字段非法: algorithm={:?} value_len={}",
                        c.algorithm,
                        c.value_base64.len()
                    )));
                }
            }
            // 多段（Multipart）的 parts[].uuid 同样进读路径（dir.join(uuid)），同一套白名单收口
            if let Some(parts) = &v.parts {
                for p in parts {
                    if !part_uuid_is_safe(&p.uuid) {
                        return Err(Error::CorruptMeta(format!(
                            "parts[].uuid 非法: {:?}",
                            p.uuid
                        )));
                    }
                }
            }
        }
        Ok(m)
    }

    /// 链尾 = 最新（写端追加序不变量；`is_latest` 是重算的冗余位，不参与判定）。
    pub fn latest(&self) -> Option<&Version> {
        self.versions.last()
    }

    /// 链尾是最新版本且为 delete marker（版本控制桶的对象「已删除」态）。
    pub fn latest_is_marker(&self) -> bool {
        self.latest().is_some_and(|v| v.delete_marker())
    }

    /// 按 S3 线格式版本号找链内版本（`"null"` = nil uuid 的无版本条目）。
    pub fn find_version(&self, version_id: &str) -> Option<&Version> {
        self.versions
            .iter()
            .find(|v| v.version_id_str() == version_id)
    }

    /// 链内最大 epoch（老条目按 0）。写路径用它 +1 生成新版本 epoch。
    pub fn max_epoch(&self) -> u64 {
        self.versions
            .iter()
            .filter_map(|v| v.epoch)
            .max()
            .unwrap_or(0)
    }

    /// 元数据择新秩 `(max_epoch, 链尾 mod_time)`：多盘副本选举与集群读共用一个口径。
    /// epoch 优先（单调写序号），回拨/同钟场景再比墙钟。
    pub fn rank(&self) -> (u64, i64) {
        (
            self.max_epoch(),
            self.latest().map_or(i64::MIN, |v| v.mod_time_nanos),
        )
    }

    /// 全链重算 `is_latest`（每次 meta 改动后、落盘前调用；链尾为 true）。
    pub fn refresh_latest_flags(&mut self) {
        let n = self.versions.len();
        for (i, v) in self.versions.iter_mut().enumerate() {
            v.is_latest = Some(i + 1 == n);
        }
    }
}

/// 版本目录名的宽松白名单：非空、无路径分隔符、无 `..`、无控制字符。
fn part_uuid_is_safe(uuid: &str) -> bool {
    !uuid.is_empty()
        && !uuid.contains('/')
        && !uuid.contains('\\')
        && !uuid.contains("..")
        && !uuid.chars().any(|c| c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> XlMeta {
        XlMeta {
            object_key: "a/b.txt".into(),
            versions: vec![Version {
                version_id: uuid::Uuid::nil(),
                mod_time_nanos: 1_700_000_000_000_000_000,
                size: 11,
                etag: "5eb63bbbe01eeed093cb22bb8f5acdc3".into(),
                content_type: Some("text/plain".into()),
                user_metadata: [("k".to_string(), "v".to_string())].into_iter().collect(),
                part: PartRef {
                    uuid: "u".into(),
                    part: 1,
                    size: 11,
                    sha256: "aa".into(),
                },
                parts: None,
                checksum: None,
                data_layout: None,
                inline: None,
                is_delete_marker: None,
                is_latest: None,
                epoch: None,
                encryption: None,
                lock: None,
                replication_status: None,
                tags: None,
            }],
        }
    }

    #[test]
    fn roundtrip() {
        let m = sample();
        let bytes = m.encode().unwrap();
        let back = XlMeta::decode(&bytes).unwrap();
        assert_eq!(back.versions.len(), 1);
        assert_eq!(back.versions[0].etag, m.versions[0].etag);
        assert_eq!(
            back.versions[0].user_metadata.get("k"),
            Some(&"v".to_string())
        );
    }

    #[test]
    fn first_byte_is_format_version() {
        let bytes = sample().encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
    }

    #[test]
    fn decode_rejects_unknown_version() {
        let mut bytes = sample().encode().unwrap();
        bytes[0] = 99;
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_rejects_empty() {
        assert!(matches!(XlMeta::decode(&[]), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn latest_returns_single_version() {
        let m = sample();
        assert_eq!(m.latest().unwrap().size, 11);
    }

    #[test]
    fn decode_skips_unknown_fields() {
        // 前向兼容：v1 解码器遇到未来版本新增的字段必须跳过而不是报错
        #[derive(serde::Serialize)]
        struct Future {
            object_key: String,
            versions: Vec<Version>,
            brand_new_field: u64, // v1 不认识的字段
        }
        let bytes = rmp_serde::to_vec_named(&Future {
            object_key: "a".into(),
            versions: vec![],
            brand_new_field: 7,
        })
        .unwrap();
        let mut framed = vec![FORMAT_VERSION];
        framed.extend(bytes);
        assert!(XlMeta::decode(&framed).is_ok());
    }

    #[test]
    fn nested_unknown_fields_are_skipped() {
        // 前向兼容的承诺粒度是嵌套字段：未来版本往 Version / PartRef 里加字段，
        // v1 解码器也必须能读，不只顶层。
        #[derive(serde::Serialize)]
        struct FutureVersion {
            version_id: uuid::Uuid,
            mod_time_nanos: i64,
            size: u64,
            etag: String,
            content_type: Option<String>,
            user_metadata: BTreeMap<String, String>,
            part: PartRef,
            brand_new_v_field: u64, // v1 不认识的嵌套字段
        }
        #[derive(serde::Serialize)]
        struct FutureMeta {
            object_key: String,
            versions: Vec<FutureVersion>,
        }
        let src = sample();
        let v = &src.versions[0];
        let bytes = rmp_serde::to_vec_named(&FutureMeta {
            object_key: src.object_key.clone(),
            versions: vec![FutureVersion {
                version_id: v.version_id,
                mod_time_nanos: v.mod_time_nanos,
                size: v.size,
                etag: v.etag.clone(),
                content_type: v.content_type.clone(),
                user_metadata: v.user_metadata.clone(),
                part: v.part.clone(),
                brand_new_v_field: 7,
            }],
        })
        .unwrap();
        let mut framed = vec![FORMAT_VERSION];
        framed.extend(bytes);
        let back = XlMeta::decode(&framed).unwrap();
        assert_eq!(back.versions[0].etag, v.etag);
        assert_eq!(back.versions[0].part.sha256, "aa");
    }

    #[test]
    fn latest_is_none_for_empty_versions() {
        let m = XlMeta {
            object_key: "a".into(),
            versions: vec![],
        };
        assert!(m.latest().is_none());
    }

    #[test]
    fn truncated_payload_is_corrupt() {
        let bytes = sample().encode().unwrap();
        let cut = &bytes[..bytes.len() - 3];
        assert!(matches!(XlMeta::decode(cut), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_rejects_unsafe_part_uuid() {
        // part.uuid 是版本目录名（读路径会拼进盘上路径）：含 / \ .. 空或控制字符的 uuid
        // 能把读路径指到对象目录之外，解码时一律拒绝。宽松白名单——不按 UUID 格式收严，
        // 非 UUID 形态的历史数据（如 fixture "u"）仍必须可读。
        for bad in ["a/../b", "../x", "a/b", "a\\b", "", "a\u{0}b"] {
            let mut m = sample();
            m.versions[0].part.uuid = bad.into();
            let bytes = m.encode().unwrap();
            assert!(
                matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))),
                "{bad:?} 应当被拒"
            );
        }
        assert_eq!(
            XlMeta::decode(&sample().encode().unwrap()).unwrap(),
            sample()
        );
    }

    /// epoch 排秩（切片 4）：墙钟回拨时 epoch 单调者胜——老副本不得凭大钟翻盘。
    #[test]
    fn rank_prefers_epoch_over_wall_clock() {
        let with = |epoch: Option<u64>, t: i64| {
            let mut m = sample();
            for v in &mut m.versions {
                v.epoch = epoch;
                v.mod_time_nanos = t;
            }
            m
        };
        let new = with(Some(5), 100); // 回拨后写入的副本：小钟、大 epoch
        let old = with(Some(4), 1_000_000); // 回拨前副本：大钟
        assert!(new.rank() > old.rank(), "epoch 优先于墙钟");
        // 老格式（无 epoch → 0）：不得凭大钟压过带 epoch 的副本
        let legacy = with(None, 9_000_000);
        assert_eq!(legacy.rank().0, 0);
        assert!(old.rank() > legacy.rank(), "带 epoch 的副本胜老格式副本");
        // 同为老格式：墙钟照常区分
        let legacy2 = with(None, 1);
        assert!(legacy.rank() > legacy2.rank());
    }

    /// M6 切片 3：复制状态 decode 白名单（合法四态 + 非法拒绝）。
    #[test]
    fn replication_status_decode_whitelist() {
        for ok in ["PENDING", "COMPLETED", "FAILED", "REPLICA"] {
            let mut m = sample();
            m.versions[0].replication_status = Some(ok.to_string());
            let bytes = m.encode().unwrap();
            assert_eq!(XlMeta::decode(&bytes).unwrap(), m, "{ok} 应可读");
        }
        for bad in ["", "COMPLETE", "pending", "OK", "REPLICA "] {
            let mut m = sample();
            m.versions[0].replication_status = Some(bad.to_string());
            let bytes = m.encode().unwrap();
            assert!(
                matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))),
                "{bad:?} 应被拒"
            );
        }
    }

    #[test]
    fn golden_bytes_stable() {
        // 字段名与表示在 named 编码下就是磁盘契约：自读自写的 roundtrip 抓不到
        // 字段重命名或 Uuid 表示漂移（str vs bin8），黄金字节能。
        // 更新流程：确认改动是有意的 → cargo test -p xl-store golden -- --nocapture
        // 打印实际摘要 → 更新下常量。改格式 = 有意识地改，不是顺手改。
        use sha2::Digest;
        let bytes = sample().encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
        // 已人工核对：首字节 01；version_id 为 bin8（c4 10 + 16 字节全零）；
        // M2a Task 5 起 Version 增加 parts 字段（单段写 nil）、协议补全 Task 3 起增加 checksum 字段
        //（默认 nil）、M2b Task 3 起增加 data_layout/inline 字段（默认 nil）、M4 切片 3 起增加
        // is_delete_marker/is_latest（无版本写 nil——老解码器跳过未知字段 = 前向兼容契约），
        // 摘要均按「有意识更新」流程重算（本次为第四次；**第五次**：M4 切片 4 增 epoch 字段；
        // **第六次**：M5 切片 2 增 encryption 字段——明文对象写 nil，老解码器跳过；
        // **第七次**：M5 切片 4 增 lock 字段——无锁对象写 nil，老解码器跳过；
        // **第八次**：M6 切片 3 增 replication_status 字段——非复制对象写 nil，老解码器跳过；
        // **第九次**：M8 前置增 tags 字段——无标签对象写 nil，老解码器跳过）。
        // mod_time_nanos 为 uint64（cf）；顶层/嵌套均为字段名 map。
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        println!("{digest}");
        assert_eq!(
            digest,
            "e85b90792fc2b61c86937b28244125bdcff5d6744ce984398a557f4be605d302"
        );
    }
}

#[cfg(test)]
mod parts_tests {
    use super::*;

    fn version(parts: Option<Vec<PartRef>>) -> Version {
        Version {
            version_id: uuid::Uuid::nil(),
            mod_time_nanos: 1,
            size: 5,
            etag: "e".into(),
            content_type: None,
            user_metadata: Default::default(),
            part: PartRef {
                uuid: "single".into(),
                part: 1,
                size: 5,
                sha256: "s".into(),
            },
            parts,
            checksum: None,
            data_layout: None,
            inline: None,
            is_delete_marker: None,
            is_latest: None,
            epoch: None,
            encryption: None,
            lock: None,
            replication_status: None,
            tags: None,
        }
    }

    #[test]
    fn effective_parts_single_vs_multi() {
        // None（M1 单段）→ 兼容位单条
        let v = version(None);
        let ps = v.effective_parts();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].uuid, "single");

        // Some（MPU）→ 原列表为准
        let v = version(Some(vec![
            PartRef {
                uuid: "u".into(),
                part: 1,
                size: 5,
                sha256: "a".into(),
            },
            PartRef {
                uuid: "u".into(),
                part: 2,
                size: 7,
                sha256: "b".into(),
            },
        ]));
        let ps = v.effective_parts();
        assert_eq!(ps.len(), 2);
        assert_eq!(ps[1].part, 2);
    }

    #[test]
    fn decode_rejects_unsafe_parts_uuid() {
        // 多段 parts[].uuid 同样进读路径：含 ../ 必须解码即拒
        let mut v = version(Some(vec![PartRef {
            uuid: "../x".into(),
            part: 1,
            size: 5,
            sha256: "s".into(),
        }]));
        v.part.uuid = "ok".into();
        let m = XlMeta {
            object_key: "a".into(),
            versions: vec![v],
        };
        let bytes = m.encode().unwrap();
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_rejects_unsafe_checksum_record() {
        // checksum 会作为响应头回显：非法字符/超长在解码即拒（磁盘侧防御纵深）
        for bad in [
            meta_checksum("SHA256", "AA\nBB"),
            meta_checksum("SHA256", &"A".repeat(200)),
            meta_checksum("坏算法", "AAAA"),
            meta_checksum("", "AAAA"),
        ] {
            let bytes = bad.encode().unwrap();
            assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
        }
        // 合法值照常解码
        let ok = meta_checksum("SHA256", "uiDrfZrla2mExtJI5U9Pqm9eMzAbCJ3tgpddwnU6cLg=");
        assert!(XlMeta::decode(&ok.encode().unwrap()).is_ok());
    }

    fn meta_checksum(algorithm: &str, value: &str) -> XlMeta {
        let mut v = version(None);
        v.checksum = Some(ChecksumRecord {
            algorithm: algorithm.into(),
            value_base64: value.into(),
        });
        XlMeta {
            object_key: "a".into(),
            versions: vec![v],
        }
    }

    #[test]
    fn decode_old_file_without_layout_fields_is_none() {
        // 前向兼容另一半：旧文件（无 data_layout/inline 字段）解码 → None（有意识降级为旧形态读）
        let m = version(None);
        let bytes = XlMeta {
            object_key: "a".into(),
            versions: vec![m],
        }
        .encode()
        .unwrap();
        let back = XlMeta::decode(&bytes).unwrap();
        assert!(back.versions[0].data_layout.is_none());
        assert!(back.versions[0].inline.is_none());
    }

    #[test]
    fn decode_rejects_oversized_inline() {
        // 信任边界：inline 超 MAX_INLINE 一律拒（防损坏 meta 触发超大分配）
        let mut v = version(None);
        v.inline = Some(InlineData {
            bytes: vec![0u8; MAX_INLINE as usize + 1],
        });
        let bytes = XlMeta {
            object_key: "a".into(),
            versions: vec![v],
        }
        .encode()
        .unwrap();
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));

        // 上限之内照常
        let mut ok = version(None);
        ok.inline = Some(InlineData {
            bytes: vec![7u8; 32],
        });
        let bytes = XlMeta {
            object_key: "a".into(),
            versions: vec![ok],
        }
        .encode()
        .unwrap();
        assert!(XlMeta::decode(&bytes).is_ok());
    }

    #[test]
    fn decode_rejects_inconsistent_shard_count() {
        let mut v = version(None);
        v.data_layout = Some(DataLayout {
            data: 8,
            parity: 4,
            shard_size: 4,
            shards: vec![ShardRef {
                drive: 0,
                part: 1,
                shard: 0,
                size: 4,
                crc32c: 1,
            }], // 1 片 ≠ 8+4
        });
        let bytes = XlMeta {
            object_key: "a".into(),
            versions: vec![v],
        }
        .encode()
        .unwrap();
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn decode_old_file_without_parts_field_is_none() {
        // 前向兼容的另一半：旧文件（无 parts 字段）解码 → None
        #[derive(serde::Serialize)]
        struct OldVersion {
            version_id: uuid::Uuid,
            mod_time_nanos: i64,
            size: u64,
            etag: String,
            content_type: Option<String>,
            user_metadata: BTreeMap<String, String>,
            part: PartRef,
        }
        #[derive(serde::Serialize)]
        struct OldMeta {
            object_key: String,
            versions: Vec<OldVersion>,
        }
        let bytes = rmp_serde::to_vec_named(&OldMeta {
            object_key: "a".into(),
            versions: vec![OldVersion {
                version_id: uuid::Uuid::nil(),
                mod_time_nanos: 1,
                size: 5,
                etag: "e".into(),
                content_type: None,
                user_metadata: Default::default(),
                part: PartRef {
                    uuid: "u".into(),
                    part: 1,
                    size: 5,
                    sha256: "s".into(),
                },
            }],
        })
        .unwrap();
        let mut framed = vec![FORMAT_VERSION];
        framed.extend(bytes);
        let back = XlMeta::decode(&framed).unwrap();
        assert!(back.versions[0].parts.is_none());
        assert!(back.versions[0].tags.is_none(), "旧 meta 无 tags → None");
        assert_eq!(back.versions[0].effective_parts()[0].uuid, "u");
    }

    #[test]
    fn validate_tags_matrix() {
        let tag = |k: &str, v: &str| ObjectTag {
            key: k.into(),
            value: v.into(),
        };
        // 正向：10 条 / 128 字符键 / 256 字符值 / 中文字符按标量计数
        let max_ok: Vec<ObjectTag> = (0..MAX_TAGS)
            .map(|i| ObjectTag {
                key: format!("{i}{}", "键".repeat(127)),
                value: "v".repeat(256),
            })
            .collect();
        assert!(validate_tags(&max_ok).is_ok());
        assert!(validate_tags(&[tag("中文键", "中文值")]).is_ok());
        assert!(validate_tags(&[tag("a b", "v+1=2:3/4@5.6_7-8")]).is_ok());
        // 反向
        assert!(validate_tags(&vec![tag("k", "v"); MAX_TAGS + 1]).is_err());
        assert!(validate_tags(&[tag("", "v")]).is_err());
        assert!(validate_tags(&[tag(&"k".repeat(129), "v")]).is_err());
        assert!(validate_tags(&[tag("k", &"v".repeat(257))]).is_err());
        assert!(validate_tags(&[tag("k", "v"), tag("k", "w")]).is_err());
        assert!(validate_tags(&[tag("k!", "v")]).is_err());
        assert!(validate_tags(&[tag("k\n", "v")]).is_err());
    }

    #[test]
    fn decode_rejects_invalid_tags() {
        let mut v = version(None);
        v.tags = Some(vec![
            ObjectTag {
                key: "dup".into(),
                value: "1".into(),
            },
            ObjectTag {
                key: "dup".into(),
                value: "2".into(),
            },
        ]);
        let bytes = XlMeta {
            object_key: "a".into(),
            versions: vec![v],
        }
        .encode()
        .unwrap();
        assert!(matches!(XlMeta::decode(&bytes), Err(Error::CorruptMeta(_))));
    }
}
