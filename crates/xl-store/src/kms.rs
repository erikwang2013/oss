//! M5 切片 1：密钥库（KeyProvider）——DEK 信封加密的「主密钥侧」。
//!
//! 形态：每对象一把 32B DEK（[`new_dek`]，OsRng）；DEK 不进对象 meta 明文，只存
//! `wrapped = nonce(12) ‖ GCM 密文(32) ‖ tag(16)` 与 `key_id`。数据面（切片 2/3）用 DEK
//! 加对象、用 `key_id` 解 wrap——**换主密钥后旧对象仍可读的条件 = 旧 key_id 的钥仍在**
//! （验收 ⑤；KMS 文件实现的多钥目录即为此）。
//!
//! 两个实现：
//! - [`FileKeyProvider`]：单主密钥文件（SSE-S3 用）。生产路径 `OSS_MASTER_KEY_FILE`，
//!   默认 `<data_root>/.oss.sys/master.key`（0600，首启生成；**多节点须分发同一把**——
//!   README 口径）。env 接线在数据面消费它的切片（2/3）落 oss-server config。
//! - [`KmsFileProvider`]：多主密钥目录（SSE-KMS 用）。`key-<n>.bin`，当前钥 = 最大 n，
//!   [`KmsFileProvider::rotate`] 加一新钥即换钥；旧 key_id 的 wrap 照旧可解（⑤）。
//!   命名 CMK（显式 `x-amz-server-side-encryption-aws-kms-key-id`）放在
//!   `<dir>/cmk/<name>/key-<n>.bin`：名白名单 [a-z0-9-]{1,64}（即路径白名单），每环独立
//!   轮换，key_id 形态 `cmk:<name>:k<n>`。命名环**只经管理 API 创建**，读路径绝不自动生成。
//!
//! HTTP KMS：不做真实实现（切片钉子）——接入点即 [`KeyProvider`] trait，HTTP 客户端
//! 未来以同一 trait 落地，数据面无需改动。

use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead as _, OsRng};
use aes_gcm::{AeadCore as _, Aes256Gcm, KeyInit as _};

use crate::error::{Error, Result};

/// 主密钥/DEK 长度（AES-256）。
pub const KEY_LEN: usize = 32;
/// wrap 层 nonce 长度（GCM 96-bit）。**全随机、每 wrap 独立**（非派生——切片 1 钉子）。
const NONCE_LEN: usize = 12;
/// GCM tag 长度。
const TAG_LEN: usize = 16;
/// 合法 wrapped 长度：nonce ‖ 密文(DEK) ‖ tag。
const WRAPPED_LEN: usize = NONCE_LEN + KEY_LEN + TAG_LEN;

/// 每对象一把新 DEK（OsRng；getrandom 灾难性失败即 panic——OS 级故障，无降级余地）。
pub fn new_dek() -> [u8; KEY_LEN] {
    let mut dek = [0u8; KEY_LEN];
    use aes_gcm::aead::rand_core::RngCore as _;
    OsRng.fill_bytes(&mut dek);
    dek
}

/// 主密钥侧接口：DEK ⇄ wrap 字节串 + key_id（key_id 随对象 meta 落盘/随 wrap 返回）。
#[async_trait::async_trait]
pub trait KeyProvider: Send + Sync + std::fmt::Debug {
    /// 用**当前**主密钥包裹 DEK → `(wrapped, key_id)`；key_id 记录进对象元数据。
    async fn wrap_dek(&self, dek: &[u8; KEY_LEN]) -> Result<(Vec<u8>, String)>;
    /// 显式 CMK 变体：`Some(name)` = 用命名 CMK 的当前版本；`None` = 默认钥（同 `wrap_dek`）。
    /// 不支持命名 CMK 的 provider（SSE-S3 单钥）对 `Some` 一律拒绝。
    async fn wrap_dek_named(
        &self,
        dek: &[u8; KEY_LEN],
        cmk: Option<&str>,
    ) -> Result<(Vec<u8>, String)> {
        match cmk {
            None => self.wrap_dek(dek).await,
            Some(_) => Err(Error::Crypto(
                "该密钥 provider 不支持显式 CMK（SSE-S3 只能用文件主密钥）".into(),
            )),
        }
    }
    /// 用 `key_id` 指定的主密钥解包；错钥/篡改 → `Error::Crypto`（fail-closed，绝不回退
    /// 「试一把看看」的宽口径）。
    async fn unwrap_dek(&self, wrapped: &[u8], key_id: &str) -> Result<[u8; KEY_LEN]>;
}

fn gcm(key: &[u8; KEY_LEN]) -> Aes256Gcm {
    Aes256Gcm::new(key.into())
}

/// AES-256-GCM wrap：nonce 全随机，独立于任何对象状态（同一 DEK 重复 wrap 也不同文）。
fn wrap_with(master: &[u8; KEY_LEN], dek: &[u8; KEY_LEN]) -> Result<Vec<u8>> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = gcm(master)
        .encrypt(&nonce, dek.as_slice())
        .map_err(|_| Error::Crypto("wrap 加密失败".into()))?;
    let mut out = Vec::with_capacity(WRAPPED_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

fn unwrap_with(master: &[u8; KEY_LEN], wrapped: &[u8]) -> Result<[u8; KEY_LEN]> {
    if wrapped.len() != WRAPPED_LEN {
        return Err(Error::Crypto(format!(
            "wrapped 长度非法：{}（应 {WRAPPED_LEN}）",
            wrapped.len()
        )));
    }
    let (nonce, ct) = wrapped.split_at(NONCE_LEN);
    let pt = gcm(master)
        .decrypt(aes_gcm::Nonce::from_slice(nonce), ct)
        .map_err(|_| Error::Crypto("unwrap 解密失败（错钥或篡改）".into()))?;
    pt.try_into()
        .map_err(|_| Error::Crypto("unwrap 明文长度非法".into()))
}

/// **只读**：32B 主密钥文件读取。缺文件 → `Io(NotFound)`；长度非法/权限过宽 → `Crypto`。
/// 绝不创建——读路径带创建副作用会在「已轮换未保留 / 目录缺文件」时生成幽灵钥并被
/// open_or_init 的 max 扫描扶正（verifier 高 1）。
fn read_key_file(path: &Path) -> Result<[u8; KEY_LEN]> {
    use std::os::unix::fs::PermissionsExt as _;
    let md = std::fs::metadata(path)?;
    let mode = md.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(Error::Crypto(format!(
            "主密钥文件 {} 权限过宽（{mode:o}，应 0600）——修正后重启（verifier 中 3：创建后不再校验会让误 chmod 静默泄钥）",
            path.display()
        )));
    }
    let bytes = std::fs::read(path)?;
    let len = bytes.len();
    bytes
        .try_into()
        .map_err(|_| Error::Crypto(format!("主密钥文件 {} 长度非法：{len}", path.display())))
}

/// **创建路径**专用（仅 open_or_init / rotate 调用）：不存在则 0600 生成（`create_new` 防
/// 竞态；并发首启败者有界重试等胜者写完），存在则只读返回。umask 只会收窄权限位。
async fn read_or_create_key(path: &Path) -> Result<[u8; KEY_LEN]> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true).mode(0o600);
    match opts.open(path) {
        Ok(mut f) => {
            use std::io::Write as _;
            let key = new_dek();
            f.write_all(&key)?;
            // 先落盘再交用：半写文件不得被后续读取当作有效钥（长度校验会拒绝，此处加保险）
            f.sync_all()?;
            // 目录项也须落盘（防断电后文件消失 → 下次首启生成新钥 → 既有 wrap 全报废；
            // 目录 fsync 的 EINVAL 宽容口径同 Store::sync_dir；open 失败同入 warn——verifier N4）
            if let Some(dir) = path.parent()
                && let Err(e) = std::fs::File::open(dir).and_then(|d| d.sync_all())
            {
                tracing::warn!(dir = %dir.display(), error = %e, "主密钥目录 fsync/open 失败（容忍；断电保证按文件系统复核）");
            }
            Ok(key)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // 并发首启：胜者可能刚 open、尚未写完 32B → 有界重试（≤500ms），仍无效才报错
            for _ in 0..10 {
                match read_key_file(path) {
                    Ok(k) => return Ok(k),
                    Err(Error::Crypto(_)) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            read_key_file(path)
        }
        Err(e) => Err(e.into()),
    }
}

/// 单主密钥文件 provider（SSE-S3 用）。key_id 恒 `"file"`——文件即唯一钥；换文件 = 换钥，
/// 旧 wrap 不可解（该口径随 README 记档；多钥轮换用 [`KmsFileProvider`]）。
pub struct FileKeyProvider {
    key: [u8; KEY_LEN],
}

/// 手工 Debug：**不得**打印钥字节（derive 会把 [u8;32] 原文打进日志/panic）。
impl std::fmt::Debug for FileKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileKeyProvider").finish_non_exhaustive()
    }
}

impl FileKeyProvider {
    /// 默认路径：`<data_root>/.oss.sys/master.key`（env `OSS_MASTER_KEY_FILE` 的接线见模块头注）。
    pub fn default_path(data_root: &Path) -> PathBuf {
        crate::layout::sys_dir(data_root, "master.key")
    }

    /// 存在即读（校验 32B）、缺失即生成（0600）——幂等，可重复 open。
    pub async fn open_or_init(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        Ok(Self {
            key: read_or_create_key(path).await?,
        })
    }
}

#[async_trait::async_trait]
impl KeyProvider for FileKeyProvider {
    async fn wrap_dek(&self, dek: &[u8; KEY_LEN]) -> Result<(Vec<u8>, String)> {
        Ok((wrap_with(&self.key, dek)?, "file".to_string()))
    }

    async fn unwrap_dek(&self, wrapped: &[u8], key_id: &str) -> Result<[u8; KEY_LEN]> {
        if key_id != "file" {
            return Err(Error::Crypto(format!(
                "key_id 不匹配：单钥文件 provider 只认 \"file\"，实得 {key_id:?}"
            )));
        }
        unwrap_with(&self.key, wrapped)
    }
}

/// 多主密钥目录 provider（SSE-KMS 用）：`<dir>/key-<n>.bin`（0600，32B）。
/// 当前钥 = 最大 n；`key_id` = `"k<n>"`（前缀留协议判别余地，同 `enc:v1:` 风格）。
pub struct KmsFileProvider {
    dir: PathBuf,
    current: u32,
    current_key: [u8; KEY_LEN],
}

/// 手工 Debug：目录/编号可记，**钥字节不打印**（同上防泄露口径）。
impl std::fmt::Debug for KmsFileProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsFileProvider")
            .field("dir", &self.dir)
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

fn key_id_of(n: u32) -> String {
    format!("k{n}")
}

fn parse_key_file(name: &str) -> Option<u32> {
    // 只认规范名（拒前导零/「+7」等变体，防「解析成 n 但重建路径找不到」的幽灵分支）
    let n: u32 = name
        .strip_prefix("key-")?
        .strip_suffix(".bin")?
        .parse()
        .ok()?;
    (name == format!("key-{n}.bin")).then_some(n)
}

// ---------- 显式 CMK（命名密钥环） ----------

/// CMK 名最大长度；名同时是 `cmk/<name>/` 路径分量，故字符集即路径白名单。
pub const CMK_NAME_MAX: usize = 64;

/// 严格 CMK 名：`[a-z0-9-]{1,64}`，首尾须字母数字——无 `/`、`.`、`..`、大写与空名，
/// 路径穿越面在解析处闭合（不依赖后续拼接）。
pub fn validate_cmk_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= CMK_NAME_MAX
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name.as_bytes()[name.len() - 1].is_ascii_alphanumeric();
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "KMS CMK 名称非法：{name:?}（须 [a-z0-9-]，1..={CMK_NAME_MAX}，首尾字母数字）"
        )))
    }
}

/// 线格式 CMK id → 本地 CMK 名。接受裸名或 `arn:…:key/<name>`（AWS 客户端两种都发）；
/// 仅取 `key/` 后的部分并套严格白名单，ARN 其余段不参与寻址。
pub fn parse_cmk_id(raw: &str) -> Result<&str> {
    let s = raw.trim();
    let name = if s.starts_with("arn:") {
        s.rsplit_once("key/")
            .filter(|(p, _)| p.len() > 4)
            .map(|(_, n)| n)
            .ok_or_else(|| Error::InvalidArgument(format!("KMS key ARN 非法：{raw:?}")))?
    } else {
        s
    };
    validate_cmk_name(name)?;
    Ok(name)
}

/// 对象 meta 的内部 key_id：`cmk:<name>:k<n>`（冒号在名称白名单之外，形态无歧义）。
pub fn cmk_key_id(name: &str, n: u32) -> String {
    format!("cmk:{name}:k{n}")
}

/// 规范 CMK key_id 解析；异形（非规范版本号/非法名）→ None。
pub fn parse_cmk_key_id(key_id: &str) -> Option<(&str, u32)> {
    let rest = key_id.strip_prefix("cmk:")?;
    let (name, ver) = rest.rsplit_once(':')?;
    let n: u32 = ver.strip_prefix('k')?.parse().ok()?;
    (ver == format!("k{n}") && validate_cmk_name(name).is_ok()).then_some((name, n))
}

/// 命名 CMK 的只读快照（运维/控制台用；**绝不含钥字节**）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CmkInfo {
    pub name: String,
    /// 当前版本号（新写入用）。
    pub current: u32,
    /// 全部版本（解码仍需旧版本——轮换不重写旧对象）。
    pub versions: Vec<u32>,
    /// 当前版本钥文件的 mtime（unix 纳秒；仅运维展示）。
    pub updated_nanos: i64,
}

/// 列出目录内全部规范 key 版本（空目录 → 空表）。
fn list_key_versions(dir: &Path) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        if let Some(n) = parse_key_file(&e?.file_name().to_string_lossy()) {
            out.push(n);
        }
    }
    out.sort_unstable();
    Ok(out)
}

impl KmsFileProvider {
    /// 默认目录：`<data_root>/.oss.sys/kms/`（env `OSS_KMS_DIR` 的接线同模块头注口径）。
    pub fn default_dir(data_root: &Path) -> PathBuf {
        crate::layout::sys_dir(data_root, "kms")
    }

    /// 目录为空（**真**空）→ 生成 `key-0`（首启）；否则当前钥 = 最大编号。幂等。
    /// `cmk/` 子目录不参与默认环扫描，但**逐个校验**（见 [`Self::validate_cmk_root`]）：
    /// 命名 CMK 绝不在读路径自动生成，损坏/异名目录一律拒启（fail-closed）。
    pub async fn open_or_init(dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let mut max: Option<u32> = None;
        let mut any_entry = false;
        let mut rd = tokio::fs::read_dir(dir).await?;
        while let Some(e) = rd.next_entry().await? {
            let fname = e.file_name();
            if fname == "cmk" {
                if !e.file_type().await?.is_dir() {
                    return Err(Error::Crypto(format!(
                        "KMS 目录 {} 的 cmk 项不是目录——拒启",
                        dir.display()
                    )));
                }
                continue; // 命名环单独校验
            }
            any_entry = true;
            if let Some(n) = parse_key_file(&fname.to_string_lossy()) {
                max = Some(max.map_or(n, |m: u32| m.max(n)));
            }
        }
        let n = match max {
            Some(n) => n,
            None if any_entry => {
                // 非空却无合规钥（钥被改名/备份恢复缺片/误放杂文件）→ 拒启：静默生成会把
                // 新钥扶正为 current，旧 wrap 全报废且当时无告警（verifier N1）
                return Err(Error::Crypto(format!(
                    "KMS 目录 {} 非空但无合规主密钥文件（key-<n>.bin）——拒启；确认后清理或恢复既有钥",
                    dir.display()
                )));
            }
            None => {
                Self::write_new(dir, 0).await?;
                0
            }
        };
        let p = Self {
            dir: dir.to_path_buf(),
            current: n,
            current_key: Self::read_key(dir, n)?,
        };
        p.validate_cmk_root()?;
        Ok(p)
    }

    /// `cmk/` 根校验：每个子目录名必须是合法 CMK 名、是目录、且至少有一把规范钥文件；
    /// 任一不合规 → 拒启（防「拼错目录名 → 运行期才 404」与异名/穿越名混入）。
    fn validate_cmk_root(&self) -> Result<()> {
        let root = self.dir.join("cmk");
        let rd = match std::fs::read_dir(&root) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in rd {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Crypto(format!("KMS CMK 目录名非 UTF-8：{root:?}")))?;
            validate_cmk_name(&name)
                .map_err(|e| Error::Crypto(format!("KMS CMK 目录名非法（{e}）——拒启")))?;
            if !entry.file_type()?.is_dir() {
                return Err(Error::Crypto(format!("KMS cmk 下 {name:?} 不是目录——拒启")));
            }
            let dir = root.join(&name);
            let versions = list_key_versions(&dir)?;
            if versions.is_empty() {
                return Err(Error::Crypto(format!(
                    "KMS CMK {name:?} 目录无合规主密钥文件（key-<n>.bin）——拒启"
                )));
            }
            // 权限/长度校验：读一把当前钥即触发 read_key_file 的 0600+32B 检查
            let _ = Self::read_key(&dir, *versions.last().unwrap())?;
        }
        Ok(())
    }

    fn cmk_dir(&self, name: &str) -> Result<PathBuf> {
        validate_cmk_name(name)?;
        Ok(self.dir.join("cmk").join(name))
    }

    /// 命名 CMK 的当前版本号；不存在 → `InvalidArgument`（客户端可选错名，400）。
    pub fn ensure_cmk(&self, name: &str) -> Result<u32> {
        let dir = self.cmk_dir(name)?;
        let versions = match std::fs::read_dir(&dir) {
            Ok(_) => list_key_versions(&dir)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        versions.last().copied().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "KMS CMK {name:?} 不存在或未配置（cmk/{name}/key-0.bin）"
            ))
        })
    }

    /// 当前版本的钥字节（wrap 用）；不存在 → InvalidArgument（同上，客户端可纠正）。
    fn cmk_current_key(&self, name: &str) -> Result<(u32, [u8; KEY_LEN])> {
        let dir = self.cmk_dir(name)?;
        let n = self.ensure_cmk(name)?;
        Ok((n, Self::read_key(&dir, n)?))
    }

    /// 新建命名 CMK（`key-0`，0600）。已存在 → InvalidArgument（不覆盖、不轮换）。
    pub async fn create_cmk(&self, name: &str) -> Result<u32> {
        let dir = self.cmk_dir(name)?;
        tokio::fs::create_dir_all(&dir).await?;
        if !list_key_versions(&dir)?.is_empty() {
            return Err(Error::InvalidArgument(format!("KMS CMK {name:?} 已存在")));
        }
        if std::fs::read_dir(&dir)?.next().is_some() {
            return Err(Error::Crypto(format!(
                "KMS CMK {name:?} 目录非空但无合规钥——拒绝生成 key-0（防覆盖/混淆）"
            )));
        }
        Self::write_new(&dir, 0).await?;
        Ok(0)
    }

    /// 命名 CMK 轮换：新增 `key-<max+1>` 并置为当前；旧版本保留（旧对象仍可解）。
    pub async fn rotate_cmk(&self, name: &str) -> Result<u32> {
        let dir = self.cmk_dir(name)?;
        let cur = self.ensure_cmk(name)?;
        let next = cur.checked_add(1).ok_or_else(|| {
            Error::Crypto(format!(
                "CMK {name:?} 版本号溢出（current={cur}）——需人工整理"
            ))
        })?;
        Self::write_new(&dir, next).await?;
        Ok(next)
    }

    /// 全部命名 CMK 快照（名/当前版本/版本表/mtime；无 `cmk/` = 空表）。
    pub fn list_cmks(&self) -> Result<Vec<CmkInfo>> {
        let root = self.dir.join("cmk");
        let rd = match std::fs::read_dir(&root) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for entry in rd {
            let entry = entry?;
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if validate_cmk_name(&name).is_err() || !entry.file_type()?.is_dir() {
                continue;
            }
            let dir = root.join(&name);
            let versions = list_key_versions(&dir)?;
            let Some(&current) = versions.last() else {
                continue;
            };
            let updated_nanos = std::fs::metadata(Self::key_path(&dir, current))
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            out.push(CmkInfo {
                name,
                current,
                versions,
                updated_nanos,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// 当前版本的指纹（SHA-256 前 8 字节 hex）——审计/运维关联用，**不回钥字节**。
    pub fn cmk_fingerprint(&self, name: &str) -> Result<String> {
        use sha2::{Digest as _, Sha256};
        let (_, key) = self.cmk_current_key(name)?;
        Ok(hex::encode(&Sha256::digest(key)[..8]))
    }

    /// 当前主密钥编号（落对象 meta 的 key_id 由 wrap 返回值携带，此值供运维/日志）。
    pub fn current_id(&self) -> u32 {
        self.current
    }

    fn key_path(dir: &Path, n: u32) -> PathBuf {
        dir.join(format!("key-{n}.bin"))
    }

    /// 创建/取回指定编号的钥（仅 open_or_init 首启与 rotate 换钥经此）。
    async fn write_new(dir: &Path, n: u32) -> Result<()> {
        read_or_create_key(&Self::key_path(dir, n)).await?;
        Ok(())
    }

    /// 只读取钥（unwrap 读路径与 open_or_init 命中分支经此；绝不创建——verifier 高 1）。
    fn read_key(dir: &Path, n: u32) -> Result<[u8; KEY_LEN]> {
        read_key_file(&Self::key_path(dir, n))
    }

    /// 换钥：新增 `key-<max+1>` 并置为当前；旧编号的 wrap 照旧可解（验收 ⑤）。
    pub async fn rotate(&mut self) -> Result<u32> {
        let next = self.current.checked_add(1).ok_or_else(|| {
            Error::Crypto(format!(
                "主密钥编号溢出（current={}）——目录需人工整理",
                self.current
            ))
        })?;
        Self::write_new(&self.dir, next).await?;
        self.current_key = Self::read_key(&self.dir, next)?;
        self.current = next;
        Ok(next)
    }
}

#[async_trait::async_trait]
impl KeyProvider for KmsFileProvider {
    async fn wrap_dek(&self, dek: &[u8; KEY_LEN]) -> Result<(Vec<u8>, String)> {
        Ok((wrap_with(&self.current_key, dek)?, key_id_of(self.current)))
    }

    async fn wrap_dek_named(
        &self,
        dek: &[u8; KEY_LEN],
        cmk: Option<&str>,
    ) -> Result<(Vec<u8>, String)> {
        let Some(name) = cmk else {
            return self.wrap_dek(dek).await;
        };
        let (n, key) = self.cmk_current_key(name)?;
        Ok((wrap_with(&key, dek)?, cmk_key_id(name, n)))
    }

    async fn unwrap_dek(&self, wrapped: &[u8], key_id: &str) -> Result<[u8; KEY_LEN]> {
        // 显式 CMK：`cmk:<name>:k<n>` → 命名环；缺失/坏名 → Crypto（数据面 fail-closed）
        if let Some((name, n)) = parse_cmk_key_id(key_id) {
            let dir = self
                .cmk_dir(name)
                .map_err(|_| Error::Crypto(format!("key_id 形态非法：{key_id:?}")))?;
            let key = Self::read_key(&dir, n).map_err(|e| match e {
                Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Crypto(
                    format!("CMK {name:?} 的 key-{n} 不存在（已轮换且未保留？）"),
                ),
                other => other,
            })?;
            return unwrap_with(&key, wrapped);
        }
        // 规范形态校验：只认 "k<n>"（"k+1"/"k01" 等价异形一律拒——与文件名口径同规）
        let n: u32 = key_id
            .strip_prefix('k')
            .and_then(|s| s.parse().ok())
            .filter(|n| key_id == key_id_of(*n))
            .ok_or_else(|| Error::Crypto(format!("key_id 形态非法：{key_id:?}")))?;
        let key = Self::read_key(&self.dir, n).map_err(|e| match e {
            Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => Error::Crypto(format!(
                "key_id {key_id} 的主密钥不存在（已轮换且未保留？）"
            )),
            other => other,
        })?;
        unwrap_with(&key, wrapped)
    }
}

// ---------- M5 切片 6：SK（access_keys.secret_key）信封加密（裁决 T1） ----------
//
// 形态：`enc:v1:<key_id>:<b64(ct)>:<b64(wrapped)>`；`ct = nonce(12)‖GCM 密文‖tag(16)`，
// AAD = `b"oss-sk-v1"`（与数据面单元域分隔）；每行新生成 32B DEK，`wrapped` = wrap_dek(DEK)。
// key_id 路由（"file" → FileKeyProvider；"k<n>" → KmsFileProvider）由调用侧（vault）做。

use base64::Engine as _;

/// SK 密文前缀（判别明/密文；明文存量不动、混存期共存）。
pub const SECRET_PREFIX: &str = "enc:v1:";
/// SK 层 AAD（域分隔）。
const SK_AAD: &[u8] = b"oss-sk-v1";

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// 是否已封装（前缀判别）。
pub fn is_sealed(s: &str) -> bool {
    s.starts_with(SECRET_PREFIX)
}

/// 密文行 → key_id（未封装/坏形态 → None）。
pub fn sealed_key_id(stored: &str) -> Option<&str> {
    let rest = stored.strip_prefix(SECRET_PREFIX)?;
    rest.split(':').next().filter(|k| !k.is_empty())
}

/// 封装明文 SK → `enc:v1:...`（每行新 DEK；provider 决定 key_id）。
pub async fn seal_secret(p: &dyn KeyProvider, plaintext: &str) -> Result<String> {
    let dek = new_dek();
    let cipher = gcm(&dek);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: plaintext.as_bytes(),
                aad: SK_AAD,
            },
        )
        .map_err(|_| Error::Crypto("SK 加密失败".into()))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ct.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ct);
    let (wrapped, key_id) = p.wrap_dek(&dek).await?;
    Ok(format!(
        "{SECRET_PREFIX}{key_id}:{}:{}",
        b64().encode(&blob),
        b64().encode(&wrapped)
    ))
}

/// 解封 `enc:v1:...` → 明文 SK；未封装行 → `Error::InvalidArgument`（调用方先 `is_sealed`）。
/// 篡改/丢钥/坏 b64 → `Error::Crypto`（fail-closed）。
pub async fn open_secret(p: &dyn KeyProvider, stored: &str) -> Result<String> {
    let rest = stored
        .strip_prefix(SECRET_PREFIX)
        .ok_or_else(|| Error::InvalidArgument("非 enc:v1 封装行".into()))?;
    let mut parts = rest.splitn(3, ':');
    let key_id = parts.next().filter(|k| !k.is_empty());
    let (Some(key_id), Some(blob_b64), Some(wrapped_b64), None) =
        (key_id, parts.next(), parts.next(), parts.next())
    else {
        return Err(Error::Crypto("SK 封装行形态非法".into()));
    };
    let blob = b64()
        .decode(blob_b64)
        .map_err(|_| Error::Crypto("SK 封装箱坏 b64".into()))?;
    let wrapped = b64()
        .decode(wrapped_b64)
        .map_err(|_| Error::Crypto("SK 封装 wrapped 坏 b64".into()))?;
    if blob.len() < NONCE_LEN + TAG_LEN {
        return Err(Error::Crypto("SK 封装箱过短".into()));
    }
    let dek = p.unwrap_dek(&wrapped, key_id).await?;
    let (nonce, ct) = blob.split_at(NONCE_LEN);
    let pt = gcm(&dek)
        .decrypt(
            aes_gcm::Nonce::from_slice(nonce),
            aes_gcm::aead::Payload {
                msg: ct,
                aad: SK_AAD,
            },
        )
        .map_err(|_| Error::Crypto("SK 解密失败（错钥/篡改/域不符）".into()))?;
    String::from_utf8(pt).map_err(|_| Error::Crypto("SK 明文非 UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试里手工造钥文件须 0600（生产同口径；0644 会被权限检查拒）。
    fn write_key_file(path: &Path, bytes: &[u8; KEY_LEN]) {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        use std::io::Write as _;
        f.write_all(bytes).unwrap();
    }

    #[tokio::test]
    async fn file_provider_roundtrip() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("k/master.key"))
            .await
            .unwrap();
        let dek = new_dek();
        let (wrapped, kid) = p.wrap_dek(&dek).await.unwrap();
        assert_eq!(kid, "file");
        assert_eq!(wrapped.len(), WRAPPED_LEN);
        assert_eq!(p.unwrap_dek(&wrapped, "file").await.unwrap(), dek);
        // nonce 全随机：同一 DEK 两次 wrap 密文不同
        let (w2, _) = p.wrap_dek(&dek).await.unwrap();
        assert_ne!(wrapped, w2, "wrap nonce 必须每 wrap 独立随机");
        // 篡改/tag 破坏 → 拒绝
        let mut bad = wrapped.clone();
        bad[WRAPPED_LEN - 1] ^= 1;
        assert!(matches!(
            p.unwrap_dek(&bad, "file").await,
            Err(Error::Crypto(_))
        ));
        // 错 key_id → 拒绝
        assert!(matches!(
            p.unwrap_dek(&wrapped, "k0").await,
            Err(Error::Crypto(_))
        ));
    }

    /// 注：mode 断言在 umask 含 0o077 位的环境下对「创建改 0644」突变会假绿（0644 & ~0077 =
    /// 0600）——CI 常规 umask 022 下有效；容器化 CI 若改 umask 需同步复核。
    #[tokio::test]
    async fn master_key_file_generation_is_idempotent_and_0600() {
        use std::os::unix::fs::PermissionsExt as _;
        let td = tempfile::tempdir().unwrap();
        let path = FileKeyProvider::default_path(td.path());
        let p1 = FileKeyProvider::open_or_init(&path).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "主密钥文件必须 0600");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), KEY_LEN as u64);
        // 二次 open：不重生成（内容不变），且两 provider 同一把钥
        let before = std::fs::read(&path).unwrap();
        let p2 = FileKeyProvider::open_or_init(&path).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let dek = new_dek();
        let (w, _) = p1.wrap_dek(&dek).await.unwrap();
        assert_eq!(p2.unwrap_dek(&w, "file").await.unwrap(), dek);
    }

    #[tokio::test]
    async fn master_key_wrong_size_rejected() {
        use std::os::unix::fs::OpenOptionsExt as _;
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("short.key");
        // 0600 造短文件：确保测的是长度检查（不是权限检查）
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        use std::io::Write as _;
        f.write_all(b"0123456789").unwrap();
        drop(f);
        assert!(matches!(
            FileKeyProvider::open_or_init(&path).await,
            Err(Error::Crypto(_))
        ));
    }

    #[tokio::test]
    async fn kms_roundtrip_default_key_and_unknown_id_rejected() {
        let td = tempfile::tempdir().unwrap();
        let p = KmsFileProvider::open_or_init(td.path()).await.unwrap();
        assert_eq!(p.current_id(), 0);
        let dek = new_dek();
        let (wrapped, kid) = p.wrap_dek(&dek).await.unwrap();
        assert_eq!(kid, "k0");
        assert_eq!(p.unwrap_dek(&wrapped, "k0").await.unwrap(), dek);
        // 未知 key_id：不存在、形态非法，都拒
        assert!(matches!(
            p.unwrap_dek(&wrapped, "k9").await,
            Err(Error::Crypto(_))
        ));
        assert!(matches!(
            p.unwrap_dek(&wrapped, "nonsense").await,
            Err(Error::Crypto(_))
        ));
    }

    /// 验收 ⑤：换主密钥后旧 wrap 仍可解（旧 key_id 的钥仍在目录里）。
    #[tokio::test]
    async fn kms_rotation_keeps_old_wraps_readable() {
        let td = tempfile::tempdir().unwrap();
        let mut p = KmsFileProvider::open_or_init(td.path()).await.unwrap();
        let dek = new_dek();
        let (old_wrap, old_kid) = p.wrap_dek(&dek).await.unwrap();
        assert_eq!(old_kid, "k0");

        let n = p.rotate().await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(p.current_id(), 1);
        // 新 wrap 走新钥
        let dek2 = new_dek();
        let (new_wrap, new_kid) = p.wrap_dek(&dek2).await.unwrap();
        assert_eq!(new_kid, "k1");
        // 旧 wrap 仍可解（DEK 未变——验收⑤口径）
        assert_eq!(p.unwrap_dek(&old_wrap, "k0").await.unwrap(), dek);
        assert_eq!(p.unwrap_dek(&new_wrap, "k1").await.unwrap(), dek2);
        // 重开（重启）：当前钥仍为 k1，旧 wrap 依然后可解
        let p2 = KmsFileProvider::open_or_init(td.path()).await.unwrap();
        assert_eq!(p2.current_id(), 1);
        assert_eq!(p2.unwrap_dek(&old_wrap, "k0").await.unwrap(), dek);
    }

    #[tokio::test]
    async fn new_dek_is_32b_and_random() {
        let a = new_dek();
        let b = new_dek();
        assert_ne!(a, b, "DEK 必须随机（OsRng）");
        assert_ne!(a, [0u8; KEY_LEN]);
    }

    /// verifier 中 5：wrapped 非 60B 一律 Crypto 拒绝（长度检查被删则此测红；<12B 还会落到
    /// Nonce::from_slice 的 panic 面——本测同时钉「不 panic」）。
    #[tokio::test]
    async fn unwrap_rejects_wrong_length() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        for len in [0usize, 12, 59, 61] {
            let bad = vec![0u8; len];
            assert!(
                matches!(p.unwrap_dek(&bad, "file").await, Err(Error::Crypto(_))),
                "len={len} 应 Crypto 拒绝"
            );
        }
    }

    /// verifier 高 1：读路径绝不创建钥文件——(a) 未知 key_id 不留文件；(b) 已删的钥不复活。
    #[tokio::test]
    async fn unwrap_never_creates_key_files() {
        let td = tempfile::tempdir().unwrap();
        let dir = td.path().join("kms");
        let p = KmsFileProvider::open_or_init(&dir).await.unwrap();
        let dek = new_dek();
        let (w, _) = p.wrap_dek(&dek).await.unwrap();
        assert!(p.unwrap_dek(&w, "k9").await.is_err());
        let names = |d: &Path| -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(d)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            v.sort();
            v
        };
        assert_eq!(names(&dir), vec!["key-0.bin"], "未知 key_id 不得落盘幽灵钥");
        // 钥被删（备份恢复缺片/误删）→ 读失败，且不得复活出新钥
        std::fs::remove_file(dir.join("key-0.bin")).unwrap();
        assert!(matches!(
            p.unwrap_dek(&w, "k0").await,
            Err(Error::Crypto(_))
        ));
        assert!(names(&dir).is_empty(), "缺钥时不得创建替代钥");
    }

    /// verifier 中 3：已存在文件权限过宽须拒（chmod 0644 后 open_or_init 报 Crypto）。
    #[tokio::test]
    async fn existing_key_with_loose_permissions_is_rejected() {
        use std::os::unix::fs::PermissionsExt as _;
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("master.key");
        FileKeyProvider::open_or_init(&path).await.unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            FileKeyProvider::open_or_init(&path).await,
            Err(Error::Crypto(_))
        ));
    }

    /// verifier 低 6 + N2：key_id 只认规范形态（"k+1"/"k01" 与文件名口径同规拒绝）。
    /// 末段在**目标编号真实存在**（k1）时仍须拒——否则删掉规范 filter 会命中 key-1.bin 而假绿。
    #[tokio::test]
    async fn key_id_must_be_canonical() {
        let td = tempfile::tempdir().unwrap();
        let mut p = KmsFileProvider::open_or_init(&td.path().join("kms"))
            .await
            .unwrap();
        let dek = new_dek();
        let (w, _) = p.wrap_dek(&dek).await.unwrap();
        for weird in ["k+1", "k01", "k 1", "k"] {
            assert!(
                matches!(p.unwrap_dek(&w, weird).await, Err(Error::Crypto(_))),
                "{weird:?} 应拒"
            );
        }
        // N2 判别段：rotate 后 k1 存在——"k01"/"k+1" 若被宽容会命中 key-1.bin 而解开
        p.rotate().await.unwrap();
        let (w1, kid) = p.wrap_dek(&new_dek()).await.unwrap();
        assert_eq!(kid, "k1");
        assert!(
            p.unwrap_dek(&w1, "k1").await.is_ok(),
            "规范 key_id 须命中 k1"
        );
        for weird in ["k01", "k+1"] {
            assert!(
                matches!(p.unwrap_dek(&w1, weird).await, Err(Error::Crypto(_))),
                "{weird:?} 在 k1 存在时仍须拒（规范形态判据）"
            );
        }
    }

    /// verifier N2：`parse_key_file` 规范名判据直接钉死（异形/溢出/夹带一律 None）。
    #[test]
    fn parse_key_file_accepts_only_canonical_names() {
        assert_eq!(parse_key_file("key-0.bin"), Some(0));
        assert_eq!(parse_key_file("key-7.bin"), Some(7));
        assert_eq!(parse_key_file("key-4294967295.bin"), Some(u32::MAX));
        for bad in [
            "key-007.bin",
            "key-01.bin",
            "key-+1.bin",
            "key- 1.bin",
            "key-1.bin.bak",
            "key-1.BIN",
            "key-1.bin ",
            "key-1",
            "key-.bin",
            "key-4294967296.bin",
            "other.bin",
        ] {
            assert_eq!(parse_key_file(bad), None, "{bad:?} 应拒");
        }
    }

    /// verifier N1：非空但无合规钥文件的目录 → 拒启（不得静默生成 key-0 被 max 扫描扶正）。
    #[tokio::test]
    async fn non_empty_dir_without_canonical_keys_is_rejected() {
        let td = tempfile::tempdir().unwrap();
        // (a) 只有异名钥文件（改名/备份恢复缺片）→ 拒，且不得冒出 key-0.bin
        let dir = td.path().join("kms");
        std::fs::create_dir_all(&dir).unwrap();
        write_key_file(&dir.join("key-007.bin"), &[3u8; KEY_LEN]);
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir).await,
            Err(Error::Crypto(_))
        ));
        assert!(!dir.join("key-0.bin").exists(), "拒启路径不得生成新钥");
        // (b) 只有无关文件（README 等）→ 同拒
        let dir2 = td.path().join("kms2");
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir2.join("README"), b"kms dir").unwrap();
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir2).await,
            Err(Error::Crypto(_))
        ));
        assert!(!dir2.join("key-0.bin").exists());
        // (c) 真空目录仍是合法首启（生成 key-0）——不误伤
        let dir3 = td.path().join("kms3");
        let p = KmsFileProvider::open_or_init(&dir3).await.unwrap();
        assert_eq!(p.current_id(), 0);
        assert!(dir3.join("key-0.bin").exists());
    }

    /// verifier 高 1 附：编号已在 u32::MAX 的目录，rotate 明确报溢出而不是回绕/panic。
    #[tokio::test]
    async fn rotate_overflow_is_an_error() {
        let td = tempfile::tempdir().unwrap();
        write_key_file(&td.path().join("key-4294967295.bin"), &[9u8; KEY_LEN]);
        let mut p = KmsFileProvider::open_or_init(td.path()).await.unwrap();
        assert_eq!(p.current_id(), u32::MAX);
        assert!(matches!(p.rotate().await, Err(Error::Crypto(_))));
    }

    /// 同 provider 不同钥（另一目录的 provider）→ unwrap 必须失败（跨钥混淆面）。
    #[tokio::test]
    async fn unwrap_with_other_key_fails() {
        let tda = tempfile::tempdir().unwrap();
        let tdb = tempfile::tempdir().unwrap();
        let pa = FileKeyProvider::open_or_init(&tda.path().join("master.key"))
            .await
            .unwrap();
        let pb = FileKeyProvider::open_or_init(&tdb.path().join("master.key"))
            .await
            .unwrap();
        let dek = new_dek();
        let (w, _) = pa.wrap_dek(&dek).await.unwrap();
        assert!(matches!(
            pb.unwrap_dek(&w, "file").await,
            Err(Error::Crypto(_))
        ));
    }

    /// 钥不进日志：Debug 输出不得含钥字节原文（derive 会泄露 → 手工实现钉死）。
    #[tokio::test]
    async fn debug_does_not_leak_key_bytes() {
        let td = tempfile::tempdir().unwrap();
        let fp = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        let d = format!("{fp:?}");
        assert!(d.contains("FileKeyProvider"));
        assert!(
            !d.contains(&format!("{:?}", fp.key)),
            "FileKeyProvider Debug 泄露钥"
        );
        let kp = KmsFileProvider::open_or_init(&td.path().join("kms"))
            .await
            .unwrap();
        let d = format!("{kp:?}");
        assert!(d.contains("KmsFileProvider") && d.contains("current"));
        assert!(
            !d.contains(&format!("{:?}", kp.current_key)),
            "KmsFileProvider Debug 泄露钥"
        );
    }

    /// M5 切片 6：SK 封存/解封 + 失败面。
    #[tokio::test]
    async fn seal_open_secret_roundtrip_and_failures() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        let sealed = seal_secret(&p, "SK_TOP_SECRET_0001").await.unwrap();
        assert!(sealed.starts_with("enc:v1:file:"), "{sealed}");
        assert!(is_sealed(&sealed));
        assert_eq!(sealed_key_id(&sealed), Some("file"));
        assert_eq!(
            open_secret(&p, &sealed).await.unwrap(),
            "SK_TOP_SECRET_0001"
        );
        // 非封装行 → InvalidArgument（调用方先 is_sealed）
        assert!(matches!(
            open_secret(&p, "plain-sk").await.unwrap_err(),
            Error::InvalidArgument(_)
        ));
        // 篡改 ct（b64 中段翻一个字符）→ Crypto
        let mut parts: Vec<String> = sealed.split(':').map(str::to_string).collect();
        let mut ct = parts[3].clone().into_bytes();
        ct[2] = if ct[2] == b'A' { b'B' } else { b'A' };
        parts[3] = String::from_utf8(ct).unwrap();
        let tampered = parts.join(":");
        assert!(matches!(
            open_secret(&p, &tampered).await.unwrap_err(),
            Error::Crypto(_)
        ));
        // 错 key_id（file 封装改成 k9）→ file provider 拒
        let wrong_id = sealed.replacen("enc:v1:file:", "enc:v1:k9:", 1);
        assert!(matches!(
            open_secret(&p, &wrong_id).await.unwrap_err(),
            Error::Crypto(_)
        ));
        // 坏 b64 → Crypto；形态残缺 → Crypto
        assert!(matches!(
            open_secret(&p, "enc:v1:file:!!!:###").await.unwrap_err(),
            Error::Crypto(_)
        ));
        assert!(matches!(
            open_secret(&p, "enc:v1:file:onlyonepart")
                .await
                .unwrap_err(),
            Error::Crypto(_)
        ));
        // 每行独立 DEK：同明文两次封装 → 字节不同
        let s2 = seal_secret(&p, "SK_TOP_SECRET_0001").await.unwrap();
        assert_ne!(sealed, s2);
        assert_eq!(open_secret(&p, &s2).await.unwrap(), "SK_TOP_SECRET_0001");
    }

    /// M5 切片 6（P3-1 域分隔）：用**其他 AAD**（数据面形态）封装同样的 SK → 必解不开。
    #[tokio::test]
    async fn seal_secret_aad_domain_separation() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        let dek = new_dek();
        let (wrapped, kid) = p.wrap_dek(&dek).await.unwrap();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ct = gcm(&dek)
            .encrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: b"SK_TOP_SECRET_0001",
                    aad: b"wrong-aad-domain", // ← 域错
                },
            )
            .unwrap();
        let mut blob = Vec::new();
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ct);
        let forged = format!(
            "{SECRET_PREFIX}{kid}:{}:{}",
            b64().encode(&blob),
            b64().encode(&wrapped)
        );
        assert!(matches!(
            open_secret(&p, &forged).await.unwrap_err(),
            Error::Crypto(_)
        ));
        // 同构造但 AAD 正确 → 可解（证明差异确实来自 AAD，而非构造路径）
        let nonce2 = Aes256Gcm::generate_nonce(&mut OsRng);
        let ct2 = gcm(&dek)
            .encrypt(
                &nonce2,
                aes_gcm::aead::Payload {
                    msg: b"SK_TOP_SECRET_0001",
                    aad: SK_AAD,
                },
            )
            .unwrap();
        let mut blob2 = Vec::new();
        blob2.extend_from_slice(&nonce2);
        blob2.extend_from_slice(&ct2);
        let ok = format!(
            "{SECRET_PREFIX}{kid}:{}:{}",
            b64().encode(&blob2),
            b64().encode(&wrapped)
        );
        assert_eq!(open_secret(&p, &ok).await.unwrap(), "SK_TOP_SECRET_0001");
    }

    /// M5 切片 6（P3-4）：短 blob 护栏（防 `split_at` panic）——合法 wrapped + 短 ct。
    #[tokio::test]
    async fn seal_secret_short_blob_guard() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        let sealed = seal_secret(&p, "SK_TOP_SECRET_0001").await.unwrap();
        let parts: Vec<&str> = sealed.split(':').collect();
        let short = format!(
            "{SECRET_PREFIX}file:{}:{}",
            b64().encode([0u8; 5]), // < NONCE_LEN + TAG_LEN
            parts[4]
        );
        assert!(matches!(
            open_secret(&p, &short).await.unwrap_err(),
            Error::Crypto(_)
        ));
    }

    // ---------- 3.16：显式 CMK（命名环） ----------

    /// 名白名单 = 路径白名单：穿越/大写/空/超长一律拒；线格式接受裸名与 ARN。
    #[test]
    fn cmk_name_validation_and_parsing() {
        let max = "a".repeat(CMK_NAME_MAX);
        for ok in ["a", "tenant-a", "k0", "a-b-c", max.as_str()] {
            assert!(validate_cmk_name(ok).is_ok(), "{ok:?} 应合法");
        }
        let too_long = "a".repeat(CMK_NAME_MAX + 1);
        for bad in [
            "",
            "-a",
            "a-",
            "A",
            "a_b",
            "a.b",
            "a/b",
            "..",
            "a b",
            too_long.as_str(),
        ] {
            assert!(
                matches!(validate_cmk_name(bad), Err(Error::InvalidArgument(_))),
                "{bad:?} 应拒"
            );
        }
        assert_eq!(parse_cmk_id("tenant-a").unwrap(), "tenant-a");
        assert_eq!(
            parse_cmk_id(" arn:aws:kms:us-east-1:000000000000:key/tenant-a ").unwrap(),
            "tenant-a"
        );
        for bad in [
            "",
            "..",
            "x/y",
            "arn:aws:kms:us-east-1:000000000000:alias/tenant-a",
            "arn:aws:kms:key/",
        ] {
            assert!(parse_cmk_id(bad).is_err(), "{bad:?} 应拒");
        }
        assert_eq!(cmk_key_id("t-1", 7), "cmk:t-1:k7");
        assert_eq!(parse_cmk_key_id("cmk:t-1:k7"), Some(("t-1", 7)));
        for bad in [
            "cmk:t-1:k01",
            "cmk:t-1:k+1",
            "cmk:T:k0",
            "cmk::k0",
            "cmk:t-1",
            "k0",
        ] {
            assert_eq!(parse_cmk_key_id(bad), None, "{bad:?} 应拒");
        }
    }

    /// 创建 → wrap/unwrap → 轮换（旧 wrap 仍可解）→ 重启；跨 CMK 解不开。
    #[tokio::test]
    async fn cmk_create_roundtrip_rotation_and_restart() {
        use std::os::unix::fs::PermissionsExt as _;
        let td = tempfile::tempdir().unwrap();
        let dir = td.path().join("kms");
        let p = KmsFileProvider::open_or_init(&dir).await.unwrap();
        // 未创建：写路径 400，读路径绝不生成目录/钥
        assert!(matches!(
            p.wrap_dek_named(&new_dek(), Some("tenant-a")).await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(
            !dir.join("cmk/tenant-a").exists(),
            "读/写路径不得自动生成 CMK"
        );
        // 创建：key-0、0600
        assert_eq!(p.create_cmk("tenant-a").await.unwrap(), 0);
        let key0 = dir.join("cmk/tenant-a/key-0.bin");
        assert_eq!(
            std::fs::metadata(&key0).unwrap().permissions().mode() & 0o777,
            0o600,
            "CMK 钥文件必须 0600"
        );
        let dek = new_dek();
        let (w0, kid0) = p.wrap_dek_named(&dek, Some("tenant-a")).await.unwrap();
        assert_eq!(kid0, "cmk:tenant-a:k0");
        assert_eq!(p.unwrap_dek(&w0, &kid0).await.unwrap(), dek);
        // 缺失版本：Crypto，且不得生成 key-1
        assert!(matches!(
            p.unwrap_dek(&w0, "cmk:tenant-a:k1").await,
            Err(Error::Crypto(_))
        ));
        assert!(!dir.join("cmk/tenant-a/key-1.bin").exists());
        // 跨 CMK：另一把钥解不开
        p.create_cmk("tenant-b").await.unwrap();
        let dek_b = new_dek();
        let (wb, _) = p.wrap_dek_named(&dek_b, Some("tenant-b")).await.unwrap();
        assert!(matches!(
            p.unwrap_dek(&wb, &kid0).await,
            Err(Error::Crypto(_))
        ));
        // 轮换：新写 k1；旧 wrap 依 k0 可解
        assert_eq!(p.rotate_cmk("tenant-a").await.unwrap(), 1);
        let (w1, kid1) = p.wrap_dek_named(&dek, Some("tenant-a")).await.unwrap();
        assert_eq!(kid1, "cmk:tenant-a:k1");
        assert_eq!(p.unwrap_dek(&w0, "cmk:tenant-a:k0").await.unwrap(), dek);
        assert_eq!(p.unwrap_dek(&w1, &kid1).await.unwrap(), dek);
        assert_ne!(w0, w1);
        // 重启：当前版本 k1，旧版本仍可解
        let p2 = KmsFileProvider::open_or_init(&dir).await.unwrap();
        assert_eq!(p2.ensure_cmk("tenant-a").unwrap(), 1);
        assert_eq!(p2.unwrap_dek(&w0, "cmk:tenant-a:k0").await.unwrap(), dek);
        assert_eq!(p2.unwrap_dek(&w1, "cmk:tenant-a:k1").await.unwrap(), dek);
        // 列表：按名排序、版本表、指纹只含 hex（无钥字节）
        let list = p2.list_cmks().unwrap();
        assert_eq!(
            list.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["tenant-a", "tenant-b"]
        );
        assert_eq!(list[0].versions, vec![0, 1]);
        assert_eq!(list[0].current, 1);
        assert!(list[0].updated_nanos > 0);
        let fp = p2.cmk_fingerprint("tenant-a").unwrap();
        assert_eq!(fp.len(), 16);
        assert!(fp.bytes().all(|b| b.is_ascii_hexdigit()));
        // 重复创建 / 未知轮换 → 400
        assert!(matches!(
            p2.create_cmk("tenant-a").await,
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            p2.rotate_cmk("nope").await,
            Err(Error::InvalidArgument(_))
        ));
        // None 保持默认环行为（k<n>）
        let (_, kid) = p2.wrap_dek_named(&new_dek(), None).await.unwrap();
        assert_eq!(kid, "k0");
    }

    /// `cmk/` 根：异名/空目录/非目录项/权限过宽 → 拒启（读路径不修不建）。
    #[tokio::test]
    async fn cmk_dir_validation_rejects_bad_layout() {
        use std::os::unix::fs::PermissionsExt as _;
        let td = tempfile::tempdir().unwrap();
        let dir = td.path().join("kms");
        let p = KmsFileProvider::open_or_init(&dir).await.unwrap();
        p.create_cmk("good-name").await.unwrap();
        assert!(KmsFileProvider::open_or_init(&dir).await.is_ok());
        // 异名目录
        std::fs::create_dir_all(dir.join("cmk/Bad_Name")).unwrap();
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir).await,
            Err(Error::Crypto(_))
        ));
        std::fs::remove_dir_all(dir.join("cmk/Bad_Name")).unwrap();
        // 空 CMK 目录（无 key-<n>.bin）
        std::fs::create_dir_all(dir.join("cmk/empty")).unwrap();
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir).await,
            Err(Error::Crypto(_))
        ));
        std::fs::remove_dir_all(dir.join("cmk/empty")).unwrap();
        // 权限过宽
        let key = dir.join("cmk/good-name/key-0.bin");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir).await,
            Err(Error::Crypto(_))
        ));
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        // cmk 项是文件不是目录
        let td2 = tempfile::tempdir().unwrap();
        let dir2 = td2.path().join("kms");
        let p2 = KmsFileProvider::open_or_init(&dir2).await.unwrap();
        let _ = p2;
        std::fs::write(dir2.join("cmk"), b"not a dir").unwrap();
        assert!(matches!(
            KmsFileProvider::open_or_init(&dir2).await,
            Err(Error::Crypto(_))
        ));
    }

    /// SSE-S3 单钥 provider 不支持显式 CMK（fail-closed，绝不回退默认钥）。
    #[tokio::test]
    async fn file_provider_rejects_named_cmk() {
        let td = tempfile::tempdir().unwrap();
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        assert!(matches!(
            p.wrap_dek_named(&new_dek(), Some("tenant-a")).await,
            Err(Error::Crypto(_))
        ));
        let dek = new_dek();
        let (w, _) = p.wrap_dek_named(&dek, None).await.unwrap();
        assert_eq!(p.unwrap_dek(&w, "file").await.unwrap(), dek);
    }

    /// 并发首启竞态：败者先读到半写文件 → 有界重试等到 32B 写完即成功（不误报损坏）。
    #[tokio::test]
    async fn concurrent_first_boot_retries_until_key_complete() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("master.key");
        write_key_file(&path, &[0u8; KEY_LEN]);
        std::fs::write(&path, b"half").unwrap(); // 模拟胜者写到一半（截断保持 0600）
        let writer_path = path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            write_key_file(&writer_path, &[7u8; KEY_LEN]);
        });
        let p = FileKeyProvider::open_or_init(&path).await.unwrap();
        let dek = new_dek();
        let (w, _) = p.wrap_dek(&dek).await.unwrap();
        assert_eq!(p.unwrap_dek(&w, "file").await.unwrap(), dek);
    }
}
