# M1a 后端垂直切片 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 打通「e-cat 起服务 → s3s 挂载 → 单盘直存 → aetherupload 分块上传原生 ingest → 控制台 API」这条完整链路，让 `aws-cli`/`rclone` 能增删查列、浏览器能上传、控制台能看见。

**Architecture:** 三个 crate 单向依赖 `oss-server → xl-s3 → xl-store`。`xl-store` 是不依赖任何框架的存储内核（M1 单盘、无纠删码，但提交协议与 `xl.meta` 格式按最终设计落）。`xl-s3` 把 s3s 的 `S3` trait 实现到内核上。`oss-server` 用 e-cat 组装一个 axum Router，同时挂 aetherupload 的四条路由、控制台 API 与 S3 的 fallback 服务。

**Tech Stack:** Rust 2024 edition · ecat 7.0.0 · s3s 0.17.0 · aetherupload-rust 1.0.3 · axum 0.8（由 ecat 传入）· sqlx 0.8（SQLite）· serde/rmp-serde · reed-solomon-erasure（M2 才用，M1 先不加）

**Spec:** `docs/superpowers/specs/2026-10-10-minio-clone-design.md` 的 §3 / §4 / §5 / §6 / M1 章节

---

## 执行前必读：四件事

1. **s3s 的 DTO 是代码生成的**（`src/dto/generated.rs`，6.2 万行）。所有字段都是 `Option<T>`，一律用 `..Default::default()` 收尾。本计划给出的字段名来自 0.17.0 的实际源码；若某个字段名编译不过，**以编译器为准**去 `generated.rs` 里核对，不要改设计。
2. **类型别名**：`BucketName = String`、`ObjectKey = String`、`ContentType = String`、`ContentLength = i64`、`ETag = String`、`LastModified = Timestamp`（`Timestamp: From<SystemTime>`）、`ObjectList = Vec<Object>`、`Buckets = Vec<Bucket>`。
3. **M1 的两处刻意简化**，都在代码里带 `ponytail:` 注释，M2 补：
   - S3 的 `PutObject` 把请求体整份读进内存（上限可配）。M2 换流式。
   - 单进程内的对象锁（`tokio::sync::Mutex` 的 HashMap）。M3 换 flock / quorum 锁。
4. **开工前的探针编译**：Task 1 完成后，把 Task 9–14 里的 s3s / aetherupload 代码片段先拼进一个临时 bin 做一次 `cargo check`（约 2 分钟），一次扫清剩余的 API 臆测。计划中已用 rustc 探针对 s3s 0.17.0 与 aetherupload 1.0.3 的真实签名核对过两批（builder 非链式、`HttpError` 无 `status_code()` 等），同类问题可能还有。

**M1 明确的安全边界（不要误解为已实现）**：S3 面**只有认证、没有授权** —— 任何有效 AK/SK 可访问所有桶。授权（桶策略/IAM）在 M4。控制台面走 JWT，有鉴权。

---

## 文件结构

```
oss/
├── Cargo.toml                                  # workspace 根
├── oss.toml                                    # 运行配置（示例）
├── crates/
│   ├── xl-store/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                          # 再导出
│   │       ├── error.rs                        # Error / Result
│   │       ├── layout.rs                       # 桶名/键校验 · __XLDIR__ 编码 · 盘路径
│   │       ├── meta.rs                         # xl.meta v1 编解码（msgpack + 版本字节）
│   │       └── store.rs                        # Store：桶/对象操作 · 提交协议 · 对象锁
│   ├── xl-s3/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                          # 再导出 OssS3 / SigV4Auth
│   │       ├── convert.rs                      # xl-store 类型 ↔ s3s DTO 的翻译 · 错误映射
│   │       ├── auth.rs                         # S3Auth 实现（AK → SK，查 access_keys 表）
│   │       └── service.rs                      # S3 trait 的全部 M1 操作
│   └── oss-server/
│       ├── Cargo.toml
│       ├── migrations/
│       │   └── 0001_init.sql                   # 控制面 5 张表
│       └── src/
│           ├── main.rs                         # e-cat App 组装与启动
│           ├── lib.rs                          # 模块导出（给集成测试用）
│           ├── config.rs                       # oss.toml + 环境变量
│           ├── state.rs                        # AppState
│           ├── db.rs                           # SqlxClient 连池 + schema 初始化 + 仓储函数
│           ├── router.rs                       # 一个 Router：控制台 + aetherupload + S3 fallback
│           ├── aether.rs                       # aetherupload Runtime · SqlInstantStore · IngestSink
│           └── console.rs                      # /api/v1 控制台 API
└── tests/
    └── e2e_s3.sh                               # aws-cli / rclone 端到端验收脚本
```

---

## Task 1: workspace 骨架 + 依赖锁版本

**Files:**
- Create: `Cargo.toml`, `crates/xl-store/Cargo.toml`, `crates/xl-s3/Cargo.toml`, `crates/oss-server/Cargo.toml`
- Create: `crates/*/src/lib.rs`（占位一行）

- [x] **Step 1: 写 workspace 根 `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/xl-store", "crates/xl-s3", "crates/oss-server"]

[workspace.package]
edition = "2024"
rust-version = "1.89"
license = "Apache-2.0"

[workspace.dependencies]
xl-store = { path = "crates/xl-store" }
xl-s3 = { path = "crates/xl-s3" }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
rmp-serde = "1"
uuid = { version = "1", features = ["v4", "serde"] }
md-5 = "0.10"
sha2 = "0.10"
hex = "0.4"
thiserror = "2"
bytes = "1"
futures = "0.3"
tokio = { version = "1", features = ["fs", "io-util", "sync", "rt", "rt-multi-thread", "macros", "signal"] }
tokio-util = { version = "0.7", features = ["io"] }
tracing = "0.1"

# 发布版依赖：绝不加 "0.8" 之外的 axum —— ecat 与 aetherupload 都锚定 axum 0.8
axum = { version = "0.8", default-features = false }
s3s = "0.17"
aetherupload-rust = { version = "1.0.3", features = ["ecat"] }
ecat = { version = "7.0.0", features = ["http", "middleware", "tracing"] }
sqlx = { version = "0.8", default-features = false, features = ["sqlite", "runtime-tokio", "macros", "migrate"] }
```

- [x] **Step 2: 写三个 crate 的 `Cargo.toml`**

`crates/xl-store/Cargo.toml`：

```toml
[package]
name = "xl-store"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
bytes.workspace = true
futures.workspace = true
hex.workspace = true
md-5.workspace = true
rmp-serde.workspace = true
serde.workspace = true
sha2.workspace = true
thiserror.workspace = true
tokio.workspace = true
tracing.workspace = true
uuid.workspace = true

[dev-dependencies]
tempfile = "3"
```

`crates/xl-s3/Cargo.toml`：

```toml
[package]
name = "xl-s3"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[dependencies]
xl-store.workspace = true
async-trait = "0.1"
bytes.workspace = true
futures.workspace = true
s3s.workspace = true
sqlx = { workspace = true }
tracing.workspace = true

[dev-dependencies]
md-5.workspace = true
hex.workspace = true
http = "1"
tempfile = "3"
tokio.workspace = true
```

`crates/oss-server/Cargo.toml`：

```toml
[package]
name = "oss-server"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true

[[bin]]
name = "oss-server"
path = "src/main.rs"

[dependencies]
xl-store.workspace = true
xl-s3.workspace = true
aetherupload-rust.workspace = true
axum.workspace = true
bytes.workspace = true
ecat.workspace = true
futures.workspace = true
jsonwebtoken = "9"
serde.workspace = true
serde_json.workspace = true
s3s.workspace = true
sha2.workspace = true
sqlx.workspace = true
tokio.workspace = true
toml = "0.9"
tracing.workspace = true

[dev-dependencies]
md-5.workspace = true
reqwest = { version = "0.12", features = ["json", "stream"] }
tempfile = "3"
tokio = { workspace = true, features = ["full"] }
```

- [x] **Step 3: 三个 `lib.rs` 各写一行**

`crates/xl-store/src/lib.rs`：
```rust
pub mod error;
pub mod layout;
pub mod meta;
pub mod store;
```

`crates/xl-s3/src/lib.rs`：
```rust
pub mod auth;
pub mod convert;
pub mod service;
```

`crates/oss-server/src/lib.rs`：
```rust
pub mod aether;
pub mod config;
pub mod console;
pub mod db;
pub mod router;
pub mod state;
```

对应地，创建后续任务要填充的空文件（空文件 `cargo check` 合法）（注意：0 字节文件会被 `cargo fmt --all --check` 判 diff —— 本步已顺带跑过 `cargo fmt --all`，占位文件均为 1 字节换行）：
`crates/xl-store/src/{error,layout,meta,store}.rs`、`crates/xl-s3/src/{auth,convert,service}.rs`、
`crates/oss-server/src/{aether,config,console,db,router,state}.rs`。

- [x] **Step 4: 验证编译 + axum 版本唯一**

Run:
```bash
cd /home/wwwroot/oss && cargo check --workspace 2>&1 | tail -5
cargo tree -d -p oss-server 2>/dev/null | grep -i "^axum" || echo "OK: axum 只有一份"
```
Expected: `cargo check` 成功；输出两行 axum——`axum v0.7.9`（来自 `ecat-errors → tonic 0.12` 的 gRPC 内部私有依赖，feature 关不掉，M1 用不到）与 `axum v0.8.9`（路由面：我们 + `ecat-transport-http` + aetherupload 统一）。**两份永不碰面，接受**；但若出现第三个 axum 版本、或 0.8.x 出现两份——停下来。

- [x] **Step 5: 提交**

```bash
git add Cargo.toml crates/ && git commit -m "chore: workspace 骨架，锁定 axum 0.8 单一版本"
```

---

## Task 2: xl-store —— 错误类型与盘布局

**Files:**
- Create: `crates/xl-store/src/error.rs`
- Create: `crates/xl-store/src/layout.rs`

- [x] **Step 1: 写失败的测试**（放在 `layout.rs` 底部 `#[cfg(test)]`）

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_dir_roundtrip() {
        for key in ["a.txt", "a/b/c.txt", "x/y", "deep/a/b/c/d.bin"] {
            let dir = object_dir_name(key).unwrap();
            assert_eq!(key_from_dir_name(&dir).unwrap(), key);
        }
    }

    #[test]
    fn object_dir_flattens_slashes() {
        assert_eq!(
            object_dir_name("a/b/c.txt").unwrap(),
            "a__XLDIR__b__XLDIR__c.txt"
        );
    }

    #[test]
    fn rejects_path_traversal() {
        for bad in [
            "../etc/passwd",
            "a/../../b",
            "/abs",
            "a//b",
            "a/",
            "",
            "a\u{0}b",
        ] {
            assert!(object_dir_name(bad).is_err(), "{bad} 应当被拒");
        }
        // 拼接路径时桶名参数同样收口校验
        assert!(object_dir(Path::new("/tmp/root"), "..", "k").is_err());
        assert!(object_dir(Path::new("/tmp/root"), "../../etc", "k").is_err());
    }

    #[test]
    fn rejects_keys_containing_separator() {
        // 键里出现 __XLDIR__ 会让往返编码错位，必须拒绝而不是静默损坏
        assert!(object_dir_name("a__XLDIR__b").is_err());
    }

    #[test]
    fn key_len_limit_is_bytes() {
        assert!(validate_key(&"汉".repeat(341)).is_ok());
        assert!(validate_key(&"汉".repeat(342)).is_err()); // 341×3=1023B ≤1024，342×3=1026B >1024
    }

    #[test]
    fn key_rejects_control_chars() {
        assert!(validate_key("a\u{7f}b").is_err());
        assert!(validate_key("a\u{80}b").is_err()); // DEL 与 C1
    }

    #[test]
    fn bucket_name_rules() {
        assert!(validate_bucket("my-bucket").is_ok());
        for bad in ["ab", "My-Bucket", "-lead", "trail-", "a..b", "a_b", ""] {
            assert!(validate_bucket(bad).is_err(), "{bad} 应当被拒");
        }
        // S3 规则：不得形如 IP 地址（规格 §2.1）
        assert!(validate_bucket("192.168.1.1").is_err());
        assert!(validate_bucket("1.2.3.4.5").is_ok()); // 五段不是 IPv4，合法
    }
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store layout 2>&1 | tail -20`
Expected: 编译错误（`object_dir_name` 未定义）

- [x] **Step 3: 实现 `error.rs`**

```rust
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bucket not found: {0}")]
    NoSuchBucket(String),
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),
    #[error("key not found: {0}")]
    NoSuchKey(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("corrupt metadata: {0}")]
    CorruptMeta(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

- [x] **Step 4: 实现 `layout.rs`**

```rust
//! 盘布局与寻址：对象名展平、桶名/键校验、盘上路径推导。
//!
//! 为什么展平：对象的全部内容（xl.meta + 版本目录）必须落在**同一个父目录**下，
//! 这样「提交」永远是一次同盘 rename，原子性不依赖任何锁，也不需要跨目录事务。

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// 对象名里 `/` 的展平替身。键里出现这个字面量会让往返编码错位，一律拒绝。
pub const SEP: &str = "__XLDIR__";
/// 系统目录：临时区、回收站、锁。
pub const SYS_DIR: &str = ".oss.sys";

const MAX_KEY_LEN: usize = 1024;

pub fn validate_bucket(name: &str) -> Result<()> {
    let ok_len = (3..=63).contains(&name.len());
    let ok_chars = name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.');
    let ok_edges = !name.starts_with(['-', '.']) && !name.ends_with(['-', '.']);
    let ok_no_dotdot = !name.contains("..");
    // 不得形如 IPv4 地址（规格 §2.1；S3 的既有规则，见 aws 文档 "must not be formatted as an IP address"）
    let parts: Vec<&str> = name.split('.').collect();
    let looks_like_ipv4 = parts.len() == 4
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.bytes().all(|b| b.is_ascii_digit())
                && p.parse::<u16>().is_ok_and(|n| n <= 255)
        });
    if looks_like_ipv4 {
        return Err(Error::InvalidBucketName(name.to_string()));
    }
    if ok_len && ok_chars && ok_edges && ok_no_dotdot {
        Ok(())
    } else {
        Err(Error::InvalidBucketName(name.to_string()))
    }
}

pub fn validate_key(key: &str) -> Result<()> {
    let reject = |why: &str| Err(Error::InvalidKey(format!("{key}: {why}")));

    if key.is_empty() {
        return reject("空键");
    }
    if key.len() > MAX_KEY_LEN {
        return reject("超过 1024 字节");
    }
    if key.contains(SEP) {
        return reject("包含保留分隔符");
    }
    if key.starts_with('/') {
        return reject("以 / 开头");
    }
    if key.contains("//") {
        return reject("含空路径段");
    }
    if key.split('/').any(|seg| seg == ".." || seg == ".") {
        return reject("含 . 或 .. 路径段");
    }
    if key.ends_with('/') {
        return reject("以 / 结尾");
    }
    if key.chars().any(|c| c.is_control() || c == '\\') {
        return reject("含控制字符或反斜杠");
    }
    Ok(())
}

pub fn object_dir_name(key: &str) -> Result<String> {
    validate_key(key)?;
    Ok(key.replace('/', SEP))
}

pub fn key_from_dir_name(dir: &str) -> Result<String> {
    Ok(dir.replace(SEP, "/"))
}

pub fn bucket_dir(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket)
}

/// 所有对象读写路径都汇合到这里，桶名校验在此收口；S3 入口虽由 s3s 的默认 AwsNameValidation 先拦，但控制台/aether 上传直达内核，不能依赖上游。
pub fn object_dir(root: &Path, bucket: &str, key: &str) -> Result<PathBuf> {
    validate_bucket(bucket)?;
    validate_key(key)?;
    Ok(bucket_dir(root, bucket).join(object_dir_name(key)?))
}

pub fn sys_dir(root: &Path, sub: &str) -> PathBuf {
    root.join(SYS_DIR).join(sub)
}

#[cfg(test)]
mod tests {
    // 见 Step 1
}
```

- [x] **Step 5: 跑测试确认通过**

Run: `cargo test -p xl-store layout 2>&1 | tail -12`
Expected: `test result: ok. 5 passed`

- [x] **Step 6: 提交**

```bash
git add crates/xl-store/src/error.rs crates/xl-store/src/layout.rs
git commit -m "feat(xl-store): 盘布局与键编码，拒目录穿越与保留分隔符"
```

---

## Task 3: xl-store —— `xl.meta` v1 编解码

**Files:**
- Create: `crates/xl-store/src/meta.rs`

- [x] **Step 1: 写失败的测试**

```rust
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
    fn golden_bytes_stable() {
        // 字段名与表示在 named 编码下就是磁盘契约：自读自写的 roundtrip 抓不到
        // 字段重命名或 Uuid 表示漂移（str vs bin8），黄金字节能。
        // 更新流程：确认改动是有意的 → cargo test -p xl-store golden -- --nocapture
        // 打印实际摘要 → 更新下常量。改格式 = 有意识地改，不是顺手改。
        use sha2::Digest;
        let bytes = sample().encode().unwrap();
        assert_eq!(bytes[0], FORMAT_VERSION);
        // 已人工核对：首字节 01；version_id 为 bin8（c4 10 + 16 字节全零）；
        // mod_time_nanos 为 uint64（cf）；顶层/嵌套均为字段名 map。
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        println!("{digest}");
        assert_eq!(
            digest,
            "da898c71e7d91dbc23826a6e84269cd2962a530ff364a9fc3c59eccc7ea2821f"
        );
    }
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store meta 2>&1 | tail -20`
Expected: 编译错误（`XlMeta` 未定义）

- [x] **Step 3: 实现 `meta.rs`**

```rust
//! xl.meta v1 —— 每个对象目录一份，是**唯一的提交点**。
//!
//! 编码：`[1 字节格式版本][msgpack 载荷]`。版本字节是前向兼容的锚点：
//! 后续阶段（版本控制/加密/对象锁/复制）只往 `Version` 里加 `Option` 字段，
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        match bytes.split_first() {
            Some((&FORMAT_VERSION, rest)) => {
                rmp_serde::from_slice(rest).map_err(|e| Error::CorruptMeta(e.to_string()))
            }
            Some((&v, _)) => Err(Error::CorruptMeta(format!("不支持的格式版本 {v}"))),
            None => Err(Error::CorruptMeta("空文件".into())),
        }
    }

    /// M1：取唯一的一条。M4 起改为取 is_latest（格式已在 §5.1 预留该字段）。
    pub fn latest(&self) -> Option<&Version> {
        self.versions.last()
    }
}

#[cfg(test)]
mod tests {
    // 见 Step 1
}
```

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p xl-store meta 2>&1 | tail -12`
Expected: `test result: ok. 6 passed`

- [x] **Step 5: 提交**

```bash
git add crates/xl-store/src/meta.rs
git commit -m "feat(xl-store): xl.meta v1 格式，版本字节 + 前向兼容解码"
```

---

## Task 4: xl-store —— `Store` 打开与桶操作

**Files:**
- Create: `crates/xl-store/src/store.rs`

- [x] **Step 1: 写失败的测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
        (s, dir)
    }

    #[tokio::test]
    async fn open_creates_sys_dirs() {
        let (_, dir) = store().await;
        for sub in ["tmp", "trash", "locks"] {
            assert!(dir.path().join(layout::SYS_DIR).join(sub).is_dir());
        }
    }

    #[tokio::test]
    async fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        Store::open(StoreConfig::new(dir.path())).await.unwrap();
        Store::open(StoreConfig::new(dir.path())).await.unwrap();
    }

    #[tokio::test]
    async fn create_and_list_buckets() {
        let (s, _d) = store().await;
        // 倒序创建：断言要证明的是「按名字排序」，不是创建顺序
        s.create_bucket("beta").await.unwrap();
        s.create_bucket("alpha").await.unwrap();
        let names: Vec<String> = s
            .list_buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[tokio::test]
    async fn list_buckets_missing_root_is_empty() {
        // 盘根整个消失（挂载点没了/被带外清掉）：列举返回空，不报错
        let (s, dir) = store().await;
        std::fs::remove_dir_all(dir.path()).unwrap();
        assert!(s.list_buckets().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_bucket_is_idempotent_error() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        assert!(matches!(
            s.create_bucket("alpha").await,
            Err(Error::BucketAlreadyExists(_))
        ));
    }

    #[tokio::test]
    async fn create_bucket_rejects_bad_name() {
        let (s, _d) = store().await;
        assert!(matches!(
            s.create_bucket("A_B").await,
            Err(Error::InvalidBucketName(_))
        ));
    }

    #[tokio::test]
    async fn delete_bucket_requires_empty() {
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        // Task 5 前没有 put_object：直接摆一个对象目录（含内容）来制造「非空」。
        let obj_dir = layout::object_dir(d.path(), "alpha", "k.txt").unwrap();
        tokio::fs::create_dir_all(&obj_dir).await.unwrap();
        tokio::fs::write(obj_dir.join("part.1"), b"hi")
            .await
            .unwrap();
        assert!(matches!(
            s.delete_bucket("alpha").await,
            Err(Error::BucketNotEmpty(_))
        ));
        tokio::fs::remove_dir_all(&obj_dir).await.unwrap();
        s.delete_bucket("alpha").await.unwrap();
        assert!(!s.bucket_exists("alpha").await.unwrap());
    }

    #[tokio::test]
    async fn missing_bucket_is_reported() {
        let (s, _d) = store().await;
        // put_object 属 Task 5；ensure_bucket 是同一前置，先用 delete_bucket 验它。
        assert!(matches!(
            s.delete_bucket("nope").await,
            Err(Error::NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn ensure_bucket_rejects_traversal() {
        // ensure_bucket 内的桶名校验是删桶/列举的收口点：`..` 若放行，delete_bucket 会去删盘根之外
        let (s, _d) = store().await;
        assert!(matches!(
            s.delete_bucket("..").await,
            Err(Error::InvalidBucketName(_))
        ));
        assert!(matches!(
            s.delete_bucket("A_B").await,
            Err(Error::InvalidBucketName(_))
        ));
    }
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store store 2>&1 | tail -20`
Expected: 编译错误（`Store` 未定义）

- [x] **Step 3: 实现 `store.rs` 的骨架与桶操作**

```rust
//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::Stream;

use crate::error::{Error, Result};
use crate::layout;

pub type ByteStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub root: PathBuf,
    /// 提交点是否 fsync。测试可关（快），生产必须开。
    pub fsync: bool,
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fsync: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BucketInfo {
    pub name: String,
    pub creation_nanos: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub last_modified_nanos: i64,
    pub user_metadata: std::collections::BTreeMap<String, String>,
}

pub struct Store {
    cfg: StoreConfig,
}

impl Store {
    pub async fn open(cfg: StoreConfig) -> Result<Self> {
        for sub in ["tmp", "trash", "locks"] {
            tokio::fs::create_dir_all(layout::sys_dir(&cfg.root, sub)).await?;
        }
        Ok(Self { cfg })
    }

    pub fn root(&self) -> &Path {
        &self.cfg.root
    }

    /// 桶操作的统一前置。名字校验在此收口：桶目录是 `root.join(bucket)`，
    /// 不过滤 `..`/绝对路径就会让删桶、列举跑到盘根之外。
    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        if tokio::fs::try_exists(layout::bucket_dir(&self.cfg.root, bucket)).await? {
            Ok(())
        } else {
            Err(Error::NoSuchBucket(bucket.to_string()))
        }
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        let dir = layout::bucket_dir(&self.cfg.root, bucket);
        match tokio::fs::create_dir(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(Error::BucketAlreadyExists(bucket.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        layout::validate_bucket(bucket)?;
        Ok(tokio::fs::try_exists(layout::bucket_dir(&self.cfg.root, bucket)).await?)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::bucket_dir(&self.cfg.root, bucket);
        let mut entries = tokio::fs::read_dir(&dir).await?;
        let non_empty = entries.next_entry().await?.is_some();
        drop(entries);
        // 桶里只要还有对象目录就拒绝（S3 语义）
        if non_empty {
            return Err(Error::BucketNotEmpty(bucket.to_string()));
        }
        // ponytail: check-then-remove 窗口在 Task 5 加锁后收窄；M1 单进程下仅 tokio 任务间竞态
        match tokio::fs::remove_dir(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                Err(Error::BucketNotEmpty(bucket.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut out = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.cfg.root).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == layout::SYS_DIR {
                continue;
            }
            let nanos = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.created().or_else(|_| m.modified()).ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            out.push(BucketInfo {
                name,
                creation_nanos: nanos,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}
```

注（与骨架的差异，已裁决）：`locks` 字段 / `lock_for` 方法推迟到 Task 5 首次有调用者（`put_object_stream`）时加入——Task 4 无调用者，dead_code 会与 0 告警闸冲突。

`Cargo.toml` 无需改动：`tempfile` 已在 `[dev-dependencies]`，`#[tokio::test]` 与 `tokio::fs` 由 `[dependencies]` 里 tokio 已有的 features（macros / rt / fs）覆盖。

注（Minor 2，记录待 M2）：桶路径上是普通文件（非目录）时，`ensure_bucket` 会把它当作已存在的桶；M2 处理，本任务不动。

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-store store 2>&1 | tail -14`
Expected: 全绿——`store` 模块 `test result: ok. 9 passed`（`cargo test -p xl-store` 合计 26 passed）。原预期「两个测试失败」在 Rust 里不可达：测试引用的 `put_object`/`delete_object` 在 Task 5/6 才实现，缺失时是整个测试模块编译不过；已适配为不依赖它们的形态（见 Step 1）。

- [x] **Step 5: 提交**

```bash
git add crates/xl-store/ && git commit -m "feat(xl-store): Store 打开与桶操作"
```

---

## Task 5: xl-store —— `put_object` 提交协议

这是整个内核最关键的一个函数。提交顺序不能变。

**Files:**
- Modify: `crates/xl-store/src/store.rs`

- [x] **Step 1: 写失败的测试**

```rust
// >>> 追加到 store.rs 的 tests 模块末尾（Task 5 段落，逐字取自最终实现）：
    // ---- Task 5: put_object 提交协议 ----

    #[tokio::test]
    async fn put_then_head() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let info = s
            .put_object(
                "alpha",
                "dir/hello.txt",
                bytes::Bytes::from_static(b"hello world"),
                Some("text/plain".into()),
            )
            .await
            .unwrap();
        assert_eq!(info.size, 11);
        // 单段 PUT 的 ETag 就是整份 MD5
        assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        assert_eq!(info.content_type.as_deref(), Some("text/plain"));

        let head = s.head_object("alpha", "dir/hello.txt").await.unwrap();
        assert_eq!(head.etag, info.etag);
    }

    #[tokio::test]
    async fn put_with_fsync_disabled_still_commits() {
        // cfg.fsync=false 是提交路径上的另一条分支（测试快、生产必须开），必须同样可提交可读
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = StoreConfig::new(dir.path());
        cfg.fsync = false;
        let s = Store::open(cfg).await.unwrap();
        s.create_bucket("alpha").await.unwrap();
        let info = s
            .put_object("alpha", "k", Bytes::from_static(b"no-fsync"), None)
            .await
            .unwrap();
        assert_eq!(info.size, 8);
        assert_eq!(s.head_object("alpha", "k").await.unwrap().size, 8);
    }

    #[tokio::test]
    async fn put_streams_without_buffering_whole_body() {
        // 3 个 1KB 的块，验证流式写与增量哈希
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let chunks: Vec<Result<Bytes>> = vec![
            Ok(Bytes::from(vec![b'a'; 1024])),
            Ok(Bytes::from(vec![b'a'; 1024])),
            Ok(Bytes::from(vec![b'a'; 1024])),
        ];
        let stream = futures::stream::iter(chunks);
        let info = s
            .put_object_stream("alpha", "big.bin", Box::pin(stream), None)
            .await
            .unwrap();
        assert_eq!(info.size, 3072);
    }

    #[tokio::test]
    async fn overwrite_is_atomic_and_old_version_gone() {
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"one"), None)
            .await
            .unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"two"), None)
            .await
            .unwrap();

        let info = s.head_object("alpha", "k").await.unwrap();
        assert_eq!(info.size, 3);

        // 对象目录下应当只剩一个新版本目录 + xl.meta（老版本进了 trash）
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        let mut names: Vec<String> = Vec::new();
        let mut entries = tokio::fs::read_dir(&obj_dir).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            names.push(e.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        assert_eq!(
            names.len(),
            2,
            "对象目录下应只有 xl.meta 与一个新的版本目录: {names:?}"
        );
        assert!(names.iter().any(|n| n == "xl.meta"));
    }

    #[tokio::test]
    async fn crash_between_steps_leaves_no_visible_object() {
        // 模拟「tmp 里写了但没提交」：手造一个 tmp 孤儿，断言对象仍不可见
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let orphan = layout::sys_dir(d.path(), "tmp").join("orphan-uuid");
        tokio::fs::create_dir_all(&orphan).await.unwrap();
        tokio::fs::write(orphan.join("part.1"), b"junk")
            .await
            .unwrap();
        assert_eq!(
            s.head_object("alpha", "ghost")
                .await
                .unwrap_err()
                .to_string(),
            "key not found: ghost"
        );
    }

    #[tokio::test]
    async fn concurrent_puts_to_same_key_serialize() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let s = std::sync::Arc::new(s);
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let s = std::sync::Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put_object("alpha", "same", bytes::Bytes::from(vec![i; 64]), None)
                    .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        // 必须能读出一份完整的、单一写入者的内容（不能是交错损坏）
        let info = s.head_object("alpha", "same").await.unwrap();
        assert_eq!(info.size, 64);
    }
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store put 2>&1 | tail -20`
Expected: 编译错误（`put_object` 未实现）
实际：`error: could not compile`（15 处 E0422/E0433/E0599：`put_object` / `put_object_stream` / `head_object` / `XlMeta` 均未定义）。

- [x] **Step 3: 实现 `put_object`**

```rust
// >>> 模块顶部 import 区最终形态（含既有行）：
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::Stream;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, Version, XlMeta};

// >>> `Store` 结构体新增字段：
pub struct Store {
    cfg: StoreConfig,
    /// 进程内 per-object 互斥，`bucket/key` → 锁：同键的并发写必须串行，
    /// 否则两个写者会把对方的版本目录当「老版本」扔进回收站。
    /// ponytail: 单进程锁；跨进程用 `.oss.sys/locks/<sha1(key)>.lock` 的 flock（设计 §4.4，M2/M3）
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

// >>> `open` 初始化锁表：
    pub async fn open(cfg: StoreConfig) -> Result<Self> {
        for sub in ["tmp", "trash", "locks"] {
            tokio::fs::create_dir_all(layout::sys_dir(&cfg.root, sub)).await?;
        }
        Ok(Self {
            cfg,
            locks: Mutex::new(HashMap::new()),
        })
    }

// >>> `impl Store` 内 `root()` 之后：
    fn lock_for(&self, bucket: &str, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let id = format!("{bucket}/{key}");
        let mut locks = self.locks.lock().unwrap();
        let lock = locks
            .entry(id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        // 回收无人持有的旧条目，锁表不随历史写过的键无限增长
        locks.retain(|k, v| k == &id || Arc::strong_count(v) > 1);
        lock
    }

// >>> `impl Store` 内 `list_buckets` 之后（提交协议 + 读探针；`now_nanos` 在模块级）：
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream(
            bucket,
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
            content_type,
        )
        .await
    }

    /// 提交协议（顺序不可变）：
    ///   1. 分片/数据写 `.oss.sys/tmp/<uuid>/part.1`，边写边算 MD5+SHA256+size
    ///   2. 候选 `xl.meta` 写进同一个 tmp 目录
    ///   3. **同盘 rename** tmp/<uuid> → <bucket>/<obj>/<uuid>     ← 数据就位
    ///   4. 写 `<obj>/xl.meta.new` 并 fsync，再 rename 覆盖 `xl.meta` ← **提交点**
    ///   5. 老版本目录 → `.oss.sys/trash/`（异步删）
    ///
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，启动自检回收）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，clean 回收）。读路径只信 xl.meta。
    pub async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        mut body: ByteStream,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;

        let _guard = self.lock_for(bucket, key).lock_owned().await;

        let old_meta = self.read_meta(bucket, key).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(&self.cfg.root, "tmp").join(&uuid);
        tokio::fs::create_dir_all(&tmp).await?;

        // 1) 流式写数据 + 增量哈希
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

        let etag = hex::encode(md5.finalize());
        let version = Version {
            version_id: uuid::Uuid::nil(), // M1 无版本控制：nil = S3 的 "null" 版本
            mod_time_nanos: now_nanos(),
            size,
            etag: etag.clone(),
            content_type: content_type.clone(),
            user_metadata: Default::default(),
            part: PartRef {
                uuid: uuid.clone(),
                part: 1,
                size,
                sha256: hex::encode(sha.finalize()),
            },
        };
        let meta = XlMeta {
            object_key: key.to_string(),
            versions: vec![version],
        };

        // 2) 候选 meta 落 tmp
        tokio::fs::write(tmp.join(meta::META_FILE), meta.encode()?).await?;

        // 3) 同盘 rename：数据就位（此时对读路径仍不可见）
        let obj_dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        tokio::fs::create_dir_all(&obj_dir).await?;
        tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;

        // 4) 提交点：写 xl.meta.new 再原子覆盖
        self.write_meta(bucket, key, &meta).await?;

        // 5) 老版本进回收站
        if let Some(old) = old_meta.as_ref().and_then(|m| m.latest()) {
            let old_dir = obj_dir.join(&old.part.uuid);
            let trash = layout::sys_dir(&self.cfg.root, "trash").join(&old.part.uuid);
            if tokio::fs::try_exists(&trash).await? {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now_nanos(),
            user_metadata: Default::default(),
        })
    }

    async fn read_meta(&self, bucket: &str, key: &str) -> Result<Option<XlMeta>> {
        let path = layout::object_dir(&self.cfg.root, bucket, key)?.join(meta::META_FILE);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let m = XlMeta::decode(&bytes)?;
                // 键一致性：目录名与 xl.meta 记录的键不符 = 元数据错位，宁可报损坏也不服务错误内容
                if m.object_key != key {
                    return Err(Error::CorruptMeta(format!(
                        "键不一致：请求 {key:?}，xl.meta 中为 {:?}",
                        m.object_key
                    )));
                }
                Ok(Some(m))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_meta(&self, bucket: &str, key: &str, m: &XlMeta) -> Result<()> {
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        let new_path = dir.join(meta::META_NEW_FILE);
        let mut f = tokio::fs::File::create(&new_path).await?;
        f.write_all(&m.encode()?).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
        }
        drop(f);
        tokio::fs::rename(&new_path, dir.join(meta::META_FILE)).await?;
        Ok(())
    }

    // Task 5 的测试要一个「提交后是否可见」的探针，head_object 与其组装函数随本 task 落地；
    // get / delete / list / copy 仍留给 Task 6。
    fn load_object_info(&self, bucket: &str, key: &str, m: &XlMeta) -> Result<ObjectInfo> {
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
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
```

`md-5` 的 crate 名是 `md-5`，导入路径是 `md5`（`use md5::{Digest as _, Md5}`）。

注（与最初骨架的差异，已裁决；下方代码块已同步为最终实现形态）：
- `locks` 字段 / `lock_for` 方法随本 task 落地（Task 4 推迟至此，首个调用者是 `put_object_stream`）。形态：`Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>`，加锁键为 `bucket/key`；`lock_for` 在取锁时 `retain` 回收无外部持有者的旧条目，锁表不随历史写过的键无限增长。
- `head_object` 与 `load_object_info` 也随本 task 落地：Step 1 的 `put_then_head` / `crash_between_steps_leaves_no_visible_object` 需要一个「提交后是否可见」的探针，缺它们整个测试模块编译不过。**Task 6 的 Step 3 骨架里已含这两个函数，实现 Task 6 时不要重复添加**（get / delete / list / copy 仍留给 Task 6）。
- `read_meta` 增加键一致性校验（Task 3 质量评审 Minor 3）：解码出的 `object_key` 与请求的 key 不符 → `Error::CorruptMeta`（信息含两个 key），测试 `read_meta_rejects_mismatched_object_key`。
- 补 `put_with_fsync_disabled_still_commits`：`cfg.fsync=false` 是提交路径上的另一条分支，必须同样可提交可读。

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-store 2>&1 | tail -20`
Expected（已更新为实际）：`xl-store` 34 passed / 0 failed（Task 4 的 26 + 本 task 的 8）；四闸全绿（fmt / clippy 0 告警 / `-p xl-store` / `--workspace -j 2`）。Step 2 的失败形态是编译错误（`put_object` / `put_object_stream` / `head_object` / `XlMeta` 未定义），与 Task 4 Step 4 注记的「模块级编译不过」同因。

- [x] **Step 5: 加一个「断电」验证：在提交点之前中断**

```rust
// >>> 追加到 tests 模块（Task 5 段落续）：「断电」与 read_meta 一致性测试：
    #[tokio::test]
    async fn part_written_but_not_committed_is_invisible() {
        // 直接把一个完整版本目录摆进对象目录、但不写 xl.meta，
        // 读路径必须报 NoSuchKey（证明「只有 xl.meta 是提交点」）
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        tokio::fs::create_dir_all(obj_dir.join("dangling-uuid"))
            .await
            .unwrap();
        tokio::fs::write(obj_dir.join("dangling-uuid").join("part.1"), b"data")
            .await
            .unwrap();
        assert!(matches!(
            s.head_object("alpha", "k").await,
            Err(Error::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn read_meta_rejects_mismatched_object_key() {
        // 内容可解码、但 object_key 与目录名不符的 xl.meta：读路径必须报 CorruptMeta，
        // 而不是把「张冠李戴」的元数据当成请求的键（错位会静默服务错误内容）
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        tokio::fs::create_dir_all(&obj_dir).await.unwrap();
        let m = XlMeta {
            object_key: "other".into(),
            versions: vec![],
        };
        tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
            .await
            .unwrap();

        let err = s.head_object("alpha", "k").await.unwrap_err();
        assert!(matches!(err, Error::CorruptMeta(_)));
        let msg = err.to_string();
        assert!(msg.contains("\"k\"") && msg.contains("\"other\""), "{msg}");
    }
```

Run: `cargo test -p xl-store 2>&1 | tail -6`
Expected: 通过
实际：通过。

- [x] **Step 6: 提交**

```bash
git add crates/xl-store/ && git commit -m "feat(xl-store): put_object 提交协议（tmp → rename → xl.meta 覆盖）"
```

---

## Task 6: xl-store —— 读 / 删 / 列举 / 复制

**Files:**
- Modify: `crates/xl-store/src/store.rs`

- [ ] **Step 1: 写失败的测试**

```rust
    #[tokio::test]
    async fn get_reads_back_exact_bytes() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"payload"), None).await.unwrap();
        let (info, bytes) = s.get_object("alpha", "k").await.unwrap();
        assert_eq!(bytes, bytes::Bytes::from_static(b"payload"));
        assert_eq!(info.etag, {
            let mut h = md5::Md5::new();
            md5::Digest::update(&mut h, b"payload");
            hex::encode(md5::Digest::finalize(h))
        });
    }

    #[tokio::test]
    async fn get_missing_key() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        assert!(matches!(s.get_object("alpha", "nope").await, Err(Error::NoSuchKey(_))));
    }

    #[tokio::test]
    async fn list_with_prefix_and_delimiter() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        for k in ["a.txt", "docs/1.txt", "docs/2.txt", "docs/deep/3.txt", "z.txt"] {
            s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None).await.unwrap();
        }

        let all = s.list_objects("alpha", &ListRequest::default()).await.unwrap();
        assert_eq!(all.objects.len(), 5);
        assert_eq!(all.objects[0].key, "a.txt"); // 字典序
        assert!(!all.is_truncated);

        let prefixed = s
            .list_objects("alpha", &ListRequest { prefix: Some("docs/".into()), delimiter: Some("/".into()), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(prefixed.objects.len(), 2);
        assert_eq!(prefixed.common_prefixes, vec!["docs/deep/".to_string()]);
    }

    #[tokio::test]
    async fn list_pagination() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        for i in 0..5 {
            s.put_object("alpha", &format!("k{i}"), bytes::Bytes::from_static(b"x"), None).await.unwrap();
        }
        let page1 = s.list_objects("alpha", &ListRequest { max_keys: Some(2), ..Default::default() }).await.unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_token.clone().unwrap();

        let page2 = s
            .list_objects("alpha", &ListRequest { max_keys: Some(2), continuation_token: Some(token), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(page2.objects[0].key, "k2");
    }

    #[tokio::test]
    async fn copy_object_duplicates_content() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "src", bytes::Bytes::from_static(b"copy me"), Some("text/plain".into())).await.unwrap();
        let info = s.copy_object("alpha", "src", "alpha", "dst").await.unwrap();
        assert_eq!(info.size, 7);
        let (_, bytes) = s.get_object("alpha", "dst").await.unwrap();
        assert_eq!(bytes, bytes::Bytes::from_static(b"copy me"));
    }
```

注（可编译前提已就绪）：本块用到的 `put_object` / `head_object` 已随 Task 5 落地，可直接编译；本 Step 新引入的是 `get_object` / `list_objects` / `copy_object` / `ListRequest`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store 2>&1 | tail -20`
Expected: 编译错误（`get_object` / `list_objects` / `copy_object` / `ListRequest` 未定义）

- [ ] **Step 3: 实现读 / 删 / 列举 / 复制**

注（勿重复添加）：本块里的 `load_object_info` 与 `head_object` 已在 Task 5 落地（Task 5 的测试需要「提交后是否可见」的探针），实现 Task 6 时**跳过这两个函数**；其余（`ListRequest` / `ListResult` / `get_object` / `delete_object` / `list_objects` / `copy_object`）照常实现。

```rust
#[derive(Debug, Clone, Default)]
pub struct ListRequest {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<usize>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_token: Option<String>,
}

const DEFAULT_MAX_KEYS: usize = 1000;

impl Store {
    fn load_object_info(&self, bucket: &str, key: &str, m: &XlMeta) -> Result<ObjectInfo> {
        let v = m.latest().ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: v.size,
            etag: v.etag.clone(),
            content_type: v.content_type.clone(),
            last_modified_nanos: v.mod_time_nanos,
            user_metadata: v.user_metadata.clone(),
        })
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self.read_meta(bucket, key).await?.ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        self.load_object_info(bucket, key, &m)
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        let info = self.head_object(bucket, key).await?;
        // ponytail: M1 整份读进内存；M2 换流式（大对象与 Range 是 M2 的事）。
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        let v = self.read_meta(bucket, key).await?.unwrap().latest().unwrap().clone();
        let bytes = tokio::fs::read(dir.join(&v.part.uuid).join(format!("part.{}", v.part.part))).await?;
        Ok((info, Bytes::from(bytes)))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_for(bucket, key).lock_owned().await;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // S3 语义：删不存在的键也返回成功
            Err(e) => Err(e.into()),
        }
    }

    /// 列举：直接走盘。M1 每个对象读一次 xl.meta（O(n) 次系统调用），
    /// M4 用 scrub 的命名空间索引优化（ponytail: 先对，再快）。
    pub async fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        self.ensure_bucket(bucket).await?;
        let mut keys = self.collect_keys(bucket).await?;
        keys.sort();

        let prefix = req.prefix.clone().unwrap_or_default();
        let max = req.max_keys.unwrap_or(DEFAULT_MAX_KEYS).min(1000);
        let start_after = req.continuation_token.clone();

        let mut objects = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut is_truncated = false;
        let mut next_token = None;

        for k in keys {
            if !k.starts_with(&prefix) {
                continue;
            }
            if let Some(after) = &start_after {
                // token = 本页「未返回的第一个 key」，恢复时包含该 key 本身
                if &k < after {
                    continue;
                }
            }
            if let Some(d) = &req.delimiter {
                let rest = &k[prefix.len()..];
                if let Some(pos) = rest.find(d.as_str()) {
                    let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
                    if objects.len() + common_prefixes.len() >= max {
                        is_truncated = true;
                        next_token = Some(k.clone());
                        break;
                    }
                    if !common_prefixes.contains(&cp) {
                        common_prefixes.push(cp);
                    }
                    continue;
                }
            }
            if objects.len() + common_prefixes.len() >= max {
                is_truncated = true;
                next_token = Some(k.clone());
                break;
            }
            if let Some(m) = self.read_meta(bucket, &k).await? {
                objects.push(self.load_object_info(bucket, &k, &m)?);
            }
        }
        common_prefixes.sort();
        Ok(ListResult { objects, common_prefixes, is_truncated, next_token })
    }

    async fn collect_keys(&self, bucket: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut entries = tokio::fs::read_dir(layout::bucket_dir(&self.cfg.root, bucket)).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            keys.push(layout::key_from_dir_name(&name)?);
        }
        Ok(keys)
    }

    pub async fn copy_object(&self, src_bucket: &str, src_key: &str, dst_bucket: &str, dst_key: &str) -> Result<ObjectInfo> {
        let (info, bytes) = self.get_object(src_bucket, src_key).await?;
        self.put_object(dst_bucket, dst_key, bytes, info.content_type).await
    }
}
```

- [ ] **Step 4: 跑测试**

Run: `cargo test -p xl-store 2>&1 | tail -12`
Expected: 全绿

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/ && git commit -m "feat(xl-store): 读/删/列举（prefix+delimiter+分页）/复制"
```

---

## Task 7: oss-server —— 控制面 schema 与仓储

**Files:**
- Create: `crates/oss-server/migrations/0001_init.sql`
- Create: `crates/oss-server/src/db.rs`

- [ ] **Step 1: 写 schema**

```sql
-- M1 五张表。对象数据永远不进 SQL —— 这里只有控制面状态。
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS access_keys (
    access_key  TEXT PRIMARY KEY,
    secret_key  TEXT NOT NULL,           -- M1 明文存；M5 随 SSE 一起改为加密存
    user_id     INTEGER NOT NULL REFERENCES users(id),
    status      TEXT NOT NULL DEFAULT 'active',
    expires_at  INTEGER,                 -- NULL = 永不过期；M5 的 STS 会话用同一列
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name          TEXT PRIMARY KEY,
    owner_id      INTEGER REFERENCES users(id),
    creation_date INTEGER NOT NULL,
    versioning    TEXT NOT NULL DEFAULT 'Off',  -- M4 用
    object_lock   INTEGER NOT NULL DEFAULT 0    -- M5 用
);

CREATE TABLE IF NOT EXISTS uploads (
    saved_path    TEXT PRIMARY KEY,      -- aetherupload 的三段式寻址
    bucket        TEXT NOT NULL,
    key           TEXT NOT NULL,
    group_name    TEXT NOT NULL,
    size          INTEGER NOT NULL,
    etag          TEXT NOT NULL,
    original_name TEXT,                  -- 前端 register 时填
    owner_id      INTEGER REFERENCES users(id),  -- 同上
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS instant_index (
    k          TEXT PRIMARY KEY,
    v          TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
```

- [ ] **Step 2: 写失败的测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> Db {
        Db::connect("sqlite::memory:").await.unwrap()
    }

    #[tokio::test]
    async fn init_creates_tables() {
        let db = db().await;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('users','access_keys','buckets','uploads','instant_index')")
            .fetch_one(db.pool()).await.unwrap();
        assert_eq!(n, 5);
    }

    #[tokio::test]
    async fn user_and_access_key_roundtrip() {
        let db = db().await;
        let uid = db.create_user("erik", "secret", true).await.unwrap();
        db.create_access_key("AKIA_TEST", "SK_TEST", uid).await.unwrap();
        let sk = db.secret_for_access_key("AKIA_TEST").await.unwrap();
        assert_eq!(sk.as_deref(), Some("SK_TEST"));
        assert_eq!(db.secret_for_access_key("NOPE").await.unwrap(), None);
    }

    #[tokio::test]
    async fn instant_index_expires() {
        let db = db().await;
        db.instant_set("k", "v", 3600).await.unwrap();
        assert_eq!(db.instant_get("k").await.unwrap().as_deref(), Some("v"));
        db.instant_set("expired", "v", 0).await.unwrap();
        assert_eq!(db.instant_get("expired").await.unwrap(), None);
    }

    #[tokio::test]
    async fn upload_records() {
        let db = db().await;
        db.record_upload("g_202610_abc.txt", "files", "abc.txt", "file", 7, "etag1").await.unwrap();
        db.register_upload("g_202610_abc.txt", "my.txt", None).await.unwrap();
        let rows = db.list_uploads(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].original_name.as_deref(), Some("my.txt"));
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test -p oss-server db 2>&1 | tail -20`
Expected: 编译错误（`Db` 未定义）

- [ ] **Step 4: 实现 `db.rs`**

```rust
//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;
use sqlx::{Row, SqlitePool};

pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub struct Db {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UploadRecord {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub original_name: Option<String>,
    pub created_at: i64,
}

const SCHEMA: &str = include_str!("../migrations/0001_init.sql");

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect(url).await?;
        // sqlx::raw_sql 支持一个字符串里的多条语句，schema 文件原样执行
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn create_user(&self, username: &str, password: &str, is_admin: bool) -> Result<i64> {
        let hash = crate::db::hash_password(password);
        let id = sqlx::query("INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)")
            .bind(username)
            .bind(hash)
            .bind(is_admin as i64)
            .bind(now())
            .execute(&self.pool)
            .await?
            .last_insert_rowid();
        Ok(id)
    }

    pub async fn user_exists(&self, username: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    pub async fn verify_user(&self, username: &str, password: &str) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT id, password_hash FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| {
            let hash: String = r.get("password_hash");
            (crate::db::hash_password(password) == hash).then(|| r.get::<i64, _>("id"))
        }))
    }

    pub async fn create_access_key(&self, ak: &str, sk: &str, user_id: i64) -> Result<()> {
        sqlx::query("INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES (?, ?, ?, ?)")
            .bind(ak)
            .bind(sk)
            .bind(user_id)
            .bind(now())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// SigV4 校验用：AK → SK。过期或非 active 一律当作不存在（返回 None）。
    pub async fn secret_for_access_key(&self, ak: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT secret_key FROM access_keys \
             WHERE access_key = ? AND status = 'active' AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(ak)
        .bind(now())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("secret_key")))
    }

    pub async fn list_access_keys(&self, user_id: i64) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT access_key FROM access_keys WHERE user_id = ? ORDER BY created_at")
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.get("access_key")).collect())
    }

    pub async fn insert_bucket(&self, name: &str, owner_id: Option<i64>) -> Result<()> {
        sqlx::query("INSERT OR IGNORE INTO buckets (name, owner_id, creation_date) VALUES (?, ?, ?)")
            .bind(name)
            .bind(owner_id)
            .bind(now())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_bucket_row(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM buckets WHERE name = ?").bind(name).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_bucket_rows(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT name FROM buckets ORDER BY name").fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    pub async fn instant_get(&self, k: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT v FROM instant_index WHERE k = ? AND expires_at > ?")
            .bind(k)
            .bind(now())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("v")))
    }

    pub async fn instant_set(&self, k: &str, v: &str, ttl_seconds: u64) -> Result<()> {
        // ttl = 0 视为「立刻过期」，与 aetherupload 的 SETEX 语义一致
        let expires = now() + ttl_seconds as i64;
        sqlx::query("INSERT INTO instant_index (k, v, expires_at) VALUES (?, ?, ?) \
                     ON CONFLICT(k) DO UPDATE SET v = excluded.v, expires_at = excluded.expires_at")
            .bind(k)
            .bind(v)
            .bind(expires)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn instant_del(&self, k: &str) -> Result<()> {
        sqlx::query("DELETE FROM instant_index WHERE k = ?").bind(k).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn record_upload(&self, saved_path: &str, bucket: &str, key: &str, group: &str, size: i64, etag: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(saved_path) DO NOTHING",
        )
        .bind(saved_path)
        .bind(bucket)
        .bind(key)
        .bind(group)
        .bind(size)
        .bind(etag)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn register_upload(&self, saved_path: &str, original_name: &str, owner_id: Option<i64>) -> Result<()> {
        sqlx::query("UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = ?")
            .bind(original_name)
            .bind(owner_id)
            .bind(saved_path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_uploads(&self, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = sqlx::query("SELECT * FROM uploads ORDER BY created_at DESC LIMIT ?")
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| UploadRecord {
                saved_path: r.get("saved_path"),
                bucket: r.get("bucket"),
                key: r.get("key"),
                group_name: r.get("group_name"),
                size: r.get("size"),
                etag: r.get("etag"),
                original_name: r.get("original_name"),
                created_at: r.get("created_at"),
            })
            .collect())
    }
}

/// M1 的口令哈希：SHA-256 加固定前缀盐。
/// ponytail: 不是 Argon2/bcrypt；M4 引入真实口令策略时一并换掉。
pub fn hash_password(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}
```

- [ ] **Step 5: 补依赖并跑测试**

`crates/oss-server/Cargo.toml` 的 `[dependencies]` 加 `hex = { workspace = true }`；`[dev-dependencies]` 加 `sqlx = { workspace = true }`。

Run: `cargo test -p oss-server db 2>&1 | tail -12`
Expected: 全绿（4 个测试）

- [ ] **Step 6: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): 控制面 5 张表与仓储"
```

---

## Task 8: oss-server —— 配置、AppState、e-cat 起服务

**Files:**
- Create: `crates/oss-server/src/config.rs`, `state.rs`
- Create: `crates/oss-server/src/main.rs`
- Create: `oss.toml`

- [ ] **Step 1: 写 `config.rs`**

```rust
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen: String,
    pub data_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub database_url: String,
    pub jwt_secret: String,
    /// 单个 S3 PutObject 允许的最大字节数（内存上限）。
    /// ponytail: M1 整份读进内存；M2 换流式后可删掉这个上限。
    #[serde(default = "default_put_limit")]
    pub max_put_bytes: usize,
    #[serde(default)]
    pub groups: Vec<GroupTarget>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GroupTarget {
    /// aetherupload 的分组名（**不能含下划线**）
    pub group: String,
    pub bucket: String,
    pub max_size: u64,
    pub extensions: Vec<String>,
}

fn default_put_limit() -> usize {
    256 * 1024 * 1024
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    pub fn bucket_for_group(&self, group: &str) -> Option<&str> {
        self.groups.iter().find(|g| g.group == group).map(|g| g.bucket.as_str())
    }
}
```

- [ ] **Step 2: 写 `oss.toml`**

```toml
listen = "0.0.0.0:9000"
data_dir = "./data"
staging_dir = "./staging"
database_url = "sqlite://./oss.db?mode=rwc"
jwt_secret = "dev-only-change-me"
max_put_bytes = 268435456

[[groups]]
group = "image"
bucket = "images"
max_size = 10485760
extensions = ["jpg", "jpeg", "png", "gif", "webp", "svg"]

[[groups]]
group = "video"
bucket = "videos"
max_size = 0
extensions = ["mp4", "mov", "mkv", "webm"]

[[groups]]
group = "file"
bucket = "files"
max_size = 0
extensions = []
```

- [ ] **Step 3: 写 `state.rs`**

```rust
use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use xl_store::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Db>,
    pub store: Arc<Store>,
}

impl AppState {
    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        Self { cfg: Arc::new(cfg), db: Arc::new(db), store: Arc::new(store) }
    }
}
```

- [ ] **Step 4: 写失败的冒烟测试**（`crates/oss-server/tests/smoke.rs`）

```rust
use std::sync::Arc;

use oss_server::config::{AppConfig, GroupTarget};
use oss_server::db::Db;
use oss_server::router::build_router;
use oss_server::state::AppState;
use xl_store::store::{Store, StoreConfig};

async fn state(dir: &std::path::Path) -> AppState {
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        staging_dir: dir.join("staging"),
        database_url: "sqlite::memory:".into(),
        jwt_secret: "test".into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![GroupTarget { group: "file".into(), bucket: "files".into(), max_size: 0, extensions: vec![] }],
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store)
}

#[tokio::test]
async fn health_endpoint_is_up() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = reqwest::get(format!("http://{addr}/health/live")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}
```

- [ ] **Step 5: 跑测试确认失败**

Run: `cargo test -p oss-server --test smoke 2>&1 | tail -20`
Expected: 编译错误（`build_router` 未定义）

- [ ] **Step 6: 实现 `router.rs` 的最小版本**（M1 后续任务往这里加）

```rust
use axum::Router;
use axum::routing::get;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .with_state(state)
}
```

`state.rs` 里给 `AppState` 加 `#[derive(Clone)]`（已有）。

- [ ] **Step 7: 写 `main.rs`**

```rust
use std::sync::Arc;

use ecat::App;
use ecat::transport_http::HttpServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "oss.toml".to_string());
    let cfg = oss_server::config::AppConfig::load(std::path::Path::new(&cfg_path))?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    std::fs::create_dir_all(&cfg.staging_dir)?;

    let db = oss_server::db::Db::connect(&cfg.database_url).await?;
    let store = xl_store::store::Store::open(xl_store::store::StoreConfig::new(&cfg.data_dir)).await?;

    // M1 还没有注册界面：用环境变量引导出第一个管理员（幂等，已存在则跳过）。
    // OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 未设置时不做任何事。
    if let (Ok(user), Ok(pass)) = (std::env::var("OSS_ADMIN_USER"), std::env::var("OSS_ADMIN_PASSWORD")) {
        if db.verify_user(&user, &pass).await?.is_none() && !db.user_exists(&user).await? {
            db.create_user(&user, &pass, true).await?;
            tracing::info!(user = %user, "创建管理员账号");
        }
    }

    // 每个上传分组对应的桶，启动时确保存在（内核 + 控制面各一份记录）
    for g in &cfg.groups {
        if !store.bucket_exists(&g.bucket).await? {
            store.create_bucket(&g.bucket).await?;
        }
        db.insert_bucket(&g.bucket, None).await?;
    }

    let listen = cfg.listen.clone();
    let state = oss_server::state::AppState::new(cfg, db, store);
    let router = oss_server::router::build_router(state);

    let mut app = App::builder()
        .name("oss")
        .server(HttpServer::new(listen).router(router))
        .build()?;
    app.run().await?;
    Ok(())
}
```

`main.rs` 里 `use std::sync::Arc;` 用不到就删掉。

- [ ] **Step 8: 跑测试 + 手动起服务**

Run:
```bash
cargo test -p oss-server --test smoke 2>&1 | tail -8
cargo run -p oss-server 2>&1 | head -5 &
sleep 2 && curl -s localhost:9000/health/live && echo && kill %1
```
Expected: 测试通过；`curl` 输出 `ok`

- [ ] **Step 9: 提交**

```bash
git add crates/oss-server/ oss.toml && git commit -m "feat(server): e-cat 起服务 + /health/live 冒烟"
```

---

## Task 9: xl-s3 —— SigV4 凭据（`S3Auth`）

**Files:**
- Create: `crates/xl-s3/src/auth.rs`
- Create: `crates/xl-s3/src/convert.rs`

- [ ] **Step 1: 写失败的测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn known_access_key_resolves() {
        let db = oss_test_db().await;
        db.create_user("u", "p", true).await.unwrap();
        db.create_access_key("AKIA_TEST", "SK_TEST", 1).await.unwrap();
        let auth = SigV4Auth::new(db.pool().clone());
        assert_eq!(auth.get_secret_key("AKIA_TEST").await.unwrap().expose(), "SK_TEST");
    }

    #[tokio::test]
    async fn unknown_access_key_is_rejected() {
        let db = oss_test_db().await;
        let auth = SigV4Auth::new(db.pool().clone());
        assert!(auth.get_secret_key("NOPE").await.is_err());
    }
}
```

> 为了让 `xl-s3` 的测试能建库，`xl-s3` 的 dev-dependencies 加 `tokio`、`tempfile`，并在 `crates/xl-s3/tests/` 或 `auth.rs` 的测试模块里提供 `oss_test_db()` 辅助：直接用 `sqlx::SqlitePool::connect("sqlite::memory:")` + `oss-server` 的 schema？——**不要**：`xl-s3` 不能反向依赖 `oss-server`。改为在 `xl-s3` 里内联最小 schema：

```rust
#[cfg(test)]
pub(crate) async fn oss_test_db() -> sqlx::SqlitePool {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);\
         CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
          status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL);",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 auth 2>&1 | tail -20`
Expected: 编译错误

- [ ] **Step 3: 实现 `auth.rs`**

```rust
//! SigV4 凭据解析：s3s 负责验签，我们只负责「AK 换 SK」。
//! M1 只有认证没有授权 —— 任何有效 AK 可访问所有桶；授权在 M4。

use sqlx::SqlitePool;

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3ErrorCode, S3Result};

#[derive(Clone)]
pub struct SigV4Auth {
    pool: SqlitePool,
}

impl SigV4Auth {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl S3Auth for SigV4Auth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let row = sqlx::query(
            "SELECT secret_key FROM access_keys \
             WHERE access_key = ? AND status = 'active' AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(access_key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        row.map(|r| SecretKey::from(sqlx::Row::get::<String, _>(&r, "secret_key")))
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidAccessKeyId, "unknown access key"))
    }
}

#[cfg(test)]
mod tests {
    // 见 Step 1
}
```

> 若 `SecretKey::from(String)` 不存在，看 `src/auth/secret_key.rs` 的构造方法并改用之（`SecretKey::new` / `From<&str>`）。

- [ ] **Step 4: 实现 `convert.rs` 的错误映射**

```rust
//! xl-store ↔ s3s 的错误与类型翻译。

use s3s::{S3Error, S3ErrorCode};
use xl_store::error::Error as StoreError;

pub fn to_s3_error(e: StoreError) -> S3Error {
    let code = match &e {
        StoreError::NoSuchBucket(_) => S3ErrorCode::NoSuchBucket,
        StoreError::BucketAlreadyExists(_) => S3ErrorCode::BucketAlreadyOwnedByYou,
        StoreError::BucketNotEmpty(_) => S3ErrorCode::BucketNotEmpty,
        StoreError::NoSuchKey(_) => S3ErrorCode::NoSuchKey,
        StoreError::InvalidKey(_) => S3ErrorCode::InvalidArgument,
        StoreError::InvalidBucketName(_) => S3ErrorCode::InvalidBucketName,
        StoreError::CorruptMeta(_) | StoreError::Encode(_) => S3ErrorCode::InternalError,
        StoreError::Io(_) => S3ErrorCode::InternalError,
    };
    S3Error::with_message(code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_key_errors() {
        assert_eq!(to_s3_error(StoreError::NoSuchKey("k".into())).code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(to_s3_error(StoreError::NoSuchBucket("b".into())).code(), &S3ErrorCode::NoSuchBucket);
    }
}
```

- [ ] **Step 5: 跑测试**

Run: `cargo test -p xl-s3 2>&1 | tail -12`
Expected: 全绿

- [ ] **Step 6: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): SigV4 凭据解析与错误映射"
```

---

## Task 10: xl-s3 —— 桶操作

**Files:**
- Create: `crates/xl-s3/src/service.rs`

- [ ] **Step 1: 写失败的测试**

```rust
// crates/xl-s3/tests/bucket_ops.rs
use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (OssS3::new(store, 1024 * 1024), dir)
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
async fn create_head_list_delete_bucket() {
    let (s3, _d) = service().await;

    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    s3.head_bucket(req(HeadBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();

    let out = s3.list_buckets(req(ListBucketsInput::default())).await.unwrap().output;
    let names: Vec<String> = out.buckets.unwrap().into_iter().filter_map(|b| b.name).collect();
    assert_eq!(names, vec!["alpha".to_string()]);

    s3.delete_bucket(req(DeleteBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    assert!(s3.head_bucket(req(HeadBucketInput { bucket: "alpha".into(), ..Default::default() })).await.is_err());
}

#[tokio::test]
async fn head_missing_bucket_is_no_such_bucket() {
    let (s3, _d) = service().await;
    let err = s3.head_bucket(req(HeadBucketInput { bucket: "ghost".into(), ..Default::default() })).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchBucket);
}
```

> `S3Request` 的 `trailing_headers` 字段名以 `protocol.rs` 为准；若构造不出来，改用 `S3Request::new(input)` 之类的构造器（若存在），或在本任务里加一个 `fn s3req()` 辅助函数集中处理。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test bucket_ops 2>&1 | tail -20`
Expected: 编译错误

- [ ] **Step 3: 实现 `service.rs` 的骨架与桶操作**

```rust
//! s3s 的 `S3` trait 实现。M1 覆盖：桶 CRUD、对象 CRUD、ListObjectsV2、CopyObject、批量删除。
//! Multipart / Range / 版本 / 加密 / 锁 / 复制分别是 M2/M5/M6 的事。

use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result};
use xl_store::store::Store;

use crate::convert::to_s3_error;

pub struct OssS3 {
    store: Arc<Store>,
    /// ponytail: 单个 PUT 的内存上限；M2 换流式后删除。
    max_put_bytes: usize,
}

impl OssS3 {
    pub fn new(store: Arc<Store>, max_put_bytes: usize) -> Self {
        Self { store, max_put_bytes }
    }
}

/// `xl.meta` 的 `mod_time_nanos` → S3 的 Last-Modified。不要用 now() 冒充。
fn last_modified(nanos: i64) -> LastModified {
    LastModified::from(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos.max(0) as u64))
}

#[async_trait::async_trait]
impl S3 for OssS3 {
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        self.store.create_bucket(&bucket).await.map_err(to_s3_error)?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = req.input.bucket;
        match self.store.bucket_exists(&bucket).await {
            Ok(true) => Ok(S3Response::new(HeadBucketOutput::default())),
            Ok(false) => Err(s3s::s3_error!(NoSuchBucket)),
            Err(e) => Err(to_s3_error(e)),
        }
    }

    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = req.input.bucket;
        self.store.delete_bucket(&bucket).await.map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(&self, _req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.store.list_buckets().await.map_err(to_s3_error)?;
        let list = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(CreationDate::from(std::time::SystemTime::now())),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput { buckets: Some(list), ..Default::default() }))
    }
}
```

> `CreationDate` 的类型以 `generated.rs` 为准（可能是 `Timestamp` 别名）。若 `CreationDate::from(SystemTime)` 不成立，改用其实际构造方式。

- [ ] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 --test bucket_ops 2>&1 | tail -12`
Expected: 全绿

- [ ] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): 桶操作（create/head/delete/list）"
```

---

## Task 11: xl-s3 —— 对象操作

**Files:**
- Modify: `crates/xl-s3/src/service.rs`

- [ ] **Step 1: 写失败的测试**

```rust
// crates/xl-s3/tests/object_ops.rs
#[tokio::test]
async fn put_head_get_delete() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();

    let body = StreamingBlob::from_bytes(Bytes::from_static(b"hello s3"));
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "a/b.txt".into(),
        body: Some(body),
        content_type: Some("text/plain".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    let head = s3.head_object(req(HeadObjectInput { bucket: "alpha".into(), key: "a/b.txt".into(), ..Default::default() })).await.unwrap().output;
    assert_eq!(head.content_length, Some(8));
    // ETag 必须是整份 MD5 —— 测试里现算，不写死
    let expected_md5 = {
        use md5::{Digest as _, Md5};
        let mut h = Md5::new();
        h.update(b"hello s3");
        hex::encode(h.finalize())
    };
    assert_eq!(head.e_tag.as_ref().map(ETag::value), Some(expected_md5.as_str()));

    let got = s3.get_object(req(GetObjectInput { bucket: "alpha".into(), key: "a/b.txt".into(), ..Default::default() })).await.unwrap().output;
    assert_eq!(got.content_length, Some(8));

    s3.delete_object(req(DeleteObjectInput { bucket: "alpha".into(), key: "a/b.txt".into(), ..Default::default() })).await.unwrap();
    assert!(s3.head_object(req(HeadObjectInput { bucket: "alpha".into(), key: "a/b.txt".into(), ..Default::default() })).await.is_err());
}

#[tokio::test]
async fn delete_objects_is_batch_and_idempotent() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    for k in ["k1", "k2"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(), key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))), ..Default::default()
        })).await.unwrap();
    }
    // DeleteObjectsInput 没有 Default，用生成 builder 构造
    let out = s3.delete_objects(req(DeleteObjectsInput::builder()
        .bucket("alpha".to_string())
        .delete(Delete { objects: vec![
            ObjectIdentifier { key: "k1".into(), ..Default::default() },
            ObjectIdentifier { key: "k2".into(), ..Default::default() },
            ObjectIdentifier { key: "never-existed".into(), ..Default::default() },
        ], ..Default::default() })
        .build()
        .unwrap())).await.unwrap().output;
    assert!(out.errors.is_none(), "删除不存在的键不应报错（S3 语义）");
}
```

> 测试所需依赖（`md-5` / `hex` / `http` / `tempfile` / `tokio`，以及 `s3s` 本体）已在 Task 1 的 `[dependencies]` / `[dev-dependencies]` 配齐。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -20`
Expected: 编译错误

- [ ] **Step 3: 实现对象操作**

```rust
// 追加到 impl S3 for OssS3
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput { bucket, key, body, content_type, .. } = req.input;
        let mut body = body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;

        // ponytail: M1 整份读进内存（带上限）。M2 换流式：直接把 StreamingBlob
        // 包成 ByteStream 交给内核，内核本来就是流式写盘的。
        let mut buf = Vec::new();
        let mut total = 0usize;
        while let Some(chunk) = futures::StreamExt::next(&mut body).await {
            let chunk = chunk.map_err(|e| s3s::s3_error!(InternalError, "{e}"))?;
            total += chunk.len();
            if total > self.max_put_bytes {
                return Err(s3s::s3_error!(EntityTooLarge, "object exceeds max_put_bytes"));
            }
            buf.extend_from_slice(&chunk);
        }

        let info = self.store
            .put_object(&bucket, &key, Bytes::from(buf), content_type)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutObjectOutput { e_tag: Some(ETag::Strong(info.etag)), ..Default::default() }))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;
        let (info, bytes) = self.store.get_object(&bucket, &key).await.map_err(to_s3_error)?;
        let len = bytes.len() as i64;

        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(bytes)),
            content_length: Some(len),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            ..Default::default()
        }))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let info = self.store.head_object(&req.input.bucket, &req.input.key).await.map_err(to_s3_error)?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(info.size as i64),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            ..Default::default()
        }))
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.store.delete_object(&req.input.bucket, &req.input.key).await.map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket;
        for obj in req.input.delete.objects {
            self.store.delete_object(&bucket, &obj.key).await.map_err(to_s3_error)?;
        }
        Ok(S3Response::new(DeleteObjectsOutput::default()))
    }
```

- [ ] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -12`
Expected: 全绿（ETag 断言用真实 md5）

- [ ] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): 对象 CRUD 与批量删除"
```

---

## Task 12: xl-s3 —— ListObjectsV2 与 CopyObject

**Files:**
- Modify: `crates/xl-s3/src/service.rs`

- [ ] **Step 1: 写失败的测试**

```rust
// 追加到 crates/xl-s3/tests/object_ops.rs
#[tokio::test]
async fn list_objects_v2_prefix_delimiter_and_paging() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    for k in ["a.txt", "docs/1.txt", "docs/2.txt", "docs/deep/3.txt"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(), key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))), ..Default::default()
        })).await.unwrap();
    }

    let out = s3.list_objects_v2(req(ListObjectsV2Input {
        bucket: "alpha".into(), prefix: Some("docs/".into()), delimiter: Some("/".into()),
        ..Default::default()
    })).await.unwrap().output;
    assert_eq!(out.contents.unwrap().len(), 2);
    assert_eq!(out.common_prefixes.unwrap()[0].prefix.as_deref(), Some("docs/deep/"));

    let page = s3.list_objects_v2(req(ListObjectsV2Input {
        bucket: "alpha".into(), max_keys: Some(2), ..Default::default()
    })).await.unwrap().output;
    assert_eq!(page.is_truncated, Some(true));
    assert!(page.next_continuation_token.is_some());
}

#[tokio::test]
async fn copy_object_copies_bytes() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(), key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"copy"))), ..Default::default()
    })).await.unwrap();

    // CopyObjectInput 没有 Default；copy_source 是必填枚举（s3s 已解码并去掉前导 '/'）
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("alpha".to_string())
        .key("dst".to_string())
        .copy_source(CopySource::Bucket { bucket: "alpha".into(), key: "src".into(), version_id: None })
        .build()
        .unwrap())).await.unwrap();

    let got = s3.get_object(req(GetObjectInput { bucket: "alpha".into(), key: "dst".into(), ..Default::default() })).await.unwrap().output;
    assert_eq!(got.content_length, Some(4));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -20`
Expected: 编译错误 / `NotImplemented`

- [ ] **Step 3: 实现**

```rust
// 追加到 impl S3 for OssS3
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input { bucket, prefix, delimiter, max_keys, continuation_token, .. } = req.input;

        let result = self.store
            .list_objects(&bucket, &xl_store::store::ListRequest {
                prefix,
                delimiter,
                max_keys: max_keys.map(|m| m as usize),
                continuation_token,
            })
            .await
            .map_err(to_s3_error)?;

        let contents: ObjectList = result
            .objects
            .iter()
            .map(|o| Object {
                key: Some(o.key.clone()),
                size: Some(o.size as i64),
                e_tag: Some(ETag::Strong(o.etag.clone())),
                last_modified: Some(last_modified(o.last_modified_nanos)),
                storage_class: Some(ObjectStorageClass::from("STANDARD")),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            contents: Some(contents),
            key_count: Some(result.objects.len() as i32),
            common_prefixes: Some(
                result.common_prefixes.into_iter().map(|p| CommonPrefix { prefix: Some(p) }).collect(),
            ),
            is_truncated: Some(result.is_truncated),
            next_continuation_token: result.next_token,
            ..Default::default()
        }))
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        // s3s 已将 x-amz-copy-source 解码、去掉前导 '/' 并解析成枚举；这里只接受桶形式
        let CopySource::Bucket { ref bucket, ref key, .. } = req.input.copy_source else {
            return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
        };

        let info = self.store
            .copy_object(bucket, key, &req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(info.etag)),
                last_modified: None,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
```

- [ ] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 2>&1 | tail -12`
Expected: 全绿

- [ ] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): ListObjectsV2 与 CopyObject"
```

---

## Task 13: 接线 —— S3 挂进 e-cat 的 fallback

**Files:**
- Create: `crates/oss-server/tests/common/mod.rs`（测试共用的 `state()` 辅助，被 smoke / s3_over_http / aether_ingest / console_api 四个测试文件复用）
- Modify: `crates/oss-server/src/router.rs`

- [ ] **Step 0: 把 T8 里的 `state()` 抽成共用模块**

`crates/oss-server/tests/common/mod.rs`：

```rust
use oss_server::config::{AppConfig, GroupTarget};
use oss_server::db::Db;
use oss_server::state::AppState;
use xl_store::store::{Store, StoreConfig};

pub async fn state(dir: &std::path::Path) -> AppState {
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        staging_dir: dir.join("staging"),
        database_url: "sqlite::memory:".into(),
        jwt_secret: "test".into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![
            GroupTarget { group: "file".into(), bucket: "files".into(), max_size: 0, extensions: vec![] },
        ],
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store)
}
```

之后每个测试文件顶部写 `mod common;` 并 `use common::state;`（T8 的 smoke.rs 里那份内联实现删掉，改为引用）。

- [ ] **Step 1: 写失败的测试**（`crates/oss-server/tests/s3_over_http.rs`）

```rust
// 起真服务，用 aws-sigv4 打的请求跑一个 PUT → HEAD → GET → LIST → DELETE 的闭环。
// 为了不引 aws-sdk（体积大），用 reqwest 手工构造签名。
// 更省事的做法：直接调 s3s 的 S3Service（不经 HTTP）验证挂载正确，
// 而真正的 SigV4 兼容性交给 Task 16 的 aws-cli / rclone 脚本（那是端到端验收）。

#[tokio::test]
async fn s3_fallback_is_mounted() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("u", "p", true).await.unwrap();
    st.db.create_access_key("AKIA_TEST", "SK_TEST", 1).await.unwrap();
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // 无凭据的 S3 请求必须被拒（说明 s3s 的鉴权层确实挂上了，而不是 404）
    let resp = reqwest::get(format!("http://{addr}/alpha/whatever")).await.unwrap();
    assert_eq!(resp.status(), 403, "未经签名的 S3 请求应被 s3s 拒绝");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test s3_over_http 2>&1 | tail -10`
Expected: FAIL（当前 fallback 未挂，返回 404 而非 403）

- [ ] **Step 3: 修改 `router.rs`**

```rust
use std::sync::Arc;

use axum::Router;
use axum::error_handling::HandleError;
use axum::routing::get;
use s3s::service::S3ServiceBuilder;

use crate::state::AppState;
use xl_s3::auth::SigV4Auth;
use xl_s3::service::OssS3;

pub fn build_router(state: AppState) -> Router {
    // s3s 0.17 的 builder 是 `&mut self` 风格，不能链式调用
    let mut s3_builder = S3ServiceBuilder::new(OssS3::new(Arc::clone(&state.store), state.cfg.max_put_bytes));
    s3_builder.set_auth(SigV4Auth::new(state.db.pool().clone()));
    let s3 = s3_builder.build();

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
}

/// S3 业务错误（NoSuchBucket 等）由 s3s 在 ops 层序列化成带正确状态码的 XML 响应，不会走到这里；
/// 到这里的是 HTTP 层错误（body 过大、格式非法等）—— 兜底 500。
async fn handle_s3_error(err: s3s::HttpError) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err.into_std_error().to_string())
}
```

> `s3s::HttpError` 只有 `from_std_error` / `into_std_error`（`src/protocol.rs`）；
> 没有 `status_code()`，也不存在 `S3ServiceError` 这个类型。

- [ ] **Step 4: 跑测试**

Run: `cargo test -p oss-server --test s3_over_http 2>&1 | tail -10`
Expected: PASS（403）

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): s3s 挂进 e-cat router 的 fallback"
```

---

## Task 14: aetherupload 接线 + 原生 ingest

**Files:**
- Create: `crates/oss-server/src/aether.rs`
- Modify: `crates/oss-server/src/router.rs`, `state.rs`

- [ ] **Step 1: 写失败的测试**

```rust
// crates/oss-server/tests/aether_ingest.rs
// 直接测 sink 与 instant store，不经 HTTP（HTTP 链路交给 Task 16 的端到端脚本）。

#[tokio::test(flavor = "multi_thread")] // block_on 用了 block_in_place，单线程 runtime 会 panic
async fn ingest_sink_puts_finished_file_into_store() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("files").await.unwrap();

    // 造一个「已落盘的成品」：aetherupload 落盘后是 <root>/file/<subdir>/<md5>.txt
    let sub = "2026-10";
    let group_dir = st.cfg.staging_dir.join("file").join(sub);
    std::fs::create_dir_all(&group_dir).unwrap();
    let body = b"finished upload";
    let md5 = format!("{:x}", {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(body);
        h.finalize()
    });
    let path = group_dir.join(format!("{md5}.txt"));
    std::fs::write(&path, body).unwrap();

    let resource = aetherupload::resource::Resource::new(
        &st.cfg.staging_dir, "file", "file", sub, &format!("{md5}.txt"),
    );

    st.ingest.upload_complete(&resource);

    // 对象应当已经把文件装进内核，ETag 就是 aetherupload 已经算好的整份 md5
    let info = st.store.head_object("files", &format!("{sub}/{md5}.txt")).await.unwrap();
    assert_eq!(info.size, body.len() as u64);
    assert_eq!(info.etag, md5);

    // uploads 表留下记录，供控制台展示
    let rows = st.db.list_uploads(10).await.unwrap();
    assert_eq!(rows[0].bucket, "files");
}
```

> `Resource::new(upload_root: &Path, group, group_dir, group_sub_dir, name)` —— `aetherupload-rust-1.0.3/src/resource.rs:25`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test aether_ingest 2>&1 | tail -15`
Expected: 编译错误（`st.ingest` 未定义）

- [ ] **Step 3: 实现 `aether.rs`**

```rust
//! aetherupload 接线：四条路由 + 秒传索引（落 SQL，不引 Redis）+ 上传完成后原生 ingest。
//!
//! 为什么用原生 ingest 而不是 aetherupload 的 S3 驱动：省一跳本机 HTTP，
//! 且 aetherupload 已经算完整份 MD5 —— 直接就是单段 PUT 的 ETag，白拿。

use std::sync::Arc;

use aetherupload::config::{Config, GroupConfig, StorageDriver, SubdirRule};
use aetherupload::events::EventSink;
use aetherupload::instant::InstantStore;
use aetherupload::resource::Resource;
use aetherupload::Runtime;

use crate::config::AppConfig;
use crate::db::Db;
use crate::state::AppState;
use xl_store::store::Store;

/// 同步 trait → async 运行时的桥。
///
/// aetherupload 的 `EventSink` 与 `InstantStore` 都是**同步契约**（对齐 PHP 语义），
/// 而我们的存取层是 async。`block_in_place` 需要 multi-thread runtime
/// （`#[tokio::main]` 默认即是；current_thread 下会 panic —— 测试里用
/// `#[tokio::test(flavor = "multi_thread")]`）。
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

/// 秒传索引落在已有的 SQLite 上 —— 少一个 Redis 依赖，TTL 语义与 SETEX 一致。
#[derive(Clone)]
pub struct SqlInstantStore {
    db: Arc<Db>,
}

impl SqlInstantStore {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

impl InstantStore for SqlInstantStore {
    fn get(&self, key: &str) -> aetherupload::Result<Option<String>> {
        block_on(self.db.instant_get(key)).map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }

    fn set_ex(&self, key: &str, value: &str, ttl_seconds: u64) -> aetherupload::Result<()> {
        block_on(self.db.instant_set(key, value, ttl_seconds))
            .map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }

    fn del(&self, key: &str) -> aetherupload::Result<()> {
        block_on(self.db.instant_del(key)).map_err(|e| aetherupload::Error::Io(std::io::Error::other(e.to_string())))
    }
}

/// 上传完成 → 把成品文件装进存储内核。
#[derive(Clone)]
pub struct IngestSink {
    pub store: Arc<Store>,
    pub db: Arc<Db>,
    pub cfg: Arc<AppConfig>,
}

impl IngestSink {
    /// 分组 → 目标桶 → 对象键（`<子目录>/<md5>.<ext>`）。
    fn resolve(&self, resource: &Resource) -> Option<(String, String)> {
        let bucket = self.cfg.bucket_for_group(&resource.group)?.to_string();
        let key = if resource.group_sub_dir.is_empty() {
            resource.name.clone()
        } else {
            format!("{}/{}", resource.group_sub_dir, resource.name)
        };
        Some((bucket, key))
    }
}

impl EventSink for IngestSink {
    fn upload_complete(&self, resource: &Resource) {
        let Some((bucket, key)) = self.resolve(resource) else {
            tracing::warn!(group = %resource.group, "上传分组没有配置目标桶，跳过 ingest");
            return;
        };

        // 事件里抛错等同于上传失败 —— 这里必须把错误传播出去（panic 会被控制器接住并清理临时文件）
        let result = block_on(async {
            let bytes = tokio::fs::read(&resource.path).await?;
            let size = bytes.len() as u64;
            let info = self.store.put_object(&bucket, &key, bytes.into(), None).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((size, info))
        });

        match result {
            Ok((size, info)) => {
                let saved_path = format!("{}_{}_{}", resource.group, resource.group_sub_dir, resource.name);
                if let Err(e) = block_on(self.db.record_upload(
                    &saved_path, &bucket, &key, &resource.group, size as i64, &info.etag,
                )) {
                    tracing::error!(error = %e, "记录 uploads 失败");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "ingest 失败");
                panic!("ingest 失败: {e}");
            }
        }
    }
}

/// 组装 aetherupload 的运行时：暂存盘、分组、秒传索引、事件出口。
pub fn build_runtime(state: &AppState) -> Arc<Runtime> {
    let mut cfg = Config::default();
    cfg.root_dir = state.cfg.staging_dir.to_string_lossy().into_owned();
    cfg.chunk_size = 1_000_000;
    cfg.resource_subdir_rule = SubdirRule::Month;
    cfg.instant_completion = true; // 秒传开启（索引落 SQL）
    cfg.storage.driver = StorageDriver::Local; // 暂存与成品都在本地；ingest 之后由内核负责

    for g in &state.cfg.groups {
        cfg.groups.insert(
            g.group.clone(),
            GroupConfig {
                group_dir: g.group.clone(),
                resource_maxsize: g.max_size,
                resource_extensions: g.extensions.clone(),
                event_before_upload_complete: false,
                event_upload_complete: true, // 必须为 true，否则不触发 ingest
            },
        );
    }

    let runtime = Runtime::new(cfg, &state.cfg.staging_dir)
        .expect("aetherupload Runtime 构造失败")
        .with_instant(Arc::new(SqlInstantStore::new(Arc::clone(&state.db))))
        .with_events(Arc::new(IngestSink {
            store: Arc::clone(&state.store),
            db: Arc::clone(&state.db),
            cfg: Arc::clone(&state.cfg),
        }));

    // 分组目录（`_header/` 与各分组目录）必须先存在，否则预处理会报笼统的上传错误。
    // aetherupload 自带 e2e 也是用 list_groups 建的。
    let _ = aetherupload::console::list_groups(&runtime, &mut |_| {});

    Arc::new(runtime)
}
```

- [ ] **Step 4: `state.rs` 加字段**

```rust
#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Db>,
    pub store: Arc<Store>,
    pub ingest: Arc<crate::aether::IngestSink>,
}

impl AppState {
    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        let cfg = Arc::new(cfg);
        let db = Arc::new(db);
        let ingest = Arc::new(crate::aether::IngestSink {
            store: Arc::new(store),
            db: Arc::clone(&db),
            cfg: Arc::clone(&cfg),
        });
        Self { cfg, db, store: Arc::clone(&ingest.store), ingest }
    }
}
```

- [ ] **Step 5: `router.rs` 挂 aetherupload 的四条路由**

```rust
    // aetherupload：四条路由（预处理 / 分块 / 展示 / 下载）
    let aether = aetherupload::integrations::ecat::routes(crate::aether::build_runtime(&state));

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .merge(aether)
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
```

注意：`aetherupload::integrations::ecat::routes` 返回的是 `Router`（不带 state），
`merge` 进带 `AppState` 的 Router 之前要先 `.with_state(())` 转成 `Router<()>`——
若编译报 `Router<S>` 不匹配，用 `Router::<()>::new().merge(aether)` 的写法把两者都降到 `Router<()>`，
再把 `AppState` 用 `Extension` 层注入控制台 API（或干脆让控制台 API 自己 `with_state` 一个子 Router 再 merge）。

- [ ] **Step 6: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -15`
Expected: 全绿（含 ingest 测试）

- [ ] **Step 7: 手动验证四条路由挂上了**

Run:
```bash
cargo run -p oss-server &
sleep 2
# 分组目录必须先建好，否则预处理会报笼统的上传错误
curl -s -X POST localhost:9000/aetherupload/preprocess \
  -F resource_name=a.txt -F resource_size=11 -F group=file \
  -F resource_hash=5eb63bbbe01eeed093cb22bb8f5acdc3 | head -c 300
echo
kill %1
```
Expected: 返回 JSON 且 `"error":"0"`，含 `chunkSize` / `resourceTempBaseName` / `groupSubDir` / `savedPath` 字段。
分组目录由 `aether::build_runtime` 构造时调用 `aetherupload::console::list_groups` 自动创建。
（`cargo run -p aetherupload-rust` 不可用：它是 registry 依赖，不是 workspace 成员。）

- [ ] **Step 8: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): aetherupload 接线 + 上传完成原生 ingest"
```

---

## Task 15: 控制台 API

**Files:**
- Create: `crates/oss-server/src/console.rs`
- Modify: `crates/oss-server/src/router.rs`

- [ ] **Step 1: 写失败的测试**

```rust
// crates/oss-server/tests/console_api.rs
#[tokio::test]
async fn login_then_list_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    // 未带 token 必须 401
    let resp = reqwest::get(format!("{base}/api/v1/buckets")).await.unwrap();
    assert_eq!(resp.status(), 401);

    let client = reqwest::Client::new();
    let login: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send().await.unwrap().json().await.unwrap();
    let token = login["token"].as_str().unwrap();

    let buckets: Vec<String> = client
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(token)
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(buckets, vec!["alpha".to_string()]);
}

#[tokio::test]
async fn wrong_password_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let app = build_router(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "WRONG"}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test console_api 2>&1 | tail -10`
Expected: 404（路由未挂）

- [ ] **Step 3: 实现 `console.rs`**

```rust
//! 控制台 API。M1 只有：登录、桶列表、对象列表、下载、删除、上传记录、访问密钥。
//! 授权模型（谁能做什么）在 M4；M1 只区分「登录 / 未登录」。

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Claims {
    sub: String,
    exp: usize,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/buckets", get(list_buckets).post(create_bucket))
        .route("/buckets/{bucket}", delete(delete_bucket))
        .route("/buckets/{bucket}/objects", get(list_objects))
        .route("/buckets/{bucket}/objects/{*key}", get(get_object).delete(delete_object))
        .route("/uploads", get(list_uploads))
        .route("/uploads/register", post(register_upload))
        .route("/access-keys", get(list_access_keys).post(create_access_key))
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    match state.db.verify_user(&body.username, &body.password).await {
        Ok(Some(uid)) => {
            let exp = now_secs() as usize + 12 * 3600;
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &Claims { sub: uid.to_string(), exp },
                &jsonwebtoken::EncodingKey::from_secret(state.cfg.jwt_secret.as_bytes()),
            );
            match token {
                Ok(token) => (StatusCode::OK, Json(serde_json::json!({ "token": token }))).into_response(),
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "token 签发失败").into_response(),
            }
        }
        _ => (StatusCode::UNAUTHORIZED, "用户名或密码错误").into_response(),
    }
}

/// 从 Authorization: Bearer 解出用户 id。M1 的最小鉴权。
/// ponytail: 12 行自写中间件；M4 引入策略模型时换 ecat-auth 的 JwtAuthLayer。
fn auth(state: &AppState, headers: &HeaderMap) -> Result<i64, StatusCode> {
    let raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let decoded = jsonwebtoken::decode::<Claims>(
        raw,
        &jsonwebtoken::DecodingKey::from_secret(state.cfg.jwt_secret.as_bytes()),
        &jsonwebtoken::Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    decoded.claims.sub.parse::<i64>().map_err(|_| StatusCode::UNAUTHORIZED)
}

async fn list_buckets(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    match state.store.list_buckets().await {
        Ok(list) => Json(list.into_iter().map(|b| b.name).collect::<Vec<_>>()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateBucketBody { name: String }

async fn create_bucket(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<CreateBucketBody>) -> impl IntoResponse {
    let uid = match auth(&state, &headers) { Ok(u) => u, Err(s) => return s.into_response() };
    if let Err(e) = state.store.create_bucket(&body.name).await {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    let _ = state.db.insert_bucket(&body.name, Some(uid)).await;
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_bucket(State(state): State<AppState>, headers: HeaderMap, Path(bucket): Path<String>) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    match state.store.delete_bucket(&bucket).await {
        Ok(()) => {
            let _ = state.db.delete_bucket_row(&bucket).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct ListQuery { prefix: Option<String>, delimiter: Option<String> }

async fn list_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ListQuery>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    let req = xl_store::store::ListRequest { prefix: q.prefix, delimiter: q.delimiter, ..Default::default() };
    match state.store.list_objects(&bucket, &req).await {
        Ok(r) => Json(serde_json::json!({
            "objects": r.objects.iter().map(|o| serde_json::json!({
                "key": o.key, "size": o.size, "etag": o.etag, "content_type": o.content_type
            })).collect::<Vec<_>>(),
            "common_prefixes": r.common_prefixes,
            "is_truncated": r.is_truncated,
            "next_token": r.next_token,
        })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn get_object(State(state): State<AppState>, headers: HeaderMap, Path((bucket, key)): Path<(String, String)>) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    match state.store.get_object(&bucket, &key).await {
        Ok((info, bytes)) => {
            let ct = info.content_type.unwrap_or_else(|| "application/octet-stream".into());
            ([(axum::http::header::CONTENT_TYPE, ct)], bytes).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn delete_object(State(state): State<AppState>, headers: HeaderMap, Path((bucket, key)): Path<(String, String)>) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    match state.store.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn list_uploads(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    match state.db.list_uploads(100).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct RegisterBody { saved_path: String, original_name: String }

async fn register_upload(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<RegisterBody>) -> impl IntoResponse {
    let uid = match auth(&state, &headers) { Ok(u) => u, Err(s) => return s.into_response() };
    match state.db.register_upload(&body.saved_path, &body.original_name, Some(uid)).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn list_access_keys(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let uid = match auth(&state, &headers) { Ok(u) => u, Err(s) => return s.into_response() };
    match state.db.list_access_keys(uid).await {
        Ok(keys) => Json(keys).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateKeyBody { access_key: Option<String>, secret_key: Option<String> }

async fn create_access_key(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<CreateKeyBody>) -> impl IntoResponse {
    let uid = match auth(&state, &headers) { Ok(u) => u, Err(s) => return s.into_response() };
    let ak = body.access_key.unwrap_or_else(|| format!("OSS{}", uuid::Uuid::new_v4().simple()));
    let sk = body.secret_key.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    match state.db.create_access_key(&ak, &sk, uid).await {
        Ok(()) => Json(serde_json::json!({ "access_key": ak, "secret_key": sk })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
```

> `oss-server` 的 `[dependencies]` 需要加 `uuid = { workspace = true }`。

- [ ] **Step 4: `router.rs` 挂控制台**

```rust
        .nest("/api/v1", crate::console::routes())
```

- [ ] **Step 5: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -15`
Expected: 全绿

- [ ] **Step 6: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): 控制台 API（登录/桶/对象/上传记录/密钥）"
```

---

## Task 16: 端到端验收脚本与运行说明

**Files:**
- Create: `tests/e2e_s3.sh`
- Create: `README.md`

- [ ] **Step 1: 写端到端脚本**

```bash
#!/usr/bin/env bash
# M1 验收：S3 兼容性（aws-cli + rclone）+ aetherupload 上传链路。
# 前置：docker 可用（用于跑 minio/mc 作为对照）；本机装了 aws-cli 与 rclone。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST}"
SK="${SK:-SK_TEST}"
BUCKET="e2e-$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
aws_s3() { aws --endpoint-url "$ENDPOINT" s3 "$@"; }

echo "== 1. 建桶 =="
aws_s3 mb "s3://$BUCKET"

echo "== 2. 上传（走 s3s 的 PutObject）=="
head -c 1048576 /dev/urandom > "$WORK/one.bin"
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/one.bin"

echo "== 3. 列举 =="
aws_s3 ls "s3://$BUCKET/" | tee "$WORK/ls.txt"
grep -q "one.bin" "$WORK/ls.txt"

echo "== 4. 下载并比对（字节一致）=="
aws_s3 cp "s3://$BUCKET/one.bin" "$WORK/back.bin"
cmp "$WORK/one.bin" "$WORK/back.bin"

echo "== 5. head-object 的 ETag 必须是整份 MD5 =="
LOCAL_MD5=$(md5sum "$WORK/one.bin" | cut -d' ' -f1)
REMOTE_ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key one.bin --query ETag --output text | tr -d '"')
[ "$LOCAL_MD5" = "$REMOTE_ETAG" ] || { echo "ETag 不符: $LOCAL_MD5 vs $REMOTE_ETAG"; exit 1; }

echo "== 6. 前缀与分隔符 =="
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/dir/a.bin"
aws --endpoint-url "$ENDPOINT" s3api list-objects-v2 --bucket "$BUCKET" --delimiter / --query 'CommonPrefixes[].Prefix' --output text | grep -q "dir/"

echo "== 7. CopyObject =="
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$BUCKET" --key copy.bin --copy-source "$BUCKET/one.bin" >/dev/null
aws_s3 ls "s3://$BUCKET/copy.bin" | grep -q copy.bin

echo "== 8. 批量删除 =="
aws --endpoint-url "$ENDPOINT" s3api delete-objects --bucket "$BUCKET" \
  --delete "Objects=[{Key=one.bin},{Key=copy.bin}],Quiet=false" >/dev/null
aws_s3 ls "s3://$BUCKET/" | tee "$WORK/ls2.txt"
grep -q "dir/" "$WORK/ls2.txt"   # 未被删除的键仍在
if grep -q -e one.bin -e copy.bin "$WORK/ls2.txt"; then echo "批量删除未生效"; exit 1; fi

echo "== 9. rclone 对照 =="
export RCLONE_CONFIG_OSS_TYPE=s3
export RCLONE_CONFIG_OSS_PROVIDER=Other
export RCLONE_CONFIG_OSS_ACCESS_KEY_ID="$AK"
export RCLONE_CONFIG_OSS_SECRET_ACCESS_KEY="$SK"
export RCLONE_CONFIG_OSS_ENDPOINT="${ENDPOINT#http://}"
export RCLONE_CONFIG_OSS_FORCE_PATH_STYLE=true
rclone lsf oss:"$BUCKET"
rclone copyto "$WORK/one.bin" oss:"$BUCKET/rclone.bin"
rclone cat oss:"$BUCKET/rclone.bin" | cmp - "$WORK/one.bin"   # rclone 读回内容与本地一致
if rclone lsf oss:"$BUCKET" | grep -qx one.bin; then echo "删除未生效"; exit 1; fi

echo "== 10. 删桶 =="
aws_s3 rb "s3://$BUCKET" --force

echo "M1 S3 验收全部通过"
```

- [ ] **Step 2: 跑脚本**

Run:
```bash
chmod +x tests/e2e_s3.sh
# 先准备好管理员与访问密钥（一次性）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw cargo run -p oss-server &
sleep 2
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"pw"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"access_key":"AKIA_TEST","secret_key":"SK_TEST"}'
echo
# 然后用这把密钥跑脚本
ENDPOINT=http://127.0.0.1:9000 AK=AKIA_TEST SK=SK_TEST ./tests/e2e_s3.sh
kill %1
```
Expected: 脚本打印「M1 S3 验收全部通过」

- [ ] **Step 3: 写 `README.md`** 的运行部分

```markdown
# OSS —— 自研分布式对象存储系统

S3 兼容的自研对象存储（MinIO 的等价实现）。设计文档见
`docs/superpowers/specs/2026-10-10-minio-clone-design.md`。

## 当前进度：M1（后端垂直切片）

已实现：单盘直存内核（无纠删码）· S3 桶/对象 CRUD · ListObjectsV2 · CopyObject ·
批量删除 · SigV4 认证 · aetherupload 分块上传与原生 ingest · 控制台 API。
未实现（见设计文档的阶段表）：纠删码 · 多盘 · 分布式 · 版本控制 · 加密 · 复制 · 权限。

## 跑起来

```bash
# 1. 起服务（首次启动会自动建数据目录、暂存目录、桶目录与管理员）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server

# 2. 登录拿 token（M1 还没有注册界面，账号来自上面的环境变量）
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"changeme"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

# 3. 建一对访问密钥给 S3 客户端用
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{}'
```

> 注意：aetherupload 的分组目录（暂存盘下的 `_header/` 与各分组目录）由
> `aether::build_runtime` 在启动时调用 `aetherupload::console::list_groups` 自动创建；
> 若预处理报笼统的上传错误，先检查暂存盘目录是否被清理。

## 验收

```bash
tests/e2e_s3.sh
```
```

- [ ] **Step 4: 提交**

```bash
git add tests/ README.md && git commit -m "test: M1 端到端验收脚本与运行说明"
```

---

## M1 完成标准（对照设计文档的 M1 验收）

| 设计文档的验收项 | 本计划的覆盖 |
|---|---|
| aws-cli 的 mb/ls/cp/rm 通过 | Task 16 脚本第 1-8 步 |
| rclone 增删查列通过 | Task 16 脚本第 9 步 |
| 浏览器 1GB 上传 + 断线续传 | Task 14 的接线 + aetherupload 自带前端脚本（浏览器侧验证在 M1b 的控制台里做） |
| 秒传命中，0 分块传输 | Task 14 的 `SqlInstantStore`；人工验证：连传两次同一文件，第二次 `chunk_total` 不增长 |
| 控制台可浏览/下载/删除 | Task 15 的 API（界面在 M1b） |
| `curl` 打 preprocess 返回协议一致的 JSON | Task 14 Step 7 |

> **M1 未覆盖、明确留给后续的**（这些是**已知的功能缺口**，不是待办 —— 不要在 M1 里"顺手做了"）：
> - Multipart Upload、Range 请求、大对象流式 PUT（都是 M2；M1 的 S3 PUT 有 256 MiB 内存上限）
> - `x-amz-meta-*` 用户元数据**会被丢弃**（`xl.meta` 的 `user_metadata` 字段已预留，M2 接上）。
>   影响：`aws s3 cp --metadata foo=bar` 静默丢失该元数据
> - S3 面的授权（M4；M1 任何有效 AK 可访问所有桶）
> - `uploads` 表的 `owner_id` 精确归属（M1 只在前端 register 时填，不参与鉴权）
> - 访问密钥的 SK 明文存库（M5 随 SSE 一起改成加密存）

---

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| s3s 0.17 的 DTO 字段名与计划不符 | 计划里已标注「以 `generated.rs` 为准」；Task 1 的编译关卡会第一时间暴露 |
| 依赖树有两份 axum（0.7.9 / 0.8.9） | 已查明并接受：0.7.9 来自 `ecat-errors → tonic 0.12`（必选依赖，仅在 tonic 内部，M1 不经过）；路由面全部 0.8.9（我们 + `ecat-transport-http` + aetherupload），永不碰面。代价仅是编译时间 |
| `Resource::new` 的签名 | 已按真实签名写死（`aetherupload-rust-1.0.3/src/resource.rs:25`，5 参） |
| `block_in_place` 需要 multi-thread runtime | `#[tokio::main]` 默认即是；测试必须用 `#[tokio::test(flavor = "multi_thread")]`（Task 14 已注明）。若将来有人改成 `current_thread`，秒传与 ingest 会 panic —— 这是**刻意的响亮失败**，不做静默降级 |
| aetherupload 的分组目录必须先建 | `aether::build_runtime` 构造时调用 `aetherupload::console::list_groups` 自动创建（Task 14） |
