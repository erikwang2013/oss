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
        // ponytail: 桶级互斥未实现——并发 delete_bucket + put 可让桶「复活」，Task 6/M2 处理
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
        let (s, d) = store().await;
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

        // 盘上数据校验：meta 记录的 part.uuid 目录里，part.1 必须就是写进去的字节
        let obj_dir = layout::object_dir(d.path(), "alpha", "dir/hello.txt").unwrap();
        let on_disk = XlMeta::decode(
            &tokio::fs::read(obj_dir.join(meta::META_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        let part = obj_dir
            .join(&on_disk.latest().unwrap().part.uuid)
            .join("part.1");
        assert_eq!(
            tokio::fs::read(&part).await.unwrap(),
            b"hello world".to_vec()
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_streams_without_buffering_whole_body() {
        // 握手式断言：第一块发出后，盘上 tmp/<uuid>/part.1 必须已含它的字节，之后才放行剩余块。
        // 若实现把整份 body 缓冲到流结束再写，这里的轮询必然等不到而失败——这才是真流式的判别点。
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let s = std::sync::Arc::new(s);
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes>>(1);
        let put = tokio::spawn({
            let s = std::sync::Arc::clone(&s);
            async move {
                s.put_object_stream("alpha", "big.bin", Box::pin(rx), None)
                    .await
            }
        });

        let first = Bytes::from(vec![b'a'; 1024]);
        futures::SinkExt::send(&mut tx, Ok(first.clone()))
            .await
            .unwrap();
        let tmp_root = layout::sys_dir(d.path(), "tmp");
        let mut on_disk = false;
        for _ in 0..200 {
            if let Ok(mut entries) = tokio::fs::read_dir(&tmp_root).await {
                while let Ok(Some(e)) = entries.next_entry().await {
                    if let Ok(bytes) = tokio::fs::read(e.path().join("part.1")).await
                        && bytes == first.as_ref()
                    {
                        on_disk = true;
                        break;
                    }
                }
            }
            if on_disk {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            on_disk,
            "第一块在 body 结束前没有落盘：实现把整份 body 缓冲了"
        );

        // 放行剩余块并收尾
        futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
            .await
            .unwrap();
        futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
            .await
            .unwrap();
        drop(tx);
        let info = put.await.unwrap().unwrap();
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
        // 崩在 3/4 之间的现场：tmp 孤儿 + 一个已被 rename 进对象目录、但无 meta 引用的版本目录。
        // 读者必须仍看到旧版本（这里 k 已有提交版本），绝不能看到未提交的数据。
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let old = s
            .put_object("alpha", "k", Bytes::from_static(b"one"), None)
            .await
            .unwrap();

        let orphan = layout::sys_dir(d.path(), "tmp").join("orphan-uuid");
        tokio::fs::create_dir_all(&orphan).await.unwrap();
        tokio::fs::write(orphan.join("part.1"), b"junk")
            .await
            .unwrap();
        let dangling = layout::object_dir(d.path(), "alpha", "k")
            .unwrap()
            .join("dangling-uuid");
        tokio::fs::create_dir_all(&dangling).await.unwrap();
        tokio::fs::write(dangling.join("part.1"), b"uncommitted")
            .await
            .unwrap();

        let head = s.head_object("alpha", "k").await.unwrap();
        assert_eq!(head.etag, old.etag);
        assert_eq!(head.size, 3);
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

        let (info, body) = s.get_object("alpha", "same").await.unwrap();
        assert_eq!(info.size, 64);
        assert_eq!(body.len(), 64);
        // 读回内容必须是某个写入者一次写入的 64 字节（单一重复值），不能是拼接/交错/截断产生的碎片
        assert!(
            body.iter().all(|b| *b == body[0]),
            "读回内容不是单次完整写入: {body:?}"
        );
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
        //（本键刚 clone 出来，count ≥ 2 必留；其余条目只为 map 自己持有时删掉）
        locks.retain(|_, v| Arc::strong_count(v) > 1);
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
    ///   5. 老版本目录 → `.oss.sys/trash/`（移入 trash，由 GC 回收——M1 未实现回收器）
    ///
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，启动自检回收）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，clean 回收）。读路径只信 xl.meta。
    ///
    /// M1 持久化假设：`fsync=true` 时数据文件与 `xl.meta.new` 都先做 file fsync；
    /// ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可。**没有目录 fsync**，
    /// 换文件系统（或要求断电零丢失）需复核这两条假设。
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

        // M-6 裁决：损坏的 meta 让 PUT fail-closed（报错），不静默当作空对象覆盖——盘上可能还有旧数据，宁可失败也不毁
        let old_meta = self.read_meta(bucket, key).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(&self.cfg.root, "tmp").join(&uuid);
        let now = now_nanos();

        // 1)+2) 数据与候选 meta 落 tmp；任何写失败/流错误都 best-effort 清掉本轮 tmp
        //（未提交前 tmp 不被任何 meta 引用，失败即弃；残留也只占空间，由启动自检回收）
        let staged: Result<(u64, String, XlMeta)> = async {
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
                mod_time_nanos: now,
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
            Ok((size, etag, meta))
        }
        .await;
        let (size, etag, meta) = match staged {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

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
            // best-effort：提交点已过，回收站的任何错误都不该让已提交的 PUT 报失败
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now,
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
- 质量评审收尾轮已并入（I-1 / M-1/2/3/4/5/7/8）：盘上数据断言、trash 分支 best-effort、`now_nanos` 单次取值、tmp 失败清理、`retain` 简化、三个测试改写；M-4 采用**握手式**断言（未走改名退路）。M-6/M-8 裁决记录在文末「已知风险与处置」。

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-store 2>&1 | tail -20`
Expected（已更新为实际）：`xl-store` 34 passed / 0 failed（Task 4 的 26 + 本 task 的 8）；四闸全绿（fmt / clippy 0 告警 / `-p xl-store` / `--workspace -j 2`）。质量评审收尾轮（I-1 / M-1/2/3/4/5/7/8）后仍 34 passed —— 全部为改写既有测试，未新增用例。Step 2 的失败形态是编译错误（`put_object` / `put_object_stream` / `head_object` / `XlMeta` 未定义），与 Task 4 Step 4 注记的「模块级编译不过」同因。

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
- Modify: `crates/xl-store/src/meta.rs`（收尾轮 M-4：`decode` 增加 `part.uuid` 白名单校验）

- [x] **Step 1: 写失败的测试**

```rust
// >>> 追加到 store.rs 的 tests 模块末尾（Task 6 段落，逐字取自最终实现）：
    // ---- Task 6: 读 / 删 / 列举 / 复制 ----

    #[tokio::test]
    async fn get_reads_back_exact_bytes() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"payload"), None)
            .await
            .unwrap();
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
        assert!(matches!(
            s.get_object("alpha", "nope").await,
            Err(Error::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn get_dangling_meta_version_reports_no_such_key() {
        // 并发覆写窗口的现实形态：xl.meta 指向的版本目录已被移走（覆写者提交后把它扔进 trash）。
        // 读路径重读 meta 重试一次后必须报 NoSuchKey —— 不得 panic，也不得把裸 IO 错误抛给上层。
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", Bytes::from_static(b"x"), None)
            .await
            .unwrap();
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        let m = XlMeta::decode(
            &tokio::fs::read(obj_dir.join(meta::META_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        tokio::fs::remove_dir_all(obj_dir.join(&m.latest().unwrap().part.uuid))
            .await
            .unwrap();

        assert!(matches!(
            s.get_object("alpha", "k").await,
            Err(Error::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn list_with_prefix_and_delimiter() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        for k in [
            "a.txt",
            "docs/1.txt",
            "docs/2.txt",
            "docs/deep/3.txt",
            "z.txt",
        ] {
            s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
                .await
                .unwrap();
        }

        let all = s
            .list_objects("alpha", &ListRequest::default())
            .await
            .unwrap();
        assert_eq!(all.objects.len(), 5);
        assert_eq!(all.objects[0].key, "a.txt"); // 字典序
        assert!(!all.is_truncated);

        let prefixed = s
            .list_objects(
                "alpha",
                &ListRequest {
                    prefix: Some("docs/".into()),
                    delimiter: Some("/".into()),
                    ..Default::default()
                },
            )
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
            s.put_object(
                "alpha",
                &format!("k{i}"),
                bytes::Bytes::from_static(b"x"),
                None,
            )
            .await
            .unwrap();
        }
        let page1 = s
            .list_objects(
                "alpha",
                &ListRequest {
                    max_keys: Some(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_token.clone().unwrap();

        let page2 = s
            .list_objects(
                "alpha",
                &ListRequest {
                    max_keys: Some(2),
                    continuation_token: Some(token),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page2.objects[0].key, "k2");
    }

    #[tokio::test]
    async fn copy_object_duplicates_content() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object(
            "alpha",
            "src",
            bytes::Bytes::from_static(b"copy me"),
            Some("text/plain".into()),
        )
        .await
        .unwrap();
        let info = s.copy_object("alpha", "src", "alpha", "dst").await.unwrap();
        assert_eq!(info.size, 7);
        let (_, bytes) = s.get_object("alpha", "dst").await.unwrap();
        assert_eq!(bytes, bytes::Bytes::from_static(b"copy me"));
    }

    // ---- Task 6 收尾轮：delete 语义 / 列举边界 / 桶级竞态 ----

    #[tokio::test]
    async fn delete_then_get_and_head_are_no_such_key() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", Bytes::from_static(b"x"), None)
            .await
            .unwrap();
        s.delete_object("alpha", "k").await.unwrap();
        assert!(matches!(
            s.get_object("alpha", "k").await,
            Err(Error::NoSuchKey(_))
        ));
        assert!(matches!(
            s.head_object("alpha", "k").await,
            Err(Error::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn delete_missing_key_is_ok_and_leaves_others() {
        // S3 语义：删不存在的键返回成功；且不得误伤桶内其他键
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "keep", Bytes::from_static(b"keep"), None)
            .await
            .unwrap();
        s.delete_object("alpha", "nope").await.unwrap();
        let (_, bytes) = s.get_object("alpha", "keep").await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"keep"));
    }

    #[tokio::test]
    async fn delete_then_put_same_key_reads_back() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", Bytes::from_static(b"one"), None)
            .await
            .unwrap();
        s.delete_object("alpha", "k").await.unwrap();
        let info = s
            .put_object("alpha", "k", Bytes::from_static(b"two"), None)
            .await
            .unwrap();
        assert_eq!(info.size, 3);
        let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"two"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_and_deletes_never_observe_torn_state() {
        // put/delete 并发下读路径的不变式：每次读要么是某个写入者一次写入的完整 64 字节，
        // 要么 NoSuchKey —— 不得出现其他 Err（IO 错误），也不得读出碎片内容。
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let s = std::sync::Arc::new(s);
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let s = std::sync::Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                if i % 2 == 0 {
                    s.put_object("alpha", "same", Bytes::from(vec![i; 64]), None)
                        .await
                        .map(|_| ())
                } else {
                    s.delete_object("alpha", "same").await
                }
            }));
        }
        for _ in 0..200 {
            match s.get_object("alpha", "same").await {
                Ok((info, body)) => {
                    assert_eq!(info.size, 64);
                    assert_eq!(body.len(), 64);
                    assert!(body.iter().all(|b| *b == body[0]), "碎片内容: {body:?}");
                }
                Err(Error::NoSuchKey(_)) => {}
                Err(e) => panic!("读路径不应出现其他错误: {e}"),
            }
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn list_delimiter_with_pagination_no_duplicate_prefix() {
        // 复现（评审）：max=1 时，已发过的 common prefix 若仍计入容量会被再次截断，
        // token 原地打转（下一页空页或重复 cp）。去重检查必须在容量判断之前。
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        for k in ["a", "b/x", "b/y"] {
            s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
                .await
                .unwrap();
        }
        let req = |token: Option<String>| ListRequest {
            max_keys: Some(1),
            delimiter: Some("/".into()),
            continuation_token: token,
            ..Default::default()
        };

        let page1 = s.list_objects("alpha", &req(None)).await.unwrap();
        assert_eq!(page1.objects.len(), 1);
        assert!(page1.common_prefixes.is_empty());
        assert!(page1.is_truncated);
        let token = page1.next_token.clone().unwrap();

        let page2 = s.list_objects("alpha", &req(Some(token))).await.unwrap();
        assert!(page2.objects.is_empty());
        assert_eq!(page2.common_prefixes, vec!["b/".to_string()]);
        assert!(
            !page2.is_truncated && page2.next_token.is_none(),
            "已发过的 cp 不应再触发截断（否则客户端拿同一个 token 空转）"
        );
    }

    #[tokio::test]
    async fn list_max_keys_zero_is_empty_page() {
        // max-keys=0：空页且不截断（报截断会让客户端拿同一个 token 空转）
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"x"), None)
            .await
            .unwrap();
        let r = s
            .list_objects(
                "alpha",
                &ListRequest {
                    max_keys: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(r.objects.is_empty() && r.common_prefixes.is_empty());
        assert!(!r.is_truncated);
        assert!(r.next_token.is_none());
    }

    #[tokio::test]
    async fn list_empty_delimiter_is_ignored() {
        // delimiter="" 视同未指定：否则 find("") 命中每个键的首字节，整份键都被当 common prefix
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        for k in ["a/b", "c"] {
            s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
                .await
                .unwrap();
        }
        let r = s
            .list_objects(
                "alpha",
                &ListRequest {
                    delimiter: Some(String::new()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.objects.len(), 2);
        assert!(r.common_prefixes.is_empty());
    }

    #[tokio::test]
    async fn collect_keys_on_vanished_bucket_is_no_such_bucket() {
        // 桶被并发删除的窗口：read_dir 命中 NotFound 必须映射 NoSuchBucket，而不是裸 IO 错误
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        std::fs::remove_dir(layout::bucket_dir(d.path(), "alpha")).unwrap();
        assert!(matches!(
            s.collect_keys("alpha").await,
            Err(Error::NoSuchBucket(_))
        ));
    }
```

```rust
// >>> 追加到 meta.rs 的 tests 模块末尾（Task 6 收尾轮 M-4，逐字取自最终实现）：
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
```

注（可编译前提已就绪）：本块用到的 `put_object` / `head_object` 已随 Task 5 落地，可直接编译；本 Step 新引入的是 `get_object` / `list_objects` / `copy_object` / `ListRequest`。

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-store 2>&1 | tail -20`
Expected: 编译错误（`get_object` / `list_objects` / `copy_object` / `ListRequest` 未定义）
实际：`error: could not compile`（14 处 E0422/E0433/E0599：`get_object` / `list_objects` / `copy_object` / `ListRequest` 未定义）。

- [x] **Step 3: 实现读 / 删 / 列举 / 复制**

注（勿重复添加）：本块里的 `load_object_info` 与 `head_object` 已在 Task 5 落地（Task 5 的测试需要「提交后是否可见」的探针），实现 Task 6 时**跳过这两个函数**；其余（`ListRequest` / `ListResult` / `get_object` / `delete_object` / `list_objects` / `copy_object`）照常实现。

```rust
// >>> 模块级新增（`ObjectInfo` 之后）：
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

// >>> `impl Store` 内 `head_object` 之后（`load_object_info` / `head_object` 已在 Task 5 落地，此处不重复）：
    /// 读对象：一次 meta 读取定 info 与数据文件位置。
    ///
    /// 读路径不加锁，meta 与数据文件之间隔着一次 open：覆写者在提交点之后才把老版本目录移进
    /// trash，所以先读到的 meta 可能指向刚被移走的版本目录（数据文件 ENOENT）。此时重读一次
    /// meta 拿新版本再试；两轮都读不到 = 对象确实不在（或被并发删除）→ NoSuchKey。
    /// ponytail: 固定重试一次；持续覆写风暴下不保证收敛（M2 快照读解决）。
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;

        for _ in 0..2 {
            let m = self
                .read_meta(bucket, key)
                .await?
                .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
            let info = self.load_object_info(bucket, key, &m)?;
            let path = dir.join(&v.part.uuid).join(format!("part.{}", v.part.part));
            // ponytail: M1 整份读进内存；M2 换流式（大对象与 Range 是 M2 的事）。
            match tokio::fs::read(&path).await {
                Ok(bytes) => return Ok((info, Bytes::from(bytes))),
                // 旧版本目录已被并发覆写者移走：下一轮重读 meta 拿新版本
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        // 两轮 meta → 数据文件都 ENOENT：按 S3 语义报 NoSuchKey，而不是把裸 IO 错误抛给上层
        Err(Error::NoSuchKey(key.to_string()))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        // 与 put 同键串行：删除进行中不得有写者把新版本 rename 进来（否则版本目录会被连带删掉）
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
        let max = req.max_keys.unwrap_or(DEFAULT_MAX_KEYS).min(1000);
        // max-keys=0：空页且不截断（报截断会让客户端拿同一个 token 空转）
        if max == 0 {
            return Ok(ListResult::default());
        }
        // 空 delimiter 视同未指定：find("") 命中每个键的首字节，会把整份键都当 common prefix
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());

        let mut keys = self.collect_keys(bucket).await?;
        keys.sort();

        let prefix = req.prefix.clone().unwrap_or_default();
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
            if let Some(d) = delimiter {
                let rest = &k[prefix.len()..];
                if let Some(pos) = rest.find(d) {
                    let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
                    // 去重检查在容量判断之前：已发过的 cp 不再占位、也不触发截断，
                    // 否则下一页会拿同一个 token 原地打转（空页或重复 cp）
                    if !common_prefixes.contains(&cp) {
                        if objects.len() + common_prefixes.len() >= max {
                            is_truncated = true;
                            next_token = Some(k.clone());
                            break;
                        }
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
        Ok(ListResult {
            objects,
            common_prefixes,
            is_truncated,
            next_token,
        })
    }

    async fn collect_keys(&self, bucket: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut entries =
            match tokio::fs::read_dir(layout::bucket_dir(&self.cfg.root, bucket)).await {
                Ok(e) => e,
                // 桶被并发删除的窗口：映射 NoSuchBucket，而不是把裸 IO 错误抛给上层
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::NoSuchBucket(bucket.to_string()));
                }
                Err(e) => return Err(e.into()),
            };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            keys.push(layout::key_from_dir_name(&name)?);
        }
        Ok(keys)
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ObjectInfo> {
        let (info, bytes) = self.get_object(src_bucket, src_key).await?;
        self.put_object(dst_bucket, dst_key, bytes, info.content_type)
            .await
    }

// >>> crates/xl-store/src/meta.rs（收尾轮 M-4）：`XlMeta::decode` 的改动（`impl` 内其余既有内容不动）：
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
        }
        Ok(m)
    }

// >>> 同上，模块级新增（`impl XlMeta` 之外）：
/// 版本目录名的宽松白名单：非空、无路径分隔符、无 `..`、无控制字符。
fn part_uuid_is_safe(uuid: &str) -> bool {
    !uuid.is_empty()
        && !uuid.contains('/')
        && !uuid.contains('\\')
        && !uuid.contains("..")
        && !uuid.chars().any(|c| c.is_control())
}
```

注（与最初骨架的差异，已裁决；上方代码块已同步为最终实现形态）：
- `get_object` 的读路径并发防护（质量评审挂载项）：原骨架是 `head_object` + 再读一次 `read_meta().unwrap().latest().unwrap()`，既有 `unwrap` panic 路径，也在「meta 已换、旧版本目录被并发覆写者移进 trash」的窗口里把数据文件 ENOENT 原样抛给上层。最终形态：**一次 meta 读取定 info 与数据文件位置；数据文件 ENOENT 时重读 meta 重试一次，两轮都 ENOENT → `NoSuchKey`**（无 panic；其他 IO 错误照旧上抛）。`ponytail:` 注释标明固定一次重试，M2 快照读收口。
- 测试补 `get_dangling_meta_version_reports_no_such_key`：手工构造「xl.meta 指向已被移走的版本目录」，断言 `NoSuchKey`（不 panic）。
- `concurrent_puts_to_same_key_serialize` 末尾补读回校验（挂载项）：`get_object` 读回的 size/长度与写入一致、且内容是某个写入者一次写入的完整 64 字节（单一重复值）。该测试的最终形态已同步进 Task 5 的 Step 1 块。
- Task 5 留在 `load_object_info` 上的过渡注释（「get / delete / list / copy 仍留给 Task 6」）已随本 task 移除。
- 文档挂载项：删除两处孤立的裸 ` ``` `（原 1253 / 1513 行，导致 Step 2/3 文字与实现块渲染破损）；收尾轮处置 Task 16 验收区的第三处（原约 3943 / 3944 行）：外层 README 块改用 4 反引号围栏、嵌套内嵌的 3 反引号 bash 块（未删任何行）。

注（质量评审收尾轮，I-1 / M-2/3/4 与两条观察，已并入；上方代码块已同步）：
- **I-1 delete 测试**（补 4 条）：`delete_then_get_and_head_are_no_such_key`（put→delete→get/head 均 NoSuchKey）、`delete_missing_key_is_ok_and_leaves_others`（删不存在的键 → Ok 且不误伤其他键）、`delete_then_put_same_key_reads_back`（删后重写同键可读）、`concurrent_puts_and_deletes_never_observe_torn_state`（multi_thread 下 put/delete 与 200 次并发读：每次读要么单写者完整 64 字节、要么 NoSuchKey，绝无其他 Err/碎片）。
- **M-2 delimiter 去重与容量的顺序**：`contains` 去重检查提到容量判断之前——已发过的 cp 不再占位、也不触发截断，否则下一页拿同一个 token 原地打转（复现用例 `list_delimiter_with_pagination_no_duplicate_prefix`：keys=[a, b/x, b/y]、delim=/、max=1）。
- **M-3 列举边界**：`max_keys=Some(0)` → 空页且 `is_truncated=false`、无 token（防客户端空转，`list_max_keys_zero_is_empty_page`）；`delimiter=Some("")` 视同未指定（`list_empty_delimiter_is_ignored`，否则 `find("")` 命中首字节、整份键被当 cp）。
- **M-4 `part.uuid` 白名单校验**（meta.rs，本 task 第二个被改的文件）：`XlMeta::decode` 末尾拒绝含 `/`、`\`、`..`、空或控制字符的 `part.uuid`（宽松形态，不按 UUID 格式收严；fixture `"u"` 与非 UUID 历史数据仍可读），测试 `decode_rejects_unsafe_part_uuid`。Task 3 的 Step 3 块保持骨架形态未回改，最终 decode 见上方 `// >>>` 片段。
- **桶级竞态**（评审观察）：`collect_keys` 的 `read_dir` 遇 NotFound（桶被并发删除）映射 `NoSuchBucket` 而非裸 Io，测试 `collect_keys_on_vanished_bucket_is_no_such_bucket`。
- 记账项：`delete_bucket` 的 ponytail 注释「Task 6/M2 处理」改为「M2 处理」；「已知风险与处置」表补「删除路径残留」一行。

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-store 2>&1 | tail -12`
Expected: 全绿
实际：`xl-store` 49 passed / 0 failed。构成：Task 4/5 的 34 + 本 task 主轮 6（`get_reads_back_exact_bytes` / `get_missing_key` / `get_dangling_meta_version_reports_no_such_key` / `list_with_prefix_and_delimiter` / `list_pagination` / `copy_object_duplicates_content`；`concurrent_puts_to_same_key_serialize` 为原地增强，未新增用例）+ 收尾轮 9（store.rs 8：delete 4 条 / delimiter 分页 / max-keys=0 / 空 delimiter / 桶级竞态；meta.rs 1：坏 `part.uuid`）。四闸全绿（fmt / clippy 0 告警 / `-p xl-store` / `--workspace -j 2`）。

- [x] **Step 5: 提交**

```bash
git add crates/xl-store/ && git commit -m "feat(xl-store): 读/删/列举（prefix+delimiter+分页）/复制"
```

---

## Task 7: oss-server —— 控制面 schema 与仓储

**Files:**
- Create: `crates/oss-server/migrations/0001_init.sql`
- Create: `crates/oss-server/src/db.rs`

- [x] **Step 1: 写 schema**

```sql
-- M1 五张表。对象数据永远不进 SQL —— 这里只有控制面状态。
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0,  -- M1 只写 0/1（创建时定）；不加 CHECK，M4 引角色时再演进
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS access_keys (
    access_key  TEXT PRIMARY KEY,
    secret_key  TEXT NOT NULL,           -- M1 明文存；M5 随 SSE 一起改为加密存
    user_id     INTEGER NOT NULL REFERENCES users(id),
    status      TEXT NOT NULL DEFAULT 'active',  -- 只有 'active' 能通过 xl-s3/src/auth.rs 的 AK→SK 查询
    expires_at  INTEGER,                 -- NULL = 永不过期；M5 的 STS 会话用同一列
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name          TEXT PRIMARY KEY,
    owner_id      INTEGER REFERENCES users(id),
    creation_date INTEGER NOT NULL,
    versioning    TEXT NOT NULL DEFAULT 'Off',  -- M4 用
    object_lock   INTEGER NOT NULL DEFAULT 0    -- M1 只写 0；M5 用。不加 CHECK
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

-- ponytail: 过期行只增不删、expires_at 无索引；升级：set 时顺手清过期行，或加索引 + 周期清扫。
CREATE TABLE IF NOT EXISTS instant_index (
    k          TEXT PRIMARY KEY,
    v          TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
```

- [x] **Step 2: 写失败的测试**

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
        assert!(db.register_upload("g_202610_abc.txt", "my.txt", None).await.unwrap());
        let rows = db.list_uploads(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].original_name.as_deref(), Some("my.txt"));
    }

    // ---- Task 7 收尾轮（I-1 / M-1）新增 ----

    #[tokio::test]
    async fn register_upload_without_record_is_false() {
        let db = db().await;
        assert!(!db.register_upload("nope.txt", "my.txt", None).await.unwrap());
    }

    // （已回撤：Task 9 收尾轮删除，生产路径由 xl-s3 接管）——本测试与下方的 rejects_inactive
    // 均已随 `secret_for_access_key` 一并删除；`user_and_access_key_roundtrip` 改为直查断言。
    #[tokio::test]
    async fn secret_for_access_key_rejects_expired() {
        let db = db().await;
        let uid = db.create_user("erik", "secret", true).await.unwrap();
        db.create_access_key("AKIA_EXPIRED", "SK_TEST", uid).await.unwrap();
        sqlx::query("UPDATE access_keys SET expires_at = ? WHERE access_key = ?")
            .bind(now() - 1).bind("AKIA_EXPIRED").execute(db.pool()).await.unwrap();
        assert_eq!(db.secret_for_access_key("AKIA_EXPIRED").await.unwrap(), None);
    }

    #[tokio::test]
    async fn secret_for_access_key_rejects_inactive() {
        let db = db().await;
        let uid = db.create_user("erik", "secret", true).await.unwrap();
        db.create_access_key("AKIA_OFF", "SK_TEST", uid).await.unwrap();
        sqlx::query("UPDATE access_keys SET status = 'inactive' WHERE access_key = ?")
            .bind("AKIA_OFF").execute(db.pool()).await.unwrap();
        assert_eq!(db.secret_for_access_key("AKIA_OFF").await.unwrap(), None);
    }
}
```

- [x] **Step 3: 跑测试确认失败**

Run: `cargo test -p oss-server db 2>&1 | tail -20`
Expected: 编译错误（`Db` 未定义）
实际：`error: could not compile`（2 处 E0425/E0433，均为 `Db` 未定义）。

- [x] **Step 4: 实现 `db.rs`**

```rust
//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
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

// migrations/ 只是目录命名习惯：本实现用 include_str! + raw_sql 原样执行整份 schema，
// 不建 _sqlx_migrations —— 将来加 0002 不会自动执行（M2 起再引 sqlx migrate）。
const SCHEMA: &str = include_str!("../migrations/0001_init.sql");

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        // WAL 走连接选项、不走 URL：sqlx 0.8 的 URL 解析只认 mode/cache/immutable/vfs，
        // `?journal_mode=WAL` 会直接报 unknown query parameter。选项形式对池内每条新连接生效。
        let opts = url
            .parse::<SqliteConnectOptions>()?
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(opts).await?;
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
        // ponytail: 非常量时间比较 + 用户存在性时序（未命中不比对）M1 已接受；M4 换 Argon2 时随方案处置。
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

    // （已回撤：Task 9 收尾轮删除，生产路径由 xl-s3 接管）
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
        let rows = sqlx::query("SELECT access_key FROM access_keys WHERE user_id = ? ORDER BY created_at, access_key")
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

    /// 返回是否命中：0 行命中 = saved_path 不存在（调用方按 404 处理），不静默吞掉 original_name（规格 §5.4）。
    pub async fn register_upload(&self, saved_path: &str, original_name: &str, owner_id: Option<i64>) -> Result<bool> {
        let n = sqlx::query("UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = ?")
            .bind(original_name)
            .bind(owner_id)
            .bind(saved_path)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n == 1)
    }

    pub async fn list_uploads(&self, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = sqlx::query("SELECT * FROM uploads ORDER BY created_at DESC, saved_path LIMIT ?")
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

- [x] **Step 5: 补依赖并跑测试**

`crates/oss-server/Cargo.toml` 的 `[dependencies]` 加 `hex = { workspace = true }`；`[dev-dependencies]` 加 `sqlx = { workspace = true }`。

Run: `cargo test -p oss-server db 2>&1 | tail -12`
Expected: 全绿（4 个测试）
实际：主轮 `oss-server` 4 passed；收尾轮补 3 条后 7 passed / 0 failed（`init_creates_tables` / `user_and_access_key_roundtrip` / `instant_index_expires` / `upload_records` / `register_upload_without_record_is_false` / `secret_for_access_key_rejects_expired` / `secret_for_access_key_rejects_inactive`）。四闸全绿（fmt / clippy 0 告警 / `-p oss-server` / `--workspace -j 2`；workspace 合计 56 = xl-store 49 + oss-server 7）。

- [x] **Step 6: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): 控制面 5 张表与仓储"
```

注（Task 7 执行偏差，仅此一处）：提交范围在 `git add crates/oss-server/` 之外连带根 `Cargo.lock` —— 新增 `hex` 依赖使 oss-server 的锁文件清单变化，单独 add crate 会把 `Cargo.lock` 留在脏区。实现代码与计划逐字一致（rustfmt 仅换行）；`sqlite::memory:` 的池内共享由 sqlx 0.8.6 保证（自动 `shared_cache` + 每池唯一库名），测试建库方式与计划相同。

注（Task 7 质量评审收尾轮，I-1 裁决 + Minor，已并入；上方代码块已同步）：
- **I-1 `register_upload` 返回命中数**：签名改 `Result<bool>`（`execute()?.rows_affected() == 1`）。0 行命中 = `saved_path` 不存在，调用方按 404 处理（不允许静默吞掉 `original_name`，规格 §5.4）。测试补 `register_upload_without_record_is_false`；Task 15 的 register 路由片段同步改为 `Ok(true) → 204 / Ok(false) → 404 / Err(e) → 500`（收尾轮裁决修订 Err 分支：DB 故障是服务端故障，与错误分档口径一致；原「Err(e) → 400」措辞已改正，见 Task 15 收尾轮定稿记录）。
- **M-1**：`secret_for_access_key` 补两条测试——`secret_for_access_key_rejects_expired`（`expires_at` 在过去 → None）/ `secret_for_access_key_rejects_inactive`（`status='inactive'` → None）。
- **M-2**：`verify_user` 加 `ponytail:` 注释（非常量时间比较 + 用户存在性时序，M1 已接受；M4 换 Argon2 时随方案处置）。
- **M-4**：`instant_index` 建表处加 `ponytail:` 注释（过期行只增不删、`expires_at` 无索引；升级：set 时顺手清过期行 / 加索引 + 周期清扫）。
- **M-5**：`access_keys.status` / `users.is_admin` / `buckets.object_lock` 各加一行取值域注释，不加 CHECK，M4/M5 自然再演进。
- **M-6**：两处 ORDER BY 加兜底列（`created_at, access_key` / `created_at DESC, saved_path`）保证同秒全序。
- **M-7**：`SCHEMA` 常量旁注明 `migrations/` 目录名与本实现（`include_str!` + `raw_sql`，无 `_sqlx_migrations`）的差异——将来加 0002 不会自动执行。
- **M-8**（记账，未改）：`oss-server` 的 `sqlx` 同时列在 `[dependencies]` 与 `[dev-dependencies]`（本计划 Step 5 的产物，无行为影响——Cargo 特性并集）；留到收尾清理时删除 dev 侧重复行。

注（Task 8 回填）：上方 Step 4 的 `Db::connect` 代码块已同步为最终形态——`SqlitePool::connect(url)` 换为 `SqliteConnectOptions` + `journal_mode(Wal)` + `connect_with`（WAL 挂账项的裁决落地，理由与实证见 Task 8 注记）；Task 7 实施时该两行尚未存在，其余逐字未动。

---

## Task 8: oss-server —— 配置、AppState、e-cat 起服务

**Files:**
- Create: `crates/oss-server/src/config.rs`, `state.rs`
- Create: `crates/oss-server/src/main.rs`
- Create: `oss.toml`

- [x] **Step 1: 写 `config.rs`**

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
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("读取配置文件 {} 失败: {e}", path.display()))?;
        Ok(toml::from_str(&text)?)
    }

    pub fn bucket_for_group(&self, group: &str) -> Option<&str> {
        self.groups.iter().find(|g| g.group == group).map(|g| g.bucket.as_str())
    }
}
```

- [x] **Step 2: 写 `oss.toml`**

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

- [x] **Step 3: 写 `state.rs`**

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

- [x] **Step 4: 写失败的冒烟测试**（`crates/oss-server/tests/smoke.rs`）

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

- [x] **Step 5: 跑测试确认失败**

Run: `cargo test -p oss-server --test smoke 2>&1 | tail -20`
Expected: 编译错误（`build_router` 未定义）

- [x] **Step 6: 实现 `router.rs` 的最小版本**（M1 后续任务往这里加）

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

- [x] **Step 7: 写 `main.rs`**

```rust
use ecat::App;
use ecat::transport_http::HttpServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 先初始化日志：App::run() 里也有一次 init（has_been_set 守卫，不会重复），
    // 但启动自举（管理员引导/建桶）发生在 run() 之前，不提前 init 这些日志会被静默丢弃。
    ecat_logging::init();

    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "oss.toml".to_string());
    let cfg = oss_server::config::AppConfig::load(std::path::Path::new(&cfg_path))?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    std::fs::create_dir_all(&cfg.staging_dir)?;

    let db = oss_server::db::Db::connect(&cfg.database_url).await?;
    let store =
        xl_store::store::Store::open(xl_store::store::StoreConfig::new(&cfg.data_dir)).await?;

    // M1 还没有注册界面：用环境变量引导出第一个管理员（幂等，已存在则跳过）。
    // OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 未设置时不做任何事。
    // 空凭据一律拒绝建号：空口令账号会被空口令登录命中（M1 的 verify_user 只做哈希比对，无口令强度校验）。
    match (
        std::env::var("OSS_ADMIN_USER"),
        std::env::var("OSS_ADMIN_PASSWORD"),
    ) {
        (Ok(user), Ok(pass)) if !user.is_empty() && !pass.is_empty() => {
            // 已存在的同名账号一律不覆盖（含口令不同的情况）
            if !db.user_exists(&user).await? {
                db.create_user(&user, &pass, true).await?;
                tracing::info!(user = %user, "创建管理员账号");
            }
        }
        (Ok(user), Ok(pass)) => {
            tracing::warn!(
                user_empty = user.is_empty(),
                pass_empty = pass.is_empty(),
                "管理员凭据为空字符串，跳过引导（空口令账号会被空登录命中）"
            );
        }
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
            tracing::warn!("OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 只设置了一个，跳过管理员引导");
        }
        (Err(_), Err(_)) => {}
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

- [x] **Step 8: 跑测试 + 手动起服务**

Run:
```bash
cargo test -p oss-server --test smoke 2>&1 | tail -8
cargo run -p oss-server 2>&1 | head -5 &
sleep 2 && curl -s localhost:9000/health/live && echo && kill %1
```
Expected: 测试通过；`curl` 输出 `ok`
实际：smoke 1 passed；主轮 `-p oss-server` 7 + 1 passed；`cargo test --workspace -j 2` 57 passed（xl-store 49 + oss-server 7 + smoke 1）。起服务改造为直接跑 `target/debug/oss-server`（与 `cargo run` 同一产物，避免 cargo 转发信号的不确定性），`curl -s -w " [http %{http_code}]\n" localhost:9000/health/live` → `ok [http 200]`；追加验证 SIGTERM 优雅停机（日志 `received SIGTERM` → `shutting down` → 退出，9000 端口释放）。

- [x] **Step 9: 提交**

```bash
git add crates/oss-server/ oss.toml && git commit -m "feat(server): e-cat 起服务 + /health/live 冒烟"
```

注（Task 8 执行偏差，两处 + WAL 挂账项并入）：
- **WAL（并入的挂账项）方案变更**：`database_url` 带 `?journal_mode=WAL` 在 sqlx 0.8.6 上不可行——URL 解析只认 4 个参数 `mode`/`cache`/`immutable`/`vfs`（源码 `sqlx-sqlite-0.8.6/src/options/parse.rs` 实证，其余 key 一律 `Error::Configuration`），实测起服务直接报 `Configuration("unknown query parameter \`journal_mode\` while parsing connection URL")`。**WAL 经连接选项而非 URL 参数**：`Db::connect` 用 `url.parse::<SqliteConnectOptions>()?.journal_mode(SqliteJournalMode::Wal)` + `SqlitePool::connect_with`（对池内每条新连接生效；`mode=rwc` 的 create_if_missing 语义由 from_str 保留）。`oss.toml` 维持 `sqlite://./oss.db?mode=rwc`（即上方代码块，最终形态已同步）。实测：起服务后 `PRAGMA journal_mode` → `wal`，`oss.db-wal` 落盘；既有 7 个 db 测试全绿（connect_with 路径无回归）。**以后不要再往 `database_url` 加 SQLite 参数**——sqlx 0.8 只认上述 4 个。`db.rs` 这两行属本 task 裁决范围，随 Task 8 提交并入（Task 7 的 Step 4 代码块已同步，见其注记）。
- **`main.rs` 管理员引导两次调整（上方 Step 7 代码块已是最终形态）**：首轮因 clippy `collapsible_if`（0 告警闸）把计划原文的嵌套 `if let` 收成 edition 2024 let-chain；收尾轮再按质量评审 Minor 1/3 改为带空凭据守卫的 `match`（见下条）。另按计划提示删掉了未使用的 `use std::sync::Arc;`；rustfmt 仅换行。

注（Task 8 质量评审收尾轮，Minor 1/3/5/7；上方 Step 1 / Step 7 代码块已同步为最终形态）：
- **Minor 1（信任边界，必修）**：管理员引导加空凭据守卫——`OSS_ADMIN_USER` / `OSS_ADMIN_PASSWORD` 任一为空字符串时**不建号**并 `warn!`（记 `user_empty`/`pass_empty` 两个字段）；只设置了一个变量时同样 `warn!`，不静默。理由进了注释：空口令账号会被空口令登录命中（M1 的 `verify_user` 只做哈希比对，无口令强度校验）。
- **Minor 3**：幂等条件删掉冗余的 `verify_user(&user, &pass).await?.is_none()`——与 `!user_exists(&user).await?` 合取时恒等价于后者；注释改为「已存在的同名账号一律不覆盖（含口令不同的情况）」。
- **Minor 5**：`AppConfig::load` 的 io 错误 `map_err` 带上文件路径（`读取配置文件 {path} 失败: {e}`）；`toml::from_str` 的解析错误保持原样（自带行列号）。
- **Minor 7**：根 `.gitignore` 在「运行产物」块补 `staging/` 与 `/oss.db*`（`oss.toml` 指向的运行产物，实测此前未被忽略；`data/` 原本已在）。`.gitignore` 不属计划代码块，未回填。
- **记账（本轮不动）**：Minor 2（分组名下划线校验入 `config` load）与 6a（仓库根 `oss.toml` 的解析测试）并入 Task 14 派发清单；Minor 4（双启竞态）与 6b/6c 入收尾清单。
- **追加（本轮验证发现的必修项，上方 Step 7 代码块已同步）**：引导阶段（`App::run()` 之前）的 `tracing::*` 事件原被静默丢弃——subscriber 由 `App::run()` 内部的 `ecat_logging::init()` 安装。实测证据：有效凭据场景用户确实建了（users=1）但「创建管理员账号」不出现在日志。修法：`main` 开头先 `ecat_logging::init()`（`App::run()` 有 `has_been_set()` 守卫，不会重复初始化、不 panic），`oss-server` 加直接依赖 `ecat-logging = "7.0.0"`（与 ecat 7.0.0 所用同版本，Cargo.lock 内唯一）。修后 Minor 1 的「应 warn」冒烟可观测（空凭据 → WARN 行 + users=0）。

---

## Task 9: xl-s3 —— SigV4 凭据（`S3Auth`）

**Files:**
- Create: `crates/xl-s3/src/auth.rs`
- Create: `crates/xl-s3/src/convert.rs`
- 收尾轮另修（评审 5 条 Minor）：`crates/xl-store/src/layout.rs`、`crates/oss-server/src/db.rs`、`crates/oss-server/migrations/0001_init.sql`

- [x] **Step 1: 写失败的测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// 最小控制面 schema 的内联建库：`xl-s3` 不能反向依赖 `oss-server`，
    /// 列与 `oss-server/migrations/0001_init.sql` 对齐（只取本任务用到的列）。
    async fn oss_test_db() -> sqlx::SqlitePool {
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

    async fn insert_user(pool: &SqlitePool) {
        sqlx::query("INSERT INTO users (id, username) VALUES (1, 'u')")
            .execute(pool)
            .await
            .unwrap();
    }

    async fn insert_key(pool: &SqlitePool, ak: &str, sk: &str, expires_at: Option<i64>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at) \
             VALUES (?, ?, 1, ?, 0)",
        )
        .bind(ak)
        .bind(sk)
        .bind(expires_at)
        .execute(pool)
        .await
        .unwrap();
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// 负例锁死错误码：认证失败的对外形态只有 InvalidAccessKeyId。
    #[track_caller]
    fn assert_invalid_ak(err: &S3Error) {
        assert!(
            matches!(err.code(), S3ErrorCode::InvalidAccessKeyId),
            "期望 InvalidAccessKeyId，实得 {:?}",
            err.code()
        );
    }

    #[tokio::test]
    async fn known_access_key_resolves() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_TEST", "SK_TEST", None).await;
        let auth = SigV4Auth::new(pool);
        assert_eq!(
            auth.get_secret_key("AKIA_TEST").await.unwrap().expose(),
            "SK_TEST"
        );
    }

    #[tokio::test]
    async fn future_expiry_access_key_resolves() {
        // 钉住秒单位与比较方向（expires_at > now 才通过）；M5 的 STS 会话用同一列
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_TTL", "SK_TTL", Some(now() + 3600)).await;
        let auth = SigV4Auth::new(pool);
        assert_eq!(
            auth.get_secret_key("AKIA_TTL").await.unwrap().expose(),
            "SK_TTL"
        );
    }

    #[tokio::test]
    async fn unknown_access_key_is_rejected() {
        let auth = SigV4Auth::new(oss_test_db().await);
        assert_invalid_ak(&auth.get_secret_key("NOPE").await.unwrap_err());
    }

    #[tokio::test]
    async fn inactive_or_expired_access_key_is_rejected() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at) \
             VALUES ('AKIA_OFF', 'SK_OFF', 1, 'inactive', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        insert_key(&pool, "AKIA_OLD", "SK_OLD", Some(1)).await;
        let auth = SigV4Auth::new(pool);
        assert_invalid_ak(&auth.get_secret_key("AKIA_OFF").await.unwrap_err());
        assert_invalid_ak(&auth.get_secret_key("AKIA_OLD").await.unwrap_err());
    }

    #[tokio::test]
    async fn db_error_maps_to_internal_error() {
        let pool = oss_test_db().await;
        sqlx::query("DROP TABLE access_keys")
            .execute(&pool)
            .await
            .unwrap();
        let auth = SigV4Auth::new(pool);
        let err = auth.get_secret_key("AKIA_TEST").await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("internal error"), "5xx 不泄漏内部细节");
    }
}
```

> **调和注记（实现时定稿）**：原稿的 `oss_test_db()` 返回 `db.create_user(...)/db.pool()` 形式的对象，但那等价于 `oss-server::db::SqlxClient`，而 `xl-s3` 不能反向依赖 `oss-server`。定稿：`oss_test_db()` 直接返回 `sqlx::SqlitePool`（内联最小 schema，见上），用户/密钥数据用 `sqlx::query` 直接 INSERT，列与 `oss-server/migrations/0001_init.sql` 一致（`status` 只用 `'active'`/`'inactive'`，见 `db.rs` 的 revoke）。`xl-s3` 的 dev-dependencies 需要 `tokio`、`tempfile`（已在 workspace 骨架里）。收尾轮按评审补：`inactive_or_expired`（覆盖查询里的 `status='active'`/`expires_at` 谓词——撤销的密钥若能通过认证是认证绕过）、`future_expiry`（未来过期必须通过）、`db_error`（DB 故障 → InternalError），负例全部锁 `InvalidAccessKeyId`。

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 auth 2>&1 | tail -20`
Expected: 编译错误（`SigV4Auth` 未定义，实测 3 处 E0433）

- [x] **Step 3: 实现 `auth.rs`**

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

// ponytail: 每请求查一次 AK、无缓存 —— SQLite 读不阻塞、撤销即时生效；量大再加缓存。
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
        .map_err(|e| {
            tracing::error!(error = ?e, access_key, "access key lookup failed");
            S3Error::with_message(S3ErrorCode::InternalError, "internal error")
        })?;

        row.map(|r| SecretKey::from(sqlx::Row::get::<String, _>(&r, "secret_key")))
            .ok_or_else(|| {
                S3Error::with_message(S3ErrorCode::InvalidAccessKeyId, "unknown access key")
            })
    }
}

#[cfg(test)]
mod tests {
    // 见 Step 1
}
```

> 收尾轮改动：DB 错误细节（含 SQL 文案）只进 `tracing::error!`，响应体统一 `internal error`；`InvalidAccessKeyId` 文案保留。已核实 `SecretKey::from(String)`、`SecretKey::expose()`、`S3Error::with_message` 与全部 `S3ErrorCode` 变体均为 s3s 0.17.0 真实 API（源码核对，无臆测）。

- [x] **Step 4: 实现 `convert.rs` 的错误映射**

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
        // s3s 没有 InvalidObjectName，键名不合法归 InvalidArgument
        StoreError::InvalidKey(_) => S3ErrorCode::InvalidArgument,
        StoreError::InvalidBucketName(_) => S3ErrorCode::InvalidBucketName,
        // 内部错误：细节（盘上路径、内部文案）只进日志，不进 5xx 响应体
        StoreError::CorruptMeta(_) | StoreError::Encode(_) | StoreError::Io(_) => {
            tracing::error!(error = ?e, "store internal error");
            return S3Error::with_message(S3ErrorCode::InternalError, "internal error");
        }
    };
    S3Error::with_message(code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 4xx 是可纠正错误：原消息对客户端有指导意义，保留。
    #[test]
    fn client_errors_keep_original_message() {
        let cases = [
            (
                StoreError::NoSuchBucket("b".into()),
                S3ErrorCode::NoSuchBucket,
                "bucket not found: b",
            ),
            (
                StoreError::BucketAlreadyExists("b".into()),
                S3ErrorCode::BucketAlreadyOwnedByYou,
                "bucket already exists: b",
            ),
            (
                StoreError::BucketNotEmpty("b".into()),
                S3ErrorCode::BucketNotEmpty,
                "bucket not empty: b",
            ),
            (
                StoreError::NoSuchKey("k".into()),
                S3ErrorCode::NoSuchKey,
                "key not found: k",
            ),
            (
                StoreError::InvalidKey("k".into()),
                S3ErrorCode::InvalidArgument,
                "invalid key: k",
            ),
            (
                StoreError::InvalidBucketName("b".into()),
                S3ErrorCode::InvalidBucketName,
                "invalid bucket name: b",
            ),
        ];
        for (e, code, msg) in cases {
            let err = to_s3_error(e);
            assert_eq!(err.code(), &code);
            assert_eq!(err.message(), Some(msg));
        }
    }

    #[test]
    fn internal_errors_are_generic() {
        for e in [
            StoreError::CorruptMeta("不支持的格式版本 9".into()),
            StoreError::Encode("e".into()),
            StoreError::Io(std::io::Error::other("io")),
        ] {
            let err = to_s3_error(e);
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
            assert_eq!(err.message(), Some("internal error"), "5xx 不泄漏内部细节");
        }
    }
}
```

- [x] **Step 5: 跑测试**

Run: `cargo test -p xl-s3 2>&1 | tail -12`
Expected: 全绿（收尾轮后 xl-s3 7 passed）

- [x] **Step 6: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): SigV4 凭据解析与错误映射"
```

### 收尾轮（评审 5 条 Minor）—— 定稿记录

1. **4xx/5xx 分流**（错误面咽喉）：`convert.rs` 六个可纠正错误保留原消息；`CorruptMeta/Encode/Io` 走 `tracing::error!` + 通用消息 `internal error`。`auth.rs` 查询失败同型（`error = ?e, access_key` 进日志，响应体 `internal error`）。测试钉住两类消息形态。
2. **控制字符不进 XML**：根因在 `xl-store/src/layout.rs::validate_key` 把原始 key 直拼进 `InvalidKey` 消息——改 `key.escape_debug()`（保留「哪段不合规」）；`meta.rs` 消息不含用户字节（版本号/字节数/空文件），已核对不动。新增测试 `invalid_key_message_escapes_user_bytes`。
3. **断言收紧 + 边界**：`auth.rs` 负例改 `assert_invalid_ak`（`matches!` 锁 `InvalidAccessKeyId`）；新增 `future_expiry_access_key_resolves`、`db_error_maps_to_internal_error`。
4. **去重复查询**：删 `oss-server/src/db.rs` 的 `secret_for_access_key` 及其两条测试（生产路径由 `xl-s3/src/auth.rs` 接管，防 M5 双改）；`user_and_access_key_roundtrip` 改为直查断言；migration 注释改指 `xl-s3/src/auth.rs`。本任务上方 Task 8 记录里的该函数/测试已被本轮回撤（保留为历史）。
5. **ponytail 注释**：`auth.rs` 注明每请求一查无缓存及其理由。
6. Minor 6（时钟 helper 合并）记账不动。

收尾轮提交：`fix(xl-s3): 错误面收口（5xx 通用化+tracing）；auth 断言与边界；db 去重`（带计划文件）

---

## Task 10: xl-s3 —— 桶操作

**Files:**
- Create: `crates/xl-s3/src/service.rs`

- [x] **Step 1: 写失败的测试**

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

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test bucket_ops 2>&1 | tail -20`
Expected: 编译错误
实际：`error: could not compile`（1 处 E0432：`no OssS3 in service` —— `service.rs` 此前为 1 字节占位）。

- [x] **Step 3: 实现 `service.rs` 的骨架与桶操作**

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
    /// 单次 PUT 的整份读入上限，见 `put_object` 的 ponytail 取舍。
    max_put_bytes: usize,
}

impl OssS3 {
    pub fn new(store: Arc<Store>, max_put_bytes: usize) -> Self {
        Self {
            store,
            max_put_bytes,
        }
    }
}

/// nanos → S3 时间戳：`LastModified` 与 `CreationDate` 同为 `Timestamp` 别名，
/// 对象用 `mod_time_nanos`、桶用 `creation_nanos`。不要用 now() 冒充。
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
                creation_date: Some(last_modified(b.creation_nanos)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput { buckets: Some(list), ..Default::default() }))
    }
}
```

> `CreationDate` 的类型以 `generated.rs` 为准（可能是 `Timestamp` 别名）。若 `CreationDate::from(SystemTime)` 不成立，改用其实际构造方式。
> 已核实（源码核对，无臆测）：`CreationDate = Timestamp`（`generated.rs:7525`），`impl From<SystemTime> for Timestamp` 成立（`dto/timestamp.rs:60`），计划代码原样编译。

注（Task 10 执行偏差，已裁决）：`max_put_bytes` 字段推迟到 Task 11 首次有消费者时加入（`put_object` 读取）——Task 10 无读取者，dead_code 实测 `field max_put_bytes is never read`，会撞 0 告警闸，与 Task 4→5 的 `locks` 同款裁决。构造函数签名不变（`new(store, _max_put_bytes)`，`bucket_ops.rs` 与 Task 11 的调用形状不动）。**Task 11 已落地还原**（见 Task 11 定稿记录）：字段加回结构体、参数去 `_` 前缀；上方 Step 3 代码块已按最终形态同步。`list_buckets` 的 `creation_date` 取值与测试断言见下方收尾轮记录。

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 --test bucket_ops 2>&1 | tail -12`
Expected: 全绿
实际：全绿——`--test bucket_ops` 2 passed；`-p xl-s3` 9 passed（7 单元 + 2 集成）。四闸全绿（fmt / clippy 0 告警 / `-p xl-s3` / `--workspace -j 2`；workspace 合计 65 = xl-store 50 + oss-server 6 + xl-s3 9）。

- [x] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): 桶操作（create/head/delete/list）"
```

### 收尾轮（规格评审裁决 2 条）—— 定稿记录

1. **`list_buckets` 的 `creation_date` 取真实值（必修）**：`CreationDate::from(SystemTime::now())` → `last_modified(b.creation_nanos)`（`BucketInfo.creation_nanos` 即真实建桶时间）。`last_modified` helper 由此获得首个消费者、不再 dead_code——推迟项只剩 `max_put_bytes` 一项。测试加断言钉住（`bucket_ops.rs::create_head_list_delete_bucket`）：**上界取 list 之前捕获的 `before_list`，为紧判别点**——实测 now() 冒充形态下 created 必晚于 before_list 而失败（已实证：贴回 fake 复跑，断言按预期红）；下界留 50ms 容差，依据：文件时间戳取自内核粗粒度时钟，python 实测 mkdir 的 mtime 比精确时钟滞后 0.9–1.06ms 且同 tick 内恒定（CLK_TCK=100 的机器上限 10ms），无容差会假阳性（首版无容差断言即因此红）。上方 Step 3 代码块已同步。
2. **计划注记措辞修正**：「Task 11 的 Step 3 代码块已按此写」不准确（该块只消费不声明），已按评审改为上方偏差注中的措辞。

四闸复跑全绿（fmt / clippy 0 告警 / `-p xl-s3` 9 passed / `--workspace -j 2` 65 passed）。收尾轮提交：`fix(xl-s3): list_buckets 取真实创建时间；计划注记更正`（带计划文件）。

---

## Task 11: xl-s3 —— 对象操作

**Files:**
- Modify: `crates/xl-s3/src/service.rs`

- [x] **Step 1: 写失败的测试**

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

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -20`
Expected: 编译错误（实测能编译：`S3` trait 自带默认方法体，失败形态是运行时 `NotImplemented`，同 Task 12 Step 2）

- [x] **Step 3: 实现对象操作**

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
        // §3.4：一次最多 1000 个
        if req.input.delete.objects.len() > 1000 {
            return Err(s3s::s3_error!(InvalidRequest, "too many objects in a single delete request"));
        }

        // §3.4 部分失败语义：逐键删，成功的进 deleted、键级错误进 errors 后继续；
        // 桶级错误（桶不存在/桶名不合法）是请求级错误，整体返回 Err。
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in req.input.delete.objects {
            match self.store.delete_object(&bucket, &obj.key).await {
                Ok(()) => deleted.push(DeletedObject { key: Some(obj.key), ..Default::default() }),
                Err(e @ (StoreError::NoSuchBucket(_) | StoreError::InvalidBucketName(_))) => {
                    return Err(to_s3_error(e));
                }
                Err(e) => {
                    // 经 to_s3_error：4xx 保留原文，5xx 细节只进日志
                    let s3err = to_s3_error(e);
                    errors.push(Error {
                        key: Some(obj.key),
                        code: Some(s3err.code().as_str().to_string()),
                        message: Some(s3err.message().unwrap_or_default().to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }
```

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -12`
Expected: 全绿（ETag 断言用真实 md5）

- [x] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): 对象 CRUD 与批量删除"
```

### 定稿记录

- `max_put_bytes` 按 Task 10 偏差注还原：字段加回 `OssS3` 结构体、构造参数去 `_` 前缀（`bucket_ops.rs` 调用形状不变）；Task 10 的 Step 3 代码块与偏差注已同步。
- Step 3 实现与计划代码块逐字一致（`delete_objects` 见下条），仅另加 `use bytes::Bytes;` 与 `use xl_store::error::Error as StoreError;`（块内只含方法体，未含 imports）与 rustfmt 换行。
- **`delete_objects` 按规格 §3.4 修正（质量评审 I-1）**：原块只做「首个错误即整体 Err」的简化循环，与 §3.4 的「部分失败时 Errors 数组列出失败项，成功的仍在 Deleted 里，不做事务回滚」不符。定稿：入口 1000 上限（超出 → `InvalidRequest`）；逐键删，成功 push `DeletedObject{key}`、键级错误经 `to_s3_error` 收进 `errors`（`Error{key, code: s3err.code().as_str(), message}`——`S3ErrorCode` 无 `Display`，字符串形态走 `as_str()`）后继续；桶级错误（`NoSuchBucket` / `InvalidBucketName`）仍整体返回 Err。上方 Step 3 块已同步。
- 测试补强（评审 M-1/M-2）：批量后逐键 head 确认已删；get_object 收 body 字节断言；删后 head 钉 `NoSuchKey`；混批（`ok1` + 非法键 `a//b`）钉 `deleted`/`errors`/互不影响；`>1000` 键钉 `InvalidRequest`；`max_put_bytes` 边界对（恰 1 MiB 通过、1 MiB+1 → `EntityTooLarge`）。
- 口径注：规格 §3.3 的「任何错误一律 404」指对象/桶不存在的读语义；键不合规仍按 §0.2 报 400（`head_object` 的 `InvalidKey` → `InvalidArgument`）。规格 §3.3 已加同款括号，计划此处落账。
- 测试 helper（`service()` / `req()`）与 `bucket_ops.rs` 有意重复一份：计划未要求共用模块（Task 12 继续往 `object_ops.rs` 追加）。

---

## Task 12: xl-s3 —— ListObjectsV2 与 CopyObject

**Files:**
- Modify: `crates/xl-s3/src/service.rs`
- Modify: `crates/xl-store/src/store.rs`（仅 `copy_object` 加一行 `ponytail:` 标记；评审微项 3 并入）

- [x] **Step 1: 写失败的测试**

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

// 评审挂载项（并入本轮）：负数 max-keys 防护
#[tokio::test]
async fn list_objects_v2_rejects_negative_max_keys() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();

    // i32 → usize 的 `as` 对负数会回绕成巨值，再被 store 的 .min(1000) 钳成 1000 ——
    // 非法参数被静默当默认值。口径：max-keys < 0 → InvalidRequest。
    let err = s3.list_objects_v2(req(ListObjectsV2Input {
        bucket: "alpha".into(), max_keys: Some(-1), ..Default::default()
    })).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}

// 评审挂载项（并入本轮）：规格 §3.5 边界「源 = 目标 → InvalidRequest」
#[tokio::test]
async fn copy_object_rejects_same_source_and_destination() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput { bucket: "alpha".into(), ..Default::default() })).await.unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(), key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))), ..Default::default()
    })).await.unwrap();

    // M1 无 metadata-directive REPLACE / 存储类变更概念，同桶同键一律拒
    let err = s3.copy_object(req(CopyObjectInput::builder()
        .bucket("alpha".to_string())
        .key("src".to_string())
        .copy_source(CopySource::Bucket { bucket: "alpha".into(), key: "src".into(), version_id: None })
        .build()
        .unwrap())).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p xl-s3 --test object_ops 2>&1 | tail -20`
Expected: 编译错误 / `NotImplemented`

实测：编译通过，4 个新用例失败形态均为运行时 `NotImplemented`（同 Task 11 Step 2 注记）；两个补强的 delete 用例当时实现已通过。

- [x] **Step 3: 实现**

> 上方块已按最终形态同步（含评审挂载项：负数 max-keys 防护、src == dst 拒绝；见下方定稿记录）。
> 块内只含方法体：`use xl_store::store::{ListRequest, Store};` 由顶层 imports 提供。

```rust
// 追加到 impl S3 for OssS3
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            prefix,
            delimiter,
            max_keys,
            continuation_token,
            ..
        } = req.input;

        // max-keys < 0：i32 → usize 的 `as` 会回绕成巨值、再被 store 的 .min(1000) 钳成 1000，
        // 非法参数被静默当默认值。报 InvalidRequest，不猜客户端意图。
        let max_keys = match max_keys {
            Some(m) if m < 0 => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "max-keys must not be negative"
                ));
            }
            other => other.map(|m| m as usize),
        };

        let result = self
            .store
            .list_objects(
                &bucket,
                &ListRequest {
                    prefix,
                    delimiter,
                    max_keys,
                    continuation_token,
                },
            )
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
                storage_class: Some(ObjectStorageClass::from_static(
                    ObjectStorageClass::STANDARD,
                )),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            contents: Some(contents),
            key_count: Some(result.objects.len() as i32),
            common_prefixes: Some(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            is_truncated: Some(result.is_truncated),
            next_continuation_token: result.next_token,
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // s3s 已将 x-amz-copy-source 解码、去掉前导 '/' 并解析成枚举；这里只接受桶形式
        let CopySource::Bucket {
            ref bucket,
            ref key,
            ..
        } = req.input.copy_source
        else {
            return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
        };

        // 规格 §3.5 边界：M1 无 metadata-directive REPLACE / 存储类变更概念，
        // src == dst（同桶同键）一律拒（S3 语义：InvalidRequest）
        if bucket.as_ref() == req.input.bucket.as_str() && key.as_ref() == req.input.key.as_str() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "copy source and destination are the same"
            ));
        }

        let info = self
            .store
            .copy_object(bucket, key, &req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(info.etag)),
                // 与 get/head_object 同一翻译：返回 dst 对象的真实 mtime，不用 now() 冒充
                last_modified: Some(last_modified(info.last_modified_nanos)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
```

- [x] **Step 4: 跑测试**

Run: `cargo test -p xl-s3 2>&1 | tail -12`
Expected: 全绿

实测：`object_ops` 12 passed（原 5 + 本轮 7：4 新用例 + 恰 1000 用例 + 非桶 CopySource 用例 + 源缺失用例）；四闸全绿（fmt / clippy 0 告警 / `-p xl-s3` / `--workspace -j 2`）。

- [x] **Step 5: 提交**

```bash
git add crates/xl-s3/ && git commit -m "feat(xl-s3): ListObjectsV2 与 CopyObject"
```

### 定稿记录

- Step 3 块已同步为最终形态（`cargo fmt` 展开 + 本轮挂载项）；另在顶层 imports 加 `use xl_store::store::{ListRequest, Store};`（块内只含方法体，未含 imports）。
- `ObjectStorageClass::from("STANDARD")` 编译不过（s3s 只实现 `From<String>` / `from_static`，无 `From<&str>`），按「执行前必读」第 1 条以编译器为准改为 `ObjectStorageClass::from_static(ObjectStorageClass::STANDARD)`。
- **负数 max-keys 防护（评审挂载）**：`max_keys.map(|m| m as usize)` 对负数会 `as` 回绕成巨值，再被 store 的 `.min(1000)` 钳成 1000 —— 非法参数静默变默认值。口径：`m < 0 → InvalidRequest`（与 `delete_objects` 超 1000 键同码）。测试 `list_objects_v2_rejects_negative_max_keys`。
- **`copy_object` 源=目标检查（规格 §3.5；评审挂载）**：同桶同键在调 store 之前拒 `InvalidRequest`（M1 无 REPLACE / 存储类变更概念，凡 src == dst 即拒）；`CopySource` 非桶形式（AccessPoint / Outpost ARN）同样 `InvalidRequest`。测试 `copy_object_rejects_same_source_and_destination`。
- **`delete_objects` 测试补强（上轮评审残余；挂载项 3）**：混批顺序改为 `ok1` / `a//b`（非法）/ `ok2`，断言 `deleted` 恰为 `[ok1, ok2]` 且两键删后 head 均 404 —— 钉「键级错误后继续且不回滚」；新增 `delete_objects_accepts_exactly_1000_keys`（恰 1000 通过，与既有 `>1000 → InvalidRequest` 成对）。实现未动。按本文件惯例，测试补强记在定稿记录、不重写 Task 11 Step 1 块。
- 实际测试文件相对上方 Step 1 块另加断言（不改变块语义）：`list_objects_v2_prefix_delimiter_and_paging` 断言 contents 键序与 `key_count`、`is_truncated=false`，并跟随 `next_continuation_token` 取第二页、拼回全量 4 键（无重无漏）；`copy_object_copies_bytes` 收 body 字节断言 `== b"copy"`（不只长度）。与 Task 11 的「钉字节」评审口径一致。
- 计划 Task 12 块内无 `percent_decode` 等作废代码（该批 API 臆测已在前几轮探针清理），无需删改；块内 `xl_store::store::ListRequest` 行内路径按上方同步改为顶层 import——语义不变。
- **`CopyObjectResult.last_modified` 补 `Some(...)`（规格评审微项 1）**：原计划块为 `None`（观察项），评审确认补齐——复用 `last_modified` helper 返回 dst 对象的真实 mtime（与 `get/head_object` 同一翻译）。测试断言 copy 响应里的 `last_modified` 与随后 `head_object(dst)` 的 `last_modified` 严格相等（钉真实 mtime，不是 `is_some()` 的弱断言）。
- **非桶 `CopySource` 补用例（规格评审微项 2）**：`copy_object_rejects_non_bucket_copy_source` 用 `CopySource::AccessPoint` 钉住既有守卫的 `InvalidRequest` 路径（此前只有代码、无用例）。
- 口径注：`max-keys` 上限 >1000 由 store 静默钳到 1000（Task 4 定稿），本轮只增负数拒绝，未动钳位语义。
- **M2 备忘（评审微项 4，M1 不改）**：`key_count` 只数 objects，而 store 的 `max_keys` 预算把 common_prefixes 计入 —— 带 `delimiter` 的分页下 `KeyCount` 与 AWS 语义可能有差（AWS 的 KeyCount 计 Contents+CommonPrefixes），M2 对齐全量列举语义时一并核。另记：`encoding-type=url`、`start-after`、`fetch-owner` 三个 ListObjectsV2 参数 M1 均未处理（规格 §4.1 参数表列了），属计划层范围，M1 落账不做。
- **copy 静默忽略面 M2 备忘（评审微项 1，与上条自 key_count 备忘并列）**：`CopySource::Bucket` 的 `versionId` 被吞（M1 无版本，真 S3 对不存在/不支持的 versionId 报 404）与 `x-amz-copy-source-if-*` 条件头被吞（真 S3 条件不满足报 412）——M1 均未处理，M4/M6 接版本与条件复制时一并补。
- **测试补强（评审微项 2）**：`list_objects_v2_prefix_delimiter_and_paging` 的 Contents 断言补 `size == Some(1)`、`e_tag == md5("x")`（取有区分力的值，不钉占位）；新增 `copy_object_source_missing_is_no_such_key`（源对象不存在 → `NoSuchKey`）。
- **`xl-store::store::copy_object` 加 `ponytail:` 标记（评审微项 3）**：注明 M1 读出整份字节再写回（内存中介，上限同 put），M2 换分片级服务端复制；与 `put_object` 的标记风格一致。`xl-store` 本轮仅此一行注释，行为未动（其测试不受影响）。

---

## Task 13: 接线 —— S3 挂进 e-cat 的 fallback

**Files:**
- Create: `crates/oss-server/tests/common/mod.rs`（测试共用的 `state()` 辅助，被 smoke / s3_over_http / aether_ingest / console_api 四个测试文件复用）
- Modify: `crates/oss-server/src/router.rs`

- [x] **Step 0: 把 T8 里的 `state()` 抽成共用模块**

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

- [x] **Step 1: 写失败的测试**（`crates/oss-server/tests/s3_over_http.rs`）

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

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test s3_over_http 2>&1 | tail -10`
Expected: FAIL（当前 fallback 未挂，返回 404 而非 403）
实际：FAIL，`left: 404, right: 403`，与预期一致。

- [x] **Step 3: 修改 `router.rs`**

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

- [x] **Step 4: 跑测试**

Run: `cargo test -p oss-server --test s3_over_http 2>&1 | tail -10`
Expected: PASS（403）
实际：PASS。四闸实测：`cargo fmt --all`（无 diff）· `cargo clippy --workspace --all-targets` 0 告警 · `cargo test -p oss-server` 5+1+1 全绿 · `cargo test --workspace -j 2` 78 passed / 0 failed。

- [x] **Step 5: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): s3s 挂进 e-cat router 的 fallback"
```

---

## Task 14: aetherupload 接线 + 原生 ingest

**Files:**
- Create: `crates/oss-server/src/aether.rs`
- Modify: `crates/oss-server/src/router.rs`, `state.rs`

- [x] **Step 1: 写失败的测试**

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

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test aether_ingest 2>&1 | tail -15`
Expected: 编译错误（`st.ingest` 未定义）
实际：FAIL，`error[E0609]: no field 'ingest' on type 'AppState'`，与预期一致。

- [x] **Step 3: 实现 `aether.rs`**

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

    // Runtime 的上传根是 `base_path.join(root_dir)`；base_path 给空串，
    // upload_root 便恰好等于 staging_dir —— 相对（oss.toml 的 `./staging`）与绝对
    // （集成测试的 tempdir）两种配置下都成立。
    let runtime = Runtime::new(cfg, "")
        .expect("aetherupload Runtime 构造失败")
        .with_instant(Arc::new(SqlInstantStore::new(Arc::clone(&state.db))))
        .with_events(Arc::new(IngestSink {
            store: Arc::clone(&state.store),
            db: Arc::clone(&state.db),
            cfg: Arc::clone(&state.cfg),
        }));

    // 分组目录必须先存在，否则预处理会报笼统的上传错误。aetherupload 自带 e2e 也是用 list_groups 建的。
    let _ = aetherupload::console::list_groups(&runtime, &mut |_| {});

    // `_header/`（断点文件目录）单独补建：list_groups 只在它自己新建根目录时顺带建 `_header/`，
    // 而启动自举早已建好 staging_dir —— 不补这一下，预处理会以 "fail to write head resource" 失败。
    std::fs::create_dir_all(state.cfg.staging_dir.join(aetherupload::header::HEADER_DIR))
        .expect("创建 aetherupload _header 目录失败");

    Arc::new(runtime)
}
```

- [x] **Step 4: `state.rs` 加字段**

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

- [x] **Step 5: `router.rs` 挂 aetherupload 的四条路由**

```rust
    // aetherupload：四条路由（预处理 / 分块 / 展示 / 下载）。
    // `routes` 返回的是 Router<()>（runtime 已 state 进各 handler）；`.with_state(())`
    // 只是把 marker 参数转回本函数的 Router<AppState>，为后续 `.nest` 控制台 API 留位。
    let aether = aetherupload::integrations::ecat::routes(crate::aether::build_runtime(&state));

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
```

注意（定稿）：`aetherupload::integrations::ecat::routes` 返回 `Router`（= `Router<()>`），
axum 0.8 没有 `From<Router<()>> for Router<S>` 的转换，直接 `merge` 不过编译。
但 `with_state<S2>` 的 `S2` 是自由的 marker 参数 —— `aether.with_state(())` 即可并入
`Router<AppState>`，无需把两者降到 `Router<()>`（也就不需要 `Extension` 注入）。
Task 15 的 `.nest("/api/v1", crate::console::routes())`（`Router<AppState>`）因此照旧成立。

- [x] **Step 6: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -15`
Expected: 全绿（含 ingest 测试）
实际：PASS。四闸实测：`cargo fmt --all`（无 diff）· `cargo clippy --workspace --all-targets` 0 告警 · `cargo test -p oss-server` 5+2+1+1 全绿 · `cargo test --workspace -j 2` 全绿（xl-store 51 / xl-s3 7+2+12 / oss-server 5+2+1+1）。

- [x] **Step 7: 手动验证四条路由挂上了**

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

实际：PASS。响应 `{"error":0,"chunkSize":1000000,"groupSubDir":"202610","resourceTempBaseName":"69f87b216bd206c2","resourceExt":"txt","savedPath":""}`（`error` 是数字 0，非字符串 "0"）。
首跑暴露真问题：`./staging` 已被启动自举建好，`list_groups` 便跳过 `_header/`（它只在自建根目录时顺带建），
预处理以 `"fail to write head resource"` 失败 —— build_runtime 现已单独补建 `_header/`（见 Step 3 块），复跑即 0。
（跑验证用的端口 9000 与 `./staging`、`./data`、`./oss.db*` 均在 .gitignore 内。）

- [x] **Step 8: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): aetherupload 接线 + 上传完成原生 ingest"
```

（实际一并带上 `crates/xl-store/src/layout.rs` 与计划文件——见定稿记录并入项 4。）

### 定稿记录

- **`Runtime::new` 的 base_path 改为空串（偏离计划块原文）**：`Runtime::new(cfg, base_path)` 内部是
  `base_path.join(cfg.root_dir)`。计划原文传 `&state.cfg.staging_dir` 且 `root_dir = staging_dir`：
  staging_dir 为绝对路径（测试的 tempdir）时 `join` 按语义返回绝对路径、侥幸成立；但 oss.toml 写的是
  相对路径 `./staging`，会拼出 `./staging/./staging`（多一层，落盘位置与暂存目录语义不符）。
  改传空串后两种路径下 upload_root 都恰好等于 staging_dir。实测 `./staging/{file,video,image}` 建在预期位置。
- **`_header/` 补建（计划遗漏的必需项，Step 7 首跑实证）**：`list_groups` 只在它自己新建根目录时
  递归建 `_header/`；启动自举已先 `create_dir_all(staging_dir)`，于是预处理必报
  `"fail to write head resource"`。build_runtime 末尾补
  `create_dir_all(staging_dir.join(aetherupload::header::HEADER_DIR))`（HEADER_DIR 是公开常量，值 `_header`），
  失败即 panic 快速失败。测试 `build_runtime_prepares_staging_dirs` 钉住（`_header/` + 分组目录）。
- **`merge` 的 state 处理（计划「注意」段的定稿口径）**：见 Step 5 下方注记——用
  `aether.with_state(())` 并入 `Router<AppState>`，未采用「降到 Router<()> + Extension」的备选方案；
  Task 15 的 `.nest` 口径不受影响。`router.rs` 的 `handle_s3_error` 注释按并入项 1 修正为
  「只有 s3s 连错误响应都序列化失败时才到达，近乎不可达的兜底」，**函数体未动**（Task 13 块内为旧注释，此处不追改）。
- **并入项 2、3（`tests/s3_over_http.rs`）**：文件头注释重写为两行实话——本文件只验「挂载 + 鉴权接线」
  （403 来自 default_check，排除 setAuth 漏接的半成品形态），签名闭环交 Task 16；
  并删掉 `create_user` + `create_access_key` 两行死装置（请求不带凭据，对断言零贡献）。断言与行为未动。
- **并入项 4（`xl-store/src/layout.rs`）**：`validate_bucket` 的 `InvalidBucketName` 消息改用
  `name.escape_debug()`（与 `validate_key` 同款、与非 URL 来源对齐；控制字符不再原样进 S3 响应体），
  三处 `Err(...)` 收口为一个 `reject()` 闭包；新增 `invalid_bucket_message_escapes_user_bytes`
  （断言消息不含原始 `\0` 且含转义形式 `\\0`）。`xl-s3/src/convert.rs` 既有的
  `"invalid bucket name: b"` 断言不受影响（该名字无控制字符）。
- **测试文件与 Step 1 块的两处差异（按 Task 11 先例记在定稿记录，不重写 Step 1 块）**：顶部补
  `use aetherupload::events::EventSink;`（调 trait 方法需要）；另加第二个测试
  `build_runtime_prepares_staging_dirs`（对应上条 `_header/` 修复）。其余与块内逐字一致。

### 收尾轮（质量评审 3 Important + 6 Minor）—— 定稿记录

- **I-1 ingest 失败补偿（清秒传索引）+ 注释改实话**：失败分支在 panic 前 best-effort 摘索引
  （`clear_instant_index`）。键构造以 aetherupload 1.0.3 源码为准：控制器在写索引处用
  `InstantIndex::key(group, hash)`（`upload.rs:396-398`），`hash` 即整份 md5，而成品名是
  `get_file_name(real_hash, ext)` = `<md5>.<ext>`（`util.rs:64`）—— 故键 = `KEY_PREFIX + group + "_" + file_stem(name)`
  （`InstantIndex::key` 自带安全字符集校验，`SavedPath` 编码不参与键）。原注释「panic 会被控制器接住并清理临时文件」
  系误述，改为：「aetherupload 的 ecat controller 以 catch_unwind 吞掉回调 panic、响应仍为成功
  （`emit_safely`，upload.rs:465）；本 sink 以 panic + `tracing::error` 保证服务端可见，并清索引防钉死」。
  测试 `ingest_failure_clears_instant_index`：预先写索引 + 不建桶触发失败，断言 panic 发生、索引被摘、上传表为空。
  **反证实测**：临时注释掉清索引一行 → 该测试红（`left: Some("file_2026-10_<md5>.txt")`），复原即绿。
- **I-2 ingest 改流式**：`upload_complete` 改用 `tokio::fs::File` + `tokio_util::io::ReaderStream`
  （`map_err(xl_store::error::Error::from)` 转流错误类型）包成 `ByteStream` 喂 `store.put_object_stream`；
  大小取 `file.metadata()`。未走「保留 read + ponytail 标记」的退路。etag 语义不变（对内核收到的字节算 MD5），
  既有测试断言 `info.etag == md5` 依旧通过。`oss-server/Cargo.toml` 新增 `tokio-util.workspace = true`。
- **Minor 1**：`list_groups` 返回码接住 —— 非 0 记 `error!`（分组目录缺失会让预处理报笼统错误），
  回调从 `|_| {}` 改为 `tracing::debug!(msg = %m, ...)` 留住文本（首次建目录时会打印每个已建目录）。
- **Minor 2**：`SqlInstantStore` 三处 map_err 收口为 `store_err()`，进入前 `tracing::warn!` ——
  内核把索引错误一概当「未命中」吞掉，这是唯一观测点；变体保持 `Error::Io` 不换。
- **Minor 3**：`Db::instant_delete_prefix`（`DELETE ... WHERE k LIKE ? || '%'`，前缀只来自 `KEY_PREFIX`、
  无 LIKE 通配符，注释已写明）+ `SqlInstantStore::delete_prefix` 落地（此前 trait 默认实现是静默空操作，
  `InstantIndex::delete_all` 会装作成功）。测试 `instant_delete_prefix_clears_only_that_prefix`。
- **Minor 4**：风险表加「`record_upload` 失败：uploads 行缺失但秒传索引已写」一行（见文末表）。
- **Minor 5**：`aether_ingest.rs` 补断言 —— `saved_path` 与 `SavedPath::encode("file", sub, name)` 严格相等
  （sink 内也改用该函数，不再手拼 `format!("{}_{}_{}")`）、`.key/.size/.etag` 逐项、暂存盘成品文件仍在
  （`path.exists()`）；新增 `sink_without_bucket_skips_ingest`（直造空 groups 的 sink，断言早返回不碰
  `block_in_place`：用单线程 `#[tokio::test]`，若误触会 panic）。
- **Minor 6**：`build_runtime` 的 `with_events` 改为复用 `Arc::clone(&state.ingest)`（显式
  `as Arc<dyn EventSink>`，`Arc::clone` 的返回值不做 unsize 推断，编译器要求写明），删掉第二处 IngestSink 构造
  —— 测试与生产是同一个实例。
- **I-3**：风险表加「`/aetherupload/*` 无认证层、匿名可写」一行（见文末表）；README 一句话由 Task 16 补。
- **四条路由的真机实测（收尾轮，覆盖 Task 16 脚本不碰的 aether 链路）**：起真服务后
  ① `preprocess` → `{"error":0,...}`；② `uploading`（单块，hello world / md5 `5eb63bbb…`）→
  `{"error":0,"savedPath":"file_202610_5eb63bbb….txt"}`，对象落盘 `data/files/202610__XLDIR__<md5>.txt/`
  且内容为 `hello world`（**流式 ingest 在真实 HTTP 链路生效**），`uploads` 行
  `saved_path/bucket/key/size/etag` 全对；③ 同 hash 再传 → 秒传命中（索引 get 走到）；
  ④ `GET display/{saved_path}` 返回内容；⑤ `GET download/{saved_path}/{new_name}` 200 + 附件头。
  这同时实证 ecat 的 HTTP 服务跑在我们的 multi-thread runtime 上（否则 ingest 的 `block_in_place` 会 panic 被
  `emit_safely` 吞掉、对象不会落盘）。注：display/download 是**路径式**路由
  （`/{saved_path}`、`/{saved_path}/{new_name}`，integrations/mod.rs:52-62），拿 query 形式访问会落到 S3 fallback。
- 四道闸（收尾轮）：fmt 无 diff · clippy 0 告警 · `cargo test -p oss-server` 6+4+1+1 全绿 · `--workspace -j 2` 全绿。

---

## Task 15: 控制台 API

**Files:**
- Create: `crates/oss-server/src/console.rs`
- Modify: `crates/oss-server/src/router.rs`

- [x] **Step 1: 写失败的测试**

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

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test console_api 2>&1 | tail -10`
Expected: 失败（控制台路由未挂 → 落 Task 13 的 s3s fallback：GET 形态 403、POST 形态 501。原稿「404（路由未挂）」是接线前的旧形态；见定稿记录并入项 1）
实际：3 个测试全红 —— `login_then_list_buckets` 在未带 token 的 GET 上 `left: 403 / right: 401`，`wrong_password_is_401` 在 POST 上 `left: 501 / right: 401`。

- [x] **Step 3: 实现 `console.rs`**

```rust
//! 控制台 API。M1 只有：登录、桶列表、对象列表、下载、删除、上传记录、访问密钥。
//! 授权模型（谁能做什么）在 M4；M1 只区分「登录 / 未登录」。

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
        .route(
            "/buckets/{bucket}/objects/{*key}",
            get(get_object).delete(delete_object),
        )
        .route("/uploads", get(list_uploads))
        .route("/uploads/register", post(register_upload))
        .route(
            "/access-keys",
            get(list_access_keys).post(create_access_key),
        )
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    match state.db.verify_user(&body.username, &body.password).await {
        Ok(Some(uid)) => {
            let exp = now_secs() as usize + 12 * 3600;
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &Claims {
                    sub: uid.to_string(),
                    exp,
                },
                &jsonwebtoken::EncodingKey::from_secret(state.cfg.jwt_secret.as_bytes()),
            );
            match token {
                Ok(token) => {
                    (StatusCode::OK, Json(serde_json::json!({ "token": token }))).into_response()
                }
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "token 签发失败").into_response(),
            }
        }
        Ok(None) => (StatusCode::UNAUTHORIZED, "用户名或密码错误").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "登录查询失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// store 错误分档：可纠正的保留原消息，服务端内部的记日志 + 通用文案（5xx 不泄漏内部细节）。
fn store_error(e: xl_store::error::Error) -> (StatusCode, String) {
    use xl_store::error::Error;
    match &e {
        Error::NoSuchBucket(_) | Error::NoSuchKey(_) => (StatusCode::NOT_FOUND, e.to_string()),
        Error::InvalidBucketName(_) | Error::InvalidKey(_) => {
            (StatusCode::BAD_REQUEST, e.to_string())
        }
        // 这两个语义是「客户端可纠正的冲突」，S3 本身也用 409；不落 5xx
        Error::BucketAlreadyExists(_) | Error::BucketNotEmpty(_) => {
            (StatusCode::CONFLICT, e.to_string())
        }
        _ => {
            tracing::error!(error = %e, "控制台 store 操作失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        }
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

    decoded
        .claims
        .sub
        .parse::<i64>()
        .map_err(|_| StatusCode::UNAUTHORIZED)
}

async fn list_buckets(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.list_buckets().await {
        Ok(list) => Json(list.into_iter().map(|b| b.name).collect::<Vec<_>>()).into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateBucketBody {
    name: String,
}

async fn create_bucket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
    };
    if let Err(e) = state.store.create_bucket(&body.name).await {
        return store_error(e).into_response();
    }
    if let Err(e) = state.db.insert_bucket(&body.name, Some(uid)).await {
        // 桶已建成：控制面行缺失只影响列表展示，不失败请求
        tracing::warn!(error = %e, "写入桶行失败");
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_bucket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.delete_bucket(&bucket).await {
        Ok(()) => {
            if let Err(e) = state.db.delete_bucket_row(&bucket).await {
                // 桶已删除：控制面行残留只影响列表展示，不失败请求
                tracing::warn!(error = %e, "清理桶行失败");
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => store_error(e).into_response(),
    }
}

#[derive(Deserialize)]
struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    /// 翻页参数按 spec §0.3 命名，透传给 store 的 max_keys / continuation_token
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn list_objects(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ListQuery>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    let req = xl_store::store::ListRequest {
        prefix: q.prefix,
        delimiter: q.delimiter,
        max_keys: q.limit,
        continuation_token: q.cursor,
    };
    match state.store.list_objects(&bucket, &req).await {
        Ok(r) => Json(serde_json::json!({
            "objects": r.objects.iter().map(|o| serde_json::json!({
                "key": o.key, "size": o.size, "etag": o.etag, "content_type": o.content_type
            })).collect::<Vec<_>>(),
            "common_prefixes": r.common_prefixes,
            "is_truncated": r.is_truncated,
            // next_cursor 是 spec §0.3 的契约名；next_token 为既有字段，保留不删
            "next_token": r.next_token.clone(),
            "next_cursor": r.next_token,
        }))
        .into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

async fn get_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.get_object(&bucket, &key).await {
        Ok((info, bytes)) => {
            let ct = info
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into());
            ([(axum::http::header::CONTENT_TYPE, ct)], bytes).into_response()
        }
        Err(e) => store_error(e).into_response(),
    }
}

async fn delete_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

async fn list_uploads(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.db.list_uploads(100).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询上传记录失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct RegisterBody {
    saved_path: String,
    original_name: String,
}

async fn register_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
    };
    match state
        .db
        .register_upload(&body.saved_path, &body.original_name, Some(uid))
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        // DB 故障是服务端故障（与 store_error 的 5xx 分档一致）
        Err(e) => {
            tracing::error!(error = %e, "登记上传记录失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn list_access_keys(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
    };
    match state.db.list_access_keys(uid).await {
        Ok(keys) => Json(keys).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询访问密钥失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct CreateKeyBody {
    access_key: Option<String>,
    secret_key: Option<String>,
}

/// 用户自定 AK/SK 的最小校验：≥16 字节、ASCII 字母数字与 `_-+/=`（覆盖 S3 形态的
/// 字母数字 AK 与 base64 形态的 SK）；不满足直接 400，不落到 DB 层。
fn valid_credential(s: &str) -> bool {
    s.len() >= 16
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+' | b'/' | b'='))
}

async fn create_access_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateKeyBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
    };
    if let Some(ak) = body.access_key.as_deref().filter(|s| !valid_credential(s)) {
        return (
            StatusCode::BAD_REQUEST,
            format!("access key 非法：需 ≥16 位且仅限字母数字与 _-+/=（{ak}）"),
        )
            .into_response();
    }
    if body
        .secret_key
        .as_deref()
        .is_some_and(|s| !valid_credential(s))
    {
        return (
            StatusCode::BAD_REQUEST,
            "secret key 非法：需 ≥16 位且仅限字母数字与 _-+/=".to_string(),
        )
            .into_response();
    }
    let ak = body
        .access_key
        .unwrap_or_else(|| format!("OSS{}", uuid::Uuid::new_v4().simple()));
    let sk = body
        .secret_key
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    match state.db.create_access_key(&ak, &sk, uid).await {
        Ok(()) => Json(serde_json::json!({ "access_key": ak, "secret_key": sk })).into_response(),
        // UNIQUE 冲突说人话；不落 sqlite 原文
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            (StatusCode::BAD_REQUEST, "access key 已存在").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "写入访问密钥失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
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

- [x] **Step 4: `router.rs` 挂控制台**

```rust
        .nest("/api/v1", crate::console::routes())
```

- [x] **Step 5: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -15`
Expected: 全绿
实际：PASS。四闸实测：`cargo fmt --all`（无 diff）· `cargo clippy --workspace --all-targets` 0 告警 · `cargo test -p oss-server` 6+4+3+1+1 全绿 · `cargo test --workspace -j 2` 全绿（xl-store 51 / xl-s3 7+2+12 / oss-server 6+4+3+1+1）。

- [x] **Step 6: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): 控制台 API（登录/桶/对象/上传记录/密钥）"
```

（实际一并带上计划文件——见定稿记录。）

### 定稿记录

- **Step 2 的 Expected 更正（并入项 1）**：原稿「404（路由未挂）」与 Task 13 接线后的实际失败形态不符——未注册路径落 s3s fallback：
  **GET 形态 403**（s3s 鉴权拒绝，spec-task13 实测；本轮 `login_then_list_buckets` 在 `/api/v1/buckets` 上复现 `left: 403 / right: 401`）、
  **POST 形态 501**（s3s 无对应 op → NotImplemented；本轮 `wrong_password_is_401` 在 `/api/v1/login` 上实测 `left: 501 / right: 401`）。
  并入项只说 403，实测补全为两种形态，上方该行已按实测同步。
- **`register_upload` 三分支（并入项 2）**：按 I-1 同步后的现文写——`Ok(true) → 204 / Ok(false) → 404 / Err(e) → 500`
  （Err 分支的 400→500 修订见下方收尾轮记录与 Task 7 顶部记录）。
  新增测试 `register_upload_hit_is_204_miss_is_404`（Step 1 块外，按 Task 11 先例记在这里）：`record_upload` 后 register → 204、
  `GET /uploads` 能看到 `original_name`、无行 `saved_path` → 404。Err→500 见 `register_upload_db_failure_is_500`。
- **过期 AK 边界测试（并入项 3，挂账跳过并说明）**：quality-task7 ② 所指的原用例 `secret_for_access_key_rejects_expired`
  （`now() - 1` 绑定）随 Task 9 收尾轮已删除（生产路径由 xl-s3 接管）。现存唯一的过期用例在
  `crates/xl-s3/src/auth.rs::inactive_or_expired_access_key_is_rejected`，且用的是固定 `Some(1)` 而非 `now-1`——
  **文件在 xl-s3，不在 Task 15 范围**（Files 只含 `console.rs` / `router.rs`），跳过不越界改动，留给 xl-s3 侧任务把边界钉到 `expires_at == now`（严格 `>`）。
- **`aether.rs` 注释补前提（并入项 4，实现未动）**：`clear_instant_index` 的键构造注释补一行
  「键重建成立的前提是 lax_mode 关闭（客户端声明 hash == 真 md5）；开启 lax_mode 后需同步改此处」（aetherupload 的 `Config::lax_mode` 默认 false）。
- **与 Step 3 块的差异（以编译器/闸门为准）**：删掉块首 `use std::sync::Arc;`（console.rs 未用到，留着撞 clippy 未用导入闸）；
  其余逐字一致（rustfmt 折行不算）。本块已在收尾轮整块同步为最终形态（`git diff --no-index` 提取块 vs 源文件无差异），此差异描述的是同步前的原块。
  `oss-server/Cargo.toml` 按上方注记加 `uuid.workspace = true`（workspace 的 uuid 已带 `v4`，无需另配 feature）。
- **测试文件与 Step 1 块的差异**：顶部补 `mod common; use common::state; use oss_server::router::build_router;`
  （块内按 smoke.rs 惯例省略）；新增第三个测试见上。
- **`LoginResponse` 保持为 pub 未在 handler 中使用（照块）**：login 响应用 `serde_json::json!` 构造，`LoginResponse` 是块内给出的公开类型、
  未构造使用；pub 项不触发 dead_code 告警，按「严格照块」保留。

### 收尾轮（质量评审 1 Important + 4 Minor）—— 定稿记录

- **I-1（安全回归保护）**：新增两个测试。① `forged_or_expired_token_is_401`：换 secret 签名的 token（签名不过）与
  `exp = now-3600`（越过 jsonwebtoken 默认 60s leeway）的 token 都必须 401 —— 测试内直接用 jsonwebtoken encode（已是普通依赖），
  自定 `TestClaims` 形状（`console::Claims` 是私有的）。② `all_routes_reject_anonymous`：11 条路由去掉 login = 10 条，
  表驱动（method + path 数组）× 无 token → 401，「新增 handler 时同步加表项，漏 auth 即红」（axum 不暴露路由枚举）。注意 POST 路由的 `Json` 提取先于 handler，
  表内必须带能解析的 body 才走得到鉴权分支。实现未动。
- **M-2（状态码说真话）**：新增 `store_error()` 分档 —— `NoSuchBucket/NoSuchKey → 404`、`InvalidBucketName/InvalidKey → 400`、
  `Io/CorruptMeta/Encode → 500`（`tracing::error!` + 通用文案，5xx 不泄内部细节）；**`BucketAlreadyExists/BucketNotEmpty → 409`**
  （并入项未列这两个变体，「其余 → 500」若把可纠正的冲突吞进 5xx 就违背了本条「说真话」，S3 自身对二者也用 409）。login 拆三态：
  `Ok(None)` → 401 原文案、`Err` → 500 通用 + 日志。`list_uploads`/`list_access_keys` 的 5xx 同样改通用文案 + 日志
  （原样 `e.to_string()` 会泄 sqlite 原文）。测试 `store_error_status_codes` 钉 404/400/409 三条。
  **`register_upload` 的 Err 分支（微调轮裁决）**：该 Err 是 sqlx 错误、非 `xl_store::Error`，M-2 机制上不适用；
  原先沿 Task 7 I-1 定稿返回 400，经裁决 **改判 500**（通用文案 + `tracing::error!`）——DB 故障是服务端故障，与本条 list_*/create 分档一致。
  Task 7 的三分支口径就此修订为 **204 / 404 / 500**（Task 7 顶部记录与上方并入项 2 措辞已同步改正）。Err→500 分支在 HTTP 面
  构造得到：`register_upload_db_failure_is_500`（经池 DROP TABLE uploads 制造 DB 故障，断言 500 + 通用文案不泄内部细节）。
- **M-3（翻页参数）**：`ListQuery` 加 `limit`/`cursor`（spec §0.3 命名）→ `max_keys`/`continuation_token` 透传，store 零改动。
  **响应按 spec §0.3 补了 `next_cursor`**（与既有 `next_token` 同值；并入项「响应字段不动」按「不删不改既有字段」执行 —— 新增是加性的，
  且是 M1b 前端的契约名）。测试 `objects_pagination_follows_cursor`：2 键 `limit=1` → 第一页 1 键 + `is_truncated` +
  `next_cursor == next_token`，跟 cursor 拿到第二页。
- **M-4（AK 入参校验）**：`valid_credential()` = ≥16 字节 + ASCII 字母数字与 `_-+/=`；自定 AK/SK 不符 → 400 自己的文案
  （AK 非法时回显用户输入，JSON 转义兜底；SK 不回显）。DB 侧 `is_unique_violation()` → 400「access key 已存在」（不透 sqlite 原文）；
  其余 DB 错 → 500 通用 + 日志。测试 `access_key_validation_and_duplicate` 钉 太短 / 字符集外 / 合法（列表可见）/ 重复 四态。
- **M-5**：`create_bucket`/`delete_bucket` 的两处 `let _ =` 换成 `tracing::warn!`（桶已建成 / 已删除后控制面行写失败只影响展示，不失败请求）。
- **M-6 记账**：`api` 桶名与 `/api/v1` nest 的遮蔽 —— S3 侧桶名 `api` 下键前缀 `v1/` 的路径式访问会被控制台 nest 吃掉（M1 接受；
  M2 视需要列保留名/在桶名校验里拒绝）。
- **Step 3 块已同步**：console.rs 收尾轮后整块替换为最终形态（含 `store_error` / login 三态 / `limit`·`cursor` / `next_cursor` / AK 校验）。
- **反证实测**：`git stash push -- crates/oss-server/src/console.rs`（回到本任务首笔提交的旧实现）→ 新用例恰 3 红
  （pagination / store_error / access_key），I-1 的两个认证用例照绿（auth 逻辑本就正确，新用例防的是将来回归）；pop 复原即全绿。
- 四道闸（收尾轮）：fmt 无 diff · clippy 0 告警 · `cargo test -p oss-server` 6+4+8+1+1 全绿 · `--workspace -j 2` 全绿。

---

## Task 16: 端到端验收脚本与运行说明

**Files:**
- Create: `tests/e2e_s3.sh`
- Create: `README.md`

- [x] **Step 1: 写端到端脚本**

```bash
#!/usr/bin/env bash
# M1 验收：S3 兼容性（aws-cli + rclone）。
# 前置：本机装了 aws-cli 与 rclone。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST_ACCESS_KEY}"
SK="${SK:-SK_TEST_SECRET_KEY_0001}"
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
export RCLONE_CONFIG_OSS_ENDPOINT="$ENDPOINT"           # 必须带 scheme：不带时 rclone 默认 https
export RCLONE_CONFIG_OSS_FORCE_PATH_STYLE=true
export RCLONE_CONFIG_OSS_LIST_VERSION=2                 # provider=Other 默认发 v1；M1 只实现 ListObjectsV2
rclone lsf oss:"$BUCKET"
rclone copyto "$WORK/one.bin" oss:"$BUCKET/rclone.bin"
rclone cat oss:"$BUCKET/rclone.bin" | cmp - "$WORK/one.bin"   # rclone 读回内容与本地一致
if rclone lsf oss:"$BUCKET" | grep -qx one.bin; then echo "删除未生效"; exit 1; fi

echo "== 10. 删桶 =="
aws_s3 rb "s3://$BUCKET" --force

echo "M1 S3 验收全部通过"
```

- [x] **Step 2: 跑脚本**

Run:
```bash
chmod +x tests/e2e_s3.sh
# 先准备好管理员与访问密钥（一次性）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw cargo run -p oss-server &
sleep 2
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"pw"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"access_key":"AKIA_TEST_ACCESS_KEY","secret_key":"SK_TEST_SECRET_KEY_0001"}'
echo
# 然后用这把密钥跑脚本
ENDPOINT=http://127.0.0.1:9000 AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ./tests/e2e_s3.sh
kill %1
```
Expected: 脚本打印「M1 S3 验收全部通过」

- [x] **Step 3: 写 `README.md`** 的运行部分

````markdown
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
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
```
````

- [x] **Step 4: 提交**

```bash
git add tests/ README.md && git commit -m "test: M1 端到端验收脚本与运行说明"
```

### 定稿记录

- **凭据并入（Task 15 的 ≥16 校验）**：脚本默认值与 Step 2 的 provisioning / 调用行统一换成
  `AKIA_TEST_ACCESS_KEY` / `SK_TEST_SECRET_KEY_0001`（原 `AKIA_TEST`/`SK_TEST` 过不了 `valid_credential`，会被 400 拒掉）。
- **实跑修订两处（原文本跑不通，实跑发现）**：① `RCLONE_CONFIG_OSS_ENDPOINT` 不能剥 `http://` —— rclone 对不带 scheme
  的 endpoint 默认按 https 连，明文端口上 TLS 握手挂死；② 补 `RCLONE_CONFIG_OSS_LIST_VERSION=2` —— provider=Other
  默认发 ListObjects **v1**，M1 只实现 V2，否则 501 NotImplemented（rclone 报 `ListObjects is not implemented yet`）。
- **`console.rs` 一处统一（并入）**：login 的 token 签发失败分支改 `tracing::error!` + 通用文案「internal error」，
  对齐 M-2 的 5xx 口径（该文件唯一漏网处）。
- **README 两处（并入）**：`_header/` 措辞按 `aether.rs` 事实改写（分组目录走 `list_groups`，`_header/` 由
  `build_runtime` 单独补建）；补一句「`/aetherupload/*` 无认证层（匿名可写），勿暴露公网；认证覆盖层随部署形态（M2）落地」。
- **实跑（四道闸）**：fmt 无 diff · clippy 0 告警 · `cargo test --workspace -j 2` 全绿（93 测）·
  起真服务（`OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw`）→ 建密钥 → `tests/e2e_s3.sh` 十步全过 → kill。

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
| aetherupload 的分组目录必须先建 | `aether::build_runtime` 构造时调用 `aetherupload::console::list_groups` 建分组目录，并**单独补建 `_header/`**（list_groups 只在自建根目录时顺带建它）—— Task 14 及收尾轮 |
| 损坏的 `xl.meta` 让 PUT fail-closed（报错，不静默覆盖） | 有意为之（M-6 裁决）：解码失败/键不一致时不把旧 meta 当「无对象」继续写，避免毁掉盘上旧数据；处置靠人工修复或删该对象，**不做自动覆盖** |
| `tmp` / `trash` 无回收器（规格评审观察 C） | M1 只保证不可见（读路径只信 `xl.meta`）；孤儿目录随 M2 的启动自检与 GC 清理，当前仅占盘 |
| 删除路径残留（`remove_dir_all` 部分失败/崩溃，留下无 meta 的垃圾目录） | 不可见（读路径只信 `xl.meta`，列举跳过无 meta 的目录），但会让桶「非空」而删不掉（`BucketNotEmpty`）→ M2 的 fsck 清理垃圾目录 |
| **`/aetherupload/*` 四条路由无认证层，匿名可写**（质量评审 I-3） | 规格 §0.1 提的「会话 Cookie/JWT 覆盖层」在 M1a/M1b 计划里均无落点；四条路由当前匿名可达（预处理/分块/展示/下载）。处置：M1 收尾时向用户裁决是否补最小鉴权（如仅允许回环或加静态 token），否则随部署形态（M2）接。README 的已知限制由 Task 16 补一句 |
| **`record_upload` 失败：uploads 行缺失但秒传索引已写**（质量评审 Minor 4） | 索引里的 `savedPath` 指向已进桶的对象，但 `register_upload` 永远 404（无行可更新），且同内容**重传不补**（秒传命中即返回，不再触发 ingest/记录）→ 该内容在控制台不可见。M1 记录失败只 `tracing::error!`；处置：M2 把「写索引」挪到 ingest 成功之后，或让 register 支持 upsert |
