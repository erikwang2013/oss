# oss

自研分布式对象存储系统（S3 兼容，MinIO 的等价实现）。Rust 2024。

## 当前状态

M1–M9 已完成，最新发布 **v1.20.1（2026-10-28）**；M5 安全与锁定全部交付——集群行一致性
广播 · 密钥库/KMS · 加密数据面 DARE 式 · S3 面 SSE-S3/KMS/C · 对象锁 · STS（含
`AssumeRoleWithWebIdentity`）· SK 信封加密落库。**以 docs/ 为准**：

- `docs/superpowers/specs/2026-10-10-minio-clone-design.md` —— 总体设计（9 阶段 M1–M9、盘布局、xl.meta v1；§8 含各阶段落地状态）
- `docs/superpowers/specs/2026-10-10-feature-spec.md` —— 功能域规格（行为/接口/边界/错误码；M5 域含落地记录）
- `docs/superpowers/plans/` —— 各里程碑实现计划与定稿记录（最新：`2026-10-16-m5-security-lock.md`）
- `README.md` —— 运行 / 运维 / 验收 / 已知边界（对外口径的主入口）

## 结构（五个 crate，单向依赖）

`crates/xl-store`（存储内核，不依赖任何框架：xl.meta 编解码 / EC 8+4 / quorum / 版本链 /
加密与锁 / 密钥库）→ `crates/xl-s3`（s3s 的 S3 trait 实现）与 `crates/xl-client`（mesh 的
tonic 实现，`crates/xl-proto` 为节点间 gRPC 协议）→ `crates/oss-server`（e-cat 应用 +
控制台 API + aetherupload 接线）。

## 构建与测试

```bash
cargo check --workspace
cargo test --workspace
```

收敛口径「七闸」（提交前）：`cargo test -p xl-store --lib` · `-p xl-store --test kill9` ·
`-p oss-server` · `-p xl-s3` · `-p xl-client -p xl-proto` · `cargo clippy --workspace
--all-targets -- -D warnings` · `cargo fmt --all --check`。

## 注意

- 依赖版本锁在 workspace `Cargo.toml`；axum 必须全仓唯一 0.8.x（ecat 与 aetherupload 都锚定它）
- **升级须整栈同版本**（分片 RPC 带分块字段）：不要滚动升级，先停后换或整栈重建
- e2e：`tests/e2e_s3.sh`（需 aws-cli + rclone；`E2E_LONG=1` 门控长跑）、
  `tests/e2e_m3_cluster.sh`（4 节点集群 + 混沌段）
