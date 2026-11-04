# oss

自研分布式对象存储系统（S3 兼容，MinIO 的等价实现）。Rust 2024。

## 当前状态

M1–M9 已完成，最新发布 **v1.30.0（2026-11-05）**；M5 安全与锁定全部交付——集群行一致性
广播 · 密钥库/KMS · 加密数据面 DARE 式 · S3 面 SSE-S3/KMS/C · 对象锁 · STS（含
`AssumeRoleWithWebIdentity`）· SK 信封加密落库。v1.30 继续控制面去数据库化：在 v1.29 的 `ControlStore`/`ControlTxn` 缝上完成访问点/OLAP、lifecycle/inventory/tier、buckets/bucket_policies/event_configs/replication_rules 的读写、快照与桶删除级联迁移，并加入集群遗留控制对象重认证工具（只读 `ReadControlRaw`、canonical 分组、fenced 标记、CLI dry-run/apply）；自然键身份、快照字段顺序与 site/raft 触发器语义不变，legacy raw SQL 差分测试锁定行为，修复策略覆盖写的 SQLite UPSERT 触发器误报。SQLite 仍为默认后端，无 API、配置、Wire/OSSCTL 或数据格式变更。v1.29 交付 `.oss.sys` 系统命名空间、控制对象 CAS/单调 fencing/持久化锁 token/提交证书与 quorum 读，以及 `vpc_endpoints` 迁移；v1.28 系列交付 Raft 日志格式标记与旧区间有界压缩、M8 follower restore fencing、AP 直连 mTLS/AWS PrivateLink API 验证、CMK→legacy 全量迁移、Level C 只读排空、JWT 多密钥轮换、feature 9 opt-in 严格硬配额，以及真实蓝绿/MPU 排空/单机→集群 e2e；v1.28.1–v1.28.5 为 CI/测试稳定与性能加固补丁。
**以 docs/ 为准**：

- `docs/superpowers/specs/2026-10-10-minio-clone-design.md` —— 总体设计（9 阶段 M1–M9、盘布局、xl.meta v1；§8 含各阶段落地状态）
- `docs/superpowers/specs/2026-10-10-feature-spec.md` —— 功能域规格（行为/接口/边界/错误码；M5 域含落地记录）
- `README.md` —— 运行 / 运维 / 验收 / 已知边界（对外口径的主入口）
- `docs/crate-upgrade-checklist.md` —— `aetherupload-rust` 跨版本升级 checklist（高风险依赖变更）

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
