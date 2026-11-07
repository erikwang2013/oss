# oss

自研分布式对象存储系统（S3 兼容，MinIO 的等价实现）。Rust 2024。

## 当前状态

M1–M9 已完成，最新发布 **v1.32.2（2026-11-07）**；M5 安全与锁定全部交付——集群行一致性
广播 · 密钥库/KMS · 加密数据面 DARE 式 · S3 面 SSE-S3/KMS/C · 对象锁 · STS（含
`AssumeRoleWithWebIdentity`）· SK 信封加密落库。v1.31 交付对象控制存储后端（opt-in）：27 张控制表的节点本地 COW B+tree、单根 CAS、盘级多数派 quorum；读侧热视图按 generation 切换且只重建脏域；文件 Raft 日志/元数据/快照与 witness 元数据状态机；站点 LWW apply/ship/配对走缝，游标/死信本地化；账户用量本地缓存 + 复制账本采样校准；10 万行快照导入 8.3s、导出 1.5s。**v1.32.0 移除 SQLite，对象控制根成为唯一后端**：根 `control/state/root` CAS 提交，节点/大值内容寻址存于 `<data_dir>/.oss.sys/control/state/{node,blob}/`，本地运行态在 `<data_dir>/.oss.sys/local/`，Raft 文件在 `<raft.db_path>.d/`；`sqlx`/SQLite、`migrations/`、`[cluster]`/`[stretch]` 与 `database_url` 移除。升级为停全部节点的一次性单向割接：冷备 `oss.db`/`raft.db`/数据盘，用归档的 GitHub Release v1.31.0 二进制 `oss-server-v1.31.0-linux-x86_64`（sha256 `fc2ea96fd0eb1777724da7b9ad07b4ad6520da7373bc9780099d8a211560b1ae`）执行 `control-migrate --from <oss.db> --state-dir <data_dir> --raft-db <raft.db> --dry-run|--apply`，校验后启动新二进制；首次新写入后无反向写入器。控制写 release build 实测（fsync on）：单写 p99 9.2ms、8 写线程中位 p99 12.7ms、吞吐约 1.4k 次/s（SATA SSD），达到单盘 ≤20ms / 多盘 ≤50ms 预算；本机高负载机械盘会出现 >20ms 的尾延迟尖峰，不宣称任意负载下 p99 达标。v1.32.0 缺省配置仍选择已删除的 SQLite 后端，未写 `[server].control_backend` 的安装启动即 panic；v1.32.1 让对象控制根无条件装配，显式 `sqlite` 以迁移指引拒绝。v1.32.2 用 `vendor/` 补丁移除 protobuf 2.28、lru 0.7.8 与 unmaintained instant，`cargo audit` 退出 0。v1.30 完成访问点/OLAP、lifecycle/inventory/tier、buckets/bucket_policies 等切片迁移与集群遗留控制对象重认证；v1.29 交付 `.oss.sys` 命名空间、控制对象 CAS/fencing/quorum 读与 `vpc_endpoints` 迁移。
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
  `tests/e2e_no_db.sh`（strict no-db 门禁：standalone/site/Raft/witness）
