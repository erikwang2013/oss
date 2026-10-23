# 能力边界清零计划（2026-10-22）

## 目标

把 README、设计文档和 M9 计划中仍标注为“未接线 / 未实现 / 非目标”的产品能力逐项落地，保持现有 S3、M7、M8、M9 行为不回归。

基线：`a58d64a`（v1.13.1）。工作分支：`feat/capability-gaps`。

> **完成状态（2026-10-23）**：工作流 A–D 已全部实现、集成并验收；交付范围与证据见
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md`。本文件保留为计划基线，
> 「待侦察补充」已由四份 recon 报告落实。

## 工作流

1. 侦察：每个工作流先产出实现报告，明确代码路径、数据模型、测试和风险。
2. 实现：每个写手在独立 worktree 中改独立文件；热文件按顺序合并。
3. 评审：每个工作流至少一名只读 reviewer 做 delta review。
4. 验证：聚焦测试、全量 e2e、并发故障注入；证据绑定到集成 HEAD。
5. 发布前：版本、README、设计文档、验收记录同步；单独授权后才 push/tag/release。

## 工作流 A：S3 协议缺口

- ListObjects v1
- UploadPartCopy
- ListMultipartUploads
- GET partNumber
- 多区间 Range 与 HEAD Range
- 版本控制 Suspended / null version
- noncurrent version expiration
- 匿名 Principal
- Put/GetObjectRetention 与控制台锁状态
- `s3:ObjectAccessed:*` 事件

## 工作流 B：TLS 与安全传输

- HTTP/S3/控制台监听器 TLS
- Raft/site/witness 路由 TLS
- gRPC mesh TLS + 节点身份校验
- site_client / repl_client / tier 出站 HTTPS
- Kafka SASL/SSL（先确认 rskafka 0.6 可行性）
- `[tls]` 配置、证书/CA、fail-closed、自签测试证书

## 工作流 C：M8 能力

- 访问点 alias / 匿名 / 跨账号 / VPC
- 访问点缺失操作
- Batch 抢占 / 跨账号清单
- Inventory Parquet / hive / 跨账号 / 增量
- tier 本地冷盘池与远端 GC
- 生命周期完整 Filter
- Object Lambda WASM 与边界
- 控制台对应页面

## 工作流 D：滚动升级

- 版本握手 / 兼容窗口
- 混合版本集群的 quorum 与快照安全
- 配置门控与回滚
- 三 voter 旧/新混跑测试
- 若无法安全实现，必须给出可验证的限制证明和自动拒绝条件

## 验收闸门

- `cargo fmt --all --check`
- `cargo clippy -p oss-server -p xl-s3 -p xl-store -p xl-client -p xl-proto --all-targets -- -D warnings`
- 聚焦单元/集成测试
- S3 协议矩阵、TLS 真进程 e2e、M8 e2e、M9 strict/stretch/recovery、M7 e2e
- 所有新增开关默认 fail-closed，旧配置行为不变
- 文档与 README 边界表同步

## 待侦察补充

具体文件、依赖、合并顺序和“不可行”判定由四个 recon 报告补充后冻结。
