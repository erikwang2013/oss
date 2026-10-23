# 能力边界清零验收记录（2026-10-23）

- 基线：`v1.13.1`（`a58d64a`）
- 发布：**v1.14.0**（2026-10-23），tag `v1.14.0`
- 集成结果：已合并到 `main`（能力代码 `86616eb`；其后为测试、文档、站点与版本号更新）
- 状态：实现、集成、聚焦测试与 S3 全量 e2e 完成；已提交、推送并发布
- 构建环境：主机 `/home` 为 5400rpm 机械盘且被其它会话重压，集成验证统一使用
  `CARGO_TARGET_DIR=/tmp/target-retention`（SSD）；`CARGO_BUILD_JOBS=1`。

## 1. 交付范围

### S3 协议

| 能力 | 落点 | 状态 |
|---|---|---|
| ListObjects v1（marker/delimiter/max-keys/NextMarker） | `xl-s3/service/list_v1.rs` | 已完成（前置批次） |
| 版本控制三态 Off/Enabled/Suspended、null 版本 | store/service 全写删路径 | 已完成（前置批次） |
| 版本列表分页修复 | `xl-store/store/version.rs` + mesh 透传 | 已完成（前置批次） |
| UploadPartCopy | `service/upload_part_copy.rs`（区间/条件/versionId/SSE-C/边界） | 已完成 |
| ListMultipartUploads（含集群 `ListUploadsLocal` RPC） | `service/list_multipart_uploads.rs`、xl-proto/xl-client/mesh | 已完成 |
| GET/HEAD `partNumber`（`part_sizes` 元数据、段读、Content-Range） | `xl-store/meta.rs`、`store/read.rs`、`service.rs` | 已完成；旧对象退化 part 1 |
| 多区间 Range、HEAD Range | router 中间件剥多区间；HEAD 区间读 | 已完成（见边界） |
| noncurrent expiration/transition（`NewerNoncurrentVersions`） | `xl-s3/lifecycle.rs`、`oss-server/lifecycle.rs`、DB/Raft | 已完成 |
| 匿名 Principal `"*"`（仅数据面白名单） | `access.rs`、`policy.rs`、`delete_objects` 逐键 | 已完成；安全复审关闭 |
| Put/GetObjectRetention + 控制台锁状态 | `service/retention.rs`、`store/version.rs`、console/web | 已完成 |
| `s3:ObjectAccessed:*`（Get/Head） | `events.rs`、`notify.rs`、`service.rs` | 已完成 |

### TLS 与传输

ecat 监听器、witness、tonic mesh、site/repl/tier 出站 HTTPS、Kafka SASL/SSL、`[tls]`
配置与 fail-closed 已在集成基线完成；本批新增 TLS 单测修复（对端地址允许单个尾斜杠）。

### M8

| 能力 | 状态 |
|---|---|
| Inventory Parquet + Hive `symlink.txt`（CSV 口径不变） | 已完成 |
| tier 本地冷盘池 `kind="local"`（原子写/MD5/路径穿越/恢复） | 已完成 |
| tier 远端 GC/对账（admin、默认 dry-run、live 引用/grace/上限、集群 degraded fail-closed） | 已完成 |
| 生命周期完整 Filter（tag/size/And/Date/EODM/多转换）+ 控制台编辑器 | 已完成 |
| Batch/Object Lambda WASM（`olap-wasm` 门控，默认关闭；超时/fuel/坏模块/feature off fail-closed） | 已完成 |
| AP DeleteObjects 逐键、`<name>-s3alias` 路径/Host、SourceIp 条件、控制台 Alias 列 | 已完成 |

### M9 滚动升级

Level B 前向兼容窗口已实现：Raft RPC wire/feature 头、term-stamped peer map、leader
`effective_feature_level`（含 learners）、`[raft].feature_level` 校验、启动/apply 门控、
`schema_meta.raft_feature_level` 粘性标记、成员 add/promote 预检、status 诊断、runbook 与
`tests/e2e_m9_rolling.sh`。D6 原判「本批无 replicated `ControlData` 变更」不成立：
`LifecycleRuleRow.config_json` 已进快照。2026-10-23 热修把 `BucketSetLifecycleV2` 标为
level 2（`feature_level=1` 时 append 前拒绝，503 `feature_unavailable`），并让快照编码器
在存在 `config_json` 时拒绝 OSSCTL1；`raft-cutover` 复用同一守卫（`--dry-run` 亦拒绝），
避免安装出无法建快照的基线（fail-closed，待 OSSCTL2）。

## 2. 明确不做的边界（非目标，fail-closed/记档）

- AP 匿名、跨账号、VPC 端点、AP 桶级 Create/Delete/ListBuckets、AP 缓存一致性。
- Inventory 增量/跨账号；Batch 抢占/跨账号/每动作桶通知。
- Lifecycle `AbortIncompleteMultipartUpload`（需 MPU 索引，继续 501）。
- Tier 流式搬运（全内存）。
- Level C 任意双向混合版本；M3/M4 数据面、TLS 物料、跨版本 cutover/force-recover 仍非滚动。
- 匿名控制面/桶级操作、匿名对象锁/保留期头（一律 403）。

## 3. 验收证据（HEAD `86616eb` / `6182b77`）

| 闸门 | 命令 | 结果 |
|---|---|---|
| 格式 | `cargo fmt --all --check` | 0 |
| Clippy | `cargo clippy -p oss-server -p xl-s3 -p xl-store -p xl-client -p xl-proto --all-targets -- -D warnings` | 0（`state.rs::new_with` 标注既有 too_many_arguments） |
| 库测试 | `cargo test -p xl-store --lib` / `-p xl-s3 --lib` / `-p oss-server --lib` | 259+1 ignored / 136 / 140，全 0（security 修复后 xl-s3 复跑 136） |
| S3 协议批 | `bucket_ops, object_ops, mpu_ops, lock_ops, lifecycle_ops` | 84 passed，0 |
| M8 批 | `m8_batch, m8_lifecycle_api, lifecycle, m8_inventory, m8_inventory_parquet, m8_tier, m8_tier_gc, console_api` | 89 passed，0 |
| M9/TLS/auth 批 | `raft_handshake, raft_membership, raft_commands, m9_contract, tls_config_matrix, tls_listener, tls_outbound, tls_mesh, observability, permissions, policy_audit, s3_over_http` | 51 passed，0 |
| AP/安全聚焦 | `m8_ap` 17、`permissions` 5、`policy_audit` 2、`s3_over_http` 1 | 全 0 |
| 生命周期/控制台 | `lifecycle` 8、`m8_lifecycle_api` 13 | 全 0 |
| S3 全量 e2e | `tests/e2e_s3.sh`（真实进程 + aws-cli/rclone + redpanda） | **38 节全部通过，RC=0**；含 v1 列表、MPU/partNumber/Range、Suspended、匿名 §17f、SSE/对象锁/STS、Select/Kafka/复制、AP 大对象回归 |
| 安全复审 | `/tmp/security-review-capability-gaps.md` + `/tmp/security-recheck.md` | 0 Critical；2 High/1 Medium/4 Low 全部 CLOSED；GO |

分页补充回归：`list_marker_delimiter_pagination_no_loss`（V1 marker+delimiter 跨页 CP 不丢不重）通过。

## 4. 安全修复摘要

- **H1 AP op 混淆**：`derive_op` 严格查询白名单；`?legal-hold` 映射独立 `Get/PutObjectLegalHold`
  策略动作；`x-id` 必须等于推导 op；multipart POST 一律拒绝；重写后再次推导，不一致 500。
- **H2 SourceIp**：IP 条件不可求值时 Deny 命中、Allow 不命中；`X-Forwarded-For` 只取最右一跳
  （仅单层反代）、重复头行视为无对端；补测试。
- **M1 匿名审计放大**：匿名 deny 审计走进程级令牌桶（20 burst / 5 每秒），allow 不抽样。
- **L1 匿名锁头**：匿名请求携带对象锁/保留期/bypass 头一律 403。
- **L2 AP 资源轴**：对象动作只匹配对象 ARN，桶 ARN 只参与桶级动作（破坏性变更已在 README 记档）。
- **L3/L4**：AP 无策略不额外限制、旧 `*-s3alias` 桶被遮蔽，均记档。

## 5. 已知限制 / 后续项

1. `part_sizes` 只在 MPU complete 落盘；复制/tier 摄入路径未携带，复制对象的 `partNumber`
   退化为旧语义（part 1 整对象，>1 416）。建议下一批在复制/摄入 meta 中透传。
2. 签 Range 的 SigV4 客户端（Range 计入 SignedHeaders）多区间请求会 403
   `SignatureDoesNotMatch`；预签名 URL 不受影响（中间件在验签前剥离）。
3. AP `X-Forwarded-For` 仅单层反代成立；多层反代需在最近一跳覆盖。
4. 滚动升级 e2e 脚本已提供但未实跑（缺 v1.13.1 旧二进制）；level-2 产品命令为
   `BucketSetLifecycleV2`（结构化生命周期），写入门控/激活由 `tests/raft_commands.rs`
   与 store 单元测试覆盖。
5. Web `tsc`/`vite` 未运行（worktree 无 `node_modules`）；前端改动为增量、类型一致。
6. 生命周期破坏性 sweep 的进程内测试覆盖，但未跑独立长时 e2e；AbortIncompleteMultipartUpload 仍 501。

## 6. 文档、图片与站点同步（2026-10-23）

- README：匿名桶策略、AP 资源轴破坏性升级注意、gRPC/滚动升级口径、sync Suspended、
  桶默认加密、MPU/控制台标签、生命周期结构化配置、partNumber 复制退化、Level B 状态/限制/
  配置/测试锚、事件白名单、复制限速、技术栈版本与测试计数均已同步。
- Feature spec：请求头阶段表、CopyObject REPLACE、对象标签、Suspended DELETE、retention/bypass、
  生命周期 §9、分层 §14、Inventory Parquet/Hive、Batch WASM、运维命令、TLS §1.5 已更新。
- 设计文档：状态/版本、桶默认加密、AP 别名、MPU 标签、M6/M7/M8/M9 的 TLS 与 Level B、
  tier 本地盘/GC、非目标列表已更新；历史计划统一加「历史文档」横幅。
- 图片：architecture / M6 / M7 / M8 / object-lifecycle / request-flow 标签更新；
  social-preview 修正 forward-ref 渐变并重渲染（中文与蓝色渐变正常）；
  mascot-static.png / apple-touch-icon.png 用 rsvg-convert 重渲染（修复黑底）。
- 站点：index/install/usage/changelog/api 增补新能力、TLS/保留期/匿名/滚动升级/协议边界；
  未发布内容统一标「未发布（能力边界清零）」，最新发布仍为 v1.13.1；sitemap/manifest/SEO 同步。

## 7. 发布状态

提交、测试、文档与站点已纳入 `main`，并发布 **v1.14.0**（tag 与 GitHub Release）。
注意：GitHub 仓库当前为 private，站点上的 GitHub/Release/源码包链接对未授权访客不可用；
公开仓库需单独决定。站点静态文件已纳入版本控制，但尚未部署到 `oss.erik.xyz`。
