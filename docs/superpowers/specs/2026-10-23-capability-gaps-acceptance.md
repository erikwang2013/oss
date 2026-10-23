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

- AP 跨账号、VPC 端点、AP 桶级 Create/Delete/ListBuckets、AP 缓存一致性
  （AP 匿名已于后续批次支持，见 §8）。
- Inventory 增量/跨账号；Batch 抢占/跨账号（每动作桶通知已于后续批次实现，见 §8）。
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
- **L3/L4**：AP 无策略不额外限制、旧 `*-s3alias` 桶被遮蔽，当时记档；
  均已在后续批次修复（无策略 → 拒绝；无 AP 别名回落桶路由），见 §8。

## 5. 已知限制 / 后续项

1. ~~`part_sizes` 只在 MPU complete 落盘；复制/tier 摄入路径未携带~~ —— **已在 post-1.14 硬化修复**：
   站点复制/raft 重推透传，tier 归档/恢复保留。
2. ~~签 Range 的 SigV4 客户端多区间请求会 403~~ —— **已修复**：改为确定性 400
   `InvalidArgument`；未签 Range 的请求仍 200 整对象。
3. ~~AP `X-Forwarded-For` 仅单层反代成立~~ —— **已修复**：支持 hops/CIDR 信任链，
   非法/重复 XFF fail-closed。
4. 滚动升级 e2e 已实跑（65 PASS / 0 FAIL / 4 预期 SKIP）；level-2 产品命令为
   `BucketSetLifecycleV2`（结构化生命周期），写入门控/激活由 `tests/raft_commands.rs`
   与 store 单元测试覆盖；v1.14.0 的 V2 级别误判已由 post-1.14 热修复修正。
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

## 8. 2026-10-23 后续修复（v1.14.1 候选）

- **4.1 part_sizes**：站点复制、raft 同 id 重推与 sync 写门新增内部签名头 `x-oss-part-sizes`；
  摄入端仅 REPLICA 接受，store 信任边界校验（≤10000 段、总和 == 实际 size）后落
  `Version.part_sizes`；tier 归档/恢复原地保留。异构远端复制/非 MPU 单段 PUT 仍为旧语义。
- **4.2 签名多区间 Range**：中间件按 `Authorization` SignedHeaders /
  `X-Amz-SignedHeaders` 识别；Range 签入则保留至验签后由 s3s 返回确定性 400
  `InvalidArgument`；未签入仍剥离 → 200 整对象。
- **4.3 AP SourceIp 信任链**：`OSS_AP_TRUSTED_PROXY_HOPS`（默认 1，向后兼容）或
  `OSS_AP_TRUSTED_PROXY_CIDRS`；重复/格式非法 XFF fail-closed；非法配置启动失败。
- **4.4 AP 无策略默认拒绝**（破坏性，AWS 对齐）；DeleteObjects 同口径。
- **4.5 无 AP 的 `*-s3alias` 回落普通桶路由**；与 AP 别名冲突时 AP 优先（AP 缓存未命中
  时回落，可用性取舍）。
- **4.6 匿名审计**：`OSS_ANON_DENY_AUDIT_{BURST,PER_SEC,DEDUPE_SECS}` 可配置并钳制；
  `OSS_AUDIT_RETENTION_DAYS` 钳 0..=36500，清理按批 10000。
- **3.1 AP 匿名**：无凭据请求经 AP 需 AP 策略 `Principal:"*"` 与桶策略匿名基线同时放行
  （交集，AP 不放宽桶策略）；仅数据面白名单，控制面/桶级/锁头拒绝，审计 uid=0。
  此条为安全敏感变更，须单独复审。
- **3.2 账户命名空间 v1**：`account_id_of(uid)` 生成 12 位账户（账户 = 用户）；桶账户取
  桶主（admin/NULL 桶 = `000000000000`）；AP ARN 账户段 = 桶账户，旧 LEGACY 账户段保留
  一版作资源别名。桶策略/AP 策略 `Principal` 支持 `arn:aws:iam::<12位>:root`（同 uid），
  LEGACY 主体永不匹配，`user/<name>` 形态不支持。**非租户隔离**：无账户管理员/共享/配额，
  既有权限与桶策略语义不变。破坏性升级点：此前静默无效的 ARN 主体开始生效。
- **3.3 AP SourceVpc 来源证明**：`StringEquals.aws:SourceVpc`（保存时校验规范形态）；
  `OSS_AP_VPC_ID` 静态证明优先，否则仅 `OSS_AP_TRUSTED_PROXY=1` 采信恰好一条
  `x-oss-source-vpc`；重复/畸形/未信任 → 无证明。无证明时 Deny 命中、Allow 不命中
  （fail-closed）。**不是 PrivateLink 端点身份**。
- **3.4 AP 桶级动作拒绝**：策略保存/加载时拒绝 `s3:CreateBucket`/`s3:DeleteBucket`/
  `s3:ListAllMyBuckets` 精确动作（通配仍允许）；此前接受但永远不生效。旧策略含这些动作
  会加载失败（500 fail-closed），需移除或改通配。
- **3.5 AP 缓存 TTL/负缓存/有界**：`OSS_AP_CACHE_TTL_SECS`（默认 30）、
  `OSS_AP_NEGATIVE_CACHE_TTL_SECS`（默认 5，0 = 关闭）、`OSS_AP_CACHE_MAX_ENTRIES`
  （默认 10000）；合法范围外启动失败。未知 AP 负缓存、DB/解析错误不缓存（保持 500）；
  控制面写即时失效；TTL 兜底。跨节点读己之写受 apply 延迟影响，强一致性 non-goal。
- **3.9 Batch 每动作桶通知**：成功动作经 `EventSink` 补发到**对象所在桶**的配置
  （Copy→目标桶 `s3:ObjectCreated:Copy`；Delete→`s3:ObjectRemoved:Delete` /
  `DeleteMarkerCreated`；Restore→`s3:ObjectRestore:Post`；NotFound、ReplaceTags、
  InvokeLambda 不发）。事件源 `source.userAgent="oss-batch"`；不跨桶/跨账号投递；
  发射失败不影响任务结果（outbox 至少一次）。
