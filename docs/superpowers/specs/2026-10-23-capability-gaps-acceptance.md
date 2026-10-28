# 能力边界清零验收记录（2026-10-23）

- 基线：`v1.13.1`（`a58d64a`）
- 历史发布：**v1.14.0**（2026-10-23）· **v1.15.0** / **v1.16.0**（2026-10-24）·
  **v1.17.0**（2026-10-24，§13 架构切片）· **v1.18.0**（2026-10-25，文档精简）·
  **v1.19.0**（2026-10-26，Wave1 地基）
- 最新发布：**v1.20.1**（2026-10-28），tag `v1.20.1`；包含 §15（软配额 + shrink e2e + 锁修复）
- 集成结果：已合并到 `main`（v1.14.0 能力代码 `86616eb`；v1.15.0 为 post-1.14 硬化集成；
  v1.16.0 为 §2–§4 backlog 第二轮；v1.17.0 为 §13 架构切片；v1.18.0 文档精简；
  v1.19.0 为 Wave1 地基；v1.20.0 为 §14 Wave2 缺口修复，三条工作流独立评审后合并）
- 状态：实现、集成、聚焦测试、S3 全量 e2e、M8/M9/E2E_LONG 完成；已提交、推送并发布
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
- Inventory 增量（节点本地水位，沿用既有 3.6 实现）；跨账户目标已由 3.7 支持（仅管理员可配置，
  见 §8）；Batch 抢占/跨账号（每动作桶通知已于后续批次实现，见 §8）。
- Lifecycle `AbortIncompleteMultipartUpload`（需 MPU 索引，继续 501）。
- ~~Tier 流式搬运（全内存）~~ —— post-1.14 3.11 已实现：Transition 按 8 MiB 窗口读取源、
  出站 256 KiB 块流式发送（S3 先落暂存文件预计算 SigV4 payload hash）；Restore 流式落盘并
  校验长度/SHA-256，空间不足或校验失败回 Archived 并清理暂存。Archived GET（含 Range）
  直读远端仍不在范围，须先 Restore。
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

v1.14.0、v1.15.0、backlog 第二轮与 v1.17.0 已合并到 `main`；**v1.17.0（2026-10-24）** 为 §13
架构切片，tag 与 GitHub Release 均按 `vx.y.z` 命名。
注意：GitHub 仓库当前为 public；站点静态文件已更新到 v1.17.0，但尚未部署到 `oss.erik.xyz`。

## 8. 2026-10-23 后续修复（随 v1.15.0 发布）

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
- **3.7 Inventory 跨账户目标**：配置写入时记录源桶 12 位账户（存储 JSON 扩展键
  `ossSourceAccountId`，旧配置读取时按当前桶账户推导）；`destination_account_id`（S3 线格式
  `AccountId`）可选，若提供必须为 12 位数字且等于目标桶当前账户。源/目标桶账户不同的
  **跨账户目标仅管理员可创建**（非管理员 403，即使持目标桶写权限）；运行期复核记录源账户与
  目标桶账户，漂移即失败且不写任何输出；写入仍走配置所有者的 `PutObject` 检查，不新增绕过。
  目标桶字段接受桶名或 `arn:aws:s3:::<bucket>`（归一为桶名）。manifest 追加 `accountId`（源）
  与 `destinationAccountId`（目标）。**账户 = 用户命名空间 v1，不是租户隔离**；增量模式仍为
  3.6 的节点本地实现，本项不改。
- **3.9 Batch 每动作桶通知**：成功动作经 `EventSink` 补发到**对象所在桶**的配置
  （Copy→目标桶 `s3:ObjectCreated:Copy`；Delete→`s3:ObjectRemoved:Delete` /
  `DeleteMarkerCreated`；Restore→`s3:ObjectRestore:Post`；NotFound、ReplaceTags、
  InvokeLambda 不发）。事件源 `source.userAgent="oss-batch"`；不跨桶/跨账号投递；
  发射失败不影响任务结果（outbox 至少一次）。


## 9. 2026-10-23 后续能力：SSE-KMS 显式 CMK（3.16，随 v1.15.0 发布）

- **线级**：`x-amz-server-side-encryption-aws-kms-key-id` 接受裸名或
  `arn:…:key/<name>`，须与 `aws:kms` 同现；名 `[a-z0-9-]{1,64}`（首尾字母数字），
  ARN 归一为规范名。未知/未配置 → 400 `InvalidArgument`（fail-closed，不回退默认钥）；
  与 SSE-C 互斥；有 CMK 但节点无 KMS provider → 500（部署错误，不降级明文）。
- **密钥环**：`<OSS_KMS_DIR>/cmk/<name>/key-<n>.bin`，每环独立；创建 `key-0`、轮换追加
  `key-<n+1>` 且新写用新钥，旧版本保留解密（同 AWS KMS 不重加密旧密文）；文件 0600、
  32B、规范化命名校验；`cmk/` 根目录逐项校验；**无删除 API**（删有活对象的 CMK = 数据丢失，
  crypto-shredding 需单独 rewrap 作业）。
- **记录**：`EncryptionRecord.kms_key_id`（`#[serde(default)]`，旧记录可读）；对象
  `key_id="cmk:<name>:k<n>"`，用户态字段存规范名；解码校验 canonical key_id、名一致、
  长度/可打印，异常记录失败关闭。旧二进制遇 `cmk:` key_id、`aws:kms:<name>` 桶默认或
  `sse-kms:<name>` MPU kind 一律失败关闭（不 promise 滚动兼容）。
- **桶默认加密**：`aws:kms:<raw id>` 存入既有 TEXT 列；PUT 时要求 CMK 已存在，GET 原样
  回显配置的 id（名或 ARN）；`AES256` + key id / SSE-C 规则拒绝。
- **MPU / Copy**：`sse_kind` 扩展 `sse-kms:<name>`，create/complete 严格匹配；CopyObject
  目标显式 CMK 覆盖桶默认；`sse_out`/CreateMPU/UploadPart 回显 CMK。
- **控制台**：`GET/POST /api/v1/encryption/cmks` 与 `POST …/{name}/rotate`（admin-only，
  rotate 返回新版本）；审计 `control:cmk-create` / `control:cmk-rotate` 仅记名/版本/指纹；
  `/api/v1/encryption` 增加 `cmks`（名、当前版本、版本表、更新时间，无钥字节）；Web 设置页
  只读展示命名 CMK，桶页保留 `aws:kms:<cmk>` 原值避免误清空。
- **测试证据**（本地，`CARGO_TARGET_DIR=/tmp/target-cmk`）：
  `cargo test -p xl-store --lib` 268 passed；`cargo test -p xl-s3 --lib` 139 passed；
  `--test object_ops` 55 passed；`--test bucket_ops` 9 passed；`--test mpu_ops` 11 passed；
  `cargo test -p oss-server --test console_api` 30 passed；`cargo fmt --all -- --check`、
  clippy 与 Web `tsc -p tsconfig.app.json --noEmit` 见提交记录/报告。
- **提交**：`2c075b7` 命名 CMK 环/信封/记录；`a18ead5` MPU 与存储测试；`9e714ce`
  S3 接入；`c5b1dd6` S3 线级测试；`86b757a` 控制台 API；`954c616` 前端。
- **残留风险**：多节点须共享同一 KMS 目录且同步新增/轮换，缺环节点 4xx/5xx fail-closed；
  CMK 不是授权边界（任何可写对象者可选用已存在 CMK）；无删除与 rewrap/re-encrypt 作业；
  对象响应回显规范名而桶默认原样回显，ARN 场景两处字符串可能不同（语义同一把钥）。

## 10. 2026-10-23 第二批：Select Parquet 与 M7 版本/元数据复制

| 能力 | 落点 | 状态与边界 |
|---|---|---|
| 4.18 S3 Select Parquet | `xl-s3/select.rs`、`service.rs` | 已完成：低层 row reader 逐行按列名转 JSON 对象，复用既有 SQL 子集（`s.col`/`s.a.b`，`SELECT *` 整行对象，`_N` 不可用）；请求 `CompressionType` 必须 `NONE`，不接受 ScanRange（400）；行组 `total_byte_size` 合计 ≤512MiB；文件内部仅 NONE/SNAPPY（parquet `snap` feature）；BZIP2 仍 400（工作区无 bzip2 依赖，不新增） |
| 4.16 M7 版本定向删除复制 | `oss-server/repl_client.rs`、`replication.rs`、`xl-s3/service.rs`、`db.rs`、`job_actions.rs` | 已完成：站点 PUT 强制沿用源 UUID（token 认证；同 id 同字节幂等、异体 409）；marker 同 id 创建；`DELETE ?versionId=` 带 REPLICA + bypass，目标 404 视为已删；同版本更早出站行未结算前等待（防复活）；M6 远端不投递版本删除；`repl_outbox.is_version_delete` 幂等补列 |
| 4.17 M7 元数据复制 | `repl_client.rs`、`replication.rs` | 已完成：标签/存储类/checksum/content-type/用户元数据/对象锁随版本下发（到期 retention 不下发、legal hold 保留）；仅 SSE-S3 以 AES256 复制、目标本地重封装；**SSE-C 与 SSE-KMS 永久死信且不发请求**（不静默换钥/降级明文）；归档类 GLACIER/DEEP_ARCHIVE 不复制 |
| 4.13 M3 MPU 粘节点 | — | **阻塞/设计，未完成**：upload_id 为纯 UUID 且暂存区节点本地，`MeshTransport` 无 MPU 转发 RPC，`OssS3` 无 mesh/路由句柄；安全实现需 upload_id→节点持久目录 + 流式转发 RPC + 失败语义，超出本 lane 最小改动。维持 README/M3 C8「MPU 会话粘节点」边界；测试不可行（无路由层可测） |

**边界（记档）**：混合版本升级前已复制的历史异步版本两端 UUID 不同，其定向删除在目标按同 id
找不到而幂等成功，可能短暂不一致（建议同版本升级）；M6 远端仍不携带对象元数据、不投递版本删除；
直接读 tier 仍不在范围——归档对象需 Restore 本地化（§14.2 直读 403 `InvalidObjectState`）。

## 11. 2026-10-24 post-v1.14 硬化集成与最终闸门

**集成**：`feat/post-1.14-hardening`（基于 `v1.14.0`），合并 §8–§10 各 lane、验证 lane 与
Inventory 跨账户 WIP；闸门证据对应 Rust/前端树 `2bda0bc`（其后仅文档/站点/版本号提交）。
**已发布 v1.15.0（2026-10-24）**：合并到 `main` 并打 tag/Release；仓库 private、站点未部署（§7）。

### 11.1 集成树闸门

| 闸门 | 命令/范围 | 结果 |
|---|---|---|
| 格式 | `cargo fmt --all --check` | 0 |
| Clippy | `cargo clippy -p oss-server -p xl-s3 -p xl-store -p xl-client -p xl-proto --all-targets -- -D warnings` | 0 warning |
| 库测试 | `cargo test -p xl-store -p xl-s3 -p oss-server --lib` | 268（1 ignored）/ 150 / 152，0 failed |
| 清单/控制台 | `--test m8_inventory`、`m8_inventory_parquet`、`console_api` | 20 / 4 / 30，0 failed |
| S3 全量 e2e | `tests/e2e_s3.sh`（38 节；真实进程 + aws-cli/rclone + redpanda；服务同时设 `OSS_MASTER_KEY_FILE` 与 `OSS_KMS_DIR`；脚本 md5 `16202c6d7910e354be5aaa74fd5d3b3f`） | **RC=0**；仅 `E2E_LONG` 默认跳过；含 4.2 签名多区间 `400 InvalidArgument` 断言 |
| Web | `tsc -b` / `vite build` / Playwright `e2e/m8-console.spec.ts` | 0 / 0（仅既有 >500 kB chunk 警告）/ **9 passed** |
| M9 lane | `e2e_m9` sync / stretch / recovery / rolling | 78/0/1、59/0/1、133/0/1、65/0/4（SKIP 为预期矩阵） |
| M8 lane | `tests/e2e_m8.sh` | PASS，4433s（Tier 段按环境 SKIP） |

M9/M8 行为合并前各 lane 树上的实跑结果；合并后重跑的是上表库测试、S3 全量 e2e 与 Web。
e2e 首次运行暴露两处**测试断言漂移**（非产品缺陷）：`e2e_s3.sh` 11d 仍断言旧 403
`SignatureDoesNotMatch`，已按 §8 4.2 改为 400 `InvalidArgument`；Playwright 设置页仍匹配旧文案
「配置由环境变量驱动」，已按 CMK 改版改为 `/环境变量驱动/`。修正后均重跑通过。

### 11.2 本批状态汇总

§8–§10 的能力均已在集成树落地：AP 匿名/账户命名空间 v1/SourceVpc/缓存 TTL/默认拒绝/旧别名回落、
XFF 信任链、CMK 命名环、TLS 热加载、tier 流式、Batch 每动作通知、M7 版本定向删除与元数据复制、
Select Parquet、本地 tier openat2 加固、GC 二次快照、生命周期游标、Inventory 增量、AbortMPU、
Level B 生命周期级别热修；本批另并入 Inventory 跨账户目标（§8 3.7，配置/运行期双重账户复核）。
破坏性升级点与逐项边界以 §8–§10 为准。

### 11.3 仍阻塞 / 非目标（最终状态）

- **Level C 任意双向混合版本**：旧解码器（v1.13.1）无法识别 Level B 数据，需双解码/双写窗口与
  回滚矩阵；§12 只做了 LC-0 来源校验，完整 Level C 未做。设计见
  `docs/superpowers/specs/2026-10-24-architecture-backlog.md`。
- **M3/M4 数据面滚动升级**：Level B 仅覆盖控制面命令与快照兼容；数据面、TLS 物料（mesh 入站）、
  跨版本 cutover/force-recover 仍要求停写/滚动重启；设计见架构 backlog。
- **完整多租户账户体系**：账户 = 用户命名空间 v1，无账户管理员/共享/配额/隔离；AP 跨账号与
  Inventory 目标账户都不是租户边界。设计见架构 backlog。
- **M3 shrink / 单机→集群**：架构项目，未实现；设计见架构 backlog。
- **M8 多节点控制面**：§12 已做单例后台任务 leader 门控，但作业/清单/恢复/审计运行状态
  仍为节点本地，无 fencing/复制；完整多节点见架构 backlog。
- **直接读 tier（含 Range）**：仍不直读远端，归档对象须先 Restore；缺分块哈希，范围读无法
  在不取整对象时校验完整性。
- **Inventory**：跨账户目标仅管理员可配置，运行期复核失败即不写输出；增量仍是节点本地水位。
- **M7 混合版本历史异步版本**：§12 新增内容指纹兼容删除，但只对升级后新删除生效，升级前
  已发生的删除不回溯；M6 无 `site_token` 的旧远端仍不携带元数据/版本删除。
- **已由 §12 补**：`AssumeRoleWithWebIdentity`、Select BZIP2、M3 MPU 跨节点路由、CMK
  删除/rewrap、OSSCTL2/LC-0、M6 token 远端、TLS mesh/Kafka 热加载、Batch 抢占/跨账户清单、
  M8 leader 门控、tier 清扫/预留/0600、AP 可信对端 CIDRS。

## 12. 2026-10-24 backlog 第二轮：执行 §2–§4

**范围**：v1.15.0 后剩余验证缺口、显式非目标与技术债。集成分支
`feat/post-1.15-backlog`（基于 `v1.15.0`），已合并到 `main` 并发布 **v1.16.0**。

### 12.1 已落地

| 项 | 落点 | 边界/证据 |
|---|---|---|
| Select BZIP2 | `xl-s3/select.rs`、`service.rs` | bzip2 0.6（纯 Rust 后端），CSV/JSON 多流解压，512MiB 解压预算；ScanRange 仍 400；Parquet 仍要求 `CompressionType=NONE`；单测+`select_ops` 10 passed |
| STS WebIdentity | `xl-s3/sts_web_identity.rs`、`sts.rs`、config | 默认关闭；本地 JWKS、RS256/ES256、iss/aud/kid/exp、RoleArn→sub/email 精确映射；匿名 Action 仅此一个；12 个离线测试 |
| CMK 删除/rewrap | `xl-store/kms.rs`、`store/cmk_rewrap.rs`、console | 删除先全引用扫描，有引用 409；quarantine→静默→复扫→purge，失败恢复；rewrap 逐版本重加密到当前钥，默认 dry-run，保留 ETag/version-id/元数据；本地节点口径 |
| M3 MPU 跨节点 | `xl-proto`、`xl-client`、`xl-store/store/mpu_route.rs` | Create 返回 `<base64url(node)>.<uuid>`；UploadPart/Complete/Abort/ListParts 由非属主经 mesh 流式转发（1 MiB 块，1h 超时）；属主不可达 503；旧裸 UUID/混合版本仍需原节点亲和 |
| OSSCTL2 + LC-0 | `raft/data.rs`、`cutover/export.rs`、`cutover.rs` | OSSCTL2 携带 feature_level 与 config_json，旧读者明确拒绝；snapshot install/cutover/force-recover 接受并单调提升级别；export 增 snapshot_format/wire/feature/source_build 校验，缺 provenance 旧导出警告接受、显式字段严格拒绝 |
| M6 token 远端 / M7 定向删除 | `replication.rs`、`repl_client.rs`、`xl-s3/service.rs` | 可选 `[[remotes]].site_token` 启用同源 id/版本删除/元数据；无 token 保持旧语义；SSE-C/KMS 永久死信；M7 按同源 id 定向删除、404 幂等。安全复审后**移除内容指纹自动删除**：未映射 legacy 副本无来源身份，保留待人工对账（防误删共享目标上另一来源的同内容副本） |
| TLS mesh/Kafka 热加载 | `xl-client`、`tls*.rs`、events | mesh gRPC 客户端与 Kafka TLS 客户端按成功 reload 代次重建，在途请求持旧 client；mesh 入站监听器仍需重启；Kafka SASL 仍启动期配置 |
| Batch 抢占/跨账户清单 | `jobs.rs`、`job_actions_api.rs` | 同一提交者的高优先级就绪任务让低优先级运行作业在动作边界 `preempted`，未执行任务回队、不重复副作用；优先级限定 `-100..=100`（越界 400），跨提交者不抢占（安全复审 M-2）；非管理员清单桶限同账户+读，管理员可跨账户，授权先于 HEAD/GET；控制台未展示 `preempted` |
| M8 leader 门控 | `raft/mod.rs`、`status.rs`、jobs/inventory/tier/lifecycle | Raft 模式仅 leader 跑单例任务；切换下一 tick 生效；未知/失去 leader fail-closed；运行状态仍节点本地（无 fencing/复制） |
| tier 清扫/预留/0600 | `tier.rs`、`tier_jobs.rs`、`local_tier.rs` | `tier-*.stage` 启动+周期清理（age ≥300s，默认 1h；活跃集合保护）；per-device 进程内预留防并发写爆；暂存文件创建即 0600 |
| 安全修复 | router/tls_server/inventory/replication/xl-client | AP 转发头要求直连对端命中 `OSS_AP_TRUSTED_PEER_CIDRS`（纯 HTTP listener 无 ConnectInfo，不能信任）；Inventory 运行前复核 owner 活跃与源读权限；M6 SSE-C/KMS 死信；M-1 移除不安全内容指纹删除；M-2 优先级钳制+同提交者抢占；L-1 WebIdentity 匿名端点全局令牌桶（20/s，burst 40，503 SlowDown）；L-2 mesh token 常数时间比较 |

### 12.2 验证

- 集成树 lib 测试：xl-store 282+1 ignored、xl-s3/oss-server 全绿；本轮一次全量跑出现
  `concurrent_puts_and_deletes_never_observe_torn_state` 一次 `AlreadyExists` 失败，单测连续 5 次
  通过（高负载并行 flake，已记录待复现）。
- 聚焦测试：bzip2 `select_ops` 10；CMK `tests_cmk_rewrap` 4 + kms 24 + console 2；WebIdentity
  12；MPU xl-store 6 + xl-client 7；OSSCTL2/LC-0 raft 相关 41/5/12/4；tier 26 集成用例；
  replication 19 + m8_batch 18/19；batch jobs_engine 10、m8_batch 19、console_api 30；m8gate
  raft 45、jobs_engine 11、m8_inventory 23。
- 安全复审：v1.15.0 面 0 Critical/0 High/1 Medium（可信代理直连伪造）+3 Low；Medium 与
  tier 0600/Inventory 运行期权限/M6 CMK 明文已修复。本轮新增面独立复审（sec3）
  0 Critical/0 High/2 Medium/4 Low：M-1 内容指纹误删→移除自动内容删除；M-2 跨租户抢占
  →优先级钳制+同提交者；L-1 匿名 WebIdentity 限速、L-2 mesh token 常数时间比较已修复；
  L-3 mesh 明文 SSE-C、L-4 旧裸 UUID MPU 粘节点为已记档残余。报告：
  `/tmp/security-review-backlog.md`。复审复核：M-1/M-2/L-2 CLOSED，L-1 CPU 放大 CLOSED
  但保留“单客户端可耗尽全局匿名桶”的 Low 残余；无新增 Critical/High/Medium。
- `cargo fmt --all --check`、5 crate clippy `--all-targets -- -D warnings`：0 warning。
- **S3 全量 e2e**：集成树 38 节 RC=0（真实进程 + aws-cli/rclone + redpanda；
  `OSS_MASTER_KEY_FILE` + `OSS_KMS_DIR`；脚本 md5 `16202c6d7910e354be5aaa74fd5d3b3f`）。
- **M9 集成树**：sync 78/0/1、stretch 59/0/1、recovery 133/0/1、rolling 65/0/4，全部 exit 0
  （rolling 使用 v1.13.1 旧二进制；SKIP 为 D6 预期项）。
- **M8 e2e（集成树）**：PASS，总耗时 4742s；Tier=ran（真实远端 S3 tier），M6/M7 复制 e2e
  全部通过（E2E_M8_REPL_SMOKE=1）。
- **E2E_LONG**：`E2E_LONG=1` 100×100MB 并发上传 + 中断重试，全部读回校验和一致
  （`长跑通过`）；全脚本 38 节继续跑完并打印“S3 验收全部通过”。
- 未通过项不作为“已验收”。

### 12.3 仍未完成（架构级）

- Level C 完整双向窗口（仅 LC-0 来源校验落地）。
- M3/M4 数据面滚动升级。
- 完整多租户账户体系与 AP 跨账号/VPC 端点身份。
- M8 多节点运行状态复制/fencing（仅 leader 门控）。
- M3 shrink / 单机→集群。
- 直接读 tier（缺分块哈希，范围读完整性无法保证）。
- M6 无 `site_token` 历史远端、M7 升级前已删除副本、marker 指纹缺失等兼容边界。

上述设计、MVP 切片、依赖与风险见
`docs/superpowers/specs/2026-10-24-architecture-backlog.md`。

## 13. 2026-10-24 v1.17.0：架构切片（§2–§5 继续执行）

**范围**：架构级非目标的最小可行切片、兼容边界、安全/运维残余与验证缺口。基于
`v1.16.0`，集成分支 `feat/post-1.16-backlog`；已合并并发布 **v1.17.0**。

### 13.1 已落地

| 项 | 落点 | 说明 |
|---|---|---|
| 多租户账户 MVP | `accounts`/`account_members` 迁移、accounts_api、Acl | 账户/成员/状态/桶数配额；回填保持派生 ARN；停用账户全链 fail-closed；账户写不参与 Raft（503） |
| AP 跨账户 / SourceVpce | xl-s3/ap.rs、access.rs、m8_ap | 账户根 principal 按成员关系解析；仅 AP 策略 Principal 可跨账户，最终取基线交集；SourceVpce 为显式证明（静态/可信头），非 PrivateLink 身份 |
| tier 分块哈希 + 直读 | xl-store/meta.rs、store/tier.rs、oss-server/tier_direct.rs、xl-s3/tier_direct.rs | `TierState.chunks` 增量元数据；`OSS_TIER_DIRECT_READ=1` 默认关，归档 GET Range 按块校验 SHA-256；无表/无 Range/漂移 fail-closed；跨进程 `.oss-tier-space` flock 预留 |
| M7 对账 / OSSCTL2 激活 | replication.rs、console/cli、raft/cutover/export.rs | 管理员列出/点名删除未映射 REPLICA（默认 dry-run，绝不内容匹配）；ExportFile 增加 activation_level，force-recover 保持 level 2 |
| WebIdentity 加固 | sts_web_identity.rs | JWKS 文件热加载（失败保留旧钥）、`require_email_verified`、按 IP 令牌桶（全局桶保留） |
| CMK 运维 | kms.rs、store/cmk_rewrap.rs、cli.rs | 离线 `cmk scan/rewrap/delete/trash`；桶默认加密纳入引用；`.cmk-trash` list/purge/restore；集群逐节点口径 |
| mesh 安全 | xl-client、xl-store、xl-s3 | SSE-C MPU 跨节点强制 mesh TLS；REPLICA 头需 site token 认证，否则按普通请求入复制 |
| Batch UI/边界 | web jobState/Jobs、job_actions | `preempted` 非终态展示/轮询；跨账户通知安全边界固化 |
| DP-0 数据面握手 | proto NodeHealth.data_wire_version、mesh_gate、store | 版本 2/legacy 1 严格相等；混版数据面 503 `peer_incompatible`；状态/metrics 暴露；探测预算 100ms 不拖累读快路径 |
| M8 运行态 fencing | raft/mod.rs、db.rs、jobs/inventory/tier/lifecycle | FencingEpoch(term,leader) 落入运行态表；旧 term 写拒绝/接管重盖章；仍无跨节点运行态复制 |

### 13.2 验证

- 库测试：xl-store 300（1 ignored）、xl-s3 176、oss-server 179；全绿。
- 聚焦集成批：accounts、m8_ap、permissions、console_api、replication、sync_gate、
  raft_cutover*、raft_commands、m8_batch、jobs_engine、m8_inventory、m8_tier*、
  lifecycle*、tls_*、observability、object_ops/replication_ops/select_ops/olap_ops、
  xl-client —— 全绿（修复两处旧夹具/旧库无账户表回落 + REPLICA 标签语义）。
- S3 全量 e2e：38 节全部通过，RC=0（含 Kafka 三阶段、双实例复制与 §20 标签/CopyObject）。
- M9：sync 78/0/1、stretch 59/0/1、recovery 133/0/1（v1.17.0 集成树复跑）。
- rolling：65/0/4（v1.17.0 集成树重跑；首跑 64/1/4 为高负载下 60s 轮询超时，非产品失败）。
- fmt/clippy：`cargo fmt --all --check` 与 clippy `-D warnings` 均为 0；Web `tsc`/`vite build`
  通过、Vitest 21/21、oxlint 0。Playwright 未跑：本机 9000 端口被 MinIO 占用，配置会复用该服务。
- M8 e2e：PASS 4633s（Tier=ran + M6/M7 smoke）。
- E2E_LONG：100×100MB 全部读回校验和一致（含中断重试）。受 `/home` 机械盘被其它会话重压影响，在 `/tmp` SSD 上完成。

### 13.3 仍未完成（架构级）

- Level C 完整双读写窗口（仅 LC-0/OSSCTL2 来源校验）。
- M3/M4 数据面完整滚动（仅 DP-0 握手；DP-1 读路径矩阵、DP-2 TLS 入站热轮换仍缺）。
- 完整多租户（账户表未随 Raft 复制；软字节配额；账户管理员 UI 不完整）。
- M8 多节点运行态复制/fencing 后的跨节点接管（仅本地 fencing epoch）。
- M3 shrink / 单机→集群。
- VPC endpoint 的真实 PrivateLink/ENI 身份（SourceVpce 仅为部署证明）。
- 直接读 tier 的已知限制：分块表无独立签名、ETag 口径不比较、跨进程预留只协调协作进程。
- Level B 滚动 e2e 的 4 个 v1.13.1 侧 SKIP、M6 无 token 历史远端、M7 升级前删除不回溯。

## 14. 2026-10-27 v1.20.0：Wave2 缺口修复

三条工作流（独立 worktree + 独立评审）已合并到本地 `main` 并发布 v1.20.0：
① DP-2 mesh 入站 TLS 热轮换 + 真实滚动 e2e；② 账户/成员写路径改走 Raft 命令；
③ 缩容 topology v4 + 离线 apply/verify/commit/rollback。

### 14.1 交付

| 项 | 范围 | 说明 |
|---|---|---|
| DP-2 mesh 入站 TLS 热轮换 | `crates/oss-server/src/tls.rs`、`crates/xl-client/src/lib.rs` | `Tls::mesh_server_watch()` + `serve_tls_watch`：每个入站连接取当前 rustls 配置（ALPN h2，`MESH_MAX_CONCURRENT_HANDSHAKES=1024`）；mTLS client-CA 对新连接生效，旧连接保持旧物料直到自然关闭；测试 `tls_mesh_inbound` 3 项 |
| 真实滚动 e2e | `tests/e2e_dproll.sh`（新增）、`tests/e2e_m9_rolling.sh` | 同版本 3 节点（A/B 数据 + C witness；PUT/GET/List V1/V2/MPU/版本删除；逐节点 TERM 重启；读不断、sync 写 503 fail-closed）→ 最终树 **116 PASS / 0 FAIL**；v1.13.1 旧二进制（`/tmp/oss-old-target/debug/oss-server`）→ **64 PASS / 0 FAIL / 4 文档化 SKIP** |
| 账户写路径改走 Raft | `raft/command.rs`、`raft/apply/accounts.rs`、`accounts_api.rs`、`accounts_support.rs`、`db.rs` | 7 个 level-3 `ControlCommand`（Create/Update/SetQuota/Delete/MemberAttach/MemberSetStatus/UserCreate），apply 按 username 自然键；写路由统一经 `ControlPlane`（standalone = Local apply 同事务）；follower 503 + leader hint；fresh Raft 原子写 `accounts_canonical_v1`；OSSCTL3 快照携带账户行 |
| 缩容 topology v4 + apply | `xl-store/src/store.rs`、`store/shrink_apply.rs`、`shrink_apply_util.rs`、`oss-server/src/cli.rs` | `.oss.sys/topo` v4（`set=/node=/slot=/epoch=/hash=`，set 0 也带 `set=` 使 v1.19 parser 拒启 = 回退防线）；**只在 `cluster-shrink commit` 写**，启动只校验 legacy 三元组不改写；apply（CRC32C、EC 重建、可续跑、receipt）/verify（full fsck）/commit（原子写、幂等、commit 前重查 namespace）/rollback（仅 commit 前） |

### 14.2 验证

- 全量：`cargo test -p xl-store -p xl-s3 -p oss-server -p xl-client --no-fail-fast` 所有 target
  通过；`raft_access` 在高负载下曾一次挂起（kill 后单跑 6/6 通过，判定负载 flake）。
- 聚焦：TLS 入站 3/3；shrink xl-store 347 passed / 1 ignored；账户/控制台/Raft 组全绿。
- `cargo clippy -p xl-store -p xl-s3 -p oss-server -p xl-client --all-targets -- -D warnings`
  RC=0；`cargo fmt --all --check` clean。
- e2e：`tests/e2e_dproll.sh` 116/0；`tests/e2e_m9_rolling.sh`（v1.13.1 旧二进制）64/0/4。
- 三条工作流各自独立评审；评审 P1/P2 已修复（见 §14.3）。

### 14.3 评审修复与仍未完成

已修复（评审 → 修复 commit）：

- P1 启动期 eager v2/v3→v4 升级伪造 commit、破坏 v1.19 回退 → `8cacf98` 改为只在 commit 写 v4，
  启动只校验不改写；`/api/v1/cluster/status` fingerprint 改读磁盘 topo。
- P2 commit 信任 receipt 而不重查 namespace → commit 前重跑 `namespace_digest()`。
- P2 账户 API 状态码/文案回归 → `352a9dd`、`91d2f71`（重复用户名 400、停用账户 400、缺失 404、
  最后一个管理员 400）。
- P2 Raft 模式下本地前置检查在滞后 follower 上误拒 → `401455a` 用 `!state.control.is_raft()`
  包裹（残余：`attach_member` 的 uid 查询仍可能在滞后副本上 404，重试可恢复）。
- P3 `raft_guard` 控制表直写扫描 allowlist 未含 `raft/apply/accounts.rs` → `f61abec`。

仍未完成（架构级，与 §13.3 对照）：

- Level C 完整双读写窗口与补丁线（v1.13.2 / CMK 重包 / 三二进制 e2e 均未合并发布）。
- M8 多节点运行态复制、schedule lease、worker takeover、consistent read、故障切换。
- 完整多租户：软字节配额、跨账户共享、AP 真实 PrivateLink 身份（`SourceVpce` 仍是部署证明）。
- 缩容 4→3 的 loopback e2e 已由 2026-10-28 的 `tests/e2e_m3_shrink.sh` 补上（86 PASS / 0 FAIL）；单机→集群仍为重新灌入。
- 直接读 tier 的已知限制不变（分块表无独立签名、ETag 口径、跨进程预留只协调协作进程）。
- Level B 滚动 e2e 的 4 个 v1.13.1 侧 SKIP、M6 无 site_token 历史远端、M7 升级前删除不回溯。

## 15. 2026-10-28 v1.20.1：post-v1.20.0 follow-ups

v1.20.1 发布后补齐的两项（v1.20.0 tag 保留，v1.20.1 为可用补丁）：

### 15.1 账户软字节配额（AC-2）

- 新增只读端点 `GET /accounts/{id}/usage`：先 `require_member`（非成员 403，不泄露账户
  存在性；管理员缺失账户 404），返回 `account_id/used_bytes/complete/soft_byte_quota/
  exceeded/bucket_count`。
- 新增配置路由 `PUT /accounts/{id}/soft-quota`（全局管理员）：body 必填
  `{"soft_byte_quota": <非负整数|null>}`，缺字段/负数/字符串 400；`null` = 取消。
  它与 `PUT /accounts/{id}/quota` 完全独立，设置软配额不会误清 `bucket_quota`，反之亦然。
  命令为独立 level-3 变体 `ControlCommand::AccountSetSoftQuota`——不能给
  `AccountSetQuota` 加字段，旧二进制 `deny_unknown_fields` 会 fail-stop。**混版窗口内
  使用该路由前必须先升级全集群到含此变体的二进制。**
- 用量 = 账户成员（非全局管理员）名下所有桶的**当前版本对象**字节和；逐桶
  `Store::list_objects`（`max_keys=1000` + continuation token），集群复用 mesh 合并列举。
  桶集合与 `Db::account_bucket_count_sql` 共用 `account_buckets_from` SQL 片段，避免口径漂移。
  集群丢 peer 时 `used_bytes` 只是下界：JSON `complete=false`，响应头回 `partial`（不冒充 ok）。
- 软配额**只告警不阻写**：配置了 quota 时响应头 `x-oss-account-soft-quota:
  exceeded|ok|partial`；配额 NULL 或负数（沿用 bucket_quota 的「负数 = 不限」）时无头。
  超限后 PUT 仍成功（测试锁定）。硬字节配额仍是明确非目标。
- 测试：`cargo test -p oss-server --test accounts` → 10 passed（含 >1000 键分页、超限后
  SigV4 PUT=200、独立路由不误清配额、403/404、NULL 配额）；`raft_accounts` 4 passed、
  `raft_command_contract` 3 passed（新增变体已进冻结 fixture）。

### 15.2 shrink 4→3 loopback e2e

- 新增 `tests/e2e_m3_shrink.sh`（442 行，真实进程）：4 节点×3 盘=12 槽 → 3 节点仍 12 槽
  （槽 9/10/11 → n1/n2/n3 drive 0），epoch 1→2；maintenance 重启 → plan → 退役 n4 拒绝
  apply/commit → 逐节点 apply+verify → commit 前 rollback 演练 → 逐节点 commit 写 topo v4 →
  3 节点新配置读回 EC/inline/双版本 → 旧配置被 v4 拒启 → n4 旧配置无法加入（写 503）。
- 最终运行：**97 PASS / 0 FAIL / 0 SKIP**（评审后加固：apply `reconstructed==0` 证明分片从
  退役节点真拷而非 EC 重建；新配置运行态再写一个新 EC 对象并断言槽 9/10/11 按新槽表落盘、
  n1–n3 读回一致；rollback 前 3 片断言；TERM 优雅退出、随机测试凭据、输出二进制 sha256）；
  无产品代码修复。
- 实测语义：commit 必须在各节点 serve 运行中执行（进程内 caps 仍为旧值，peers 健康校验通过）；
  commit 后旧配置 CLI 在 `Store::open` 被 v4 拒启，因此 CLI 层重跑 commit 不可达（in-process
  幂等由 store 级测试覆盖）。

### 15.3 v1.20.0 锁文件修复

- v1.20.0 版本 bump 的 `sed` 误把 `Cargo.lock` 中依赖 `either` 的 `1.19.0` 改成不存在的
  `1.20.0`（checksum 不变），导致从 tag 源码构建时 cargo 解析失败。
- 已恢复 `either = 1.19.0`，`cargo metadata --locked --offline` 通过；`cargo test` /
  `clippy` / `fmt` 在新锁上通过。
- v1.20.0 tag / GitHub Release 的源码归档仍带坏锁，不能直接构建；v1.20.1 已修复并发布，
  请勿使用 v1.20.0 源码包。

