# M8 S3 控制面服务 实施计划（工作令 v3.1）

> 设计文档 §M8 / §5.2 / §7.1 / §7.4 / §7.7；功能规格 §13–§17。
> 基线：`7c4dd73`（M1–M7 完成）。M8 与 M9 解耦，不依赖 openraft。
>
> **验收**（设计文档 §M8）：
> ① 访问点：`/ap/<name>/<key>` 路径可用；对象访问同时满足访问点策略 ∩ 桶策略 ∩ 身份策略。
> ② Object Lambda：经访问点 GET 返回变换后内容，Content-Length 与 ETag 与变换后字节一致。
> ③ Batch：1000 对象批量打标签；报告 CSV 含 taskId/resultCode/resultString；可重试失败任务。
> ④ Inventory：每日 manifest.json + data/*.csv.gz，AWS v1 列含加密/复制/锁字段。
> ⑤ 分层：生命周期转 GLACIER；HEAD 显示存储类；GET 返回 InvalidObjectState；RestoreObject
>    返回 `x-amz-restore: ongoing-request="true"`；完成后可读；到期回收恢复副本。
>
> 计划状态：**v3.1（2026-10-19）**。v2 按 `/tmp/plan-review-m8.md` 的 B1–B12、C1–C13 逐项修订；
> v3 按 `/tmp/m8-plan-review-scoped.md` 的 3 条阻断 + N1/N2/N3/N13 追加；v3.1 按
> `/tmp/m8-plan-review-v3.md` 的唯一阻断 R1 修订（TierGc 契约前移到 S4a）。计划作者只读源码、
> 只改本计划，不碰源码、不跑 cargo。
>
> v3 锚点：`/tmp/m8-plan-review-scoped.md`（NO-GO，阻断 3 条：Transition 配置入口、S2/S3
> 漏 S4a 依赖、B10 跨 crate partial）。
> v3.1 锚点：`/tmp/m8-plan-review-v3.md`（NO-GO，仅 R1：S3 隐式依赖 S4b 的 TierGc 契约）。

## v2 修订清单（逐项收口）

| 项 | 修订落点 |
|---|---|
| A5 | P0 契约与 S6 测试：`x-amz-tagging-count` 在 **GET 与 HEAD** 标签非空时都返回；TagSet 体只在 `GetObjectTagging`。 |
| A7 | P0 为 s3-gaps 所有的 **external prerequisite**，不计入 M8 人日/关键路径；M8 不自补，验收③ gate 在 s3-gaps 0a–2 之后。 |
| B11 | 闸门统一为七闸（xl-store 含 kill9、kill9 专项、oss-server lib+全部集成、xl-s3、xl-client+xl-proto、clippy、fmt）；feature/web/e2e 另列。 |
| B1 | S0 统一新增 `0003_m8.sql`（全部 M8 表/索引，含 `tier_restores` DDL），`db.rs` 增加 `SCHEMA_M8` 并在 `connect` 执行；旧库升级测试。 |
| B2 | S4a 定义 phase gate：`effective_parts` 在 Archived/Restoring 返回空；fsck/read/gc/version 读路径统一；归档对象 fsck 不报缺片，GC 可回收。 |
| B3 | `archive_version(..., storage_class)` 在同一 meta 提交写 `storage_class`；S4a 补 HEAD/List/Copy/MPU 断言。 |
| B4 | S1：CopyObject 的 AP 策略目标轴按 `s3:PutObject`；桶级 op 使用 AP 桶 ARN；标准桶 ARN 兼容；Deny 优先与测试。 |
| B5 | S1：`OssS3` 实现 `Clone`；router 先克隆出 inner，inner 共享 events/repl/inventory/restore/tier sink；补 AP Put 事件/复制测试。 |
| B6 | S4a（metadata）独立提交，S5 依赖 S4a 而非整个 S4；S4b（transition/restore）可与 S5 并行。 |
| B7 | 依赖按当前清单修正：根增 `reqwest` workspace；xl-s3 增 reqwest/urlencoding/运行时 base64/md-5、dev axum；oss-server 运行时 md-5/flate2/csv；wasmtime optional。 |
| B8 | `xl-store::error::Error` 增 `InvalidObjectState`，`convert.rs` 映射 403；读路径使用。 |
| B9 | S4b：新增 `RestoreSink` 注入，RestoreObject 只走 `mark_restore_pending → sink.enqueue_restore` 单一路径；禁止 xl-s3 直写 jobs DB。 |
| B10 | S0/S6：抽 `clear_bucket_m8(bucket)`；S3、console、site apply、cluster mesh 四条删桶路径统一调用并失效 `ApCache`。 |
| B12 | S4a：MPU Create 持久化 `storage_class`，Complete 读取并落版本；Create 拒绝归档类；补 MPU 测试。 |
| C1 | S1：AP 策略严格解析/保存校验/运行 fail-closed；未知字段、Condition/NotAction/NotResource、坏 Effect 拒绝；最终只写一条合并审计。 |
| C2 | S1：AP `check_access` 对原始后缀只解码一次得到 canonical key，拒绝 NUL、`.`/`..` 段、非法 UTF-8；策略/审计用 canonical，转发用 raw。 |
| C3 | S1：op 推导先判 `uploadId+partNumber`；`UploadPartCopy` 明确 501，不再误判 CopyObject。 |
| C4 | S2：`no_proxy()`、显式 gzip 按解压后计数、输入上限、IPv4-mapped/CGNAT/保留段；补代理/gzip 炸弹/超限测试。 |
| C5 | S2：补 `If-Match`/`If-Unmodified-Since` 412、`partNumber` 拒绝；透传 version-id/storage-class/restore/lock/SSE-C 响应头。 |
| C6 | S0：领取 SQL 用 `jobs.priority DESC`；可配 `OSS_JOB_WORKERS`；`deadline_at`/任务超时；每用户/全局 in-flight 配额。 |
| C7 | S4b：规则/配置确定 tier；MPU 源按 size/SHA-256 校验；sweep 去重；replication pending 跳过；restore 行先写、meta 后写、启动对账；reaper 以 Version 为准。 |
| C8 | S3：Batch 写入/删除复用 mutation hooks（复制入队、TierGcSink）；SSE-C 源 PermanentFailure；Batch 不发通知但必须发复制/清 tier。 |
| C9 | S5：run_key 改为调度槽（Daily/Weekly），另存 started_at；force-run API；可选列只保留可推导项，其余 400；写目标桶按配置所有者再校验；fileSchema 逗号串。 |
| C10 | S6：补 Batch 1000+1 失败+retry、Inventory force-run、Tier force-sweep、AP 完整链路与 Restore 状态机可执行锚。 |
| C11 | 安全门补 AP 审计/Principal 边界、OLAP 代理/gzip/输入/WASM 指针、Batch 各动作权限、tier 凭据信任面、作业配额。 |
| C12 | 工期改为 **5–6 周**；给 M8.1 可选裁剪清单；修正测试冲击与性能边界。 |
| C13 | 删除“已存在 ap 桶”无效声明（`validate_bucket` 要求 3..=63）；STS 限额改为 64 KiB；记录 `S3Route` 顺序、`OssS3::Clone`、`S3AccessContext` 私有、Restore 状态头 override。 |

## v3 修订清单（scoped 复核）

| 项 | 修订落点 |
|---|---|
| 阻断 1 | S4b 新增「生命周期配置入口（Transition 规则）」：实现 S3 `Put/GetBucketLifecycleConfiguration` + `DeleteBucketLifecycle`（`LifecycleStore` 注入）、扩展控制台 `LifecycleRuleIn`（`Row` 由 S0 扩）、明确 DB 列/读写/回读、M4 `sweep_once` 只删过期、S4b `sweep_transitions` 读取规则→任务；S6 e2e 先建规则再 force-sweep；补 API/作业/回读/错误矩阵测试。 |
| 阻断 2 | 切片表：S2 改为依赖 `S1 + S4a`，S3 补 `S4a`；顺序改为 `S0 → S4a →（S1 → S2）与（S4b/S5）`；`x-amz-restore` 交叉测试明确到 S4b；S2 验收锚补归档 403/storage-class 和 restore 头交叉点；S3 Copy 归档源测试依赖 S4a。 |
| 阻断 3 | S0 的 B10 改写为跨 crate `BucketCleanupSink { clear_bucket_m8, invalidate_ap_cache }`：xl-s3 定义 trait，oss-server `SqlBucketCleanup` 实现并注入 `OssS3`/`SqliteControlBackend`/console/site；路径③ tx 内 SQL、commit 后失效 cache；四路径调用者/注入点/测试文件逐一列出。 |
| N1 | inner builder **不调用** `set_auth(None)`/`set_access(None)`（s3s 0.17 setter 只收实现，builder 默认 None）；补 unsigned inner 集成测试。 |
| N2 | `ApRoute` 必须包装/委托现有 `StsRoute`：`is_match` 先 AP 后 STS，`check_access`/`call` 对应委托；只占一个 `set_route` 槽，保留 STS 回归。 |
| N3 | tier 远端 key 在 `TierBackend` 边界保持 raw；只在 `ReplClient` canonical path 编码一次；不在 oss-server 预编码，避免双重编码；补 raw/保留字符测试。 |
| N13 | 在 `job_actions.rs` 定义 `BatchMutationHooks { repl: Arc<dyn ReplicationSink>, tier_gc: Arc<dyn TierGcSink> }`，作为 Batch 写/删的唯一副作用入口；不另造第三套 sink。 |

## v3.1 修订清单（scoped 复核 R1）

| 项 | 修订落点 |
|---|---|
| R1 | 把 `TierGcSink` trait、`DeleteOutcome` 的 `TierRef` 扩展、`OssS3::with_tier_gc` 前移到 **S4a**（S2/S3 已依赖 S4a）；S4b 只保留 TierGc 实现/worker 与 Transition/Restore。S3 依赖保持 `S0/S2/P0 + S4a`，Delete/清 tier 随 S4a 可用，仅 Restore action 等 S4b；切片表、顺序、验收锚同步。S4a 估时 **2–3 d → 3 d**（多出契约、`DeleteOutcome` 调用点修复与测试）。 |

## 钉子（防蔓延）

- **M9 不做**：openraft / 控制面强一致 / sync 桶 / stretch EC / 站点故障语义 / 多节点控制面。
  M8 控制面仍是单节点 SQLite；站点/集群复制不感知 M8 新表。
- **不做已有功能的替代实现**：M6 `repl_outbox` 复制重试保留现状，不搬进新作业引擎；M4 生命周期
  **过期删除**仍走 `sweep_once`，新引擎只接 Transition、Batch、Restore、Inventory 四类。
- **P0 标签不在 M8 实施**：s3-gaps 拥有 P0；M8 只消费契约。P0 不计入 M8 人日和关键路径；
  验收③以 s3-gaps 切片 0a–2 合并为外部 gate。只有 s3-gaps 明确暂停并交接时才由单个提交补，
  且不得与 s3-gaps 双写 `meta.rs`/`service.rs`/`access.rs`/`policy.rs`。
- **访问点**：不做 `<name>-s3alias` 主机名、VPC/网络来源限制、跨账号、S3 Control API、匿名访问、
  多节点 AP 缓存一致性。AP 只支持路径式 `/ap/<name>/<key>`。
- **Object Lambda**：只做读变换（GET）；HEAD 直通；不做写变换、按请求头选变换器、试跑接口；
  WASM 默认编译关闭。
- **Batch**：不做优先级抢占（仅按 priority 取任务）、跨账号清单、AWS Lambda ARN、未来定时执行。
- **Inventory**：不做 Parquet、hive/ 布局、跨账号投递、增量清单；单次运行单数据文件。
- **分层**：v1 仅远端 S3 兼容 tier；本地冷盘池后置；不直写归档类；每条规则至多一个 Transition；
  存储类不随复制/站点传播。
- **兼容**：归档对象只在同版本新二进制内可读；启用 Transition 前需全量升级（旧二进制读到已回收
  分片会失败）。发布/推送/tag 只在用户明确口令时执行。

## 外部前置：P0 对象标签（s3-gaps 所有）

M8 只钉消费契约，**不实现**；契约以 `2026-10-19-s3-gaps-tagging-replace.md` v2 为准，不得出现第二套字段：

| 面 | 契约 |
|---|---|
| 元数据 | `Version.tags: Option<Vec<ObjectTag>>`（`ObjectTag { key, value }`，`#[serde(default)]`；保留客户端顺序；空 vec 写端归一 `None`；marker 恒 `None`）；`ObjectInfo.tags: Vec<ObjectTag>` |
| 协议 | `Put/Get/DeleteObjectTagging`；`PutObject` 的 `x-amz-tagging`；`CopyObject` 默认复制源版本标签，`x-amz-tagging-directive: REPLACE` 覆盖；**MPU 标签不在契约内** |
| 版本 | 无 `versionId` 取链尾；链尾 marker → 404 `NoSuchKey`；点名 marker → 405 `MethodNotAllowed`（`Error::DeleteMarker`）；未知版本 → 404 `NoSuchKey` |
| 限制 | ≤10 条；key 1..=128、value 0..=256（Unicode 标量）；字符集与重复键按 s3-gaps A7；非法 → 400 `InvalidTag` |
| 读回 | **GET/HEAD 标签非空时都回 `x-amz-tagging-count`**；TagSet 体只在 `GetObjectTagging` |
| 授权 | `GetObjectTagging → s3:GetObject`；`Put/DeleteObjectTagging → s3:PutObject`；沿用粗粒度动作，不新增词表 |
| 复制 | 标签不随 M6/M7 复制；Batch `ReplaceTags` 只改本端 |

**P0 gate**：S3 Batch 的 `ReplaceTags` 与验收③在 s3-gaps 切片 0a–2 合并后才可开跑；此前 S3 只做
不含标签的清单/报告骨架与单测。P0 若未合并，M8 不自行补第二套实现。

## 切片总表（M8 自身，不含 P0）

| # | 切片 | 交付 | 验收锚 | 依赖 | 估时 |
|---|---|---|---|---|---|
| S0 | 作业引擎与迁移 | `0003_m8.sql`（全部 M8 表/索引）+ `jobs`/`job_tasks` + 领取/重试/超时/配额/报告 + 控制台作业 API | ③⑤ 底座 | 无 | 4–5 d |
| S1 | 访问点 | AP 表/CRUD + `ApRoute` + 策略交集 + 内层转发 + 审计/规范化 | ① | 无（S0 的表由 S0 建） | 5–6 d |
| S2 | Object Lambda | HTTP 变换器（SSRF/输入/限额/条件头）+ WASM feature + GET 语义 | ② | `S1 + S4a`（存储类/InvalidObjectState）；`x-amz-restore` 交叉测试待 S4b | 4–5 d |
| S4a | 分层元数据前置 | `storage_class`/`tier` 字段、phase gate、MPU class、`InvalidObjectState`、`TierGcSink`/`DeleteOutcome::TierRef` | ⑤ 元数据；S2/S3 前置 | 无 | 3 d |
| S4b | Transition/Restore | 生命周期 Transition 配置/读取 + tier 后端、Transition 作业、RestoreSink、reaper/对账、TierGc 实现 | ⑤ | S0/S4a | 5–6 d |
| S5 | Inventory | 配置 op + 薄封装 + 日/周生成器 + force-run + manifest/CSV.gz | ④ | S4a（仅元数据） | 5–6 d |
| S3 | Batch Operations | 清单 + 五种动作 + 报告 + retry-failed + mutation hooks | ③ | `S0/S2/P0 + S4a`；Delete/清 tier 随 S4a，Restore 动作需 S4b | 4–5 d |
| S6 | 控制台/Web/E2E/文档 | 新 API 页面、对象页、指标、e2e、文档/图、定稿 | ①–⑤ | 全部 | 5–6 d |

顺序：`S0 → S4a →（S1 → S2）与（S4b/S5）`；S3 在 S2 + S4a（+P0 external gate）后启动，
Delete/清 tier 随 S4a，Restore 动作等 S4b；S6 最后。S4a 3 d 进入关键路径，**两人并行约 5–6 周**（设计口径 4–6 周；
按集成/e2e/返工取上限）；单人串行约 9–11 周。M8 不因 M9 延期。

**M8.1 可选裁剪（仅当必须压到 4 周）**：WASM；Batch `InvokeLambda`；Inventory version 模式与
`IsMultipartUploaded`/`IntelligentTieringAccessTier`、Weekly；Batch Copy/Delete/Restore 动作（只留
ReplaceTags）；Web Jobs/Tiers 页面（保留 API）；AP 桶级 op；多 tier/non-archive Transition。
验收①–⑤仍须 AP 对象 GET/List 策略、HTTP OLAP、ReplaceTags 1000、Daily Inventory 必含列、GLACIER/Restore。

## 执行模型

- 一个切片一个写入者，按文件划界；共享热点 `crates/xl-store/src/meta.rs` 由 S4a 单写，
  `service.rs` 的写入顺序为 **S4a → S1（`Clone`/字段）→ S4b（lifecycle/restore 方法）**；
  S4b 与 S1 并行时先做 `lifecycle.rs`/store/tests，`service.rs` 等 S1 合入后再改。
  `db.rs`/`0003_m8.sql` 由 S0 单写，后续切片不改编排。
  `access.rs`/`policy.rs` 由 S1 → S4b 串行：S1 先加 AP op 推导/授权，S4b 再追加
  lifecycle/restore action，S4b 不得抢在 S1 前改这两个文件。`router.rs`/`state.rs` 同理：
  S1 先落 outer/inner 与 `ApCache`，S4b 只追加 `with_lifecycle`/`lifecycle` 字段并按同一串行队列合入。
- 新增文件优先：`crates/xl-s3/src/{ap.rs, olap.rs, inventory.rs, restore.rs, lifecycle.rs,
  bucket_cleanup.rs}`、`crates/oss-server/src/{jobs.rs, job_actions.rs, access_points_api.rs,
  lifecycle_store.rs, bucket_cleanup.rs, tier.rs, inventory.rs, inventory_api.rs, tiers_api.rs}`、
  `crates/xl-store/src/store/tier.rs`。
- 编译冲击一次性处理：`put_object_stream_with_meta` 新参数（s3-gaps 已加 tags，本计划加
  `storage_class`）触及约 40 个调用点；`ObjectInfo`/`VersionEntry`/`DeleteOutcome` 字面量与 match 由
  编译器驱动补齐；不新增 builder。受影响既有测试必须回归：`classify`/`s3_action`、router
  fallback/STS、fsck/kill9、site/cluster 删除路径、复制入队。
- 黄金摘要：s3-gaps 已改到第九次；S4a 给 `Version`/`ObjectInfo`/`VersionEntry` 加字段会再次改变
  黄金摘要，由主会话在 S4a 合并后统一回填，切片作者不要各自更新。
- 性能锚：e2e 记录 1000 Batch（1000+1 键）耗时与 1001 键 Inventory 扫描耗时；AP `is_match` 对每个
  S3 请求都跑（保持 O(1) 前缀/命名判断）；Batch/Inventory 单任务全量扫描、tier 1 GiB 全缓冲均为
  在案瓶颈，不把“单 worker”留成隐性限制。
- 迁移：`0001` 重放、`ensure_columns` 补列、`0002_m7_site.sql`、`0003_m8.sql` 重放；索引一并
  `IF NOT EXISTS`。`0003` 不依赖 `ensure_columns` 新列。

---

## S0：作业引擎与迁移（`0003_m8.sql` + jobs/job_tasks）

### 迁移（单写者）
`crates/oss-server/migrations/0003_m8.sql` 一次性建全部 M8 表/索引（后续切片只使用、不改文件）：

```sql
CREATE TABLE IF NOT EXISTS job_tasks (
  job_id INTEGER NOT NULL, task_no INTEGER NOT NULL, action TEXT NOT NULL,
  payload_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
  attempt INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL DEFAULT 0,
  dedupe_key TEXT, deadline_at INTEGER,
  result_code TEXT, result_string TEXT, started_at INTEGER, finished_at INTEGER,
  PRIMARY KEY (job_id, task_no));
CREATE UNIQUE INDEX IF NOT EXISTS idx_job_tasks_dedupe
  ON job_tasks(dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_tasks_claim ON job_tasks(state, next_at);
CREATE INDEX IF NOT EXISTS idx_job_tasks_job_state ON job_tasks(job_id, state);

CREATE TABLE IF NOT EXISTS access_points (
  name TEXT PRIMARY KEY, bucket TEXT NOT NULL, owner_id INTEGER NOT NULL,
  policy_json TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS idx_access_points_bucket ON access_points(bucket);

CREATE TABLE IF NOT EXISTS olap_configs (
  access_point TEXT PRIMARY KEY REFERENCES access_points(name) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('http','wasm')),
  endpoint TEXT, module BLOB, module_sha256 TEXT,
  timeout_ms INTEGER NOT NULL DEFAULT 5000,
  max_response_bytes INTEGER NOT NULL DEFAULT 67108864,
  updated_at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS inventory_configs (
  bucket TEXT NOT NULL, id TEXT NOT NULL, owner_id INTEGER NOT NULL,
  config_json TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(bucket,id));
CREATE INDEX IF NOT EXISTS idx_inventory_configs_bucket ON inventory_configs(bucket);

CREATE TABLE IF NOT EXISTS inventory_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, bucket TEXT NOT NULL, config_id TEXT NOT NULL,
  run_key TEXT NOT NULL, started_at INTEGER NOT NULL, state TEXT NOT NULL, job_id INTEGER,
  manifest_key TEXT, error TEXT, created_at INTEGER NOT NULL,
  UNIQUE(bucket, config_id, run_key));
CREATE INDEX IF NOT EXISTS idx_inventory_runs_cfg ON inventory_runs(bucket, config_id);

CREATE TABLE IF NOT EXISTS tier_restores (
  bucket TEXT NOT NULL, key TEXT NOT NULL, version_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL DEFAULT 0, job_id INTEGER,
  state TEXT NOT NULL DEFAULT 'pending', created_at INTEGER NOT NULL,
  PRIMARY KEY(bucket, key, version_id));
CREATE INDEX IF NOT EXISTS idx_tier_restores_expiry ON tier_restores(state, expires_at);
```

`db.rs`：`const SCHEMA_M8: &str = include_str!("../migrations/0003_m8.sql");`；`connect()` 在
`ensure_columns()` 与 `SCHEMA_M7` 之后执行 `sqlx::raw_sql(SCHEMA_M8).execute(&pool)`。`jobs` 新列
（`spec_json/progress/total/succeeded/failed/report_bucket/report_prefix/priority/requested_by/
parent_job_id/started_at/finished_at`）与 `lifecycle_rules.rule_id/expiration_days/
transition_days/transition_class/transition_tier` 继续走 `ensure_columns`；补列后建
`CREATE UNIQUE INDEX IF NOT EXISTS idx_lifecycle_rule_id ON lifecycle_rules(bucket, rule_id)
WHERE rule_id IS NOT NULL`（重复 connect 幂等）。`lifecycle_rules` 旧
`expire_days` 列保持 NOT NULL：无 Expiration 的新规则写哨兵 `-1`，语义由 `expiration_days` 承担，
worker 只把 `expiration_days` 非 NULL 或旧 `expire_days>=0` 视为有过期动作。`LifecycleRuleRow`
同步扩为 `rule_id/prefix/expire_days/expiration_days/transition_days/transition_class/
transition_tier/enabled/updated_at`，`list_lifecycle_rules/set_lifecycle_rules` 读写全字段；
S4b 只做 `LifecycleRuleSpec`↔Row 映射，不再改 `db.rs`。扩 Row 会触及
`tests/lifecycle.rs` 的 Row 字面量，S0 同片机械补齐。`tier_restores` 无 FK，由 B10 的
`clear_bucket_m8` 清理。

**旧库升级测试**：临时库分别只执行 0001+0002（模拟旧库）与空库，调用 `Db::connect`；断言
`job_tasks/access_points/olap_configs/inventory_configs/inventory_runs/tier_restores` 与 jobs/
lifecycle 新列、`idx_lifecycle_rule_id` 存在；重复 connect 幂等。`0003` 任一语句失败必须启动
失败（不得吞错）。

### 桶删除统一清理（B10，v3 跨 crate）
- **契约**：`xl-s3` 新增 `bucket_cleanup.rs`：
  ```rust
  #[async_trait]
  pub trait BucketCleanupSink: Send + Sync {
      async fn clear_bucket_m8(&self, bucket: &str) -> Result<(), String>;
      fn invalidate_ap_cache(&self, bucket: &str);
  }
  ```
  `OssS3` 新增 `bucket_cleanup: Option<Arc<dyn BucketCleanupSink>>` 与
  `with_bucket_cleanup(Option<Arc<dyn BucketCleanupSink>>)`；`#[derive(Clone)]` 共享同一 `Arc`。
- **实现**：oss-server `SqlBucketCleanup { db: Arc<Db>, ap_cache: Arc<ApCache> }` 实现 trait；
  `clear_bucket_m8` 调 `Db`，`invalidate_ap_cache` 调 `ApCache::invalidate_bucket`。`Db` 提供：
  - `clear_bucket_m8(&self, bucket)`：pool 版，供 S3/console/cluster；
  - `clear_bucket_m8_tx(&mut SqliteConnection, bucket)`：同一 SQL 片段，供 site apply 事务内调用。
  删除 `access_points`（`olap_configs` 由 FK 级联；FK 未开则显式删）、`inventory_configs`、
  `inventory_runs`、`tier_restores`、`lifecycle_rules`；`jobs` 按审计保留。`tier_restores`/
  `lifecycle_rules` 无 FK，必须显式删。
- **四条路径的调用者/注入点**：
  ① `xl-s3` `service.rs::delete_bucket`：物理删除成功后（含 site-conflict 幂等成功分支），
     `bucket_cleanup.clear_bucket_m8` best-effort + 总是 `invalidate_ap_cache`；失败 warn，不改变
     已完成的物理删除结果。
  ② `oss-server` 控制台删桶：`AppState.bucket_cleanup` 调 `clear_bucket_m8` + `invalidate_ap_cache`；
     失败 warn/审计，不把已删物理桶补偿回来。
  ③ `site.rs::apply_bucket` 的 `e.op == "delete"`：在既有 `tx` 内调
     `Db::clear_bucket_m8_tx(tx, bucket)`；**cache 失效必须在 `tx.commit()` 成功后**由调用方
     `state.bucket_cleanup.invalidate_ap_cache(bucket)` 执行；commit 失败不失效 cache。
  ④ `cluster_control.rs::control_write` 的 `BucketRowSet { set: None }`：`SqliteControlBackend`
     新增 `cleanup: Option<Arc<dyn BucketCleanupSink>>` 与 `with_bucket_cleanup(...)`；
     `new(pool)` 保持默认 `None`（既有测试调用点不改）；删 `buckets` 行后调 `clear_bucket_m8` +
     `invalidate_ap_cache`（best-effort）。生产装配注入 `SqlBucketCleanup`，单测可注入 fake/None。
- **装配顺序**：`AppState` 先建 `Arc<ApCache>`，再建 `Arc<SqlBucketCleanup>` 并存为
  `AppState.bucket_cleanup: Arc<dyn BucketCleanupSink>`；`SqliteControlBackend`
  在 `Store::open` 前用 `with_bucket_cleanup(cleanup.clone())` 注入；router 构造 `OssS3` 时
  `with_bucket_cleanup(Some(cleanup.clone()))`；console/site 从 `AppState` 取同一实例。
- 测试文件 `crates/oss-server/tests/m8_bucket_cleanup.rs`：四条路径各删桶 → M8 表为空；重建同名桶后
  AP/Inventory 404（不继承旧策略/endpoint）；预热的旧 AP 缓存不再命中；路径③提交失败时 cache 不被
  失效；路径①/④清理失败只 warn 不影响删桶结果。

### 作业状态与调度
- 状态：保留 M4 `running|done|error`，新增 `queued|paused|cancelled`；任务 `queued|running|done|failed|
  cancelled`。设计 Completed/Failed 映射 `done/error`。
- 状态机：作业 `queued → running → done|error`；`running ⇄ paused`；`queued|running|paused → cancelled`；
  任务 `queued → running → done|failed|cancelled`。取消时未跑任务标 cancelled，在跑任务允许完成。
- 领取：`OSS_JOB_WORKERS`（默认 2，0 关闭）+ 每 worker 每 `OSS_JOBS_INTERVAL_SECS`（默认 2s）tick；
  `BEGIN IMMEDIATE` 内按
  `JOIN jobs j ON j.id=job_tasks.job_id WHERE t.state='queued' AND t.next_at<=? AND j.state='running'
   ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT ?` 领取并置 running；
  `deadline_at=now+超时`。SQLite 单进程多 worker，共享同一 WAL；多进程/多节点不支持（落档）。
- 超时/重试：失败 `attempt += 1`；未达 `OSS_JOB_MAX_ATTEMPTS`（默认 3）→ queued +
  `next_at=now+min(300,10*2^(attempt-1))+jitter`；达上限 → failed/PermanentFailure。worker 发现
  `running AND deadline_at<now` 按一次失败回收；tier 任务超时用 `OSS_TIER_TASK_TIMEOUT_SECS`
  （默认 3600），其余 `OSS_JOB_TASK_TIMEOUT_SECS`（默认 900）。
- 配额：Batch 创建时计 `requested_by` 的 queued/running/paused 作业数，
  `OSS_JOBS_MAX_INFLIGHT_PER_USER`（默认 5）与全局 `OSS_JOBS_MAX_INFLIGHT_GLOBAL`（默认 32）；
  超限控制台 429。Batch 任务数 `OSS_BATCH_MAX_TASKS`（默认 10000）。
- 优先级：`jobs.priority` 参与上述 ORDER BY；不做抢占。暂停只影响未领取任务。
- 恢复：启动把 `running` 且 `started_at < now-300` 或 deadline 过期任务按一次失败处理；作业级
  running 由任务聚合推进。单节点无租约。
- 报告：kind=batch 且 report_prefix 时，终态写 `<report_prefix>/<job_id>/results.csv`（csv crate）；
  写失败作业 `error` + 文案，不改任务终态。报告列固定见 S3。
- API：`GET /api/v1/jobs` 保持 admin-only；`GET /api/v1/jobs/{id}`、pause/resume/cancel/retry-failed
  对 admin 或 `requested_by` 开放；详情含 tasks 分页；retry-failed 建子作业（`parent_job_id`），只含
  失败任务，报告落新 job_id 子目录。`spec_json` 写入后不可变。
- 测试：状态机、双 worker 抢同一 task、退避/上限、超时回收、暂停/取消、优先级、配额、重启恢复、
  报告字节/列、M4 lifecycle 兼容。

## S1：访问点（模型 + CRUD + `/ap/<name>/<key>` + 策略交集）

### 数据与控制面
- 表见 S0；`access_points` 的 bucket 删除级联由 `clear_bucket_m8`（B10）统一处理。
- 控制台 API（`access_points_api.rs`）：`GET/POST /api/v1/access-points`、
  `GET/DELETE /api/v1/access-points/{name}`、`PUT /api/v1/access-points/{name}/policy`。
  创建/删除 admin 或桶 owner；policy admin 或 bucket owner；list admin 全量、owner 仅自己。
- 校验：name `[a-z0-9-]{3,63}` 且首尾非 `-`；目标桶存在；同名冲突；**不保留 `ap` 桶名**——
  `validate_bucket` 已要求 3..=63，`ap` 本来非法，无需额外保留/迁移声明。
- `AppState` 增 `ap_cache: Arc<ApCache>`（`RwLock<HashMap<String, ApEntry>>`，含 bucket/policy/
  transform 摘要）；启动加载、CRUD 后刷新、桶删除失效。
- 策略保存/加载：`parse_ap_policy` 严格校验（见下）；损坏策略不得当作“无策略”。

### 路由与鉴权（关键决策）
- **外层/内层**：`OssS3` 实现 `Clone`（字段均为 Arc/值；`Acl` 已 Clone）。router 先构造带全部 sink 的
  `outer_s3`，`let inner_s3 = outer_s3.clone()`；outer 用 `s3_builder.set_auth(SigV4Auth...)`、
  `set_access(acl)`、`set_route(ApRoute::new(ap, StsRoute::new(...)))`；inner 用
  `S3ServiceBuilder::new(inner_s3)`，**不调用** `set_auth`/`set_access`（s3s 0.17 的 setter 只接受
  实现，builder 默认就是 None；N1），不挂 route，不监听、不出 router。inner 必须共享
  events/repl/inventory/restore/tier 全部 sink（否则 AP Put 静默丢事件/复制，B5 测试钉住）。
- **route 组合（N2）**：s3s builder 只有一个 `set_route` 槽，不能同时挂 `ApRoute` 与既有 `StsRoute`。
  `ApRoute::new(ap, sts)` 包装 `StsRoute`：`is_match` 先判 `/ap/`，否则委托 STS；`check_access`/
  `call` 对应委托（AP 命中走 AP，否则走 STS）；STS 回归测试保留。不得为 AP 再建第二个 router。
- 外层 `custom_route_max_body_size = None`（流式 PUT；该值是全局放宽，所有 custom route 都需自守）；
  STS route 自行 **64 KiB** 读限（`sts.rs` 的 `MAX_BODY`，不是 1 MiB）。`is_match` 先判 AP 前缀，
  否则委托 STS。
- `S3Route` 顺序：`is_match` 在签名前，`check_access`/`call` 在签名后且 extensions 会带入
  `S3Request`。**不在 `is_match` 重写路径**；只在 `call` 重写，避免破坏 SigV4。
- `is_match`：形态匹配 `/ap/<name>` 或 `/ap/<name>/<raw key>`（name 满足命名规则，key 可空）；
  不查缓存、不读 body；把 `{name, raw_suffix}` 放进 `req.extensions`。未注册名字由 `check_access`
  返回 404 `NoSuchAccessPoint`，fail-closed，不回落标准路径。
- `check_access`（签名已由外层校验）：
  1. `req.credentials.access_key` → `Acl::user_of_key` 得 `AuthedUser`；匿名 403；
  2. 从 extensions 取 raw_suffix；**只解码一次**得 canonical key（`urlencoding::decode`）；拒绝
     非法百分号编码、非法 UTF-8、NUL、`.`/`..` 路径段；bucket 取 AP 缓存；key 空 = 桶级 op；
  3. 按 method/query/header 推导 op（表见下），未知形状 501；
  4. 调 `Acl::check_op(&self, user, op, bucket, canonical_key, copy_src, audit=false)`——从现有
     `Acl::check` 抽出的 `&self` 方法，**保留表三档、桶策略、Copy 源/目标双轴、会话融合与审计
     语义**；`check` 自身改为调用它（audit=true），既有 xl-s3 测试逐字节回归。`S3AccessContext`
     仍是 `pub(crate)`、不可从外部构造，`check_op` 必须定义在 `Acl` 上而不是自由函数；
  5. AP 策略（`PolicyRequest.ap = Some(ApContext)`）求值：显式 Deny → 拒；有匹配 Allow → 与步骤 4
     取交集；无匹配 → 拒；无 AP 策略 = 不额外限制。CopyObject 的 AP 目标轴用 `s3:PutObject`；
     源读由步骤 4 的标准桶/源桶策略判定，AP 策略不再放宽源；
  6. 最终写**一条**审计：`check_op` 的 detail 与 `ap:allow`/`ap:deny`/`ap:error` 合并；AP 拒绝
     必须落 deny 行。`AuthedUser` 插入 `S3Request.extensions`。
- **canonical key 一致性**：策略、审计、`check_op` 都用同一个解码结果；`call` 转发用原始 raw_suffix，
  内层 s3s 只再解码一次，二者落到同一 key。测试 `%2F`、`%252F`（双重编码）、`%2e%2e`、非 ASCII。
- **AP 策略 ARN**：`ApContext { name, region, account }`，account=`000000000000`，region 取
  `[site].region` 或 `us-east-1`。`resource_arns`：
  - 对象（key Some）：AP 对象 ARN `arn:aws:s3:<region>:<account>:accesspoint/<name>/object/<key>`
    与标准 `arn:aws:s3:::bucket/key` 兼容；
  - 桶级（key None）：AP 桶 ARN `arn:aws:s3:<region>:<account>:accesspoint/<name>` 与标准
    `arn:aws:s3:::bucket` 兼容。
  AP 策略显式 Deny 压过任何 Allow；测试 Copy 目标 `s3:PutObject`、List/Head `s3:ListBucket`、Deny 优先。
- **AP 策略严格解析（C1）**：`parse_ap_policy` 使用 `#[serde(deny_unknown_fields)]` 的 AP 专用
  `PolicyDoc`/`Statement`/`PrincipalField`（或等价显式校验），拒绝未知字段、`Condition`、
  `NotAction`、`NotResource`、非法 Effect（只 Allow/Deny）；Principal 仅 `*`/AK/`uid:`；
  保存 400；运行加载失败 → 500 `InternalError`（不当作无策略）。不新增 Condition/VPC 语义（落档）。
- **op 推导表**（先判更具体形状，未列形状 501）：
  - `PUT uploadId+partNumber` → UploadPart；若同时带 `x-amz-copy-source` → **501 UploadPartCopy**；
  - `PUT` + `x-amz-copy-source` 且无 uploadId → CopyObject；
  - GET 无子资源 → GetObject；HEAD 对象 → HeadObject；GET `list-type=2` → ListObjectsV2；
    GET `versions` → ListObjectVersions；PUT/GET/DELETE `tagging` → Put/Get/DeleteObjectTagging；
    POST `restore` → RestoreObject（S1 可推导但内层 501；S4b 启用，S1 不改 `classify`/
    `s3_action`）；POST `uploads` → CreateMultipartUpload；
    POST `uploadId` → CompleteMultipartUpload；DELETE `uploadId` → AbortMultipartUpload；
    GET/PUT `versioning` → Get/PutBucketVersioning；HEAD 桶路径 → HeadBucket；
    PUT/DELETE 对象无子资源 → Put/DeleteObject；`?delete` → 501。
- `call`：OLAP GET/HEAD 走 S2；其余把 URI 重写为 `/<bucket>/<raw_suffix>`（不加、不去百分号编码）；
  保留 query/headers/body；调 `inner.call(HttpRequest)`，转 `S3Response`。转发前删除
  `Authorization` 头与 query 的 `X-Amz-Signature/X-Amz-Credential/X-Amz-Date/X-Amz-Expires/
  X-Amz-SignedHeaders/X-Amz-Security-Token/X-Amz-Algorithm`；保留 `x-amz-*` 业务头。
  支持：Get/Head/Put/Delete Object、ListObjectsV2、ListObjectVersions、Create/Complete/Abort MPU、
  UploadPart、CopyObject、Get/Put/DeleteObjectTagging、RestoreObject、Get/PutBucketVersioning、
  HeadBucket。不支持（501）：ListBuckets、Create/DeleteBucket、DeleteObjects、UploadPartCopy、
  所有控制面/ACL op。内层无 `AuthedUser`，DeleteObjects 逐键授权无法保留，故排除。
- 事件/复制：inner 与 outer 共享 sink，AP Put/Copy/Delete 正常发事件/入复制队列；AP 不额外发事件。
- 安全：AP 只做额外限制，不放松桶/身份策略；内层不可从网络到达（单测断言 router 只有外层
  fallback）；缓存缺失 fail-closed；`AuthedUser` 只作诊断。

### 测试
- `xl-s3/src/ap.rs`：op 矩阵（含 UploadPartCopy 501）、URI 重写（`%2F`/双重编码/`..`/非 ASCII/
  桶级空 key）、鉴权头与签名 query 剥离、canonical key、缓存未命中。
- `oss-server/tests/m8_ap.rs`（in-process + SigV4 helper）：CRUD/权限；AP 策略 3×3（身份/桶/AP
  各自 Deny/Allow/无匹配）+ Deny 优先；Copy 目标 `s3:PutObject`；桶级 AP ARN；跨 AP 隔离；
  GET/HEAD/PUT/DELETE/ListV2/MPU/Copy 经 AP；Copy 源/目标双授权；DeleteObjects/UploadPartCopy 501；
  AP 删除后 404；桶删除级联；未签名/坏签名 403；匿名 403；AP Put 触发事件/复制各一次；坏策略 500；
  inner `S3Service::call` 无签名/无 access 直接 200（N1）；`ApRoute` 未命中时 STS 回归（N2）。
- 边界：主机别名/匿名/跨账号/多节点不做；AP 策略仅现有方言 + 严格子集；经 AP 的 aws-chunked 尾随
  checksum 不转发 trailing headers（预计算 `x-amz-checksum-*` 可用）；当前路径式，将来虚拟主机需
  重写 Host。

## S2：Object Lambda（HTTP 变换器 + WASM feature）

### 配置与依赖
- **依赖 S1 + S4a**：归档/过期 403 `InvalidObjectState`、`x-amz-storage-class` 来自 S4a；S4a
  未合并前 S2 只用 `STANDARD` fixture 测非归档路径。对象锁/SSE 头沿用 M4 `ObjectInfo.lock/
  encryption`（S2 只断言透传，不新增字段）。`x-amz-restore` 只做透传，与 Restore 的交叉联测等
  S4b（S2 验收锚不把 restore 作为前置）。
- `olap_configs` 表见 S0；API `GET/PUT /api/v1/access-points/{name}/transform`（owner/admin）。
  `kind=http` 必须有 endpoint；`kind=wasm` 仅 `olap-wasm` feature 时可用；模块 ≤16 MiB 存 BLOB，
  保存编译并记 SHA-256；每 AP 至多一个变换器；AP/桶删除级联。
- 根 `Cargo.toml` 增 `reqwest = { version = "0.13", default-features = false, features =
  ["rustls-tls","gzip","stream"] }`（workspace 依赖；oss-server 现有 dev-dep 改
  `reqwest.workspace = true` 并额外 `features = ["json"]`）。
- `xl-s3` 增 `reqwest.workspace = true`、`urlencoding = "2"`；把 `base64`、`md-5` 从
  dev-dependencies 移到正常依赖；dev 增 `axum.workspace = true`。
- `oss-server` 正常依赖增 `md-5.workspace = true`、`flate2.workspace = true`、`csv = "1"`；
  `wasmtime` 仅 `xl-s3` optional + `olap-wasm` feature（默认不编译；版本按 MSRV 1.89 固定并验证）。
- `oss-server` 透传 `olap-wasm` feature。

### GET 语义
- 仅 `GetObject`；AP 授权按原始对象；归档/过期 → 403 `InvalidObjectState`；Range/多段 →
  400 `InvalidRequest`；`partNumber` → 400。
- 条件头对原始对象求值：`If-None-Match`/`If-Modified-Since` → 304（不调变换器）；
  `If-Match`/`If-Unmodified-Since` 不满足 → 412；不转发条件头给变换器。
- 读原始字节（SSE-C 头解析；缺钥 400），输入上限 `OSS_OLAP_MAX_INPUT_BYTES`（默认 64 MiB），
  超限 400/413；整体缓冲输出（≤`max_response_bytes`，默认 64 MiB）。
- 返回：`Content-Length`=输出字节数、`ETag="md5(输出)"`、`Content-Type`=变换器响应头否则原始；
  `Last-Modified`、`x-amz-meta-*` 透传；**同时透传** `x-amz-version-id`、`x-amz-storage-class`、
  `x-amz-restore`、对象锁头、SSE-C 算法/key-MD5 响应头；不带 Range/Accept-Ranges。
- 变换器非 2xx/超时/超限/连接失败 → 500 `InternalError`（文案不泄露 endpoint/IP）。
- HEAD 直通原始元数据，不调变换器（Content-Length 原始；落档）；带变换器的写操作 405；
  无变换器 AP 正常转发。
- HTTP ABI：`POST <endpoint>`，body 原始字节，头 `x-oss-bucket`、`x-oss-version-id`、`x-oss-etag`、
  `x-oss-size`、`x-oss-key-b64: base64url(key)`；响应体变换后字节，`Content-Type` 可选；不转发
  调用方凭据/头。

### SSRF 与资源限额（C4）
- 仅 http/https；拒绝 userinfo 与非 http(s) scheme；端口可显式配置；保存配置时校验。
- 每次请求解析主机：拒绝 loopback、私网、链路本地、ULA、组播、未指定、CGNAT `100.64.0.0/10`、
  文档/保留段，以及 **IPv4-mapped IPv6**（`::ffff:127.0.0.1` 等）和 IPv6 私网；测试专用
  `OSS_OLAP_ALLOW_PRIVATE=1` 才放行。
- `ClientBuilder::no_proxy()`（环境代理不得绕过 DNS 钉扎）；`resolve` 把域名钉到已校验 IP；
  `redirect(Policy::none())`；TLS 校验默认开；连接超时 2s、总超时 `timeout_ms`（1–30s）、
  并发信号量 16。
- 显式 `.gzip(true)`（或拒绝 `Content-Encoding`），按**解压后**字节计数；超限即断；输入上限
  独立于响应上限；测试代理环境、gzip 炸弹、超大输入。
- WASM（默认关闭）：guest 导出 `memory/alloc/dealloc/transform`；host 必须校验指针/长度、分配失败、
  重叠/越界；fuel 默认 2 亿、epoch 超时、`StoreLimits` 内存 256 MiB、实例/表各 1、输出 ≤64 MiB、
  module ≤16 MiB；AP 缓存更新时失效旧 `Module`；无 WASI/import；失败 500，详情只进日志。

### 测试
- `xl-s3/tests/olap_ops.rs`：mock axum 变换器；请求头/体、输出长度/ETag、Content-Type、条件
  304/412、Range/partNumber 400、超时、超限、5xx、重定向/私网/IPv4-mapped/代理/gzip 炸弹；
  **经 `/ap/<name>/<key>` 的完整链路**（不只是 service 层）；响应头透传。S4a 落地后加
  `x-amz-storage-class` 与归档 403 断言；S4b 落地后加 `x-amz-restore` 头交叉断言（不阻塞 S2 收口）。
- `--features olap-wasm`：WAT 小模块、fuel、内存、坏 ABI、越界指针、超大模块、模块更新缓存失效；
  无 feature 时 wasm 配置 400。

## S3：Batch Operations（清单 → 任务 → 报告）

- **依赖 `S0/S2/P0 + S4a`**：Copy 源版本读、归档判定与 `PermanentFailure` 用 S4a 的
  `storage_class`/`InvalidObjectState`；Delete 的 `DeleteOutcome::TierRef`、`TierGcSink` 与
  `BatchMutationHooks.tier_gc` 也在 S4a（S4b 只提供真实 sink 实现），故 Delete/清 tier 在 S4b
  前可完整实现，用 S4a 的 archived fixture + fake sink 测。只有 Restore 动作需 S4b 的
  `RestoreSink`，在 S4b 前该 action 返回创建期 400（能力未启用），不静默降级。归档源 Copy 测试
  随 S4a 启用；P0 为 external gate。
- 控制台 `POST /api/v1/jobs`（kind=batch）body：`manifest_bucket/manifest_key/operation/
  target{bucket,prefix}/tags/restore/transform_access_point/report_bucket/report_prefix/priority`。
- **创建时授权（按创建者身份）**：清单桶 `GetObject`；Copy 源桶及源键 `GetObject`；目标桶按动作
  `PutObject`（Copy/ReplaceTags）或 `DeleteObject`（Delete）；Restore 需 `s3:RestoreObject`；
  InvokeLambda 需目标对象 `GetObject`；报告桶 `PutObject`；`transform_access_point` 需 owner/admin
  且 kind=http；`ReplaceTags` 标签创建时校验（≤10/字符合法），非法 400。超配额 429。
  **逐任务执行前再校验一次**（桶策略/身份可能已变），失败记 PermanentFailure 而不是整作业崩。
- 清单：CSV `bucket,key[,versionId]`（表头/CRLF/BOM 容忍）；行数 ≤10000、对象 ≤16 MiB、键
  `validate_key`；非法行创建失败 400；创建时快照到 `job_tasks`，之后改清单不影响。
- **BatchMutationHooks（N13）**：在 `job_actions.rs` 定义
  `struct BatchMutationHooks { repl: Arc<dyn ReplicationSink>, tier_gc: Arc<dyn TierGcSink> }`；
  Copy 成功后 `hooks.repl.on_write(...)`，Delete 返回的 `TierRef` 交 `hooks.tier_gc.delete(...)`。
  Batch 不新增第三套 sink、不直连 `ReplClient`/tier 后端；与 `OssS3` 注入同一组 `Arc`
  （`TierGcSink` 契约/字段来自 S4a，S4b 才有真实实现）。
- 动作（`job_actions.rs` 直接 store/Acl，副作用统一走上述 hooks，C8）：
  - CopyObject：源版本读；归档/SSE-C 源 → PermanentFailure；目标写 `target.bucket + target.prefix +
    原 key`，标签/元数据按 Copy 语义；写后调用 `ReplicationSink.on_write`；目标覆盖；
  - DeleteObject：逐键 `delete_object_versioned`（带 versionId 定点删）；`DeleteOutcome` 的 tier refs
    交给 `TierGcSink`（S4a 后可用）；
  - ReplaceTags/PutObjectTagging：整体替换为作业 tags（≤10）；对象不存在 → PermanentFailure；
  - RestoreObject：等价 S3 RestoreObject，走 S4b 的 RestoreSink；已在恢复 → 幂等成功；
  - InvokeLambda：对象 POST 给指定 OLAP HTTP 变换器，2xx 成功，输出丢弃；SSE-C/WASM 拒绝。
  Batch **不发桶通知事件**；但复制入队与 tier 远端删除必须执行（落档差异）。
- 报告：`taskId,bucket,key,versionId,resultCode,resultString,startTime,endTime`；resultCode
  `Succeeded|PermanentFailure`；ISO8601 UTC；写 `<report_prefix>/<job_id>/results.csv`；取消时未执行
  任务不进报告；重试成功记 Succeeded。
- 执行器由 `oss-server` 注入同一组 `Arc<dyn ReplicationSink>`/`Arc<dyn TierGcSink>`（与 `OssS3` 相同；
  `tier_gc` 契约随 S4a，真实实现随 S4b 注入），不复制 S3 service 的鉴权/事件逻辑；这是 Batch 与
  在线写路径一致性的唯一入口。
- 重试：任务指数退避；`retry-failed` 建子作业只含 PermanentFailure，报告独立目录；父报告不变。
- 测试：1000 行 ReplaceTags；五种动作；CSV 引号/CRLF/BOM/非法行/行数上限；versionId；无权限
  PermanentFailure 不中断；清单/报告桶权限；取消/暂停；retry-failed；复制桶 Copy 入队、归档 Delete
  清 tier（S4a archived fixture + fake sink）、SSE-C 拒绝；归档源 Copy → PermanentFailure（S4a 后
  启用）；报告列/时间格式；重复执行幂等。

## S4a：分层元数据前置（独立提交，S2/S3/S5 依赖）

- `Version` 新增（全部 Option，旧 meta 解码 None）：
  - `storage_class: Option<String>`（None=STANDARD）；
  - `tier: Option<TierState>`：`{ backend, key, size, etag, sha256, phase: Archived|Restoring|Restored,
    archived_at, restore_expires_at }`。
- **TierGc 契约（R1，前移到本片）**：
  - `xl-store::store::version` 增 `TierRef { backend: String, key: String }`（派生
    Debug/Clone/PartialEq）；`DeleteOutcome` 改为
    `Version { version_id, tier: Option<TierRef> }`、`Object { tiers: Vec<TierRef> }`（`Marker`/
    `NotFound` 不变）。现有 `delete_object_versioned*` 从 `Version.tier` 填 ref；非归档版本为
    `None`/空；S4b 的归档删除辅助复用同一 `TierRef`。`Version` 增字段、`Object` 由 unit 变
    struct，所有 match 调用点（xl-store/xl-s3/oss-server 测试）由编译器兜底补 `..`/`Object { .. }`，
    S4a 同片完成。
  - `xl-s3` 增 `TierGcSink` trait（`async fn delete(&self, refs: &[TierRef]) -> Result<(), String>`）
    与 `OssS3::with_tier_gc(Option<Arc<dyn TierGcSink>>)`；本片只落契约与注入，真正 tier DELETE
    的实现在 S4b（本片可注入 fake/None）。
  - 测试：`DeleteOutcome` 带/不带 tier ref；`Version` 点名删/`Object` 整键删；`Marker`/`NotFound`
    无 ref；fake `TierGcSink` 收到 refs、失败不影响删除结果；`OssS3` 未注入时删除仍成功。
- 可接受类：STANDARD/STANDARD_IA/ONEZONE_IA/INTELLIGENT_TIERING/GLACIER/GLACIER_IR/DEEP_ARCHIVE/
  REDUCED_REDUNDANCY；未知 400 `InvalidStorageClass`；归档类直写 PUT/MPU/Copy 目标一律 400。
- `ObjectInfo` 增 `storage_class/tier_phase/restore_expires_at`；`VersionEntry` 增 `storage_class`；
  `put_object_stream_with_meta` 增 `storage_class` 参数（tags 已由 P0 加）；调用点 Put/Copy/MPU
  Complete/Aether/复制入站；列表/HEAD/GET 回填，替换 `service.rs` 现有 STANDARD 硬编码。
- **phase 视图（B2）**：`Version::effective_parts()` 在 `tier.phase == Archived|Restoring` 返回空；
  `Restored` 返回本地新 part。`fsck` 旧 parts 分支先判 phase：Archived/Restoring 直接计 ok、不查
  本地分片；`read.rs` 的 `get_object_ext`/`get_object_range_ext` 和 `version.rs::read_version_data`
  在 `effective_parts` 之前做 phase gate；`gc` 空引用集自然不保护已归档 uuid；`version.rs` 版本读
  同一视图。归档对象 fsck 不报 `missing_shards`，GC 可回收，GET 403。
- **MPU storage class（B12）**：`MpuMeta` 增 `storage_class: Option<String>`；
  `create_multipart_upload_ext` 增参数，service Create 解析 `CreateMultipartUploadInput.storage_class`，
  校验合法类并拒绝归档类；Complete 从 `mpu.meta` 读取，传给
  `complete_multipart_upload_versioned`/`put_object_stream_with_meta`；`CompleteMultipartUploadInput`
  无 class 字段，Complete 不接受客户端 class。
- `Error::InvalidObjectState(String)` 加入 `xl-store::error`；`convert.rs` 映射
  `S3ErrorCode::InvalidObjectState`（403）；归档/过期 GET 与复制读使用。
- 测试：meta 往返/旧 meta；PUT/HEAD/GET/List/Copy/MPU 类断言；归档类直写拒绝；MPU Create/Complete
  class；`effective_parts` phase；fsck 归档干净；GC 回收；GET 403 错误码。

## S4b：存储分层与 Restore

### 生命周期配置入口（Transition 规则，阻断 1）
- **共享类型与校验（xl-s3）**：`lifecycle.rs` 增
  `LifecycleRuleSpec { rule_id: Option<String>, prefix: String, expiration_days: Option<u32>,
  transition_days: Option<u32>, transition_class: Option<String>, transition_tier: Option<String>,
  enabled: bool }` 与 `validate_rules(&mut Vec<LifecycleRuleSpec>, tier_ids: &[String])`
  （就地补全缺省 rule_id 与解析后的 transition_tier）。console 与 S3
  两个入口只用这一份校验；上限沿用 M4 的 100 条（AWS 1000 落档为边界）。约束：每条至少一个
  Expiration/Transition；`transition_days`/`transition_class` 同有同无；class ∈
  {STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, GLACIER_IR, DEEP_ARCHIVE}；
  days 0..=36500；prefix ≤1024 且无控制字符；rule_id ≤255 且桶内唯一；prefix 桶内唯一（沿用 M4
  主键）。**归档类**（GLACIER/GLACIER_IR/DEEP_ARCHIVE）的 tier 在写入时解析并持久化：显式
  `transition_tier` 必须存在；为空且恰一个 tier → 取该 id；0 个或多于 1 个 → 400。解析后存实际
  tier id（之后改 `[[tiers]]` 不静默改目标）。**非归档类**（STANDARD_IA/ONEZONE_IA/
  INTELLIGENT_TIERING）不需要 tier，`transition_tier` 归一为 NULL（显式给出也忽略）。
- **S3 API（xl-s3 service）**：实现 `put_bucket_lifecycle_configuration` /
  `get_bucket_lifecycle_configuration` / `delete_bucket_lifecycle`（覆盖 s3s 默认 501）。
  `OssS3` 增 `lifecycle: Option<Arc<dyn LifecycleStore>>` 与 `with_lifecycle`；未注入 → 501。
  `Acl::classify`/`policy::s3_action` 增 `s3:GetLifecycleConfiguration`（Get）与
  `s3:PutLifecycleConfiguration`（Put/Delete），桶级 ARN；否则新 op fail-closed。PUT 全量替换
  （AWS 语义），GET 无配置 → 404 `NoSuchLifecycleConfiguration`，DELETE 幂等 204。
  `LifecycleError` 映射：`NoSuchBucket→404 NoSuchBucket`、`NoSuchConfiguration→404
  NoSuchLifecycleConfiguration`、`Invalid→400 InvalidRequest`、`NotImplemented→501`、
  `Internal→500 InternalError`。
- **解析/回读**：`PutBucketLifecycleConfigurationInput.lifecycle_configuration.rules` 必须存在且
  1..=100；`Rule.id` 缺省生成 `rule-<序号>` 并持久化；`status` 仅 Enabled/Disabled ↔ `enabled`。
  Filter 仅接受空或 `filter.prefix`；legacy `rule.prefix` 等价映射，二者同时出现且不一致 → 400；
  `filter.tag`/`object_size_*`/`and` → 501 `NotImplemented`。Expiration 仅 `days`（映射
  `expiration_days`）；`date`/`expired_object_delete_marker` → 501。Transitions 仅 1 条且只认
  `days`+`storage_class`；`date`、多条、`noncurrent_version_*`、`abort_incomplete_multipart_upload`
  → 501。**先拒绝不支持的字段（501），再校验至少一个受支持动作（否则 400）**，避免把 501 误报成
  400。回读按 `(prefix, rule_id)` 稳定排序，字段与规范化输入一致（writer 只输出 filter.prefix）。
- **存储（S0 列 + 统一读写）**：`LifecycleStore` trait（xl-s3）：
  ```rust
  pub enum LifecycleError { NoSuchBucket, NoSuchConfiguration, Invalid(String),
      NotImplemented(String), Internal(String) }
  #[async_trait]
  pub trait LifecycleStore: Send + Sync {
      async fn get_rules(&self, bucket: &str)
          -> Result<Option<Vec<LifecycleRuleSpec>>, LifecycleError>;
      async fn put_rules(&self, bucket: &str, rules: &[LifecycleRuleSpec])
          -> Result<(), LifecycleError>;
      async fn delete_rules(&self, bucket: &str) -> Result<(), LifecycleError>;
  }
  ```
  `SqlLifecycleStore { db: Arc<Db>, tier_ids: Arc<Vec<String>> }`（tier 配置启动期固定；
  oss-server）实现：`put_rules` 先 clone 成 `Vec` 再调 `validate_rules`，写规范化后的规则
  （两个入口都无法绕过）；`db.bucket_exists` 决定 404；
  `Db::list_lifecycle_rules/set_lifecycle_rules`（S0 扩展）已是 `rule_id, prefix, expire_days
  (=expiration 或哨兵 -1), expiration_days, transition_days, transition_class, transition_tier,
  enabled, updated_at`；S4b 只做 Spec↔Row 映射；`set_lifecycle_rules` 由 S0 保持单事务
  `DELETE + INSERT`；
  旧 M4 行（`expiration_days IS NULL AND expire_days>=0`）读作 `expiration_days=expire_days`；
  `rule_id IS NULL` 的旧行回读时按 `(prefix)` 序号合成 `rule-<n>`（只读合成，不回写）。
  `AppState` 增 `lifecycle: Arc<dyn LifecycleStore>`；S3 outer/inner 与 console 共用同一实例注入；
  console `LifecycleRuleIn` 增 `rule_id/transition_days/transition_class/transition_tier`，且
  `expire_days` 改为可空（缺省=无过期），PUT/GET 走同一 `LifecycleStore`，不再直接调 `Db`。
- **规则读取者**：M4 `sweep_once` 只处理 `expiration_days` 非空（或旧 `expire_days>=0`）且 Enabled
  的规则，Transition-only 规则必须跳过；S4b `sweep_transitions` 遍历 `Db::list_bucket_rows`，逐桶
  `LifecycleStore::get_rules`，只取 `transition_days` 非空的 Enabled 规则，按 `transition_days`
  与对象 LastModified 判到期，用持久化的 class/tier 入队（任务 payload 固化 class/tier，执行期
  不回读规则；入队后改规则不影响在队任务），去重键同 C7。console GET 与 S3
  `GetBucketLifecycleConfiguration` 必须返回同一组规则。
- **错误矩阵**：桶不存在（三个 op）→ 404 NoSuchBucket；GET 无规则 → 404
  NoSuchLifecycleConfiguration；config 缺失/空 rules → 400 MalformedXML；>100 条 → 400
  InvalidRequest；rule_id/prefix 重复、既无 expiration 也无 transition、prefix/filter 冲突、
  归档类 tier 缺失或歧义 → 400 InvalidRequest；非法 class → 400 InvalidStorageClass；days 越界/ID 超长 →
  400 InvalidArgument；不支持的 Filter/Tag/Size/And、Expiration.date、Transition.date、多条
  Transition、noncurrent/abort/expired-marker → 501 NotImplemented。Disabled 规则可写可读但不被
  任何 sweep 选中；DELETE 后 GET → 404。
- **测试**：`xl-s3/tests/lifecycle_ops.rs`（PUT/GET/DELETE 往返、filter/expiry/transition 解析、
  归档类无 tier 400/非归档类无 tier 可写、disabled、错误矩阵、未注入 store 501、
  `classify`/`policy` 两个新 action）；`oss-server/tests/
  m8_lifecycle_api.rs`（console 与 S3 经同一 DB 互读、旧 M4 哨兵行回读、PUT 全量替换、
  rule→`sweep_transitions` 任务与去重、`sweep_once` 跳过 transition-only、删桶清规则；
  `tests/lifecycle.rs` Row 字面量由 S0 补齐）；e2e 建规则后 force-sweep。

### tier 后端与配置
- `[[tiers]]` 静态配置（模型同 `[[remotes]]`）：`id, endpoint, bucket, access_key, secret_key,
  region`；v1 仅远端 S3 兼容端点，https 启动期拒绝（TLS 后置）。
- `TierBackend` trait：`put(key, Bytes, etag)` / `get(key)` / `delete(key)` / `head(key)`；
  `S3TierBackend` 在 `ReplClient` 上扩展 `get_object`/`head_object`（SigV4+hyper，流式后续）。
  远端 key `<tier_id>/<bucket>/<version_id>/<percent-encoded key>`；PUT 用 **Content-MD5 与 SHA-256**
  双重校验，GET 校验 size/SHA-256；远端 ETag 存入 `tier.etag` 但**不与源 multipart ETag 比较**。
- Transition 规则的存储/配置/回读见上节；sweep `OSS_TIER_SWEEP_INTERVAL_SECS`（默认 3600）或
  admin `POST /api/v1/tiers/sweep` force；单次最多 10000 个对象。
- **tier key 编码（N3）**：Transition 作业从 store 取到的 key 保持 raw 传给 `TierBackend`；
  `S3TierBackend` 调 `ReplClient` 时由 canonical path 统一编码一次，oss-server 不得预编码。
  测试含空格、`+`、`%2F`、中文键的 PUT/GET 往返（远端 key 唯一且可读回）。
- **去重（C7）**：任务 `dedupe_key = tier:{bucket}:{key}:{version_id}:{class}`；
  `INSERT OR IGNORE` 唯一索引；终态清空 `dedupe_key`；`archive_version` 对已 Archived 同 class
  幂等成功（不同 class 400）。sweep 跳过 `replication_status ∈ {PENDING, FAILED}`，下轮重试。
- 归档任务：读对象（≤`OSS_TIER_MAX_BYTES` 1 GiB）→ tier PUT → SHA/大小校验 →
  `archive_version(..., storage_class)` → 发 `s3:LifecycleTransition`；失败退避，meta 失败保留本地。
- 非归档 Transition（STANDARD_IA 等）：只 `set_storage_class` 元数据，不搬数据、不建 tier 记录，
  同样发事件。
- `archive_version` 在同一 meta 提交写 `tier` 与 `storage_class`，phase=Archived；清
  `data_layout/inline/parts`，保留 size/etag/part 兼容位；提交后回收该版本本地分片（同 meta 其它
  版本引用同 uuid 则保留并 warn）；失败保持原状。
- **RestoreObject**：非归档 → 403 `InvalidObjectState`；已在 Restoring/Restored → 409
  `RestoreAlreadyInProgress`；`days` 1..=36500（缺省 1），tier 接受 Standard/Bulk/Expedited 但语义
  相同；`Acl::classify`/`policy::s3_action` 增 `RestoreObject → s3:RestoreObject`（对象 ARN，
  S1 不回改）；参数取 s3s XML body，body 缺失时再解析 `x-amz-restore-request: days=<n>,tier=<tier>`；
  两者都缺 400。`mark_restore_pending` → `RestoreSink.enqueue_restore(bucket,key,version,days)`
  （单一路径，B9）→ 202 + 状态头；sink 失败回滚 pending。**xl-s3 不直写 jobs DB**。
- **RestoreSink**：xl-s3 trait，oss-server 注入；实现在一个 DB 事务插 `tier_restores`
  `(expires_at=0, state='pending', job_id)` 并创建 kind=restore 单任务作业；返回 job_id。
- **恢复原子性（C7）**：job 侧 `prepare_restore` 写本地新数据（不可见）→ 更新 `tier_restores`
  `expires_at=now_complete+days*86400, state='restored'` → `commit_restore` 写 meta phase=Restored
  与 `Version.tier.restore_expires_at`。任一步崩溃：row state/expiry 与 Version 不一致由启动
  `reconcile_restore` 处理（pending/expired 重置并重排；Restoring+row restored 则重排或回滚）；
  meta 失败把 row 回 pending/expires=0。`restore_version` 拆成 prepare/commit 两阶段以满足此顺序。
- **reaper**：每 `OSS_TIER_REAPER_INTERVAL_SECS`（默认 300）读 `tier_restores`
  `state='restored' AND expires_at>0 AND expires_at<=now`，调 `expire_restore`（删本地恢复副本、
  phase=Archived、保留 tier）后删行；**以 Version 的 phase/expiry 为准**：restored 行但版本
  Restoring/Archived 只修正/删除孤儿行、不误删；启动对账有 row 的版本，并扫描
  `tier.phase=Restoring` 但无 row/无 job 的版本（`scan_tier_pending`，元数据树 O(N) 一次）→ 重置
  Archived 并重排恢复；读/HEAD 发现 `Version.tier.restore_expires_at<=now` 时即使行缺失也按过期
  处理并 best-effort 删行。读路径过期一律 403。
- **删除回收**：`DeleteOutcome`/`TierRef`/`TierGcSink`/`OssS3::with_tier_gc` 已在 S4a；本片实现
  `S3TierGc`（基于 `TierBackend` 的 DELETE）并注入 router outer/inner。`delete_archived` 返回
  `TierRef`；S3 删除、M4 生命周期删除、Batch Delete 都交给该 sink best-effort DELETE，失败只记
  日志（孤儿，无 GC）。`OssS3::with_restore` 仍在本片。
- 事件：白名单增 `s3:LifecycleTransition`、`s3:ObjectRestore:Post`、`s3:ObjectRestore:Completed`；
  Restore 受理发 Post，完成发 Completed；`RestoreObjectOutput` 无状态字段，必须用
  `S3Response.status/headers` 覆盖 202 + `x-amz-restore`（线上被 s3s 0.17 固定为 200，见「定稿」）；
  GET/HEAD 可用 s3s DTO 的 `restore` 字段
  或响应头输出，`tag_count` 属 P0 契约（GET/HEAD）。
- 测试：meta/旧 meta；类 PUT/HEAD/GET/List/Copy/MPU；归档类直写拒绝；mock tier Transition
  （字节/SHA、本地回收、重复 sweep 去重、replication pending 跳过）；MPU 源 Transition（不比较
  multipart ETag）；HEAD 三态；GET 403；Restore 202/409/403；完成后可读/ETag；过期 403+分片回收；
  tier 5xx 重试耗尽回 Archived；meta 失败保留本地；SSE 不转；版本删除远端；重启对账；崩溃注入；
  事件命名。

## S5：Inventory

- 配置：实现 s3s 的 `Put/Get/Delete/ListBucketInventoryConfiguration`（`OssS3` 默认 501 覆盖）；
  新 trait `xl_s3::inventory::InventoryStore`（`SqlInventoryStore` 在 oss-server 实现），
  `OssS3::with_inventory` 注入；`Acl::classify`/`policy::s3_action` 增
  `s3:Get/Put/DeleteInventoryConfiguration`（桶级 ARN），否则新 op 无法鉴权。表见 S0。
- 控制台薄封装：`GET /api/v1/buckets/{b}/inventory`、`PUT .../inventory`（upsert 单条）、
  `DELETE .../inventory/{id}`；owner/admin。校验 id `[A-Za-z0-9._-]{1,64}`、Daily/Weekly、CSV
  （Parquet 501）、目标桶存在且配置所有者有 PutObject、前缀/可选字段白名单、
  `included_object_versions`。每桶最多 `OSS_INVENTORY_MAX_CONFIGS`（默认 10）。
- 生成器（`inventory.rs`，worker `OSS_INVENTORY_INTERVAL_SECS` 默认 60，0 关闭，测试注入 now）：
  **调度槽（C9）**：Daily `run_key=YYYY-MM-DD`，Weekly `YYYY-Www`（ISO 周）；路径用槽
  `T00:00:00Z`；`inventory_runs.started_at` 存实际开始；唯一 `(bucket,config_id,run_key)` 去重；
  停机只补最近一次（落档）。创建 kind=inventory 单任务作业（内部翻页，progress=已扫描数）。
  **force-run**：`POST /api/v1/buckets/{b}/inventory/{id}/run`（owner/admin，测试/运维）创建
  `run_key = manual-<unix>` 的立即运行；e2e 用它，不靠等 Daily due。
- 扫描：前缀 filter；当前对象用 `list_objects`（`ObjectInfo` 已含 storage class/encryption/lock/
  replication）；含版本用 `list_object_versions`（`VersionEntry` 已含 storage_class）；不足字段按需
  head 单版本（成本落档）。页大小 1000，顺序稳定（key, versionId）。
- 输出（以配置所有者身份，运行时再校验目标桶 PutObject；失败 job error）：
  `<dest-prefix>/<source-bucket>/<config-id>/<run_key>/data/<uuid>.csv.gz` + `manifest.json`；
  CSV 必含 `Bucket,Key,VersionId,IsLatest,DeleteMarker,Size,LastModifiedDate,ETag,StorageClass`；
  可选仅支持可由现有元数据推导的 `ReplicationStatus,EncryptionStatus,ObjectLockRetainUntilDate,
  ObjectLockMode,ObjectLockLegalHoldStatus,ChecksumAlgorithm`；`IsMultipartUploaded` /
  `IntelligentTieringAccessTier` 明确 400（不静默空值，C9）。`fileSchema` 是**逗号分隔字符串**，
  不是 JSON 数组。日期 ISO8601 UTC 毫秒；gzip flate2；staging 临时文件边写边算 MD5/大小，再流式
  PUT；单文件 >5 GiB 拒绝并 error。
  manifest：`sourceBucket/destinationBucket/version:"2016-11-30"/creationTimestamp/fileFormat:"CSV"/
  fileSchema/files[{key,size,md5Checksum}]`。
- 测试：S3 配置 CRUD/权限；校验矩阵；Daily/Weekly due、同日去重、停机补一次；空桶；>1000 键
  翻页；版本桶 marker/IsLatest；加密/复制/锁列；prefix；gzip/CSV/schema/manifest MD5；force-run；
  目标桶撤权 → job error；禁用配置不跑；可选列拒绝；每桶上限。

## S6：控制台/Web/E2E/文档

- API 汇总：`access_points_api.rs`（S1 CRUD/策略 + S2 transform）、`jobs_api.rs`（S0 + Batch 创建）、
  `inventory_api.rs`（S5 + force-run）、`tiers_api.rs`（`GET /tiers` 健康、`POST /tiers/sweep` admin）；
  `console.rs` lifecycle 扩字段并改用 `LifecycleStore`（S4b）。
- 权限：AP/inventory owner/admin；`GET /jobs` admin-only；job 详情/控制 creator/admin；tiers admin。
  审计：AP CRUD/策略、job 创建/取消、transform 配置、force-run、force-sweep 写 `audit_log`。
- Web：`pages/AccessPoints.tsx`、`pages/Jobs.tsx`、`pages/Tiers.tsx`；`Objects.tsx` 增标签/
  存储类/Restore 状态；`Buckets.tsx` 增 Inventory 表单（频率/前缀/目标/可选字段/立即运行）。
- 指标：`oss_jobs{state}`、`oss_job_tasks{state}`、`oss_tier_objects{phase}`、
  `oss_inventory_last_run_timestamp`、`oss_ap_requests_total`（可选）。
- **E2E `tests/e2e_m8.sh`**：
  1. 播种 AK/桶 → 控制台建 AP；AP 路径/策略交集/OLAP 由 `cargo test --test m8_ap` 与
     `tests/olap_ops.rs` 覆盖（aws-cli 不支持 `/ap/` 自定义路径），e2e 只验控制面 CRUD 与标准路径
     未回归；
  2. **Batch 1000+1**：建 `batch-src` 循环 PutObject 1000 键（`xargs -P`）→ 上传 header+1000 行
     manifest → `POST /jobs` ReplaceTags → 轮询终态，断言 `total=1000,succeeded=1000,failed=0`；
     取报告断言固定表头、1000 行、`Succeeded`、taskId 唯一、时间可解析，抽 3 键 `GetObjectTagging`；
     再加 1 个不存在键（共 1001 行）→ 1000 Succeeded/1 PermanentFailure；创建该键 → `retry-failed`
     → 子 job `total=1,succeeded=1`、子报告 1 行、父报告不变；
  3. Inventory：配置 → `POST .../run` force-run → 拉 manifest/CSV.gz → gunzip、列与 fileSchema
     一致、files[0].md5/size 一致、对象数/版本数/marker 正确；
  4. 分层：mock tier（本地 axum S3 stub 或第二 oss 实例）→
     `aws s3api put-bucket-lifecycle-configuration` 建 Transition 规则（GLACIER、days=0）→
     `get-bucket-lifecycle-configuration` 断言回读 → `POST /tiers/sweep` force → HEAD 类 →
     GET 403 → Restore 受理（线上 200，见定稿）/完成可读 → `delete-bucket-lifecycle` 后 GET 404；到期回收由
     `m8_tier.rs` 用 past expiry 直接调 reaper；
  5. 回归：标准 S3、STS、M4 lifecycle、M6/M7 复制。
- 文档：README「M8 控制面」；设计 §M8 落地状态/偏差（M6 重试不迁移、HEAD 不变换、单节点、
  仅远端 tier、直写归档拒绝、Batch 不发通知、Inventory 可选列限制）；功能规格 §14–§16 注记；
  `docs/assets/m8-control-plane.svg`；API 表补实际路由。
- 定稿：七闸 + feature 闸 + web + e2e 全绿后写提交哈希、测试计数、e2e 输出、残余边界；发布待口令。

## 安全门（汇总）

1. **AP**：外层验签 → `Acl::check_op`（表三档+桶策略+会话+Copy 双轴）→ AP 策略严格交集；未知 op
   501；canonical key 单一解码；AP 策略坏/未知字段 fail-closed（保存 400/运行 500）；最终审计一条；
   inner 不挂 router、无鉴权但仅进程内；未签名/坏签名/匿名 403；AP 策略 Principal 仅 `*`/AK/uid，
   无 Condition/VPC。
2. **OLAP**：no_proxy + DNS 钉扎 + 私网/IPv4-mapped/CGNAT 拒绝 + 禁重定向 + 超时/并发/输入/输出
   上限 + gzip 解压后计数 + 不转发调用方凭据；错误不泄露内网信息。
3. **WASM**：默认不编译；无 WASI/import；指针/长度/alloc 失败/越界检查 + fuel/epoch/内存/实例/
   输出/模块上限；仅 owner/admin 上传；模块更新失效缓存。
4. **Batch/Inventory**：创建时与逐任务双重授权；清单行数/大小/键校验；报告/清单桶权限；
   `validate_key` 防穿越；Batch 复制/清 tier hooks 不遗漏；SSE-C 源拒绝；报告原样写。
5. **Tier**：静态凭据明文/http 与 `[[remotes]]` 同信任面（仅内网/管理员配置）；PUT Content-MD5+
   SHA-256，GET size/SHA；不做公网 SSRF 检查（内网冷存预期）。
6. **作业**：spec 不可变；控制限 creator/admin；配额；审计；错误文案不含凭据。
7. **生命周期配置**：S3 与 console 共用 `validate_rules`；S3 op 走 `s3:Get/PutLifecycleConfiguration`
   桶级授权，console owner/admin；PUT 全量替换单事务；Filter/date/多 Transition/noncurrent 一律 501；
   tier 在写入时解析并持久化，避免配置漂移。

## 已知边界（在案）

1. 控制面单节点：AP 缓存、作业引擎、Inventory 调度、tier reaper 均按单进程；多节点共享同一
   SQLite 不支持，强一致归 M9。
2. AP 仅路径前缀寻址；无 `<name>-s3alias`/匿名/跨账号/VPC；不支持 ListBuckets/Create/DeleteBucket/
   DeleteObjects/UploadPartCopy；桶级只支持 AP 目标桶 Head/List/Versioning 白名单。
3. 对象标签依赖 s3-gaps 契约；复制/站点不传标签；标签上限 10 条；P0 不属 M8。
4. OLAP 只读 GET；HEAD 直通；输出全缓冲；WASM 默认关闭；不做试跑；变换器 endpoint 由管理员信任。
5. Batch 无抢占/跨账号；InvokeLambda 仅 HTTP；动作不发桶通知但发复制/清 tier；清单/任务上限。
6. Inventory 全量扫描、单数据文件、不做 Parquet/hive/跨账号/增量；版本可选字段可能 N+1 head；
   `IsMultipartUploaded`/`IntelligentTieringAccessTier` 不支持。
7. 分层 v1 仅远端 S3 tier；全内存搬运（1 GiB）；SSE 不转；存储类不随复制；直写归档拒绝；
   每规则单目标类；Transition 单次 1 万对象。S3 生命周期配置 v1 上限 100 条、Filter 仅 prefix、
   Expiration/Transition 仅 days；date/tag/size/And/noncurrent/abort 一律 501；每桶每 prefix
   一条规则（沿用 M4 主键）。
8. 归档对象与同版本新二进制绑定；旧二进制读已回收分片会失败；启用 Transition 前全量升级。
9. tier 远端删除 best-effort，失败留孤儿；无 GC/对账；恢复到期精度 300s；restore 行/版本对账
   只处理有 row 的恢复。
10. 归档对象复制/站点复制不可用（GET InvalidObjectState），需先 Restore。
11. 事件白名单只增三项；`ObjectAccessed` 等未实现项仍 400。
12. 作业状态库内 `done/error` 映射 Completed/Failed；取消时未执行任务不进报告。
13. 报告/清单 CSV 不做公式转义；桶内对象由调用方审查；fileSchema 为逗号串。
14. 发布/推送/tag 待用户明确口令。

## 验收与闸门

### 七闸（每片收口）
① `cargo test -p xl-store`（lib，含 kill9 两例）  
② kill9 专项  
③ `cargo test -p oss-server`（lib + 全部集成二进制）  
④ `cargo test -p xl-s3`  
⑤ `cargo test -p xl-client -p xl-proto`  
⑥ `cargo clippy --workspace --all-targets -- -D warnings`  
⑦ `cargo fmt --all --check`

**另列**（不计入七闸）：feature 闸 `cargo test -p xl-s3 --features olap-wasm`（支持 wasmtime 的环境）；
Web `npm test` + `npm run build`；E2E `tests/e2e_m8.sh` 与既有 `e2e_s3.sh`/`e2e_sites.sh` 回归。
不自动触发 CI（ci.yml 仅 workflow_dispatch）。

### 验收锚
- ① `m8_ap.rs`：AP 策略矩阵 + canonical key + Copy 目标 `s3:PutObject` + 桶级 ARN + Deny 优先 +
  AP Put 事件/复制；完整路径 `olap_ops.rs`。
- ② `olap_ops.rs`：经 AP 的 mock HTTP 变换器，输出字节、Content-Length、ETag、Range 400、条件
  304/412、输入/gzip/代理/响应头；`x-amz-storage-class`/归档 403 在 S4a 后、`x-amz-restore` 在
  S4b 后补交叉断言。
- ③ `e2e_m8.sh`：1000 行 ReplaceTags + 1 失败 + retry-failed + 报告断言；P0 gate 之后执行；
  归档源 Copy PermanentFailure、Delete/清 tier 随 S4a（fake sink），Restore 动作随 S4b。
- ④ Inventory：Rust 注入 now 的 Daily/Weekly due/去重 + e2e force-run 的 manifest/CSV/MD5/列断言。
- ⑤ `m8_tier.rs` + `m8_lifecycle_api.rs` + e2e force-sweep：归档/HEAD 类/GET 403/Restore
  202/409/完成/过期 403/回收；tier 5xx 重试耗尽、meta 失败、重启对账；S4a 的
  `DeleteOutcome` tier refs + `TierGcSink` 注入/未注入空删；S3 `Put/GetBucketLifecycleConfiguration`
  + `DeleteBucketLifecycle` 往返/错误矩阵/rule→sweep 任务去重，e2e 先建规则再 force-sweep。

## 缺口收口与偏差（2026-10-20）

S6 之外的控制面缺口以 Task 7a/7b/7c 收口（不在原 S0–S6 编号内，均已在 8116a12 之前落地）：
- 7a（`2650b86`/`33291cc`/`6189b95`）：MPU create 的 `x-amz-tagging` 持久化；控制台对象标签
  路由；审计过滤（from/to/uid/op/bucket/key-prefix）；加密/KMS 只读状态。
- 7b（`312c94a`/`1ad61c2`）：控制台通知 CRUD、死信列表/重投、一次性 Select 代理、IAM 预设模板。
- 7c（`1850276`/`8cc69c4`/`67bebd1`）：桶默认加密（SSE-S3/SSE-KMS）落在 PUT/MPU/Copy；
  复制出站 `max_bytes_per_sec` 令牌桶限速。
- 未做（记录非目标）：`AssumeRoleWithWebIdentity`、SSE-KMS 显式 CMK/多钥轮换（用户
  2026-10-17 决定另行处理）；M9 同步强一致不在 M8。
- 其余设计偏差见设计文档 §M8「与设计的偏差（记档）」。

## 定稿（2026-10-20）

- 代码 HEAD：`8116a12`（S0–S6 + 7a/7b/7c；Task 6b 未改 Rust/Web 源码）。
- 文档/e2e 提交：`a3579ee`（README、设计/功能规格、两张 SVG、`tests/e2e_m8.sh`）+
  后续小修（e2e 注明 1 worker 前置、Inventory 诊断输出）；本定稿与报告另提交。
- 七闸（base `8116a12`）：① `xl-store` 220 passed / 1 ignored；② kill9 专项 1 passed；
  ③ `oss-server` 203 passed；④ `xl-s3` 181 passed / **1 failed**；⑤ `xl-client`+`xl-proto`
  5 passed；⑥ clippy `--workspace --all-targets -D warnings` exit 0；⑦ fmt `--check` exit 0。
- ④ 唯一失败 `olap_ops::checksum_headers_removed_after_transform` 为测试夹具过期：内联
  `SCHEMA` 缺 `default_encryption` 列（生产库由 0003 迁移补列），非产品缺陷；feature 闸
  `-p xl-s3 --features olap-wasm` 同因 185 passed / 1 failed。受限「不改 Rust」未修夹具。
- Web：`npm test` 18 passed（3 files）；`npm run build` 通过；Playwright `m8-console` 9 passed。
- E2E：`tests/e2e_m8.sh` 全跑 **PASS**，耗时 4679s（约 78 分钟；1 worker × 1s 间隔、
  1000 与 1000+1 两个 Batch 作业）；步骤 4 Tier=ran，步骤 6 默认 SKIP。
  另单独执行 M6 `tests/e2e_replication.sh` 与 M7 `tests/e2e_sites.sh`：均 PASS（exit 0）。
- 评审/修复：Task 6b 自审（按任务约束未另设评审子代理）；e2e 多轮运行中发现并修正 5 处
  脚本/文档缺陷——Inventory MD5 base64、`Transitions` 列表、版本/删除标记清理、
  Restore 线上 200、Batch 拆为 1000 与 1001 两段。
- 残余边界：RestoreObject 线上 200（s3s 0.17 生成代码丢弃 handler 的 202）；SQLite 单写者
  `finish_task` 读→写升级可能锁冲突，任务延迟到 deadline 回收（建议 1 worker 或改造事务）；
  失败重试退避含 `now%1000` 秒抖动；控制面单节点；tier 仅远端 http。
- 发布：仅本地提交；推送/tag/Release 待用户明确口令。
