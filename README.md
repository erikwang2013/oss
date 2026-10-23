![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

# oss（吞吞）—— 自研分布式对象存储系统

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研对象存储（MinIO 的等价实现）。目标形态是分布式 + 纠删码；
当前完成 **M1（单盘垂直切片）+ M2a（可用性切片：Multipart Upload、Range GET、
`x-amz-meta-*` 元数据、流式 PUT、孤儿回收）+ M2b（存储切片：单机多盘 + 纠删码 8+4、
quorum 写、拔盘容忍与读时修、`fsck` 巡检、kill -9 断电一致性用例）+ M3（分布式切片：
节点间 gRPC mesh、静态槽位拓扑、跨节点 quorum 读写、节点故障容忍与读修复、quorum 租约锁、
混沌 e2e）+ M4（运维与 IAM：版本控制、生命周期、桶策略、审计、heal/scrub、rebalance、
多 set）+ M5（安全与锁定：SSE 三模式、对象锁、STS 临时凭据、SK 加密存）+ M6（事件/复制/分层
与恢复）+ M7（多站点异步复制）+ M8（访问点、Object Lambda、Batch、Inventory、控制台）+ M9
（同步强一致：sync 桶、stretch 2×6、3-voter Raft 控制面）**——`aws-cli`/`rclone`
的大文件默认路径（≥8 MiB 自动 multipart）与断点习惯直接可用，浏览器能分块上传，控制台能看见。

**v1.14.0（2026-10-23）能力边界清零**：补齐 S3 协议细节（ListObjects v1、UploadPartCopy、
ListMultipartUploads/partNumber、多区间/HEAD Range、Suspended、noncurrent、匿名、retention、
ObjectAccessed）、全链路 TLS/SASL、M8 边界（AP 别名/DeleteObjects/SourceIp、Inventory
Parquet/Hive、tier 本地盘/GC、生命周期全 Filter、Batch WASM）、M9 Level B 控制面滚动升级；
并完成安全复审（0 Critical，2 High 已修复）。详见 `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md`。

- **五个 crate 单向依赖 `oss-server → xl-s3 / xl-client → xl-store`（`xl-client → xl-proto`）**：
  `xl-store` 是不依赖任何框架的存储内核（提交协议与 `xl.meta` 格式按最终设计落盘）；`xl-s3` 把
  s3s 的 S3 trait 实现到内核上；`xl-proto` 是节点间 gRPC 协议消息与 service（prost + tonic-build
  生成）；`xl-client` 是 mesh 的 tonic 实现（`TonicMesh` 客户端 + 服务端 handler）；`oss-server`
  用 e-cat 组装一个 axum Router，同时承载控制台 API、aetherupload 分块上传与 S3 服务。
- **两条客户端入口**：`aws-cli` / `rclone` 走 S3 面（SigV4 认证）；
  浏览器控制台（React 19 + antd，M1b）走 `/api/v1` 与 aetherupload 自带脚本（分块 / 断线续传 / 秒传）。
- **技术栈**：Rust 2024 · ecat 7.0 · s3s 0.17 · aetherupload-rust 3.0.0 · axum 0.8 · sqlx / SQLite · serde + rmp-serde。

## 当前进度

已实现（M1）：单盘直存内核（无纠删码）· S3 桶/对象 CRUD · ListObjectsV2 · CopyObject ·
批量删除 · SigV4 认证 · aetherupload 分块上传与原生 ingest · 控制台 API。
已实现（M2a）：Multipart Upload（五操作）· Range GET（206/416）· `x-amz-meta-*` 元数据往返 ·
流式 PutObject（不再整份读内存）· 启动自检与 `clean` 按龄续回收（trash/tmp/未引用版本目录）。
已实现（M2b）：多盘存储（`data_dirs`，每盘一份命名空间镜像）· EC 8+4 分片（quorum 写
≥ data+1、拔盘重建、读时修）· 小对象内联（< 128 KiB 进 `xl.meta`）· meta 两阶段多盘提交
（最新副本胜）· `fsck [--repair]` 巡检 · 桶级互斥与 flock 跨进程锁 · 目录 fsync 与
kill -9 断电用例（SIGABRT 四注入点）。
已实现（权限体系）：子账户与桶级**读/写/删**三档授权（管理员在控制台「子账户」页创建、
管理、授权）· 三面统一执行（S3 的 AK/SK、控制台 JWT、aetherupload 上传认证）· 子账户可建
自有桶（桶主全权）· 上传/下载/删除均按权限门。
已实现（M3）：分布式切片——节点间 gRPC mesh（`xl-proto`/`xl-client`）· 静态扁平拓扑（全局
槽位表 + 拓扑指纹）· 跨节点 quorum 读写（提交 quorum + 补偿、读覆盖率快路径）· 节点故障容忍
（kill -9 / SIGSTOP 混沌 e2e 实证）· 读修复（read-repair + fsck 集群化差集补齐）· 节点间
quorum 租约锁（节点域 Q_n）· `/health/cluster` 集群健康端点。
已实现（M4 + 扩展包 C1-C5）：版本控制（版本链 / delete marker / 回滚 / 点名版本读删）·
生命周期到期自动删（jobs 进度可见）· 桶策略（显式 Deny 优先；CopyObject 源/目标双轴判定；
批量删逐键授权）· 审计日志与查询（C5 保留期自动清理）· `heal`/`scrub` 独立命令 · rebalance
追加式扩容（C4 同盘 hardlink 快路径）· 多 set 横向扩容（桶钉 set；C2 控制台 sets 视图；
C3 建桶缺省自动指派负载最小的 set）· 登录限速（C1：账号 10 分钟 5 败锁定 + 全局 60 秒 120 败
暂拒，429 + Retry-After）。
已实现（M5 安全与锁定）：集群控制面行一致性广播（切片 0）· 主密钥密钥库（切片 1，单钥文件 +
多钥 KMS 目录）· 加密数据面（切片 2，先加密后 EC 的 DARE 式块对齐 AES-256-GCM）· S3 面
SSE-S3 / SSE-KMS / SSE-C 全链（切片 3）· 对象锁 GOVERNANCE / COMPLIANCE / Legal Hold +
桶级默认保留规则（切片 4）· STS 临时凭据 AssumeRole/GetSessionToken + 会话策略交集（切片 5）·
`access_keys.secret_key` 信封加密 + 存量透明迁移（切片 6）。**WebIdentity 经成本评估裁剪延后**
（需出站 TLS 客户端新依赖栈；验收 ①–⑤ 不涉；记档于 M5 计划——用户知悉项）。
已实现（M6 数据流）：事件通知（S3 通知配置 + Kafka，至少一次 outbox）· S3 Select（CSV/JSON、
SQL 白名单、ScanRange、Progress/Stats 帧）· 桶复制（规则 API、9 写入口入队、SigV4 出站、
PENDING→COMPLETED/FAILED、停机积压追平）。
已实现（M7 多站点异步复制）：单节点站点对（`[site]` 唯一开关 + 管理 API 配对/ping/status）·
控制面 changelog（SQLite 触发器捕获、LWW `(ts,priority,origin,seq)` + 本地写门控 + apply 抑制）·
配对快照（单事务、入钟、只本地胜者）· 对象复制复用 `repl_outbox`（逐 peer 入队、发送前
`GET ?versioning` 预检、metadata 透传、REPLICA 防环）· 停机积压追平、死信/游标/状态面。
已实现（M8 完整，2026-10-20）：**S3 缺口（P0）** 对象标签 `Put/Get/DeleteObjectTagging` +
PUT `x-amz-tagging` + GET/HEAD `x-amz-tagging-count`，CopyObject `metadata/tagging-directive`
两轴（REPLACE 独立、同键仅 metadata REPLACE 放行）· **访问点与 Object Lambda（S1/S2）**
（路径式 `/ap/<name>/<key>`、严格 AP 策略与身份/桶策略交集、canonical key、签名剥离与内层
转发、AP CRUD/策略 API；HTTP 变换器默认、WASM 变换器 `olap-wasm` feature 门控）· **作业引擎
与 Batch Operations（S0/S3）**（`jobs/job_tasks`、`BEGIN IMMEDIATE` 领取、优先级、指数退避、
超时回收、失败重试子作业、CSV 报告、1000+1 清单与 retry-failed）· **Inventory（S5）**
（Daily/Weekly 调度、force-run、manifest.json + `data/*.csv.gz` / `data/*.parquet`、AWS v1
必含列 + 可选列、Hive `symlink.txt` 清单）· **存储类/分层与 Restore（S4a/S4b）**（`storage_class`/
`TierState`、Archived/Restoring phase gate → 403 `InvalidObjectState`、远端 S3 与本地冷盘池
tier 后端、生命周期 Transition、RestoreObject 异步受理、到期回收、`TierGcSink` 删除契约、
管理员显式触发的远端 GC/对账（默认 dry-run））· **S6 控制台页面与 `tests/e2e_m8.sh`**。
**不在 M8**（记档非目标）：跨站点强一致（M9）· `AssumeRoleWithWebIdentity` · SSE-KMS 显式
CMK/多钥轮换 · AP 跨账号/VPC 端点（AP 匿名已支持，见「访问点」）· Inventory 跨账号投递 ·
Batch 抢占/跨账号清单。
已实现（M9 S0–S7，2026-10-21）：模式矩阵/配置守卫与能力开关 · sync 写门（统一
origin version id、staging 明文 tee 0600、写 intent compensate-only）· sync remote-first 删除
（内部 token 路由、删桶 quiesce、删除 intent）· stretch 2×6 / EC 8+4 数据面 · Raft 控制面
（控制写单命令 + apply 后缓存失效、`/site/changelog` 409、启动模式断言、真实
`/api/v1/raft/status`）· 3-voter 成员变更/leader 单跳转发 · 双边配对与 B′ 替换 · witness
独立运行时 · 11 表逻辑快照/安装 · M7→Raft cutover · `raft-export/raft-force-recover` ·
`sync-reconcile [--repair]` · console Raft 集群卡片。`RAFT_READY`/`SYNC_READY`/
`STRETCH_READY` 均为 true。**S8/S9 验收**：stretch 模式下的配对/加 witness 组合、
灾备与 11 表并发 diff 均已由 `tests/e2e_m9.sh`（`M9_STRETCH=1`）/
`tests/e2e_m9_recovery.sh` 覆盖（五个 e2e 与验收证据见下）。

## 架构

![系统架构](docs/assets/architecture.svg)

M6 数据流（事件通知 / S3 Select / 桶复制）：

![M6 数据流：事件 / Select / 复制](docs/assets/m6-dataflow.svg)

M7 多站点异步复制（控制面 changelog + 对象 outbox）：

![M7 多站点复制](docs/assets/m7-sites.svg)

M8 控制面（访问点与 OLAP / 作业与 Batch / Inventory / 分层与 Restore / 控制台页面，S0–S6）：

![M8 控制面](docs/assets/m8-control-plane.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` 编解码（版本链 / 加密记录 / 锁记录 / 标签 / storage_class / tier）、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock、密钥库（`kms.rs`）与块对齐信封加密（`crypto.rs`） |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`）、SSE 与对象锁头解析/响应装配；M8 增访问点路由（`ap.rs`）、Object Lambda（`olap.rs`）、Restore/生命周期（`restore.rs`/`lifecycle.rs`）、TierGc/删桶清理契约 |
| `crates/xl-proto` | 节点间 gRPC 协议：`oss.proto` 消息与 service 定义（prost + tonic-build 全量生成） |
| `crates/xl-client` | mesh 的 tonic 实现：`TonicMesh` 客户端（懒连接 / 超时 / token 拦截器 + traceparent 注入）与服务端 handler（令牌校验，只操作本节点本地槽） |
| `crates/oss-server` | ecat + axum 组装：控制台 API（JWT）、aetherupload 接线、S3 fallback、运维 CLI；M8 增作业引擎（`jobs.rs`）、Batch（`job_actions*.rs`）、访问点/OLAP（`access_points_api.rs`）、Inventory（`inventory*.rs`）、分层/恢复（`tier*.rs`）与 0003 迁移 |
| `web` | 控制台前端（M1b）：React 19 + antd + Vite，Playwright e2e；M8 增访问点/作业/分层/清单/通知/审计/设置页面 |
| `tests/e2e_s3.sh` | aws-cli / rclone 端到端验收脚本（20 个主步骤 / 38 节，含 M6 Select/Kafka/复制；+ `E2E_LONG=1` 门控长跑） |
| `tests/e2e_m8.sh` | M8 控制面验收（AP CRUD、Batch 1000+1、Inventory 强制运行、Transition/Restore 可选段、S3/STS/M4 回归） |
| `tests/e2e_replication.sh` | 桶复制双实例验收（源/目标真进程：同 ETag、PENDING→COMPLETED、停机积压→追平、marker） |
| `tests/e2e_sites.sh` | 多站点复制双实例验收（双向配对/快照、建桶立刻写、断连积压、marker、并发同 key、重启不回退） |
| `tests/e2e_m9.sh` | M9 强一致验收（A/B 数据站点 + witness C 三进程：配对/3-voter、控制面并发写后 **11 张复制表**三端字段级一致、sync remote-first + 断连 503、MPU/Copy/版本删、kill/重启追平）；v1.13.1 发布时 78 PASS / 0 FAIL / 1 SKIP（本批未重跑；stretch 由专用脚本覆盖） |
| `tests/e2e_m9_rolling.sh` | M9 Level B 控制面滚动升级演练（逐台换二进制、wire/feature 握手、level 2 门控、回滚；需 `OSS_OLD_BIN`=v1.13.1，未提供时 SKIP；本批未实跑） |
| `tests/e2e_m9_recovery.sh` | M9 灾备验收（真进程 + CLI：M7 export/cutover 字段级冲突与 overwrite、force-recover 后 `bootstrap=false` 重启；witness 运行时/export/force-recover；B′ 替换 + `sync-reconcile --repair` 门） |
| `tests/e2e_stretch.sh` | M9 S4 stretch 2×6 真进程验收（两站点 × 6 盘、EC 8+4、单站故障 fail-closed、恢复、配置负例；不依赖 witness C） |

磁盘布局速记（M2b 多盘）：每个盘各持一份**完整命名空间**——`<盘>/<桶>/<展平键>/` 下是
`xl.meta`（该盘副本）与版本目录 `<uuid>/part.1.<shard>`（落到本盘的那些分片；单盘形态即全部分片）。
**提交永远是「数据先到位 → meta 翻转」**：单盘 = 同盘 rename；多盘 = 各盘 meta 两阶段提交
（全盘 `xl.meta.new` 准备 ≥ quorum 才逐盘转正；读取**最新副本**胜）。小对象（< 128 KiB）内联进
`xl.meta`、不落数据文件。启用加密（M5）后数据文件存的是**密文分片**（先加密后 EC），
包裹的 DEK 与 `key_id` 记在 `xl.meta` 内（元数据本身不含密钥明文）。临时区在每盘
`.oss.sys/{tmp, trash, locks}`，盘身份标识 `drive-id` 防插错盘。
控制面元数据（用户/密钥/桶/上传记录/秒传索引）在 SQLite。

## 请求周期

![请求周期](docs/assets/request-flow.svg)

两条入口（S3 直传、浏览器分块上传）在 xl-store 的**提交协议**处汇合：
暂存 → 同盘 rename 数据就位 → `xl.meta` 覆盖即提交；提交之前的一切对客户端不可见。
EC 写/读（含区间读）已分块流式：内存峰值 ≈ 块缓冲（MiB 级），与对象大小无关
（96 MiB 对象实测 VmHWM 107 MiB）。

## 对象生命周期

![对象生命周期](docs/assets/object-lifecycle.svg)

读路径只信 `xl.meta`：无有效 meta 的目录永不被服务；崩溃只会留下不可见的残骸
（孤儿版本目录 / trash 残留 / 未完成的 multipart 暂存区）。服务启动时自检清 `tmp`/`trash`，
`oss-server clean <days>` 按 mtime 回收其余残骸（M2a 已落地）。

## 跑起来

```bash
# 1. 起服务（首次启动会自动建数据目录、暂存目录、桶目录与管理员）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server

# 2. 登录拿 token（M1 还没有注册界面，账号来自上面的环境变量）
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"changeme"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

# 3. 建一对访问密钥给 S3 客户端用（显式指定，与下面验收段的 AK/SK 一致；重复执行会提示「access key 已存在」，幂等）
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"access_key":"AKIA_TEST_ACCESS_KEY","secret_key":"SK_TEST_SECRET_KEY_0001"}'
```

> 注意：暂存盘下的 `_header/` 与各分组目录都由 `aether::build_runtime` 在启动时创建
> （分组目录经 `aetherupload::console::list_groups`，`_header/` 单独补建——list_groups
> 只在自建根目录时顺带建它）；若预处理报笼统的上传错误，先检查暂存盘目录是否被清理。

> 已知限制：
> - `/aetherupload/*` 四条路由**需 JWT**：登录后带 cookie `oss_token` 或 `Authorization: Bearer <token>` 访问
>   （前端在登录成功时种 cookie，M1b）；缺/坏/过期一律 401。彻底的角色/策略模型仍随部署形态（M4+）。
> - ListObjects v1 已实现（marker / NextMarker / delimiter / max-keys）；rclone 仍可用 `LIST_VERSION=2`。
> - 单次 PUT / 单个 part 仍受 `max_put_bytes`（默认 256 MiB）上限；multipart 大对象整体不受该值约束。
> - Multipart：`UploadPartCopy`、`ListMultipartUploads`（含集群 `ListUploadsLocal`）、GET/HEAD `partNumber`
>   已实现；`partNumber` 与 `Range` 同时出现 → 400 `InvalidRequest`。
> - `partNumber` 依赖 Complete 时落盘的 `part_sizes`：MPU Complete 落盘；站点复制、raft
>   同 id 重推与 sync 写门随 `x-oss-part-sizes` 透传；tier 归档/恢复原地保留（restore 后仍可分段读）。
>   异构远端复制、非 MPU 单段 PUT 无该表时退化为旧语义（仅 part 1 = 整对象；>1 → 416）。
> - Range：HEAD 支持单区间（200 + 区间 `Content-Length`/`Content-Range`，越界 416）；GET 多区间
>   （`bytes=0-9,20-29`）不返回 `multipart/byteranges`——s3s 解析前剥掉 Range → 200 整对象。
>   Range 未计入 SigV4 SignedHeaders 时（普通未签名请求、常规预签名）照旧剥离；计入
>   SignedHeaders 时保留到验签之后，由 s3s 返回确定性 400 `InvalidArgument`（不再 403
>   `SignatureDoesNotMatch`）。
> - 对象标签：`Put/Get/DeleteObjectTagging`、PUT 的 `x-amz-tagging`、GET/HEAD 的
>   `x-amz-tagging-count` 已落地；≤10 条、键 ≤128 / 值 ≤256 Unicode 标量，非法 → 400 `InvalidTag`。
>   标签按版本存储；复制摄入（`REPLICA`）不落标签。MPU 标签已落地，控制台可编辑对象标签。
> - CopyObject 指令：`x-amz-metadata-directive` / `x-amz-tagging-directive`（COPY/REPLACE，两轴独立）
>   已落地；REPLACE 未提供即清空；同桶同键仅 metadata REPLACE 解锁；COPY 时携带 tagging 头被忽略。
> - **控制台页面**：M5 加密/KMS 只读状态、M6 事件页与 Select 工作台、M4 桶策略编辑与审计查询、
>   对象标签编辑、桶默认加密、通知死信重投均已落地（M8 S6/7a/7b）；详见「控制台页面（S6）」节。
> - Multipart **part 级** `x-amz-checksum-*` 不校验（静默忽略；组合语义仍未实现，记档）；单段 PUT 的
>   Content-MD5 / `x-amz-checksum-*` 已在提交点前校验（不符 → `BadDigest`，对象不落盘）。
> - 控制台「上传记录」是登记历史：对象或暂存文件被删后，记录行仍在（按设计，作审计线索）。
> - `oss.toml` 默认 `listen = "0.0.0.0:9000"` 与占位 `jwt_secret` 仅限本机开发；对外部署前必须换成回环绑定与随机密钥
>   （占位密钥可被用来伪造控制台与 `/aetherupload/*` 的 token；与 0.0.0.0 绑定合用即 LAN 风险面）。
> - **多盘（M2b）运维边界**：
>   - `data_dirs` 配 2..11 个盘会在启动时**报错**（EC 8+4 的冗余需要 ≥ 12；单盘可运行但无冗余，启动 warn）。
>     `[ec] enabled=false` 恢复 M1 旧形态（只允许单盘）。
>   - **首启前确认各盘挂载点已挂载**：首启时「未挂载的挂载点目录」与全新盘不可区分，会被当作新盘初始化。
>     运行时掉线的盘会被自动判为不在册（写按 quorum 绕行、删对象 fail-closed、插回自动恢复），
>     但它**必须曾在挂载状态下启动过**（盘上有 `.oss.sys` 与 `drive-id`）。
>   - 掉线盘在场时 **删对象/删桶返回 500**（防「插回即复活」）；这是有意为之的 fail-closed，插回后重试即可。
>   - 读 meta 副本择新自 M4 起按 `(epoch, mod_time)` 排秩——写端 epoch 单调 +1，NTP 回拨不再产生「旧副本凭大钟胜出」；
>     epoch 随 `xl.meta` 前向兼容（老文件按 0 参与排秩）。
>   - 目录 fsync 的断电零丢失保证按 ext4/XFS 假设；其它文件系统上目录 fsync 失败仅 warn，需自行复核。
> - 长跑验收（rclone 并发 100×100 MB + 中断重试，设计验收 #3）在 `tests/e2e_s3.sh` 的
>   `E2E_LONG=1` 门控段（默认跳过，人工触发；`E2E_LONG_N`/`E2E_LONG_MB` 调规模）。
> - **权限体系边界**：
>   - 授权模型 = 每（子账户 × 桶）三档（读/写/删）+ 桶主全权 + 管理员全量；**无行 = 无权限**。
>     S3 面（AK/SK）、控制台、aetherupload 上传三个入口执行同一份判定。
>   - 子账户可在控制台/ S3 建**自有桶**（自动全权）；删桶限桶主或管理员；历史遗留桶
>     （无归属记录）仅管理员可见。
>   - 管理员可**停用/启用**账号（Users 页）：停用即全链路失效（登录 401、既有 JWT 下一请求 401、
>     AK 403、上传面 401；无主动吊销名单，靠下一请求时校验）。任何人可在「访问密钥」页**自助改密**
>     （验旧口令）；管理员也可为他人重置口令。
>   - 控制台上传只能进「`[[groups]]` 分组映射的目标桶」——子账户要往自有桶传，走 S3 面。
>   - 撤回授权对**在途分块上传**不追溯（preprocess 已放行的会传完）；新请求即时生效。
>   - 建子账户 API 固定为非管理员（防提权）；口令至少 8 位；删除用户级联清其密钥与授权。
>   - 口令存 **Argon2id**（m=19456, t=2, p=1）；历史 sha256 口令在**成功登录时透明升级**（无需重置）。
>   - **登录限速（C1）**：账号维度 10 分钟窗口失败 ≥5 次 → 锁 10 分钟；全局维度 60 秒窗口
>     失败 ≥120 次（跨用户名，防随机用户名洪水刷 Argon2）→ 全体暂拒（429 + `Retry-After`）。
>     单进程内存态：每节点独立、重启清零（限速非持久配额）。只护登录口；已认证的改密不在此列。
>   - **桶策略只在 S3 面生效**：控制台与 aetherupload 上传面仍走三档 permissions。
>   - **删桶连带清空该桶策略与生命周期规则**（S3 面与控制面删桶都清）——同名重建不继承，防旧规则误删。
> - **多 set（M4）边界**：
>   - 多 set 下**数据面每 op +1 次本地 DB 查询**（桶 → set；本地 miss 才扇出对端；单 set 零新增）。
>     解析 fail-closed：传输错 / set 越界 / 对端答复不一致一律上抛，**绝不静默回落 set0**。
>   - **跨 set 桶名唯一性在并发建桶下是 best-effort**（既有跨节点查重 TOCTOU 同款）。
>   - **`[[groups]]` 分组的目标桶钉 set0**（控制台上传面不参与新 set）。
>   - **加盘只能加在最后一个 set**（前滚约束）：往中间 set 加盘 → 启动指纹拒启，不可行。
> - **加密与对象锁（M5）边界**：详见「加密（M5）」「对象锁（M5）」两节记档段；跨节点要点——
>   桶锁状态列（`buckets.object_lock*`）随控制面行广播传播，属**最终一致**：窗口期内非版本化
>   覆盖写/回收路径 **fail-closed 拒删**（不静默放行）；行同步强一致归 M9。**多节点加密须先
>   把同一把主密钥（文件或 KMS 目录整份）分发到全部节点**再开写，否则本节点读不了别节点写的对象。

## Docker（compose）

**单机**（oss-server + 控制台 nginx）：

```bash
docker compose up --build        # 控制台 http://localhost:8080 · S3/API http://localhost:9000
```

**M3 集群**（4 节点 × 3 盘 = 12 槽；服务名即节点地址）：

```bash
docker compose -f docker-compose.cluster.yml up --build
# S3/控制面入口：n1:9000 · n2:9001 · n3:9002 · n4:9003（任意节点等价）；控制台 :8080 反代 n1
```

- 配置：单机 `docker/oss.toml`；集群 `docker/cluster/n{1..4}/oss.toml`（静态拓扑，注释在文件里）。
- **密钥自动生成**：`jwt_secret` / 集群 `secret` / 管理员口令**没填**就首次启动自动生成（64 hex），
  落盘宿主 `./docker/secrets/generated.env`——0644 直接 `cat` 看管理员口令，已 gitignore；
  集群四节点共享同一份（flock 串行化，并发首启收敛到同一份）。**轮换 = 删掉该文件重启**；
  想用自有值：在 `oss.toml` / `OSS_ADMIN_PASSWORD` 里显式填写（显式值优先，绝不覆盖）。
- 数据/暂存在具名卷（`docker compose down -v` 才清）；集群每节点独立卷。
- S3 客户端**直连节点端口**（SigV4/流式不经 nginx）；控制台走 :8080 的反代。
- 镜像：`Dockerfile`（oss-server release 构建，含 protoc）与 `docker/Dockerfile.web`（vite 构建 + nginx）。
- 集群 compose 的节点 `addr` 用服务名（主机名）——引导解析绑定 + 对端按名拨号（同一份配置两用）。

## 控制台（M1b）

```bash
# 1. 起后端（同 M1a）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173 ，用上面的账号登录
```

- 上传复用 aetherupload 自带脚本（分块 / 断线续传 / 秒传），前端不含分片逻辑
- **子账户**（管理员菜单）：创建/删除子账户、逐桶勾选 读/写/删 授权；子账户登录后只见
  授权桶（菜单无「子账户」页），上传与删除按钮按权限显隐（后端 403 兜底）
- **集群**（管理员菜单）：模式 / 槽位 / quorum 未达计数 + 节点表 + 拓扑指纹（15s 自刷新）；单机显示单机模式提示
- **上传落点**：按上传分组落桶（`oss.toml` 的 `[[groups]]`，如 `file` → `files`）；对象名 = 内容的整份 MD5 +
  扩展名，子目录 = UTC 年月（如 `202610/<md5>.bin`）。所以在对象浏览器里是根据内容哈希找文件，不是原始文件名
- 秒传提示「上传成功（秒传）」表示 0 分块传输
- e2e：`cd web && npx playwright install chromium && npm run e2e`。Playwright 自带双 webServer
  （`cargo run` + `vite`），后端按 `web/playwright.config.ts` 里的 `pw` 自启；**已有后端在跑时会被复用**
  （`reuseExistingServer`），此时运行中实例的管理员口令必须也是 `pw`，否则登录 401——口令不一致就先停掉
  自起的服务，让 Playwright 用 `pw` 起自己的（「跑起来」段用的 `changeme` 只是示例占位）。
- 1 GB 上传与断线续传为**人工验证项**（不在 e2e 里）：
  `head -c 1073741824 /dev/urandom > /tmp/1g.bin` → 面板上传观察进度推进 → 中途 DevTools → Network → Offline
  （或拔网线）→ 输出区出现「网络故障，正在重试……」→ 恢复网络 → 自动继续至成功；下载后 `cmp` 与本地一致。
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传未排期；当前限同一页面会话）

## 运维命令

`oss-server [serve|groups|build|clean <days>|fsck [--repair]|heal|scrub|rebalance [--dry-run|--apply]]`（无参数 = `serve`）。运维命令读当前目录的
`oss.toml`，并走与服务端相同的启动自举（建目录/连库/开存储；管理员与桶的引导是幂等的）：

```bash
oss-server groups        # 建分组目录（幂等；启动自举也会做，上传报笼统错误时先跑它确认）
oss-server build         # 按磁盘现状重建秒传索引（先清旧索引，再全量扫描暂存盘）
oss-server clean <days>  # 按 mtime 回收 days 天前的残骸：
                         #   暂存盘 `_header/` 与 `*.part`；数据盘 `.oss.sys/{tmp,trash}` 孤儿
                         #   与各对象目录下未被引用的版本目录（含未完成的上传暂存区）
oss-server fsck          # 全盘巡检：孤儿版本目录 / EC 分片缺坏（CRC32C）/ tmp-trash 计数 /
                         #   stray 分片文件（rebalance/崩溃残留；--repair 删除）；
oss-server fsck --repair #   同上 + 最小修复（删孤儿 + 读时修分片）。报告落
                         #   `.oss.sys/fsck/<时间戳>-<kind>.json`
oss-server heal          # fsck --repair 全量 + 缺失桶目录补建（集群差集）+ 逐版本修复
                         #   （历史版本同样点名读触发回写）；每节点各自执行——本地语义：
                         #   补的是本机盘上的缺口，集群收敛 = 各节点各跑一次
                         #   禁忌同 fsck --repair：不得与在途上传/写入并跑（读时修/孤儿
                         #   清理与在途版本目录竞争）
oss-server scrub         # 全量深度巡检（只读）：每个 EC 版本逐块读取 + 逐块 12×W 对偶校验
                         #   （可定位坏片；常量内存不组装整份对象）；历史版本全覆盖；
                         #   健康对象计 ok_versions；可重建的坏/缺块不退出非零（数据仍在
                         #   EC 冗余内、读路径自愈），仅深度失败（存活不足/对偶不过）exit 1（cron 可盯）
oss-server rebalance     # 扩容搬迁（默认 dry-run）：存量对象迁到展开放置（placement=v2；
                         #   未开则空跑）；逐对象 copy→锁内翻转→清老片，幂等可重跑；
                         #   报告 `<时间戳>-rebalance[-dry].json`（操作序见「扩容」节）
```

分工：`fsck` = 整片 CRC32C 快检（轻）；`scrub` = 块级深度验证（慢、可定位坏片）。

建议 cron（每日 `clean 1` 与 `build` 各一次，每周 `fsck --repair` 一次）：

```cron
17 4 * * * cd /path/to/oss && ./oss-server clean 1
47 4 * * * cd /path/to/oss && ./oss-server build
23 4 * * 0 cd /path/to/oss && ./oss-server fsck --repair
```

- `clean` 按 mtime 判定：上传持续时长在回收窗口内时不会被误删；超出窗口的长挂上传（仅 S3 直传可达，
  aether 与本地上传是分钟级）可能在仍进行时被判过期——本次写入失败、无数据损失；`days` 限 1..=36525
- `build` 先清空秒传索引再按盘重建：重建瞬间的并发上传会从「秒传命中」回退为正常上传，不丢数据
- 未知子命令或非法参数打印用法并 exit 2；命令失败 exit 1
- 配置文件路径固定为当前目录的 `oss.toml`（原先的 `oss-server <配置路径>` 用法已移除）

## TLS（传输加密）

默认明文（无 `[tls]` 时与旧版行为一致）。配置 `[tls.server]` 后，主 HTTP/S3/控制台监听器、
witness、Raft RPC、节点间 gRPC mesh、site/repl/tier 出站与 Kafka 全部启用 TLS——单一开关，
没有「半 TLS」模式。证书在启动时读取并校验；**轮换证书需要重启**（运行期不热加载）。

```toml
[tls.server]
cert = "/etc/oss/server.crt"
key  = "/etc/oss/server.key"
# 可选：要求并校验客户端证书（mTLS）。共享监听器上的 S3/控制台/Raft 客户端都必须出示。
client_ca = "/etc/oss/ca.crt"

[tls.client]
# 出站额外信任根（平台根始终参与校验）；自签/私有 CA 部署时配置。
ca_cert = "/etc/oss/ca.crt"
# 可选：出站客户端身份（mTLS，client_cert/client_key 必须成对）。
client_cert = "/etc/oss/client.crt"
client_key  = "/etc/oss/client.key"
```

- 对端地址仍可写 `http://`（集群节点、site endpoint、Raft `advertise_endpoint`、stretch）：
  连接时按 `[tls.server]` 升级为 https。未启用 `[tls.server]` 时写 `https://` 启动期拒绝
  （不静默降级明文）。`[listen]` 建议只写 `host:port`。
- `[[remotes]]`/`[[tiers]]` 的 `endpoint` 支持 `http://`/`https://`；证书主机名与 endpoint
  不一致时用 `server_name` 覆写（仅 https）。
- Kafka：`[[kafka.targets]].tls = true` 启用 SSL；`sasl` 支持 PLAIN / SCRAM-SHA-256 /
  SCRAM-SHA-512，且必须同时 `tls = true`。SNI 固定为 broker 主机名，证书 SAN 必须匹配。
- mTLS 是共享监听器的 opt-in：启用后**所有**客户端（S3、控制台、Raft、site）都必须带
  CA 签发的客户端证书；没有 `verify=false` 旁路。
- 出站（site/repl/tier/Kafka/mesh）始终校验平台根 + `[tls.client].ca_cert`；mTLS 身份用
  `[tls.client].client_cert/client_key`。

## 集群模式（M3）

**静态拓扑**：扁平 node × drive，配置即拓扑（无 DC/区域层级）。未配 `[cluster]` = 单机（本机行为与 v1.4.0
逐字节一致）；配置要求 **槽位数 ≥ data+parity（12）**。

```toml
[cluster]
node_id = "n1"
secret  = "cluster-internal-token-change-me"   # 节点间 gRPC 共享令牌（明文，内网假设）

[[cluster.nodes]]
id = "n1"
addr = "127.0.0.1:19101"   # 该节点 gRPC 监听（HTTP 端口另配）
drive_count = 3
# …n2/n3/n4 同构
```

启动：各节点在自己的目录放好 `oss.toml`（`data_dirs` 三条 = drive_count 3）后 `oss-server serve`；
顺序无所谓（滚动启动探活只告警；可达对端 node_id 与配置不符会拒启，防错接线）。

**临界口径（4 节点 × 3 盘 = 12 槽，EC 8+4）**：
- 停 1 节点（剩 9 槽 = 恰好 Q_s）：**写零余量**（9 片全须成功，客户端重试收敛）、读 9≥8 有余；
  本配置**单节点故障即临界**——要余量用 6×2（12 槽，停 1 余 1）。
- 停 2 节点（剩 6 槽 <9）：写被拒（锁/meta quorum 未达 → 5xx）；**读同不可达**——
  GET 报 `500 CorruptMeta`（**不是 404**，不许把「quorum 覆盖不足」伪装成「不存在」）。

**配置反例**：4×4 = 16 槽**勿用**——EC 只有 12 分片，`shard % 16` 只落槽 0..11，第 4 节点零分片，
停任一持片节点剩 8<9 反而更容易不可写。

**运维口径**：
- 每节点定期 `clean` + `fsck --repair`（同前 cron）；**远端孤儿只由该节点自己回收**——
  掉线期间其他节点删不掉它的残骸。分片缺失靠读时修兜底 + fsck 巡检；缺键 meta 由
  `fsck --repair` 从对端差集检出并补建（「加回节点后补齐」；批量口径用 `heal`——差集补建 + 桶目录补建 + 逐版本修复）。
- 节点掉线期间 `clean` 的对象目录回收会**保守空转**（不可逆删数据前全节点探活）；`trash/tmp` 照常。
- **`fsck --repair` 不得与在途分块上传并跑**（无龄期删孤儿会清掉暂存区）。
- **M3 不支持单机原地升级为多节点**（M4 的 rebalance 只覆盖既有集群扩容）：单机数据要进集群请重新灌入。
- `data_dirs` 顺序是拓扑的一部分：错位会在启动时被 `.oss.sys/topo` 指纹拒绝（大声失败）。

**观测**：`GET /metrics`（Prometheus 文本；**公开**，注释已写明与 `/health/cluster` 同量级信息）——`oss_quorum_unmet_total{op="write"|"lock"}` ·
`oss_node_up{node=…}` · `oss_drive_count` · `oss_slot_count`。M9 另暴露
`oss_raft_role{role="leader"|"follower"|"witness"}`、`oss_raft_term`、`oss_raft_last_applied`、
`oss_raft_members`、`oss_raft_production_ready`、`oss_raft_leader_changes_total`、
`oss_raft_apply_errors_total`、`oss_raft_forward_total`、`oss_raft_snapshot_bytes`；
sync 面 `oss_sync_acked_total`、`oss_sync_failed_total`、`oss_sync_write_intent_recovered_total`、
`oss_sync_delete_partial_total`、`oss_sync_delete_recovered_total`、`oss_sync_ambiguous_total`；
stretch 面 `oss_stretch_slots_ok`（未配置 `[stretch]` 时为 0）。控制台「集群」页（管理员菜单：模式 / 槽位 / quorum 未达计数 +
节点表 + 拓扑指纹，15s 自刷新；单机显示单机模式提示）与 `GET /api/v1/cluster/status`（admin；单机优雅降级 `mode:"single"`）同源。

```yaml
scrape_configs:
  - job_name: oss
    static_configs: [{ targets: ['oss:9000'] }]
```

**已知边界（M3）**：
- 分块上传（MPU）会话**粘节点**：同一 upload 的后续请求须回到发起节点。
- 节点间锁无写入侧 fencing：租约 TTL 远大于单写操作；兜底是版本 uuid 隔离（交错写最多留孤儿目录，
  由各节点 clean 回收，已提交数据不损坏）。
- 控制面（账号/权限）**读扇出**：任一节点铸造的 AK 全集群可用；写入仍只落本节点
  （跨节点强一致归 M9）。
- 节点间 gRPC 默认明文；配置 `[tls.server]` 后 mesh / Raft / site / repl / tier / Kafka 全部
  走 TLS（见 TLS 节）。traceparent 轻量传播（`RUST_LOG=debug` 下可按同一 trace id 在双节点
  日志抽样；完整 OTLP 归 M4）。
- 命名勿混淆：oss-server 的 aetherupload `staging_dir` ≠ 存储内核的 `.oss.sys/tmp`。
- **升级**：M9 Raft 控制面支持 Level B 滚动（基线 v1.13.1；步骤见「cutover / force recovery
  runbook」第 6 条），先 follower/learner、最后 leader，`feature_level` 默认 1。
  **M3/M4 数据面（分片 RPC/EC/放置）仍不支持滚动**：新旧混跑的写会安全失败（新协调者→旧节点
  被旧端 CRC 门拒收），跨界读按缺失降级；数据面升级须停写后整栈更换。TLS 物料与从
  < v1.13.1 跨版本也不在 Level B 范围内。
- **EC 区间读语义**（M4 切片 2）：Range 读不再做整片 CRC32C / 对象级 SHA256（改逐块 12×W 对偶校验，坏片可定位自愈）；
  **范围外的损坏不再触发读时修**（整读仍会），批量修复归 `heal`（M4 切片 4）。

## 扩容（rebalance，M4）

追加式扩容：放置开关（`placement = "v2"`）+ 尾部追加拓扑 + 逐对象搬迁。**操作序**（顺序不可乱）：

1. **全节点升级二进制**（placement 仍默认 v1，行为不变）。
2. 各节点 `oss.toml` 加 `placement = "v2"`——**全节点同步**。混版窗口内**部分写入会降级/失败**（可诊断）：
   旧节点发出/转发的分片请求没有 slot（proto 未知字段 → 0），新服务端按「slot 0 属主」校验而拒（除 slot 0
   属主那台）；反向（新→旧）旧服务端忽略未知字段按 v1 重算 = v1 下结果正确。**升级窗口只做升级、不承载业务写；
   全节点升级完再开 `placement="v2"`。**
3. 追加拓扑（加盘/加节点，见下）。
4. 逐节点 `oss-server rebalance --dry-run` 看计划。
5. `oss-server rebalance --apply`（幂等，可重跑续搬；同盘搬迁走 **hardlink 快路径**，C4——
   免整片重拷、崩溃安全，跨设备自动回落分块 copy）。

**加盘**：只能往**最后一个节点**追加 `data_dirs`（中间节点加盘会位移其后节点槽位，启动时指纹拒启）。
新盘必须先「入册」——在已挂载的新盘上
`mkdir -p <盘>/.oss.sys && cp <任一旧盘>/.oss.sys/drive-id <盘>/.oss.sys/`
（防遮挂载点 impostor 的既有策略不允许自动初始化非首启盘）。
注意：**单节点盘数不得超过 parity**（默认 8+4 即每节点 ≤4 盘，超过启动拒启）——加盘是 3→4 而非 3→5；
更大容量走**加节点**（新节点同样 ≤4 盘）。

**加节点**：`cluster.nodes` 尾部追加 + 该节点自备配置启动。**加节点后也要全节点 placement=v2 + rebalance**
（v1 恒等放置永不使用新槽）。

**单机多盘**：超过 12 盘时自动 v2（无需配置）；旧对象用 `rebalance` 搬迁。

**边界**：不支持缩容（删节点/盘）；**配置回退也不可行**（旧槽位引用 fail-closed 500），只能前滚；
同刻只跑一个 rebalance；`rebalance` 与 `fsck`/`heal` **不得并跑**——fsck 的 stray 扫描会把搬迁中
「已 copy 未 flip」的半成品新片当作残骸删除（对象级自愈可重跑，但白搬一轮）；搬迁残窗与老片清理
兜底归 `fsck`（stray 分片扫描，跑在 rebalance 停后）。

## 版本控制（M4）

`aws s3api put-bucket-versioning --versioning-configuration Status=Enabled` 开启；`list-object-versions` 枚举；
GET / HEAD / DeleteObject 带 `--version-id` 点名版本；`copy-object --copy-source 'bucket/key?versionId=<旧>'` 即**回滚**
（旧版本 → 新版本）；无版本号 DELETE = delete marker（GET 404、对象列表隐身、版本列表可见）。`tests/e2e_s3.sh` 第 16 节实弹覆盖。

**v1 边界（诚实记档）**：
- 版本控制三态：`Off` / `Enabled` / `Suspended`。Off→Enabled、Off/Enabled→Suspended 允许；
  改回 `Off` 仍拒绝（`NotImplemented`，防整链替换丢历史）；Object Lock 桶与 M9 sync 桶拒绝 Suspended
  （`InvalidBucketState`）。Suspended 下 PUT/Complete 只替换 null 版本，无版本号 DELETE 追加 null delete marker，
  `versionId=null` 可点名删除；未开过版本的桶 `get-bucket-versioning` 回 `Off`（AWS 留空）。
- 所有写响应透出 `x-amz-version-id`（PUT / Complete / Copy / DeleteObject；DeleteObject 另带 `x-amz-delete-marker: true`）；
  GET / HEAD 也回实际版本号。点名 GET / HEAD marker 版本 → 405。
- 对象列表（ListObjectsV2）只见「链尾非 marker」的键；区间读对历史版本同 S3 语义。
- `ListObjectVersions` v1 **忽略 delimiter**（不产 CommonPrefixes）；续点（key-marker / version-id-marker）不匹配链时
  返回 400 InvalidArgument（fail-loud）。
- 集群：版本追加仍走 write_meta quorum（整份 meta 重写）；`list-object-versions` 集群路径为逐键 quorum 读
  （低频运维面，量级上去再做分页 RPC——记档）。
- 对象锁（M5）：开锁桶须版本控制 Enabled（建桶旗标会一并自动开）；锁定版本的删除语义见「对象锁（M5）」节。

## 生命周期（M4）

控制台 API `PUT /api/v1/buckets/{bucket}/lifecycle`（body 规则数组，**全量替换**；桶主或管理员）
配置，`GET` 同路径回读；S3 `Put/Get/DeleteBucketLifecycleConfiguration` 与控制台共用同一校验。
规则结构（旧扁平字段仍兼容，新字段优先且互斥）：

```json
{"rule_id":"cold","enabled":true,
 "filter":{"prefix":"","tags":[{"key":"env","value":"prod"}],
           "object_size_greater_than":1024,"object_size_less_than":1048576},
 "expiration_days":30,
 "expiration_date":"2026-12-31",
 "expired_object_delete_marker":false,
 "transitions":[{"days":7,"storage_class":"GLACIER","tier":"t1"}],
 "noncurrent_version_expiration":{"noncurrent_days":30,"newer_noncurrent_versions":2},
 "noncurrent_version_transitions":[{"noncurrent_days":30,"newer_noncurrent_versions":2,
                                    "storage_class":"GLACIER","tier":"t1"}]}
```

Filter 的 prefix/tag/size/And 与 Expiration.Date/ExpiredObjectDeleteMarker、Transition.Date、
多转换、noncurrent expiration/transition 均已支持；`AbortIncompleteMultipartUpload` 仍 501
（需 MPU 索引）。后台 worker 按 `OSS_LIFECYCLE_INTERVAL_SECS`（默认 60s；显式 `0` 关 worker）
扫描，到期判定 `now - last_modified ≥ expire_days×24h`（**0 = 立即到期**）。版本控制桶默认打
**delete marker**（不物理删）；noncurrent 规则按“新版本创建时间”计算；非版本桶 = 整键删除。

进度：`GET /api/v1/jobs`（admin）看最近作业（scanned / deleted / skipped / state / error，逐规则一条）。

**边界**：
- 转换/分层归 M8。
- **同刻建议只跑一个 serve 实例**：每实例各挂 worker（多实例会重复执行；幂等但白跑）。
- **大桶性能**：worker 每周期对该桶全前缀列举一次——前缀很大时建议调大 `OSS_LIFECYCLE_INTERVAL_SECS`。
- **饥饿边界**：单轮单规则 ≤10 万键（100 页 × 1000）；超出即**截停记 jobs.error**（`state=error`，不静默）——
  但**扫描位点不跨轮持久化**，前缀下若长期 >10 万「未到期」键，尾部的到期键可能长期扫不到（调小 prefix 拆规则可解）。
- **陈旧作业行**：进程崩溃可能留下 `state='running'` 的 jobs 行（无自动回收，展示层可按 updated_at 过旧识别）。

## 桶策略与审计（M4）

控制台 API `GET/PUT /api/v1/buckets/{bucket}/policy`（桶主或管理员；**桶不存在 = 404**——防给
尚不存在的桶预置策略、同名桶日后创建即继承）：`PUT` 为**全量替换**（≤64 KiB、≤100 条 statement、
顶层必须对象、`Effect` 必须为 `Allow`/`Deny` 且 Principal/Action/Resource 非空；越出支持子集或
拼错即 400，不静默空转），`GET` 回读原文（未配 = `null`）。S3 面判定口径：**显式 Deny 优先 →
Allow 提升 → 无匹配回落三档 permissions**；策略读/解析失败 fail-closed 拒。`CopyObject`
**源/目标两轴独立判定**：目标桶写策略与源桶读策略各判各的，**源桶 Deny 压过目标桶 Allow**
（否则有目标桶写权者可绕过源桶 Deny 拷走机密），且**源桶 Allow 不能替代目标桶写权限**。
`DeleteObjects` **逐键授权**：批量删的键在请求体、`check` 阶段不可见，授权下放 handler 逐键判定
（与单键删同口径——策略 Deny → 拒 / Allow → 放 / 无匹配 → 回落三档）；Deny 键以 `AccessDenied`
记入响应 `errors[]` 并落审计，其余键照删（§3.4 部分失败语义；否则前缀级 Deny 会被一条批量删
请求整体绕过）。

支持子集（`crates/xl-s3/src/policy.rs`）：`Effect` · `Principal.AWS`（`"*"` | AK | `"uid:<n>"`）·
`Action`（精确或尾部 `*` 通配，如 `s3:Get*`）· `Resource`（桶级 `arn:aws:s3:::<bucket>` /
对象级 `arn:aws:s3:::<bucket>/<prefix>*`，**按 op 类别单轴匹配**——桶级 op 只见桶 ARN、
对象 op 只见对象 ARN，与 AWS 一致）。**明确不做**：Conditions / NotAction / NotResource /
Principal 的 ARN 形态（`Condition` 仅在 AP 策略支持 `IpAddress`/`NotIpAddress` + `aws:SourceIp`，
见「访问点」节）。显式 Deny 对桶主与管理员同样生效（AWS 语义，e2e 实测）——管理员只旁路
三档 permissions，不旁路桶策略 Deny。

**匿名（无凭据）放行（2026-10-23 落地）**：`Principal:"*"` 对签名请求仍表示「任意有效 AK」；
对无凭据请求只在**数据面白名单** `GetObject` / `HeadObject` / `PutObject` / `DeleteObject` /
`DeleteObjects` 上参与判定，且必须由桶策略显式 Allow。无策略 / 无匹配 / 策略读取失败一律 403
（fail-closed）；显式 Deny 优先。列表、建删桶、策略/ACL/版本控制/生命周期/加密/对象锁/
tag/retention/legal-hold、MPU、CopyObject、控制台 API 等即使策略写 `s3:*` 也 403。
`DeleteObjects` 在请求级只要求「存在桶策略」，随后逐键复评（被拒键进 `errors[]`，其余照删）。
匿名请求落审计 uid=0、detail 为 `anonymous:*` / `policy:*`；判定发生在对象查找前，
拒绝与否不泄露键是否存在。匿名请求携带对象锁/保留期/bypass 头一律 403（见「对象锁」）。

审计：S3 面每次授权判定（允许与拒绝都）落一行 SQLite `audit_log`，`detail` 记策略来源
（`policy:allow` / `policy:deny` / `policy:error`）；**保留期清理（C5）**：`OSS_AUDIT_RETENTION_DAYS`
（默认 0 = 不清理，钳 0..=36500）开启后每小时按批 10000 条清 `ts` 超出保留期的行
（best-effort，失败只 warn）。
（`policy:allow` / `policy:deny` / `policy:error`）；`GET /api/v1/audit?limit=&decision=allow|deny`
（仅管理员；limit 默认 100、钳 1..=1000；`decision` 只认 allow/deny，其它值视为不过滤，最新在前）。
`tests/e2e_s3.sh` 第 17 节实弹覆盖（Deny 优先 / 校验 400 / 审计查询 / 清策略恢复 / 删桶清策略 /
批量删逐键授权）。
**匿名拒绝限流（M1）**：匿名**允许**判定照常全量落行；匿名**拒绝**经进程级令牌桶限流
（`OSS_ANON_DENY_AUDIT_BURST` 默认 20、`OSS_ANON_DENY_AUDIT_PER_SEC` 默认 5；可选
`OSS_ANON_DENY_AUDIT_DEDUPE_SECS` 默认 0，按 `(op,bucket,key)` 去重）。参数启动时钳制
（burst 1..=10000、每秒 0..=10000、去重 0..=86400），非法值 warn 后回默认；洪水期丢弃不写
（每累计 1000 条 warn 一次），防止未认证流量无界撑大 `audit_log`。匿名请求携带对象锁/
治理旁路头一律 403（见「对象锁」）。

## 多 set（M4）

单部署多 EC 组（set）的横向扩容形态：新 set 加容量，**老 set 零重排**；单 set（k=1）与既有部署
逐字节一致（RPC 缺省字段、指纹、放置全等价；原地升级 = 末尾追加 `set=1` 条目，老段不动）。

**配置**：`[[cluster.nodes]]` 新增可选 `set`（缺省 0）。同一物理节点可在多个 set 各出一条
（同 `id`、**`addr` 必须一致**——节点间 RPC 只按 id 路由，不一致拒启）；set 号必须从 0 连续
（缺号 fail-closed）。**`data_dirs` 顺序契约（硬）**：按 set 升序、set 内按节点声明序拼接；
本节点各 set 的 `drive_count` 之和 == `data_dirs.len()`（open 自校验）。每个 set 独立走全套拓扑
校验（槽 ≤255、≥ data+parity、多节点时每节点盘数 ≤ parity；flat 并集另受单节点总盘数 ≤255 约束）。

```toml
[[cluster.nodes]]          # set0 段（缺省，可不写 set）：n1..n3 各一条
id = "n1"
addr = "127.0.0.1:19101"
drive_count = 3
# n2 / n3 同构…

[[cluster.nodes]]          # set1 段：同物理节点再出一条（原地升级 = 末尾追加，老段在前不动）
id = "n1"
addr = "127.0.0.1:19101"   # 同 id 多 set 的 addr 必须一致
drive_count = 3
set = 1
# n2 / n3 同构…
```

**桶钉 set**：控制台建桶（`POST /api/v1/buckets`）可选 `set`（校验 < 组数；**缺省自动指派**
到桶数最少的 set，C3）；S3 面 CreateBucket（协议无 set 形参）缺省同样**自动指派**到负载最小的
set（单 set 恒 0）。归属存 `buckets.set_id` 列（老库启动幂等补列，存量 = 0）。
**set 一经创建不可变**——没有改 set 的 API，已有桶不跨 set 搬迁。

**隔离口径**：set 只约束**数据分片**（放置公式与分片 RPC 写/读/删）；**桶目录与 `xl.meta`
跨全部 set 镜像**（命名空间面：每盘一份完整命名空间副本，全盘迭代）——空桶目录与元数据副本
出现在非属主 set 的盘上属预期，不是串组。

**拓扑指纹**：每盘一条。set0 段盘写 `v2 node=… slot=…`（与单 set 部署逐字节一致、不重写），
set>0 段盘写 `v3 set=… node=… slot=…`（slot 为**组内槽**）；`v3` 且 `set=0` = 拒启（写路径永不
产出该形态）。

**扩容约束**：加盘只允许加在**最后一个 set**（往中间 set 加盘会位移其后段 → 启动指纹拒启）；
跨 set 不搬迁已有桶——多 set 是给新桶加容量，不是对存量重分布。

**观测**：`GET /api/v1/cluster/status`（admin）新增 `sets` 数组（每 set 一条：组内槽数、节点表、
本节点是否属该组）；顶层 `nodes` / `slot_count` 为按 id 去重的节点并集（命名空间面口径）。
**各 set 的槽号是独立槽域，跨 set 会重号——按 set 分组读**（控制台「集群」页已按 set 分组展示，
C2）。

## 加密（M5）

**顺序是硬约束：先加密，再纠删码**——写路径 `明文 → 块对齐流式加密（AES-256-GCM，DARE 式
65520B 明文单元 → 65536B 密文单元）→ EC 编码 → 落盘`，读路径反向；因此**单个分片（不足 quorum）
不含任何明文信息**，parity 块同理（验收 ②）。

三种模式由请求头选定（**env 一个不配 = 加密整体不启用，零行为变化**）：

| 模式 | 启用 / 请求 | 密钥来源 | 盘上存什么 |
|---|---|---|---|
| SSE-S3 | env `OSS_MASTER_KEY_FILE=<路径>`；头 `x-amz-server-side-encryption: AES256` | 单主密钥文件（缺省 `<data_root>/.oss.sys/master.key`，0600 首启自动生成；**多节点须分发同一把**） | 包装后的 DEK，`key_id="file"` |
| SSE-KMS | env `OSS_KMS_DIR=<目录>`；头 `aws:kms` | 多钥目录 `key-<n>.bin`（当前钥 = 最大 n） | 包装后的 DEK + `key_id="k<n>"` |
| SSE-C | 头 `x-amz-server-side-encryption-customer-{algorithm,key,key-MD5}`（每次请求带 32B 客户端钥） | 客户端 | **只存 `base64(MD5(key))` 校验值（AWS 同款 24 字符头），密钥绝不落盘** |

- 每对象一把 32B DEK（OsRng 生成）；单元 nonce = 随机种子(8B) ‖ BE32(单元号)，单元号把
  （EC 分片, 块）双射到密文单元——同一 DEK 下 nonce 不重用。
- 响应头：PUT / HEAD / GET 回 `x-amz-server-side-encryption`（SSE-C 另回 customer-key-MD5 等
  校验头），`aws-cli` 的 `sse` 参数直连可用。
- **口径**：ETag / Content-MD5 / SHA256 全部对**明文**计算（协议语义不变）；分片 CRC32C 对**密文**；
  SSE-KMS 请求若**显式带 CMK**（`…-aws-kms-key-id`）→ 400 拒绝（多主密钥轮换编排未做，
  明确拒优于静默忽略）；读/删 SSE-C 对象必须带同一把钥（错钥 → 400，校验值即判据）。
- MPU：SSE 意图在 CreateMultipartUpload 记录（含 SSE-C 校验值），Complete 统一落地；
  SSE-C 的 UploadPart / Complete 须全程带同名钥；读写响应带版本/加密头。
- **换主密钥不重写数据**（验收 ⑤）：DEK 的包裹体在 `xl.meta`，只要旧 `key_id` 的钥仍在即可解——
  轮换 = 往 `OSS_KMS_DIR` 加入更大的 `key-<n>.bin`（新写用新钥，旧对象照读）；
  单钥文件 provider 直接换文件会让旧 `key_id="file"` 的包裹不可解（**先迁到 KMS 目录再轮换**）。
- **SK 加密存（M5）**：`access_keys.secret_key` 同样信封加密（`enc:v1:` 前缀，AAD 域分隔，
  每行独立 DEK；kms 目录优先、单钥文件兜底）——**两处写点**（控制台建键、STS 签发）与**读侧
  透明迁移**（存量明文行被读到即原地重写为密文）；集群下密文过网、消费端解密（mesh 不出现
  明文 SK）。**运维硬约束**：多节点须分发同一把主密钥；**撤掉 `OSS_MASTER_KEY_FILE`/`OSS_KMS_DIR`
  环境变量会让既有密文行全部 500（fail-closed）**——变更前先规划。**威胁模型边界（记档）**：
  加密只防**只读**泄露（备份/快照/误拷贝）；对数据库有写权限者可写入自选明文 SK 接管任意 AK
  （切片前有 DB 写权本就全权，非新增面）。
- **边界（记档）**：桶级默认加密（`put-bucket-encryption`）已落地（单条 SSE-S3/SSE-KMS 规则；
  SSE-C 与显式 CMK 拒绝）；aetherupload 内部摄取不带加密（存入为明文）；
  未配对应 env 时请求该模式 → 500（fail-closed，不静默落明文）；
  HTTP KMS 未实作（`KeyProvider` trait 已留接入点，文件型 KMS 即多钥目录）。
- `tests/e2e_s3.sh` 第 18 节（随 M5 收尾落地）实弹覆盖「写入 → head 头断言 → 盘上 grep
  明文特征串」。

## 对象锁（M5）

**桶级**：`CreateBucket` 带 `x-amz-bucket-object-lock-enabled: true` 建桶即开锁（**自动开版本
控制**，AWS 语义）；或对已开版本控制的桶 `PutObjectLockConfiguration` 开锁并设**默认保留规则**
（`Days`/`Years`，归一为天存储，上限 36500 天；`Enabled` 只能开不能关——二次 Put 只许改规则）。
默认规则对**未带锁头**的 PUT 生效（显式锁头优先，不叠加）。

**对象级头**（PutObject / CopyObject 目标 / CreateMultipartUpload；MPU 在 create 时记录、
complete 时落地）：

- `x-amz-object-lock-mode: GOVERNANCE | COMPLIANCE` + `x-amz-object-lock-retain-until-date`
  —— 必须**成对**且 until 在未来（否则 400）；桶未开锁带锁头 → 400。
- `x-amz-object-lock-legal-hold: ON | OFF` —— 独立于 retention。

**删除守卫**（点名删版本 / 批量删 / 覆盖写回收 / 控制台删路径统一）：`until ≤ now`（到期）可删；
未到期 **COMPLIANCE 一律拒（含管理员）**；未到期 **GOVERNANCE 仅
`x-amz-bypass-governance-retention: true` 放行**（调用方须已鉴权）；**Legal Hold ON 一律拒**。
拒删 = 403 `AccessDenied`（批量删逐键记入响应 `errors[]`，其余键照删）。无版本号 DELETE 照常打
delete marker（锁定版本天然留存）；生命周期删除同理。
Legal Hold 开关 API：`put/get-object-legal-hold`（`put` 须点名版本且**桶已开锁**，否则 400；
`get` 为元数据面读——SSE-C 对象不要求客户钥）。
响应面：GET / HEAD 回 `x-amz-object-lock-mode` / `-retain-until-date` / `-legal-hold`。
**策略动作**：`Get/PutObjectLegalHold` 映射 `s3:GetObjectLegalHold` / `s3:PutObjectLegalHold`
（独立于 `s3:GetObject`/`s3:PutObject`；AP 策略须显式授予）。直接 S3 路径按 Read/Write 门
（与 retention 同轴），非管理员需具备对应权限；这是 2026-10-23 的权限口径变化，发布说明需点名。
**匿名边界**：无凭据请求携带任一
对象锁头（`x-amz-object-lock-mode` / `-retain-until-date` / `-legal-hold`）或
`x-amz-bypass-governance-retention` 一律 403——匿名策略没有可表达的锁动作，fail-closed。

**保留期**：`Put/GetObjectRetention` 已接线（`versionId` 必填；COMPLIANCE 不可缩短/清除，
GOVERNANCE 缩短/清除须 `x-amz-bypass-governance-retention`）；控制台对象列表展示 retention / legal-hold 状态。
**边界（记档）**：`buckets` 表锁列跨节点的**行广播**传播，窗口期内非版本化回收路径
fail-closed 拒删（强一致归 M9，见「已知限制」）。

## STS 临时凭据（M5）

标准 STS 协议（Query/XML）由 s3s 前置路由拦截（`POST /` + form 编码；**须 SigV4 签名**，
不覆写 `check_access`）。实现两种操作：

```bash
# AssumeRole（调用者须**管理员**——最小模型无角色信任策略，防子账户自铸 admin 会话提权；
# 「角色 = 用户别名」，ARN 形如 arn:oss:iam::oss:role/<用户名>）
aws --endpoint-url http://127.0.0.1:9000 sts assume-role \
  --role-arn arn:oss:iam::oss:role/erik --role-session-name s1 --duration-seconds 900 \
  [--policy '{"Version":"2012-10-17","Statement":[...]}']
# GetSessionToken（任意长时键自派生，TTL 900..=129600 秒，默认 12h）
aws --endpoint-url http://127.0.0.1:9000 sts get-session-token --duration-seconds 900
```

- 响应含 `AccessKeyId`（`ASIA…`）/ `SecretAccessKey` / `SessionToken` / `Expiration`；
  临时凭据对 S3 面**全集群可用**（会话行随 AK 扇出）。
- **会话策略（仅 AssumeRole 的 `Policy` 参数，≤4096B）与用户权限取交集**：在表三档 × 桶策略
  融合**之后**再叠——会话 Deny → 拒；**无匹配 → 拒**（未 Allow 即拒）；会话不能抬升权限。
  `DeleteObjects` 逐键同样叠会话判定（防批量删绕过）；审计 detail 缀 `;session:<allow|deny>`。
- **过期**：会话行过期 → `ExpiredToken`（长时键过期维持 `InvalidAccessKeyId`）；**父链一层**：
  父键（或父属主账号）删/停/过期 → 子即失效（父须为长时键；会话不能作父）。
- **边界（记档）**：`SessionToken` 本体不落库不校验（AK 即凭据——s3s 认证接口无头面，AWS
  信任链差异）；错误响应用 S3 错误 XML（非 STS `ErrorResponse` 形态，成功路径 aws-cli 直用）；
  GET query 形（`?Action=`）不接；时长越界报 400 `InvalidArgument`（AWS 为 `ValidationError`）；
  会话**只对 S3 面**生效（控制台/aether 走 JWT，无 AK 入口）；过期会话行不自动清理（自然不可用）。
- **AssumeRoleWithWebIdentity：裁剪延后**（成本评估：JWKS 拉取需出站 TLS 客户端新依赖栈；
  验收 ①–⑤ 不涉及；记档于 M5 计划，接入点已留——`StsRoute` 动作分发 + `check_access` 匿名覆写）。

## 事件通知（M6）

桶通知配置走 S3 协议（`PutBucketNotificationConfiguration` / `GetBucketNotificationConfiguration`）；
事件写入本地 outbox（`event_outbox`，至少一次）后由 worker 投递。目标只支持 Kafka：

```toml
# oss.toml：通知目标静态配置（id 与桶配置 ARN 对应）
[[kafka.targets]]
id = "t1"
brokers = ["127.0.0.1:9092"]
topic = "oss-events"
# 可选：TLS / SASL（SASL 必须同时 tls = true；见 TLS 节）
tls = false
# [kafka.targets.sasl]
# mechanism = "scram-sha-512"   # plain（缺省）/ scram-sha-256 / scram-sha-512
# username = "..."
# password = "..."
```

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-notification-configuration \
  --bucket bkt --notification-configuration '{
    "QueueConfigurations":[{
      "Id":"all","QueueArn":"arn:oss:sqs::t1:kafka",
      "Events":["s3:ObjectCreated:Put","s3:ObjectRemoved:Delete"],
      "Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"logs/"}]}}
    }]}'
```

- 事件名白名单：`s3:ObjectCreated:{Put,Post,Copy,CompleteMultipartUpload}` ·
  `s3:ObjectRemoved:{Delete,DeleteMarkerCreated}` · `s3:LifecycleExpiration:{Delete,DeleteMarkerCreated}` ·
  `s3:LifecycleTransition` · `s3:ObjectRestore:{Post,Completed}` ·
  `s3:ObjectAccessed:{*,Get,Head}`（读事件见下条）。
  事件 JSON 为 AWS 通知形状（`eventVersion 2.1`、`eventSource oss:s3`、urlencoded key、sequencer）。
- 投递语义：**至少一次**（稳态不重；崩溃窗可能重投）。永久错（鉴权/协议）>10 次入死信；
  连接类错误永不放弃（退避封顶 300s）。Topic 需**预创建**（部署前置；UTOP 不判死）。
- 无 Kafka 时的调试旁路：`OSS_EVENT_SINK=file:/<dir>`（落文件，测试用）；worker 间隔
  `OSS_EVENT_INTERVAL_SECS`（默认 1；0=关）。
- 读访问事件：`s3:ObjectAccessed:*` / `:Get` / `:Head` 已实现（仅成功 200/206；304/416/错误不发）；
  其余 ObjectAccessed 子操作（如 GetTagging）仍 400；TLS/SASL 见 TLS 节。

## S3 Select（M6）

`SelectObjectContent` 支持 CSV / JSON（LINES/DOCUMENT）× 输出 CSV / JSON，压缩 NONE/GZIP：

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api select-object-content \
  --bucket bkt --key data.csv \
  --expression "SELECT s._1, s._2 FROM S3Object WHERE s._2 > 15 LIMIT 10" --expression-type SQL \
  --input-serialization '{"CSV":{},"CompressionType":"NONE"}' \
  --output-serialization '{"CSV":{}}' /tmp/out.csv
```

- SQL 子集（白名单外一律 `InvalidRequest`，不静默错结果）：`SELECT 列|* FROM S3Object [s]
  [WHERE …] [LIMIT n]`；比较、AND/OR/NOT（SQL 三值逻辑）、BETWEEN/IN/LIKE/IS NULL、CAST；
  函数 `substring/trim/lower/upper/char_length/abs/ceil/floor/round`；聚合
  `count(*)/sum/min/max/avg`（不支持 GROUP BY；聚合须独占投影）。CSV 列 `s._1`；JSON 路径 `s.a.b`。
- 输入：CSV 分隔符/引号/转义/记录分隔符/注释（缺省 `#`）/表头可配；`AllowQuotedRecordDelimiter`
  缺省 false。ScanRange 按「记录首字节落在区间内则整条处理」；仅给 End = 扫描末尾 N 字节；
  GZIP 与 ScanRange 互斥；JSON DOCUMENT 不支持 ScanRange。
- 输出：Records ≤256KiB 分片 + Progress（约 1MiB 节奏，`--request-progress`）+ Stats + End；
  Stats 三计数独立（`BytesScanned` 原始/压缩后、`BytesProcessed` 送入求值器、`BytesReturned` 输出）。
- 上限/边界（记档）：对象 ≤256MiB、GZIP 解压 ≤512MiB（全量缓冲，无并发预算）；BZIP2 返回 400
  （后置）；Parquet 未支持；CRLF 保留 `\r`；空 CSV 字段进 sum/avg 报 400；LIKE 模式 ≤4096 字节。

## 桶复制（M6）

源桶须**版本控制 Enabled**；目标用 `[[remotes]]` 静态配置，规则 `Destination.Bucket` 写成
`<remote-id>/<桶名>`：

```toml
[[remotes]]
id = "r1"
endpoint = "https://10.0.0.2:9000"  # http/https 均可；https 见 TLS 节
server_name = "s3.internal"           # 可选：证书主机名与 endpoint 不一致时覆写
access_key = "..."                   # 目标实例上铸造的 AK
secret_key = "..."
region = "us-east-1"
max_bytes_per_sec = 1048576          # 可选：出站令牌桶限速（字节/秒，≥1024；缺省不限速）
```

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-replication --bucket srcbucket \
  --replication-configuration '{"Role":"arn:oss:iam::oss:role/replication","Rules":[{
    "ID":"logs","Status":"Enabled","Filter":{"Prefix":"logs/"},
    "Destination":{"Bucket":"r1/dstbucket"},
    "DeleteMarkerReplication":{"Status":"Enabled"}}]}'
```

- 覆盖全部写入口：PUT / MPU complete / CopyObject / 单删与批量删 marker / aether 摄取 /
  控制台删除 / 生命周期删除。对象另带 `x-amz-replication-status`：源 `PENDING`→`COMPLETED`
  （任一目的地永久失败超限 → `FAILED`）；目标为 `REPLICA`（防环，REPLICA 不再入队）。
- 出站为 SigV4 + http1 PUT（`x-amz-replication-status: REPLICA` 纳入签名）；worker 300s 租约 +
  R1 分级重试（连接/408/429/5xx 永不放弃、其余 4xx 永久、>10 次死信）；积压写入 `repl_outbox`，
  目标恢复后自动追平。间隔 `OSS_REPL_INTERVAL_SECS`（默认 1；0=关）。
- 限速按远端独立令牌桶（`[[remotes]].max_bytes_per_sec`）；缺省不限速，旧配置零改动。
- 边界（记档）：目标桶/主题需**预创建**；SSE-C 源对象不可复制（客户钥不落盘）；版本定向删除
  与 `Priority` 不生效（不同目的地都会复制）；MPU 源 ETag 目标重算（不做 ETag 断言）；
  单对象整读入内存（≤16 并发）；`Role` 不参与鉴权。

## 多站点复制（M7）

两个**单节点**实例建立站点对，双向异步复制控制面与版本化对象。`[site]` 是唯一开关（与
`[cluster]` 互斥；未配置时站点 API 一律 404）：

```toml
[site]
name = "site-a"
region = "ap-east-1"     # 配对时对端校验一致
priority = 10            # 同秒 LWW 的裁决优先级
token = "<32–128 位十六进制>"   # 控制面通道共享密钥；缺省首启随机生成
```

```bash
# 管理员面：本站身份（含 token）→ 在 A 上配对 B（B 端也需配对 A 才是双向）
curl -s $A/api/v1/site -H "authorization: Bearer $JWT"
curl -s -X POST $A/api/v1/sites -H "authorization: Bearer $JWT" -H 'content-type: application/json' -d '{
  "peer_id":"b","endpoint":"http://10.0.0.2:9000","region":"eu-west-1","priority":10,
  "s3_access_key":"<B 上铸造的 AK>","s3_secret_key":"<SK>",
  "peer_site_token":"<B 的 site token>"}'
curl -s $A/api/v1/sites/b/status -H "authorization: Bearer $JWT"   # lag/dead/outbox
```

- **控制面**：`buckets`（列级 versioning/owner/lock）、`bucket_policies`、`permissions`、
  `users`、长时 `access_keys` 的变更由 SQLite 触发器写入 `site_changelog`；worker 逐 peer
  按 cursor 出货（≤100 条/批，`OSS_SITE_INTERVAL_SECS` 默认 1s）。冲突用 LWW
  `(ts, priority, origin, seq)`：ts 新者胜，同秒按 priority，再按 origin 字典序；本地写若旧于
  已知时钟会被 BEFORE 门控拒绝（409/500 直到追上）。**管理员行不复制**、STS 会话键不复制。
- **配对快照**：`POST /sites` 在单事务内写 `sites` + cursor + 五类实体快照，快照同事务入本地
  时钟；只快照「本地时钟无行或 origin=本站」的实体，避免同时配对互相回显。
- **对象**：复用 `repl_outbox`（`target_kind='site'`），每个 peer 一行；仅版本化数据写与
  marker 入队，删除要求源桶 versioning=Enabled。worker 发送前签名 `GET /{bucket}?versioning`
  （10s 缓存；404/非 Enabled → 可重试），PUT 透传 content-type 与 `x-amz-meta-*`、要求目标返回
  真实 version-id（`null` → 重试）。同版本多目标聚合 PENDING→COMPLETED/FAILED。
- **边界（记档）**：对端地址 http/https 均可（peer TLS 见 TLS 节）；单节点站点对；版本 UUID 两端不一致、版本定向删除
  不复制；非空桶删除不生效（保留行与数据，记死信）；锁/标签/SSE 不复制，SSE-C 源永久错；
  AK 密文需两端同主密钥（配对只校验 vault 开关一致，不校验同一把钥）；控制面最终一致（无强一致，
  归 M9）；端点/凭据明文存储；`priority` 仅在**同秒**裁决（新 ts 的低优先级仍胜旧 ts 的高优先级——
  与早期规格「按优先级裁决」有偏差）；解配期间的本地控制面改动不被捕获，且对端快照可能在重配对时
  覆盖这些改动（上次由对端胜出的实体）——需在重配对后重做；`x-amz-replication-status: REPLICA`
  由任意已认证写者提供，可被用于抑制复制（M6 既有信任面，M7 的 DELETE 防环依赖同一头）；桶删除时
  若策略/授权行的 LWW 时钟更新，会按 L7 保留，同名桶重建可能继承这些残留行（跨站点并发删/改窗口）。

## M8 控制面（S0–S6 已落地）

以下为 M8 已落地能力与用法；设计偏差与已知边界见本节末与设计文档 §M8。
控制面状态存于 SQLite `0003_m8.sql`（`access_points`/`olap_configs`/`jobs`/`job_tasks`/
`inventory_configs`/`inventory_runs`/`tier_restores`），**单节点单进程**语义。

### 对象标签与 Copy 指令（P0，已完整）

```bash
aws --endpoint-url $E s3api put-object --bucket b --key k --body f \
  --tagging "env=prod&team=%E4%B8%AD%E6%96%87"
aws --endpoint-url $E s3api get-object-tagging --bucket b --key k
aws --endpoint-url $E s3api put-object-tagging --bucket b --key k \
  --tagging 'TagSet=[{Key=a,Value=1}]'
aws --endpoint-url $E s3api delete-object-tagging --bucket b --key k
# Copy 两轴：REPLACE 未提供即清空；COPY 时携带 tagging 头被忽略
aws --endpoint-url $E s3api copy-object --bucket b --key dst --copy-source b/src \
  --metadata-directive REPLACE --tagging-directive REPLACE --tagging "x=9"
```

约束：≤10 条；key 1..=128 / value 0..=256（Unicode 标量）；非法 → 400 `InvalidTag`；
无 versionId 取链尾，链尾 marker → 404，点名 marker → 405；标签不随复制/站点传播。

### 作业引擎与 Batch Operations（S0/S3）

```bash
curl -s $E/api/v1/jobs/1 -H "authorization: Bearer $JWT"      # 详情 + tasks 分页
curl -s -X POST $E/api/v1/jobs/1/pause -H "authorization: Bearer $JWT"
curl -s -X POST $E/api/v1/jobs/1/retry-failed -H "authorization: Bearer $JWT"
```

- 环境：`OSS_JOB_WORKERS`（默认 2，0=关闭）、`OSS_JOBS_INTERVAL_SECS`（2）、
  `OSS_JOB_MAX_ATTEMPTS`（3）、`OSS_JOB_TASK_TIMEOUT_SECS`（900）、
  `OSS_TIER_TASK_TIMEOUT_SECS`（3600）。
- 领取按 `priority DESC, next_at, task_no`；失败指数退避（≤300s），达上限记
  `PermanentFailure`；终态批作业写 `<report_prefix>/<job_id>/results.csv`。
- 单节点 SQLite 多 worker；多进程/多节点不支持（M9）。M4 生命周期**过期删除**仍走
  `sweep_once`，不搬入该引擎。

### 访问点与 Object Lambda（S1/S2）

```bash
curl -s -X POST $E/api/v1/access-points -H "authorization: Bearer $JWT" \
  -H 'content-type: application/json' -d '{"name":"ap-one","bucket":"b","policy":{
    "Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
      "Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}}'
aws --endpoint-url $E s3api get-object --bucket ap-one --key k out   # 实际走 /ap/ap-one/k
```

- 路由：路径式 `/ap/<name>/<key>`；别名 `<name>-s3alias` 同时支持路径首段与
  Host 首标签（`<name>-s3alias.<endpoint>`）。别名仅在该 AP 存在时接管；不存在则回落普通
  桶路由（M8 前创建的 `*-s3alias` 旧桶仍可访问；与 AP 别名冲突时 AP 优先）。
  未知 `/ap/<name>/` 前缀仍 404。标准 SigV4 签名照常覆盖原始 path/host。
  桶名保留 `-s3alias` 后缀（建桶即拒）。
  **升级注意（L2，破坏性）**：对象动作的 AP 策略现在只匹配对象 ARN
  （`arn:aws:s3:::<bucket>/<key>` 或 AP object ARN）。旧策略用桶 ARN（不带 `/*`）授予
  `s3:GetObject` / `s3:PutObject` / `s3:DeleteObject` 的，升级后会从「放行全部对象」变为
  「全部拒绝」（fail-closed）；升级前请改为对象 ARN。
- 鉴权 = 身份/表三档/桶策略/会话策略 **∩** AP 策略（显式 Deny 优先；AP 有策略但无匹配 → 拒）。
  **升级注意（L3，破坏性）**：AP **无策略 = 默认拒绝**（AWS 对齐；升级前为「不额外限制」），
  建 AP 后须显式下发策略，否则该 AP 所有请求 403。
  匿名（无凭据）经 AP 另需**两道门同时放行**：AP 策略显式 Allow `Principal:"*"`（Deny 优先，
  `*` 只匹配匿名、不复用 AK 规则）**且**桶策略匿名基线放行；交集语义，AP 绝不放宽桶策略。
  匿名仅限数据面白名单（见「桶策略与审计」），控制面/桶级动作/对象锁头一律 403，审计 uid=0。
  CopyObject 目标轴按 `s3:PutObject`；审计合并为一条。`DeleteObjects` 逐键叠 AP 策略
  （被拒键 `<Error>AccessDenied`，其余键照删；请求体上限 1 MiB / 1000 键）。
  对象动作只匹配对象 ARN（`arn:aws:s3:::<bucket>/<key>` 与 AP object ARN），桶 ARN 只参与
  桶级动作（与 `policy.rs` 同口径，L2）。
- **授权 op = 执行 op（H1）**：`derive_op` 对查询子资源走严格白名单（未知/多子资源 → 501），
  含 `?legal-hold` → `Get/PutObjectLegalHold`；`x-id` 必须与推导 op 一致（重复/不一致 → 400）；
  `Content-Type: multipart/form-data` 的 POST 一律拒（s3s 会将其解析为 PostObject，绕过查询）；
  重写请求后再次推导 op，不一致即 500 fail-closed。正常 Get/Put/Copy/List/MPU/DeleteObjects 不变。
- AP 策略为严格子集：拒绝未知字段 / 未知条件运算符 / `NotAction` / `NotResource`；
  `Condition` 仅 `IpAddress`/`NotIpAddress` + `aws:SourceIp`（IPv4/IPv6 CIDR，保存时校验）；
  Principal 仅 `*` / AK / `uid:<n>`；非法策略保存 400，运行期加载失败 500（不当作无策略）。
  SourceIp 对端来自连接地址；ecat 内置 listener 无 connect_info，反代部署须显式设置
  `OSS_AP_TRUSTED_PROXY=1` 才采信 `X-Forwarded-For`。信任模型：`OSS_AP_TRUSTED_PROXY_HOPS`
  （默认 1 = 最右一跳，向后兼容）取右起第 N 跳；或 `OSS_AP_TRUSTED_PROXY_CIDRS`（逗号分隔
  CIDR）从右向左跳过可信代理，取第一个非可信地址。仅单行 XFF、每项必须是合法 IP；重复
  IP/重复头行/任一项格式非法 → 视为无对端（fail-closed）。反代须**覆盖**而非透传客户端
  自带的 XFF。**IP 条件无法求值（默认不信任且无对端）时：Deny 语句命中（fail-closed），
  Allow 语句不命中**——缺对端绝不会静默放过 `Deny NotIpAddress`。
- 支持：Get/Head/Put/DeleteObject、DeleteObjects、ListObjectsV2、ListObjectVersions、MPU
  Create/Complete/Abort/UploadPart/UploadPartCopy、CopyObject、Get/Put/DeleteObjectTagging、
  Get/PutObjectRetention、Get/PutObjectLegalHold、Get/PutBucketVersioning、HeadBucket、
  RestoreObject。桶生命周期配置、桶默认加密、Inventory 配置是控制面 / S3 桶级操作，
  **不经 AP**（`/ap/...` 上无对应形状，调用 501）。
- 边界：不做跨账号/VPC 端点/多节点 AP 缓存一致性；经 AP 的 aws-chunked 尾随
  checksum 不转发 trailing headers。AP 无策略 = 拒绝（AWS 对齐，L3 破坏性变更见上）；
  匿名经 AP 需 AP + 桶策略双门（见上）。

### 存储类、分层与 Restore（S4a/S4b）

- PUT/MPU Create/Copy 接受 `x-amz-storage-class`：STANDARD/STANDARD_IA/ONEZONE_IA/
  INTELLIGENT_TIERING/GLACIER/GLACIER_IR/DEEP_ARCHIVE/REDUCED_REDUNDANCY；未知或
  **归档类直写** → 400 `InvalidStorageClass`。HEAD/GET/List/Versions 回读存储类（None=STANDARD）。
- `Version` 增 `storage_class` / `tier`（`backend/key/size/etag/sha256/phase/archived_at/
  restore_expires_at`）；Archived/Restoring 的本地分片视图为空，GET/Range → 403
  `InvalidObjectState`（HEAD 仍可读元数据）；fsck 不报缺片。
- 删除归档版本时 `DeleteOutcome` 带回 `TierRef`，`TierGcSink` 清远端（真实 tier 后端与
  Transition/Restore 在 S4b）。
- 边界：存储类/标签不随 M6/M7 复制传播；归档对象需在同版本二进制内读取。

### Object Lambda（S2，只读变换）

```bash
# 配置变换器（owner/admin）；kind=http，endpoint 由管理员信任
curl -s -X PUT $E/api/v1/access-points/ap-one/transform \
  -H "authorization: Bearer $JWT" -H 'content-type: application/json' \
  -d '{"kind":"http","endpoint":"https://olap.example.com/transform","timeout_ms":5000,"max_response_bytes":67108864}'
aws --endpoint-url $E s3api get-object --bucket ap-one --key k out   # 实际 GET /ap/ap-one/k
```

- ABI：`POST <endpoint>`，body = 原始对象字节；头 `x-oss-bucket/version-id/etag/size/key-b64`；
  响应体即变换后字节，`Content-Type` 可覆盖；`Content-Length`/`ETag` 按变换结果重算。
- 只变换 GET 对象体；HEAD 直通、写操作 405。条件头 304/412 透传；Range → 400；输出全缓冲。
- 限额：输入 `OSS_OLAP_MAX_INPUT_BYTES`（默认 64 MiB）、输出 `max_response_bytes`（默认 64 MiB，
  上限 1 GiB）、`timeout_ms` 1–30s、每进程 16 并发；HTTP 仅 http/https，拒内网/保留地址与
  重定向，每次解析钉扎（SSRF 防线）。
- WASM：`{"kind":"wasm","module_b64":"..."}`（模块 ≤16 MiB，保存即编译）；须以
  `--features olap-wasm` 构建，否则保存 400「未启用」，GET 不会静默直通。
- 边界：无写变换；配置无试跑；变换器故障 → GET 500（不返回原文）。

### Batch Operations（S3）

```bash
# 1) 清单：对象桶内 CSV，表头 bucket,key[,versionId]（BOM/CRLF 容忍）
# 2) 建作业（JWT）；operation = CopyObject|DeleteObject|ReplaceTags|RestoreObject|InvokeLambda
curl -s -X POST $E/api/v1/jobs \
  -H "authorization: Bearer $JWT" -H 'content-type: application/json' -d '{
    "kind":"batch","manifest_bucket":"b","manifest_key":"manifest.csv",
    "operation":"ReplaceTags","tags":{"env":"prod"},
    "report_bucket":"reports","report_prefix":"batch/run1","priority":0}'
# → 201 {"job_id":1,"total":1001}
curl -s $E/api/v1/jobs/1 -H "authorization: Bearer $JWT"   # {"job":{state,total,succeeded,failed,…},"tasks":[…]}
curl -s -X POST $E/api/v1/jobs/1/retry-failed -H "authorization: Bearer $JWT"  # → {"job_id":2}
```

- 任务动作：Copy 按 Copy 语义（目标桶/前缀、REPLACE 元数据/标签）；Delete 清 tier refs；
  ReplaceTags 整体替换标签；Restore 走 S4b 恢复；InvokeLambda 支持 HTTP 变换器，
  构建开启 `olap-wasm` 时也支持 WASM（输出丢弃）。
- 配额：`OSS_JOB_MAX_INFLIGHT_PER_USER`（100）/`OSS_JOB_MAX_INFLIGHT_GLOBAL`（1000）；
  创建期逐行授权（去重），清单/任务上限见错误文案。
- 报告：`<report_prefix>/<job_id>/results.csv`，固定列
  `taskId,bucket,key,versionId,resultCode,resultString,startTime,endTime`；仅已启动任务入报告；
  `Succeeded`/`PermanentFailure`；时间 `%Y-%m-%dT%H:%M:%SZ`。
- `retry-failed` 建子作业（`parent_job_id`，仅失败任务），父报告不被改写；无失败项 → 409。
- 边界：无抢占、无跨账号清单；作业终态 `done`/`error`/`cancelled`（有失败即 `error`）。

### Inventory（S5）

```bash
curl -s -X PUT $E/api/v1/buckets/b/inventory -H "authorization: Bearer $JWT" \
  -H 'content-type: application/json' -d '{
    "id":"inv1","is_enabled":true,"frequency":"Daily",
    "destination_bucket":"reports","destination_prefix":"inv","format":"CSV",
    "filter_prefix":"","included_object_versions":"All",
    "optional_fields":["ReplicationStatus","EncryptionStatus"]}'
curl -s -X POST $E/api/v1/buckets/b/inventory/inv1/run -H "authorization: Bearer $JWT"
# → 202 {"run_key":"manual-<epoch>","job_id":3}
aws --endpoint-url $E s3 cp \
  s3://reports/inv/b/inv1/manual-<epoch>/manifest.json - | head
```

- 调度：Daily（UTC 日期）/Weekly（ISO 周），worker `OSS_INVENTORY_INTERVAL_SECS`（默认 60，
  0 关闭）；同槽唯一去重，停机只补最近一槽；force-run 供测试/运维（owner/admin）。
- 输出：`<dest_prefix>/<source_bucket>/<config_id>/<run_key>/manifest.json` +
  `data/<uuid>.csv.gz`（CSV）或 `data/<uuid>.parquet`（Parquet）；manifest 含
  `sourceBucket/destinationBucket/version/fileFormat/fileSchema/files[]`，`files[]` 的
  `size`/`md5Checksum` 是落盘文件字节数与 MD5（base64，对齐 AWS v1）。
- Parquet：无 Arrow 依赖的低层 writer，SNAPPY 压缩、8192 行/行组；列名用 AWS Inventory 的
  lower_snake_case（`bucket,key,version_id,is_latest,is_delete_marker,size,last_modified_date,
  e_tag,storage_class,...`），`last_modified_date` 为 TIMESTAMP_MILLIS；`fileSchema` 是
  Hive/Athena 可读的 Parquet schema 文本。
- Hive 清单：每次运行同写 `<run_key>/hive/dt=YYYY-MM-DD-HH-MM/symlink.txt`（UTC），内容为
  一行 `s3://<dest>/<data_key>`，供 Athena SymlinkTextInputFormat 读取；与数据/manifest 同样
  套用目标桶默认加密。CSV 与 Parquet 共用该清单。
- 列：必含 9 列 `Bucket,Key,VersionId,IsLatest,DeleteMarker,Size,LastModifiedDate,ETag,StorageClass`；
  可选 6 列按固定顺序追加 `ReplicationStatus,EncryptionStatus,ObjectLockRetainUntilDate,
  ObjectLockMode,ObjectLockLegalHoldStatus,ChecksumAlgorithm`。
- 边界：全量扫描、单数据文件（≤5 GiB）、CSV/Parquet；不做跨账号/增量；
  `IsMultipartUploaded`/`IntelligentTieringAccessTier` 不支持；可选列可能逐版本 head。

### 分层 Transition 与 Restore（S4b）

```toml
# oss.toml：远端 S3 兼容 tier（kind 缺省 = s3）
[[tiers]]
id = "t1"
kind = "s3"
endpoint = "http://127.0.0.1:40125"   # http/https
bucket = "cold"
access_key = "TIERAK"
secret_key = "TIERSK"
region = "us-east-1"

# 本地冷盘池：落盘 <path>/<tier_id>/<bucket>/<version_id>/<raw key>；
# 同目录临时文件 + fsync + rename，路径段拒绝绝对路径/`..`/NUL。
[[tiers]]
id = "local1"
kind = "local"
path = "/var/lib/oss/cold"
```

```bash
# 生命周期转 GLACIER（days=0 = 下一轮 sweep 立即满足；tier 由写入时解析并持久化）
aws --endpoint-url $E s3api put-bucket-lifecycle-configuration --bucket b \
  --lifecycle-configuration '{"Rules":[{"ID":"cold","Status":"Enabled","Filter":{"Prefix":""},
    "Transitions":[{"Days":0,"StorageClass":"GLACIER"}]}]}'
aws --endpoint-url $E s3api get-bucket-lifecycle-configuration --bucket b   # 回读往返
curl -s -X POST $E/api/v1/tiers/sweep -H "authorization: Bearer $JWT"      # {"queued":n}
curl -s $E/api/v1/tiers -H "authorization: Bearer $JWT"                    # [{id,kind,path,endpoint,bucket,healthy}]
# 远端 GC/对账：默认 dry-run；真删必须显式 dry_run=false
curl -s -X POST $E/api/v1/tiers/gc -H "authorization: Bearer $JWT" \
  -H 'content-type: application/json' -d '{"dry_run":true,"grace_days":7}'
curl -s -X POST $E/api/v1/tiers/gc -H "authorization: Bearer $JWT" \
  -H 'content-type: application/json' -d '{"dry_run":false,"tier":"t1","grace_days":7,"max_deletes":1000}'

aws --endpoint-url $E s3api head-object --bucket b --key k                 # StorageClass=GLACIER
aws --endpoint-url $E s3api get-object --bucket b --key k out              # 403 InvalidObjectState
curl -si --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" \
  -X POST "$E/b/k?restore" -H 'x-amz-restore-request: days=1'              # 202 + ongoing-request="true"（见下）
aws --endpoint-url $E s3api get-object --bucket b --key k out              # 恢复完成后可读
aws --endpoint-url $E s3api delete-bucket-lifecycle --bucket b
```

- 生命周期规则也可经控制台 `GET/PUT /api/v1/buckets/{b}/lifecycle` 配置
  （`transition_days/transition_class/transition_tier`）；S3 与 console 共用同一校验，PUT 全量替换。
- Transition 由 `OSS_TIER_SWEEP_INTERVAL_SECS`（默认 3600）周期扫描 + 管理端 force-sweep；
  按对象去重，仅当前非加密非分层版本，单次 ≤10000 对象；全内存搬运（单对象 ≤`OSS_TIER_MAX_BYTES`，默认 1 GiB）。
- Restore：body XML `<RestoreRequest><Days>n</Days></RestoreRequest>` 或
  `x-amz-restore-request: days=n[,tier=]`，days 1..=36500（缺省 1）；返回
  HTTP 202 + `x-amz-restore: ongoing-request="true"`；HEAD 三态（Archived 无头 / Restoring /
  Restored `ongoing-request="false", expiry-date=…`）。
  s3s 0.17 的 RestoreObject 生成代码固定按 200 OK 序列化并丢弃 `S3Response.status`；
  router 层中间件把 POST `?restore` 的 200 改写为 202（错误状态与其它请求原样透传）。
- 到期回收：reaper `OSS_TIER_REAPER_INTERVAL_SECS`（默认 300），精度约 300s；过期后回到
  Archived（GET 再次 403）。启动时对账 pending/restored 行与 Version。
- 本地冷盘池（`kind = "local"`）：`put` 写同目录 `.<name>.tmp-<uuid>` + `fsync` + `rename`，
  校验 MD5（ETag 为 32 位 hex 时）；`head` 在根目录缺失时返回错误（控制台 health 为 false）；
  启动后可直接读回已落盘对象，并清理同目录崩溃残留；`delete` 自底向上删空目录。
- 远端 GC/对账（`POST /api/v1/tiers/gc`，仅管理员）：先列远端、再全量扫本地版本 meta；
  任一 list/扫描失败整轮中止且不删除；live `TierRef` 永不删除；只删 `<tier_id>/` 前缀内、
  年龄 ≥ `grace_days`（默认 7）的对象；每轮最多 `max_deletes`（默认 1000）；删除失败逐条返回
  并保留待下轮；`missing_live` 只提示 live 引用在远端缺失，不写任何数据。`dry_run` 默认 true。
- 边界：tier 支持远端 S3 与本地冷盘池；全内存搬运；无自动定时 GC（须管理员显式触发）；
  远端删除仍 best-effort，失败留孤儿待 GC 重试；SSE 对象不 Transition；归档对象复制/站点
  复制不可用（先 Restore）。

### 控制台页面（S6）

- **访问点**：AP CRUD、严格策略编辑、HTTP/WASM 变换器配置与回读；Alias 列（派生
  `<name>-s3alias`，可复制）；策略支持 DeleteObjects 与 SourceIp 条件。
- **作业**：Batch 创建、进度、任务分页、暂停/恢复/取消、retry-failed；详情展示报告桶/前缀
  （报告对象在对象浏览器查看）。
- **存储层**：`[[tiers]]` 只读状态（health，含 `kind`/`path`）与 force-sweep；远端 GC/对账
  （默认 dry-run）。
- **桶页 / 对象页**：Inventory 配置弹窗；对象标签编辑、存储类、恢复状态与 Restore 按钮、
  retention / legal-hold 列；桶默认加密；桶策略模板；生命周期完整 Filter 编辑器。
- **审计 / 设置 / 事件 / Select**：审计过滤；SSE/KMS 只读状态；事件通知 CRUD + 管理员死信
  查看/重投；一次性 Select 代理工作台。
- 管理员菜单按 `is_admin` 门控；M8 页面 Playwright 用例 `web/e2e/m8-console.spec.ts`。

### M8 已知边界（在案）

1. 控制面单节点：AP 缓存、作业引擎、Inventory 调度、tier reaper 均按单进程；多节点共享
   SQLite 不支持。IAM/桶/策略等 11 张控制表在 `[site]+[raft]` 模式下由 M9 Raft 强一致；
   其余 M8 状态仍单节点。
2. AP 支持 `/ap/<name>` 与 `<name>-s3alias`（路径/Host）；匿名经 AP 需 AP 策略
   `Principal:"*"` 与桶策略匿名基线同时放行；无跨账号/VPC 端点；不支持
   ListBuckets/Create/DeleteBucket。AP 无策略 = 拒绝（AWS 对齐，L3）；无 AP 的
   `*-s3alias` 桶回落普通桶路由（L4 已修复，见访问点节）。
3. Object Lambda 只读 GET；WASM 默认关闭；输出全缓冲；endpoint 由管理员信任（仍有内网 SSRF 拒绝）。
4. Batch 无抢占/跨账号；InvokeLambda 支持 HTTP（`olap-wasm` 构建另支持 WASM）；
   动作不发桶通知但发复制/清 tier。
5. Inventory 全量扫描、CSV/Parquet 单数据文件；不做跨账号/增量；Hive 兼容面仅
   `symlink.txt`（不含 Athena 建表/分区自动发现）。
6. 分层支持远端 S3 与本地冷盘池、全内存搬运；S3 生命周期已支持 tag/size/And Filter、
   Expiration.Date/ExpiredObjectDeleteMarker、Transition.Date、多转换、noncurrent expiration/transition；
   仍不支持 AbortIncompleteMultipartUpload（需 MPU 索引，501）。
7. 归档对象与同版本二进制绑定；启用 Transition 前须全量升级。tier 远端删除 best-effort；
   GC/对账仅管理员显式触发（默认 dry-run），无自动定时 GC；恢复到期精度约 300s。
8. 作业状态库内 `done/error`（映射 Completed/Failed）；取消时未执行任务不进报告；
   CSV 不做公式转义。
9. RestoreObject 的 202 由 router 层中间件补写（s3s 0.17 生成代码丢弃
   `S3Response.status`）；识别口径为 POST + `restore` 查询子资源。

## M9 强一致（S0–S7 已落地）

**能力状态（2026-10-23，main）**：`crates/oss-server/src/raft/mod.rs` 的
`RAFT_READY=true`、`SYNC_READY=true`、`STRETCH_READY=true` 决定运行时门控（能力位还要求
对应配置存在）。`GET /api/v1/raft/status` 报真实状态（`enabled/node_id/role/leader/term/
applied/lag/stale/snapshot/purged/voters/learners/production_ready`，以及 Level B 的
`wire_version` / `feature_level` / `activation_level` / `effective_feature_level` 与
`compat.{min_wire_version,min_membership_wire_version,peers,mixed,blocked_reason}`；`voters<3`
时 `production_ready=false`）。`lag` = `last_log_index - last_applied.index`，即本机已追加、
尚未 apply 的日志条数（无值按 0；**不是**与 leader 的差距，未收到的日志不计）；
`stale` = 本节点非 leader 且 `lag>0`，只说明本机有未 apply 的已追加日志。follower 的读是
本机已 apply 状态，**不是**线性一致读；分区/断连时 `lag` 仍可能为 0、
**`stale=false` 不代表已追上 leader**，不能作为新鲜度保证。`/api/v1/site/raft/*` 提供
append/vote/snapshot/status/membership/write/join。

- **已落地**：S0 模式矩阵/配置校验/能力开关；S1–S3 sync 数据面（统一 origin version id、
  写门与写 intent、MPU/Copy 边界、remote-first 删除、内部 token 路由、删桶 quiesce、删除
  intent 恢复）；S4 stretch 2×6 数据面；S5 openraft 0.9.25 SQLite 日志/状态机/快照/HTTP
  网络；S6 控制写路由（11 张控制表经 `ControlPlane` 单命令提交、确定性 payload、apply 后
  缓存/目录副作用、`/site/changelog` 409、启动模式断言）；S7 3-voter 成员变更/leader 单跳
  转发、双边配对与 B′ 替换、witness 独立运行时、11 表逻辑快照/安装、M7→Raft
  `raft-cutover`/`raft-export`/`raft-force-recover`、`sync-reconcile [--repair]`、console
  Raft 集群卡片。M7/Local 路径保持字节不变。
- **验收证据（v1.13.1 发布，代码 HEAD `ac007e7`，2026-10-22）**：`M9_STRICT=1 tests/e2e_m9.sh`
  （sync）78 PASS / 0 FAIL / 1 预期 SKIP；`M9_STRICT=1 M9_STRETCH=1 tests/e2e_m9.sh`
  （stretch+Raft 配对/witness）59 PASS / 0 FAIL / 1 预期 SKIP；`tests/e2e_m9_recovery.sh`
  133 PASS / 0 FAIL / 1 预期 SKIP；M7 回归 `tests/e2e_sites.sh` PASS。Rust 聚焦测试
  164 PASS（lib 112 + raft_access 6 + raft_s71 6 + raft_cutover 4 + sync_gate 36）；
  `cargo fmt --all --check` 与
  `cargo clippy -p oss-server -p xl-s3 --all-targets -- -D warnings` 通过。
  2026-10-23 能力清理合并的聚焦证据见
  `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md`：xl-store 259+1 ignored /
  xl-s3 136 / oss-server 140，S3 e2e 38 节；本批未重跑全量 M9 e2e。
  `tests/e2e_stretch.sh` ALL PASS；本次无前端改动，web test/build 沿用发布时结果。

### 模式矩阵

| 配置 | 数据面 | 控制面 | 当前构建 |
|---|---|---|---|
| 无 `[site]/[cluster]/[stretch]` | 单节点（单盘或本地多盘 EC） | 本地 SQLite | 可用（M1/M2） |
| `[site]` | M7 异步站点对 | 本地 SQLite | 可用（M7） |
| `[cluster]` | 扁平多节点 EC | 本地 SQLite | 可用（M3/M4） |
| `[site]` + `[raft]` | M7 异步站点对；sync 桶（1 数据 peer） | Raft 1/2/3 voters（A/B 配对 + C witness） | 可用；控制写经 Raft；`production_ready` 需 ≥3 voters |
| `[site]` + `[stretch]` + `[raft]` | 2×6 stretch EC | Raft（A/B 配对；witness 可选） | 数据面与 stretch+Raft 组合 e2e ALL PASS（`M9_STRETCH=1 tests/e2e_m9.sh` 59 PASS） |

启动校验（`config.rs`，均 fail-closed）：`[raft]` 必须有 `[site]`；`[stretch]` 必须有
`[site]` + `[raft]`；`[cluster]` 与 `[site]`/`[stretch]` 互斥；`[raft]`/`[stretch]` 下
`[[groups]]` 必须为空；stretch 还要求恰好 2 个逻辑站点、本站 6 盘、对端
`peer_drives=6`、EC 8+4、`inline_threshold=0`、`placement="v2"`、非空 `mesh_secret`、
`self_addr≠peer_addr` 且 `self_addr` 与主 HTTP/`[raft].advertise_endpoint` 端口分离。

### witness 部署

生产形态为 **3 voters：A/B 两个数据站点 + C witness**，quorum 2；任意两票可提交，
A+B、A+C、B+C 均可写。C 只持久化 `raft.db` + 控制面 `state_db`（11 张控制表），
**不建 Store、不提供 S3/console/对象 worker、不存对象数据、不在 `sites` 表**；sync 门控的
“恰好 1 个数据 peer”按 `sites` 行数计，不受 C 影响。C 通过快照追平控制状态。

| 节点 | Raft `node_id` | 角色 | 关键配置/职责 |
|---|---|---|---|
| A（数据站点） | 1 | voter，`bootstrap=true` | `[site]`+`[raft]`；写对象；`advertise_endpoint=http://a:9000` |
| B（数据站点） | 2 | voter，`bootstrap=false` | `[site]`+`[raft]`；写对象；等 leader/配对加入 |
| C（witness） | 3 | witness，`bootstrap=false` | 独立 `witness.toml`：`node_id/advertise_endpoint/cluster_token/db_path/state_db`；仅控制面 |

启动路径为 `oss-server witness --config witness.toml`（`WitnessToml` + `Db::connect_witness`，
不构造 Store/S3；HTTP 只暴露 `/health*` 与 `/api/v1/site/raft/*`）。成员流程：A 初始化
`{A}` → 控制台 `POST /api/v1/sites` 配对 B（A 侧 `add_learner(B, blocking)` 后
`change_membership({A,B})`，B 侧 `/join` 写本地 `sites` 行）→
`raft-add-witness --config a/oss.toml --node-id 3 --endpoint http://c:PORT` 让 C 追加快照并加入
第 3 voter；成员集以 `metrics().membership_config` 为准，不硬编码 `{A,B,C}`。C 可当选 leader，
follower 的成员变更/控制写经 `x-oss-raft-forwarded` 单跳转发到 leader。

### sync 写语义（S1–S3 已实现；S6 起能力门开启）

生产配置（A/B 配对为 2 voters；加 witness C 后 3 voters）：

```toml
[site]
name = "A"
region = "r1"
token = "<32–128 位十六进制>"

[raft]
node_id = 1
role = "voter"
bootstrap = true
feature_level = 1                      # Level B 滚动升级；缺省 1，范围 1..=2（见 runbook）
advertise_endpoint = "http://a:9000"   # 主 HTTP；不是 mesh 端口
cluster_token = "<共享集群令牌>"
db_path = "/var/lib/oss/raft.db"       # Raft 运行时要求显式路径；当前配置加载未强制（RaftToml.db_path 可选）

[sync]
max_object_bytes = 67108864            # 默认 64 MiB；staging 前超出 → 400
timeout_ms = 10000                     # 默认 10s；仅 sync 远端调用 → 超时 503
```

- **创建前提（S6 起）**：sync 桶要求 `[raft]` + `RAFT_READY` + 恰好 1 个数据 peer；
  创建即 `versioning=Enabled`，`replication_mode` 创建后不可变。`SYNC_READY=true`：
  S3 面 `x-oss-replication-mode: sync` 与控制台建桶均经 Raft 提交（单节点或 3-voter 均可）。
  sync 桶拒绝对象锁（409）、SSE-C（400）、Tagging API（501）与
  生命周期 Transition（400，仅允许 Expiration）；`Suspended` 版本控制在 sync 桶被拒
  （`InvalidBucketState`）。
- **同一 origin version id**：提交前预生成本地版本 UUID；远端 token PUT 携带签名头
  `x-oss-origin-version-id`。接收端在 sync 桶 + 合法 `x-oss-site-token` 下采用该 UUID 作为
  `Version.version_id`，并写 `Version.remote_version_id=Some(uuid)`；响应
  `x-amz-version-id` 必须等于该 UUID，否则 502 `sync version id mismatch`、本地不提交。
  接收端同 key/version id/size/ETag 重放幂等返回既有版本，不同则 409。非 token 的
  M6/M7 `REPLICA` 摄入保持旧行为，不采用此规则。
- **remote-first 写序（PUT/MPU Complete/Copy）**：本地 staging 不可见 → 拼接/暂存时写明文
  tee（`<data_dir>/.oss.sys/tee/<uuid>.plain`，权限 0600）→ 写 `sync_write_intents`
  （`state=remote_pending`，`PRAGMA synchronous=FULL`）→ 远端 token PUT（要求 2xx 且同 id）
  → 本地 meta 提交 → 清理 intent + tee。远端不可达/超时/5xx → 503（远端确定性 4xx
  保留 400/409 语义），本地无 meta；本地提交失败 → token DELETE 同 id 补偿；
  kill -9 落在远端 ack 与本地 meta 之间 → 恢复只做**补偿**
  （本地 meta 不存在则删远端同 id，存在则只清 intent/tee），**绝不从 tee 续提交**；远端暂时
  不可达时 intent 保留重试。MPU tee 在 Complete 拼接 part 流时一次写出，tee 缺失 → 500；
  tee 不能重建 MPU ETag/元数据。无 intent 的孤儿 tee 超过 **1 小时**由启动/周期清理删除，
  有 intent 的交给恢复 worker（启动先跑一轮，之后 30s 周期；只处理超过
  `timeout + 60s` 的行，避免误伤在途）。
- 元数据复制 content-type/user-metadata/标签/storage-class；PUT/MPU 的 checksum 远端重算，
  CopyObject 继承源对象 checksum（`x-amz-checksum-*`）并随 sync 门 PUT 透传且进 SigV4
  SignedHeaders，源对象无 checksum 时目标也不伪造；A/B 两侧 HEAD 校验和一致。
  SSE-S3/KMS 由目标本地决定。默认 64 MiB / 10s 要求约 ≥52 Mbps 上行，WAN 带宽不足时须下调
  `max_object_bytes` 或提高 `timeout_ms`；默认面向小对象/合规桶。

### sync 删除语义（S3 已实现）

- **唯一入口**：S3 `DeleteObject`/`DeleteObjects`、生命周期 Expiration、Batch Delete、
  控制台对象/桶删除统一经 `SiteSyncDelete`；sync 桶 remote-first（先远端 ack，再本地），
  async/M7 桶保持原 store 路径。
- **内部 token 路由**（JWT-exempt，`x-oss-site-token` 常量时间比较，缺/错 403；只做本地
  操作、永不转发）：`POST /api/v1/site/delete-object`（版本/`marker_id`/`all_versions`）、
  `HEAD /api/v1/site/head-object`（200 存在 / 405 delete marker / 404 不存在）、
  `GET /api/v1/site/bucket-empty`、`POST /api/v1/site/bucket-quiesce`、
  `POST /api/v1/site/delete-bucket`。普通 S3 `DELETE` 永不携带该 token，始终走客户端
  remote-first；代理剥离 token 只会让内部路由 403，不会降级成客户端路径。
- **点名版本**：本地版本 id 即远端 id；内部路由对不存在版本幂等返回 204。若 M9 前的对象
  没有 `remote_version_id` 或映射不一致 → 409，文案提示运行
  `sync-reconcile --repair`（见下）。
- **DeleteObject（无 versionId）**：预生成 marker UUID，远端先创建同 id marker 再本地创建；
  重试契约 **at-least-once**：对象不可见性幂等，但 `ListObjectVersions` 可能出现多个
  delete marker。
- **DeleteObjects**：逐键 remote-first；远端不可达/超时/5xx → **整体 503**（成功的键
  已经提交，客户端不会收到逐键的 200+Error 结果），指标
  `oss_sync_delete_partial_total` 递增；远端确定性 4xx（400/409）保留为逐键 `Errors`。
  “幂等”仅指对象不可见/同 id 版本删除，不指 marker 列表逐项相同。
- **删桶**：置 `deleting=1` → 等本站 active writer 归零（30s 上限）→ 远端
  `bucket-quiesce`（远端同样 30s 等自己的 writer）→ 两端 `bucket-empty` → 远端
  `delete-bucket` → 本地删。非空/超时 → 回滚 deleting + 409/503；失败保留 deleting 与
  `delete_bucket` intent 周期重试。删除中的 PUT/MPU Complete/Copy/Delete → 503
  （MPU initiate/UploadPart 不在拦截范围）。
- **删除 intent 恢复**：`sync_delete_intents` 每操作一行 UUID，远端调用前 pending、ack 后
  done、本地成功后删行；30s worker + 启动首轮。`remote_pending` 用 HEAD 三态判断：
  200/405 → 远端仍存在，重试内部删除；404 → 已不存在，本地删；`remote_done` 直接补本地。
- **`sync-reconcile [--repair]`**：只读模式报告缺映射/远端缺对象；`--repair` 对可读明文
  （≤ `[sync].max_object_bytes`）按本地 version id 重推、刷新 `remote_version_id`、清理
  旧站 `sync_*_intents` 与 `repl_outbox(target_kind='site')`，全部成功后才清
  `sites.reconcile_required`（B′ 替换前该标志令 sync 写/删一律 503）。加密/超大/冲突对象
  报错并保持 gate，退出码 1。只读模式只报告（`markers_missing` 为远端缺失 marker 数）。
  `--bucket B` 仅限定扫描/诊断，**不清**站点级 gate/intents/outbox；清门需全库运行。
  用法：`oss-server sync-reconcile [--repair] [--bucket B] [--json] --config a.toml`
  （离线 `Db::connect_offline` + `Store::open`，不加载 AppState/不打开 raft.db；建议停服避免
  对象目录并发写）。

### stretch 2×6（数据面 + Raft 控制面已接线）

目标配置（两数据站点各一份，C witness 另配；省略 `listen`/`data_dir(s)`/`database_url`
等通用项，本站必须恰好 6 条 `data_dirs`）：

```toml
[site]
name = "A"
region = "r1"
token = "<64 hex>"

[stretch]
node_id = "A"
self_addr = "a:9100"          # 本站 mesh gRPC；独立于主 HTTP
peer_node_id = "B"
peer_addr = "b:9100"
peer_drives = 6
placement = "v2"
mesh_secret = "<A/B 共享>"
mesh_timeout_ms = 5000

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 0          # stretch 强制 0

[raft]
node_id = 1                   # A=1,B=2,C=3（目标 3-voter）
role = "voter"
bootstrap = true
advertise_endpoint = "http://a:9000"
cluster_token = "<共享集群令牌>"
db_path = "/var/lib/oss/raft.db"
```

- **拓扑**：恰好 2 个数据站点 × 6 盘 = 12 槽，EC 8+4，`placement="v2"`；每站点恰 6 个
  分片，meta 镜像 12 份。`Q_s=data+1=9`；单站点 6 槽 < 9，**读写都必须两站点齐全**，
  单站点故障返回非 2xx（500/503），**不承诺站点容灾**，也不存在“单站点降级读”。
- **数据路径**：`self_addr` 是真实的 `xl_client::serve` mesh gRPC 监听（不是测试 harness），
  与主 HTTP/`advertise_endpoint` 必须不同端口；`peer_addr` 为对端 mesh。跨站每 64 KiB 块
  对每个远端分片一次 RPC，WAN 延迟/带宽直接限制吞吐，只适合小对象/合规对象；可用
  `mesh_timeout_ms` 调整。
- **inline=0**：对象字节只落分片、不进 `xl.meta`；meta 仍含 key/标签/user-metadata/
  content-type。
- **关闭 M7 异步站点复制**：`target_kind='site'` 的 `repl_outbox` 必须恒为 0，任何一次
  stretch 启动发现遗留行即拒绝启动；M6 规则复制（外部 remote）仍可用。stretch 下创建
  sync 桶 400；首次启用还要求全新空库（`buckets=0`、各 `data_dirs` 只有 `.oss.sys`、
  无 sync 桶），通过后写 `stretch_initialized`，之后正常重启不再要求空库；**不支持原地
  迁移**。`[stretch]` 与 `[cluster]`/`[[groups]]` 互斥。未配对（本地 `buckets` 行数为 0）
  的 Raft 启动会**跳过桶目录对账**，避免把对端站点拥有的目录当孤儿隔离；完成配对后正常
  重启即执行完整对账。
- **单盘故障**：EC 8+4 可在站点内重建，但 `Q_s=9` 仍要求至少 9 槽可达；站点恢复后用
  `fsck --repair`/`heal` 通过 mesh 修复缺失分片。

### cutover / force recovery runbook

硬约束：**禁止手改旧 `raft.db`**，新集群绝不写旧 epoch。

**M7 → Raft cutover**（两台 M7 库切到控制面 Raft）：

1. 停 A/B；`oss-server raft-export --config a/oss.toml --out /backup/export.json`（11 张复制表，
   自然键；`--witness` 仅导出 witness 的 `state_db`（只含控制面 11 表，不含对象）；
   该导出不能用于 cutover；可用于 force recovery，对象数据仍从备份恢复）。
2. 对每个 M7 目标库先 dry-run：`raft-cutover --config x/oss.toml --from export.json --dry-run`；
   无冲突后去掉 `--dry-run`。冲突按 **11 张表自然键逐字段**报告
   （CLI 每行 `conflict <table> <key> <field>`，同一行多个字段不同即多条；
   `Diff` 另含 source/target，但当前 CLI 不打印），而非整行摘要。
   目标必须是 **未进入 raft mode** 的 M7 库、版本/admin/vault
   与导出一致、`[cluster]` 不存在、site changelog lag=0、`site_apply_dead` 为空；
   `--overwrite` 才允许覆盖目标独有复制行。cutover 成功后写 `raft_cutover` 并置
   `raft_mode=on`（此后无 `[raft]` 的启动会被拒绝）。
3. 启动 A（`bootstrap=true`）、B（`false`），控制台配对，`raft-add-witness` 加 C。

**force recovery**（A+B 永久丢失，只剩导出/备份）：

1. 准备一个**干净目标控制库**：同版本、已 `raft-cutover` 到 raft mode、`raft_applied`
   为空、无 `raft_snapshot` 行、存在本地 admin（**witness 目标例外**：witness 无本地
   管理员也可恢复，安装属主写哨兵 `0`）；`raft.db` 路径必须全新/空。
2. `oss-server raft-force-recover --config survivor/oss.toml --from export.json --node-id N --yes`
   ——导入 11 表并在空日志上 `initialize({N})`；不会重置/覆盖既有 `raft_applied`，非空即拒绝。
   `--config` 也可指向 witness 的 `witness.toml`（`[raft].state_db` 经
   `Db::connect_witness` 打开；数据站点配置优先），恢复后同样以 `bootstrap=false` 启动。
3. 以 `bootstrap=false` 启动该节点（身份校验会比对 `raft_bootstrap=0`，不再 initialize）；
   其他站点恢复后按配对/learner 流程加入。
4. 对象从备份/异地副本恢复；sync 桶对端恢复后跑 `sync-reconcile --repair`；stretch 两站点
   数据都必须从备份恢复。未提交写会丢失。
5. 单点永久丢失：**C 丢失** → A+B 仍 quorum 2，部署新 witness（新 node_id = 当前最大 + 1）
   加 learner 追平后替换；**B 永久丢失且有 sync 桶** → A+C 仍 quorum 2，复用旧 B 的 site
   UUID/token、新 Raft node_id 部署 B'，`sync-reconcile --repair` 完成前不得标可服务
   （有 sync 桶时 `DELETE /sites` 应 409）；**A+B 同时丢失** → 按上述 force recovery。
6. **升级顺序（M9 控制面 Level B 滚动；数据面仍非滚动）**：Raft RPC 携带并校验
   `x-oss-raft-target-node-id`（防止旧 endpoint 上的新 node_id 误答旧成员请求），
   v1.13.1 起为滚动基线；本版本新增 `x-oss-raft-wire-version` /
   `x-oss-raft-feature-level` 握手（缺失/非法 = 1）。步骤如下：
   - **二进制滚动（feature level 1）**：逐台重启，**先 follower/learner、最后 leader**；
     每台重启后在 `/api/v1/site/raft/status` 等到 `applied` 追平再动下一台。此阶段
     保持 `[raft].feature_level=1`（缺省，旧配置无需改动），行为与旧版一致。
   - **启用 level 2**：全部 voter/learner 都已是新二进制（`wire_version=2`）后，逐台把
     `[raft].feature_level=2` 写入配置并重启。leader 的 `effective_feature_level` 只在
     所有当前 voter **和 learner** 都在当前 term 宣告 2 时才是 2；否则 level-2 写入返回
     503 `{"error":"feature_unavailable","required":N,"effective":M}`。
   - **激活 = 不归点**：首条 level-2 entry 提交时，同一 apply 事务写
     `schema_meta.raft_feature_level=2`（只升不降）。之后回滚必须换回支持该级别的二进制；
     marker 高于配置或二进制上限时启动直接拒绝。**回滚只在激活前可行**：停集群、换回旧
     二进制、删除 `feature_level` 键（旧 `RaftToml` 拒绝未知键）、启动。
   - **本版本限制**：发布二进制没有产品级 level-2 命令（仅进程内 `FeatureProbe` 测试覆盖），
     因此写入 `feature_level=2` 不会产生 level-2 entry、也不会激活；激活路径留待首个
     level-2 命令落地。`tests/e2e_m9_rolling.sh` 已提供但本批未实跑（缺 v1.13.1 旧二进制）。
   - **窗口内禁止成员变更**：新 leader 在 add/promote/replace 前预检——现有 voter/learner
     任一 `wire<2`、目标未达标或不可达 → 409 `peer_incompatible`；不可达的旧节点只能被
     移除/替换。console 配对（`/sites`、`/join`）同样要求对端 identity 带 wire 2 且
     feature 不低于本机（旧节点缺省 = 1 → 409）。
   - **不在 Level B 范围内**：M3/M4 数据面（分片 RPC/EC/放置）、TLS 物料、从 < v1.13.1
     跨大版本、Level C 双向混合写、快照 OSSCTL2（本版本无复制结构变更，快照仍为
     OSSCTL1）。
   - 全流程演练：`tests/e2e_m9_rolling.sh`（需 `OSS_OLD_BIN` = v1.13.1；未提供时 SKIP）。

### 部署与安全边界

- Raft/site/mesh 传输默认明文；生产部署应配置 `[tls.server]`（见 TLS 节）启用全链路
  TLS/mTLS。未启用时只能部署在受控私网或外部 TLS 终结/隧道；`cluster_token`、`site.token`、
  `mesh_secret` 不得出现在公网。
- 能力开关已翻（`RAFT_READY`/`SYNC_READY`/`STRETCH_READY=true`）。控制面支持 1–3 voters：
  A/B 配对后 2 voters，`raft-add-witness` 加 C 后 3 voters（quorum 2，`production_ready=true`）；
  单节点 bootstrap 仍可用于开发。`tests/e2e_m9.sh`（3 进程）与 `tests/e2e_stretch.sh`
  已在本机通过；上线前仍应在目标网络重跑并保留证据。

## 验收



覆盖范围：S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）见下方脚本；控制台的登录/上传/秒传/密钥常规路径见
「控制台（M1b）」的 Playwright e2e；**1 GB 上传与断线续传为人工验证项**（指引同 M1b 段）。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
# M5 全量（步骤 18）：服务须以密钥库启动（OSS_MASTER_KEY_FILE/OSS_KMS_DIR），可选：
#   E2E_DATA_DIR=<data_dir>  盘上明文特征 grep（含明文对照键的判别性断言）
#   E2E_DB=<sqlite 路径>     SK 落库密文前缀（enc:v1:）断言
```

M6 全量（§19：Select 对当前 ENDPOINT + Kafka（docker redpanda）+ 复制双实例）：

```bash
cargo build -p oss-server && AK=... SK=... ENDPOINT=... tests/e2e_s3.sh
tests/e2e_replication.sh        # 仅复制：自动起源/目标两实例（约 1–2 分钟）
tests/e2e_kafka.sh              # 仅 Kafka 三阶段（redpanda docker；约 1 分钟）
tests/e2e_sites.sh              # M7 多站点：自动起 A/B 两实例并双向配对（约 2–4 分钟）
```

M8 测试锚：

```bash
cargo test -p xl-store --lib            # 标签/storage_class/tier phase/DeleteOutcome tier refs
cargo test -p xl-s3 --lib               # AP/OLAP/restore/lifecycle/inventory 形校验 + 策略矩阵
cargo test -p oss-server --lib          # 0003 迁移/补列/AP CRUD/作业状态机/Inventory/tier 调度
cargo test -p oss-server --test m8_ap --test m8_batch --test m8_tier --test m8_lifecycle_api \
  --test m8_inventory_parquet --test m8_tier_gc
cargo test -p oss-server --test jobs_engine --test console_api
cargo test -p xl-s3 --test lock_ops --test mpu_ops            # Retention / MPU 五操作线级
cargo test -p oss-server --test raft_handshake --test tls_config_matrix \
  --test tls_listener --test tls_outbound --test tls_mesh     # Level B 握手 / TLS 矩阵
cargo test -p xl-s3 --features olap-wasm   # WASM 变换器（需 wasmtime 环境）
# S3 标签 + Copy 指令在 tests/e2e_s3.sh 步骤 20（已含于全量脚本）
# 能力清理批（2026-10-23）：xl-store 259+1 ignored / xl-s3 136 / oss-server 140
```

M8 端到端（控制面；服务已启动，脚本自行播种 AK/桶，退出时清理）：

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 \
  ADMIN_USER=erik ADMIN_PASSWORD=changeme tests/e2e_m8.sh
# 分层段：服务须以 [[tiers]] 启动；可选 TIER_ENDPOINT/TIER_AK/TIER_SK 让脚本自建远端桶，
# 或 E2E_M8_SKIP_TIER=1 显式跳过（输出 SKIP 行，不静默省段）。
# 其它旋钮：E2E_M8_PARALLEL(8) / E2E_M8_JOB_TIMEOUT(3600) / E2E_M8_TIER_TIMEOUT(240) /
#           E2E_M8_M4_TIMEOUT(150) / E2E_M8_REPL_SMOKE=1（追加 e2e_replication/e2e_sites）。
# 作业吞吐取服务端 OSS_JOB_WORKERS/OSS_JOBS_INTERVAL_SECS。e2e 建议 OSS_JOB_WORKERS=1 +
# OSS_JOBS_INTERVAL_SECS=1（当前引擎 finish_task 在默认 2 worker + 负载下可能锁冲突，任务
# 停在 running 直到 deadline；见 task-6b 报告）。干净 1000 任务作业约 17 分钟；含失败重试
# 的 1001 作业受退避抖动影响可能再多数分钟到十几分钟。脚本跑 1000 与 1000+1 两个作业。
```

集群验收（M3；4 节点真进程 + 混沌段，覆盖「拔节点/分区/并发写/拉回补齐/控制面扇出」）：

```bash
cargo build -p oss-server && tests/e2e_m3_cluster.sh   # 端口全自动取空闲；约 8–15 分钟（含 §⑤ 超时预算）
# 含 §0c：桶行广播接线（建/删桶后各节点即时一致，免重启——M5 切片 0 遗留断言）
```

M9 强一致端到端（自动起 A/B 数据站点 + witness C，端口/目录全自动，退出清理）：

```bash
cargo build -p oss-server && tests/e2e_m9.sh              # 默认 sync 模式（估算 3–6 分钟）
M9_STRETCH=1 tests/e2e_m9.sh                              # Stretch EC 2×6 模式（估算 3–6 分钟）
OSS_OLD_BIN=/path/to/v1.13.1/oss-server tests/e2e_m9_rolling.sh  # Level B 控制面滚动；缺 OSS_OLD_BIN → SKIP
bash tests/e2e_m9_recovery.sh                             # 灾备：cutover/force-recover/B′ + repair（需 sqlite3）
# OSS_BIN=target/debug/oss-server 可指定二进制；M9_STRICT=1 除模式互斥的预期 SKIP 外，
# 任何 SKIP 升级为 FAIL（功能齐备后 CI 用）；e2e_m9_recovery 另需 aws-cli/curl/python3/sqlite3。
```

功能门控：脚本读 `GET /api/v1/raft/status` 的 `capabilities.{raft,sync,stretch}` 决定跑哪些段；
未接线（capabilities=false）以 `SKIP: 原因` 显式跳过并退出 0（不是静默省段）。判定只看
capabilities：`capabilities.raft=true` 时 `enabled` 必须为 true，否则 FAIL。启动阶段只有 A/B 都以
未知字段/配置校验错误退出才默认 SKIP（旧配置严格拒绝的构建）；运行期启动失败一律 FAIL。sync 桶与
stretch 按计划 v3 **互斥**（同一集群不可同时启用），默认跑步骤 1/2/3/5，`M9_STRETCH=1`
跑 1/2/4/5（步骤 3 显式 SKIP）。独立的 `tests/e2e_stretch.sh` 已落地，直接用真实两站点
验证 S4 数据面（不依赖 witness C）；`tests/e2e_m9.sh` 的 stretch 段仍按 capability 门控，
未接线时 SKIP。功能已齐备（三能力位均为 true）；验收请加 `M9_STRICT=1`：除 sync/stretch 模式互斥的预期
SKIP 外，任何 SKIP 都按 FAIL 处理（`skip_expected` 不计失败）。

## 参考文档

- 设计文档：`docs/superpowers/specs/2026-10-10-minio-clone-design.md`
- 功能规格：`docs/superpowers/specs/2026-10-10-feature-spec.md`
- M1 实现计划：`docs/superpowers/plans/2026-10-10-m1a-backend-slice.md`（前端：`…-m1b-frontend-console.md`）
- M2a 实现计划：`docs/superpowers/plans/2026-10-12-m2a-usability-slice.md`
- M2b 实现计划（多盘 / EC / 断电一致性）：`docs/superpowers/plans/2026-10-12-m2b-ec-multidisk.md`
- 子账户与权限体系实施记录：`docs/superpowers/plans/2026-10-13-subaccounts-permissions.md`
- M3 分布式实现记录：`docs/superpowers/plans/2026-10-13-m3-distributed.md`
- M4 运维与 IAM 实现记录：`docs/superpowers/plans/2026-10-14-m4-ops-iam.md`
- M5 安全与锁定实施记录（已发布）：`docs/superpowers/plans/2026-10-16-m5-security-lock.md`
- M6 数据流实施记录（已发布）：`docs/superpowers/plans/2026-10-17-m6-dataflow.md`
- M7 多站点实施记录（已发布）：`docs/superpowers/plans/2026-10-18-m7-multisite.md`
- M8 前置（对象标签 / Copy 指令）：`docs/superpowers/plans/2026-10-19-s3-gaps-tagging-replace.md`
- M8 控制面计划（v3.1，S0–S6 已实施；含验收锚与残余边界）：
  `docs/superpowers/plans/2026-10-19-m8-control-plane.md`
- M8 缺口收口（后端补口：MPU 标签、控制台标签路由、审计过滤、加密/KMS 状态、通知死信、
  Select 代理、桶默认加密、IAM 预设、复制限速；以及 M4/M5/M6 文档化缺口页）：见 M8 计划
  「缺口收口与偏差」节、设计文档 §M8 落地状态
- M8 e2e：`tests/e2e_m8.sh`（前置与旋钮见「M8 测试锚」段）
- M9 强一致计划（v5；数据面 S0–S4 + Raft S5、控制面 S6、3-voter/配对/快照/cutover S7
  已实施；S8/S9 验收已完成（证据见下））：
  `docs/superpowers/plans/2026-10-20-m9-strong-consistency.md`

---

© 2026 erik.xyz · 以 Apache-2.0 许可发布

<!-- assets-license:start -->
## 视觉资产授权 / Visual Assets License

本项目的项目宠物、图片、图标、表情（包括 `docs/pet-expressions/` 等目录下的
SVG / PNG）**不是开源资产**，版权归作者所有。未经书面许可不得复制、修改、再分发或
用于商业用途；商业使用需取得商业授权，联系 **erik@erik.xyz**。详见
[ASSETS-LICENSE.md](ASSETS-LICENSE.md)。

> 代码部分仍遵循仓库原有 `LICENSE`；第三方素材（如明确标注的 Go Gopher 等）仍遵循其原许可证。
<!-- assets-license:end -->

<!-- pet-expressions:start -->
## 项目宠物表情形态 · 吞吞

12 种彩色动态形态：笑 / 欢乐 / 哭 / 沮丧 / 惊恐 / 愤怒 / 撒娇 / 卖萌 / 讨好 / 敲打 / 捂脸 / 吐舌头。每个 SVG 自带表情、情绪特效与整体动效，单张文件见 [`docs/pet-expressions/`](docs/pet-expressions/)。

![吞吞的 12 种彩色动态表情：笑、欢乐、哭、沮丧、惊恐、愤怒、撒娇、卖萌、讨好、敲打、捂脸、吐舌头](docs/assets/mascot-expressions.svg)

| 形态 | 代码 | 形态 | 代码 | 形态 | 代码 |
| --- | --- | --- | --- | --- | --- |
| [笑](docs/pet-expressions/smile.svg) | `:emoji-smile:` | [欢乐](docs/pet-expressions/joy.svg) | `:emoji-joy:` | [哭](docs/pet-expressions/cry.svg) | `:emoji-cry:` |
| [沮丧](docs/pet-expressions/dejected.svg) | `:emoji-depressed:` | [惊恐](docs/pet-expressions/terrified.svg) | `:emoji-terrified:` | [愤怒](docs/pet-expressions/angry.svg) | `:emoji-angry:` |
| [撒娇](docs/pet-expressions/spoiled.svg) | `:emoji-coquettish:` | [卖萌](docs/pet-expressions/cute.svg) | `:emoji-cute:` | [讨好](docs/pet-expressions/plead.svg) | `:emoji-pleasing:` |
| [敲打](docs/pet-expressions/knock.svg) | `:emoji-knocking:` | [捂脸](docs/pet-expressions/facepalm.svg) | `:emoji-facepalm:` | [吐舌头](docs/pet-expressions/tongue.svg) | `:emoji-tongue:` |
<!-- pet-expressions:end -->
