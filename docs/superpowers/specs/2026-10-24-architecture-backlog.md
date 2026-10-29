# 架构级非目标 backlog（Level C / 数据面滚动 / 账户 / M8 多节点 / M3 shrink）

> 状态：**设计 backlog，五项完整目标均未实现**。v1.17.0（2026-10-24）已发布，落地账户 MVP、
> M8 运行态 fencing epoch、DP-0 数据面版本握手与 AP 跨账户/SourceVpce 证明；v1.16.0 已落地
> LC-0 来源校验与 M8 单例任务 leader 门控。以上均为部分切片，完整 Level C / 数据面滚动 /
> 多租户 / M8 多节点 / M3 shrink 仍未实现。证据见验收记录 §12–§13；本文不是发布承诺。
>
> **2026-10-27 更新（v1.20.0，验收记录 §14）**：Wave2 已交付三项切片——
> ② DP-2 mesh gRPC **入站** TLS 物料热轮换 + 真实滚动 e2e（同版本 3 节点 116/0、v1.13.1 旧
> 二进制 64/0/4）；③ 账户/成员写路径改走 Raft（7 个 level-3 命令，按 username 自然键）；
> ⑤ 缩容 topology v4 + 离线 `apply/verify/commit/rollback`（v4 只在 commit 写，作回退防线）。
> 仍未实现：Level C 双向混写与 v1.13.2 补丁线、M8 多节点运行时（lease/takeover/一致读/故障切换）、
> 软字节配额与跨账户共享、AP 真实 PrivateLink 身份、缩容 4→3 loopback e2e 与单机→集群。
>
> **2026-10-28 更新（v1.20.1）**：AC-2 软字节配额已落地只读用量端点
> `GET /accounts/{id}/usage`（只告警不阻写，硬字节配额仍为非目标）；M3 shrink 4→3 已补上
> 真实进程 loopback e2e（86/0/0）。证据见验收记录 §15。其余五项完整目标仍按本文未实现。
>
> **2026-10-28 更新（v1.21.0）**：main 已合入 M8-3 控制台一致读
> （`?consistent=1` + `x-oss-applied-index`）、账户管理员 UI、滞后 follower 的
> `attach_member`/`set_member_status` 追赶、e2e_m9 的 S9 三条断言（实跑 64/0/1）；
> tier 直读/归档绑定源整对象 SHA-256（C1–C4）与 AP 端点静态 registry（注册断言，
> Slice A）也已合入。v1.13.2 旧端曾补齐 LC-2「识别但拒绝」桩与 leader 侧 feature 闸
> （该分支已于 2026-10-29 删除，不随 v1.21.0 发布）。仍缺：Level C 三二进制 e2e/发布文档、M8-2 运行态实时复制、
> 硬字节/对象数配额、真实 PrivateLink 身份、单机→集群工具化。
>
> **2026-10-29 更新（v1.22.0）**：Level C **v1.20.1 ↔ v1.21.1** 控制面
> 混版已验证（专项 147/0/0，旧端无需补丁；v1.13.1 不再支持）；Batch Delete e2e、AP HTTP
> 信任链 E2E、follower 权限追赶、tier flake 修复与 `tier-audit`、升级 preflight、单机→集群
> re-ingest、桶配额并发修复均落地。硬字节/对象数配额与 M8-2 仍为里程碑（本波不实现）；
> AP 动态 registry（Slice B）需 feature level ≥5（4 已被 M8-2 jobs 复制占用），暂不排期。
>
> **2026-10-29 更新（M8-2 jobs 切片）**：jobs/job_tasks/tier_restores（Restore 受理行）
> 运行态复制已落地，作为 **feature level 4**（`FEATURE_LEVEL_4`/`MAX_FEATURE_LEVEL=4`）。
> v1.22 及更早二进制 advertise ≤3，混版窗口内 leader 生效级别压回 3、Job* 命令在 append
> 前被拒（`check_feature`）；启用前必须把全集群升级到含 feature 4 的二进制并移除/升级
> v1.22 及更早节点，再显式配置 `[raft].feature_level=4` 并逐节点重启。v1.20.1 Level C
> 窗口（feature ≤3）不包含 feature 4；快照格式仍是 OSSCTL3（`m8.runstate` 已携带这些表）。
>
> 基线：v1.17.0（2026-10-24），Wave2 切片见验收记录 §14（v1.20.0，2026-10-27）；非目标原文见
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md` §11.3。
> 口径：每项给「现状证据 → 真正阻塞点 → 最小可行切片（MVP）→ 明确非目标 → 接口 →
> 安全包络 → 测试 → 粗估」。粗估为 1 名熟悉本仓库的资深工程师 + 测试，**±50%**，
> 不含前端、文档、发布工程。

## 0. 总览

| # | 项目 | 代码现状（诚实结论） | MVP 体量 | 主要依赖 | 风险 |
|---|---|---|---|---|---|
| 1 | Level C 任意双向混合版本 | Level B 单向窗口仍在；OSSCTL2/3 已实现；v1.20.1↔v1.21.1 控制面混版已验证（147/0/0）；v1.13.1 旧端不再支持；缺生产 Ext 命令、完整双向混写与数据面滚动 | 6–10 人周 | 生产 Ext 命令、CMK 重包、数据面滚动 | 高 |
| 2 | M3/M4 数据面滚动升级 | DP-0/1/2/3 已交付（握手+混版读矩阵、mesh 入站 TLS 热轮换、cutover 来源校验）；仍缺跨版本 MPU 完成 | 4–8 人周 | 跨版本 MPU 语义 | 高 |
| 3 | 完整多租户账户体系 | AC-0..AC-3 + 软字节配额 + 管理员 UI 已交付；仍缺硬字节/对象数配额、canonicalization worker | 5–8 人周 | 跨节点用量聚合 | 中高 |
| 4 | M8 多节点控制面 | 11 表 + 账户两表已随 Raft 复制；M8-1 leader 门控、M8-2a runstate 快照扩展、M8-3 一致读（`consistent=1`/applied 头）已交付；jobs/job_tasks/tier_restores 运行态复制已在 feature 4 交付；仍缺 inventory/lifecycle 运行态日志复制/lease takeover/故障切换 | 6–9 人周 | OSSCTL2/3（持久化部分） | 高 |
| 5 | M3 shrink / 单机→集群 | 静态拓扑 + 位置式槽位；rebalance 只支持尾部追加；shrink 与单机转集群明确不支持 | 8–12 人周 | 拓扑 v3（显式 slot map） | 极高 |

---

## 1. Level C：任意双向混合版本升级

### 1.1 现状与证据（2026-10-28 复核）

- 控制面兼容层定义 **wire 1↔2、feature 1↔4**：`crates/oss-server/src/raft/compat.rs`
  （`WIRE_VERSION=2`、`MIN_COMPAT_WIRE_VERSION=1`、`FEATURE_LEVEL_3=3`、
  `FEATURE_LEVEL_4=4`、`MAX_FEATURE_LEVEL=4`）；生效级别取当前 term voter+learner 的
  feature 最小值（`effective_feature_level`）；激活标记只升不降
  （`bump_activation_tx`），启动守卫拒绝 marker 高于二进制或配置（`guard_startup`）。
  feature 4 是 M8-2 jobs 复制；v1.22 及更早二进制 advertise ≤3，混版窗口内 leader
  生效级别被压回 3，`check_feature` 在 append 前拒绝 Job*（旧解码器不识别这些变体，
  绝不能让其收到）。这是 Level B 的单向窗口，旧端能力上限不变。
- 快照封套已实现 **OSSCTL1/2/3**：`SNAPSHOT_FORMAT=3`（`data.rs:33`），编码按
  `max(激活级别, 数据要求)` 选格式（`data.rs:1196-1230`）。V2 载荷冻结；V3 增命名扩展区
  （`m8.runstate`、tenancy）。行结构全部 `deny_unknown_fields`，未知表/字段/扩展在任何写
  之前整包拒绝（`data.rs:18-21,1097-1134`）；`install_tx`（`data.rs:632`）不再静默忽略。
- 控制命令 `deny_unknown_fields`（`command.rs:18,28,43,60`），`min_feature_level()` 对
  结构化生命周期返回 2、对 `Ext` 与账户/成员命令返回 3、对 Job*（JobBatch/JobUpsert/
  JobTaskUpsert）返回 4；v1.13.1/v1.22 旧解码器遇到未知变体/字段会整条拒绝。
  命令上限 24 MiB（`raft/mod.rs:65`）。
- `raft/store.rs` 的 D6 路径接受 OSSCTL1/2/3（`store.rs:926`），未知扩展 fail-closed；
  旧版 KV 快照仍被拒绝。
- LC-0 来源闸门已落地：导出封套带 `snapshot_format`/`wire_version`/`feature_level`/
  `activation_level`/`source_build`（`cutover/export.rs:27-52`），`verify_export`
  （`export.rs:139`）在 cutover/force-recover 动数据前校验格式、wire 与级别。
- 数据面 `xl.meta` 是 `FORMAT_VERSION=1` 的 msgpack，字段只增 `Option` + `#[serde(default)]`
  （`crates/xl-store/src/meta.rs:15,62`）；旧解码器跳过未知字段，所以**新写旧读**在纯元数据
  层面通常可行。但 CMK 是例外：`EncryptionRecord` 的 `key_id` 形如 `cmk:<name>:k<n>`
  （`meta.rs:315-333`、`kms.rs:278-290`），v1.13.1 只认 `file`/`k` 前缀的 key id，CMK 对象
  在旧二进制上 **fail-closed 不可读**；旧二进制重写 `xl.meta`（如定向删版本）还会丢掉它不
  认识的字段（`part_sizes` 丢失只会让 partNumber Range 降级，不丢数据；CMK 则是硬失败）。
- 升级口径仍写明：**激活 = 不归点**，回滚只能在激活前（README 已精简为概览，升级窗口见
  `site/install.html:209`）；含 Level B 数据的环境只能由能写 OSSCTL2/3 的新二进制快照/切档。
- **Level C 剩余**：v1.20.1 ↔ v1.21.1 控制面混版已验证（专项 147/0/0；旧端 v1.20.1
  无需补丁；v1.13.1 旧端不再支持）。生产 `Ext` 命令零注册（`REGISTERED_EXT_TYPES` 为空，
  `command.rs:305`），完整「旧端只读 + 新 leader 协调写」的双向混写未做；数据面滚动、
  CMK 降级 e2e、回滚矩阵仍缺。CMK rewrap 工具已具备（`oss-server cmk rewrap`，
  `cli.rs:57,461-473`）。
- **注意 v1.20.1 Level C 窗口不包含 feature 4**：M8-2 jobs/tier_restores 复制只在
  feature 4 启用；启用前必须把全集群升级到含 feature 4 的二进制并移除/升级 v1.22 及
  更早节点，再显式配置 `[raft].feature_level=4` 并重启。旧节点（`deny_unknown_fields`）
  绝不能收到 Job*；混版期间 leader 生效级别 ≤3，Job* 在 append 前被拒。

### 1.2 关键结论：已发布的 v1.13.1 不能成为 Level C 的一端

Level C 要求「旧二进制读到新数据并继续作为 voter」。但 v1.13.1 已经发布，它的
`ControlCommand` 解码器（`deny_unknown_fields`）和 OSSCTL1 快照解码器**不可能事后获得新
变体/新表的解码能力**。因此只有两条路：

1. **兼容补丁线**：基于 v1.13.1 发布 v1.13.2（只加解码器与安全默认值，不加新功能），
   作为 Level C 的「旧端」。补丁仍宣告 wire 1/feature 1；遇到新数据降级为只读或拒绝，
   但必须能安全启动、参与选举、应用日志。
2. **不承诺 v1.13.1**：Level C 定义为 v1.15+ 与后续版本之间的双向窗口，v1.13.1 仍按
   Level B 单向处理。

这是产品/发布决策，不是纯代码问题。2026-10-29 起本仓按**路线 2**执行：v1.13.1 不再作为
Level C 的一端，窗口从 v1.20.1 起（已实测 v1.20.1 ↔ v1.21.1 147/0/0）；v1.13.2 兼容补丁线
已删除，不再维护。

### 1.3 最小可行切片

- **LC-0 跨版本闸门（3–5 天）**：导出封套增加 `snapshot_format`、`wire_version`、
  `feature_level`、`source_build`；`verify_export` 与 `force_recover` 在动数据前校验：
  来源 snapshot format 本机可解码、来源 feature ≤ 本机 `MAX_FEATURE_LEVEL`、来源 wire 可读；
  `--force-recover` 对不兼容导出直接拒绝，不做「先清空再失败」。**无 regret，先做。**
- **LC-1 OSSCTL2 + feature 3（3–4 人周）**：新封套 `OSSCTL2`（格式号、表清单、每表行版本、
  扩展区）；新二进制双读 OSSCTL1/2；**编码选最低公共格式**：集群中仍有仅 OSSCTL1 的 peer
  时只写 OSSCTL1（并继续拒绝 level-2 数据），全员 feature ≥3 后才允许 OSSCTL2。
  `MAX_FEATURE_LEVEL` 升到 3；旧节点不认识 3，`effective_feature_level` 自动压回 2——
  这正是门控，不需要新机制。补丁线 v1.13.2 需带 OSSCTL2 解码器。
- **LC-2 命令演进契约（1–2 人周）**：新语义只加新变体并标 level 3；禁止改旧变体字段；
  补丁线为每个新变体提供「识别但拒绝执行」的桩（返回 `FeatureUnavailable`），保证旧节点
  不会因为解码失败而退出 Raft。为未来变体保留 `Ext` 变体（payload 带 type id），避免每加
  一个命令就再发一次补丁。
- **LC-3 CMK 降级路径（1–2 人周）**：最小方案是 **meta-only rewrap**：用 CMK 解包 DEK，
  用旧 provider 重新包，只重写 `EncryptionRecord`（`key_id`/`wrapped_dek`/`kms_key_id`），
  数据分片不动。提供 `oss-server kms-rewrap --from cmk:<name> --to <legacy-key> --dry-run/--apply`
  与降级前扫描 `raft-downgrade-preflight`（列出所有 CMK 对象/桶默认值/MPU kind）。
  未 rewrap 的对象在旧端一律拒绝服务，不做「尽力读」。
- **LC-4 回滚矩阵与演练（1 人周）**：把下表固化成 `docs/` + e2e 断言；每个格必须可执行、
  可观察、有明确报错。已固化：`tests/e2e_m9_rollback.sh`（真实进程，旧端 = v1.20.1）
  覆盖 marker 1/2/3/4、导出封套 wire/feature/snapshot_format 拒绝、激活后回滚必须保留
  `[raft].feature_level=3`，以及显式 CMK 写→轮换→`cmk scan`/`cmk rewrap`→旧/新二进制读回。

### 1.4 明确非目标

- 不做 v1.x ↔ v2.x 跨大版本；不自动把 OSSCTL2 降级回 OSSCTL1（只允许空集群/新集群）。
- 不做「旧二进制写新数据」：Level C 的旧端最多只读；写仍由新 leader 协调。
- 不做 CMK 的旧端兼容读（只做 rewrap 或拒绝）；不做在线批量重加密。
- 不做无补丁线的「任意双向」承诺（见 1.2）。

### 1.5 接口

- `raft/data.rs`：`SNAPSHOT_FORMAT_MAX=2`、`encode_snapshot_with(format, data)`、
  `decode_snapshot` 返回 `(format, ControlData, extensions)`；`schema_meta` 增加
  `snapshot_format`（只升不降，独立于 feature level）。
- `raft/compat.rs`：`MAX_FEATURE_LEVEL=3`；新增 `FEATURE_OSSCTL2=3` 与能力查询
  `supports(feature)`；`/status` 输出 `snapshot_format` 与每 peer 的 feature。
- `cutover/export.rs`：`ExportFile { format, kind, version, snapshot_format, wire_version,
  feature_level, source_build }`；`verify_export` 变成纯函数 + 显式 `CompatPolicy`。
- CLI：`raft-downgrade-preflight`、`kms-rewrap`、`raft-export --format ossctl1|ossctl2`。

### 1.6 安全包络与回滚矩阵

| 已发生状态 | 旧二进制能否启动 | 允许的回退动作 | 禁止 | 数据风险 |
|---|---|---|---|---|
| marker=1，无 Level B 行 | 能（库内无 `schema_meta.raft_feature_level` 行；配置不写 `[raft].feature_level`） | 换旧二进制重启 | 无 | 无 |
| marker=2，有结构化生命周期行 | **不能**（`guard_startup` 拒绝 marker 高于配置/二进制上限；v1.20.1 需保留 `feature_level≥2`） | 保持新二进制；或恢复升级前 DB 备份 | 直接换旧二进制 | 恢复备份会丢升级后写入 |
| marker=3 / 已写 OSSCTL3 快照 | **v1.20.1 能**：同库直接启动可读，`raft-force-recover` 可安装 OSSCTL3 导出；窗口外（v1.19 及更早）不能 | 保持 v1.20.1+；或新集群重灌 | 用低于 marker 的 `feature_level` 启动（R8 拒绝）；向窗口外二进制加载 | 快照不可降级到窗口外 |
| 有 CMK 对象/桶默认值 | v1.20.1 能（自带 CMK 读路径）；pre-CMK 旧端启动后对象 500 | `oss-server cmk rewrap`（当前实现：重加密到同一 CMK 当前版本）后再降级；或删除/迁移对象 | 假装可读 | 未 rewrap 对象在 pre-CMK 端不可用 |
| 已写 OSSCTL2 快照 | **能**（v1.20.1 可解码/加载；Level B 之前不能） | v1.20.1+ 加载；或新集群重灌 | pre-Level-B 旧二进制加载 | 快照不可降级到 Level B 之前 |
| 成员变更后 | 不能（wire<2 被 `check_cluster_uniformity` 拒） | 保持新集群 | 旧二进制加入 | 成员表不可回退 |

统一规则：**任何降级都先停写、快照、跑 preflight；preflight 非零退出即中止**。

上表的可执行证据是 `tests/e2e_m9_rollback.sh`（旧端 = tag v1.20.1，新端 = 工作树二进制；
每格打印 PASS/FAIL，导出拒绝格另断言目标库零改动）。LC-3 的「CMK→legacy provider
meta-only rewrap」目前**未实现**：`cmk rewrap` 只把旧 CMK 版本加密的版本重加密到该 CMK
当前版本（数据分片重写，不是只换信封），因此不覆盖 pre-CMK 旧端的读路径。

### 1.7 测试与粗估

- 单元：OSSCTL2 编解码、旧行缺字段、未知表/扩展区保留、feature 3 门控、rewrap 三态一致。
- 集成：`tests/e2e_m9_rolling.sh`（v1.13.1 历史滚动基线）+ `tests/e2e_m9_rollback.sh`
  （Level C 窗口 v1.20.1 ↔ 新：回滚矩阵、导出封套闸门、激活后回滚、CMK rewrap 读回）。
- 故障：旧 follower 落后时新 leader 提交 OSSCTL2 快照；补丁端解码失败必须留在集群而不是退出。
- 粗估 6–10 人周，风险高（补丁线发布与双解码是主要不确定项）。

---

## 2. M3/M4 数据面滚动升级

### 2.1 现状与证据（2026-10-28 复核）

- **DP-0 数据面握手（v1.17.0）**：`NodeHealth` 新增 `data_wire_version=5`、
  `topo_epoch=6`、`topo_hash=7`、`drives=8`、`maintenance=9`
  （`crates/xl-proto/proto/oss.proto:81-85`）；`xl-store::mesh_gate` 按操作类放行，
  未知 wire/拓扑不一致 fail-closed（503 `peer_incompatible`，
  `crates/oss-server/src/raft/routes.rs:273`、`state.rs:475-485`），状态/metrics 暴露
  degraded/incompatible peers（`router.rs:529-614,835-870`）。
- **DP-1 混版读矩阵（v1.19.0）**：V1 排他 marker 对 legacy peer 改发包含式并回
  `x-oss-degraded: list-marker`（`crates/xl-store/src/store/list.rs:136-224`）；
  版本列表 `require_complete` 对 legacy/不可达 503；MPU 并集回
  `x-oss-degraded: mpu-union`（`store/uploads.rs:193-297`）；placement v2 在混版窗口
  写前 fail-closed。
- **DP-2 mesh 入站 TLS 热轮换（v1.20.0）**：`Tls::mesh_server_watch()`
  （`crates/oss-server/src/tls.rs:233`）+ `serve_tls_watch`
  （`crates/xl-client/src/lib.rs:1866`）每连接取当前 rustls 配置；同版本 3 节点
  116/0、v1.13.1 旧二进制 64/0/4 的滚动 e2e 已实跑（验收记录 §14）。
  `tests/e2e_dproll.sh` 覆盖同版本逐节点重启（读不断、sync 写 fail-closed、控制面继续提交）。
- **DP-3 跨版本 cutover/force-recover 守卫（v1.16.0）**：见 §1.1 的
  `cutover/export.rs:139`。
- 旧端基线：v1.13.1 已有 `ShardWrite/Read/Delete.slot/set`，分片 RPC 主体兼容；
  `ListUploadsLocal`（`oss.proto:19`）与 `ListReq.include_deleted/marker_exclusive`
  （`oss.proto:55-59`）是后续新增，语义差异由 DP-1 门控。
- **仍缺**：跨版本 MPU 完成——`upload_id` 是纯 UUID、暂存区节点本地，重启用另一节点
  无法完成（§2.3 非目标）；跨版本数据面的写窗口仍按 fail-closed/维护窗口执行，
  `tests/e2e_m9_rolling.sh` 只覆盖控制面双二进制滚动。`xl.meta` 旧节点重写仍会丢
  未知字段（`part_sizes`、新 tier/加密字段），需按 DP-1 矩阵逐操作验证。

### 2.2 最小可行切片

- **DP-0 数据面能力握手（1–2 人周）**：给 `NodeHealth` 加 `data_wire_version`、
  `placement_epoch`、`slot_map_hash`（proto3 加字段，旧端忽略）。新协调者按 peer 能力门控：
  - `marker_exclusive` / `include_deleted` 只发给 `data_wire ≥2` 的 peer；否则 V1 续页退回
    包含式并在响应头标 `x-oss-degraded: list-marker`，版本列表（依赖 `include_deleted`）
    直接 503，避免漏 marker 的静默错误。
  - `ListUploadsLocal` 对旧 peer 保持现有「少一路并集 + 告警」，但响应头标
    `x-oss-degraded: mpu-union`；`max-uploads` 的一致性要求由调用方决定。
  - 未知/握手失败默认按 1（与 Raft `parse_wire` 同款保守策略）。
- **DP-1 混跑操作矩阵（1 人周）**：把每个数据面 RPC/高层操作按 old→new、new→old 标注
  「兼容 / 降级 / fail-closed」，固化成测试表；重点：PUT 分片、EC 重建读、版本删除、
  metadata copy、MPU 分片/完成、ListObjects v1/v2、ListObjectVersions、tier 读取。
- **DP-2 mesh TLS 热轮换（1–2 人周）**：客户端在 `AppState` 里用 `ArcSwap<TonicMesh>`，
  SIGHUP 重建并交换；服务端把 `rustls::ServerConfig` 的证书解析器换成读 `ArcSwap` 的
  `ResolvesServerCert`，`ServerTlsConfig` 本身不变，握手时取新证书。mTLS 客户端 CA 轮换需要
  可替换的 `ClientCertVerifier`（或明确要求重启）。**证书新旧重叠期**内逐节点重启仍可用，
  热轮换是锦上添花；先做客户端重建 + 服务端证书解析器即可。
- **DP-3 跨版本 cutover/force-recover 守卫（2–3 天）**：复用 LC-0 的封套字段；`force_recover`
  在清空/安装前校验来源 wire/feature/snapshot format，并把 `verify_export` 的错误原样上报。
- **DP-4 滚动 runbook（2–3 天）**：一次一个节点；先 `heal` 补齐再动下一个；禁止与
  `rebalance`/`fsck`/`heal` 并发；升级前确认 placement v1、无进行中的 MPU（或接受该节点
  MPU 短暂不可用）；跨版本期间不发起成员变更。

### 2.3 明确非目标

- 混跑窗口内不改放置（v2/v3）、不做 rebalance、不做成员变更。
- 不承诺跨版本完成 MPU（粘节点是独立缺口；MVP 只保证同节点重启后可继续）。
- 不做跨版本 tier 直读（归档对象仍需 Restore）。
- 不做证书链在线切换而不重叠（无重叠期的信任根切换必须停写）。

### 2.4 接口

- `oss.proto`：`NodeHealth` 增字段；新增 `MeshHello`（或复用 Health）返回能力位图。
- `xl-store`：`PeerCapabilities`（进程内缓存 + TTL）、`CapabilityGate` 决定请求字段；
  不改变现有 `MeshTransport` trait 的语义，只加能力查询。
- `xl-client`：`TonicMesh::rebuild_tls(...)` 或 `ArcSwap` 包装；`serve_tls` 接受
  `ServerTlsConfig` + 可替换证书解析器。

### 2.5 安全包络

- 只允许 placement v1 混跑；EC 几何（8+4）、set 数、slot 总数必须全集群一致。
- 一次只下线一个节点；该节点 `slots` 全部 up 且 heal 完成后才继续。
- 新协调者发往旧节点的请求不得携带旧端会「理解错」的字段；宁可 503 也不静默返回不完整列表。
- 任何 CRC/checksum/解码错误立即停止滚动，不自动跳过。

### 2.6 测试与粗估

- 扩展 `tests/e2e_m9_rolling.sh`：`OSS_OLD_BIN=v1.13.1` 下做 PUT/GET/List v1/v2/版本列表/
  MPU/EC 重建；逐节点重启；kill 正在写的新节点；TLS 证书轮换（重叠期与热加载）。
- 单元：能力门控矩阵、降级响应头、旧 meta 重写丢字段的检测。
- 粗估 4–8 人周，风险高（旧端语义差异和历史数据形态是主要不确定项）。

---

## 3. 完整多租户账户体系

### 3.1 现状与证据（2026-10-28 复核）

- **AC-0/AC-1/AC-2/AC-3 已交付**：`accounts`/`account_members` 于
  `crates/oss-server/migrations/0005_accounts.sql` 建表（id 仍 12 位、ARN 不变），
  `account_usage` 于 `0006_tenancy.sql`；`accounts.soft_byte_quota` 由
  `db.rs:1046-1052` 补列。账户解析先查成员行、无行回落 `account_id_of(uid)`
  （`db.rs:2280-2290`、`xl-s3/src/access.rs:87-110`）。
- 账户管理员与生命周期：角色 `owner/admin/member`（`0005`），`require_manager`
  允许全局管理员或本账户 manager（`accounts_support.rs:9-31`）；停用账户在 AK 认证
  （`access.rs:706`）与 JWT `who()`（`console.rs:125,139`）全链 fail-closed。
- 写路径走 Raft：8 个 level-3 命令（`AccountCreate/Update/SetQuota/SetSoftQuota/
  Delete/MemberAttach/MemberSetStatus/UserCreate`，`raft/command.rs:226-275`），
  apply 在 `raft/apply/accounts.rs`；OSSCTL3 快照携带账户两表
  （`raft/data.rs:231-233`）。
- 配额：桶数硬配额在创建桶时 fail-closed；2026-10-29 起 Raft 在 leader append 前、
  standalone 在 `BEGIN IMMEDIATE` 写锁内原子判定（并发建桶不会越过配额）；
  软字节配额为只读用量端点 `GET /accounts/{id}/usage` + `PUT /accounts/{id}/soft-quota`
  （`accounts_api.rs:35-37`、`accounts_usage.rs:98-118`，仅告警不阻写）。
- 策略 principal：账户根 ARN 按**成员关系**命中全部成员，无成员行回落 uid 派生账户
  （`access.rs:87-110`）；AP 的 `account` = 桶账户，跨账户只经 AP 策略 Principal
  （`xl-s3/src/ap.rs:8,338-384`）；`aws:SourceVpce` 是显式部署证明，不是 PrivateLink
  端点身份（`ap.rs:99-110,800-814`）。Inventory 跨账户目标仅管理员可配置，运行期复核
  失败即不写输出（§11.3）；这不是租户边界。
- **仍缺**：硬字节配额与对象数配额（无跨节点实时用量聚合）；账户表 level-3
  canonicalization worker 未实现——marker `accounts_canonical_v1` 仅由一次性 standalone
  回填或 fresh Raft 启动写入（`db.rs:299-300,2242-2277`）。
- **已交付**：管理员账户控制台页面（`web/src/pages/Accounts.tsx` + `AccountModals.tsx`；
  列表/创建/改名/启停/删除、成员、桶配额、软字节配额、usage）。

### 3.2 真正阻塞点

1. 账户无生命周期（创建/停用/删除）、无成员关系，无法表达「一个账户多个用户」。
2. ARN 已对外暴露且由 uid 派生，**迁移必须保持现有 ARN 不变**，否则存量策略/客户端全断。
3. 配额需要跨节点用量，当前没有聚合；硬字节配额不能靠各节点本地计数。
4. 跨账户授权需要账户成员集合与桶策略/身份策略的交集语义；现在 ARN 只能匹配单个 uid。
5. VPC 端点身份需要端点注册表与可信证明链，不能信任客户端头。

### 3.3 最小可行切片

- **AC-0 账户实体与迁移（1–2 人周）**：新增
  `accounts(id TEXT PK, name, status, created_at)`（id 仍为 12 位）与
  `account_members(account_id, uid, role, created_at)`；启动时幂等 backfill：每个现有
  user 生成/加入其派生账户，`account_id_of(uid)` 继续作为无行时的 fallback。管理员用户
  归入系统账户（`LEGACY_ACCOUNT` 保留，永不匹配真实用户）。不改 users/buckets 表，不动 ARN。
- **AC-1 账户管理员与生命周期（1 人周）**：角色 `owner/admin/member`；全局 admin 可管理
  所有账户；账户 admin 只能管理本账户成员与配额。停用账户后：新登录拒绝、现有 JWT 在
  `who()` 路径拒绝（`console.rs:28-86` 的载荷校验处加账户状态）。删除账户要求无桶、无成员。
- **AC-2 配额（1–2 人周）**：**硬配额先做桶数量**（从复制的 `buckets` 表计数，创建桶时
  fail-closed）；对象数/字节数先做**软配额**（控制台告警 + 响应头），硬字节配额需要
  跨节点用量聚合，列为后续。账户级限速可复用现有 limiter，但需共享状态，暂不做。
- **AC-3 共享与跨账户策略（1–2 人周）**：桶策略 principal 的账户 ARN 解析为
  「该账户任一成员」；跨账户访问按 S3 语义允许桶策略直接授权（AP 仍做交集限制）。
  增加 `aws:PrincipalAccount` 条件；审计记录 `principal_account`。默认拒绝不变。
- **AC-4 AP 跨账户（1–2 人周，依赖 AC-0/AC-3）**：AP 增加显式 `owner_account` 与
  `bucket_account`；AP 策略只收窄、不放大；跨账户 AP 必须同时满足 AP 策略与桶策略；
  创建/修改 AP 需桶账户授权。AP 缓存失效继续走 apply 副作用（`raft/apply.rs`）。
- **AC-5 VPC 端点身份（1–2 人周，依赖 AC-0/AC-4）**：`vpc_endpoints(vpce_id, vpc_id,
  account_id, state)` 注册表；只接受来自**可信反代**（mTLS/固定 CIDR）的
  `x-oss-source-vpce`，并与 `aws:SourceVpc` 一样 fail-closed；新增
  `StringEquals.aws:SourceVpce` 条件；端点归属账户参与授权。客户端直连头一律忽略。

### 3.4 明确非目标

- 不做完整 IAM（users/groups/roles/policy 版本、AssumeRole、`AssumeRoleWithWebIdentity`）。
- 不做账户层级/组织（Organizations）、账单、跨账户复制、每账户独立 KMS。
- 不做硬字节配额与跨节点实时用量（本切片只做桶数硬配额 + 字节软配额）。
- 不做 AP 的桶级 Create/Delete（仍由桶 API 负责）。

### 3.5 接口

- `xl-s3::access`：`AccountId` newtype、`AccountStatus`、`account_members` 查询接口；
  `account_arn_matches` 改为账户成员匹配（保留 uid 单用户兼容路径）。
- 控制面：`ControlCommand::{AccountCreate, AccountUpdate, AccountDelete, AccountMemberSet,
  AccountQuotaSet}`（随 OSSCTL2/level 3 或先在 local 模式落地）；`ControlData` 增 accounts/
  account_members 两表。
- 策略条件：`aws:PrincipalAccount`、`aws:SourceVpce`；AP 结构体加账户字段。
- 控制台 API：`/accounts`、`/accounts/{id}/members`、`/accounts/{id}/quota`。

### 3.6 安全包络

- 迁移只增表、不改既有行；回滚 = 停用新表/新字段，ARN 与现有授权不变。
- 账户停用是 fail-closed：登录、JWT、SigV4 全部拒绝；恢复需显式启用。
- 跨账户默认拒绝；桶策略显式 Allow 才放行；AP 只能收窄。
- 配额检查在写边界（建桶）原子执行；软配额不阻塞写入，只告警。
- 账户管理操作全部审计（含操作者账户与目标账户）。

### 3.7 测试与粗估

- 迁移：从 v1.15 实际 DB（含管理员桶、`owner_id=NULL`）backfill 后 ARN 黄金样本不变。
- 授权：同账户成员、跨账户桶策略、AP 交集、`aws:PrincipalAccount`、VPC 证明缺失。
- 配额：桶数上限并发创建只成功 N 个；软配额响应头。
- 生命周期：停用后旧 JWT/新登录/SigV4、删除有桶账户被拒。
- 粗估 5–8 人周，风险中高（ARN 兼容与跨账户语义是主要风险）。

---

## 4. M8 多节点控制面

### 4.1 现状与证据（2026-10-28 复核）

§11.3 写「M8 多节点控制面：未实现（单节点）」。准确说法是：**11 张表 + level-3 账户
两表的控制写已随 M9 Raft 复制**（`raft/data.rs:1-5,231-233`），M8-1 的 leader 门控、
M8-2a 的运行态快照扩展与 M8-2 jobs/tier_restores 运行态复制（feature 4）已落地；
inventory/lifecycle 运行态复制与 lease 接管仍未完成：

- `ControlPlane` 覆盖 11 表命令与 level-3 账户/成员命令（`raft/command.rs:226-275`、
  `raft/apply/accounts.rs:60-250`）；follower 写返回 `Unavailable` + leader 提示，
  HTTP 不做 307 重定向（`control.rs` 的 `ForwardToLeader` 映射），客户端需自行重试。
- **M8-1 leader 门控已落地**：`LeaderGate`（`raft/mod.rs:184-240`）按 `is_leader()`/
  `still_current()` fencing；jobs 调度（`jobs.rs:185`）、tier sweep（`tier_sweep.rs:488`）、
  inventory（`inventory.rs:215`）、lifecycle（`lifecycle.rs:1100`）只在 leader 执行。
- 运行态部分复制：feature 4 下 `jobs`/`job_tasks` 行与 Restore 受理的 `tier_restores`
  pending 行经 Job* 命令进日志（`job_state.rs`）；`inventory_runs`/`inventory_cursors`/
  `lifecycle_cursors` 及 tier restore 的后续状态仍是节点本地表（`tier_sweep.rs`、
  `inventory.rs`），leader 本地库丢失仍会丢这些在途状态。
- M8-2a 交付的是**快照扩展**而非日志复制：九张运行态表编码为 OSSCTL3 命名扩展
  `m8.runstate`（`raft/runstate/mod.rs:1-10`、`migrations/0007_m8_runstate.sql`），
  随快照安装/导出携带；`schedule_lease` 只有表与镜像，**无 acquire/renew/takeover**
  （全仓仅 `raft/runstate/store.rs:213,432` 读写该行）。
- 审计仍本地（`state.rs` 的 `audit_log`）；AP 缓存进程本地 + TTL（`ap.rs:9-12`）；
  JWT 无状态、12h 过期（`console.rs`）；登录限流器进程本地——多节点会话天然可跨节点，
  但限流与吊销强度下降。
- **M8-3 已交付**：`?consistent=1` 有界追赶 + `x-oss-applied-index` 响应头
  （`console.rs::console_read_consistency`、`tests/m8_read_consistency.rs`）。
- **仍缺**：inventory/lifecycle 运行态日志复制、跨节点 lease/takeover、故障切换演练
  （`tests/e2e_m8.sh` 为单节点）。M8-2 的 jobs/tier_restores 切片已在 feature 4 交付；
  完整运行态复制仍约 3–4 人周。启用 feature 4 前必须全集群升级并移除/升级 v1.22 及
  更早节点（v1.20.1 Level C 窗口不含 feature 4）。

### 4.2 真正阻塞点

1. 作业/清单运行/游标/tier 恢复没有复制，leader 切换会丢在途状态或重复执行。
2. 调度器无 leader/fencing，多节点会重复跑。
3. follower 读是本地已应用状态，不是线性一致；控制台写后读可能看不到自己的写。
4. 审计本地、限流本地、会话吊销粒度不足。
5. 控制写返回 503 而非 leader 重定向，多节点前端体验差且易误判失败。

### 4.3 最小可行切片

- **M8-0 leader 提示与重定向（3–5 天）**：控制写失败时返回 307/503 + `Location`/leader
  endpoint（`raft/mod.rs:226` 的 `leader_endpoint()` 已存在）；控制台与 S3 错误映射区分
  「重试 leader」与「真失败」。无复制语义变化，先降低误操作。
- **M8-1 调度器 leader 门控 + fencing（1–2 人周）**：`spawn_workers`、inventory worker、
  tier reaper 只在 `raft.is_leader()` 时运行；apply 到新 leader 后等待一个 lease/超时再接管，
  复用现有 stale/expired 领取恢复（`jobs.rs` 的 claim 恢复、`inventory::due_runs`）。
  这一步防重复，但**不解决 leader 本地 DB 丢失**，必须标明「非持久多节点」。
- **M8-2 运行态复制（3–4 人周，依赖 LC-1 OSSCTL2）**：把 `job_tasks`、`inventory_runs`、
  `inventory_cursors`、`tier_restores`（以及必要的 `lifecycle_cursors`）纳入 `ControlData`，
  用新命令变体写；apply 落库，执行仍由 leader 完成。任务负载可能超过 24 MiB 上限，故命令
  只存元数据/结果引用，不存大 payload；输出仍写对象存储。快照安装后重建本地表。
  **已交付切片（feature 4）**：jobs/job_tasks 完整行与 Restore 受理的 tier_restores
  pending 行经 `JobBatch`/`JobUpsert`/`JobTaskUpsert` 复制，leader 生效级别 <4 时
  fail-closed；快照仍为 OSSCTL3 `m8.runstate`。剩余：inventory/lifecycle 运行态与
  lease takeover。启用前置：全集群 feature-4 二进制、移除/升级 v1.22 及更早节点、
  `[raft].feature_level=4` 显式配置并重启。
- **M8-3 读一致性（1–2 人周）**：默认 follower 本地读（保持现状），响应头带
  `x-oss-applied-index`；`?consistent=1` 走 leader 读（转发）或 ReadIndex。控制台写后跳转
  使用 `consistent=1`。不做 follower 线性一致读的通用承诺。
- **M8-4 会话/审计（1–2 人周）**：JWT 继续无状态共享密钥；登录限流先接受进程本地并写入
  文档；账户停用通过复制的 users/accounts 状态在每个节点 `who()` 检查。审计保持本地
  append + 日志外送（不进 Raft，避免写放大）；查询标注「本节点」，聚合留给外部日志系统。
- **M8-5 故障切换演练（1 人周）**：kill leader（作业/清单/tier 进行中），验证新 leader
  接管、无重复输出、清理 stale、缓存/目录副作用重建（`raft/apply.rs` 的 `SideEffect`）。

### 4.4 明确非目标

- 不做多地域 Raft、2 节点无 witness 高可用、跨站点线性一致读。
- 不做审计进 Raft、不做作业执行结果强一致、不做 exactly-once（按 at-least-once +
  幂等动作设计）。
- 不做 follower 任意读的会话一致性；只有显式 `consistent=1`。
- 不做自动负载均衡/网关；多节点入口仍由部署方负责。

### 4.5 接口

- `raft/control.rs`：`ControlError` 增加 leader endpoint；HTTP 层 307/503 映射。
- `ControlData` 新表 + 新命令变体（OSSCTL2）；`JobLease { epoch, node, expires_at }`。
- 读 API：`consistent` 查询参数/头；`x-oss-applied-index` 响应头。
- 调度器：`LeaderGate::run_if_leader(|| ...)`，fencing epoch 单调。

### 4.6 安全包络

- 只有 leader 执行调度；leader 变更后旧 leader 的写入必须被 epoch 拒绝。
- 作业动作幂等；输出路径按 `run_id` 隔离，重复执行不得覆盖/追加同一输出。
- 控制写失败不得静默降级为本地写；follower 本地写路径必须保持关闭。
- 快照安装后必须重建 AP/OLAP 缓存与桶目录副作用（已有 `PostCommit`/`after_snapshot_install`）。
- 读一致性默认弱，但必须可观测（applied index/leader），不能假装强一致。

### 4.7 测试与粗估

- e2e 3 节点：leader 切换（作业/清单/tier 进行中）、无重复输出、stale 清理、`consistent=1`
  读己之写、follower 控制写 503 + 重试成功、审计归属节点。
- 单元：LeaderGate/fencing、命令尺寸上限、快照安装重建。
- 粗估 6–9 人周（其中 M8-1 可先做 1–2 周；持久化部分依赖 OSSCTL2），风险高。

---

## 5. M3 shrink / 单机→集群

### 5.1 现状与证据

- 拓扑是**静态配置**：`NodeSpec`/`ClusterTopology`（`crates/xl-store/src/store/drives.rs:59-80`），
  槽位总数 ≤255（`ShardRef.drive: u8`，`drives.rs:96-102`）；stretch 固定 2×6、EC 8+4
  （`drives.rs:311`）；放置哈希 `sha256(bucket||'/'||key)` 前 8 字节
  （`drives.rs:395`）；`slot_of_shard`/`slot_target` 按槽位算节点与本地盘
  （`drives.rs:488-490`）。
- `.oss.sys/topo` 指纹绑定 set/node/slot，配置顺序变化即启动失败
  （`crates/xl-store/src/store.rs:424-460`；`topo_record`/`parse_topo_record` 在 `store.rs:1687-1702`）。
- rebalance 只支持**尾部追加**：只搬「链尾向前第一个非 marker 数据版本」，历史版本留老槽，
  inline/旧分片/未完成 MPU 排除，一次一个，幂等（`crates/xl-store/src/store/rebalance.rs:5,18,33,68`）。
- README 明确：M3 不支持单机原地升级为多节点，数据要重新灌入（`README.md:454`）；
  扩容只允许尾部加节点 + 全节点 `placement="v2"` + rebalance（`README.md:491-520`）；
  shrink/回滚不支持。
- MPU 粘节点（§11.3）：shrink 前必须完成或中止所有 MPU。

### 5.2 真正阻塞点

1. `ShardRef.drive` 是**逻辑槽位号**，不是节点/物理盘；槽位→节点的映射由静态配置决定。
   删除节点后，原槽位无定义，分片不可达；且改变节点数会改变所有对象的放置，理论上要重写
   所有 meta 的槽位号。
2. 没有拓扑版本/epoch 写进 meta 或控制面，无法表达「同一槽位号的归属变了」。
3. rebalance 只有追加语义，没有「迁出节点」的源端驱动/校验/提交协议。
4. 单机与集群的放置空间不同（单机 v2 自动、集群按 sets），没有共享的槽位映射，无法原地转换。

### 5.3 最小可行切片（离线、停写）

- **SH-0 拓扑 v3：显式 slot map（2–3 人周）**：把「槽位号 → (node, drive)」从位置式推导
  改为显式表：`SlotMap { epoch, slots: Vec<SlotTarget> }`，槽位总数保持不变（仍 ≤255），
  `ShardRef.drive` 继续是槽位号。这样 shrink 只改被退役槽位的映射，**其余对象放置与所有
  meta 不变**；读路径按 slot map 解析，写路径按 slot map 选择目标。`.oss.sys/topo` 升 v4
  带 epoch + hash；旧二进制看到 v4 必须拒绝启动（这是全停升级，不属于 Level B/C）。
- **SH-1 离线 shrink 工具（3–4 人周）**：`oss-server cluster-shrink plan|apply|verify`：
  1. 停写、中止/完成 MPU、备份拓扑与控制面；
  2. plan：校验剩余节点容量与**每节点每 set 槽位数 ≤ parity**（EC 8+4 时一节点掉线最多
     丢 4 片；stretch 2×6 删一节点即丢 6 片，直接拒绝）；输出受影响槽位/对象/字节；
  3. apply：对受影响槽位，按对象目录整体复制（含 inline 与历史版本的 xl.meta），EC 缺失
     分片用 `reconstruct` 重建，写目标后逐文件 CRC 校验；**meta 不改**（槽位号稳定）；
  4. verify：全量 fsck + 抽样读 + 分片计数；
  5. commit：写拓扑 v3 + epoch，重启；回滚只能在 commit 前（旧拓扑 + 旧文件未删）。
- **SH-2 单机→集群（MVP 不做，给替代）**：单机与集群放置空间不同，最小可信方案是
  重新灌入（rclone/客户端重传）；若必须工具化，用 SH-0 的 slot map 把单机槽位映射到集群
  并把对象目录全量复制，但那本质是「停机全量迁移」，工作量 ≥ SH-1，不建议先做。

### 5.4 明确非目标

- 不做在线 shrink、不做自动容量再平衡、不做自动改 EC 几何。
- 不支持 shrink 到违反「每节点每 set ≤ parity」的拓扑；不支持 stretch 2×6 的节点删除。
- 不做 shrink 期间的新写入/MPU；不做 rebalance/fsck/heal 并发。
- 不做单机↔集群的无停机双向转换（MVP 明确为重新灌入）。

### 5.5 接口

- `xl-store`：`SlotMap`/`SlotTarget`/`TopologyEpoch`；`ClusterTopology::slot_target` 改读
  slot map；`.oss.sys/topo` v4；`ShardTarget` 不变（仍解析为 Local/Remote + slot）。
- CLI：`cluster-shrink plan --to <new.toml> [--json]`、`apply`、`verify`、`rollback`（仅 commit 前）。
- 报告：受影响对象/分片/字节、不可重建对象、容量与 parity 违规、耗时。
- 控制面：拓扑 epoch 可作为只读状态出现在 `/status`；拓扑本身仍走配置文件，不进 Raft。

### 5.6 安全包络

- **停写是前提**：不是「尽力在线」，而是进入维护模式后执行；HTTP 503 带维护码。
- 备份拓扑、控制面快照、`.oss.sys/topo`；apply 前必须 `plan` 成功且人工确认。
- 复制后逐分片 CRC，EC 重建后用 `fsck` 全量验证；任一失败中止且不 commit。
- commit 前旧节点/旧分片保持只读、不删除；commit 后才允许清理。
- 容量/parity 校验 fail-closed：不满足就拒绝，不提供 `--force`。
- 一次只允许一个 shrink；与 rebalance/fsck/heal 互斥。

### 5.7 测试与粗估

- e2e：4 节点×3 盘 → 3 节点（12 槽），验证全部版本/inline/大对象/EC 缺片重建；中断
  apply 后重跑；commit 前回滚；容量不足与 parity 违规被拒；shrink 后 fsck 0。
- 单机→集群：先只做「重新灌入」的 runbook 测试；工具化另立项目。
- 粗估 8–12 人周，风险极高（离线迁移 + 拓扑语义变更是本 backlog 最危险的一项）。

---

## 6. 推荐顺序与依赖

### 6.1 依赖关系

- **LC-0 跨版本闸门**：无依赖，所有升级/恢复路径都受益，先做。
- **DP-0/DP-1 数据面握手与矩阵** → 数据面滚动；**DP-2 TLS 热轮换**可并行；两者都不依赖
  Level C。
- **LC-1 OSSCTL2** 是共享地基：Level C（LC-2/3/4）与 M8 持久化（M8-2）都依赖它。
- **Level C** 还依赖「兼容补丁线 v1.13.2」的产品决策与 CMK rewrap；没有补丁线就不要承诺
  任意双向。
- **账户 AC-0/AC-1/AC-2/AC-3** 彼此可并行于其他线；**AP 跨账户 AC-4 依赖账户**；
  **VPC 端点身份 AC-5 依赖账户 + AP**。
- **M8-0/M8-1** 可先做（防重复、改善体验）；**M8-2 依赖 OSSCTL2**；M8-3/4/5 可在 M8-1 后做。
- **M3 shrink** 依赖拓扑 v3（SH-0），与其他线无强依赖；**单机→集群**依赖同一拓扑模型，但 MVP 建议重新灌入，不排期。
- M3 MPU 粘节点是横切缺口：数据面「完全在线」与 shrink 的「无中断」声明都受它限制，应作为独立小项排期或明确降级。

### 6.2 建议排期（按投入产出）

1. **第 0 阶段（1–2 周）**：LC-0 + DP-3 跨版本守卫；顺手把 `/status` 的版本/兼容信息补齐。
2. **第 1 阶段（4–8 周）**：DP-0/DP-1/DP-2/DP-4，交付「数据面可滚动升级」；这是 §11 中
   最接近可交付、且不依赖其他大项的一项。
3. **第 2 阶段（5–8 周，可与第 1 阶段并行）**：账户 AC-0→AC-3；随后 AC-4；AC-5 视 VPC
   需求再排。账户是 AP 跨账户/VPC 的前置，越早做越省返工。
4. **第 3 阶段（4–6 周）**：LC-1 OSSCTL2；没有它，Level C 与 M8 持久化都只能停在设计。
5. **第 4 阶段（6–9 周）**：M8-1 → M8-2 → M8-3/4/5；M8-2 必须等 OSSCTL2。
6. **第 5 阶段（6–10 周）**：Level C 收尾（LC-2/3/4 + 补丁线发布/演练）；建议在 OSSCTL2
   稳定一个版本后再做。
7. **最后（8–12 周）**：SH-0/SH-1 shrink；单机→集群保持「重新灌入」文档，不工具化。

### 6.3 不建议的捷径

- 不要在 OSSCTL2 之前做 Level C：OSSCTL1 的静默丢字段会让「双向」变成数据损坏。
- 不要在 M8-2 之前宣称「多节点控制面完成」：11 表复制不等于作业/清单/审计多节点。
- 不要在拓扑 v3 之前做 shrink：改节点数会移动所有槽位，重写全部 meta 的方案不可维护。
- 不要给 shrink 提供 `--force`：parity/容量不满足时强制只会制造不可恢复的 EC 组。
- 不要把 `aws:SourceVpc` 当端点身份：它只是来源证明，端点身份必须来自注册表 + 可信代理。

本文只做设计与证据梳理；未修改任何代码或既有文档，五项能力的实现状态以 §11.3 为准。
