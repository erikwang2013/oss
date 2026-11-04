# 集群 legacy 控制对象补证书（Phase 3 前置）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为集群部署提供一次性的 legacy `.oss.sys` 控制对象迁移工具：跨节点/磁盘读取全部原始副本，按 `(control_version, content)` 分组，仅当唯一同内容组 ≥ `Q_s` 份时用现有标记协议写提交证书；否则 `CorruptMeta` 且一个字节都不改。幂等、拒绝非控制对象/`Some(false)` 残骸/混版节点，绝不覆盖已提交的更新版本。

**Architecture:** 新增只读 RPC `ReadControlRaw`（每盘原始 `xl.meta` 字节，不选举/不过滤证书）+ `NodeHealth.control_raw` 能力位。协调者 `Store` 侧新增 `scan_legacy_control_object`（全槽观察 → 解码 → 规范内容 hash 分组 → 状态判定）和 `recertify_system_object_cluster`（fenced 锁内重扫 → 现有 `write_meta_marker` 写证书）。CLI 是 `oss-server control-recertify`，默认 dry-run。不新增提交协议，不新增依赖。

**Tech Stack:** Rust 2024、tokio、tonic/prost（构建期生成）、rmp-serde、async-trait、futures、serde_json（CLI）。

**Spec:** Phase 0 设计（`docs/superpowers/plans/2026-11-02-control-plane-no-database-phase0.md`）+ 本仓库 `crates/xl-store/src/store/control.rs` 的单节点 `recertify_system_object` 语义 + VPC 切片计划末尾的迁移门禁说明。

## 裁决（Ruling）

### R1：键枚举 = 操作者显式键列表，不加 `ListControlKeys` RPC

`control-recertify --keys-file <path>`，一行一个 `control/...` 键；空行与 `#` 注释忽略；空列表、重复键、非 `control/` 前缀、非法键在任何扫描/写入前即拒绝。理由：Phase 0 已把键集归给控制 manifest；集群列举在任何盘/节点不可达时都无法证明完备，仍要套同一套全槽门禁，收益低而新面大。**代价（必须写进运维说明）**：漏掉的键不会被迁移，仍保持 `control_committed=None` 并在 Phase 3 预检 fail-closed；用完整列表重跑即可修复，不会误认证、不会丢数据。`Missing` 键按 no-op 处理（退出码 0），dry-run/摘要显式打印 `missing` 计数。

### R2：原始副本读取 = 新增只读 RPC，不复用 `ReadMeta`

`ReadMeta` 的控制分支只返回**已认证**的选举结果（`mesh_read_meta` 过滤 `control_committed=Some(true)`），`copies/corrupt` 只是计数，拿不到分组所需的内容。新增：

```proto
rpc ReadControlRaw(MetaRead) returns (ControlRawResp);
message ControlRawCopy { uint32 slot = 1; bytes meta = 2; }
message ControlRawResp { repeated ControlRawCopy copies = 1; uint32 slots_ok = 2; }
// NodeHealth：bool control_raw = 12;
```

服务端 `Store::mesh_read_control_raw`：仅接受 `bucket == ".oss.sys"` 且键前缀 `control/`（否则 `InvalidArgument`）；逐本地盘读 `xl.meta`（**忽略 `.new`**）；NotFound 计覆盖但不产出副本；读错误 → `CorruptMeta`；超过 `MAX_INLINE + 64 KiB` 先拒绝（有界内存）；返回全局槽号 `slot_base() + drive` 与原始字节，不解码、不选举、不过滤。协调者统一解码。

### R3：分组与 quorum = 全槽观察 + 规范内容 + 唯一候选

- **计数单位是槽/盘**：`Q_s = ec.data + 1`，`N = flat.slot_count()`；同一节点的多盘各算一份（逐节点 `drive_count ≤ parity < Q_s` 使多节点拓扑不可能单节点凑够 quorum）。要求 `2*Q_s > N`（否则 `Unsupported`，与 `read_control_cluster` 同口径）且 `N >= data + 2`（标记协议写空间）。
- **完备性**：必须确定性观察到全部 `N` 个槽。节点不可达、能力不符、`slots_ok != 该节点盘数`、总和 `!= N`、重复槽号、读错误 → 失败且不写。缺席槽可以没有副本，但必须“看过”。
- **规范内容**：`clone` meta → `fence_token = None`、`control_committed = None` → `encode()` → `sha256`；组键 `(control_version.unwrap_or(0), hash)`。这扩展现有 `elect_control` 的“同版本忽略 token”分组：现有读只在已认证副本之间比较，不会看到 `None`，行为不变。
- **拒绝**：解码失败、`object_key` 不匹配、非控制形态（`control_version` 与 `fence_token` 均 None）、任何 `control_committed=Some(false)`、任何损坏副本 → `CorruptMeta`。`Some(false)` 证明曾有 Phase 0 写者介入；忽略它可能把更旧的 legacy 组误认证，故整键拒绝（这类键由正常 put/delete 推进版本覆盖，不属于本工具）。
- **候选选择（与 `elect_control` 的 rank 对齐）**：每组记 `max_token` 与 `certified` 份数，rank = `(max_token, version)`。
  - 无任何证书：唯一个 `copies >= Q_s` 的组 → 可认证；0 个 → `CorruptMeta`；多个 → `CorruptMeta`（`2Q_s > N` 下理论不可达，防御性拒绝）。
  - 有证书：取 rank 最大的证书组。rank 并列但内容不同 → `CorruptMeta`（同版本分叉）。该组 `certified >= Q_s` → `AlreadyCommitted` no-op；`certified < Q_s` 但总副本（含同内容 legacy `None`）`>= Q_s` → 补齐；总副本 `< Q_s` → `CorruptMeta`。**有证书时绝不认证 legacy-only 组**：已提交状态不得被迁移工具改写；低于最高 rank 的证书副本是两阶段提交刻意保留的旧副本，忽略而非报错。
  - 无证书时，Q_s 组即最后可证明的持久状态；更高版本的少数副本是失败写残骸，认证该组会在标记阶段覆盖它们（与正常写一致），不算“覆盖已提交版本”。

### R4：写证书 = 现有 fenced 锁 + 现有标记阶段，绝不新增协议

`recertify_system_object_cluster(key)`：

1. `control_token_floor(key)`（现有 `read_control` 的 `max_token`）→ `control_lock(key, floor)`（本机 mutex+flock + fenced 集群锁）；检查 `lease_lost()`。
2. 锁内 `scan_legacy_control_object(key)` 重扫（权威证据）；`Missing`/`AlreadyCommitted` 直接返回。
3. `guard.commit_token(0, scan.max_token)`：锁 token 必须严格大于**所有已观察** token；否则 `Conflict`，重跑即可。
4. 取候选组 rank 最高 token 的副本作代表（同组内容一致），置 `control_committed = Some(true)`、`fence_token = Some(commit_token)`，版本/数据/`control_deleted`/`versions` 原样保留。
5. 再查 `lease_lost()` → `write_meta_marker(layout::SYS_DIR, key, &marker, commit_token, &[])`。**不调用 `commit_control_meta`**：那会重写第一阶段数据并可能先毁掉 legacy 证据；单节点 `recertify_system_object` 已是直接标记阶段的先例。
6. `flipped >= Q_s` 即提交；`flipped == 0` → `QuorumUnmet`；`Error::Indeterminate` 原样上抛（可能已提交，禁止假设回滚）。重跑走“有证书且总副本 ≥ Q_s”的补齐分支收敛。

写前证据：候选组在盘上已 ≥ `Q_s` 份；标记阶段只负责把该内容写到 `Q_s` 个槽，不用来“凑”quorum。锁 + `commit_meta_on` 的 `Stale` 检查保证不覆盖 token 更新的写入；rank 选择保证不覆盖已提交的更新版本。

### R5：CLI = `oss-server control-recertify`（在线管理命令，默认 dry-run）

`control-recertify --keys-file <path> [--apply] [--json]`。必须走完整 `AppState::load`（需要 `GatedMesh(TonicMesh)`），**不得**加入 `needs_full_state` 的离线列表；在“本机 serve 停止、其余节点在线”的维护窗口执行（与 fsck/rebalance 同款约束）。默认只扫描；`--apply` 才写。逐键继续、最后汇总；任一键错误/不可认证 → 退出码 1。`Indeterminate` 打印“可能已提交，禁止假设回滚；重跑同一命令核对”。单节点部署不提供此 CLI：继续用库 API `recertify_system_object`，本工具对非集群返回 `Unsupported`。

### R6：状态机与错误

| 观察 | 状态 | dry-run | apply |
|---|---|---|---|
| 全槽观察、0 副本 | `Missing` | missing | no-op，退出 0 |
| 无证书、唯一组 `copies>=Q_s` | `Certifiable` | certifiable | 写标记 → `Certified` |
| 无证书、无组达标 | `CorruptMeta` | 失败 | 不写 |
| 最高 rank 证书组 `certified>=Q_s` | `AlreadyCommitted` | already-committed | no-op |
| 最高 rank 证书组 `certified<Q_s` 且总副本 `>=Q_s` | `CompletionNeeded` | completion-needed | 写标记 → `Certified` |
| 最高 rank 证书组总副本 `<Q_s`、rank 并列分叉、任何 `Some(false)`、损坏/非控制/覆盖不全 | `CorruptMeta` | 失败 | 不写 |
| 混版（缺 `fenced_meta`/`control_quorum`/`control_raw`） | `PeerIncompatible` | 失败 | 不写 |
| `2Q_s<=N` 或 `N<data+2` | `Unsupported` | 失败 | 不写 |

## Global Constraints

- 现有行为不变：控制对象读写、数据面、快照、HTTP 语义；新增字段/能力位只做加法。旧节点 `control_raw` 缺省 false，必须 fail-closed。
- 现有测试全绿：`cargo test -p xl-store`、`cargo test -p xl-client`、`cargo test -p oss-server`；`cargo clippy -p xl-store -p xl-client -p oss-server --all-targets` 无新增 warning；`cargo fmt --all -- --check`。
- 不新增依赖；`xl-proto` 由 `build.rs` 构建期生成，不提交生成物。
- 每任务结束提交；文件保持 <500 行，测试拆到独立 `tests_*.rs`；不加 `Co-Authored-By`。
- 真实测试，不用 mock：集群用 `Cluster`/`InProcessMesh`（真实盘 + 真 quorum 路径），传输用 localhost `TonicMesh` 往返，崩溃用子进程 SIGABRT 注入。
- 失败路径必须**零写入**：测试在执行前后对全部槽的 `xl.meta`/`xl.meta.new` 做字节快照比对。
- 只动 `.oss.sys/control/`；新 RPC 只读且仅接受该命名空间。
- 禁止手工删除/改写副本“修复”quorum；工具只做证明与标记。

## Review Focus（最高风险 5 项）

1. **数据丢失 / 覆盖已提交版本**：rank 选择必须跳过两阶段提交保留的旧证书副本，却只对最高 rank 组操作；`commit_token` 必须严格大于全副本 `max_token`；写前必须已 ≥`Q_s` 份同内容；绝不使用 `commit_control_meta` 的数据阶段。任何一条失效都可能把已提交 v2 回退成 legacy v1。
2. **误认证分叉内容**：规范 hash 必须稳定（`BTreeMap` 已保证 metadata 顺序；只剥 `fence_token`/`control_committed`）；任何 `Some(false)`/损坏/重复槽/未观察槽都必须整键失败，不能“跳过坏副本继续数”。同版本不同内容必须 `CorruptMeta`。
3. **混版集群**：缺 `control_raw` 的 peer 会走 trait 默认 `Unsupported`；必须先过 `ensure_control_quorum_compatible(false)` 再查 `control_raw`，且禁止退化为逐节点本机 recertify。旧节点静默忽略新 RPC 是最大风险，能力位是唯一防线。
4. **部分写入 / Indeterminate**：标记阶段可能翻 1..Q_s-1 槽；重跑必须走补齐分支收敛，CLI 必须明确“可能已提交、重读而非回滚”；测试必须覆盖翻 1 槽后 abort。绝不能把 `Indeterminate` 当 `QuorumUnmet` 重试后假设未提交。
5. **操作者错误**：显式键列表漏键是主要残余风险（结果：该键仍 fail-closed，无假认证，完整列表重跑可修复）；dry-run 默认、`--apply` 显式、退出码、`Missing` 语义、非 `control/` 键拒绝、错配置（单节点/小拓扑）都必须有测试。运行在错误环境时工具只能读该环境，不能“猜”。

---

### Task 1: 协议、DTO 与只读 raw-copy 服务端处理器

**Files:**
- Modify: `crates/xl-proto/proto/oss.proto`
- Modify: `crates/xl-store/src/mesh.rs`
- Modify: `crates/xl-store/src/store/mesh_api.rs`
- Modify: `crates/xl-store/src/mesh_gate_tests.rs:116,228`、`crates/xl-store/src/store/tests_shrink.rs:430`（`NodeHealth` 字面量补 `control_raw`）
- Modify: `crates/xl-client/src/lib.rs:588,1273`（health 往返字段）
- Modify: `crates/xl-store/src/store/tests.rs`（顶部加 `#[path = "tests_control_migrate.rs"] mod control_migrate;`）
- Create: `crates/xl-store/src/store/tests_control_migrate.rs`
- Test: 新测试文件 + `cargo test -p xl-store`

**Interfaces:**
```rust
// mesh.rs
#[derive(Clone, Debug)] pub struct ControlRawCopy { pub slot: u16, pub meta: bytes::Bytes }
#[derive(Clone, Debug, Default)] pub struct ControlRawResp {
    pub copies: Vec<ControlRawCopy>, pub slots_ok: u32,
}
// MeshTransport 默认实现必须是 Unsupported（旧测试替身不被迫实现，且不能伪装成空）
async fn read_control_raw(&self, node: &str, r: MetaRead) -> Result<ControlRawResp> { ... }
// Store 服务端：pub async fn mesh_read_control_raw(&self, r: MetaRead) -> Result<ControlRawResp>
```

- [ ] **Step 1: 写失败测试**

在 `tests_control_migrate.rs` 写 `mesh_read_control_raw_returns_all_copies_ignoring_certificate`：`Cluster::new(4,3)`，`put_system_object` 后把 slot 0 改成 `control_committed=None`，再加一个 `.new` 伪文件；断言本节点返回 3 份 raw（含未认证）、字节等于盘上 `xl.meta`、不含 `.new`；非 `.oss.sys` bucket / 非 `control/` 键 → `InvalidArgument`。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store mesh_read_control_raw -- --nocapture`
Expected: FAIL（RPC/DTO 不存在）

- [ ] **Step 3: 加 proto/messages/能力位与处理器**

按 R2 增加 RPC、消息、`NodeHealth.control_raw=12`；`mesh_health` 置 `true`；处理器限制 `.oss.sys` + `control/`，逐盘读 `xl.meta`（忽略 `.new`），NotFound 计 `slots_ok`，读错误/超大 → `CorruptMeta`；所有 `NodeHealth` 字面量与 xl-client 转换补字段。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store mesh_read_control_raw -v && cargo test -p xl-store mesh_health -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-proto/proto/oss.proto crates/xl-store/src/mesh.rs crates/xl-store/src/store/mesh_api.rs crates/xl-store/src/mesh_gate_tests.rs crates/xl-store/src/store/tests_shrink.rs crates/xl-store/src/store/tests.rs crates/xl-store/src/store/tests_control_migrate.rs crates/xl-client/src/lib.rs
git commit -m "feat(xl-store): add read-only raw control-meta RPC and control_raw capability"
```

---

### Task 2: 传输、能力门禁与混版拒绝

**Files:**
- Modify: `crates/xl-store/src/mesh_gate.rs`
- Modify: `crates/xl-store/src/mesh_gate_tests.rs:208`
- Modify: `crates/xl-store/src/store/mesh_api.rs`（`ensure_control_raw_compatible`）
- Modify: `crates/xl-store/src/store/tests.rs`（`InProcessMesh`：字段、`set_control_raw`、health 覆盖、trait 转发）
- Modify: `crates/oss-server/src/router.rs:800`
- Modify: `crates/xl-store/src/store/tests_shrink.rs:511,520,532`
- Modify: `crates/xl-client/src/lib.rs`（`TonicMesh`、`MeshSvc`、localhost 往返测试）
- Test: `tests_control_migrate.rs` + `cargo test -p xl-client`

**Interfaces:**
```rust
// mesh_gate.rs
pub struct PeerCaps { /* ... */ pub control_raw: bool }
// mesh_api.rs，既有写门禁 + 专用位
pub(in crate::store) async fn ensure_control_raw_compatible(&self) -> Result<()>
// InProcessMesh：set_control_raw(&self, node: &str, v: Option<bool>)
```

- [ ] **Step 1: 写失败测试**

`InProcessMesh` 上加能力覆盖后写：`control_raw_gate_rejects_missing_capability`（`set_control_raw("n2", Some(false))` → `PeerIncompatible`；`None` → Ok）；`in_process_mesh_forwards_control_raw`（节点 0 经 transport 读节点 2 的副本）。xl-client 加 `control_raw_roundtrip`：localhost `serve` + `TonicMesh::read_control_raw` 返回与盘上一致。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store control_raw_gate && cargo test -p xl-client control_raw_roundtrip`
Expected: FAIL

- [ ] **Step 3: 接线**

`PeerCaps`/`from_health` 加 `control_raw`；`GatedMesh::read_control_raw` 直接转发（能力门禁在 Store 显式做）；`ensure_control_raw_compatible` 先调 `ensure_control_quorum_compatible(false)`，再逐 peer 查 `caps.control_raw`，任一失败 → `PeerIncompatible`；`TonicMesh` 与 `MeshSvc` 做 proto 双向转换；`InProcessMesh` 转发并支持 health 覆盖。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store control_raw -v && cargo test -p xl-client control_raw -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/mesh_gate.rs crates/xl-store/src/mesh_gate_tests.rs crates/xl-store/src/store/mesh_api.rs crates/xl-store/src/store/tests.rs crates/xl-store/src/store/tests_control_migrate.rs crates/xl-store/src/store/tests_shrink.rs crates/xl-client/src/lib.rs crates/oss-server/src/router.rs
git commit -m "feat(xl-store): gate raw control scanning on current peer capability"
```

---

### Task 3: 全槽扫描、规范分组与状态判定（只读）

**Files:**
- Create: `crates/xl-store/src/store/control_migrate.rs`
- Modify: `crates/xl-store/src/store.rs`（`mod control_migrate;` + `pub use`）
- Modify: `crates/xl-store/src/store/control.rs`（`validate_control_key`、`control_meta` 改 `pub(super)`）
- Test: `crates/xl-store/src/store/tests_control_migrate.rs`

**Interfaces:**
```rust
pub enum LegacyControlState { Missing, AlreadyCommitted, Certifiable, CompletionNeeded }
pub struct LegacyControlCopy {
    pub node: String, pub slot: u16, pub version: u64, pub token: u64,
    pub certified: bool, pub content: [u8; 32],
}
pub struct LegacyContentGroup {
    pub version: u64, pub content: [u8; 32],
    pub copies: usize, pub certified: usize, pub max_token: u64,
}
pub struct LegacyControlScan {
    pub key: String, pub n: u16, pub q_s: u16, pub observed: u16,
    pub copies: Vec<LegacyControlCopy>, pub groups: Vec<LegacyContentGroup>,
    pub chosen: Option<usize>, pub state: LegacyControlState,
    chosen_meta: Option<XlMeta>, // 私有；apply 用
}
impl Store {
    pub async fn scan_legacy_control_object(&self, key: &str) -> Result<LegacyControlScan>;
}
```

- [ ] **Step 1: 写失败测试**

在 `tests_control_migrate.rs` 加（用 `control_meta` 造生产形态元数据）：
`legacy_scan_groups_ignoring_token_and_certificate`（纯 helper：token/证书不同 → 同 hash；数据/版本/deleted 不同 → 不同 hash）；
`legacy_scan_certifiable_at_twelve_slots`（9 同内容 → `Certifiable`，chosen 组 9）；
`legacy_scan_below_quorum_fails`（8 同内容 → `CorruptMeta`）；
`legacy_scan_refuses_phase1_residue_and_non_control`；
`legacy_scan_requires_all_slots`（`mesh.down("n3")` 或摘一盘 `.oss.sys` → 失败）；
`legacy_scan_refuses_small_topology`（`Cluster::new(3,2)` → `Unsupported`）；
`legacy_scan_refuses_missing_control_raw`（`set_control_raw("n2", Some(false))` → `PeerIncompatible`）；
`legacy_scan_ranks_preserved_old_certificate`（v1 证书 1 份 + v2 证书 4 份 + 无 `Some(false)` → `AlreadyCommitted(v2)`）；
`classify_rejects_multiple_quorum_groups`（纯函数合成计数）。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store legacy_scan -v`
Expected: FAIL

- [ ] **Step 3: 实现扫描**

`validate_control_key` → 非集群 `Unsupported` → `ensure_control_raw_compatible` → `2*Q_s>N`/`N>=data+2` 门槛 → 本机 `mesh_read_control_raw` + 远端并发 `transport.read_control_raw`，逐节点要求 `slots_ok == drive_count`、总和 `== N`、槽号不重复；解码并执行 R2/R3 的拒绝与分组；`classify` 返回状态 + chosen 索引；chosen 组保留最高 token 的代表 meta 到私有字段。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store legacy_scan -v`
Expected: PASS；失败用例断言磁盘字节未变（本任务全只读）

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/store/control_migrate.rs crates/xl-store/src/store.rs crates/xl-store/src/store/control.rs crates/xl-store/src/store/tests_control_migrate.rs
git commit -m "feat(xl-store): scan and classify legacy control copies across the cluster"
```

---

### Task 4: fenced 证书写入、幂等与补齐

**Files:**
- Modify: `crates/xl-store/src/store/control_migrate.rs`
- Modify: `crates/xl-store/src/store/control.rs`（`ControlGuard`、`lease_lost`、`commit_token`、`control_lock`、`control_token_floor` 改 `pub(super)`；更新 `recertify_system_object` 文档注释指向集群 API）
- Test: `crates/xl-store/src/store/tests_control_migrate.rs`

**Interfaces:**
```rust
pub enum RecertifyOutcome {
    Missing,
    AlreadyCommitted { version: u64, certified: usize },
    Certified { version: u64, certified: usize, token: u64 },
}
impl Store {
    pub async fn recertify_system_object_cluster(&self, key: &str) -> Result<RecertifyOutcome>;
}
```

- [ ] **Step 1: 写失败测试**

`legacy_recertify_certifies_quorum_and_is_idempotent`（12 槽 9 份 → `Certified`；证书 ≥9、token > 旧 max；`get_system_object` 读到数据；第二次 `AlreadyCommitted` 且全盘字节不变）；
`legacy_recertify_below_quorum_writes_nothing`（**绑定负例**：8 份 → `CorruptMeta`，全盘 `xl.meta`/`.new` 快照不变，读仍 None）；
`legacy_recertify_completes_partial_certificate`（1 证书 + 8 legacy 同内容 → `Certified`，版本/内容不变）；
`legacy_recertify_never_overwrites_newer_committed`（构造 v2 证书 4 份 + v1 legacy 3 份 → `AlreadyCommitted`，v2 字节原样）；
`legacy_recertify_refuses_same_version_fork`（同版本两份不同内容，各 <Q_s → `CorruptMeta`，不写）；
`legacy_recertify_token_is_strictly_monotonic`（旧 token 高水位，新证书 token > 全部旧 token）；
`legacy_recertify_tombstone_certifies`（`control_deleted=Some(true)` → 证书落盘，`get_system_object` 仍 None）；
`legacy_recertify_mixed_version_no_writes`（`control_raw=false` → `PeerIncompatible`，字节不变）。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store legacy_recertify -v`
Expected: FAIL

- [ ] **Step 3: 实现 apply**

按 R4 顺序实现：floor → lock → lease → 锁内 scan → 状态分支 → `commit_token(0, max_token)` → 代表 meta 置证书 → lease → `write_meta_marker(..., &[])` → `flipped==0` 则 `QuorumUnmet`，`Indeterminate` 原样上抛。非集群 `Unsupported`。**不写数据阶段**。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store legacy_recertify -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/store/control_migrate.rs crates/xl-store/src/store/control.rs crates/xl-store/src/store/tests_control_migrate.rs
git commit -m "feat(xl-store): certify legacy control objects under the fenced marker protocol"
```

---

### Task 5: `oss-server control-recertify` CLI

**Files:**
- Create: `crates/oss-server/src/control_recertify.rs`
- Modify: `crates/oss-server/src/lib.rs`（`pub mod control_recertify;`）
- Modify: `crates/oss-server/src/cli.rs`（`USAGE`、`Command`、`parse_args`、`run` 分派；`needs_full_state` 保持 true）
- Test: `control_recertify.rs` 单元测试 + `cli.rs` 解析测试

**Interfaces:**
```rust
Command::ControlRecertify { keys: PathBuf, apply: bool, json: bool }
pub async fn run(store: &Store, keys: &Path, apply: bool, json: bool) -> Result<i32, Box<dyn Error + Send + Sync>>
```

- [ ] **Step 1: 写失败测试**

`parse_args_reads_control_recertify`（默认 `apply=false`；`--apply --json --keys-file k` 正确；缺 `--keys-file`/未知 flag → `USAGE`）；`keys_file_ignores_comments_and_rejects_empty_duplicate_non_control`（临时文件；含 `#`、空行、CRLF）。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server control_recertify -v && cargo test -p oss-server parse_args_reads_control_recertify -v`
Expected: FAIL

- [ ] **Step 3: 实现 CLI 与报告**

文件校验在任何扫描前完成；`state.store.cluster().is_none()` → 退出 1（提示单节点用库 API）。dry-run 调 `scan_legacy_control_object`，apply 调 `recertify_system_object_cluster`；逐键输出，`--json` 输出 `{applied, ok, keys:[{key,state,version,q_s,copies,certified,groups}]}`；任一错误/不可认证 exit 1，`Missing` 计 no-op；`Indeterminate` 专门打印“可能已提交，禁止假设回滚，重跑核对”。`run` 分支只在完整 state 下调用。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server control_recertify -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/control_recertify.rs crates/oss-server/src/lib.rs crates/oss-server/src/cli.rs
git commit -m "feat(oss-server): add control-recertify admin command"
```

---

### Task 6: 崩溃/中止注入与收敛测试

**Files:**
- Create: `crates/xl-store/tests/control_migrate_faults.rs`
- Modify: `crates/xl-store/tests/common/mod.rs`（加直接写盘 helper `write_disk_meta` / 读盘 helper，仅测试）
- Test: `cargo test -p xl-store --test control_migrate_faults`

**Interfaces:** 复用现有 `CHILD`/`PHASE`/`DIRS`/`ABORT_PREPARE`/`ABORT_CONTROL_COMMIT_K` 与 `open_store(dirs, None, true)`（12 盘单节点集群，Q_s=9、NoPeers）。

- [ ] **Step 1: 写失败测试**

`legacy_recertify_crash_after_first_certificate_converges`：9 份 legacy → 子进程 `OSS_TEST_ABORT_CONTROL_COMMIT_K=1` 调 apply → SIGABRT；父进程断言 1 份 `Some(true)` + 8 份 `None`、无可见版本；再跑一次不注入 → `Certified`/`get_system_object` 读到数据。
`legacy_recertify_crash_before_marker_keeps_legacy_and_reruns`：`OSS_TEST_ABORT_META_PREPARE=1` → 9 份 legacy `xl.meta` 字节不变（仅 `.new` 暂存）；重跑 → `Certified`。
两个用例都断言最终证书数 ≥9、token 严格递增。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store --test control_migrate_faults -- --test-threads=1`
Expected: FAIL

- [ ] **Step 3: 实现 helper/驱动并跑通**

子进程只调用公开 `recertify_system_object_cluster`；父进程用真实盘快照断言中间态与最终态。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store --test control_migrate_faults -- --test-threads=1 -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/tests/control_migrate_faults.rs crates/xl-store/tests/common/mod.rs
git commit -m "test(xl-store): crash and abort coverage for cluster control recertification"
```

---

### Task 7: 全量门禁与记录

- [ ] **Step 1:** `cargo test -p xl-store` → PASS
- [ ] **Step 2:** `cargo test -p xl-client` → PASS
- [ ] **Step 3:** `cargo test -p oss-server` → PASS（允许既有 timing flake，隔离重跑至绿，不弱化测试）
- [ ] **Step 4:** `cargo clippy -p xl-store -p xl-client -p oss-server --all-targets` → 0 new warnings
- [ ] **Step 5:** `cargo fmt --all -- --check` → PASS
- [ ] **Step 6:** 记录结果；任一失败不得进入 Phase 3 控制状态迁移。

## 门禁记录

- 日期/提交范围：2026-11-04，实现 `6e47586..2c0581b`（Task 1–6）+ `fbaf303`（Task 7 clippy 告警修复）；以下命令均以 `CARGO_INCREMENTAL=0` 运行。
- `cargo test -p xl-store`：PASS — 7 suites / 470 passed / 0 failed / 1 ignored（exit 0）
- `cargo test -p xl-client`：PASS — 2 suites / 8 passed / 0 failed / 0 ignored（exit 0）
- `cargo test -p oss-server`：PASS — 77 suites / 838 passed / 0 failed / 2 ignored（exit 0）
- `cargo clippy -p xl-store -p xl-client -p oss-server --all-targets`：PASS — 0 warnings（exit 0）
- `cargo fmt --all -- --check`：PASS（exit 0）
- 结论：全部门禁通过（含 Task 6 SIGABRT 故障注入），可进入 Phase 3 控制状态迁移。
