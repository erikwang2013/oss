# ControlStore 第一切片（vpc_endpoints）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在控制面引入后端中立的 `ControlStore` / `ControlTxn` 缝，并把最小的 `vpc_endpoints` 域迁移过去；SQLite 仍是默认后端，外部行为与现有测试逐字节不变。

**Architecture:** trait 放 `xl-store`（xl-s3 已依赖它），SQLite 适配器放 `oss-server`。读路径用 `Arc<dyn ControlStore>`（共享 `Db` 的 pool）；写路径用 `ControlTxn`，由调用方（Raft apply / snapshot install）持有事务，store 绝不自开/提交/回滚。本切片只做单表、无外键、无触发器的 `vpc_endpoints`，用双后端差分测试证明等价。

**Tech Stack:** Rust 2024、`async-trait`（xl-store 已有依赖）、sqlx/SQLite、tokio、rmp-serde。

**Spec:** Phase 0 设计（会话确认）+ `sqlite-scout` 的 Phase 1 边界提案；生产 SQL 约 584 处，本切片约 8 处。

## Global Constraints

- 现有测试全绿：`cargo test -p xl-store`、`cargo test -p oss-server`。
- 不改变外部行为：VPC/AP HTTP 状态码、错误文本前缀、静态 `[[ap.vpc_endpoints]]` 回退语义、快照字节布局（字段顺序）不变。
- 不新增依赖（`async-trait` 已是 xl-store 依赖）；不新增数据库/服务。
- `ControlStore` 必须复用 `Db` 的 pool，不得创建第二个 pool。
- `ControlTxn` 不得 begin/commit/rollback；调用方事务边界是唯一原子性来源。
- `vpc_endpoints` 的读/导出按 `vpce_id` 升序；`replace` = DELETE all + 按序 INSERT，且只在调用方校验之后。
- 每个任务结束必须提交；文件保持 <500 行（control_store 超了就拆）。
- **最终态零 SQLite**：本切片的 SQLite 适配器只是差分测试的参考实现，不是长期后端。最终切片必须删除 `sqlx` 依赖、`migrations/`、所有 SQL、`.db`/WAL/PRAGMA/触发器，并删除本切片引入的 SQLite 适配器与差分测试；CI 守卫 `rg` 零命中、`cargo tree -i sqlx` 为空。

## Review Focus

- **事务所有权**：store 自己开事务会静默破坏与 `raft_applied` 的原子性；`vpc_endpoints` 没有 guard trigger 能兜底。
- **快照兼容**：统一 `VpcEndpoint` 类型时，rmp/serde 字段顺序与 `deny_unknown_fields` 必须与现有 `VpcEndpointRow` 完全一致；旧快照必须可读。
- **账户状态校验**：`accounts.status == active` 必须在 apply 事务内、通过 `ControlTxn::account_status` 完成，不能绕回 SQL。
- **错误/HTTP 等价**：`sqlx::Error` → `ControlError` 后，API 500 状态与 `vpc_endpoints lookup: {e}` 前缀不变；DB 错误不得回退静态配置。
- **热路径/后续切片**：VPC 只证明缝；IAM/buckets 的 5/13 表级联、30+33 个触发器、SigV4 热路径留给后续切片，不要提前抽象。
- **Indeterminate 错误映射**：Phase 0 里 xl-s3 把 `Indeterminate` 折叠为 500；一旦控制写经 HTTP 暴露（Phase 3+），必须映射为明确的 409/503 + “重读对账、不得假设回滚”语义，不能当普通 500。

---

### Task 1: 类型、trait 与错误边界（xl-store）

**Files:**
- Create: `crates/xl-store/src/control_store.rs`
- Modify: `crates/xl-store/src/lib.rs`
- Test: 同文件 `#[cfg(test)]` 模块

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct VpcEndpoint {
      pub vpce_id: String,
      pub vpc_id: String,
      pub account_id: String,
      pub state: String,
      pub created_at: i64,
      pub updated_at: i64,
  }

  #[derive(Debug, thiserror::Error)]
  pub enum ControlError {
      #[error("control store backend: {0}")]
      Backend(String),
      #[error("invalid control data: {0}")]
      Invalid(String),
  }

  #[async_trait]
  pub trait ControlStore: Send + Sync {
      async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError>;
      async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError>;
  }

  #[async_trait]
  pub trait ControlTxn: Send {
      async fn account_status(&mut self, account_id: &str) -> Result<Option<String>, ControlError>;
      async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError>;
      async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError>;
      async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError>;
      async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError>;
  }
  ```
- 字段顺序与 `crates/oss-server/src/raft/data.rs:217-224` 的 `VpcEndpointRow` 完全一致；后续用 `pub type VpcEndpointRow = xl_store::control_store::VpcEndpoint;` 收敛。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn vpc_endpoint_rmp_field_order_matches_snapshot() {
    let row = VpcEndpoint { vpce_id: "vpce-1".into(), vpc_id: "vpc-1".into(),
        account_id: "acct".into(), state: "available".into(), created_at: 1, updated_at: 2 };
    let bytes = rmp_serde::to_vec_named(&row).unwrap();
    let decoded: VpcEndpoint = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(row, decoded);
    assert!(rmp_serde::from_slice::<VpcEndpoint>(&rmp_serde::to_vec_named(&row).unwrap()).is_ok());
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store vpc_endpoint_rmp_field_order_matches_snapshot -v`
Expected: FAIL（类型不存在）

- [ ] **Step 3: 实现类型/trait/错误**

只加类型和 trait，不写 SQLite 适配器；`lib.rs` 导出。`ControlError` 用 `thiserror`（已有依赖）。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store control_store -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/lib.rs
git commit -m "feat(xl-store): add ControlStore/ControlTxn seam and VpcEndpoint type"
```

---

### Task 2: SQLite 读适配器与读路径切换

**Files:**
- Create: `crates/oss-server/src/control_store.rs`
- Modify: `crates/oss-server/src/lib.rs`（mod 导出）
- Modify: `crates/oss-server/src/db.rs`（删除 `vpc_endpoint_get` / `vpc_endpoint_list`，约 2475-2519）
- Modify: `crates/oss-server/src/ap_endpoints.rs:33-48`
- Modify: `crates/oss-server/src/access_points_api.rs:682-710`
- Modify: `crates/oss-server/src/state.rs:879-886`
- Test: `crates/oss-server/src/ap_endpoints.rs` 现有测试 + 新 parity 测试

**Interfaces:**
- Produces:
  ```rust
  pub struct SqliteControlStore { pool: sqlx::SqlitePool }
  impl SqliteControlStore { pub fn new(pool: sqlx::SqlitePool) -> Self }
  #[async_trait] impl ControlStore for SqliteControlStore { ... }
  ```
- 调用方从 `Db` 取 pool 构造 `Arc<dyn ControlStore>`；不得 `SqlitePool::connect` 新池。静态配置回退只在 `None` 时发生；`Err` 直接上抛。

- [ ] **Step 1: 写失败测试（parity）**

```rust
#[tokio::test]
async fn vpc_endpoint_reads_match_legacy_rows_and_fallback_only_on_none() {
    // 两个 fresh DB：legacy db.vpc_endpoint_get vs SqliteControlStore;
    // 同一行插入后比较完整字段；DB 错误时 ap_endpoints 返回 Err（不回退静态配置）。
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server vpc_endpoint_reads_match -v`
Expected: FAIL

- [ ] **Step 3: 实现适配器并切换读路径**

`SqliteControlStore` 的 `vpc_endpoints` 用 `ORDER BY vpce_id`；`vpc_endpoint` 点查。`ap_endpoints`/AP API/state 读改用 trait；`db.rs` 旧方法删除。保持 `Arc<dyn ControlStore>` 注入（AppState 或 Db 的 getter，选改动最小者，但只共享 pool）。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server vpc -v`
Expected: PASS；现有 AP 测试全绿。

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/control_store.rs crates/oss-server/src/lib.rs crates/oss-server/src/db.rs crates/oss-server/src/ap_endpoints.rs crates/oss-server/src/access_points_api.rs crates/oss-server/src/state.rs
git commit -m "refactor(oss-server): read VPC endpoints through ControlStore"
```

---

### Task 3: SQLite 事务适配器与 apply 路径

**Files:**
- Modify: `crates/oss-server/src/control_store.rs`（`SqliteControlTxn`）
- Modify: `crates/oss-server/src/raft/apply.rs:826-868`
- Test: `crates/oss-server/src/raft/apply.rs` VPC 用例（1633+）

**Interfaces:**
- Produces:
  ```rust
  pub struct SqliteControlTxn<'a> { conn: &'a mut sqlx::SqliteConnection }
  impl<'a> SqliteControlTxn<'a> { pub fn new(conn: &'a mut sqlx::SqliteConnection) -> Self }
  #[async_trait] impl ControlTxn for SqliteControlTxn<'_> { ... }
  ```
- `account_status` 读 `accounts.status`；`vpc_endpoint_upsert` 保留 created_at、更新 vpc_id/account_id/state/updated_at；`delete` 幂等；`vpc_endpoints` 按 `vpce_id` 排序；`replace` DELETE + INSERT（不 begin/commit）。

- [ ] **Step 1: 写失败测试（apply 等价 + 回滚）**

```rust
#[tokio::test]
async fn vpc_apply_upsert_delete_matches_legacy_and_rolls_back() {
    // 同一 apply tx 内：upsert -> 校验 -> delete；断言 ControlResponse 与行内容；
    // 故意失败后 rollback，vpc_endpoints 仍为空。
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server vpc_apply_upsert_delete -v`
Expected: FAIL

- [ ] **Step 3: 实现 txn 适配器并替换 apply SQL**

`apply_command` 里用 `SqliteControlTxn::new(cw.conn())`；保持 `ControlResponse::Invalid` 映射与账户状态校验顺序。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server vpc -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/control_store.rs crates/oss-server/src/raft/apply.rs
git commit -m "refactor(oss-server): apply VPC endpoint writes through ControlTxn"
```

---

### Task 4: 快照导出/安装接线

**Files:**
- Modify: `crates/oss-server/src/raft/data.rs:594-611`（export）
- Modify: `crates/oss-server/src/raft/data.rs:826-843`（install）
- Test: `crates/oss-server/src/raft/data.rs` VPC round-trip（1818+）与快照/feature-level 测试（2283+）

**Interfaces:**
- `read_tx` 用 `ControlTxn::vpc_endpoints` 填充 `ControlData.vpc_endpoints`；`install_tx` 先用现有 706-718 校验，再 `replace_vpc_endpoints`。`VpcEndpointRow` 变为类型别名；快照字段顺序不变。
- `cutover` 的 `diff_control` 维持现状（不比 vpc/ledger），本切片只加 round-trip 测试。

- [ ] **Step 1: 写失败测试（快照 round-trip）**
- [ ] **Step 2: 运行并确认失败**
- [ ] **Step 3: 接线 export/install**
- [ ] **Step 4: 运行 VPC/快照测试**
- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/raft/data.rs
git commit -m "refactor(oss-server): export/install VPC endpoints through ControlTxn"
```

---

### Task 5: 双后端差分测试

**Files:**
- Create: `crates/oss-server/tests/control_store_vpc_diff.rs`
- Test helper: 内存 `ControlStore`/`ControlTxn`（测试文件内，或 xl-store `#[cfg(test)]`）

**Interfaces:**
- 同一操作序列分别跑 legacy SQL 与 `SqliteControlStore`/`SqliteControlTxn`（两个 fresh SQLite DB），比较完整 `VpcEndpoint` 行、`ControlResponse`、快照字节（rmp 字段顺序）、HTTP/静态回退行为。
- 覆盖：upsert 保留 created_at、delete 幂等、list 排序、apply 回滚、快照 install 后读一致、DB 错误不回退静态配置。

- [ ] **Step 1: 写差分测试**
- [ ] **Step 2: 运行并确认失败（若实现未等价）**
- [ ] **Step 3: 修实现，不改断言**
- [ ] **Step 4: 运行 `cargo test -p oss-server control_store_vpc_diff`**
- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/tests/control_store_vpc_diff.rs
git commit -m "test(oss-server): differential coverage for VPC ControlStore slice"
```

---

### Task 6: 切片门禁

- [ ] **Step 1:** `cargo test -p xl-store` → PASS
- [ ] **Step 2:** `cargo test -p oss-server` → PASS（全量；允许既有 timing flake，单独重跑至绿，不弱化测试）
- [ ] **Step 3:** `cargo clippy -p oss-server -p xl-store --all-targets` → 无新增 warning
- [ ] **Step 4:** `cargo fmt --check` → PASS
- [ ] **Step 5:** 记录结果；任何失败不得进入下一个切片（AP/OLAP → lifecycle/inventory → buckets → IAM → jobs/sites）

## 后续切片顺序

1. 本切片：`vpc_endpoints`（单表、0 FK、0 trigger）
2. AP/OLAP（`access_points` / `olap_configs`，含 AP 缓存）
3. lifecycle / inventory / tier（full-bucket replace、cursor upsert）
4. buckets / policies / event / replication（13 表级联、30+33 触发器）
5. IAM / accounts / quota（5 表级联、last-admin、SigV4 热路径）——最后做
6. jobs/outbox/sites/audit（队列、lease、LWW changelog）

## Legacy 控制对象迁移（Phase 3 前置）

Phase 0 的 `recertify_system_object` 只支持**真正的单节点部署**（所有盘本地）。集群的 legacy `control_committed=None` 控制对象不支持的：必须在 Phase 3 实现一个跨节点 raw-copy 扫描工具，按 `(control_version, content)` 分组，只有 ≥Q_s 个同内容副本才写证书，否则 `CorruptMeta`；不得用单节点逐盘 recertify 代替。真实控制状态迁移前必须通过这个门禁。

## 最终切片：SQLite 彻底移除（硬门禁，不可跳过）

前置：所有 `ControlStore`/`ControlTxn` 调用点已迁到对象后端；双后端差分全绿。

- [ ] 删除 `crates/oss-server/src/control_store.rs` 的 SQLite 适配器与 `SqliteControlTxn`
- [ ] 删除 `crates/oss-server/migrations/`、`db.rs` 的 SQL、`raft/store.rs`/`raft/data.rs` 的 SQL、所有 `sqlx::query*` 调用
- [ ] 从 `Cargo.toml`/`Cargo.lock` 移除 `sqlx` 与 `sqlite` feature；`cargo tree -i sqlx` 为空
- [ ] 删除 `oss.toml` 的 `database_url`、所有 `.db`/`-wal`/`-shm` 文件、`PRAGMA`/`BEGIN IMMEDIATE`/WAL 注释、SQLite 触发器
- [ ] 测试/fixture 全部迁到对象/内存后端；删除 legacy-vs-store 差分测试（等价已证明）
- [ ] CI 守卫：`rg -n "sqlx|SqlitePool|sqlite" crates Cargo.toml` 在生产路径零命中；`find . -name '*.db*'` 为空；`crates/oss-server/migrations/` 不存在
- [ ] E2E：空目录启动 → 建用户/桶/规则/作业 → 重启 → 数据仍在；过程中不生成 `oss.db`/`raft.db`
- [ ] 全量 `cargo test --workspace` + `cargo clippy --all-targets` + `cargo fmt --check`

**通过标准：仓库里没有 SQLite 依赖、没有 SQL、没有迁移目录、没有 .db 文件；控制面只使用对象后端。**
