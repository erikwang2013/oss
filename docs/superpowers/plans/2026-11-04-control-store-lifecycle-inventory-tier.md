# ControlStore 第三切片（lifecycle / inventory / tier）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `lifecycle_rules` / `lifecycle_cursors` / `inventory_configs` / `inventory_runs` / `inventory_cursors` / `inventory_incremental` / `tier_restores`（含 Raft apply、快照导出/安装、console/job/worker 读路径、桶删除级联）迁移到既有 `ControlStore` / `ControlTxn` 缝；SQLite 仍是默认后端，外部行为、HTTP 状态/文本、快照字节布局/feature level、worker 确定性全部不变。

**Architecture:** 读路径经 `Arc<dyn ControlStore>`（共享 `Db` pool），写路径经调用方事务内的 `ControlTxn`（Raft apply / snapshot install / single-node console 各持自己的事务，适配器绝不 begin/commit/rollback）。缝类型只认自然键；`inventory_runs` 的本地自增 `id` 作为行内不可变属性保留（执行器/快照顺序需要），但不作为 apply 身份键。域 trait 拆到 `xl-store/src/control_store/{lifecycle,inventory,tier}.rs`，SQLite 适配器拆到 `oss-server/src/control_store/{lifecycle,inventory,tier}.rs`，各文件 <500 行。

**Tech Stack:** Rust 2024、`async-trait`、sqlx/SQLite、tokio、rmp-serde、serde_json、base64（均已有依赖）。

**Spec:** Phase 0 设计 + 第一/第二切片（`docs/superpowers/plans/2026-11-03-control-store-vpc-slice.md`、`docs/superpowers/plans/2026-11-04-control-store-ap-olap-slice.md`）的缝约定。生产 SQL 目标：`db.rs` 的 lifecycle/inventory/tier 读视图与 raw 写助手、`raft/apply.rs` 及其子模块、`raft/data.rs`、`raft/runstate/store.rs` 五表、`job_state/{inventory,lifecycle,restore}.rs` 读路径。

## Global Constraints

- 现有测试全绿：`cargo test -p xl-store`、`cargo test -p oss-server`。
- **外部行为不变**：HTTP 状态码/错误文本、console JSON（含 inventory run `id`）、快照字节布局、`ControlData` / `RunState`（`m8.runstate` v1）字段名与顺序、feature level 3/5/7 判定、worker 入队/执行/收尾确定性不变。
- 缝必须后端中立：身份用自然键；本地数字 id 不得作为身份键。`inventory_runs.id` 是不可变行属性（快照顺序、执行器 API、console 展示需要），`owner_uid` 只作为运行期本地外键投影保留，二者都必须在类型注释中写明“非身份键”。
- `ControlTxn` 不得 begin/commit/rollback；事务边界是调用方唯一原子性来源。
- 复用 `Db` 的 pool，不建第二个 pool；不新增依赖；文件 <500 行。
- 桶删除级联必须走 txn；`clear_bucket_m8_tx` 与 `delete_bucket_cascade` 不保留这 7 张表的第二条 SQL 路径。
- 快照 export/install 必须经 `ControlTxn`；两套属主口径不得混用（见决策记录 4）。
- 双后端差分测试用真实 SQLite；legacy raw-SQL 参考实现只存在于测试文件。
- TDD：每个任务先写失败测试（RED）→ 最小实现（GREEN）→ 提交；任何门禁失败不得进入下一任务。
- 已知 5s 控制面状态探测 flake 可隔离重跑，绝不弱化断言。

## Review Focus

- **inventory run 身份漂移**：同一 `(bucket, config_id, run_key)` 在不同 `id` 下重放必须 `Conflict` 且零写入；快照安装必须保留显式 `id`、`ORDER BY id` 与 `next_run_id` 高水位，桶删后重建不得复用 id。测试：Task 4 `inventory_run_natural_key_conflict_is_zero_write`、`inventory_run_snapshot_preserves_ids_and_order`，Task 6 runstate round-trip。
- **fence_term 丢失**：seam 的 cursor/restore upsert/delete 必须保留 `WHERE fence_term <= excluded.fence_term`（inventory cursor delete 的本地无 fence 口径单独用 `None` 表达）；旧 leader 迟到写入必须 no-op。测试：Task 2/4/5 的 fence 测试 + 现有 `raft_lifecycle_cursor_replication`、`raft_inventory_replication`、`raft_restore_failover`。
- **整桶替换误删别的桶**：`replace_lifecycle_rules` 的 DELETE 必须带 `bucket = ?`（含 cursor 清理），只有快照安装的 `replace_all_*` 才允许全表替换。测试：Task 2 `replace_lifecycle_rules_is_bucket_scoped_and_clears_cursors`、Task 6 多桶 round-trip。
- **快照顺序 / 悬空属主 / 悬空作业引用**：lifecycle 行序 `(bucket, rule_id, prefix)`、inventory config `(bucket, id)`、runs `id`、cursors `(bucket, config_id)`、restores `(bucket, key, version_id)` 不得变；悬空 inventory 属主导出 fail-closed，安装必须在任何写之前预校验。测试：Task 6 `control_data_lifecycle_and_inventory_round_trip_preserves_order`、`snapshot_install_rejects_dangling_inventory_owner_before_writes`、`runstate_five_tables_round_trip_preserves_order`。
- **级联漏行**：7 张表在两个桶删除入口都清空，其他桶不受影响；site 同步入口只保留既有表范围但 SQL 也必须走 txn。测试：Task 7 `cascade_deletes_all_seven_tables_and_spares_other_bucket`、`site_bucket_delete_keeps_scope_but_uses_txn`，Task 8 端到端 diff。

---

## 决策记录（执行者必须按此实现，不再另选）

**1. `inventory_runs` 的身份键 = `(bucket, config_id, run_key)`，`id` 是不可变属性。**
`id` 由 SQLite `AUTOINCREMENT` 在本地分配，随行保存；apply 预检仍按 `id` 查当前行做状态机/身份校验，再用自然键冲突目标 upsert。自然键冲突但 `id` 不同由预检判 `Conflict`（零写入），不能让 SQL UNIQUE 触发 rows_affected 不变量 fail-stop。快照按 `ORDER BY id` 导出/安装，显式 id 插入推进 `sqlite_sequence`；`next_run_id` 高水位仍由 `runstate_meta` + `MAX(id)` 维护（本切片不改分配器，见决策 8）。

**2. 运行期 inventory config 视图带本地外键投影 `owner_uid`。**
`InventoryConfigEntry`（快照）只有自然键 `owner: OwnerRef`。运行期 `InventoryConfigView` 额外带 `owner_uid: i64`（原 `inventory_configs.owner_id`）与 `owner: Option<OwnerRef>`（`None` = 用户行缺失/悬空）：执行器的 `get_user(owner_uid)`、`AuthedUser.uid`、任务 `requested_by`、错误文本 `配置所有者 {uid} 不存在，拒绝运行清单` 都逐字保持。`owner_uid` 是本地执行属性，不是 seam 身份键；console/JSON 仍按既有字段输出。

**3. 域 trait 用 supertrait 拆分，控制面根 trait 只做聚合。**
`ControlStore: LifecycleStore + InventoryStore + TierStore`，`ControlTxn: LifecycleTxn + InventoryTxn + TierTxn`（逐任务加 supertrait，见 Task 2-5）。`dyn ControlStore` / `dyn ControlTxn` 仍可调用全部方法；根文件保持 <500 行。

**4. 两套口径，不得混用。**
- 运行期读（`ControlStore`）：`owner_id == 0` → `Some(Admin)`；非 0 且用户行存在 → `Some(User(username))`（管理员也读 `User`，保持 console `owner_uid` 精确）；用户行缺失 → `None`（engine fail-closed）。
- 快照导出（`ControlTxn::inventory_configs`）：`owner_id == 0 || is_admin == 1` → `Admin`；否则 `User(username)`；缺失 → `ControlError::Invalid("owner user id {id} missing")`。
- 快照安装：`OwnerRef::Admin` → 本地管理员 id（witness 0，无管理员且非 witness → `DataError::NoAdminUser`）；`OwnerRef::User(name)` → 已安装的 `users.id`，缺失 → `DataError::Format("inventory owner {name} not in snapshot")`。预校验在任何写之前完成。

**5. 桶删除级联一个 txn 方法。**
`ControlTxn::delete_lifecycle_inventory_tier_for_bucket(bucket)` 按固定顺序 DELETE：`inventory_runs`、`inventory_configs`、`inventory_cursors`、`inventory_incremental`、`tier_restores`、`lifecycle_rules`、`lifecycle_cursors`。`clear_bucket_m8_tx` / `delete_bucket_cascade` 在同一调用方事务内先 `delete_access_points_for_bucket` 再调用它；不保留逐表 SQL。

**6. site 同步的桶删除保持既有表范围。**
`site.rs::apply_bucket` 的 delete 分支原本只删 `lifecycle_rules`（不删 inventory/tier/游标）。本切片只把这条 SQL 改为 `ControlTxn::delete_lifecycle_rules_only(bucket)`（rules-only，不扩大级联范围），避免改变站点同步行为。

**7. 单机写门面保持现有数字 uid 签名。**
`Db::inventory_upsert_config(..., owner_id: i64, ...)` 内部 `owner_ref(owner_id)` 解析自然键后委托 txn（uid 必须存在，`RowNotFound`；0 → `Admin`）。其余 `Db::*_set` / `Db::inventory_create_run` 等签名不变，内部统一 `self.pool.begin()` → `SqliteControlTxn` → commit。

**8. 本切片明确不迁的两处 raw SQL（由 jobs 切片负责）。**
`runstate_meta` 的 `next_run_id` 读写（含 `apply/inventory.rs` 中 `apply_run` 后的一次 `INSERT ... ON CONFLICT` 和 `job_state::inventory::next_run_id` 的 meta+MAX 查询），以及 `Db::purge_terminal_jobs_raw` 对 `inventory_runs.job_id` / `tier_restores.job_id` 的置空 UPDATE。它们保留原语义并在计划中标注，其他生产路径不得再有这 7 张表的 raw SQL（测试除外）。

---

## File Structure

- `crates/xl-store/src/control_store.rs`：`OwnerRef`、现有 VPC/AP 类型与 trait；新增 `pub mod {lifecycle,inventory,tier}` 与 re-export；`ControlStore`/`ControlTxn` 聚合 supertrait；级联方法签名。
- `crates/xl-store/src/control_store/lifecycle.rs`（新建）：`LifecycleRuleEntry`、`LifecycleCursorRow`、`LifecycleStore`、`LifecycleTxn`、冻结测试。
- `crates/xl-store/src/control_store/inventory.rs`（新建）：`InventoryConfigEntry`、`InventoryConfigView`、`InventoryRunRow`、`InventoryCursorRow`、`InventoryIncrementalRow`、`InventoryStore`、`InventoryTxn`、冻结测试。
- `crates/xl-store/src/control_store/tier.rs`（新建）：`TierRestoreRow`、`TierStore`、`TierTxn`、冻结测试。
- `crates/oss-server/src/control_store.rs`：模块根；新增 `mod {lifecycle,inventory,tier};`；级联实现；`to_sqlx`。
- `crates/oss-server/src/control_store/lifecycle.rs`（新建）：lifecycle SQL 适配器。
- `crates/oss-server/src/control_store/inventory.rs`（新建）：inventory config/run/cursor SQL 适配器。
- `crates/oss-server/src/control_store/tier.rs`（新建）：tier restore SQL 适配器。
- `crates/oss-server/src/raft/runstate/model.rs`：5 个行类型改为 `pub use xl_store::control_store::...`（`RunState`/`Job*`/`ScheduleLeaseRow` 不动）。
- `crates/oss-server/src/raft/data.rs`：`LifecycleRuleRow`/`InventoryConfigRow` 变别名；快照读写走 txn；inventory 属主预校验。
- `crates/oss-server/src/raft/apply.rs`、`raft/apply/{lifecycle,inventory,tier_restore}.rs`：apply 读写走 txn；删 `admin_owner_id`、`current_restore`、`apply_restore` 与各 raw helper。
- `crates/oss-server/src/raft/runstate/store.rs`：5 表读/替换走 txn；`runstate_meta`/jobs 保持 raw。
- `crates/oss-server/src/db.rs`：读视图与单机写门面改委托；删 raw helper；两个桶删除入口改 txn。
- `crates/oss-server/src/job_state/{inventory,lifecycle,restore}.rs`：读路径走 `db.control_store()`；写门面签名不变。
- `crates/oss-server/src/site.rs`：lifecycle rules-only 删除走 txn。
- `crates/oss-server/tests/control_store_lifecycle_diff.rs`（新建，Task 2）。
- `crates/oss-server/tests/control_store_inventory_diff.rs`（新建，Task 3/4；若超 500 行在 Task 4 拆出 `control_store_inventory_runstate_diff.rs`）。
- `crates/oss-server/tests/control_store_tier_diff.rs`（新建，Task 5）。
- `crates/oss-server/tests/control_store_lifecycle_inventory_tier_diff.rs`（新建，Task 8）。
- `crates/oss-server/tests/support/control_store_rows.rs`（新建）：legacy raw-SQL 参考实现与全表 dump。

---

### Task 1: 行类型上移 xl-store（字节冻结）

**Files:**
- Create: `crates/xl-store/src/control_store/lifecycle.rs`
- Create: `crates/xl-store/src/control_store/inventory.rs`
- Create: `crates/xl-store/src/control_store/tier.rs`
- Modify: `crates/xl-store/src/control_store.rs`（`pub mod` + re-export；本任务不改 trait）
- Modify: `crates/oss-server/src/raft/runstate/model.rs:1-152`（5 个类型改 re-export）
- Modify: `crates/oss-server/src/raft/data.rs:117-176`（两个类型改别名）
- Modify: `crates/oss-server/src/db.rs:169-205, 4772-4788`（删本地类型，改别名/re-export）
- Test: 三个新文件的 `#[cfg(test)]`；`cargo test -p oss-server --lib raft::runstate`

**Interfaces:**
- Consumes: 无。
- Produces（字段顺序即快照顺序，逐字复制，不得重排）:
  ```rust
  // lifecycle.rs
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct LifecycleRuleEntry {
      pub bucket: String,
      pub prefix: String,
      pub rule_id: Option<String>,
      pub expire_days: i64,
      pub expiration_days: Option<i64>,
      pub transition_days: Option<i64>,
      pub transition_class: Option<String>,
      pub transition_tier: Option<String>,
      pub enabled: bool,
      pub updated_at: i64,
      #[serde(default)]
      pub config_json: Option<String>,
  }

  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
  #[serde(deny_unknown_fields)]
  pub struct LifecycleCursorRow { /* bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader */ }

  // inventory.rs
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct InventoryConfigEntry {
      pub bucket: String,
      pub id: String,
      pub owner: OwnerRef,
      pub config_json: String,
      pub updated_at: i64,
  }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct InventoryConfigView {
      pub bucket: String,
      pub id: String,
      pub owner: Option<OwnerRef>,
      /// 本地外键投影（原 inventory_configs.owner_id）；不是 seam 身份键。
      pub owner_uid: i64,
      pub config_json: String,
      pub updated_at: i64,
  }

  // 原样上移：InventoryRunRow { id, bucket, config_id, run_key, started_at, state,
  // job_id, manifest_key, error, created_at, fence_term, fence_leader }
  // InventoryCursorRow { bucket, config_id, cursor, config_updated_at, updated_at,
  // fence_term, fence_leader }
  // InventoryIncrementalRow { bucket, config_id, enabled, updated_at }

  // tier.rs
  // TierRestoreRow { bucket, key, version_id, expires_at, job_id, state, created_at,
  // fence_term, fence_leader }
  ```
- `raft::runstate::model` re-export：
  ```rust
  pub use xl_store::control_store::inventory::{
      InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
  };
  pub use xl_store::control_store::lifecycle::LifecycleCursorRow;
  pub use xl_store::control_store::tier::TierRestoreRow;
  ```
- `raft/data.rs`：`pub type LifecycleRuleRow = xl_store::control_store::LifecycleRuleEntry;`、`pub type InventoryConfigRow = xl_store::control_store::InventoryConfigEntry;`。
- ` db.rs`：`pub use xl_store::control_store::LifecycleRuleEntry as LifecycleRuleRow;`、`pub use xl_store::control_store::inventory::InventoryRunRow;`；删本地定义与 `inventory_run_row`（若无调用点则一并删；有则补 fence 两列）。

- [ ] **Step 1: 写失败测试（xl-store 冻结：rmp 字段顺序、`deny_unknown_fields`、JSON 形态）**

每个类型一个测试，例：
```rust
#[test]
fn inventory_run_rmp_matches_runstate_extension_field_order() {
    let row = InventoryRunRow { id: 7, bucket: "b".into(), config_id: "c".into(),
        run_key: "k".into(), started_at: 1, state: "running".into(), job_id: Some(2),
        manifest_key: None, error: Some("e".into()), created_at: 3, fence_term: 4,
        fence_leader: 5 };
    struct Legacy { /* 同字段同顺序 */ }
    let bytes = rmp_serde::to_vec_named(&row).unwrap();
    assert_eq!(bytes, rmp_serde::to_vec_named(&legacy_from(&row)).unwrap());
    assert_eq!(rmp_serde::from_slice::<InventoryRunRow>(&bytes).unwrap(), row);
    // 多一个 future_field 必须整行拒绝
}
```
测试名：`lifecycle_rule_rmp_matches_snapshot_field_order`、`lifecycle_cursor_rmp_matches_runstate_extension`、`inventory_config_rmp_matches_snapshot_field_order`、`inventory_run_rmp_matches_runstate_extension`、`inventory_cursor_rmp_matches_runstate_extension`、`inventory_incremental_rmp_matches_runstate_extension`、`tier_restore_rmp_matches_runstate_extension`；每个再加 `*_rejects_unknown_fields`。

- [ ] **Step 2: 运行确认 RED**

Run: `cargo test -p xl-store inventory_run_rmp_matches_runstate_extension -v`
Expected: FAIL（`InventoryRunRow` 未在 `xl_store::control_store` 定义 / 模块不存在）。

- [ ] **Step 3: 实现类型上移与别名**

原样搬移 `raft/runstate/model.rs` 的 5 个 struct（derive/serde 属性不变）；`data.rs`、`db.rs` 只改别名，所有命名构造字面量不动。`db.rs` 里 `inventory_run_row` 若保留需读满 12 列。

- [ ] **Step 4: 运行确认 GREEN**

Run: `cargo test -p xl-store -v`
Run: `cargo test -p oss-server --lib raft::runstate -v`
Run: `cargo test -p oss-server --test compat_fixtures -v`
Expected: PASS（旧 fixtures/extension 字节不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/control_store/ \
  crates/oss-server/src/raft/runstate/model.rs crates/oss-server/src/raft/data.rs crates/oss-server/src/db.rs
git commit -m "refactor(xl-store): move lifecycle/inventory/tier rows into ControlStore seam"
```

---

### Task 2: lifecycle 读写、apply、单机门面走 seam

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`（`ControlStore: LifecycleStore`、`ControlTxn: LifecycleTxn`）
- Modify: `crates/xl-store/src/control_store/lifecycle.rs`（trait 方法）
- Create: `crates/oss-server/src/control_store/lifecycle.rs`
- Modify: `crates/oss-server/src/control_store.rs`
- Modify: `crates/oss-server/src/db.rs:4828-4960, 7036-7088`
- Modify: `crates/oss-server/src/raft/apply.rs:527-570, 833-840`
- Modify: `crates/oss-server/src/raft/apply/lifecycle.rs`
- Modify: `crates/oss-server/src/job_state/lifecycle.rs:15-35`
- Test: `crates/oss-server/tests/control_store_lifecycle_diff.rs`（新建）
- Test: 现有 `crates/oss-server/tests/lifecycle.rs`、`m8_lifecycle_api.rs`、`raft_lifecycle_cursor_replication.rs`

**Interfaces:**
- Consumes: Task 1 的 `LifecycleRuleEntry`、`LifecycleCursorRow`。
- Produces:
  ```rust
  #[async_trait::async_trait]
  pub trait LifecycleStore: Send + Sync {
      /// 按 (rule_id, prefix) 升序（NULL rule_id 由 SQLite 排最前）。
      async fn lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
      /// enabled = 1，按 (bucket, rule_id) 升序。
      async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
      async fn lifecycle_cursor(&self, bucket: &str, rule_id: &str, stage: &str)
          -> Result<Option<String>, ControlError>;
      /// 整行读（job_state/apply 对账需要 fence 字段）。
      async fn lifecycle_cursor_row(&self, bucket: &str, rule_id: &str, stage: &str)
          -> Result<Option<LifecycleCursorRow>, ControlError>;
  }

  #[async_trait::async_trait]
  pub trait LifecycleTxn: Send {
      /// DELETE 本桶 rules + cursors，再按入参顺序 INSERT；行的 updated_at 原样写入。
      async fn replace_lifecycle_rules(&mut self, bucket: &str, rows: &[LifecycleRuleEntry])
          -> Result<(), ControlError>;
      /// DELETE 全部 rules（快照安装专用；不碰 cursors）。
      async fn replace_all_lifecycle_rules(&mut self, rows: &[LifecycleRuleEntry])
          -> Result<(), ControlError>;
      /// DELETE 本桶 rules + cursors（PUT 空列表/清规则）。
      async fn clear_lifecycle_rules(&mut self, bucket: &str) -> Result<(), ControlError>;
      /// 只 DELETE rules（site 同步桶删除保持既有范围）。
      async fn delete_lifecycle_rules_only(&mut self, bucket: &str) -> Result<(), ControlError>;
      /// 快照导出：按 (bucket, rule_id, prefix) 升序。
      async fn all_lifecycle_rules(&mut self) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
      async fn lifecycle_cursor(&mut self, bucket: &str, rule_id: &str, stage: &str)
          -> Result<Option<LifecycleCursorRow>, ControlError>;
      /// 返回是否恰好影响 1 行。
      async fn lifecycle_cursor_upsert(&mut self, row: &LifecycleCursorRow)
          -> Result<bool, ControlError>;
      async fn lifecycle_cursor_delete(&mut self, bucket: &str, rule_id: &str, stage: &str,
          fence_term: i64) -> Result<(), ControlError>;
      /// 快照导出：按 (bucket, rule_id, stage) 升序。
      async fn lifecycle_cursors(&mut self) -> Result<Vec<LifecycleCursorRow>, ControlError>;
      async fn replace_lifecycle_cursors(&mut self, rows: &[LifecycleCursorRow])
          -> Result<(), ControlError>;
  }
  ```

- [ ] **Step 1: 写失败测试（真实 SQLite + legacy raw 参考）**

在 `tests/control_store_lifecycle_diff.rs`（`#[path = "support/control_store_rows.rs"] mod helpers;`）：
```rust
#[tokio::test]
async fn lifecycle_reads_match_legacy_order_and_null_rule_ids() { /* 两个桶、3 行含 NULL rule_id；比较 Vec<LifecycleRuleEntry> 与 raw ORDER BY */ }

#[tokio::test]
async fn replace_lifecycle_rules_is_bucket_scoped_and_clears_cursors() {
    // b1/b2 各有 rules+cursor；replace b1 → b1 替换、b1 cursor 消失、b2 不变；
    // 行的 updated_at 采用行内值（不是额外参数）。
}

#[tokio::test]
async fn lifecycle_cursor_upsert_and_delete_are_fence_bounded() {
    // 现有 fence_term=5；term=4 upsert 返回 false 且行不变；term=5 返回 true；
    // delete(4) no-op；delete(5) 删除。
}

#[tokio::test]
async fn db_lifecycle_facades_preserve_signatures_and_now() {
    // Db::set_lifecycle_rules(bucket, rows, now) 写入 now；list order；cursor_get/set。
}
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_lifecycle_diff -v`
Expected: FAIL（trait 方法/适配器不存在）。

- [ ] **Step 3: 实现适配器并切读写路径**

- `control_store/lifecycle.rs`：三条 SELECT 用固定 SQL 常量；`replace_lifecycle_rules` 的 `DELETE` 必带 `bucket = ?`；upsert SQL 保留 `WHERE lifecycle_cursors.fence_term <= excluded.fence_term`；delete 保留 `fence_term <= ?`。
- `db.rs` 读方法改 `self.control_store().lifecycle_rules/enabled_lifecycle_rules/lifecycle_cursor`；`set_lifecycle_rules` 在一个 `pool.begin()` 内把 `now` 写进每行 `updated_at` 后调 txn；`lifecycle_cursor_set` 同事务内 upsert/delete。删 `set_lifecycle_rules_raw`、`clear_lifecycle_rules_raw`。
- `apply.rs`：`BucketSetLifecycle`/`V2` 用 `lifecycle_rows*` 的 `updated_at` 调 `replace_lifecycle_rules`；`BucketClearLifecycle` 调 `clear_lifecycle_rules`；`apply/lifecycle.rs` 的 `current`/`upsert` 改 txn 调用，`apply` 期望 `lifecycle_cursor_upsert == true`，`clear` 调 `lifecycle_cursor_delete(..., fence_term)`。
- `job_state/lifecycle.rs::read_lifecycle_cursor` 改 `db.control_store().lifecycle_cursor_row(...)`（整行含 fence；`lifecycle_cursor` 的 `Option<String>` 保留给 console/engine，二者共用 SQL）。

- [ ] **Step 4: 确认 GREEN + 回归**

Run: `cargo test -p oss-server --test control_store_lifecycle_diff -v`
Run: `cargo test -p oss-server --test lifecycle --test m8_lifecycle_api --test raft_lifecycle_cursor_replication -v`
Run: `cargo test -p oss-server --lib list_lifecycle_rules -v`
Expected: PASS（失败信息/顺序/`updated_at` 不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/control_store/lifecycle.rs \
  crates/oss-server/src/control_store.rs crates/oss-server/src/control_store/lifecycle.rs \
  crates/oss-server/src/db.rs crates/oss-server/src/raft/apply.rs \
  crates/oss-server/src/raft/apply/lifecycle.rs crates/oss-server/src/job_state/lifecycle.rs \
  crates/oss-server/tests/control_store_lifecycle_diff.rs crates/oss-server/tests/support/control_store_rows.rs
git commit -m "refactor(oss-server): route lifecycle state through ControlTxn"
```

---

### Task 3: inventory config 读写、运行期视图、apply InventoryPut/Delete

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`（`ControlStore: ... + InventoryStore`、`ControlTxn: ... + InventoryTxn`）
- Modify: `crates/xl-store/src/control_store/inventory.rs`
- Create: `crates/oss-server/src/control_store/inventory.rs`
- Modify: `crates/oss-server/src/control_store.rs`
- Modify: `crates/oss-server/src/db.rs:167-175, 6177-6301, 7176-7232`
- Modify: `crates/oss-server/src/raft/apply.rs:174-188, 761-788`
- Modify: `crates/oss-server/src/inventory_store.rs:136-153, 200-232`
- Modify: `crates/oss-server/src/inventory.rs:218-257, 391-452`
- Test: `crates/oss-server/tests/control_store_inventory_diff.rs`（新建）
- Test: `crates/oss-server/tests/m8_inventory.rs`、`raft_inventory_replication.rs`、`raft_bucket_routing_fix.rs`

**Interfaces:**
- Consumes: Task 1 的 `InventoryConfigEntry`/`InventoryConfigView`；Task 2 的 supertrait 拆分方式。
- Produces:
  ```rust
  #[async_trait::async_trait]
  pub trait InventoryStore: Send + Sync {
      /// 运行期口径；owner_uid 为本地外键投影，owner=None 表示用户行缺失。
      async fn inventory_config(&self, bucket: &str, id: &str)
          -> Result<Option<InventoryConfigView>, ControlError>;
      /// 按 id 升序。
      async fn inventory_configs(&self, bucket: &str)
          -> Result<Vec<InventoryConfigView>, ControlError>;
      /// 按 (bucket, id) 升序。
      async fn inventory_configs_all(&self) -> Result<Vec<InventoryConfigView>, ControlError>;
      async fn inventory_config_count(&self, bucket: &str) -> Result<i64, ControlError>;
  }

  #[async_trait::async_trait]
  pub trait InventoryTxn: Send {
      /// 解析 owner → 本地 uid；DELETE 该配置的 cursor + incremental；upsert config。
      async fn inventory_config_upsert(&mut self, row: &InventoryConfigEntry)
          -> Result<(), ControlError>;
      /// DELETE cursor + incremental + config；返回是否确有 config 行被删。
      async fn inventory_config_delete(&mut self, bucket: &str, id: &str)
          -> Result<bool, ControlError>;
      /// 快照导出（严格口径），按 (bucket, id) 升序。
      async fn inventory_configs(&mut self) -> Result<Vec<InventoryConfigEntry>, ControlError>;
      /// 快照安装：DELETE 全表 configs，再按入参顺序 INSERT（不碰 cursor/incremental）。
      async fn replace_inventory_configs(&mut self, rows: &[InventoryConfigEntry])
          -> Result<(), ControlError>;
  }
  ```

- [ ] **Step 1: 写失败测试**

`tests/control_store_inventory_diff.rs`：
```rust
#[tokio::test]
async fn inventory_config_runtime_view_maps_owner_four_ways() {
    // root(is_admin=1)、alice、owner_id=0、owner_id=999 悬空：
    // 运行期 → Some(User("root")) / Some(User("alice")) / Some(Admin) / None；
    // owner_uid 分别等于原 owner_id（999 也保留，供 legacy 文本/执行器）。
}

#[tokio::test]
async fn inventory_config_upsert_is_natural_key_and_clears_cursor_incremental() { /* 同 (bucket,id) 更新；cursor/incremental 被删；其他配置不动 */ }

#[tokio::test]
async fn inventory_config_delete_returns_false_for_missing_and_clears_children() { /* ... */ }

#[tokio::test]
async fn inventory_config_export_is_strict_and_ordered() {
    // 悬空 owner 导出 ControlError::Invalid("owner user id 999 missing")；
    // is_admin=1 → Admin；顺序 (bucket,id)。
}

#[tokio::test]
async fn inventory_apply_put_resolves_admin_and_user_natural_keys() { /* ... */ }
```
断言 `InventoryConfigView` 全字段；悬空 config 的运行期视图是 `owner: None` 且 `owner_uid == 999`。

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_inventory_diff -v`
Expected: FAIL。

- [ ] **Step 3: 实现适配器与调用点切换**

- 运行期 SQL：
  ```sql
  SELECT i.bucket, i.id, i.owner_id, u.username AS owner_username,
         i.config_json, i.updated_at
  FROM inventory_configs i LEFT JOIN users u ON u.id = i.owner_id
  ```
  映射：`owner_uid = owner_id`；`owner = if owner_id == 0 { Some(Admin) } else { username.map(User) }`。
- 严格导出增加 `u.is_admin AS owner_is_admin`，按决策 4 映射。
- `inventory_config_upsert` 复用 `ap_olap::resolve_owner`（改成 `pub(super)`），顺序：DELETE cursor、DELETE incremental、`INSERT ... ON CONFLICT(bucket,id) DO UPDATE`。
- `db.rs`：删本地 `InventoryConfigRow`；`inventory_get_config`/`inventory_list_configs`/`inventory_list_all_configs` 委托 store 返回 `InventoryConfigView`；`inventory_config_count` 委托；`inventory_upsert_config`/`inventory_delete_config` 改单事务 txn；删除 `inventory_upsert_raw`/`inventory_delete_raw`。
- `inventory_store.rs` 只读 `.config_json`，不改签名。
- `inventory.rs`：`due_runs_fenced_with` 用 `row.owner_uid`（与 legacy 完全一致）；`run()` 用 seam 视图的 `owner_uid` 做 `get_user`/`AuthedUser`/错误文本；`owner == None` 时仍走 `get_user(999)` 的旧分支（若用户已删则旧文本逐字保留）。因此这里**不需要**新的 fail-closed 分支文本，行为零漂移。
- `apply.rs`：`InventoryPut` 构造 `InventoryConfigEntry { owner: None→Admin / Some→User }` 调 `txn.inventory_config_upsert`（`resolve_owner` 等价 `admin_owner_id`）；`InventoryDelete` 调 `inventory_config_delete`；删除 `admin_owner_id`。

- [ ] **Step 4: 确认 GREEN + 回归**

Run: `cargo test -p oss-server --test control_store_inventory_diff -v`
Run: `cargo test -p oss-server --test m8_inventory --test raft_inventory_replication -v`
Run: `cargo test -p oss-server --lib witness_inventory_owner_sentinel_is_zero -v`
Run: `cargo test -p oss-server --test raft_bucket_routing_fix -v`
Expected: PASS（owner_id 断言不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/control_store/inventory.rs \
  crates/oss-server/src/control_store.rs crates/oss-server/src/control_store/inventory.rs \
  crates/oss-server/src/db.rs crates/oss-server/src/raft/apply.rs \
  crates/oss-server/src/inventory_store.rs crates/oss-server/src/inventory.rs \
  crates/oss-server/tests/control_store_inventory_diff.rs
git commit -m "refactor(oss-server): route inventory configs through ControlStore"
```

---

### Task 4: inventory run/cursor/incremental 读写、apply、执行器门面

**Files:**
- Modify: `crates/xl-store/src/control_store/inventory.rs`（扩展 trait）
- Modify: `crates/oss-server/src/control_store/inventory.rs`
- Modify: `crates/oss-server/src/db.rs:6303-6528`
- Modify: `crates/oss-server/src/raft/apply.rs:824-832`
- Modify: `crates/oss-server/src/raft/apply/inventory.rs`
- Modify: `crates/oss-server/src/job_state/inventory.rs:23-91`
- Test: `crates/oss-server/tests/control_store_inventory_diff.rs`（续）
- Test: `crates/oss-server/tests/raft_inventory_replication.rs`、`m8_inventory_parquet.rs`、`raft_runstate.rs`

**Interfaces:**
- Consumes: Task 1 的三个行类型；Task 3 的 `resolve_owner` 与适配器文件。
- Produces（追加到现有 trait）:
  ```rust
  // InventoryStore
  async fn inventory_run(&self, id: i64) -> Result<Option<InventoryRunRow>, ControlError>;
  async fn inventory_run_by_key(&self, bucket: &str, config_id: &str, run_key: &str)
      -> Result<Option<InventoryRunRow>, ControlError>;
  async fn inventory_cursor(&self, bucket: &str, config_id: &str)
      -> Result<Option<InventoryCursorRow>, ControlError>;
  async fn inventory_incremental(&self, bucket: &str, config_id: &str)
      -> Result<Option<InventoryIncrementalRow>, ControlError>;

  // InventoryTxn
  /// 快照：按 id 升序。
  async fn inventory_runs(&mut self) -> Result<Vec<InventoryRunRow>, ControlError>;
  async fn replace_inventory_runs(&mut self, rows: &[InventoryRunRow]) -> Result<(), ControlError>;
  /// 快照：按 (bucket, config_id) 升序。
  async fn inventory_cursors(&mut self) -> Result<Vec<InventoryCursorRow>, ControlError>;
  async fn replace_inventory_cursors(&mut self, rows: &[InventoryCursorRow]) -> Result<(), ControlError>;
  async fn inventory_incrementals(&mut self) -> Result<Vec<InventoryIncrementalRow>, ControlError>;
  async fn replace_inventory_incrementals(&mut self, rows: &[InventoryIncrementalRow])
      -> Result<(), ControlError>;
  async fn inventory_run(&mut self, id: i64) -> Result<Option<InventoryRunRow>, ControlError>;
  async fn inventory_run_by_key(&mut self, bucket: &str, config_id: &str, run_key: &str)
      -> Result<Option<InventoryRunRow>, ControlError>;
  /// 自然键 upsert；返回是否恰好 1 行。id 必须是行内不可变属性（预检保证）。
  async fn inventory_run_upsert(&mut self, row: &InventoryRunRow) -> Result<bool, ControlError>;
  /// INSERT OR IGNORE；插入成功返回本地 id。
  async fn inventory_run_create(&mut self, bucket: &str, config_id: &str, run_key: &str,
      created_at: i64, fence_term: i64, fence_leader: i64) -> Result<Option<i64>, ControlError>;
  async fn inventory_run_set_job(&mut self, run_id: i64, job_id: i64,
      fence_term: i64, fence_leader: i64) -> Result<(), ControlError>;
  async fn inventory_run_started(&mut self, run_id: i64, started_at: i64,
      fence_term: i64, fence_leader: i64) -> Result<(), ControlError>;
  async fn inventory_run_finish(&mut self, run_id: i64, state: &str,
      manifest_key: Option<&str>, error: Option<&str>,
      fence_term: i64, fence_leader: i64) -> Result<(), ControlError>;
  async fn inventory_cursor(&mut self, bucket: &str, config_id: &str)
      -> Result<Option<InventoryCursorRow>, ControlError>;
  async fn inventory_cursor_upsert(&mut self, row: &InventoryCursorRow) -> Result<bool, ControlError>;
  /// fence_term=None = 单机无 fence 口径（只删 cursor）；Some(term) 带 `<= term`。
  async fn inventory_cursor_delete(&mut self, bucket: &str, config_id: &str,
      fence_term: Option<i64>) -> Result<(), ControlError>;
  async fn inventory_incremental(&mut self, bucket: &str, config_id: &str)
      -> Result<Option<InventoryIncrementalRow>, ControlError>;
  async fn inventory_incremental_upsert(&mut self, row: &InventoryIncrementalRow)
      -> Result<bool, ControlError>;
  async fn inventory_incremental_delete(&mut self, bucket: &str, config_id: &str)
      -> Result<(), ControlError>;
  /// DELETE cursor + incremental（InventoryClear 语义）。
  async fn inventory_clear(&mut self, bucket: &str, config_id: &str) -> Result<(), ControlError>;
  ```

- [ ] **Step 1: 写失败测试**

`tests/control_store_inventory_diff.rs` 追加：
```rust
#[tokio::test]
async fn inventory_run_natural_key_conflict_is_zero_write() {
    // 先 insert id=7 自然键 k；apply InventoryBatch 带 id=8 同 k → Conflict；
    // 两表行数/内容不变（含 runstate_meta 未被推进）。
}

#[tokio::test]
async fn inventory_run_snapshot_preserves_ids_and_order() {
    // upsert ids 9,2,5；txn.inventory_runs() == [2,5,9]；replace 到新库后 ORDER BY id 一致；
    // 显式 id 插入后 CREATE 新 run 的 id > 9。
}

#[tokio::test]
async fn inventory_run_upsert_is_natural_key_but_keeps_id_immutable() { /* 预检通过时 id 不变；同键不同 id 由 apply 预检拒绝 */ }

#[tokio::test]
async fn inventory_cursor_upsert_and_delete_keep_fence_semantics() { /* term 4 vs 5；None 删除无 fence */ }

#[tokio::test]
async fn inventory_clear_deletes_cursor_and_incremental_only() { /* config/run 不受影响 */ }

#[tokio::test]
async fn inventory_run_create_is_idempotent_per_natural_key() { /* 第一次 Some(id)，第二次 None，行数 1 */ }

#[tokio::test]
async fn db_inventory_executor_facades_preserve_fence_and_started_once() { /* create/set_job/started/finish 与 legacy SQL 对照 */ }
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_inventory_diff inventory_run_ -v`
Expected: FAIL。

- [ ] **Step 3: 实现**

- 适配器：
  - `inventory_run_upsert`：`ON CONFLICT(bucket, config_id, run_key) DO UPDATE SET id=excluded.id, started_at=..., state=..., job_id=..., manifest_key=..., error=..., created_at=..., fence_term=..., fence_leader=...`；返回 `rows_affected() == 1`。**不写 runstate_meta**（保留在 `apply/inventory.rs`）。
  - `inventory_run_create`：`INSERT OR IGNORE`；`rows_affected()>0` 时返回 `Some(last_insert_rowid())`。
  - `inventory_run_set_job/started/finish`：逐字保留 legacy `UPDATE ... WHERE id = ? AND fence_term <= ?`（started 的 `CASE WHEN started_at = 0`）。
  - cursor upsert/delete、incremental upsert/delete、clear 逐字保留 SQL 语义。
  - 快照读/替换：`ORDER BY id` / `(bucket,config_id)`；替换先 DELETE 全表再按入参顺序 INSERT。
- `apply/inventory.rs`：所有 `current_*`/`apply_*` 改 txn；保留 `run_row_ok`/`cursor_row_ok`/`incremental_row_ok` 预检与整批零写入 `Conflict`；`apply_run` 后保留 `runstate_meta('next_run_id')` 的 raw upsert（决策 8），并断言 upsert 返回 true。
- `db.rs`：`inventory_create_run`/`inventory_run_set_job`/`inventory_run_started`/`inventory_run_finish`/`inventory_cursor_set`/`inventory_incremental_set`/`inventory_cursor_get`/`inventory_incremental_get` 改单事务 txn；`inventory_run_by_key` 委托 store（补 fence 两列）；删 `inventory_run_row`。
- `job_state/inventory.rs`：`read_run`/`read_run_by_key`/`read_cursor`/`read_incremental` 改 `db.control_store()`；`next_run_id` 保持 raw（决策 8）。

- [ ] **Step 4: 确认 GREEN + 回归**

Run: `cargo test -p oss-server --test control_store_inventory_diff -v`
Run: `cargo test -p oss-server --test raft_inventory_replication -v`
Run: `cargo test -p oss-server --test m8_inventory_parquet --test raft_runstate -v`
Run: `cargo test -p oss-server --lib inventory_run_ -v`
Expected: PASS（fence/顺序/next_run_id 断言不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store/inventory.rs \
  crates/oss-server/src/control_store/inventory.rs crates/oss-server/src/db.rs \
  crates/oss-server/src/raft/apply.rs crates/oss-server/src/raft/apply/inventory.rs \
  crates/oss-server/src/job_state/inventory.rs crates/oss-server/tests/control_store_inventory_diff.rs
git commit -m "refactor(oss-server): route inventory runstate through ControlTxn"
```

---

### Task 5: tier restore 读写、apply、job_state 读路径

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`（`+ TierStore` / `+ TierTxn`）
- Modify: `crates/xl-store/src/control_store/tier.rs`
- Create: `crates/oss-server/src/control_store/tier.rs`
- Modify: `crates/oss-server/src/control_store.rs`
- Modify: `crates/oss-server/src/db.rs:143-165, 5149-5343`
- Modify: `crates/oss-server/src/raft/apply.rs:1080-1149, 1291-1333`
- Modify: `crates/oss-server/src/raft/apply/tier_restore.rs`
- Modify: `crates/oss-server/src/job_state/restore.rs:16-34`
- Test: `crates/oss-server/tests/control_store_tier_diff.rs`（新建）
- Test: `crates/oss-server/tests/raft_restore_failover.rs`、`m8_tier_restore.rs`、`raft_restore_lease_replication.rs`

**Interfaces:**
- Consumes: Task 1 `TierRestoreRow`。
- Produces:
  ```rust
  #[async_trait::async_trait]
  pub trait TierStore: Send + Sync {
      async fn tier_restore(&self, bucket: &str, key: &str, version_id: &str)
          -> Result<Option<TierRestoreRow>, ControlError>;
      /// WHERE state = ?，按 created_at 升序。
      async fn tier_restores(&self, state: &str) -> Result<Vec<TierRestoreRow>, ControlError>;
  }

  #[async_trait::async_trait]
  pub trait TierTxn: Send {
      async fn tier_restore(&mut self, bucket: &str, key: &str, version_id: &str)
          -> Result<Option<TierRestoreRow>, ControlError>;
      /// 按 (bucket, key, version_id) 升序。
      async fn tier_restores(&mut self) -> Result<Vec<TierRestoreRow>, ControlError>;
      async fn replace_tier_restores(&mut self, rows: &[TierRestoreRow]) -> Result<(), ControlError>;
      /// fence 门控 upsert；返回是否恰好 1 行。
      async fn tier_restore_upsert(&mut self, row: &TierRestoreRow) -> Result<bool, ControlError>;
      async fn tier_restore_delete(&mut self, bucket: &str, key: &str, version_id: &str,
          fence_term: i64) -> Result<(), ControlError>;
      /// UPDATE state/expires_at/fence WHERE PK AND fence_term <= ?
      async fn tier_restore_set_state(&mut self, bucket: &str, key: &str, version_id: &str,
          state: &str, expires_at: i64, fence_term: i64, fence_leader: i64)
          -> Result<(), ControlError>;
  }
  ```

- [ ] **Step 1: 写失败测试**

`tests/control_store_tier_diff.rs`：
```rust
#[tokio::test]
async fn tier_reads_match_legacy_state_and_order() { /* restored 两行 ORDER BY created_at；点读全字段 */ }

#[tokio::test]
async fn tier_upsert_delete_and_set_state_are_fence_bounded() {
    // 现有 term=5；term=4 upsert 返回 false 且行不变；term=5 更新；
    // set_state(4) no-op；delete(4) no-op；delete(5) 删除。
}

#[tokio::test]
async fn create_restore_job_keeps_max_fence_and_pending_reset() {
    // 现有 restored/fence=9；UNFENCED 新请求：旧实现 WHERE 拒绝 → 行保持；
    // fence=9 新请求重置 pending；比较 raw legacy helper。
}
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_tier_diff -v`
Expected: FAIL。

- [ ] **Step 3: 实现**

- 适配器 upsert 逐字保留：
  ```sql
  INSERT INTO tier_restores (...) VALUES (...)
  ON CONFLICT(bucket,key,version_id) DO UPDATE SET expires_at=excluded.expires_at, ...
  WHERE tier_restores.fence_term <= excluded.fence_term
  ```
  返回 `rows_affected()==1`。
- `Db::restore_rows`/`restore_row` 委托 store 并映射为 `RestoreRow`（丢 fence 字段，保持公开类型）；`set_restore_state_fenced`/`delete_restore_fenced` 单事务 txn；`create_restore_job` 内的 `upsert_restore_row` 改为构造整行（state=pending、expires=0、created_at=now、job_id=Some(id)）后 `tier_restore_upsert`（语义等价：WHERE 单调 + 新行覆盖 pending；MAX 等价于 excluded）。
- `apply/tier_restore.rs` 与 `apply_job_batch` 的 restores 段改 txn；删 `current_restore`、`apply_restore`；`restore_row_ok` 保留。
- `job_state/restore.rs::read_tier_restore` 改 `db.control_store().tier_restore(...)`。

- [ ] **Step 4: 确认 GREEN + 回归**

Run: `cargo test -p oss-server --test control_store_tier_diff -v`
Run: `cargo test -p oss-server --test raft_restore_failover --test m8_tier_restore --test raft_restore_lease_replication -v`
Run: `cargo test -p oss-server --lib create_restore_job -v`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/control_store/tier.rs \
  crates/oss-server/src/control_store.rs crates/oss-server/src/control_store/tier.rs \
  crates/oss-server/src/db.rs crates/oss-server/src/raft/apply.rs \
  crates/oss-server/src/raft/apply/tier_restore.rs crates/oss-server/src/job_state/restore.rs \
  crates/oss-server/tests/control_store_tier_diff.rs
git commit -m "refactor(oss-server): route tier restores through ControlTxn"
```

---

### Task 6: 快照 Export/Install 走 txn（ControlData + RunState 五表）

**Files:**
- Modify: `crates/oss-server/src/raft/data.rs:405-505, 651-720, 905-966`
- Modify: `crates/oss-server/src/raft/runstate/store.rs:19-45, 178-291, 300-524`
- Test: `crates/oss-server/tests/compat_fixtures.rs`（现有）
- Test: `crates/oss-server/tests/raft_cutover.rs`、`raft_runstate_snapshot.rs`、`raft_runstate.rs`
- Test: 三个域 diff 文件追加 round-trip

**Interfaces:**
- Consumes: Task 2-5 的 snapshot 方法（`all_lifecycle_rules`、`replace_all_lifecycle_rules`、`replace_lifecycle_cursors`、`inventory_configs`、`replace_inventory_configs`、`inventory_runs`、`replace_inventory_runs`、`inventory_cursors`、`replace_inventory_cursors`、`inventory_incrementals`、`replace_inventory_incrementals`、`tier_restores`、`replace_tier_restores`）。
- Produces: `raft/data.rs::read_tx`/`install_tx` 与 `raft/runstate/store.rs::read_runstate_tx`/`write_rows` 内这 7 张表无 raw SQL；`RunState`/`ControlData` 字节不变。

- [ ] **Step 1: 写失败测试**

在域 diff 文件中追加（真实 SQLite）：
```rust
#[tokio::test]
async fn control_data_lifecycle_and_inventory_round_trip_preserves_order() {
    // 两桶、含 NULL rule_id 与 config_json；admin/user 属主；
    // read_tx 的 Vec 顺序 == legacy raw；serde_json 字节 == legacy；
    // install_tx 到含旧行的库后 read_tx 完全相等；旧行被替换。
}

#[tokio::test]
async fn snapshot_install_rejects_dangling_inventory_owner_before_writes() {
    // 目标库已有 lifecycle/inventory 行；data 的 inventory owner=ghost →
    // Err 文本含 "inventory owner ghost not in snapshot"；既有行逐行不变。
}

#[tokio::test]
async fn runstate_five_tables_round_trip_preserves_order_and_ids() {
    // runs ids 9,2,5；cursors/incrementals/tier_restores/lifecycle_cursors 各两行；
    // read_runstate_tx == 输入；install 到新库再读 == 输入；
    // rmp 扩展 payload 字节与旧实现 helper 一致；无效 job 引用仍被 validate 拒绝。
}
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_lifecycle_diff control_data_ -v`
Expected: FAIL（仍走 raw / 安装未替换）。

- [ ] **Step 3: 实现**

- `data.rs::read_tx`：
  ```rust
  data.lifecycle_rules = {
      let mut txn = SqliteControlTxn::new(&mut *conn);
      txn.all_lifecycle_rules().await.map_err(control_data_err)?
  };
  data.inventory_configs = {
      let mut txn = SqliteControlTxn::new(&mut *conn);
      txn.inventory_configs().await.map_err(control_data_err)?
  };
  ```
- `data.rs::install_tx`：
  - 在 AP 属主预校验后追加 inventory 属主预校验（任何写之前）：Admin 需 `ctx.admin_id` 或 witness；User 必须在 `data.users`，否则 `DataError::Format("inventory owner {name} not in snapshot")`。
  - 步骤 2 的 raw DELETE 列表删掉 `inventory_configs`、`lifecycle_rules`；步骤 11 插入改为 `txn.replace_inventory_configs(&data.inventory_configs)`；步骤 12 改为 `txn.replace_all_lifecycle_rules(&data.lifecycle_rules)`。
- `runstate/store.rs`：
  - `RUNSTATE_TABLES` 改为 `RUNSTATE_RAW_TABLES: [&str; 4] = ["jobs", "job_tasks", "schedule_lease", "runstate_meta"]`。
  - `read_runstate_tx` 五个循环改 txn 读；新增 `fn runstate_err(e: ControlError) -> DataError`（Invalid→Format 原文，Backend→Format("control store backend: {m}")）。
  - `write_rows`：raw DELETE 仅 4 表；raw INSERT jobs/job_tasks；随后一个 txn 块依次 `replace_inventory_runs`、`replace_inventory_cursors`、`replace_inventory_incrementals`、`replace_tier_restores`、`replace_lifecycle_cursors`；lease/meta 保持 raw。savepoint 边界不变。

- [ ] **Step 4: 确认 GREEN + 快照回归**

Run: `cargo test -p oss-server --test compat_fixtures --test raft_cutover --test raft_cutover_provenance -v`
Run: `cargo test -p oss-server --test raft_runstate_snapshot --test raft_runstate --test raft_runstate_followups -v`
Run: `cargo test -p oss-server --lib raft::data -v`
Run: `cargo test -p oss-server --test control_store_lifecycle_diff --test control_store_inventory_diff --test control_store_tier_diff -v`
Expected: PASS（旧 fixture 可读、JSON/rmp 字节与安装结果不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/oss-server/src/raft/data.rs crates/oss-server/src/raft/runstate/store.rs \
  crates/oss-server/tests/control_store_lifecycle_diff.rs \
  crates/oss-server/tests/control_store_inventory_diff.rs \
  crates/oss-server/tests/control_store_tier_diff.rs
git commit -m "refactor(oss-server): snapshot lifecycle/inventory/runstate through ControlTxn"
```

---

### Task 7: 桶删除级联与 site 路径走 txn

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`（`ControlTxn::delete_lifecycle_inventory_tier_for_bucket`）
- Modify: `crates/oss-server/src/control_store.rs`（级联实现）
- Modify: `crates/oss-server/src/db.rs:1185-1211, 6918-6955`
- Modify: `crates/oss-server/src/site.rs:332-346`
- Test: `crates/oss-server/src/db.rs` `#[cfg(test)]`
- Test: `crates/oss-server/tests/m8_bucket_cleanup.rs`、`lifecycle.rs`

**Interfaces:**
- Consumes: Task 2-5 的 txn 方法；现有 `delete_access_points_for_bucket`。
- Produces:
  ```rust
  /// 删该桶的 7 张表：inventory_runs、inventory_configs、inventory_cursors、
  /// inventory_incremental、tier_restores、lifecycle_rules、lifecycle_cursors。
  async fn delete_lifecycle_inventory_tier_for_bucket(&mut self, bucket: &str)
      -> Result<(), ControlError>;
  ```

- [ ] **Step 1: 写失败测试**

`db.rs` 单元测试（可访问 `pub(crate)`）：
```rust
#[tokio::test]
async fn cascade_deletes_all_seven_tables_and_spares_other_bucket() {
    // 两桶在 7 张表各 1 行；事务内 clear_bucket_m8_tx("b1")；
    // 断言 b1 七表 0 行、b2 七表各 1 行、AP/OLAP 行为不变。
}

#[tokio::test]
async fn delete_bucket_cascade_uses_same_seven_table_scope() {
    // 建 b1 桶行 + 7 表行 + permissions/bucket_policies；delete_bucket_cascade 后
    // 7 表与桶行为 0，其他桶不动。
}

#[tokio::test]
async fn site_bucket_delete_keeps_scope_but_uses_txn() {
    // lifecycle_rules 被删；lifecycle_cursors 按既有范围保留（决策 6）。
}
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --lib cascade_deletes_all_seven_tables_and_spares_other_bucket -v`
Expected: FAIL（方法不存在 / 仍是 raw）。

- [ ] **Step 3: 实现**

- 适配器级联按决策 5 顺序执行 7 条 DELETE。
- `clear_bucket_m8_tx`：一个 txn 块内 `delete_access_points_for_bucket` + `delete_lifecycle_inventory_tier_for_bucket`；删除 7 条 raw 循环。
- `delete_bucket_cascade`：同样先 AP 级联 + 新方法，再保留 permissions/bucket_policies/event/replication/buckets 的 raw（后续切片）；删除 olap/AP 的 raw 两条。
- `site.rs`：`DELETE FROM lifecycle_rules` 改为 `txn.delete_lifecycle_rules_only(bucket)`（rules-only，决策 6）；错误用 `map_err(|e| bad(crate::control_store::to_sqlx(e).to_string()))?`。
- 删除 `db.rs` 的 `set_lifecycle_rules_raw` / `clear_lifecycle_rules_raw` / `inventory_upsert_raw` / `inventory_delete_raw`（若 Task 2-4 未删干净）。

- [ ] **Step 4: 确认 GREEN + 回归**

Run: `cargo test -p oss-server --lib cascade_deletes_all_seven_tables_and_spares_other_bucket -v`
Run: `cargo test -p oss-server --test m8_bucket_cleanup -v`
Run: `cargo test -p oss-server --test lifecycle -v`
Run: `cargo test -p oss-server --test raft_commands -v`
Expected: PASS（删桶 HTTP/事务/缓存行为不变）。

- [ ] **Step 5: Commit**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs \
  crates/oss-server/src/db.rs crates/oss-server/src/site.rs
git commit -m "refactor(oss-server): delete bucket lifecycle/inventory/tier through ControlTxn"
```

---

### Task 8: 跨域端到端差分与无 raw-SQL 审计

**Files:**
- Create: `crates/oss-server/tests/control_store_lifecycle_inventory_tier_diff.rs`
- Modify: `crates/oss-server/tests/support/control_store_rows.rs`（补全表 dump / 快照 helper）
- Test: 新文件

**Interfaces:**
- Consumes: 全部 seam 方法与 legacy raw helper。
- Produces: 一个端到端差分测试，作为整切片的行为护栏。

- [ ] **Step 1: 写失败测试（先跑当前实现，若已等价则通过；测试本身必须覆盖全链路）**

```rust
#[tokio::test]
async fn end_to_end_snapshot_and_cascade_match_legacy() {
    // 两个 fresh SQLite：store_db 走 seam，ref_db 走 legacy raw helper。
    // 同一操作序列：建 users/buckets → lifecycle rules（含 NULL rule_id/config_json）
    // → inventory configs（admin/user/witness 0）→ runs/cursors/incrementals
    // → tier_restores → 导出 ControlData + RunState → 安装到第三个 fresh 库
    // → 再导出比较 JSON/rmp 字节 → 删 b1 → dump 7 表 + AP/OLAP + buckets 比较。
    // 断言：两库 dump 完全相等；b2 行完整；安装后字节相等。
}

#[tokio::test]
async fn production_sources_have_no_raw_sql_for_migrated_tables() {
    // 读 crate 源码字符串（include_str!），断言 db.rs / raft/apply*.rs / raft/data.rs /
    // raft/runstate/store.rs / job_state/*.rs 中不再出现 7 张表的 raw SQL；
    // 白名单：raft/runstate/store.rs 的 jobs/job_tasks/meta、apply/inventory.rs 的
    // runstate_meta、db.rs 的 purge_terminal_jobs_raw（决策 8）、#[cfg(test)] 模块。
}
```

- [ ] **Step 2: 确认 RED**

Run: `cargo test -p oss-server --test control_store_lifecycle_inventory_tier_diff -v`
Expected: 首轮可能因测试自身编译/断言 FAIL；修正测试后，实现不等价处必须暴露。

- [ ] **Step 3: 修实现（只修行为差异，不弱化测试）**

若 dump 差异出现在 `updated_at`/排序/fence/owner 映射，按 legacy SQL 逐列对齐；不得改断言或放宽为子集比较。

- [ ] **Step 4: 确认 GREEN**

Run: `cargo test -p oss-server --test control_store_lifecycle_inventory_tier_diff -v`
Expected: 2/2 PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/oss-server/tests/control_store_lifecycle_inventory_tier_diff.rs \
  crates/oss-server/tests/support/control_store_rows.rs
git commit -m "test(oss-server): differential coverage for lifecycle/inventory/tier seam"
```

---

### Task 9: 切片门禁

**Files:** 无新增；修复前面任务的门禁 fallout。

- [ ] **Step 1: xl-store**

Run: `cargo test -p xl-store`
Expected: PASS（含 7 个新冻结测试；记录实际数量）。

- [ ] **Step 2: oss-server 全量（允许既有 flake 隔离重跑）**

Run: `cargo test -p oss-server --no-fail-fast`
Expected: 0 failed（2 ignored 不变）。若出现已知 5s 控制面状态探测超时（`站点控制面请求超时（5s）`）或同类负载时序 flake，隔离重跑至绿；**不得**弱化断言或加 sleep。

- [ ] **Step 3: 差分套件**

Run: `cargo test -p oss-server --test control_store_lifecycle_diff --test control_store_inventory_diff --test control_store_tier_diff --test control_store_lifecycle_inventory_tier_diff`
Expected: 全 PASS。

- [ ] **Step 4: clippy + fmt**

Run: `cargo clippy -p oss-server -p xl-store -p xl-s3 --all-targets`
Expected: exit 0，0 新增 warning。
Run: `cargo fmt --all -- --check`
Expected: exit 0。

- [ ] **Step 5: 记录并提交 fallout（若有）**

```bash
git status --short
git add -A
git commit -m "chore(oss-server): lifecycle/inventory/tier slice gates"
```
若无改动，跳过提交并在最终汇报中记录门禁实际输出。

---

## 自审记录

- **覆盖**：7 张表的读（console/job/worker）、写（单机/Raft apply）、快照（ControlData + RunState）、桶删除（两个入口 + site 既有范围）、执行器/调度门面全部有任务；`runstate_meta`/purge 的例外在决策 8 明确。
- **类型一致性**：`LifecycleRuleEntry`/`InventoryConfigEntry`/`InventoryConfigView`/5 个 runstate 行类型/`TierRestoreRow` 名称在 Task 1 定义，Task 2-8 只引用；`replace_*` / `inventory_*` / `tier_*` 方法名逐任务一致。
- **Review Focus 对应**：5 条各在 Task 2/4/6/7/8 有具名测试；fence 与身份漂移同时被现有 replication 套件覆盖。
- **比例**：本计划只写签名、SQL 语义差异与测试断言；实现体由执行者按周边代码风格完成。
