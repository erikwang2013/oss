# 桶 / 桶策略 / 事件 / 复制规则 ControlStore 切片实施计划（Slice 4）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `buckets`、`bucket_policies`、`event_configs`、`replication_rules` 四张表的全部控制面读写收敛到 `ControlStore`/`ControlTxn` 缝，保留站点 changelog 触发器、Raft guard 触发器、桶删除级联范围、快照字节与错误文本完全不变。

**Architecture:** 行类型与 trait 放在 `xl-store`（后端中立、自然键）；SQLite 适配器放在 `oss-server/src/control_store/`，只借用调用方事务，不 begin/commit/rollback。apply、单节点 console、快照导出/安装、桶删除级联改为调用缝；`site.rs` 与 xl-s3 `Acl` 的原始 SQL 显式延迟到后续切片，并在源码中留 `shortcut:` 注释。

**Tech Stack:** Rust、sqlx/SQLite、async-trait、serde + rmp-serde、tokio 测试；不新增依赖。

**Spec:** 本计划自包含；上位约束见 `docs/superpowers/plans/2026-11-03-control-store-vpc-slice.md`、`2026-11-04-control-store-ap-olap-slice.md`、`2026-11-04-control-store-lifecycle-inventory-tier.md`。

## 执行前必读：接口复核

当前 checkout 为 `control-store-lifecycle-inventory-tier` 分支；写计划时 HEAD 为 `ad3d6c9`（“move lifecycle/inventory/tier rows into ControlStore seam”），且 `crates/xl-store/src/control_store.rs`、`control_store/lifecycle.rs` 仍有未提交修改。Slice 3 的 `InventoryStore`/`InventoryTxn`/`TierStore`/`TierTxn`、`ControlStore: ... + InventoryStore + TierStore`、`ControlTxn: ... + InventoryTxn + TierTxn`、`delete_lifecycle_inventory_tier_for_bucket` 可能尚未合并或命名有出入。

**执行本计划的第一个动作**：切到 Slice 3 已合并的提交，运行 `cargo test -p xl-store` 与 `cargo test -p oss-server`，逐项核对本计划引用的 Slice 3 接口；名称不同则以合并后的 seam 为准，同步修改本计划中的调用点，不改语义。**不得与 Slice 3 并行写同一工作区。**

## Global Constraints

- 外部行为不变：HTTP 状态/文本、S3 XML/JSON、快照字节与字段顺序、feature level、站点 changelog 内容与触发时机，均须与迁移前逐字节/逐语句一致。
- 缝后端中立：只用自然键；泄漏 SQL、SQLite 行 id、`set_id` 到 `xl-store` 均不允许（`set_id` 是本地路由属性，留在适配器/本地路径）。
- 适配器绝不 begin/commit/rollback；事务边界只归调用方（Raft apply、快照安装、单节点 façade）。
- 复用 `Db` 既有连接池，不新建第二个池。
- 不新增依赖；单文件 < 500 行；适配器按读写/快照拆分。
- 每个任务 TDD：先写失败测试，再实现，再全绿；每个任务独立提交。
- 不删除、不重命名任何 SQLite 触发器。本切片只保证适配器写出的语句形状让既有 30 个 site 触发器与 33 个 raft guard 以原方式触发。
- 不设计最终对象后端；不迁移 `site.rs` 的 LWW 逻辑；不动 xl-s3 `Acl` 的 S3 热路径 SQL。
- 全量门禁：`cargo test -p xl-store`、`cargo test -p oss-server`、clippy/fmt。既有 5s 探针偶发超时只允许单独重跑，**不得弱化断言**。
- 提交信息不带 `Co-Authored-By`。

## Review Focus

按风险从高到低，五项都必须在对应任务的测试里钉住：

1. **changelog 触发器漂移**：`buckets` 的 UPDATE 只应在 `versioning/owner_id/object_lock` 实值变化时写 changelog；`bucket_policies` 的 upsert 必须继续走 `INSERT ... ON CONFLICT DO UPDATE`（UPDATE 触发器），绝不能改成 `INSERT OR REPLACE` 或 DELETE+INSERT。测试：Task 7 的 site 触发器断言 + Task 8 差分。
2. **级联范围**：`delete_bucket_cascade` 必须仍清 13 张从属表且只清目标桶；`clear_bucket_m8_tx` 范围不变（不新增也不遗漏）。测试：Task 7 `cascade_deletes_all_13_tables_and_spares_other_bucket`、扩展现有 `clear_bucket_m8_tx_deletes_bucket_ap_olap_only`。
3. **属主投影三口径**：运行期宽松（NULL/0/悬空 → `Admin`，现存用户含管理员 → `User`）、严格快照（`is_admin=1` → `Admin`，悬空 → 报错）、安装（User 缺失 → 快照拒绝）。错误文本必须仍是 `owner user id {id} missing` 与 `bucket owner {name} not in snapshot`。测试：Task 2、Task 6。
4. **快照顺序与安装语义**：`ControlData` 字段顺序、四张表的 `ORDER BY`、`BucketEntry` 字段顺序、安装时保留目标 `set_id`、目标独有桶先删、`UPDATE buckets SET owner_id = NULL` 的位置。测试：Task 6 字节级 round-trip + Task 8 差分。
5. **策略/配置校验与零写**：空名称、非法 mode/versioning/lock mode、空 policy/notification/rules、缺失属主都必须**在任何写之前**拒绝；模式冲突必须零写。测试：Task 4 + Task 5。

---

## 裁决记录（本切片的设计决定）

### D1. 桶身份与属主

- 自然键 = `buckets.name`；`creation_date`、`versioning`、`object_lock`、`default_encryption`、`object_lock_default_mode`、`object_lock_default_days`、`replication_mode`、`deleting` 全部保留。
- `set_id` **不进 seam**：它是本地 set 路由属性，继续由 `cluster_control.rs`、`Db::upsert_bucket_set`、`Db::insert_bucket*`、`Db::upsert_bucket_owner_set_mode` 本地维护（见 D7）。
- 快照行 `BucketEntry.owner: OwnerRef`，字段顺序与旧 `BucketRow` 完全一致；Admin 导出为 `{"kind":"admin"}`。
- 运行期 `BucketView` 额外带两个本地投影：`owner_uid: Option<i64>`（原 `buckets.owner_id`）与 `owner_is_admin: bool`（LEFT JOIN `users.is_admin`）。它们只服务于 `bucket_account`、`owned_buckets`、事件属主等既有语义，**不是** seam 身份键。
- 运行期属主口径与 AP 切片一致：`owner_id IS NULL` 或 `Some(0)` 或用户行缺失 → `Admin`；现存用户 → `User(username)`（即使该用户是管理员）。严格导出口径：`is_admin=1` → `Admin`；悬空非 0 id → `Invalid("owner user id {id} missing")`。
- `bucket_account` 必须用原始 `owner_uid + owner_is_admin` 计算账户，不得用运行期 `OwnerRef` 反推，否则“用户后来被提升为管理员”的历史桶会改变账户归属。

### D2. 触发器

已确认的触发器面：

| 表 | site 触发器（M7，`0002_m7_site.sql`） | raft guard（M9，`0004_m9.sql`） |
|---|---|---|
| `buckets` | `site_gate/log_bucket_ins/upd/del` 各 1 | `raft_guard_buckets_ins/upd/del` |
| `bucket_policies` | `site_gate/log_policy_ins/upd/del` 各 1 | `raft_guard_bucket_policies_*` |
| `event_configs` | 无 | `raft_guard_event_configs_*` |
| `replication_rules` | 无 | `raft_guard_replication_rules_*` |

- `Db::connect` → `prepare_raft_mode(false)`：drop `raft_guard_*`，保留 site 触发器；`connect_raft` / `prepare_raft_mode(true)`：重放 M9，drop `site_*`。任一时刻只有一族的触发器生效。
- 适配器必须逐语句复刻旧 SQL 形状：
  - `bucket_create` 新建（逐字复刻旧语句，注意 `set_id` 在 `versioning` 之后、常量 0）：
    `INSERT INTO buckets (name, owner_id, creation_date, versioning, set_id, object_lock, object_lock_default_mode, object_lock_default_days, default_encryption, replication_mode, deleting) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0)`。
    同名同模式：`UPDATE buckets SET owner_id = ?, versioning = ?, object_lock = ?, object_lock_default_mode = ?, object_lock_default_days = ?, default_encryption = ?, replication_mode = ?, deleting = 0 WHERE name = ?`。**UPDATE 不写 `creation_date`、不写 `set_id`**。
  - `bucket_set_deleting`：`UPDATE buckets SET deleting = ?`。该列不在 site 触发器列清单内，不产生 changelog。
  - `bucket_set_versioning`：`UPDATE buckets SET versioning = ?`。
  - `bucket_set_object_lock`：`UPDATE buckets SET object_lock = ?, object_lock_default_mode = ?, object_lock_default_days = ?`。site UPDATE 触发器声明为 `UPDATE OF versioning, owner_id, object_lock`，且 `WHEN OLD.object_lock IS NOT NEW.object_lock`；只改默认 mode/days 时触发器会评估但不写 changelog，行为与现在相同。
  - `bucket_set_default_encryption` / `bucket_clear_default_encryption`：分别保留 `UPDATE ... SET default_encryption = ?` 与 `UPDATE ... SET default_encryption = NULL`。
  - 策略/通知/复制规则写入：`INSERT INTO <table> (...) VALUES (...) ON CONFLICT(bucket) DO UPDATE SET ...`；删除：`DELETE FROM <table> WHERE bucket = ?`。
  - 安装替换：`DELETE FROM <table>` + 按入参顺序 `INSERT`（与旧 install 第 2/12 步同文）。
- 本切片**不删除触发器**；对象后端的 changelog 捕获由最终 SQLite 移除切片设计。

### D3. 级联顺序

`delete_bucket_cascade` 保持现有 13 张从属表的范围与「先从属、最后 `buckets`」的约束，只把属于本切片/前序切片的部分换成 txn。Slice 3 的合并方法按 `inventory_runs → ... → lifecycle_cursors` 一次完成，因此 `permissions` 相对 lifecycle 两表的 DELETE 次序会从旧实现的中段移到其后；这些表都没有 site/raft 触发器（`permissions` 的触发器只依赖自身行），`site_changelog` 行集合与最终数据不受影响。`buckets` 必须仍最后删除。

1. `olap_configs`（按 AP 子查询）→ `access_points`：`ControlTxn::delete_access_points_for_bucket`（Slice 2）。
2. `inventory_runs` → `inventory_configs` → `inventory_cursors` → `inventory_incremental` → `tier_restores` → `lifecycle_rules` → `lifecycle_cursors`：`ControlTxn::delete_lifecycle_inventory_tier_for_bucket`（Slice 3）。
3. `permissions`：保持 raw（Slice 5）。
4. `bucket_policies` → `event_configs` → `replication_rules`：新增 `BucketTxn::delete_bucket_configs_for_bucket`。
5. `buckets`：新增 `BucketTxn::bucket_delete`，返回 `rows_affected > 0`。

`clear_bucket_m8_tx` 的范围**不变**：只清 AP/OLAP + 7 张 lifecycle/inventory/tier 表。它不碰策略/事件/复制/权限/桶行。`permissions` 与 `buckets` 仍由 `delete_bucket_cascade` 与 `site.rs` 各自负责。

### D4. 整桶替换与零写

- 策略、事件、复制规则都是每桶单行；apply/console 的单条写入用 upsert，安装用整表 DELETE+INSERT。两种形状都保留，不合并。
- `bucket_create` 的顺序必须与旧代码一致：先解析属主（`User` 缺失且本地有管理员 → fail-stop `bucket owner user {username} not found`；无管理员/witness → NULL），再查 `replication_mode`。同名不同模式直接返回 `ModeConflict`，此路径只读不写；同名同模式才 UPDATE。
- apply 层的校验（非空、`valid_versioning`、`valid_lock_mode`）保持在 handler 内、调用 txn 之前，不下沉到适配器；适配器只做属主解析这一项必需映射。
- 删除策略/通知/复制规则对不存在的桶返回成功（幂等），与旧 helper 相同；只有 apply handler 的 `bucket_exists` 前置检查决定 `NotFound`。

### D5. 站点路径（明确延迟）

`site.rs::apply_bucket` / `apply_policy` 本切片**不改行为**，仅加 `shortcut:` 注释。原因：

- 站点删除分支的策略/权限 DELETE 受 `site_entity_clock` 门控，不能换成普通 DELETE。
- 站点 upsert 有意只写 `owner_id/creation_date/versioning/object_lock`，不写 `default_encryption/replication_mode/deleting`，并保留目标 `set_id`；与 apply 的 `bucket_create` 语义不同。
- 站点写入发生在 `apply_ctx.active=1` 的事务内，触发器抑制逻辑与 seam 调用方模型耦合；迁移必须和站点 changelog/审计切片（Slice 6）一起做，否则会改变 gate/changelog 行为。

`xl-s3/src/access.rs` 的 `Acl`（S3 热路径，含 `PutBucketVersioning`、通知、加密、建桶记账、`forget_bucket`）同样延迟：`Acl` 只持有 `SqlitePool`，看不到 `oss-server` 适配器；迁移属 IAM/账户切片（Slice 5）或最终 SQLite 移除。

### D6. 快照导出与安装

- `BucketRow`、`BucketPolicyRow`、`EventConfigRow`、`ReplicationRuleRow` 改为 `pub type = xl_store::control_store::*Entry`（与 Slice 3 的 `LifecycleRuleRow`、Slice 2 的 `AccessPointRow` 同法），字段顺序由 xl-store freeze 测试钉死。
- 导出：`buckets` 按 `name` 升序，三个配置表按 `bucket` 升序；`ControlData` 字段顺序不变（`buckets`、`bucket_policies`、`lifecycle_rules`、`event_configs`、`replication_rules`）。
- 安装：
  - 预校验保持现状：AP 属主仍在任何写之前整包校验；桶属主在第 10 步逐行解析，缺失即返回错误并由调用方事务整体回滚（文本 `control data: bucket owner {name} not in snapshot`）。
  - 第 2 步删除清单移除本切片的 `event_configs`、`replication_rules`、`bucket_policies`；由第 12 步的 `replace_*` 自行 DELETE+INSERT。安装只在 raft/cutover 上下文执行，site 触发器不存在，raft guard 只检查 `raft_apply_ctx.active`；因此把 DELETE 从第 2 步移到第 12 步不改变任何触发器或外键行为（三表均无 FK）。
  - 第 4 步合并为 `bucket_install_prepare(wanted)`：先读 `SELECT name FROM buckets`，逐个 `DELETE` 目标独有桶，再 `UPDATE buckets SET owner_id = NULL`；顺序与旧代码一致。
  - 第 10 步用 `bucket_upsert_install(row)`：`INSERT ... ON CONFLICT(name) DO UPDATE` 复刻旧列清单，**不含 `set_id`**；属主 `User` 缺失 → `Invalid("bucket owner {name} not in snapshot")`。
  - 第 12 步调用顺序保持 `replace_bucket_policies` → lifecycle（Slice 3）→ `replace_event_configs` → `replace_replication_rules`。
- `required_feature_level` 不变：桶/策略/事件/复制规则都是 level 1（MIN）；测试钉住。

### D7. seam 之外的本地例外（白名单）

以下路径继续持有原始 SQL，源码留 `shortcut:` 注释，并进入 Task 8 审计白名单：

- `site.rs`：站点 LWW（D5）。
- `xl-s3/src/access.rs`：S3 热路径 `Acl`（D5）。
- `cluster_control.rs`：mesh 本地对等查询与 set 路由（`BucketSet` 读、`BucketRowSet` 写、`DbSetResolver`）；`OwnedBuckets`/`BucketOwner` 本切片也不切换，避免无 `ORDER BY` 的行序变化影响对端编码字节。
- `db.rs`：`insert_bucket`、`insert_bucket_full`、`insert_bucket_set`、`upsert_bucket_set`、`upsert_bucket_owner_set_mode`。理由是启动回填、set 指派、`BEGIN IMMEDIATE` 账户桶配额与 `set_id` 写；配额原子性属 Slice 5（账户/配额），set 路由属最终 set 迁移。
- `db.rs` 账户/配额读片段：`account_buckets_from`、`account_bucket_count_sql`、`bucket_quota_reached_conn`、`bucket_create_quota_reached_conn`（SQL 片段嵌入账户查询，Slice 5 账户/配额统一迁移）。
- `db.rs::delete_user` 与 `Db::delete_user_cascade` 中的 `UPDATE buckets SET owner_id = NULL WHERE owner_id = ?`（Slice 5 IAM；与 users 行删除同事务，保持 raw 以免拆散用户级联）。
- `state.rs`：stretch 启动空库守卫的计数读（启动期本地运维守卫，无行序/触发器语义）。
- 其余生产文件不得再出现这四张表的 SQL。

---

## 文件结构

| 文件 | 责任 |
|---|---|
| Create `crates/xl-store/src/control_store/bucket.rs` | 行类型、`BucketStore`、`BucketTxn`、`BucketSnapshotTxn`、freeze/拒绝未知字段测试 |
| Modify `crates/xl-store/src/control_store.rs` | `mod bucket;`、`pub use`、supertrait 拼装（按任务分批接入） |
| Create `crates/oss-server/src/control_store/bucket.rs` | 只读适配器 + 运行期属主投影 |
| Create `crates/oss-server/src/control_store/bucket_write.rs` | 点写/配置写/级联辅助 |
| Create `crates/oss-server/src/control_store/bucket_snapshot.rs` | 严格导出、整表替换、安装 |
| Modify `crates/oss-server/src/control_store.rs` | 模块声明与 trait impl 委托 |
| Modify `crates/oss-server/src/db.rs` | 读 façade 委托、写 façade 走 txn、级联改写、删除 raw helper |
| Modify `crates/oss-server/src/raft/apply.rs` | 桶命令改走 `BucketTxn` |
| Modify `crates/oss-server/src/raft/data.rs` | 行类型别名、`read_tx`/`install_tx` 改走 txn |
| Modify `crates/oss-server/src/console.rs` | 通知/加密读写改走 `Db` façade |
| Modify `crates/oss-server/src/events.rs`、`replication.rs`、`job_state/lifecycle.rs`、`raft/repair.rs` | 读路径改走 seam |
| `crates/oss-server/src/state.rs` | 不改；启动回填/空库守卫保留 raw（白名单） |
| Modify `crates/oss-server/src/site.rs`、`crates/xl-s3/src/access.rs` | 仅 `shortcut:` 注释 |
| Create `crates/oss-server/tests/control_store_bucket_diff.rs` | 点写/配置写差分测试 |
| Create `crates/oss-server/tests/control_store_bucket_cascade.rs` | site 触发器/回滚断言（公开 seam；级联测试在 db.rs 测试模块） |
| Create `crates/oss-server/tests/raw_sql_audit.rs` | 原始 SQL 白名单审计 |

---

### Task 1: xl-store 行类型与 trait

**Files:**
- Create: `crates/xl-store/src/control_store/bucket.rs`
- Modify: `crates/xl-store/src/control_store.rs`（`mod bucket;` + `pub use`；`ControlError` 增加 `MissingOwner(i64)`；本任务**不**改 supertrait）
- Modify: `crates/oss-server/src/control_store.rs`（`to_sqlx` 增加 `MissingOwner` 分支，保证本任务提交可编译）

**Interfaces:**
- Consumes: `OwnerRef`、`ControlError`。
- Produces:
  - `BucketEntry` 字段顺序固定为：`name: String, owner: OwnerRef, creation_date: i64, versioning: String, object_lock: bool, default_encryption: Option<String>, object_lock_default_mode: Option<String>, object_lock_default_days: Option<i64>, replication_mode: String, deleting: bool`。
  - `BucketView` 字段顺序固定为：`name: String, owner: Option<OwnerRef>, owner_uid: Option<i64>, owner_is_admin: bool, creation_date: i64, versioning: String, object_lock: bool, default_encryption: Option<String>, object_lock_default_mode: Option<String>, object_lock_default_days: Option<i64>, replication_mode: String, deleting: bool`。
  - `BucketPolicyEntry { bucket, policy_json, updated_at }`、`EventConfigEntry { bucket, config_json, updated_at }`、`ReplicationRuleEntry { bucket, rules_json, updated_at }`，均 `#[serde(deny_unknown_fields)]`。
  - `ControlError` 新变体 `MissingOwner(i64)`，`thiserror` 文本恰为 `owner user id {id} missing`（与 `DataError::MissingUser` 的 Display 一致）。`to_sqlx` 映射为 `sqlx::Error::Protocol(format!("owner user id {id} missing"))`；`control_data_err` 在 Task 6 映射回 `DataError::MissingUser(id)`。
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum BucketCreateOutcome { Created, Updated, ModeConflict }`（不进快照，不加 serde）。
  - `BucketStore`（只读）：`bucket`、`buckets`、`bucket_policy`、`bucket_policies`、`bucket_notification`、`event_configs`、`bucket_replication_rules`、`replication_rules`，全部 `async fn ... -> Result<_, ControlError>`；`buckets`/`bucket_policies`/`event_configs`/`replication_rules` 文档注明升序键。
  - `BucketTxn`（调用方事务内点写）：`bucket_create`、`bucket_delete`、`bucket_set_deleting`、`bucket_set_versioning`、`bucket_set_object_lock`、`bucket_set_default_encryption`、`bucket_clear_default_encryption`、`bucket_policy_set/delete`、`bucket_notification_set/delete`、`bucket_replication_rules_set/delete`、`bucket_exists`、`delete_bucket_configs_for_bucket`。
  - `BucketSnapshotTxn`（导出/安装）：`buckets`、`bucket_policies`、`replace_bucket_policies`、`event_configs`、`replace_event_configs`、`replication_rules`、`replace_replication_rules`、`bucket_install_prepare`、`bucket_upsert_install`。
- 精确签名（实现以此为准）：

```rust
#[async_trait::async_trait]
pub trait BucketStore: Send + Sync {
    async fn bucket(&self, name: &str) -> Result<Option<BucketView>, ControlError>;
    /// name 升序。
    async fn buckets(&self) -> Result<Vec<BucketView>, ControlError>;
    async fn bucket_policy(&self, bucket: &str) -> Result<Option<BucketPolicyEntry>, ControlError>;
    /// bucket 升序。
    async fn bucket_policies(&self) -> Result<Vec<BucketPolicyEntry>, ControlError>;
    async fn bucket_notification(&self, bucket: &str) -> Result<Option<EventConfigEntry>, ControlError>;
    /// bucket 升序。
    async fn event_configs(&self) -> Result<Vec<EventConfigEntry>, ControlError>;
    async fn bucket_replication_rules(&self, bucket: &str) -> Result<Option<ReplicationRuleEntry>, ControlError>;
    /// bucket 升序。
    async fn replication_rules(&self) -> Result<Vec<ReplicationRuleEntry>, ControlError>;
}

#[async_trait::async_trait]
pub trait BucketTxn: Send {
    async fn bucket_create(&mut self, row: &BucketEntry) -> Result<BucketCreateOutcome, ControlError>;
    async fn bucket_delete(&mut self, name: &str) -> Result<bool, ControlError>;
    async fn bucket_set_deleting(&mut self, name: &str, deleting: bool) -> Result<bool, ControlError>;
    async fn bucket_set_versioning(&mut self, name: &str, versioning: &str) -> Result<bool, ControlError>;
    async fn bucket_set_object_lock(&mut self, name: &str, object_lock: bool,
        default_mode: Option<&str>, default_days: Option<i64>) -> Result<bool, ControlError>;
    async fn bucket_set_default_encryption(&mut self, name: &str, algorithm: &str) -> Result<bool, ControlError>;
    async fn bucket_clear_default_encryption(&mut self, name: &str) -> Result<bool, ControlError>;
    async fn bucket_policy_set(&mut self, bucket: &str, policy_json: &str, updated_at: i64) -> Result<(), ControlError>;
    async fn bucket_policy_delete(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn bucket_notification_set(&mut self, bucket: &str, config_json: &str, updated_at: i64) -> Result<(), ControlError>;
    async fn bucket_notification_delete(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn bucket_replication_rules_set(&mut self, bucket: &str, rules_json: &str, updated_at: i64) -> Result<(), ControlError>;
    async fn bucket_replication_rules_delete(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn bucket_exists(&mut self, name: &str) -> Result<bool, ControlError>;
    async fn delete_bucket_configs_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError>;
}

#[async_trait::async_trait]
pub trait BucketSnapshotTxn: Send {
    /// 严格导出映射；悬空用户 → `MissingOwner`。name 升序。
    async fn buckets(&mut self) -> Result<Vec<BucketEntry>, ControlError>;
    async fn bucket_policies(&mut self) -> Result<Vec<BucketPolicyEntry>, ControlError>;
    async fn replace_bucket_policies(&mut self, rows: &[BucketPolicyEntry]) -> Result<(), ControlError>;
    async fn event_configs(&mut self) -> Result<Vec<EventConfigEntry>, ControlError>;
    async fn replace_event_configs(&mut self, rows: &[EventConfigEntry]) -> Result<(), ControlError>;
    async fn replication_rules(&mut self) -> Result<Vec<ReplicationRuleEntry>, ControlError>;
    async fn replace_replication_rules(&mut self, rows: &[ReplicationRuleEntry]) -> Result<(), ControlError>;
    /// 删目标独有桶，再 `UPDATE buckets SET owner_id = NULL`。
    async fn bucket_install_prepare(&mut self, wanted: &[String]) -> Result<(), ControlError>;
    /// INSERT ... ON CONFLICT(name) DO UPDATE（不含 set_id）。
    async fn bucket_upsert_install(&mut self, row: &BucketEntry) -> Result<(), ControlError>;
}
```

- 三个 trait 都 `#[async_trait::async_trait]`（上面已带）；`BucketStore: Send + Sync`，两个 txn trait `: Send`。

- [ ] **Step 1: 写失败测试**

在 `bucket.rs` 的 `#[cfg(test)]` 中为四个快照行各写一个 rmp 字段顺序测试和一个未知字段拒绝测试。以 `BucketEntry` 为例：

```rust
#[derive(Serialize)]
struct LegacyBucket {
    name: String,
    owner: super::OwnerRef,
    creation_date: i64,
    versioning: String,
    object_lock: bool,
    default_encryption: Option<String>,
    object_lock_default_mode: Option<String>,
    object_lock_default_days: Option<i64>,
    replication_mode: String,
    deleting: bool,
}

#[test]
fn bucket_entry_rmp_matches_snapshot_field_order() {
    let row = super::BucketEntry { /* 全字段样值 */ };
    let bytes = rmp_serde::to_vec_named(&row).unwrap();
    let legacy = LegacyBucket { /* 逐字段 clone */ };
    assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
    assert_eq!(rmp_serde::from_slice::<super::BucketEntry>(&bytes).unwrap(), row);
}
```

另外写 `bucket_entry_rejects_unknown_fields`、`bucket_policy_entry_rmp_matches_snapshot_field_order`、`event_config_entry_...`、`replication_rule_entry_...` 及各自的拒绝测试（模式与 `inventory.rs` 现有测试相同）。再写 `missing_owner_error_display_matches_snapshot`：`ControlError::MissingOwner(999).to_string() == "owner user id 999 missing"`（无前缀）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p xl-store control_store::bucket`
Expected: FAIL（`bucket` 模块/类型不存在）。

- [ ] **Step 3: 实现类型与 trait**

按 Interfaces 在 `crates/xl-store/src/control_store/bucket.rs` 写类型与三个 trait；在 `control_store.rs` 加 `pub mod bucket;` 并 `pub use bucket::{...}`。文档注释写明：`BucketEntry` 顺序即快照字节顺序；`BucketView.owner_uid/owner_is_admin` 是本地投影；`BucketTxn` 不 begin/commit/rollback；`BucketSnapshotTxn::bucket_upsert_install` 不写 `set_id`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p xl-store control_store::bucket`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/xl-store/src/control_store/bucket.rs \
  crates/oss-server/src/control_store.rs
git commit -m "feat(xl-store): add bucket and bucket-config control-store rows and traits"
```

---

### Task 2: SQLite 只读适配器与运行期属主投影

**Files:**
- Create: `crates/oss-server/src/control_store/bucket.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod bucket;`、`impl BucketStore for SqliteControlStore` 委托）
- Modify: `crates/xl-store/src/control_store.rs`（`ControlStore: ... + BucketStore`）

**Interfaces:**
- Consumes: Task 1 的 `BucketStore`、`BucketView`、四个 Entry。
- Produces: `SqliteControlStore` 实现 `BucketStore`；本任务起 `Arc<dyn ControlStore>` 可读这四张表。
- 运行期投影（必须逐条测试）：
  - `owner_uid == None | Some(0)` 或 `owner_username == None` → `OwnerRef::Admin`；
  - 其余 → `OwnerRef::User(username)`（管理员用户也映射为 `User`）；
  - `owner_is_admin = COALESCE(users.is_admin, 0) != 0`；
  - `owner_uid` 原样保留 `buckets.owner_id`。

- [ ] **Step 1: 写失败测试**

在 `crates/oss-server/src/control_store/bucket.rs` 的测试模块中：

```rust
#[tokio::test]
async fn runtime_bucket_owner_projection_matches_legacy() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    let admin = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    // 直接 SQL 造历史行：NULL 属主、alice、悬空 999、管理员用户 id
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, replication_mode, deleting) \
                 VALUES ('null-owner', NULL, 1, 'Off', 0, 'async', 0), \
                        ('alice-owner', ?, 2, 'Enabled', 1, 'sync', 0), \
                        ('dangling', 999, 3, 'Off', 0, 'async', 1), \
                        ('admin-owner', ?, 4, 'Off', 0, 'async', 0)")
        .bind(alice).bind(admin).execute(db.pool()).await.unwrap();

    let store = db.control_store();
    let v = store.bucket("null-owner").await.unwrap().unwrap();
    assert_eq!(v.owner, Some(xl_store::control_store::OwnerRef::Admin));
    assert_eq!(v.owner_uid, None);
    assert!(!v.owner_is_admin);

    let v = store.bucket("alice-owner").await.unwrap().unwrap();
    assert_eq!(v.owner, Some(xl_store::control_store::OwnerRef::User("alice".into())));
    assert_eq!(v.owner_uid, Some(alice));

    let v = store.bucket("dangling").await.unwrap().unwrap();
    assert_eq!(v.owner, Some(xl_store::control_store::OwnerRef::Admin));
    assert_eq!(v.owner_uid, Some(999));
    assert!(v.deleting);

    let v = store.bucket("admin-owner").await.unwrap().unwrap();
    assert_eq!(v.owner, Some(xl_store::control_store::OwnerRef::User("root".into())));
    assert!(v.owner_is_admin);
}
```

再写：`buckets_are_name_ordered`（两个桶名乱序插入，断言返回顺序）、`config_reads_are_bucket_ordered_and_exact`（策略/事件/复制各两行，断言 JSON 原文与顺序）、`missing_rows_are_none`。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server control_store::bucket`
Expected: FAIL（`bucket` 模块或 `BucketStore` 方法不存在）。

- [ ] **Step 3: 实现只读适配器**

在 `crates/oss-server/src/control_store/bucket.rs` 写：

```rust
const BUCKET_SELECT: &str = "SELECT b.name, b.owner_id, b.creation_date, b.versioning, \
    b.object_lock, b.default_encryption, b.object_lock_default_mode, \
    b.object_lock_default_days, b.replication_mode, b.deleting, \
    u.username AS owner_username, COALESCE(u.is_admin, 0) AS owner_is_admin \
    FROM buckets b LEFT JOIN users u ON u.id = b.owner_id";

fn bucket_view(r: &SqliteRow) -> BucketView { /* 运行期口径 */ }
```

`bucket` 用 `{BUCKET_SELECT} WHERE b.name = ?`；`buckets` 用 `ORDER BY b.name`。三个配置表分别 `SELECT bucket, policy_json, updated_at FROM bucket_policies WHERE bucket = ?` / 全表 `ORDER BY bucket`，`event_configs`（`config_json`）、`replication_rules`（`rules_json`）同构。错误统一 `ControlError::Backend(e.to_string())`。

在 `control_store.rs` 加 `mod bucket;` 并在 `impl ControlStore for SqliteControlStore` 中委托；在 `xl-store` 把 `ControlStore` 改为 `ControlStore: BucketStore + LifecycleStore + Send + Sync`（保留 Slice 3 已加的 inventory/tier supertrait，若已存在则只追加）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p xl-store control_store::bucket && cargo test -p oss-server control_store::bucket`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs crates/oss-server/src/control_store/bucket.rs
git commit -m "feat(oss-server): read buckets and bucket configs through ControlStore"
```

---

### Task 3: Db 读 façade 委托与消费点切换

**Files:**
- Modify: `crates/oss-server/src/db.rs`
- Modify: `crates/oss-server/src/events.rs:549`
- Modify: `crates/oss-server/src/replication.rs:67,159,222,326`
- Modify: `crates/oss-server/src/job_state/lifecycle.rs:36`
- Modify: `crates/oss-server/src/raft/repair.rs:118-123`（sync 桶清单读）
- Modify: `crates/oss-server/src/console.rs:2715-2745,2828-2844,2914-2924,4905-4914`（建桶前置检查、两处冲突补偿读、站点移除的 sync 桶计数）
- Modify: `crates/oss-server/src/console.rs:1200,2347`（通知/加密 GET）
- Modify: `crates/oss-server/src/db.rs`（本地例外处加 `shortcut:` 注释）

**Interfaces:**
- Consumes: Task 2 的 `BucketStore`。
- Produces（`Db` 公有读 API 签名不变，内部委托）：
  - `bucket_exists(&str) -> Result<bool>`：`control_store().bucket(name).is_some()`。
  - `bucket_policy(&str) -> Result<Option<String>>`：取 `BucketPolicyEntry.policy_json`。
  - 新增 `bucket_notification(&str) -> Result<Option<String>>`：取 `EventConfigEntry.config_json`。
  - `bucket_account(&str) -> Result<String>`：缺失桶 → `LEGACY_ACCOUNT`；否则 `xl_s3::access::bucket_account_id(v.owner_uid, v.owner_is_admin)`。
  - `owned_buckets(uid) -> Result<Vec<String>>`：`buckets()` 中 `owner_uid == Some(uid)`，顺序沿用 name 升序。
  - `bucket_replication_modes()`、`bucket_sync_state_row()`、`bucket_replication_mode()`、`get_bucket_versioning()`、`get_bucket_default_encryption()`、`list_bucket_default_encryptions()`、`list_bucket_rows()`：全部由 `BucketView` 投影，缺行默认值保持不变（`async`/`Off`/`None`）。
- `events.rs::load_targets`：属主读改 `SqliteControlStore::new(self.pool.clone()).bucket(bucket).await.ok().flatten().and_then(|v| v.owner_uid)`（保留原 `.ok()` 吞错语义）；`SELECT config_json FROM event_configs` 改 `bucket_notification`。
- `replication.rs`：`SqlReplStore::load_rules` 与 `SqlReplSink` 的 `SELECT rules_json`（第 222 行附近）改 `bucket_replication_rules`；两处 `SELECT versioning` 改 `bucket` 视图。缺行分别返回 `None`/`false`，读错保持现 `Ok(Some)/Ok(None)/Err` 三态分支的 warn/回落语义。
- `job_state/lifecycle.rs::lifecycle_bucket_exists`：`db.bucket_exists(bucket).await`。
- `raft/repair.rs` 的 sync 桶清单：`db.control_store().buckets()` 过滤 `replication_mode == "sync"`、取 `name`；seam 已按 name 升序，等价于旧 `ORDER BY name`。
- `console.rs` 建桶：`SELECT replication_mode` → `state.db.control_store().bucket(&body.name).await`（保留 `if let Ok(Some(v))` 吞错）；`SELECT 1 FROM buckets` → `state.db.bucket_exists`。
- `console.rs` 通知 GET：`state.db.bucket_notification`；加密 GET：`state.db.get_bucket_default_encryption`。
- `console.rs` 两处站点冲突补偿 `SELECT 1 FROM buckets WHERE name = ?` → `state.db.bucket_exists(...)`；读错分支继续保留物理桶并 warn，语义不变。
- `console.rs` 站点对端移除前的 sync 桶计数 → `state.db.bucket_replication_modes()` 中值为 `"sync"` 的条目数（复用 Task 3 已迁移的 façade，保持读错分支）。
- `state.rs` stretch 空库守卫保持 raw（D7 白名单），本任务不改。

- [ ] **Step 1: 写失败测试（新 façade）**

在 `db.rs` 测试模块新增：

```rust
#[tokio::test]
async fn bucket_read_facades_project_owner_and_configs() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date, versioning, replication_mode) \
                 VALUES ('b1', ?, 1, 'Enabled', 'sync'), ('b2', NULL, 2, 'Off', 'async')")
        .bind(alice).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('b1', '{}', 5)")
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO event_configs (bucket, config_json, updated_at) VALUES ('b1', '[]', 6)")
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES ('b1', '[]', 7)")
        .execute(db.pool()).await.unwrap();

    assert!(db.bucket_exists("b1").await.unwrap());
    assert!(!db.bucket_exists("missing").await.unwrap());
    assert_eq!(db.bucket_policy("b1").await.unwrap().as_deref(), Some("{}"));
    assert_eq!(db.bucket_notification("b1").await.unwrap().as_deref(), Some("[]"));
    assert_eq!(db.owned_buckets(alice).await.unwrap(), vec!["b1"]);
    assert_eq!(db.bucket_replication_modes().await.unwrap().get("b1").unwrap(), "sync");
    assert_eq!(db.bucket_sync_state_row("b1").await.unwrap(), Some(("sync".into(), false, "Enabled".into())));
    assert_eq!(db.bucket_sync_state_row("missing").await.unwrap(), None);
    assert_eq!(db.get_bucket_versioning("missing").await.unwrap(), "Off");
    assert_eq!(db.list_bucket_rows().await.unwrap(), vec!["b1", "b2"]);
}
```

Task 3 其余改动是行为保持型重构：由本测试 + Task 8 差分 + 既有全量测试共同钉住；不为“内部改成委托”写无法观察的伪 RED。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server bucket_read_facades_project_owner_and_configs`
Expected: FAIL（`bucket_notification` 不存在；其余断言因未迁移仍可能通过——以编译失败为 RED）。

- [ ] **Step 3: 实现委托与消费点切换**

按 Interfaces 修改。注意：
- `Db::bucket_account` 必须使用 `owner_uid/owner_is_admin`，不要用 `OwnerRef`。
- `replication.rs` 与 `events.rs` 每次构造 `SqliteControlStore::new(self.pool.clone())`，复用同一池，不新建池。
- 在 `db.rs` 的 `insert_bucket*`、`upsert_bucket_set`、`upsert_bucket_owner_set_mode` 上方各加一行：
  `// shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。`
- 在 `account_buckets_from` / `bucket_create_quota_reached_conn` / `delete_user` / `delete_user_cascade` 的 buckets SQL 上方加：
  `// shortcut: 账户配额 SQL 片段与 IAM 用户级联同属 Slice 5（账户/配额），随该切片迁移。`
- 在 `cluster_control.rs` 的 `BucketSet`/`BucketRowSet`/`DbSetResolver` 处加：
  `// shortcut: mesh 本地 set 路由；无 ORDER BY 的行序是既有对端编码行为，Slice 6 统一迁移。`

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server bucket_read_facades_project_owner_and_configs && cargo test -p oss-server`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/db.rs crates/oss-server/src/events.rs crates/oss-server/src/replication.rs \
  crates/oss-server/src/job_state/lifecycle.rs crates/oss-server/src/raft/repair.rs \
  crates/oss-server/src/console.rs crates/oss-server/src/cluster_control.rs
git commit -m "refactor(oss-server): route bucket reads through ControlStore"
```

---

### Task 4: 写适配器与 Raft apply 切换

**Files:**
- Create: `crates/oss-server/src/control_store/bucket_write.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod bucket_write;`、实现 `BucketTxn` 委托）
- Modify: `crates/xl-store/src/control_store.rs`（`ControlTxn: ... + BucketTxn`）
- Modify: `crates/oss-server/src/raft/apply.rs:420-630`
- Modify: `crates/oss-server/src/db.rs`（删除 `create_bucket_tx`、`CreateBucketOutcome` 与 11 个 `*_raw` helper）

**Interfaces:**
- Consumes: Task 1 的 `BucketTxn`、`BucketCreateOutcome`。
- Produces: `SqliteControlTxn` 实现 `BucketTxn`；apply 的桶命令全部走 txn。
- `bucket_create` 内部顺序：解析属主 → 查模式 → 写。属主映射：
  - `Admin` → `None`；
  - `User(username)` 存在 → `Some(id)`；
  - 不存在且存在管理员 → `Err(Invalid("bucket owner user {username} not found"))`；
  - 不存在且无管理员（witness）→ `None`。
- SQL 必须逐字复刻 `Db::create_bucket_tx` 的 INSERT/UPDATE 与各 `*_raw` helper（见 D2）。
- 现有 `to_sqlx` 把 `Invalid(m)` 映射为 `Protocol(m)`（不加前缀），所以属主缺失的 fail-stop 文本与旧 `invariant(...)` 完全一致；Task 1 的 `MissingOwner` 分支同样不加前缀。
- `raft/apply.rs` 现有的 `bucket_exists(cw, bucket)` helper（AP put / inventory put 仍在用）改为 `SqliteControlTxn::new(cw.conn()).bucket_exists(bucket).await.map_err(to_sqlx)`；桶 handler 可直接复用该 helper，避免重复构造 txn。改后 `apply.rs` 不再含 buckets 原始 SQL。
- apply 删除 `bucket_owner_id` helper（仅 BucketCreate 使用）；保留 `admin_owner_id`（AP/inventory 仍用）。

- [ ] **Step 1: 写失败测试**

在 `bucket_write.rs` 测试模块：

```rust
#[tokio::test]
async fn bucket_create_mode_conflict_is_zero_write() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
                 replication_mode, deleting, set_id) VALUES ('b', NULL, 7, 'Enabled', 1, 'async', 0, 3)")
        .execute(db.pool()).await.unwrap();
    let before: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT creation_date, versioning, object_lock, set_id FROM buckets WHERE name = 'b'")
        .fetch_one(db.pool()).await.unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let out = crate::control_store::SqliteControlTxn::new(&mut tx)
        .bucket_create(&xl_store::control_store::BucketEntry {
            name: "b".into(), owner: xl_store::control_store::OwnerRef::Admin,
            creation_date: 999, versioning: "Off".into(), object_lock: false,
            default_encryption: None, object_lock_default_mode: None,
            object_lock_default_days: None, replication_mode: "sync".into(), deleting: false,
        }).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(out, xl_store::control_store::BucketCreateOutcome::ModeConflict);
    let after: (i64, String, i64, i64) = sqlx::query_as(
        "SELECT creation_date, versioning, object_lock, set_id FROM buckets WHERE name = 'b'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(before, after, "冲突路径不得写任何列");
}
```

再写：`bucket_create_missing_user_with_admin_is_invalid`（有管理员，User 缺失 → `Invalid`，桶表零行）、`bucket_create_without_admin_maps_user_to_null`（witness 口径）、`point_writes_missing_bucket_return_false`、`policy_notification_replication_upsert_and_delete`（断言 `updated_at` 被替换、删除幂等）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server control_store::bucket_write`
Expected: FAIL（模块/impl 不存在）。

- [ ] **Step 3: 实现写适配器并切换 apply**

实现 `bucket_write.rs` 中的委托函数；在 `control_store.rs` 加 `mod bucket_write;` 并在 `impl BucketTxn for SqliteControlTxn<'_>` 中委托（`ControlTxn` 的 impl 块可写在 `control_store.rs`，SQL 在子模块）。在 `xl-store` 把 `ControlTxn` 改为 `ControlTxn: BucketTxn + LifecycleTxn + Send`（保留 Slice 3 的 inventory/tier supertrait）。

`raft/apply.rs`：
- 顶部 use 调整：在 `xl_store::control_store` 导入中加入 `BucketCreateOutcome`、`BucketEntry`（`OwnerRef`/`ControlTxn` 已导入），从 `crate::db` 的 use 中移除 `CreateBucketOutcome`。
- `BucketCreate`：保留现有校验，然后

```rust
let owner = owner.as_ref().map_or(OwnerRef::Admin, |u| OwnerRef::User(u.clone()));
let mut txn = crate::control_store::SqliteControlTxn::new(cw.conn());
match txn.bucket_create(&BucketEntry { name: name.clone(), owner, creation_date: *created_at,
    versioning: versioning.clone(), object_lock: *object_lock,
    default_encryption: default_encryption.clone(),
    object_lock_default_mode: object_lock_default_mode.clone(),
    object_lock_default_days: *object_lock_default_days,
    replication_mode: replication_mode.clone(), deleting: false })
    .await.map_err(crate::control_store::to_sqlx)?
{ BucketCreateOutcome::ModeConflict => ControlResponse::Conflict,
  BucketCreateOutcome::Created | BucketCreateOutcome::Updated => ControlResponse::Ok }
```

- `BucketSetDeleting/Versioning/ObjectLock/DefaultEncryption/ClearDefaultEncryption/Notification/ClearNotification/Replication/ClearReplication/SetPolicy/DeletePolicy`：用 txn 方法与 `bucket_exists` helper，响应映射完全不变。
- `BucketDelete` 暂不改（Task 7 改级联）。
- `db.rs` 删除本切片域的 raw writer：`create_bucket_tx`、`CreateBucketOutcome`、`set_bucket_deleting_raw`、`set_bucket_versioning_raw`、`set_bucket_object_lock_raw`、`set_bucket_notification_raw`、`clear_bucket_notification_raw`、`set_replication_rules_raw`、`clear_replication_rules_raw`、`set_bucket_default_encryption_raw`、`clear_bucket_default_encryption_raw`、`set_bucket_policy_raw`、`delete_bucket_policy_raw`。**不要删** `set_lifecycle_rules_raw` 等 lifecycle helper（Slice 3 域）。删除前确认无其他调用者（`grep -rn`）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server control_store::bucket_write && cargo test -p oss-server raft::apply`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs \
  crates/oss-server/src/control_store/bucket_write.rs crates/oss-server/src/raft/apply.rs crates/oss-server/src/db.rs
git commit -m "feat(oss-server): write bucket control rows through ControlTxn"
```

---

### Task 5: 单节点写 façade、console 与复制规则写

**Files:**
- Modify: `crates/oss-server/src/db.rs`（`set_bucket_policy`、`clear_bucket_policy`、`set_bucket_deleting`、`set_bucket_default_encryption`、`clear_bucket_default_encryption` 走 txn；新增 `set_bucket_notification`、`clear_bucket_notification`）
- Modify: `crates/oss-server/src/console.rs:1200,1273,1300,2347,2409,2442,2949,2952`
- Modify: `crates/oss-server/src/replication.rs:80-112`（`save_rules`/`remove_rules` 非 Raft 分支）
- Modify: `crates/xl-s3/src/access.rs`（仅 `shortcut:` 注释）

**Interfaces:**
- `Db` façade 统一模式：`let mut tx = self.pool.begin().await?; { let mut txn = SqliteControlTxn::new(&mut tx); txn....await.map_err(to_sqlx)?; } tx.commit().await?;`
- `Db::set_bucket_notification(&self, bucket: &str, config_json: &str, updated_at: i64) -> Result<()>`
- `Db::clear_bucket_notification(&self, bucket: &str) -> Result<()>`
- `Db::set_bucket_policy` / `Db::clear_bucket_policy` 签名不变。
- `Db::set_bucket_deleting` 返回 `Result<()>` 不变（tz 内 `bucket_set_deleting` 的 bool 被忽略）。
- `SqlReplStore::save_rules`/`remove_rules` 非 Raft 分支：`self.pool.begin()` → `SqliteControlTxn` → commit；时间戳仍只取一次 `now()`。
- console 单节点分支改调 `state.db` 对应 façade；`require_owner_or_admin`、状态码、日志文案与返回体保持原样。通知写入的 `updated_at` 传 `crate::db::now()`（与 Raft 分支同一时间源，且每次请求只取一次）。
- console 删桶清理：`clear_bucket_policy`/`clear_bucket_notification` 改 `state.db`；`clear_lifecycle_rules` 保持 `state.acl()`（Slice 3 域）。
- `xl-s3/src/access.rs` 在 `clear_bucket_policy` 上方加：
  `// shortcut: Acl 只持有 SqlitePool，无法看到 oss-server 适配器；S3 热路径迁移归 Slice 5（IAM/账户）或最终 SQLite 移除。`

- [ ] **Step 1: 写失败测试**

在 `db.rs` 测试模块：

```rust
#[tokio::test]
async fn single_node_bucket_config_writes_roundtrip() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    db.create_user("root", "pw", true).await.unwrap();
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('b', NULL, 1)")
        .execute(db.pool()).await.unwrap();

    db.set_bucket_policy("b", "{}", 10).await.unwrap();
    assert_eq!(db.bucket_policy("b").await.unwrap().as_deref(), Some("{}"));
    db.clear_bucket_policy("b").await.unwrap();
    assert!(db.bucket_policy("b").await.unwrap().is_none());

    db.set_bucket_notification("b", "[]", 11).await.unwrap();
    assert_eq!(db.bucket_notification("b").await.unwrap().as_deref(), Some("[]"));
    db.clear_bucket_notification("b").await.unwrap();
    assert!(db.bucket_notification("b").await.unwrap().is_none());

    assert!(db.set_bucket_default_encryption("b", "AES256").await.unwrap());
    assert_eq!(db.get_bucket_default_encryption("b").await.unwrap().as_deref(), Some("AES256"));
    assert!(db.clear_bucket_default_encryption("b").await.unwrap());
    assert!(db.get_bucket_default_encryption("b").await.unwrap().is_none());

    db.set_bucket_deleting("b", true).await.unwrap();
    assert!(db.bucket_sync_state_row("b").await.unwrap().unwrap().1);
}
```

另写 `config_write_missing_bucket_is_idempotent_or_false`（policy/notification 删除不报错；加密写入返回 false）。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server single_node_bucket_config_writes_roundtrip`
Expected: FAIL（`set_bucket_notification` 不存在）。

- [ ] **Step 3: 实现 façade 与 console/复制切换**

按 Interfaces 修改；所有错误用 `crate::control_store::to_sqlx` 映射，保持调用方 `Result` 类型。console 分支只替换调用目标，不改状态码/日志文案。`SqlReplStore` 仍保留 `pool` 字段，不引入 `Db`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server single_node_bucket_config_writes_roundtrip && cargo test -p oss-server`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/db.rs crates/oss-server/src/console.rs \
  crates/oss-server/src/replication.rs crates/xl-s3/src/access.rs
git commit -m "refactor(oss-server): route single-node bucket config writes through ControlTxn"
```

---

### Task 6: 快照导出与安装走 txn

**Files:**
- Create: `crates/oss-server/src/control_store/bucket_snapshot.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod bucket_snapshot;`、实现 `BucketSnapshotTxn` 委托）
- Modify: `crates/xl-store/src/control_store.rs`（`ControlTxn: ... + BucketSnapshotTxn`）
- Modify: `crates/oss-server/src/raft/data.rs:96-137`（四个类型别名）、`read_tx` 桶与配置段、`install_tx` 第 2/4/10/12 步

**Interfaces:**
- Consumes: Task 1 的 `BucketSnapshotTxn`、Task 4 的 `BucketTxn`。
- Produces:
  - `BucketTxn` 不变；`BucketSnapshotTxn` 实现。
  - `raft/data.rs`：`pub type BucketRow = xl_store::control_store::BucketEntry;` 等四个别名，`ControlData` 字段与顺序不动。
  - 严格导出映射（`buckets()`）：
    - `owner_id None | Some(0)` → `Admin`；
    - `is_admin != 0` → `Admin`；
    - `Some(id)` 且用户存在 → `User(username)`；
    - `Some(id)` 且用户缺失 → `Err(ControlError::MissingOwner(id))`。
  - `bucket_upsert_install`：`User` 缺失 → `Err(Invalid(format!("bucket owner {name} not in snapshot")))`；`Admin` → `owner_id NULL`；更新列不含 `set_id`。
  - `bucket_install_prepare(wanted: &[String])`：读现有桶名 → 逐个删不在 `wanted` 的桶 → `UPDATE buckets SET owner_id = NULL`。
  - `replace_bucket_policies` / `replace_event_configs` / `replace_replication_rules`：`DELETE FROM <table>` 后按入参顺序 `INSERT`（含 `updated_at`，不做 upsert）。

- [ ] **Step 1: 写失败测试**

在 `bucket_snapshot.rs` 测试模块：

```rust
#[tokio::test]
async fn strict_export_maps_owners_and_rejects_dangling() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    let admin = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
                 default_encryption, object_lock_default_mode, object_lock_default_days, \
                 replication_mode, deleting) \
                 VALUES ('admin-null', NULL, 1, 'Off', 0, NULL, NULL, NULL, 'async', 0), \
                        ('admin-user', ?, 2, 'Enabled', 1, 'AES256', 'COMPLIANCE', 7, 'sync', 1), \
                        ('alice', ?, 3, 'Off', 0, NULL, NULL, NULL, 'async', 0)")
        .bind(admin).bind(alice).execute(db.pool()).await.unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let rows = crate::control_store::SqliteControlTxn::new(&mut tx).buckets().await.unwrap();
    assert_eq!(rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
               vec!["admin-null", "admin-user", "alice"]);
    assert_eq!(rows[0].owner, xl_store::control_store::OwnerRef::Admin);
    assert_eq!(rows[1].owner, xl_store::control_store::OwnerRef::Admin); // is_admin=1
    assert_eq!(rows[2].owner, xl_store::control_store::OwnerRef::User("alice".into()));
    tx.rollback().await.unwrap();

    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('dangling', 999, 5)")
        .execute(db.pool()).await.unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    let err = crate::control_store::SqliteControlTxn::new(&mut tx).buckets().await.unwrap_err();
    assert_eq!(err.to_string(), "owner user id 999 missing"); // 不含 "control data:" 前缀
}
```

再写：`install_roundtrip_preserves_set_id_and_config_bytes`（目标库先插 `set_id=7` 的同名桶；`bucket_install_prepare` + `bucket_upsert_install` + 三个 `replace_*` 后，桶列等于快照、`set_id` 仍为 7、配置表字节与顺序一致）、`install_missing_bucket_owner_is_rejected`（断言错误文本 `bucket owner ghost not in snapshot`，且桶表无该行）。

在 `raft/data.rs` 测试模块补 `read_tx_dangling_bucket_owner_is_missing_user`：造悬空 `owner_id` 桶行，调用 `read_tx`，断言 `matches!(err, DataError::MissingUser(999))` 且 `err.to_string() == "owner user id 999 missing"`。

`buckets_only_require_feature_level_one` 放在 `raft/data.rs` 的测试模块（`required_feature_level` 是 `pub fn`）：构造只含 `buckets` 的 `ControlData`，断言返回 `MIN_FEATURE_LEVEL`。

在 `tests/compat_fixtures.rs` 补一个 JSON 字节测试：同一 `ControlData` 经旧字段顺序序列化与新别名类型序列化，`serde_json::to_vec` 相等。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server control_store::bucket_snapshot`
Expected: FAIL（模块/方法不存在）。

- [ ] **Step 3: 实现快照适配器并切换 data.rs**

实现 `bucket_snapshot.rs` 与 `impl BucketSnapshotTxn for SqliteControlTxn<'_>` 委托；在 `xl-store` 给 `ControlTxn` 追加 `BucketSnapshotTxn` supertrait。

`raft/data.rs`：
- 四个 `pub type` 别名替换本地 struct 定义；保留 `deny_unknown_fields` 语义（来自 xl-store 类型）。
- `control_data_err` 增加分支：`ControlError::MissingOwner(id) => DataError::MissingUser(id)`（保住 `owner user id {id} missing` 的精确文本，不带 `control data:` 前缀）。
- `read_tx`：桶段改为 `let mut txn = SqliteControlTxn::new(&mut *conn); data.buckets = txn.buckets().await.map_err(control_data_err)?;`，三个配置表段同理；保持与 lifecycle 段相同的相对顺序。
- `install_tx`：第 2 步删除 `bucket_policies`/`event_configs`/`replication_rules` 三行；第 4 步改为 `txn.bucket_install_prepare(&wanted_names)`；第 10 步改为 `txn.bucket_upsert_install(b)`；第 12 步改为 `txn.replace_bucket_policies(&data.bucket_policies)?`，保持 lifecycle 插入在中间，再 `replace_event_configs`、`replace_replication_rules`。
- 不修改 `required_feature_level`、`owner_of`（若仍被其他域使用则保留）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server control_store::bucket_snapshot && cargo test -p oss-server raft::data && cargo test -p oss-server --test compat_fixtures`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs \
  crates/oss-server/src/control_store/bucket_snapshot.rs crates/oss-server/src/raft/data.rs \
  crates/oss-server/tests/compat_fixtures.rs
git commit -m "feat(oss-server): export and install bucket snapshots through ControlTxn"
```

---

### Task 7: 桶删除级联、clear_bucket_m8 范围与触发器断言

**Files:**
- Modify: `crates/oss-server/src/db.rs:1165-1188`（`clear_bucket_m8_tx` 保持不变，仅确认范围）、`6885-6919`（`delete_bucket_cascade`）、以及 `db.rs` 的 `#[cfg(test)] mod tests`（新增级联/范围单元测试）
- Modify: `crates/oss-server/src/site.rs:323,422`（仅 `shortcut:` 注释）
- Create: `crates/oss-server/tests/control_store_bucket_cascade.rs`（触发器/回滚断言，只用公开 seam）

**Interfaces:**
- Consumes: `delete_access_points_for_bucket`（Slice 2）、`delete_lifecycle_inventory_tier_for_bucket`（Slice 3）、Task 4 的 `delete_bucket_configs_for_bucket` 与 `bucket_delete`。
- Produces: `Db::delete_bucket_cascade(&mut ControlWrite, bucket) -> Result<bool>` 语义与返回不变，内部 SQL 全部走缝（`permissions` 除外）。

- [ ] **Step 1: 写失败测试**

`Db::delete_bucket_cascade` 与 `control_tx` 都是 `pub(crate)`，集成测试调不到；级联测试放 `db.rs` 的 `#[cfg(test)] mod tests`（`use super::*` + `crate::raft::apply::control_tx`）：

```rust
// 建 b1/b2 两桶；13 张从属表各给两桶各 1 行；permissions 各 1 行。
// let mut tx = db.pool().begin().await.unwrap();
// let mut cw = control_tx(&mut tx);
// 调用 Db::delete_bucket_cascade(&mut cw, "b1")；断言：
// - 13 张表中 b1 行数为 0，b2 行数为 1；
// - buckets 只剩 b2；返回 true；
// - 再次调用返回 false（幂等清残留）。
#[tokio::test]
async fn cascade_deletes_all_13_tables_and_spares_other_bucket() { /* ... */ }
```

`clear_bucket_m8_tx` 范围断言**扩展现有** `clear_bucket_m8_tx_deletes_bucket_ap_olap_only`（`db.rs` 测试模块）：预置 policies/event/replication/permissions/buckets 行，调用后断言这五类行仍在，仅 AP/OLAP + 7 张 lifecycle/inventory/tier 表被清。不新建重复测试。

`tests/control_store_bucket_cascade.rs`（公开 API：`Db::connect`、`SqliteControlTxn`、`xl_store` trait）：
- 触发器断言：
- `site_bucket_update_emits_exact_changelog_and_clock`：先造桶（`sites` 为空，不触发；`site_config` 已由 `Db::connect` 种下），再 `INSERT INTO sites`，记录 `MAX(seq)`；调用 seam `bucket_set_versioning("b1","Enabled")`；断言新增恰 1 行 `entity='bucket'`、`op='upsert'`、payload 等于 `json_object` 生成的精确字符串、clock 元组等于该 changelog 行。
- `site_bucket_create_emits_insert_changelog`：先 `INSERT INTO sites`（`site_config` 已存在），再 `bucket_create` 新桶；断言恰 1 行 `entity='bucket'`、`op='upsert'`、payload 键序/值与触发器 INSERT 分支一致（含 `owner_username`）。
- `site_bucket_update_of_default_encryption_emits_nothing`：种子同上；调用 `bucket_set_default_encryption`/`bucket_set_deleting`/`bucket_set_object_lock`（仅改默认 mode/days）→ `site_changelog` 无新增。
- `site_bucket_delete_emits_name_only_payload`：`bucket_delete` → payload 恰为 `{"name":"b1"}`。
- `site_policy_upsert_and_delete_emit_policy_changelog`：upsert 与 delete 各产生 1 行，payload 字段/顺序与触发器一致。
- `event_and_replication_writes_emit_no_site_changelog`：两表写入后 `site_changelog` 无新增。
- `site_gate_rejects_stale_bucket_write`：先造桶并 `INSERT INTO sites`，再插入 `site_entity_clock('bucket','b1', now+3600, ...)`；seam 写 versioning → `Err(Backend)` 且消息含 `site replication conflict`，行未变。
- `rollback_discards_seam_writes`：事务内写 policy/notification/replication 后 rollback，三表无行。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server --lib cascade_deletes_all_13_tables_and_spares_other_bucket && cargo test -p oss-server --test control_store_bucket_cascade`
Expected: PASS（characterization baseline）。本任务是行为保持型改写，无可观察 RED：先用这套测试钉住旧 `delete_bucket_cascade`/raw helper 的级联范围与触发器行为，改写后 Step 4 必须仍全绿。若任一断言在旧实现上失败，说明测试期望与现状不符，先修测试期望，不得改实现来迁就。

- [ ] **Step 3: 实现级联与注释**

`delete_bucket_cascade`：

```rust
pub(crate) async fn delete_bucket_cascade(cw: &mut ControlWrite<'_>, bucket: &str) -> Result<bool> {
    {
        let mut txn = crate::control_store::SqliteControlTxn::new(cw.conn());
        txn.delete_access_points_for_bucket(bucket)
            .await.map_err(crate::control_store::to_sqlx)?;
        txn.delete_lifecycle_inventory_tier_for_bucket(bucket)
            .await.map_err(crate::control_store::to_sqlx)?;
    } // 归还 &mut conn，permissions 仍走 raw（Slice 5）
    sqlx::query("DELETE FROM permissions WHERE bucket = ?")
        .bind(bucket).execute(&mut *cw.conn()).await?;
    let mut txn = crate::control_store::SqliteControlTxn::new(cw.conn());
    txn.delete_bucket_configs_for_bucket(bucket)
        .await.map_err(crate::control_store::to_sqlx)?;
    txn.bucket_delete(bucket).await.map_err(crate::control_store::to_sqlx)
}
```

（Slice 3 合并后若 `delete_lifecycle_inventory_tier_for_bucket` 的借用方式不同，按实际接口调整；不得改变 DELETE 顺序。）

`site.rs` 两个函数首行加：

```rust
// shortcut: 站点 LWW 时钟门控、apply_ctx 抑制与 set_id/三列保留语义与 seam 调用方模型耦合；
// 迁移归 Slice 6（站点/审计），现在改会改变 gate/changelog 行为。
```

`clear_bucket_m8_tx` 若 Slice 3 已改为两个 txn 方法，保持；否则确认它仍只清 AP/OLAP + 7 表。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server --lib cascade_deletes_all_13_tables_and_spares_other_bucket && cargo test -p oss-server --test control_store_bucket_cascade && cargo test -p oss-server`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/db.rs crates/oss-server/src/site.rs \
  crates/oss-server/tests/control_store_bucket_cascade.rs
git commit -m "refactor(oss-server): cascade bucket deletion through ControlTxn and pin triggers"
```

---

### Task 8: 端到端差分与原始 SQL 审计

**Files:**
- Create: `crates/oss-server/tests/control_store_bucket_diff.rs`
- Modify: `crates/oss-server/src/db.rs` 的 `#[cfg(test)] mod tests`（级联差分 `cascade_matches_legacy_reference`；`delete_bucket_cascade` 是 `pub(crate)`）
- Create/Modify: `crates/oss-server/tests/raw_sql_audit.rs`（若 Slice 3 已建同名审计测试，扩展白名单与模式，不新建重复文件）
- Modify: `crates/oss-server/tests/support/control_store_rows.rs`（若 Slice 3 已建；否则在 `tests/support/` 内扩展，不新建重复 helper）

**Interfaces:**
- Consumes: Task 2/4/5/6/7 全部 seam 方法；`Db::connect`（M7 无 guard、site 触发器惰性）与 `Db::connect_raft`（无 site 触发器、33 guard）。
- Produces: 逐操作差分与审计门禁。

- [ ] **Step 1: 写失败测试**

`control_store_bucket_diff.rs` 内实现旧 SQL 副本（只存在于测试）：
`legacy_create_bucket`、`legacy_set_deleting/versioning/object_lock/default_encryption`、`legacy_policy_set/delete`、`legacy_notification_set/delete`、`legacy_replication_set/delete`。每个测试：

1. 两个独立 `Db::connect_site("sqlite::memory:", false, Some("test-site"))`（固定站点 id，否则 `site_config` 的随机 id 会让 changelog 的 `origin_site_id` 必然不同），相同种子（用户 + 旧行）；
2. 两边都 `INSERT INTO sites`（`site_config` 已由 `Db::connect` 种下）使触发器生效，记录起始 `MAX(seq)`；
3. A 库跑 legacy SQL，B 库跑 seam 调用；
4. dump `buckets`/`bucket_policies`/`event_configs`/`replication_rules`（显式列 + `ORDER BY`）逐行 `assert_eq!`；`site_changelog` 只比较确定性列 `(entity, entity_id, op, payload, priority)`（`seq > start`，按 seq 排序）——`ts` 取自 `strftime('now')`，两库跨秒会假阳性；`site_entity_clock` 比较 `(entity, entity_id, priority, origin_site_id)` 并断言 `ts` 等于该库对应 changelog 行的 `ts`；
5. 覆盖：create 新建/同模式更新/模式冲突零写、六个 set 操作、三组配置 upsert/delete、缺失桶的幂等删除。
6. `rollback_discards_txn`：B 库事务内写后 rollback，再与 A 库种子比较。

`db.rs` 测试模块补 `cascade_matches_legacy_reference`（in-crate，`control_tx` 构造 `ControlWrite`）：旧级联 SQL 副本 + 13 表种子；调用现 `delete_bucket_cascade`，dump 全部表比较（含 `site_changelog` 的 DELETE 记录）。

`raw_sql_audit.rs`：
- 递归读 `CARGO_MANIFEST_DIR/src/**/*.rs`；识别 `#[cfg(test)]` 后紧跟的顶层 `mod ... {`，跳过到列首 `}` 为止（可处理多个测试模块；不做花括号计数，避免 JSON/格式串里的 `{}` 干扰）。**不得**只截断到首个 `#[cfg(test)]`——`db.rs` 有多个测试模块，且测试模块之间仍有生产代码；`raft/apply.rs` 的 `#[cfg(test)]` match 分支不是模块，不应触发跳过；
- 命中模式（只匹配 SQL 短语，避免误报结构体名/表名字符串）：`FROM buckets`、`INTO buckets`、`UPDATE buckets`、`DELETE FROM buckets`，以及 `bucket_policies`/`event_configs`/`replication_rules` 各自的 `FROM`/`INTO`/`UPDATE`/`DELETE FROM` 短语；
- 允许文件：`src/control_store/bucket.rs`、`bucket_write.rs`、`bucket_snapshot.rs`、`src/db.rs`、`src/site.rs`、`src/cluster_control.rs`、`src/state.rs`（启动回填与空库守卫）；
- 断言已删除 helper 名称不存在：`create_bucket_tx`、`set_bucket_policy_raw`、`delete_bucket_policy_raw`、`set_bucket_notification_raw`、`clear_bucket_notification_raw`、`set_replication_rules_raw`、`clear_replication_rules_raw`、`set_bucket_default_encryption_raw`、`clear_bucket_default_encryption_raw`、`set_bucket_deleting_raw`、`set_bucket_versioning_raw`、`set_bucket_object_lock_raw`；
- 失败信息打印 `file:line: matched text`，便于新增路径时定位。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p oss-server --lib cascade_matches_legacy_reference && cargo test -p oss-server --test control_store_bucket_diff --test raw_sql_audit`
Expected: 差分测试 PASS（行为保持的验收基线；若发现真实行为差异则修实现，不改测试期望）；**审计 FAIL**——首次运行会列出尚未删除的 raw helper 或未迁移路径。审计失败是本任务的 RED。

- [ ] **Step 3: 清理剩余 raw SQL 与白名单**

按审计输出删除 Task 4 漏删的 helper 或迁移漏切的生产读路径；只允许 D7 列出的文件保留原始 SQL。若 `db.rs` 的命中都落在 D7 的本地例外（`insert_bucket*`/`upsert_bucket_*`、账户配额片段、用户删除级联），保留并确认 `shortcut:` 注释存在。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p oss-server --lib cascade_matches_legacy_reference && cargo test -p oss-server --test control_store_bucket_diff --test raw_sql_audit`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/tests/control_store_bucket_diff.rs \
  crates/oss-server/tests/raw_sql_audit.rs crates/oss-server/tests/support/control_store_rows.rs crates/oss-server/src
git commit -m "test(oss-server): differential and raw-SQL audit for bucket control-store slice"
```

---

### Task 9: 全量门禁

**Files:** 无新增；仅确认。

- [ ] **Step 1: 格式与 lint**

```bash
cargo fmt --check
cargo clippy -p xl-store -p oss-server --all-targets -- -D warnings
```

Expected: 无输出/无 warning。

- [ ] **Step 2: 单元与集成测试**

```bash
cargo test -p xl-store
cargo test -p oss-server
```

Expected: 全绿。若遇既有 5s 探针偶发超时（如 network/probe 类），单独重跑该测试确认通过并记录；不得删除/放宽断言。

- [ ] **Step 3: 复核触发器与模式**

```bash
cargo test -p oss-server assert_mode_guards
```

Expected: M7 模式 0 个 raft guard；Raft 模式 33 个 guard；site 触发器未被删除。

- [ ] **Step 4: 提交（若门禁产生格式化改动）**

```bash
git add -u
git commit -m "chore: bucket control-store slice gate fixes"
```

若 Step 1-3 无改动，跳过本步并在计划执行记录中注明“无改动”。

---

## 自检清单（执行者完成前逐项确认）

- [ ] 四张表的生产 SQL 只出现在 D7 白名单文件与适配器。
- [ ] `buckets` 的 INSERT/UPDATE 列清单与旧 `create_bucket_tx` 完全一致（INSERT 含 `set_id` 常量 0；UPDATE 不写 `set_id`/`creation_date`）。
- [ ] `bucket_policies` 写入仍是 `ON CONFLICT DO UPDATE`，没有任何 `INSERT OR REPLACE`。
- [ ] `delete_bucket_cascade` 仍清 13 张从属表且 `buckets` 最后删除，`permissions` 仍 raw，`clear_bucket_m8_tx` 范围未变。
- [ ] 运行期/严格/安装三套属主口径各有测试，错误文本逐字一致。
- [ ] `ControlData` 字段顺序、四表 ORDER BY、`BucketEntry` 字段顺序有字节级测试。
- [ ] 安装保留目标 `set_id`，目标独有桶先删、`owner_id` 置 NULL 的位置未变。
- [ ] `site.rs`、`xl-s3/src/access.rs`、`db.rs` 本地例外、`cluster_control.rs` 均有 `shortcut:` 注释。
- [ ] 无触发器被删除/重命名；无新增依赖；所有改动文件 < 500 行。
