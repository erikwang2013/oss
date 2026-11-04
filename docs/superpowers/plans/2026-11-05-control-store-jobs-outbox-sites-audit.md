# jobs / outbox / sites / audit ControlStore 切片实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把剩余调度/队列/站点/审计控制面（`jobs`、`job_tasks`、`schedule_lease`、`runstate_meta`、`event_outbox`、`repl_outbox`、`audit_log`、`uploads`、`sync_write_intents`、`sync_delete_intents`、M7 `site_*` 表）迁移到后端中立的 `ControlStore` / `ControlTxn` 缝：行类型与 trait 放 `xl-store`（自然键、无 SQLite 类型），SQLite 原始 SQL 只留 `oss-server/src/control_store/` 适配器。行为、顺序、fencing、触发器、快照字节与错误文本逐项保持；与 LWW 时钟/触发器不可分的站点写入保留 raw 并加 `shortcut:` 注释。

**Architecture:** `xl-store/src/control_store/jobs.rs`、`outbox.rs`、`audit.rs`、`sites.rs` 定义行类型与读写 trait；`oss-server/src/control_store/{jobs,jobs_write,jobs_snapshot,outbox,outbox_write,audit,sites}.rs` 实现 SQLite 适配器。只读适配器持有 `SqlitePool`；写适配器 `SqliteControlTxn<'a> { conn: &'a mut SqliteConnection }` 只借用调用方事务，绝不 begin/commit/rollback。`JobState`、`raft/apply*`、`raft/runstate/store.rs`、`raft/data.rs`、`db.rs`、`events.rs`、`replication.rs`、`sync.rs`、`audit_queue.rs`、`raft/repair.rs`、`raft/cutover.rs` 的对应 SQL 改走缝；`site.rs` 的 LWW/apply_ctx 逻辑保持 raw（D5）。

**Tech Stack:** Rust 2024、tokio、sqlx（SQLite）、async-trait、serde / rmp-serde、thiserror。不新增依赖。

**Spec / 前置阅读:** `AGENTS.md`；`docs/superpowers/plans/2026-11-04-control-store-lifecycle-inventory-tier.md`；`docs/superpowers/plans/2026-11-04-control-store-buckets-policies-event-replication.md`；`docs/superpowers/plans/2026-11-04-control-object-cluster-recertify.md`。Slice 3 决策 8 明确把 `runstate_meta.next_run_id` 与 `purge_terminal_jobs_raw` 的 FK 置空留给本切片，本切片必须收口。

## Global Constraints

- 不改变既有行为：作业状态机与重试退避、租约/fencing token 判定、`runstate_meta.next_run_id` 单调原子推进、outbox 两态租约 claim/ack 顺序、审计异步批量语义、站点 LWW 时钟元组与触发器 payload、快照 `RunState`/`ControlData` 字段顺序与字节、错误文本。
- 所有 SQLite 触发器保持原样；禁止 `INSERT OR REPLACE`；有站点 UPDATE 触发器的表继续 UPDATE-then-INSERT，避免 SQLite 3.46 UPSERT 误报 UNIQUE。
- 适配器不 begin/commit/rollback；`BEGIN IMMEDIATE` 等事务边界留在调用方（`Db::claim_task_fenced`、`Db::job_retry_failed` 等）。
- 文件 <500 行；测试放 `crates/oss-server/tests/` 或就地 `#[cfg(test)]` 小模块；不加 `Co-Authored-By`；不新增依赖；不弱化测试。
- 作用域纪律：不碰 IAM/accounts/users/access_keys/permissions 与已迁移 bucket/lifecycle/inventory/tier/AP 域；`delete_user` 中 `uploads` 清理与 `permissions` 级联保持 raw（IAM 切片所有），只加 `shortcut:`。
- 共享文件（`xl-store/src/control_store.rs`、`oss-server/src/control_store.rs`、`raft/data.rs`、`raft/apply.rs`、`tests/raft_guard.rs`）只做加法/局部修改，便于 lead 与 IAM 切片合并。

## Review Focus

1. `jobs`/`job_tasks` 的每个状态转换（queued/running/done/error/cancelled/paused/preempted；task queued/running/done/failed/cancelled）与 `progress/succeeded/failed`、`dedupe_key` 清理、退避 `10*2^attempt`（cap 300）+ 确定性 jitter 完全不变。
2. `claim_task_fenced` 的候选排序 `priority DESC, next_at, task_no`、`BEGIN IMMEDIATE`、fence term 过滤与单事务提升+领取不变；租约接管条件、半 TTL 续约、term 单调不变。
3. `runstate_meta.next_run_id` 的 `CAST(...) < CAST(...)` 单调 UPSERT 语义原样；`next_job_id` 初始化/推进与 `sqlite_sequence` 行为不变。
4. 快照安装/导出：`jobs` ORDER BY id、`job_tasks` ORDER BY job_id,task_no、`schedule_lease` 单行、`runstate_meta` 顺序、先 DELETE 后显式 id INSERT、SAVEPOINT/回滚语义不变。
5. outbox：claim `ORDER BY id` + `UPDATE ... RETURNING`、`claimed_at` token、renew/finish/requeue/bury 的 guard 条件与 `rows_affected` 不变；`event_outbox` 发送后 DELETE 与 `repl_outbox` dead-letter 状态不变。
6. 审计：批量 INSERT 顺序/事务、flush 失败语义、purge 只删到期行不变。
7. 站点：`site_entity_clock` 的 `(ts, priority, origin_site_id, seq)` 比较、`apply_ctx` 抑制、changelog payload、`INSERT OR IGNORE` 时钟行与 trigger 交互不变。

## D7 例外（seam 之外的白名单）

- `site.rs`：LWW 时钟门控、`apply_ctx` 抑制、changelog/apply_dead、站点快照 join 与 drain 游标；与 SQLite 触发器不可分，保留 raw，逐处 `shortcut: 站点 LWW/apply_ctx 与 M7 触发器不可分，站点域最终迁移（对象后端）时收口。`
- `db.rs::delete_user`/`delete_user_cascade` 的 `uploads` 清理：与 IAM 用户级联同事务，IAM 切片所有；保留 raw + `shortcut:`。
- `crates/xl-s3` 不直接访问控制表；无例外。
- 迁移 SQL、`tests/raft_guard.rs` 源码审计器自身、测试内 legacy 参考 SQL 不在审计范围。

---

## Task A1：xl-store jobs 行类型、trait 与快照冻结测试

**Files:**
- Create `crates/xl-store/src/control_store/jobs.rs`
- Modify `crates/xl-store/src/control_store.rs`（`pub mod jobs` + re-export + 超级 trait）
- Modify `crates/oss-server/src/raft/runstate/model.rs`（改为 `pub use`，`RunState` 字段顺序不动）

**Interfaces:**
```rust
pub struct JobRow { /* 22 字段，顺序与旧 JobRow 完全一致 */ }
pub struct JobTaskRow { /* 15 字段 */ }
pub struct ScheduleLeaseRow { pub term: i64, pub node: i64, pub expires_at: i64, pub acquired_at: i64 }

#[async_trait] pub trait JobStore: Send + Sync {
    async fn job(&self, id: i64) -> Result<Option<JobRow>, ControlError>;
    async fn job_task(&self, job_id: i64, task_no: i64) -> Result<Option<JobTaskRow>, ControlError>;
    async fn job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn failed_job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn queued_task_candidate(&self, now: i64, fence_term: i64, allow_queued_jobs: bool)
        -> Result<Option<JobTaskRow>, ControlError>;
    async fn jobs_to_promote(&self) -> Result<Vec<i64>, ControlError>;
    async fn job_task_active(&self, job_id: i64) -> Result<bool, ControlError>;
    async fn job_inflight_counts(&self, requested_by: Option<i64>) -> Result<(i64, i64), ControlError>;
    async fn recent_jobs(&self, limit: i64) -> Result<Vec<JobRow>, ControlError>;
    async fn job_tasks_page(&self, job_id: i64, limit: i64, offset: i64) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn terminal_jobs_before(&self, cutoff: i64, limit: i64) -> Result<Vec<i64>, ControlError>;
    async fn dedupe_taken(&self, key: &str) -> Result<bool, ControlError>;
    async fn schedule_lease(&self) -> Result<Option<ScheduleLeaseRow>, ControlError>;
    async fn runstate_meta(&self, key: &str) -> Result<Option<String>, ControlError>;
    async fn max_job_id(&self) -> Result<i64, ControlError>;
    // 恢复/审计读：running_tasks、expired_tasks、stale_tasks、orphan_jobs、job_owner、report 等
}

#[async_trait] pub trait JobTxn: Send {
    // 建作业（自动 id + tasks，dedupe 冲突返回 None）、进度/收尾、claim/requeue/preempt/release、
    // recover/take-over、heartbeat、set_error/set_state/retry_failed、purge（含 FK 置空）、
    // apply upsert（job/task/batch）、schedule_lease_upsert、runstate_meta_next_run_id。
}

#[async_trait] pub trait JobSnapshotTxn: Send {
    async fn all_jobs(&mut self) -> Result<Vec<JobRow>, ControlError>;
    async fn all_job_tasks(&mut self) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError>;
    async fn runstate_meta_all(&mut self) -> Result<Vec<(String, String)>, ControlError>;
    async fn replace_jobs(&mut self, rows: &[JobRow]) -> Result<(), ControlError>;
    async fn replace_job_tasks(&mut self, rows: &[JobTaskRow]) -> Result<(), ControlError>;
    async fn replace_schedule_lease(&mut self, row: Option<&ScheduleLeaseRow>) -> Result<(), ControlError>;
    async fn set_runstate_meta(&mut self, key: &str, value: &str) -> Result<(), ControlError>;
    async fn clear_runstate_domain(&mut self) -> Result<(), ControlError>;
}
```
- 行类型 `deny_unknown_fields`，字段顺序冻结；`JobRow`/`JobTaskRow` 继续 `Default`。
- xl-store 单元测试：用私有 legacy 结构镜像旧字段顺序，rmp-serde 编码逐字节相等；缺字段/多字段解码失败；`RunState` 快照字节不受 re-export 影响。
- `cargo test -p xl-store control_store::jobs` 绿后提交：`feat(xl-store): add job and runstate control-store rows and traits`。

## Task A2：SQLite 只读适配器 + 读路径切换

**Files:**
- Create `crates/oss-server/src/control_store/jobs.rs`（只读，<500 行；SQL 与旧 `db.rs`/`job_state` 逐字一致）
- Modify `crates/oss-server/src/control_store.rs`（`mod jobs; impl JobStore for SqliteControlStore`）
- Modify `crates/oss-server/src/job_state.rs`（`read_job`/`read_task`/`next_job_id` 等改走 seam）
- Modify `crates/oss-server/src/job_state/{worker,admin,inventory}.rs`（只读查询改走 seam）
- Modify `crates/oss-server/src/raft/runstate/store.rs`（jobs/job_tasks/schedule_lease/meta 读改走 `JobStore`/`JobSnapshotTxn`）
- Modify `crates/oss-server/src/raft/apply.rs`（`current_job`/`current_task`/`dedupe_taken` 等校验读改走 seam）
- Create `crates/oss-server/tests/control_store_jobs_diff.rs`（差分 + 零写入 + 排序）

**Steps:**
1. 适配器逐条照抄现有 SELECT（列顺序、ORDER BY、绑定类型不变），`JobRow` 映射函数集中一处。
2. `JobState` 增加 `store(db) -> SqliteControlStore` 小助手；本地路径仍调 `db` façade，但 façade 内部使用 seam（Task A4）。
3. 差分测试：内存 SQLite 执行迁移，种子覆盖 queued/running/done/error、多任务、dedupe、fence；同一查询分别走适配器与 legacy 参考 SQL，比较完整行/顺序；对只读调用前后 `total_changes()` 与关键表字节 dump 断言零写入。
4. `cargo test -p oss-server --test control_store_jobs_diff`、`cargo test -p oss-server job_state` 绿后提交：`refactor(oss-server): route job and runstate reads through ControlStore`。

## Task A3：SQLite 写适配器 + JobState/apply/快照安装切换

**Files:**
- Create `crates/oss-server/src/control_store/jobs_write.rs`、`jobs_snapshot.rs`（各 <500 行）
- Modify `crates/oss-server/src/control_store.rs`
- Modify `crates/oss-server/src/job_state/{worker,admin,inventory}.rs`（replicated 原始写走 `JobTxn`；`commit` 的 `JobBatch` 构造不变）
- Modify `crates/oss-server/src/raft/apply.rs`、`raft/apply/lease.rs`、`raft/apply/inventory.rs`
- Modify `crates/oss-server/src/raft/runstate/store.rs`（`install_runstate_tx` 走 `JobSnapshotTxn`）
- Modify `crates/oss-server/src/raft/compat.rs`（`next_run_id` 读走 seam）
- Extend `crates/oss-server/tests/control_store_jobs_diff.rs`、`raft_runstate.rs`/`raft_jobs_*` 回归

**Steps:**
1. 写适配器按旧 SQL 一一实现：`INSERT ... RETURNING id`、`ON CONFLICT(id) DO UPDATE`、terminal 清 dedupe 的 CASE、`rows_affected` 断言、purge 先 `UPDATE inventory_runs/tier_restores SET job_id/parent_job_id = NULL` 再删 tasks/jobs（决策 8 注释收口）。
2. `runstate_meta_next_run_id` 保留 `WHERE CAST(runstate_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)`；`next_job_id` 读-改-写仍在调用方事务内。
3. 快照安装：DELETE 顺序 `job_tasks`→`jobs`→`schedule_lease`→`runstate_meta`，显式 id INSERT，`RUNSTATE_RAW_TABLES` 语义不变；`SqliteControlTxn` 直接用现有 tx conn，不嵌套 SAVEPOINT（SAVEPOINT 仍在 `install_runstate_tx`）。
4. 测试：apply 与旧 SQL 的差分（含 batch 部分失败回滚零写入）、`next_run_id` 只前进不回退、lease 接管、快照往返字节一致、显式 id 安装不改变 `sqlite_sequence` 期望行为。
5. 提交：`refactor(oss-server): write jobs, leases and runstate through ControlTxn`。

## Task A4：db.rs 单节点 job 引擎走 seam + 差分收口

**Files:**
- Modify `crates/oss-server/src/db.rs`（job engine 4851–6025；`BEGIN IMMEDIATE` 调用方保留）
- Modify `crates/oss-server/src/job_state.rs`（local 路径 façade）
- Extend `crates/oss-server/tests/jobs_engine.rs` 或 `control_store_jobs_diff.rs`

**Steps:**
1. 每个 `Db::job_*`/`claim_task_fenced`/`purge_terminal_jobs_*` 保持签名与返回类型，内部改用 `SqliteControlStore`/`SqliteControlTxn`；SQL 移入适配器。
2. `claim_task`（无 fence 版本）保留其事务与调用顺序；`recover_*` 的循环/计数语义逐行镜像。
3. 差分测试：legacy SQL 副本建同构库，执行建作业/领取/重试/收尾/预占/恢复/清理，dump `jobs`+`job_tasks`+`runstate_meta` 全表（含顺序）逐字节比较；断言事务失败零写入。
4. 删除已迁走的 `*_raw` helper；对确需保留的本地例外加 `shortcut:`。
5. 提交：`refactor(oss-server): route single-node job engine through ControlTxn`。

## Task A5：源码审计 + raft guard 白名单 + A 切片门禁

**Files:**
- Extend `crates/oss-server/tests/control_store_jobs_diff.rs`（`source_audit`，复用 `strip_test_modules` 风格）
- Modify `crates/oss-server/tests/raft_guard.rs`（ALLOWED 增加 `control_store/jobs*.rs`）
- Modify `docs/superpowers/plans/2026-11-05-control-store-jobs-outbox-sites-audit.md`（追加 Gate record）

**Steps:**
1. 扫描 `src/**/*.rs`，`jobs|job_tasks|schedule_lease|runstate_meta` SQL 命中只允许适配器 + D7；`db.rs` 命中须带 `shortcut:`。
2. `cargo test -p oss-server --test raft_guard`；A 切片门禁：`cargo test -p xl-store`、`cargo test -p oss-server`（focused 先行）、`cargo clippy -p xl-store -p oss-server --all-targets -- -D warnings`、`cargo fmt --check`。
3. 提交：`test(oss-server): audit jobs ControlStore migration`。

## Task B1–B3：outbox / uploads / sync intents

- B1 `event_outbox`：`xl-store` 行类型 + `EventOutboxStore/Txn`；适配器照抄 `events.rs` 的 enqueue/renew/finish/requeue/bury/reclaim/claim RETURNING；测试并发 claim 唯一、token guard、排序、dead 状态。
- B2 `repl_outbox` + `uploads`：`replication.rs`/`raft/repair.rs`/`db.rs` upload 读写走缝；remote/site 入队、version 聚合、drop-site 删除、repair 计数；`delete_user` 的 uploads 清理保留 raw（IAM）。
- B3 `sync_write_intents`/`sync_delete_intents`：`sync.rs` 插入/状态/tee 路径/恢复查询走缝；`PRAGMA synchronous = FULL` 留在调用方（backend 特定副作用）；测试恢复扫描与删除语义。

## Task C1：audit_log

`xl-store` `AuditEntry` + `AuditTxn::audit_insert_batch`/`AuditStore` 读与 purge；`audit_queue.rs` 批量写入、`db.rs` purge/read 走缝；测试顺序/批量/异步 flush 语义不变。

## Task D1–D2：site 表

- D1：`db.rs`/`console.rs`/`raft/cutover.rs`/`raft/repair.rs` 中可安全迁移的 `sites`、`site_cursors`、`site_config` 读写走 `SiteStore/SiteTxn`（不含触发器写入路径）。
- D2：`site.rs` LWW/apply/changelog/apply_dead 保持 raw，逐处 `shortcut:`；源码审计白名单记录原因；不伪造会改变触发器/时钟语义的迁移。

## Gate record（完成后填写）

### A 切片（jobs / job_tasks / schedule_lease / runstate_meta）

- Commits：`1160c08` `refactor(oss-server): route single-node job engine through ControlTxn`；`test(oss-server): audit jobs ControlStore migration`（本提交）。
- Focused：`cargo test -p oss-server --test control_store_jobs_diff` 3/3；`cargo test -p oss-server --test raft_guard` 3/3；`cargo test -p oss-server --lib` 316/316；`cargo test -p xl-store` 483 passed / 1 ignored（含各 target）。
- Gates：`cargo clippy -p xl-store -p oss-server --all-targets -- -D warnings` 绿；`cargo fmt --check` 绿。
- 已知残余 / D7：`db.rs::claim_task_fenced` 的 `BEGIN IMMEDIATE` 留在调用方（SQLite 写锁串行化，非适配器事务）；`raft/compat.rs::guard_runstate_activation` 的悬空引用检查为启动期只读并列入源码审计白名单；B（outbox/uploads/sync intents）与 D（site 表）尚未实施，保留任务 B1–B3/D1–D2 描述与 D7 例外。
