# IAM / 账户 / 配额 ControlStore 切片实施计划（Slice 5）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `users`、`access_keys`、`permissions`、`accounts`、`account_members`、`account_usage_ledger`（以及只读的本地缓存 `account_usage` 统计口径）的控制面读写收敛到 `ControlStore`/`ControlTxn` 缝，保留 30 个 site 触发器、33 个 raft guard、快照 `ControlData` 字段顺序/字节、错误文本、账户配额与级联行为完全不变。

**Architecture:** 行类型与 trait 放 `xl-store`（自然键：username / access_key / account_id / (account_id, username)）；SQLite 适配器放 `oss-server/src/control_store/`，只读适配器持有共享 `SqlitePool`，写适配器借用调用方事务（`SqliteControlTxn::new(&mut *conn)`），绝不 begin/commit/rollback。Raft apply、快照导出/安装、cutover/recover、site 的 users/AK/permissions 同步、console/accounts API、启动回填与规范化改为调用缝。`xl-s3` 的 S3 热路径（SigV4 AK 查询/重加密、ACL 权限求值、STS、本地用量缓存）只持有 `SqlitePool` 且看不到 `oss-server` 适配器，保留 raw SQL 并加 `shortcut:` 注释（D7）。

**Tech Stack:** Rust 2024、sqlx/SQLite、async-trait、serde/rmp-serde、tokio 测试；不新增依赖。

**Spec:** 本计划自包含；上位约束见 `docs/superpowers/plans/2026-11-03-control-store-vpc-slice.md`、`2026-11-04-control-store-lifecycle-inventory-tier.md`、`2026-11-04-control-store-buckets-policies-event-replication.md`。

## Global Constraints

- 外部行为不变：HTTP 状态/文本、console JSON 字段与顺序、快照字节与字段顺序、feature level、site changelog 内容与触发时机、raft guard 拒绝文本，均须逐字节/逐语句一致。
- 缝后端中立：trait 身份键只用自然键（username / access_key / account_id / (account_id, username)）；本地数字 uid 只出现在运行期视图与 SQL 边界，不出现在快照行。
- 适配器绝不 begin/commit/rollback；事务边界只归调用方。
- 复用 `Db` 既有连接池，不新建第二个池。
- 不新增依赖；单文件 < 500 行；适配器按读/写/快照拆分；`control_store.rs` 根文件只做 `mod`/`pub use`/supertrait，不新增 SQL。
- 每个任务 TDD：先写失败测试，再实现，再全绿；每个任务独立提交。
- 不删除、不重命名任何触发器。INSERT/UPDATE/DELETE 语句形状逐字复刻，让 users/access_keys/permissions 的 site ins/upd/del 与 raft guard 以原方式触发。**不使用 `INSERT OR REPLACE`。** 已有 UPSERT 语句原样保留（apply 的 `ON CONFLICT`），除非触发器测试证明必须。**执行偏差：** site `permissions`/`access_keys` upsert 已改为 UPDATE-then-INSERT（见 D2 偏差记录）。
- 两套口径分开：运行期读宽松（悬空/缺失 → `None`/`0`），严格快照导出（悬空属主 → 精确错误文本）。
- 不迁移 `xl-s3` 热路径（D7）；不迁移 `buckets`/`uploads` 的非 IAM 列写入（D7 的跨域 FK 置空）。
- 全量门禁：`cargo test -p xl-store`、`cargo test -p oss-server`、clippy/fmt。既有 5s 探针偶发超时只允许隔离重跑，不得弱化断言。
- 提交信息不带 `Co-Authored-By`。

## Review Focus

1. **site 触发器语义漂移**：`users` 的 UPDATE 触发器带列变化 `WHEN`（username/password_hash/status/created_at）与 `is_admin=0` 双端条件；`access_keys`/`permissions` 的 gate 依赖 `site_entity_clock`。适配器必须逐字复刻现有 INSERT/UPDATE/DELETE 列清单与 upsert 形状；`apply_user` 的逐列 UPDATE（先 password_hash 再 status）必须保留为两条语句。测试：Task 7 site changelog/clock 差分。
2. **快照字节冻结**：`UserRow`/`AccessKeyRow`/`PermissionRow`/`AccountRow`/`AccountMemberRow`/`AccountUsageLedgerRow` 字段名与声明顺序必须与 `raft/data.rs` 旧结构完全一致（V1/V2/V3 已冻结）；账户两表/账本的 `skip_serializing_if` 只在 `ControlData` 层保留。测试：Task 1/2 rmp+JSON 字节测试、Task 5 `ControlData` 字节 round-trip。
3. **安装语义与 FK 顺序**：删 permissions → account_members → accounts → 目标独有桶 → 非管理员 AK → users；插 users → accounts → ledger → AK → permissions；最后清孤儿 session AK。`users` 插入按 username 重建 id，AK/permission/账户成员在适配器内按 username 解析；错误文本 `access key owner {name} not in snapshot`、`permission owner {name} not in snapshot`、`account {id} admin_uid {uid} is missing or a global admin (not exportable)`、`account_members {id}/{name} points at a global admin (not exportable)` 逐字保留。测试：Task 5。
4. **账户配额判定**：`bucket_quota_reached` 的成员行优先/uid 派生回落、NULL/负配额不限、停用账户 fail-closed、`bucket_create_quota_reached` 只在新建行时判定，必须与旧 SQL 同口径。测试：Task 4 + 既有 `accounts`/`raft_quota_ledger` 套件。
5. **零写与回滚**：所有写 façade 走单事务；校验（最后管理员守卫、角色/状态合法性、账户存在性）失败时零写。differential 测试对每个写操作比较两库最终行 + site changelog/clock，并断言 rollback 丢弃写入。

## 裁决记录（本切片的设计决定）

### D1. 自然键与行类型

- `UserEntry`（快照）字段顺序：`username, password_hash, status, created_at`；只含非管理员行（与旧 `UserRow` 一致）。
- `AccessKeyEntry`（快照）：`access_key, secret_key, username, status, expires_at, created_at, parent_ak, session_policy, kind`。属主用 username；快照只含 `is_admin=0 AND kind='long-term' AND parent_ak IS NULL`。
- `PermissionEntry`（快照）：`username, bucket, can_read, can_write, can_delete`。
- `AccountEntry`（快照）：`id, name, status, admin, bucket_quota, soft_byte_quota, created_at, updated_at`；`admin` 是用户名（`Option<String>`）。
- `AccountMemberEntry`（快照）：`account_id, username, role, created_at`。
- `AccountUsageLedgerEntry`（快照）：`account_id, used_bytes, object_count`。
- 运行期视图保留本地 uid（console JSON 边界）：`UserView { uid, username, is_admin, status, created_at }`、`PermissionRow { bucket, can_read, can_write, can_delete }`、`AccessKeyView { access_key, user_id, user_is_admin, kind, status, expires_at, parent_ak, session_policy, created_at }`、`AccountView { id, name, status, admin_uid, admin_username, bucket_quota, soft_byte_quota, hard_byte_quota, object_quota, created_at, updated_at, member_count, bucket_count }`、`AccountMemberView { uid, username, role, is_admin, status, created_at }`。这些不是 seam 身份键。
- `AccountRow`/`AccountMemberRow`/`UserRow` 的 `db.rs` 运行期类型与 JSON 输出不变，由 `AccountView`/`UserView` 投影构造。

### D2. 触发器与语句形状

| 表 | site（`0002_m7_site.sql`） | raft guard（`0004_m9.sql`） |
|---|---|---|
| `users` | `site_gate/log_user_{ins,upd,del}` | `raft_guard_users_{ins,upd,del}` |
| `access_keys` | `site_gate/log_ak_{ins,upd,del}` | `raft_guard_access_keys_{ins,upd,del}` |
| `permissions` | `site_gate/log_perm_{ins,upd,del}` | `raft_guard_permissions_{ins,upd,del}` |
| `accounts` / `account_members` / `account_usage_ledger` | 无 | 无 |

必须逐字复刻的语句：

- 建用户（console/Raft 同形）：`INSERT INTO users (username, password_hash, is_admin, status, created_at) VALUES (?, ?, ?, ?, ?)`；旧 `Db::create_user` 用 4 列 + 默认 status：`INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)`。两个形状分别保留（`user_create` 显式 status / `user_create_default_status`）。
- 用户删除守卫：`UPDATE users SET id = id WHERE id = ? AND (...)`；级联 `DELETE FROM access_keys WHERE user_id = ?`、`DELETE FROM permissions WHERE user_id = ?`、`DELETE FROM users WHERE id = ?`。`UPDATE uploads/buckets SET owner_id = NULL` 留在调用方（D7）。
- 改密：`UPDATE users SET password_hash = ? WHERE id = ?`；启停用沿用两条原子条件 UPDATE 原文。
- 权限：全量替换 `DELETE FROM permissions WHERE user_id = ?` + 逐行 `INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (?, ?, ?, ?, ?)`；site 删除 `DELETE ... WHERE user_id = ? AND bucket = ?`；site upsert 改为 UPDATE-then-INSERT（见偏差记录）；桶级联 `DELETE FROM permissions WHERE bucket = ?`。
- AK：建 `INSERT INTO access_keys (access_key, secret_key, user_id, status, kind, parent_ak, session_policy, created_at) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)`（apply 原语）；`Db::create_access_key` 旧形状 `(access_key, secret_key, user_id, created_at)`；安装形状 `(access_key, secret_key, user_id, status, expires_at, created_at, parent_ak, session_policy, kind)`。删除/状态：`DELETE ... WHERE access_key = ?`、`UPDATE access_keys SET status = ? WHERE access_key = ?`。
- site AK upsert 改为 UPDATE-then-INSERT：冲突时只更新 `secret_key/user_id/status/expires_at`（不改 `created_at/kind`），插入时带 `kind='long-term'` 常量（见偏差记录）。
- 账户：`INSERT OR IGNORE INTO accounts (...)`、`UPDATE accounts SET name/status/bucket_quota/soft_byte_quota/hard_byte_quota/object_quota ..., updated_at = ? WHERE id = ?`，停用带 `NOT EXISTS (在册全局管理员)` 子查询原文；删除 `DELETE FROM accounts WHERE id = ?`；成员 `INSERT ... ON CONFLICT(uid) DO UPDATE`、`INSERT OR IGNORE ...`、`UPDATE accounts SET admin_uid = ? WHERE id = ? AND admin_uid IS NULL`。
- 账本：`INSERT INTO account_usage_ledger (...) VALUES (...) ON CONFLICT(account_id) DO UPDATE ...`。

**执行偏差（UPSERT → UPDATE-then-INSERT）：** `permissions`/`access_keys` 的 site upsert 在 SQLite 3.46 下与 site AFTER UPDATE 触发器组合会误报 `UNIQUE constraint failed: site_entity_clock.entity, site_entity_clock.entity_id`（行已存在且时钟行存在时，`INSERT OR IGNORE` 的 PK 冲突被 UPSERT 上下文当作 UNIQUE 抛出；与 `bucket_policies` 已记录的偏差同源）。两个 site upsert 改为 `UPDATE ... WHERE`，`rows_affected == 0` 时 `INSERT`；行在→upd 触发器、行缺→ins 触发器，changelog 仍是单条 upsert。`tests/control_store_iam_diff.rs::site_triggers_accept_double_upsert_for_permissions_and_access_keys` 锁定二次 upsert 不报错、changelog=2、clock=1、冲突列按旧 UPSERT 语义更新（AK 的 `created_at/kind` 不变）。apply 路径的 `ON CONFLICT` 在 `apply_ctx=1` 下不触发 site 触发器，保持原样。

### D3. 运行期 / 严格快照两套口径

- 运行期：`user(username)`/`user_by_id` 缺失 → `None`；`login_user` 保留「active 用户 + 无停用账户」左连接语义（成员行缺失 = 派生账户有效；账户行缺失 fail-closed）；`account_id_of_user` 成员行优先、无行回落 `xl_s3::access::account_id_of(uid)`（回落值在调用方计算，适配器只返回成员行）。
- 严格快照：账户 `admin_uid` 悬空或指向全局管理员 → `ControlError::Invalid("account {id} admin_uid {uid} is missing or a global admin (not exportable)")`；成员指向全局管理员 → `ControlError::Invalid("account_members {id}/{username} points at a global admin (not exportable)")`。`data.rs` 的 `control_data_err` 把 `Invalid` 原文映射为 `DataError::Format`（无 `control data:` 前缀）。
- 安装：非管理员 username 解析失败 → `Invalid("account admin {name:?} is not a replicated non-admin user")` / `Invalid("account member {name:?} is not a replicated non-admin user")`，与旧 `DataError::Format` 文本一致。

### D4. 快照导出/安装

- `raft/data.rs` 六个行类型改为 `xl_store::control_store::iam/account` 的类型别名；`ControlData` 字段与顺序**不动**，`accounts`/`account_members`/`account_usage_ledger`/`vpc_endpoints` 的 `#[serde(default, skip_serializing_if = "Vec::is_empty")]` 保留在原位。
- 导出 ORDER BY 不变：users.username；access_keys.access_key；permissions.username,bucket；accounts.id；account_members.account_id,username；ledger.account_id。
- 安装顺序不变（见 Review Focus 3）；账户两表仍只在 canonical marker 之后导出，安装仍先校验「非 canonical 快照携带账户行」。
- `replace_accounts(rows, members, canonical)` 适配器内先 `DELETE FROM account_members` 再 `DELETE FROM accounts`，插入顺序 accounts→members；`canonical` 时写 `schema_meta.accounts_canonical_v1`。

### D5. site 同步（users / access_keys / permissions）

`site.rs::apply_user` / `apply_access_key` / `apply_permission` 的 IAM 部分改走 `SqliteControlTxn`：
- `user_of` → `txn.user_by_username`（uid+is_admin）；删除分支的 `UPDATE uploads/buckets SET owner_id = NULL` 留在 site.rs（D7 跨域）。
- `apply_user` 的 upsert 保留「不存在 → INSERT 5 列；存在且非管理员 → 分别 UPDATE password_hash / status」三条语句形状。
- `apply_access_key` 的属主检查用 `txn.access_key_owner`；upsert/delete 保留原语句。
- `apply_permission` 的删除/upsert 保留原语句。
site.rs 的 chunk 级 `apply_ctx`/clock 写入不变。

### D6. 级联与清理

- `Db::delete_user` / `Db::delete_user_cascade`：守卫与 IAM 三表删除走 txn；`UPDATE uploads/buckets SET owner_id = NULL` 留在 db.rs（跨域 FK 置空，D7）。
- `Db::delete_bucket_cascade` 的 `DELETE FROM permissions WHERE bucket = ?` 改 `IamTxn::permissions_delete_for_bucket`（替换旧 `shortcut:`）。
- 账户删除 `delete_account`：成员计数 + 桶计数 + DELETE 在同一调用方事务内，用 `AccountTxn` 读方法。
- 本地 `account_usage` 缓存由 `accounts` 的 `ON DELETE CASCADE` 自动清理（FK 已 ON），不新增 SQL。
- 安装后的孤儿 session AK 清理用 `IamTxn::access_keys_delete_orphan_sessions`。

### D7. seam 之外的本地例外（白名单）

以下路径继续持有 raw SQL，源码留 `shortcut:` 注释，并进入 Task 7 审计白名单：

- `xl-s3/src/auth.rs`：SigV4 AK→SK 查询、`enc:v1:` 重封装、STS 父链校验。S3 每请求热路径；`Acl`/`SigV4Auth` 只持有 `SqlitePool`，看不到 oss-server 适配器。迁移归最终 SQLite 移除。
- `xl-s3/src/access.rs`：ACL 权限求值、管理员 uid 查询、权限桶列举、`forget_bucket` 的 `DELETE FROM permissions WHERE bucket = ?`。
- `xl-s3/src/sts.rs`：会话 AK 签发（users 查询 + access_keys 插入）。
- `xl-s3/src/account_quota.rs`：本地 `account_usage` 缓存采样（节点本地、不进快照）。
- `oss-server/src/cluster_control.rs`：mesh `ControlQuery::AccessKey` 鉴权查询（跨节点认证热路径，与 xl-s3 同口径）。
- `oss-server/src/console.rs`：配对用「首个管理员长时 AK」查询（认证凭据路径）。
- `oss-server/src/db.rs`：`delete_user`/`delete_user_cascade`/site 删除中的 `UPDATE uploads/buckets SET owner_id = NULL`（users FK 置空，跨域，随桶/上传切片迁移）。
- `oss-server/src/state.rs`：启动期空库/模式守卫中的计数读（本地运维守卫）。
- 其余生产文件不得再出现这六张表的 SQL（测试除外）。

## 文件结构

| 文件 | 责任 |
|---|---|
| Create `crates/xl-store/src/control_store/iam.rs` | `UserEntry/AccessKeyEntry/PermissionEntry/UserView/AccessKeyView/PermissionRow`、`IamStore`、`IamTxn`、`IamSnapshotTxn`、freeze 测试 |
| Create `crates/xl-store/src/control_store/account.rs` | `AccountEntry/AccountMemberEntry/AccountUsageLedgerEntry/AccountView/AccountMemberView`、`AccountStore`、`AccountTxn`、`AccountSnapshotTxn`、freeze 测试 |
| Modify `crates/xl-store/src/control_store.rs` | `mod iam; mod account;`、`pub use`、supertrait 追加 |
| Create `crates/oss-server/src/control_store/iam.rs` | 用户/AK/权限只读适配器 |
| Create `crates/oss-server/src/control_store/iam_write.rs` | 用户/AK/权限点写与级联 |
| Create `crates/oss-server/src/control_store/iam_snapshot.rs` | 六表严格导出/安装 |
| Create `crates/oss-server/src/control_store/account.rs` | 账户/成员/账本/桶计数只读适配器 |
| Create `crates/oss-server/src/control_store/account_write.rs` | 账户/成员/账本写 + 配额判定 |
| Create `crates/oss-server/src/control_store/account_snapshot.rs` | 账户两表 + 账本导出/替换 |
| Modify `crates/oss-server/src/control_store.rs` | 仅 `mod` 声明与 `account_status` 迁出（委托到 account 适配器） |
| Modify `crates/oss-server/src/db.rs` | 读 façade 委托、写 façade 走 txn、启动回填/规范化走 txn |
| Modify `crates/oss-server/src/raft/apply.rs`、`raft/apply/accounts.rs`、`raft/apply/quota.rs` | 用户/AK/账户/账本命令走 txn |
| Modify `crates/oss-server/src/raft/data.rs` | 六个类型别名、`read_tx`/`install_tx` 走 txn |
| Modify `crates/oss-server/src/raft/store.rs`、`raft/cutover.rs`、`raft/cutover/recover.rs` | 管理员/账户计数读走 store |
| Modify `crates/oss-server/src/site.rs` | users/AK/permissions 走 txn |
| Modify `crates/oss-server/src/accounts_api.rs`、`accounts_canonical.rs`、`console.rs` | 回读/成员查询走 store |
| Modify `crates/xl-s3/src/{auth,access,sts,account_quota}.rs`、`oss-server/src/cluster_control.rs`、`console.rs` | 仅 `shortcut:` 注释 |
| Create `crates/oss-server/tests/control_store_iam_diff.rs` | 用户/AK/权限差分 + 触发器/rollback + 源码审计 |
| Create `crates/oss-server/tests/control_store_account_diff.rs` | 账户/成员/账本/配额差分 |
| Create `crates/oss-server/tests/control_store_iam_snapshot.rs` | 快照字节/安装/错误文本 |
| Modify `crates/oss-server/tests/raft_guard.rs`、`tests/control_store_bucket_diff.rs` | 新适配器文件名加入 ALLOWED |

---

### Task 1: xl-store IAM 行类型与 trait

**Files:**
- Create: `crates/xl-store/src/control_store/iam.rs`
- Modify: `crates/xl-store/src/control_store.rs`（`pub mod iam;` + `pub use`；`ControlStore: ... + IamStore`；`ControlTxn: ... + IamTxn`）

**Interfaces:**
- `UserEntry { username, password_hash, status, created_at }`、`AccessKeyEntry {...}`、`PermissionEntry {...}`（D1 顺序）、`#[serde(deny_unknown_fields)]`。
- `UserView { uid, username, is_admin, status, created_at }`、`PermissionRow { bucket, can_read, can_write, can_delete }`、`AccessKeyView { access_key, user_id, user_is_admin, kind, status, expires_at, parent_ak, session_policy, created_at }`、`UserLogin { uid, password_hash }`。
- `IamStore`：`user(username)`, `user_by_id(uid)`, `users()`, `login_user(username)`, `admin_id()`, `admin_counts() -> (i64, i64)`, `permissions_for_user(uid)`, `access_keys_for_user(uid)`, `access_key(ak)`, `access_key_owner_is_admin(ak) -> bool`。全部 `-> Result<_, ControlError>`。
- `IamTxn`：`user_by_username`, `user_uid`, `user_is_admin`, `user_create(&UserEntry) -> i64`, `user_create_default_status(...) -> i64`, `user_delete_guard(uid) -> bool`, `user_delete_rows(uid)`, `user_set_password(uid, hash) -> bool`, `user_set_status(uid, active) -> bool`, `permissions_replace(uid, &[PermissionRow])`, `permission_upsert(uid, bucket, r, w, d)`, `permission_delete(uid, bucket)`, `permissions_delete_for_bucket(bucket)`, `access_key_create(&AccessKeyEntry)`, `access_key_delete(ak) -> bool`, `access_key_set_status(ak, status) -> bool`。
- `IamSnapshotTxn`: `snapshot_users()`, `snapshot_access_keys()`, `snapshot_permissions()`, `users_insert(&[UserEntry])`, `users_delete_non_admin()`, `access_key_insert(&AccessKeyEntry)`, `access_keys_delete_non_admin()`, `access_keys_delete_orphan_sessions()`, `permission_insert(&PermissionEntry)`, `permissions_clear()`, `permissions_delete_admins()`。
- 所有 txn trait `: Send`；store `: Send + Sync`；不 begin/commit/rollback。

- [ ] **Step 1:** 在 `iam.rs` 测试模块写六个 rmp 字段顺序测试（旧结构体逐字段对照）、未知字段拒绝、`control_traits_are_object_safe` 扩展。
- [ ] **Step 2:** `cargo test -p xl-store control_store::iam` → FAIL（模块不存在）。
- [ ] **Step 3:** 实现类型/trait；在 `control_store.rs` 接入。
- [ ] **Step 4:** `cargo test -p xl-store control_store::iam` → PASS。
- [ ] **Step 5:** commit `feat(xl-store): add IAM control-store rows and traits`。

---

### Task 2: xl-store 账户/成员/账本行与 trait

**Files:**
- Create: `crates/xl-store/src/control_store/account.rs`
- Modify: `crates/xl-store/src/control_store.rs`（追加 `AccountStore`/`AccountTxn` supertrait）

**Interfaces:**
- `AccountEntry { id, name, status, admin, bucket_quota, soft_byte_quota, created_at, updated_at }`、`AccountMemberEntry { account_id, username, role, created_at }`、`AccountUsageLedgerEntry { account_id, used_bytes, object_count }`。
- `AccountView { id, name, status, admin_uid, admin_username, bucket_quota, soft_byte_quota, hard_byte_quota, object_quota, created_at, updated_at, member_count, bucket_count }`、`AccountMemberView { uid, username, role, is_admin, status, created_at }`、`AccountLimits { status, bucket_quota, hard_byte_quota, object_quota }`。
- `AccountStore`：`account(id)`, `accounts(member_uid: Option<i64>)`, `account_status(id)`, `account_limits(id)`, `account_member_account(uid)`, `account_active(uid)`, `account_member_role(account_id, uid)`, `account_members(account_id)`, `legacy_users_missing_accounts()`, `account_bucket_names(account_id)`, `account_ledger(account_id)`, `account_counts()`。
- `AccountTxn`：`account_insert_if_absent(&AccountEntry) -> bool`, `account_set_name`, `account_set_status`, `account_set_bucket_quota`, `account_set_soft_quota`, `account_set_hard_limits`, `account_delete(id) -> bool`, `account_member_count(id)`, `account_bucket_count(id)`, `account_member_upsert(account_id, username, role, created_at) -> bool`, `account_member_insert_if_absent(...) -> bool`, `account_set_admin_if_unset(account_id, username)`, `account_members_delete_admins()`, `accounts_delete_admin_derived()`, `bucket_quota_reached(owner: Option<&str>) -> bool`, `account_usage_ledger(account_id)`, `account_usage_ledger_upsert(account_id, used, count)`, `replace_account_usage_ledger(&[Entry])`。
- `AccountSnapshotTxn`: `snapshot_accounts()`, `snapshot_account_members()`, `snapshot_account_usage_ledger()`, `replace_accounts(accounts, members, canonical)`。
- `ControlTxn::account_status` 移除，改由 `AccountTxn` 提供（调用点 `txn.account_status` 不变）。

- [ ] **Step 1:** 三个快照行的 rmp 字段顺序测试 + 未知字段拒绝 + `AccountView` 构造测试（纯内存）。
- [ ] **Step 2:** `cargo test -p xl-store control_store::account` → FAIL。
- [ ] **Step 3:** 实现；`ControlStore`/`ControlTxn` 追加 supertrait。
- [ ] **Step 4:** PASS。
- [ ] **Step 5:** commit `feat(xl-store): add account, member and usage-ledger control rows`。

---

### Task 3: SQLite 只读适配器与读 façade

**Files:**
- Create: `crates/oss-server/src/control_store/iam.rs`、`account.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod` 声明；删除 `account_status` 旧实现）
- Modify: `crates/oss-server/src/db.rs`（`owner_uid`/`owner_ref`/`user_exists`/`verify_user` 读段/`get_user`/`list_users`/`count_*`/`list_access_keys`/`get_permissions`/`account_id_of_user`/`account_active`/`list_accounts`/`get_account`/`list_account_members`/`member_role`/`legacy_users_missing_accounts`/`account_bucket_names`）
- Modify: `crates/oss-server/src/accounts_api.rs`、`accounts_canonical.rs`、`console.rs` 回读点

**Interfaces:**
- `impl IamStore for SqliteControlStore` 在 `iam.rs`；`impl AccountStore for SqliteControlStore` 在 `account.rs`。
- `login_user` SQL 逐字复刻 `verify_user` 的 user+account 左连接；`admin_id` 用 `ORDER BY id LIMIT 1`。
- `AccountStore::account(s)` 用旧 `SELECT a.*, member_count, bucket_count` 口径（桶计数 SQL 片段保留在 `account.rs`，`account_buckets_from` 口径成员行优先）。
- `Db` 公有签名不变；`owner_ref(0) → Admin`、缺失 uid → `RowNotFound` 语义不变。
- `verify_user` 的密码校验/Argon2 路径不动；仅行查询改 seam；重哈希写入走 Task 4 的 `user_set_password`。

- [ ] **Step 1:** 在 `iam.rs`/`account.rs` 测试模块写运行期投影测试（用户的 active/account join；账户 member/bucket 计数；`account_id_of_user` 成员优先与派生回落；legacy fallback）。
- [ ] **Step 2:** `cargo test -p oss-server control_store::iam control_store::account` → FAIL。
- [ ] **Step 3:** 实现适配器 + `Db` 委托 + 调用点切换。
- [ ] **Step 4:** `cargo test -p oss-server control_store::iam && cargo test -p oss-server control_store::account && cargo test -p oss-server --lib` → PASS。
- [ ] **Step 5:** commit `feat(oss-server): read IAM and account control rows through ControlStore`。

---

### Task 4: 写适配器、Raft apply、账户/配额写 façade

**Files:**
- Create: `crates/oss-server/src/control_store/iam_write.rs`、`account_write.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod`）
- Modify: `crates/oss-server/src/db.rs`（`create_user`、`create_access_key`、`set_user_status`、`set_password`、`delete_user`、`set_permissions`、账户全部写方法、回填/规范化、`set_account_hard_limits`、`bucket_quota_reached_conn`、`bucket_create_quota_reached_conn`）
- Modify: `crates/oss-server/src/raft/apply.rs`（`uid`/`username_is_admin`/`access_key_owner_is_admin` helper、User*/AccessKey* 命令）
- Modify: `crates/oss-server/src/raft/apply/accounts.rs`、`raft/apply/quota.rs`
- Modify: `crates/oss-server/src/site.rs`（D5）
- Modify: `crates/oss-server/src/db.rs::delete_bucket_cascade`（permissions 走 txn）
- Modify: `crates/oss-server/src/accounts_api.rs`（建用户回读）

**Interfaces:**
- `impl IamTxn for SqliteControlTxn<'_>`、`impl AccountTxn for SqliteControlTxn<'_>`。
- `apply.rs` 的 helper 改为一次构造 `SqliteControlTxn` 并调用；User 命令保持 `ControlResponse` 映射与错误文本；`AccessKeyCreate` 属主缺失仍 `invariant("access key owner {username} not found")`。
- `Db::create_user` 单事务：`user_create_default_status` + 非管理员派生账户 `account_insert_if_absent` + `account_member_insert_if_absent`（同 uid/时间戳）；uid 由 `user_create*` 返回（INSERT 后 `last_insert_rowid`）。
- `Db::set_account_hard_limits`/`rename_account`/`set_account_status`/`set_account_quota`/`create_account`/`delete_account`/`attach_account_member`/`create_user_in_account`/`backfill_accounts`/`backfill_accounts_v2`/`canonicalize_legacy_accounts_once` 全部改 txn；函数签名与返回语义不变。
- `Db::bucket_quota_reached_conn`/`bucket_create_quota_reached_conn` 改为 `AccountTxn::bucket_quota_reached` + `IamTxn::user_uid` + `BucketTxn::bucket_exists`（调用方已持 `&mut SqliteConnection`）。
- `raft/apply/quota.rs` 的 load/write 改 `AccountTxn`；保留原测试的最小建表与断言。

- [ ] **Step 1:** 写失败测试（iam_write.rs）：`user_delete_guard_rejects_last_admin_zero_write`、`permissions_replace_roundtrip`、`access_key_create_missing_owner_text`、`user_create_default_status_shape`；account_write.rs：`account_insert_if_absent_idempotent`、`account_member_upsert_moves_account`、`set_account_status_disable_guard`、`bucket_quota_reached_semantics`、`ledger_upsert_and_saturating_read`。
- [ ] **Step 2:** `cargo test -p oss-server control_store::iam_write control_store::account_write` → FAIL。
- [ ] **Step 3:** 实现适配器与所有调用点切换；删除被替代的 raw helper（`create_user_raw`、`delete_user_cascade` 保留但内部走 txn 或删除，取决于调用者）。
- [ ] **Step 4:** `cargo test -p oss-server control_store::iam_write && cargo test -p oss-server control_store::account_write && cargo test -p oss-server raft::apply` → PASS。
- [ ] **Step 5:** commit `feat(oss-server): write IAM and account control rows through ControlTxn`。

---

### Task 5: 快照导出/安装与 store.rs/cutover 读

**Files:**
- Create: `crates/oss-server/src/control_store/iam_snapshot.rs`、`account_snapshot.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`mod`）
- Modify: `crates/oss-server/src/raft/data.rs`（类型别名、`read_tx` 六段、`install_tx` 相关步骤、`replace_accounts_tx`）
- Modify: `crates/oss-server/src/raft/store.rs`、`raft/cutover.rs`、`raft/cutover/recover.rs`
- Modify: `crates/oss-server/src/db.rs::ensure_raft_accounts_canonical`

**Interfaces:**
- `impl IamSnapshotTxn`/`AccountSnapshotTxn for SqliteControlTxn<'_>`。
- `raft/data.rs`：`pub type UserRow = xl_store::control_store::iam::UserEntry;` 等六个别名；`control_data_err` 已支持 `Invalid`；`read_tx` 用 `snapshot_*` 方法；`install_tx` 顺序不变，`user_id` 预检保留（缺失文本 `access key owner {name} not in snapshot` / `permission owner {name} not in snapshot`），AK/permission 插入由适配器解析 username。
- `replace_accounts_tx` 变薄：校验（12 位 id、status、role）仍在 `data.rs` 任何写之前完成；调用 `replace_accounts(accounts, members, canonical)`。
- `raft/store.rs` 的账户计数/管理员 id、cutover/recover 的同查询改 `SqliteControlStore`。
- `tests/control_store_iam_snapshot.rs` 加 `ControlData` JSON 字节与 `required_feature_level`（账户→3、账本→9）测试。

- [ ] **Step 1:** 写失败测试：`read_tx`/`install_tx` round-trip（两库，含悬空 admin 导出错误文本）、`install_preserves_admin_and_orders`、`snapshot_bytes_frozen`。
- [ ] **Step 2:** `cargo test -p oss-server control_store_iam_snapshot` → FAIL。
- [ ] **Step 3:** 实现并切换。
- [ ] **Step 4:** `cargo test -p oss-server control_store_iam_snapshot && cargo test -p oss-server raft::data && cargo test -p oss-server --test compat_fixtures` → PASS。
- [ ] **Step 5:** commit `feat(oss-server): export and install IAM/account snapshots through ControlTxn`。

---

### Task 6: 级联清理与 site 路径

**Files:**
- Modify: `crates/oss-server/src/db.rs`（`delete_user`、`delete_bucket_cascade`）
- Modify: `crates/oss-server/src/site.rs`（D5）
- Modify: `crates/xl-s3/src/{auth,access,sts,account_quota}.rs`、`crates/oss-server/src/cluster_control.rs`、`console.rs`（仅 `shortcut:`）

**Interfaces:**
- `delete_bucket_cascade` 的 permissions 删除改 `permissions_delete_for_bucket`；顺序 `access_points → lifecycle/inventory/tier → permissions → bucket configs → buckets` 不变。
- `Db::delete_user` 顺序：`user_delete_guard` → `UPDATE uploads` → `UPDATE buckets` → `user_delete_rows`；`delete_user_cascade` 同。
- site 三函数按 D5；错误文本/changelog 不变。

- [ ] **Step 1:** 写 site 触发器/回滚断言（`tests/control_store_iam_diff.rs`）：用户 ins/upd/del、AK ins/upd/del、权限 ins/upd/del 各产生 1 行 changelog、payload/clock 精确、管理员行不复制、rollback 丢弃。
- [ ] **Step 2:** PASS（characterization baseline）。
- [ ] **Step 3:** 改级联与 site；加 `shortcut:` 注释。
- [ ] **Step 4:** `cargo test -p oss-server --test control_store_iam_diff && cargo test -p oss-server` → PASS。
- [ ] **Step 5:** commit `refactor(oss-server): cascade user deletion and site IAM sync through ControlTxn`。

---

### Task 7: 差分测试与源码审计

**Files:**
- Create: `crates/oss-server/tests/control_store_iam_diff.rs`（差分 + 审计）
- Create: `crates/oss-server/tests/control_store_account_diff.rs`
- Modify: `crates/oss-server/tests/raft_guard.rs`（ALLOWED 加 `iam.rs`、`iam_write.rs`、`iam_snapshot.rs`、`account.rs`、`account_write.rs`、`account_snapshot.rs`）
- Modify: `crates/oss-server/tests/control_store_bucket_diff.rs`（ALLOWED 加账户适配器：账户桶计数/桶名查询属本切片）

**Interfaces:**
- 差分：两个库（`Db::connect_site` 固定站点 id + `insert_site`）；A 库 legacy raw SQL，B 库 seam；比较六表全列（显式列 + ORDER BY）、`site_changelog` 确定性列、`site_entity_clock` 元组。覆盖：建用户/改密/启停用/删用户、建/删/改状态 AK、权限全量替换/site upsert/delete、账户增删改/成员迁移/配额、账本 upsert、`delete_bucket_cascade` 的 permissions 清理、rollback。
- 审计：`strip_test_modules` 复用 bucket diff 的实现；TABLES 六表；ALLOWED = 六个适配器 + D7 文件（`db.rs`、`site.rs`、`cluster_control.rs`、`state.rs`、`console.rs`）；xl-s3 不在扫描范围；断言删除的 raw helper 名称不存在（`create_user_raw`、`set_password_raw`、`set_permissions_raw`、`create_access_key_raw`、`delete_access_key_raw`、`set_access_key_status_raw`）。
- `raft_guard` 源扫描通过（新文件名加入 ALLOWED）。

- [ ] **Step 1:** 写差分/审计；首跑审计 FAIL（列出残余 raw SQL）。
- [ ] **Step 2:** 清理遗漏或补 `shortcut:` 白名单。
- [ ] **Step 3:** `cargo test -p oss-server --test control_store_iam_diff --test control_store_account_diff --test raft_guard` → PASS。
- [ ] **Step 4:** commit `test(oss-server): differential and raw-SQL audit for IAM/account slice`。

---

### Task 8: 切片门禁

- [ ] `cargo test -p xl-store`
- [ ] `cargo test -p oss-server --lib`
- [ ] `cargo test -p oss-server --test control_store_iam_diff --test control_store_account_diff --test control_store_iam_snapshot --test raft_guard --test raft_access --test raft_accounts --test raft_accounts_follower --test raft_accounts_canonical --test raft_quota_ledger --test site_sync --test permissions --test console_api --test accounts`
- [ ] `cargo clippy -p xl-store -p oss-server --all-targets -- -D warnings`
- [ ] `cargo fmt --all --check`
- [ ] 偶发超时隔离重跑，记录；不改断言。
- [ ] commit（若有格式化改动）`chore: IAM/account control-store slice gate fixes`。

## 自检清单（执行者完成前逐项确认）

- [ ] 六张表的生产 SQL 只出现在六个适配器与 D7 白名单文件。
- [ ] users/AK/permissions 的 site 触发器列清单与 raft guard 原样触发；无 `INSERT OR REPLACE`。
- [ ] `UserRow`/`AccessKeyRow`/`PermissionRow`/`AccountRow`/`AccountMemberRow`/`AccountUsageLedgerRow` 字段顺序有字节测试；`ControlData` 字段顺序未动。
- [ ] 安装顺序与错误文本逐字一致；canonical marker 语义不变。
- [ ] `owner_ref(0) → Admin`、账户成员优先/派生回落、停用账户 fail-closed 均有测试。
- [ ] `delete_user`、`delete_bucket_cascade`、账户删除的范围未变。
- [ ] xl-s3 热路径均有 `shortcut:` 注释。
- [ ] 无触发器删除/重命名；无新增依赖；新增文件 < 500 行。
