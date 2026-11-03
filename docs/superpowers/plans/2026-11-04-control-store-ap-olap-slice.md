# ControlStore 第二切片（access_points / olap_configs）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `access_points` 与 `olap_configs`（含 AP/OLAP 缓存重建、快照导出/安装、桶删除级联）迁移到已存在的 `ControlStore` / `ControlTxn` 缝上；SQLite 仍是默认后端，外部行为、HTTP 状态/文本、快照字节布局逐字节不变。

**Architecture:** 读路径经 `Arc<dyn ControlStore>`（共享 `Db` pool），写路径经调用方事务内的 `ControlTxn`（apply / snapshot install / single-node console 各持自己的事务，适配器绝不 begin/commit/rollback）。属主在缝里是自然键 `OwnerRef`，本地数字 id 只在 console JSON 与 SQL 适配器边界出现。AP/OLAP 的 SQL 适配器拆到 `control_store/ap_olap.rs`，`control_store.rs` 保持 <500 行。

**Tech Stack:** Rust 2024、`async-trait`、sqlx/SQLite、tokio、rmp-serde、serde_json、base64（均已有依赖）。

**Spec:** 会话确认的 Phase 0 设计 + 第一切片 `docs/superpowers/plans/2026-11-03-control-store-vpc-slice.md` 的缝约定。生产 SQL 目标：`db.rs` AP/OLAP 读视图与 raw 写助手、`raft/data.rs` 快照 SQL 全部移入适配器。

## Global Constraints

- 现有测试全绿：`cargo test -p xl-store`、`cargo test -p oss-server`。
- **外部行为不变**：AP/OLAP HTTP 状态码、错误文本（`AP 已存在` / `AP 不存在` / `需要桶主或管理员` / `transform access point 需 owner/admin` / `olap module base64: {e}` / `owner user {name} not found` / `owner user id {id} missing` / `{what} owner {name} not in snapshot`）、快照字节布局、feature level 3/8/9 判定不变。
- 缝必须后端中立：自然键（`name` / `access_point` / `bucket` / `username`），不得泄露站点本地 SQL id。
- `ControlTxn` 不得 begin/commit/rollback；事务边界是调用方唯一原子性来源。
- 复用 `Db` 的 pool，不建第二个 pool；不新增依赖。
- 文件 <500 行；`control_store.rs` 超了就拆 `control_store/ap_olap.rs`。
- 快照 export/install 必须经 `ControlTxn`；base64 ↔ BLOB 与 username ↔ 本地 id 的转换点在本计划 Task 6 固定。
- 桶删除级联优先迁到 txn（Task 7），不保留第二条 SQL 路径。
- 双后端差分测试用真实 SQLite；legacy 参考实现只存在于测试文件。
- 每个任务结束提交；任何门禁失败不得进入下一任务。

## Review Focus

- **属主身份漂移**：console 列表可见性、`owner_id` JSON、job Invoke 授权都依赖“创建者 uid”。缝里 `owner` 用 username/Admin 后，任何一处仍拿数字 id 比较都会变成 403/列表错行；测试：Task 3 的 `m8_ap::console_crud_permissions_and_cache` 增加 `owner_id` JSON 断言 + Task 5 `standalone_writes_use_control_txn_and_preserve_owner_id` + Task 8 runtime parity。
- **悬空属主（用户已删、AP 未删）**：legacy console 读仍能列/删（按 stale uid 显示），但快照导出必须 fail-closed。测试：Task 6 `snapshot_admin_owner_and_dangling_owner_are_pinned`；Task 8 dangling fixture。
- **快照字节兼容**：`OwnerRef` 上移、`AccessPointRow` 变别名、OLAP `module` 改字节后，旧快照必须仍可读且新字节不变。测试：Task 1 rmp/JSON 冻结测试、Task 6 round-trip、`compat_fixtures` 现有断言。
- **缓存重建**：快照安装提交后与单机写到 DB 后，AP/OLAP 缓存必须同步；坏策略行不得让整批预加载失败。测试：Task 3 `after_snapshot_install_rebuilds_ap_and_olap_caches`；现有 `m8_ap` 缓存断言。
- **事务原子性 / 守卫触发器**：apply 与 snapshot install 内 AP+OLAP 写入必须同一事务，失败全回滚；适配器擅自 commit 会绕过 `raft_applied`。测试：Task 4 apply 回滚、Task 8 txn rollback、Task 6 install 失败后两表为空。

---

## 决策记录（执行者必须按此实现，不再另选）

**1. `OwnerRef` 上移。** 从 `crates/oss-server/src/raft/data.rs:58-64` 原样移到 `xl_store::control_store`，derive/serde 属性逐字保留：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case", tag = "kind", content = "username")]
pub enum OwnerRef {
    Admin,
    User(String),
}
```

`raft/data.rs` 只留 `pub use xl_store::control_store::OwnerRef;`，所有 `raft::data::OwnerRef` 引用不变。

**2. 两张口径，不得混用。**

- **运行期读**（`ControlStore::access_point(s)`、`olap_config(s)`，供 console/job/cache）：`owner_id == 0` → `Admin`；否则用户行存在 → `User(username)`（**即使该用户是管理员**，保持 console `owner_id` 与筛选语义）；用户行缺失（悬空）→ `Admin`（宽松，等价于 legacy “管理员可见/非管理员不可见”，且避免整批预加载失败）。
- **快照导出**（`ControlTxn::access_points`）：`owner_id == 0` 或 `is_admin = 1` → `Admin`；否则 `User(username)`；用户行缺失 → `ControlError::Invalid("owner user id {id} missing")`（fail-closed，等价旧 `DataError::MissingUser`）。

**3. console 的数字 `owner_id`** 由 `Db::owner_uid(&OwnerRef)` 在 JSON 边界解析：`Admin` → 本节点首个管理员 id（无则 0）；`User(name)` → `users.id`（缺失则 0）。管理员创建的 AP 运行期读为 `User(admin_name)`，解析回原 uid，JSON 不变。悬空属主显示为首个管理员 id 而非已删 uid：仅显示，非授权依据。

**4. 单机写 API 保持数字 uid 签名。** `Db::create_access_point(..., owner_id: i64, ...)` 内部 `owner_ref(uid)` 查 username 并委托 `ControlTxn::access_point_create`；uid 必须存在（`RowNotFound`），测试需先建用户。apply 路径直接用 `OwnerRef`。

**5. 缓存结构去掉 owner_id。** `xl_s3::ap::AccessPoint` 的 `owner_id` 字段运行时从未读取，删除字段与 `from_row` 参数。

**6. 桶删除级联** 迁到 `ControlTxn::delete_access_points_for_bucket`，`clear_bucket_m8_tx` 不再保留 AP/OLAP SQL。

---

## File Structure

- `crates/xl-store/src/control_store.rs`：`OwnerRef`、`AccessPointEntry`、`OlapConfigEntry`、扩展后的两个 trait、冻结测试。
- `crates/oss-server/src/control_store.rs`：模块根；`SqliteControlStore`/`SqliteControlTxn`（VPC 部分）、字段可见性、`mod ap_olap;`、`to_sqlx`、`control_data_err`。
- `crates/oss-server/src/control_store/ap_olap.rs`（新建）：AP/OLAP 的 SQL 读写、属主解析、SQL 行 → 自然键映射。
- `crates/oss-server/src/db.rs`：删除 AP/OLAP 读视图与 raw 写助手；保留/改造单机写门面与 `owner_uid`/`owner_ref`；`clear_bucket_m8_tx` 级联改 txn。
- `crates/oss-server/src/access_points_api.rs`：读走 seam；`who` 带 username；列表筛选/JSON owner_id 解析。
- `crates/oss-server/src/state.rs`、`raft/control.rs`：启动与快照安装后的缓存重建走 seam。
- `crates/oss-server/src/raft/apply.rs`：AP/OLAP apply 走 txn。
- `crates/oss-server/src/raft/data.rs`：快照 export/install 走 txn；`AccessPointRow` 别名；base64/owner 预校验。
- `crates/oss-server/src/job_actions.rs`、`job_actions_api.rs`：Invoke 授权按 username 比较。
- `crates/xl-s3/src/ap.rs`：删 `owner_id`。
- `crates/oss-server/tests/control_store_ap_olap_diff.rs`（新建）：双后端差分。

---

### Task 1: 缝类型与 OwnerRef 上移（xl-store）

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`
- Modify: `crates/oss-server/src/raft/data.rs`（删除本地 `OwnerRef` 定义，改为 re-export）
- Test: `crates/xl-store/src/control_store.rs` `#[cfg(test)]`、`cargo test -p oss-server --lib`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct AccessPointEntry {
      pub name: String,
      pub bucket: String,
      pub owner: OwnerRef,
      pub policy_json: Option<String>,
      pub created_at: i64,
      pub updated_at: i64,
  }

  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub struct OlapConfigEntry {
      pub access_point: String,
      pub kind: String,
      pub endpoint: Option<String>,
      pub module: Option<Vec<u8>>,
      pub module_sha256: Option<String>,
      pub timeout_ms: i64,
      pub max_response_bytes: i64,
      pub updated_at: i64,
  }
  ```
- `AccessPointEntry` 字段名/顺序与当前 `raft::data::AccessPointRow` 完全一致（Task 6 改别名）。
- `ControlStore`/`ControlTxn` 本任务只加空方法签名会破坏 oss-server 编译，因此本任务**只加类型与 re-export**；trait 方法在 Task 2、Task 4 随适配器一起加。
- Consumes: 无。

- [ ] **Step 1: 写冻结测试**

```rust
#[derive(Serialize)]
struct LegacyApRow {
    name: String,
    bucket: String,
    owner: OwnerRef,
    policy_json: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[test]
fn owner_ref_json_matches_snapshot_format() {
    assert_eq!(
        serde_json::to_string(&OwnerRef::Admin).unwrap(),
        r#"{"kind":"admin"}"#
    );
    assert_eq!(
        serde_json::to_string(&OwnerRef::User("alice".into())).unwrap(),
        r#"{"kind":"user","username":"alice"}"#
    );
    assert!(serde_json::from_str::<OwnerRef>(r#"{"kind":"admin","username":"x"}"#).is_err());
}

#[test]
fn access_point_entry_rmp_field_order_matches_snapshot() {
    let row = AccessPointEntry {
        name: "ap1".into(),
        bucket: "b1".into(),
        owner: OwnerRef::User("alice".into()),
        policy_json: Some(r#"{"Statement":[]}"#.into()),
        created_at: 1,
        updated_at: 2,
    };
    let bytes = rmp_serde::to_vec_named(&row).unwrap();
    let legacy = LegacyApRow {
        name: row.name.clone(),
        bucket: row.bucket.clone(),
        owner: row.owner.clone(),
        policy_json: row.policy_json.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    };
    assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap(),
        "字段名与顺序必须与旧快照行逐字节一致");
    assert_eq!(rmp_serde::from_slice::<AccessPointEntry>(&bytes).unwrap(), row);
    let json = serde_json::to_vec(&row).unwrap();
    assert_eq!(serde_json::from_slice::<AccessPointEntry>(&json).unwrap(), row);
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store control_store -v`
Expected: FAIL（类型不存在）

- [ ] **Step 3: 实现类型并上移 OwnerRef**

把 `OwnerRef` 原样移入 `xl-store/src/control_store.rs`（derive/serde 属性逐字不改），新增 `AccessPointEntry`、`OlapConfigEntry`。`raft/data.rs` 删除本地定义，加 `pub use xl_store::control_store::OwnerRef;`。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store control_store -v`
Expected: PASS

Run: `cargo test -p oss-server --lib raft::data -v`
Expected: PASS（OwnerRef re-export 后现有快照测试不变）

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/raft/data.rs
git commit -m "feat(xl-store): add AP/OLAP seam types and move OwnerRef"
```

---

### Task 2: SQLite 读适配器与 `owner_uid`

**Files:**
- Modify: `crates/oss-server/src/control_store.rs`（`pool`/`conn` 改 `pub(crate)`；加 `mod ap_olap;`）
- Create: `crates/oss-server/src/control_store/ap_olap.rs`
- Modify: `crates/oss-server/src/db.rs`（加 `owner_uid`；旧读方法暂留到 Task 3）
- Test: `crates/oss-server/src/db.rs` `#[cfg(test)]`

**Interfaces:**
- Produces:
  ```rust
  // xl-store
  async fn access_point(&self, name: &str) -> Result<Option<AccessPointEntry>, ControlError>;
  async fn access_points(&self) -> Result<Vec<AccessPointEntry>, ControlError>; // name 升序
  async fn olap_config(&self, access_point: &str) -> Result<Option<OlapConfigEntry>, ControlError>;
  async fn olap_configs(&self) -> Result<Vec<OlapConfigEntry>, ControlError>; // access_point 升序

  // oss-server/db.rs
  pub async fn owner_uid(&self, owner: &OwnerRef) -> Result<i64, sqlx::Error>;
  ```
- 读 SQL 固定为（`ControlStore` 实现）：
  ```sql
  SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at,
         u.username AS owner_username
  FROM access_points a LEFT JOIN users u ON u.id = a.owner_id
  ORDER BY a.name
  ```
  ```sql
  SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms,
         max_response_bytes, updated_at
  FROM olap_configs ORDER BY access_point
  ```
- 运行期属主映射：`owner_id == 0 || username.is_none() → Admin`，否则 `User(username)`。
- Consumes: Task 1 类型。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn control_store_reads_ap_olap_with_natural_owner() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let admin = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    db.create_access_point("ap-alice", "b1", alice, None, 1).await.unwrap();
    db.create_access_point("ap-root", "b1", admin, None, 2).await.unwrap();
    // owner_id=0（witness 哨兵）与悬空 id 只由快照安装产生，直接 raw 插入，
    // 避免 Task 5 把单机写 API 改成 uid→username 解析后测试口径失真。
    sqlx::query(
        "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
         VALUES ('ap-witness', 'b1', 0, NULL, 3, 3)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
         VALUES ('ap-dangling', 'b1', 999, NULL, 4, 4)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    db.set_olap_config("ap-alice", "http", Some("https://x"), None, None, 5000, 1024, 5)
        .await.unwrap();

    let store = db.control_store();
    assert_eq!(
        store.access_point("ap-alice").await.unwrap().unwrap().owner,
        OwnerRef::User("alice".into())
    );
    // 管理员拥有也读成 User（保持 console JSON/筛选语义），不是 Admin。
    assert_eq!(
        store.access_point("ap-root").await.unwrap().unwrap().owner,
        OwnerRef::User("root".into())
    );
    assert_eq!(
        store.access_point("ap-witness").await.unwrap().unwrap().owner,
        OwnerRef::Admin
    );
    assert_eq!(
        store.access_point("ap-dangling").await.unwrap().unwrap().owner,
        OwnerRef::Admin
    );
    assert_eq!(
        store.access_points().await.unwrap().iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        ["ap-alice", "ap-dangling", "ap-root", "ap-witness"]
    );

    let cfg = store.olap_config("ap-alice").await.unwrap().unwrap();
    assert_eq!(cfg.kind, "http");
    assert_eq!(cfg.endpoint.as_deref(), Some("https://x"));
    assert_eq!(cfg.module, None);
    assert_eq!(store.olap_configs().await.unwrap(), vec![cfg]);

    assert_eq!(db.owner_uid(&OwnerRef::User("alice".into())).await.unwrap(), alice);
    assert_eq!(db.owner_uid(&OwnerRef::Admin).await.unwrap(), admin);
    assert_eq!(db.owner_uid(&OwnerRef::User("ghost".into())).await.unwrap(), 0);
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server control_store_reads_ap_olap_with_natural_owner -v`
Expected: FAIL（trait 方法/`owner_uid` 不存在）

- [ ] **Step 3: 实现**

`control_store/ap_olap.rs` 内实现 `ControlStore for SqliteControlStore` 的 4 个方法；模块根 `control_store.rs` 加 `mod ap_olap;` 并把字段改 `pub(crate)`。`owner_uid` 用两条 `query_scalar`（Admin 取首个管理员；User 按 username），`fetch_optional` 后 `unwrap_or(0)`。不要改 `Db` 现有读方法。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server control_store_reads_ap_olap_with_natural_owner -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs crates/oss-server/src/control_store/ap_olap.rs crates/oss-server/src/db.rs
git commit -m "feat(oss-server): add AP/OLAP ControlStore read adapter"
```

---

### Task 3: 读路径切换、身份迁移与 `xl_s3::ap::AccessPoint` 去 id

**Files:**
- Modify: `crates/oss-server/src/access_points_api.rs`（`who`、`row_json`、`list`、`owned`、`get_transform`、`preload_olap`、`DbApSource`）
- Modify: `crates/oss-server/src/state.rs:856-877`
- Modify: `crates/oss-server/src/raft/control.rs:45-79, 137-154`
- Modify: `crates/oss-server/src/job_actions.rs:285-325, 555-575`
- Modify: `crates/oss-server/src/job_actions_api.rs:240-257`
- Modify: `crates/oss-server/src/db.rs`（删除 `AccessPointRow`、`OlapConfigRow`、`AP_ROW_SELECT_BASE`、`ap_row`、`olap_row`、`get_access_point`、`list_access_points`、`get_olap_config`、`list_olap_configs`；`access_point_crud` 测试重写）
- Modify: `crates/xl-s3/src/ap.rs`（删 `owner_id` 字段与 `from_row` 参数；~8 处测试字面量）
- Modify: `crates/oss-server/tests/m8_ap.rs`、`tests/m8_ap_trust.rs`、`tests/console_api.rs`、`tests/m8_bucket_cleanup.rs`（改读 API）
- Test: 上述现有测试 + `raft/control.rs` 新缓存测试 + `m8_ap` JSON 断言

**Interfaces:**
- `who(...) -> Result<(i64, bool, String), Response>`（uid, is_admin, username）；6 个调用点用 `_` 忽略不需要的字段。
- `row_json(r: &xl_store::control_store::AccessPointEntry, owner_id: i64) -> serde_json::Value`（同步，`owner_id` 由调用方先解析）。
- `owned(...) -> Result<(AccessPointEntry, i64), Response>`。
- `AccessPoint::from_row(name: String, bucket: String, policy_json: Option<String>, region: &str, account: &str) -> Result<Self, String>`。
- 列表筛选（`access_points_api.rs::list`）：
  ```rust
  rows.iter()
      .filter(|r| admin || matches!(&r.owner, OwnerRef::User(u) if u == &caller_username))
      .map(|r| (r, owner_id)) // owner_id 由 owner_uid 预先解析
  ```
- Invoke 授权（`job_actions_api.rs`，`caller_username` 来自 `get_user(uid)` 的 `.0`）：
  ```rust
  let owner_ok = matches!(&row.owner,
      OwnerRef::User(n) if caller_username.as_deref() == Some(n.as_str()));
  if !is_admin && !owner_ok {
      return Err(BatchError::Denied("transform access point 需 owner/admin".into()));
  }
  ```
- Consumes: Task 2 的 4 个读方法、`Db::owner_uid`。

**身份迁移逐点（行为不得漂移）：**
- `access_points_api.rs` `list`（原 :101）：不再比较 `r.owner_id == uid`；用 `who` 返回的 `username` 与 `OwnerRef::User` 比较。admin 仍短路可见。
- `row_json`（原 :80-90）：`"owner_id"` 先用 `state.db.owner_uid(&r.owner)` 解析再传入；管理员创建的 AP 仍输出其数字 id；悬空属主输出首个管理员 id（仅显示）。
- `owned`（原 :267）：返回 seam 行；`is_owner` ACL 检查（桶主）不变。
- `put_policy` 单机分支（原 :424-433）：`account` 改 `state.db.bucket_account(&row.bucket).await` 后重建缓存。
- `DbApSource::lookup`（原 :655-667）：`store.access_point(name)` + `bucket_account`。
- `job_actions.rs` `author` 返回 `(AuthedUser, String)`；`invoke`（原 :555 `let user = ...`）改 `let (user, username) = ...`，:568 比较 username。其余 4 处 `self.author(...).await?;` 无需改动（元组被丢弃）。
- `job_actions_api.rs` Invoke 分支（原 :249）：`get_user(uid)` 取 username 后比较 `OwnerRef::User`。
- `AccessPoint::from_row` 调用点：`state.rs:861`、`control.rs:52`、`control.rs:140`、`access_points_api.rs:236`、`:425`、`:660` 去掉 owner 实参。

- [ ] **Step 1: 写失败测试**

在 `raft/control.rs` 测试模块新增（复用 `post_commit_effects_idempotent_and_retryable` 的构造方式）：

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_snapshot_install_rebuilds_ap_and_olap_caches() {
    let dir = tempfile::tempdir().unwrap();
    // 普通 connect：after_snapshot_install 只读控制表并重建缓存，不依赖 apply ctx。
    let db = Arc::new(Db::connect("sqlite::memory:").await.unwrap());
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    db.create_access_point("ap1", "b1", alice, None, 1).await.unwrap();
    db.set_olap_config("ap1", "http", Some("https://x"), None, None, 5000, 1024, 2)
        .await.unwrap();
    let store = Arc::new(
        xl_store::store::Store::open(xl_store::store::StoreConfig::new(dir.path().join("data")))
            .await.unwrap(),
    );
    let ap_cache = Arc::new(xl_s3::ap::ApCache::new());
    let olap_cache = Arc::new(xl_s3::olap::OlapCache::new());
    let post = PostCommit::new(db, Arc::clone(&store), Arc::clone(&ap_cache), Arc::clone(&olap_cache), "us-east-1".into());
    post.after_snapshot_install().await;
    let ap = ap_cache.get("ap1").expect("AP cache rebuilt");
    assert_eq!(ap.bucket, "b1");
    assert!(olap_cache.get("ap1").is_ok(), "OLAP cache rebuilt");
}
```

在 `m8_ap.rs::console_crud_permissions_and_cache` 创建 `owner-ap` 后加：

```rust
let list: serde_json::Value = e.http
    .get(format!("{}/api/v1/access-points", e.base))
    .bearer_auth(&owner_tok).send().await.unwrap()
    .json().await.unwrap();
assert_eq!(list[0]["owner_id"], owner_id, "owner_id JSON 不变");
assert_eq!(list[0]["name"], "owner-ap");
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server --lib after_snapshot_install_rebuilds_ap_and_olap_caches -v`
Expected: FAIL（`AccessPointEntry` 读路径未接 / `from_row` 参数未变）

- [ ] **Step 3: 实现读切换**

按 Interfaces 与“身份迁移逐点”修改；删除 db.rs 旧读视图与四个读方法。测试更新：db.rs 内部测试改 `db.control_store()` 读并先 `create_user`；`console_api.rs`/`m8_bucket_cleanup.rs`/`m8_ap.rs`/`m8_ap_trust.rs` 的 `get_access_point`/`get_olap_config` 改 `db.control_store()`；所有 `AccessPoint::from_row` 测试调用去掉 owner 实参（`m8_bucket_cleanup`/`m8_ap_trust`/`m8_ap` 共 5 处）；`m8_ap::set_ap_policy` 的 owner 形参改名为 `_owner`（23 个调用点不变，避免 unused 警告）。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server --lib raft::control -v`
Expected: PASS

Run: `cargo test -p oss-server --test m8_ap -v`
Expected: PASS

Run: `cargo test -p oss-server -v`
Expected: PASS（全量；任何 AP 相关失败必须修因，不改断言）

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/access_points_api.rs crates/oss-server/src/state.rs \
  crates/oss-server/src/raft/control.rs crates/oss-server/src/job_actions.rs \
  crates/oss-server/src/job_actions_api.rs crates/oss-server/src/db.rs \
  crates/xl-s3/src/ap.rs crates/oss-server/tests/m8_ap.rs \
  crates/oss-server/tests/m8_ap_trust.rs crates/oss-server/tests/console_api.rs \
  crates/oss-server/tests/m8_bucket_cleanup.rs
git commit -m "refactor(oss-server): read AP/OLAP through ControlStore and natural owners"
```

---

### Task 4: 写适配器与 Raft apply 切换

**Files:**
- Modify: `crates/xl-store/src/control_store.rs`（扩 `ControlTxn`）
- Modify: `crates/oss-server/src/control_store/ap_olap.rs`
- Modify: `crates/oss-server/src/control_store.rs`（`to_sqlx`）
- Modify: `crates/oss-server/src/raft/apply.rs:631-745`
- Modify: `crates/oss-server/src/db.rs`（删除 `create_access_point_raw`、`set_access_point_policy_raw`、`delete_access_point_cascade`、`olap_upsert_raw`、`olap_delete_raw`）
- Test: `crates/oss-server/src/db.rs` txn 测试、`tests/raft_commands.rs`、`tests/raft_access.rs`

**Interfaces:**
- Produces（`ControlTxn` 新增，全部不 begin/commit）：
  ```rust
  async fn access_point_create(&mut self, row: &AccessPointEntry) -> Result<bool, ControlError>;
  async fn access_point_set_policy(&mut self, name: &str, policy_json: Option<&str>, updated_at: i64) -> Result<bool, ControlError>;
  async fn access_point_delete(&mut self, name: &str) -> Result<bool, ControlError>; // 先删该 AP 的 olap，再删 AP
  async fn access_point_exists(&mut self, name: &str) -> Result<bool, ControlError>;
  async fn access_points(&mut self) -> Result<Vec<AccessPointEntry>, ControlError>; // 严格导出映射
  async fn replace_access_points(&mut self, rows: &[AccessPointEntry]) -> Result<(), ControlError>; // DELETE olap+AP，按序 INSERT
  async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> Result<(), ControlError>;
  async fn olap_config_delete(&mut self, access_point: &str) -> Result<bool, ControlError>;
  async fn olap_configs(&mut self) -> Result<Vec<OlapConfigEntry>, ControlError>;
  async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> Result<(), ControlError>; // 只 DELETE olap_configs
  async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError>;
  ```
- `pub(crate) fn to_sqlx(e: ControlError) -> sqlx::Error`：`Invalid(m) → Protocol(m)`；`Backend(m) → Protocol(format!("control store backend: {m}"))`。
- 属主解析（adapter 内）：
  ```rust
  // Admin → 首个管理员 id，无管理员 → 0（witness 哨兵）；
  // User(name) → users.id；缺失且存在管理员 → Invalid("owner user {name} not found")；
  // 缺失且无管理员 → 0。与 apply::admin_owner_id 逐字等价。
  ```
- 严格导出映射：`owner_id == 0 || is_admin == 1 → Admin`；`username Some → User`；`None → Invalid("owner user id {id} missing")`。
- apply 映射：`bucket_exists_for_ap` → `txn.access_point_exists`；`create_raw` 的 false → `ControlResponse::Conflict`；policy/delete 的 false → `NotFound`；base64 解码留在 apply.rs（错误文本不变）。
- Consumes: Task 1/2 类型与适配器结构。

- [ ] **Step 1: 写失败测试**

在 db.rs 测试模块新增：

```rust
#[tokio::test]
async fn control_txn_ap_olap_writes_and_owner_resolution() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let admin = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        assert!(txn.access_point_create(&AccessPointEntry {
            name: "ap1".into(), bucket: "b1".into(),
            owner: OwnerRef::User("alice".into()), policy_json: None,
            created_at: 1, updated_at: 2,
        }).await.unwrap());
        assert!(!txn.access_point_create(&AccessPointEntry {
            name: "ap1".into(), bucket: "b1".into(),
            owner: OwnerRef::Admin, policy_json: None,
            created_at: 3, updated_at: 4,
        }).await.unwrap(), "同名不覆盖");
        assert!(txn.access_point_exists("ap1").await.unwrap());
        txn.olap_config_upsert(&OlapConfigEntry {
            access_point: "ap1".into(), kind: "http".into(),
            endpoint: Some("https://x".into()), module: Some(vec![1, 2, 3]),
            module_sha256: Some("ab".into()), timeout_ms: 5000,
            max_response_bytes: 1024, updated_at: 5,
        }).await.unwrap();
        let rows = txn.access_points().await.unwrap();
        assert_eq!(rows[0].owner, OwnerRef::User("alice".into()));
        assert!(txn.access_point_delete("ap1").await.unwrap());
        assert!(!txn.access_point_delete("ap1").await.unwrap());
        assert!(txn.olap_configs().await.unwrap().is_empty(), "AP 删除级联 OLAP");
        // Admin 属主映射到本节点管理员 id；witness 由 install 测试覆盖。
        assert!(txn.access_point_create(&AccessPointEntry {
            name: "ap2".into(), bucket: "b1".into(), owner: OwnerRef::Admin,
            policy_json: None, created_at: 6, updated_at: 6,
        }).await.unwrap());
        let admin_id: i64 = sqlx::query_scalar(
            "SELECT owner_id FROM access_points WHERE name = 'ap2'")
            .fetch_one(&mut *tx).await.unwrap();
        assert_eq!(admin_id, admin);
    }
    tx.commit().await.unwrap();
    assert_eq!(alice, db.owner_uid(&OwnerRef::User("alice".into())).await.unwrap());
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server control_txn_ap_olap_writes_and_owner_resolution -v`
Expected: FAIL（trait 方法不存在）

- [ ] **Step 3: 实现 trait + adapter + apply**

先加 trait 方法（xl-store），同一步在 `ap_olap.rs` 实现，避免 oss-server 中间态不编译。apply.rs 的 AP create/policy/delete、OLAP put/delete 改 txn；删除 db.rs 五个 raw 助手。`admin_owner_id` 保留给 inventory 使用。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server control_txn_ap_olap_writes_and_owner_resolution -v`
Expected: PASS

Run: `cargo test -p oss-server --test raft_commands -v`
Expected: PASS（含 `apply` 管理员/用户属主 id 断言）

Run: `cargo test -p oss-server --test raft_access -v`
Expected: PASS（AP/OLAP 单命令 + 缓存）

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/control_store.rs crates/oss-server/src/control_store.rs \
  crates/oss-server/src/control_store/ap_olap.rs crates/oss-server/src/raft/apply.rs \
  crates/oss-server/src/db.rs
git commit -m "refactor(oss-server): write AP/OLAP through ControlTxn in raft apply"
```

---

### Task 5: 单机写门面（console 路径）

**Files:**
- Modify: `crates/oss-server/src/db.rs`（`create_access_point` 返回 `Result<bool>`；`set_access_point_policy`/`delete_access_point`/`set_olap_config` 改 txn 委托；加 `owner_ref`）
- Modify: `crates/oss-server/src/access_points_api.rs:218-259`（`Ok(false)` → 409）
- Test: db.rs 测试、`tests/m8_ap.rs`、`tests/console_api.rs`、`tests/m8_batch.rs`

**Interfaces:**
- ```rust
  pub async fn owner_ref(&self, owner_id: i64) -> Result<OwnerRef, sqlx::Error>;
  // 0 → Admin；用户存在 → User(username)；否则 RowNotFound。
  pub async fn create_access_point(&self, name: &str, bucket: &str, owner_id: i64,
      policy_json: Option<&str>, now: i64) -> Result<bool>;
  // set_access_point_policy / delete_access_point / set_olap_config 签名不变
  ```
- `owner_ref` 查询：`SELECT username FROM users WHERE id = ?`。
- 每个门面 = `pool.begin()` → `SqliteControlTxn::new(&mut tx)` → trait 方法 → `tx.commit()`；错误用 `to_sqlx`。
- Consumes: Task 4 的 txn 方法。

- [ ] **Step 1: 写失败测试**

重写 db.rs `access_point_crud` 并新增：

```rust
#[tokio::test]
async fn standalone_writes_use_control_txn_and_preserve_owner_id() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let _admin = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    assert!(db.create_access_point("ap1", "b1", alice, None, 1).await.unwrap());
    assert!(!db.create_access_point("ap1", "b1", alice, None, 2).await.unwrap());
    let stored: i64 = sqlx::query_scalar("SELECT owner_id FROM access_points WHERE name='ap1'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(stored, alice, "单机创建仍写创建者本地 id");
    assert_eq!(
        db.control_store().access_point("ap1").await.unwrap().unwrap().owner,
        OwnerRef::User("alice".into())
    );
    assert!(db.set_access_point_policy("ap1", Some(r#"{"Statement":[]}"#), 3).await.unwrap());
    assert!(db.delete_access_point("ap1").await.unwrap());
    assert!(!db.delete_access_point("ap1").await.unwrap());
}
```

`access_point_crud` 改写要点：先 `create_user("u7","pw",false)`/`create_user("u8",...)`，断言 `owner == User("u7")`，`list_access_points(Some("b1"))` 断言删除（该 API 已不存在；桶过滤不是生产行为）。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server --lib standalone_writes_use_control_txn_and_preserve_owner_id -v`
Expected: FAIL（返回值/委托未实现）

- [ ] **Step 3: 实现门面与 409 映射**

`create_access_point` 查 username → txn `access_point_create` → commit。console 分支加 `Ok(false) => (409, "AP 已存在")`；保留 unique-violation 分支作为兜底（并发下不变）。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server --lib standalone_writes_use_control_txn_and_preserve_owner_id -v`
Run: `cargo test -p oss-server --lib access_point_crud -v`
Expected: PASS

Run: `cargo test -p oss-server --test m8_ap --test console_api --test m8_batch -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/db.rs crates/oss-server/src/access_points_api.rs
git commit -m "refactor(oss-server): write AP/OLAP through ControlTxn in single-node mode"
```

---

### Task 6: 快照 export/install 接线

**Files:**
- Modify: `crates/oss-server/src/raft/data.rs`（read_tx AP/OLAP 块 463-505；install 步骤 1、11、12；类型别名；预校验；`control_data_err`）
- Test: `crates/oss-server/src/raft/data.rs` 测试模块、`tests/compat_fixtures.rs`

**Interfaces:**
- `pub type AccessPointRow = xl_store::control_store::AccessPointEntry;`（`OlapConfigRow` 保持现状：`module_b64: Option<String>`）。
- 新增 `fn control_data_err(e: ControlError) -> DataError`：`Invalid(m) → DataError::Format(m)`（保 `owner user id {id} missing` 原文）；`Backend(m) → DataError::Format(format!("control store backend: {m}"))`。
- read_tx：
  ```rust
  data.access_points = {
      let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
      txn.access_points().await.map_err(control_data_err)?
  };
  data.olap_configs = {
      let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
      txn.olap_configs().await.map_err(control_data_err)?
          .into_iter().map(|e| OlapConfigRow {
              access_point: e.access_point, kind: e.kind, endpoint: e.endpoint,
              module_b64: e.module.map(|m| base64::engine::general_purpose::STANDARD.encode(m)),
              module_sha256: e.module_sha256, timeout_ms: e.timeout_ms,
              max_response_bytes: e.max_response_bytes, updated_at: e.updated_at,
          }).collect()
  };
  ```
- install 预校验（紧随现有快照行校验，步骤 1 之前）：
  ```rust
  for ap in &data.access_points {
      match &ap.owner {
          OwnerRef::Admin => {
              if ctx.admin_id.is_none() && !ctx.witness { return Err(DataError::NoAdminUser); }
          }
          OwnerRef::User(name) => {
              if !data.users.iter().any(|u| u.username.as_str() == name.as_str()) {
                  return Err(DataError::Format(
                      format!("access point owner {name} not in snapshot")));
              }
          }
      }
  }
  ```
- install 步骤 1（原 :707-715）改 `SqliteControlTxn::new(&mut *conn).replace_access_points(&[]).await.map_err(control_data_err)?;`
- install 步骤 11（原 :910-934）改 `replace_access_points(&data.access_points)`；步骤 12 的 OLAP 循环（原 :1012-1038）改为先在 data.rs 中 base64 decode（保留 `olap module base64: {e}`），再 `replace_olap_configs(&entries)`。
- Consumes: Task 4 的 txn 方法、Task 1 别名。

- [ ] **Step 1: 写失败测试**

在 data.rs 测试模块新增：

```rust
#[tokio::test]
async fn snapshot_admin_owner_and_dangling_owner_are_pinned() {
    // (a) Admin 属主安装 → owner_id = ctx.admin_id
    // (b) User("ghost") 不在 data.users → install 错误含
    //     "access point owner ghost not in snapshot"
    // (c) 本库 AP owner_id=999（无用户行）→ read_tx 错误含 "owner user id 999 missing"
    // (d) sample() 的 alice/OLAP module 字节 round-trip 不变（现有测试保留）
}
```

断言用现有 `install_tx`/`read_tx` 测试助手（`install_and_read_round_trip_via_sql` 的建库方式）；(c) 直接在测试库 raw `INSERT INTO access_points (..., owner_id=999, ...)`。

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p oss-server snapshot_admin_owner_and_dangling_owner_are_pinned -v`
Expected: FAIL

- [ ] **Step 3: 接线**

按 Interfaces 改 read_tx/install；删除 AP/OLAP 的 raw SQL 块。`AccessPointRow` 改别名后，`sample()` 与 `compat_fixtures.rs` 的构造不变。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server --lib raft::data -v`
Expected: PASS

Run: `cargo test -p oss-server --test compat_fixtures -v`
Expected: PASS（旧快照 fixture 可读）

Run: `cargo test -p oss-server --test raft_cutover -v`
Expected: PASS（导出/安装双端一致）

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/raft/data.rs
git commit -m "refactor(oss-server): snapshot AP/OLAP through ControlTxn"
```

---

### Task 7: 桶删除级联迁到 txn

**Files:**
- Modify: `crates/oss-server/src/db.rs:1310-1345`（`clear_bucket_m8_tx`）
- Test: `crates/oss-server/src/db.rs`、`tests/m8_bucket_cleanup.rs`

**Interfaces:**
- `clear_bucket_m8_tx` 开头：
  ```rust
  {
      let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
      txn.delete_access_points_for_bucket(bucket).await.map_err(crate::control_store::to_sqlx)?;
  }
  ```
- 随后循环只保留 inventory/tier/lifecycle 六条 SQL；删除 olap/AP 两条。
- Consumes: Task 4 的 `delete_access_points_for_bucket`。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn clear_bucket_m8_tx_deletes_bucket_ap_olap_only() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let _ = db.create_user("root", "pw", true).await.unwrap();
    let root: i64 = db.owner_uid(&OwnerRef::Admin).await.unwrap();
    db.create_access_point("ap-a", "b1", root, None, 1).await.unwrap();
    db.create_access_point("ap-b", "b2", root, None, 2).await.unwrap();
    db.set_olap_config("ap-a", "http", Some("https://x"), None, None, 5000, 1024, 3).await.unwrap();
    db.set_olap_config("ap-b", "http", Some("https://y"), None, None, 5000, 1024, 3).await.unwrap();
    let mut conn = db.pool().acquire().await.unwrap();
    Db::clear_bucket_m8_tx(&mut conn, "b1").await.unwrap();
    drop(conn);
    let store = db.control_store();
    assert!(store.access_point("ap-a").await.unwrap().is_none());
    assert!(store.olap_config("ap-a").await.unwrap().is_none());
    assert!(store.access_point("ap-b").await.unwrap().is_some());
    assert!(store.olap_config("ap-b").await.unwrap().is_some());
}
```

- [ ] **Step 2: 运行确认测试是绿的（characterization pin）**

Run: `cargo test -p oss-server clear_bucket_m8_tx_deletes_bucket_ap_olap_only -v`
Expected: PASS（现有 SQL 已等价；本测试锁定行为，实现替换后必须仍 PASS）

- [ ] **Step 3: 用 txn 替换两条 SQL**

改 `clear_bucket_m8_tx`；删除 AP/OLAP 两条 SQL。实现后不得改测试断言。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p oss-server clear_bucket_m8_tx_deletes_bucket_ap_olap_only -v`
Expected: PASS

Run: `cargo test -p oss-server --test m8_bucket_cleanup -v`
Expected: PASS（含 site apply 事务路径）

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/src/db.rs
git commit -m "refactor(oss-server): delete bucket AP/OLAP cascade through ControlTxn"
```

---

### Task 8: 双后端差分测试

**Files:**
- Create: `crates/oss-server/tests/control_store_ap_olap_diff.rs`
- Test: 本文件

**Interfaces:**
- 参考模型全部是测试内 raw SQL（legacy `AP_ROW_SELECT_BASE` 等价、raw insert/upsert/cascade），不调用生产读方法。
- 覆盖：
  1. **运行期读**：两个 fresh `Db`，同一用户/AP/OLAP 种子（含 admin 属主、用户属主、owner_id=0、悬空 owner_id=999），比较 `AccessPointEntry`/`OlapConfigEntry` 全字段与排序；断言 `owner_id == 0`/悬空 → `Admin`，管理员用户 → `User(name)`。
  2. **快照导出**：同一库用 raw SQL 复算旧 `AccessPointRow` 映射（is_admin→Admin、缺失→错误），与 `txn.access_points()` 逐行比较；旧 JSON 字节（`serde_json::to_vec`）逐字节一致。
  3. **安装**：legacy raw INSERT 与 `replace_access_points`/`replace_olap_configs` 各装一份，比较 `access_points`/`olap_configs` 全表（含 `module` BLOB）与 `read_tx` 输出；OLAP module 含非 UTF-8 字节。
  4. **级联**：`delete_access_points_for_bucket` vs legacy 两条 DELETE，比较整表。
  5. **回滚**：txn 写入后 `tx.rollback()`，两表为空（适配器不自行 commit）。
  6. **后端错误**：`pool.close()` 后读返回 `ControlError::Backend`，文本前缀 `control store backend: `。
  7. **冻结字节**：`AccessPointEntry` JSON 与 `OlapConfigRow` JSON 的字段顺序字符串断言。

- [ ] **Step 1: 写差分测试**

测试辅助（文件内）：

```rust
async fn fresh() -> Arc<Db> { Arc::new(Db::connect("sqlite::memory:").await.unwrap()) }

async fn legacy_owner(db: &Db, name: &str) -> Option<OwnerRef> {
    let r = sqlx::query(
        "SELECT a.owner_id, u.username AS owner_username, u.is_admin AS owner_is_admin \
         FROM access_points a LEFT JOIN users u ON u.id = a.owner_id WHERE a.name = ?")
        .bind(name).fetch_optional(db.pool()).await.unwrap()?;
    let id: i64 = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let is_admin: Option<i64> = r.get("owner_is_admin");
    Some(match (id, username, is_admin) {
        (0, _, _) => OwnerRef::Admin,
        (_, Some(_), Some(1)) => OwnerRef::Admin,
        (_, Some(u), _) => OwnerRef::User(u),
        _ => return None, // legacy export 在此报 MissingUser
    })
}
```

每个测试的断言按上面 1-7 写；测试名：`runtime_reads_match_legacy_uid_semantics`、`snapshot_export_and_install_match_legacy`、`bucket_cascade_matches_legacy`、`txn_rollback_leaves_tables_empty`、`backend_errors_propagate`、`ap_olap_snapshot_bytes_are_frozen`。

- [ ] **Step 2: 运行并确认失败（若实现未等价）**

Run: `cargo test -p oss-server --test control_store_ap_olap_diff -v`
Expected: 首次可能因测试自身编译错误 FAIL；修测试。实现等价后必须全绿。

- [ ] **Step 3: 修实现，不改断言**

差分失败只允许改生产实现；断言是规格。

- [ ] **Step 4: 运行**

Run: `cargo test -p oss-server --test control_store_ap_olap_diff -v`
Expected: 6/6 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/tests/control_store_ap_olap_diff.rs
git commit -m "test(oss-server): differential coverage for AP/OLAP ControlStore slice"
```

---

### Task 9: 切片门禁

- [ ] **Step 1:** `cargo test -p xl-store` → PASS
- [ ] **Step 2:** `cargo test -p oss-server --no-fail-fast` → PASS（允许既有 timing flake `jobs_engine::worker_timeout_is_failure`，隔离重跑至绿，不弱化测试）
- [ ] **Step 3:** `cargo test -p oss-server --test control_store_ap_olap_diff` → 6/6 PASS
- [ ] **Step 4:** `cargo clippy -p oss-server -p xl-store -p xl-s3 --all-targets` → 无新增 warning
- [ ] **Step 5:** `cargo fmt --all -- --check` → PASS
- [ ] **Step 6:** 记录结果到本文件“门禁记录”；任何失败不得进入下一切片。

---

## 后续切片顺序

1. ~~`vpc_endpoints`~~（已完成，2026-11-03）
2. **本切片**：`access_points` / `olap_configs`（含缓存、快照、桶级联）
3. lifecycle / inventory / tier（full-bucket replace、cursor upsert）
4. buckets / policies / event / replication（13 表级联、30+33 触发器）
5. IAM / accounts / quota（5 表级联、last-admin、SigV4 热路径）——最后做
6. jobs/outbox/sites/audit（队列、lease、LWW changelog）

## 最终切片：SQLite 彻底移除（硬门禁，继承第一切片定义）

- [ ] 删除 `crates/oss-server/src/control_store.rs` 与 `control_store/ap_olap.rs` 的 SQLite 适配器、`SqliteControlTxn`
- [ ] 删除 `migrations/`、`db.rs`/`raft/*.rs` 的全部 SQL、`sqlx` 依赖与 sqlite feature；`cargo tree -i sqlx` 为空
- [ ] 删除 `oss.toml` 的 `database_url`、`.db`/`-wal`/`-shm`、PRAGMA/WAL、SQLite 触发器
- [ ] 删除 legacy-vs-store 差分测试；`rg -n "sqlx|SqlitePool|sqlite" crates Cargo.toml` 生产路径零命中
- [ ] E2E：空目录启动 → 建用户/桶/AP/变换器 → 重启 → 数据仍在；无 `oss.db`/`raft.db`
- [ ] `cargo test --workspace` + `cargo clippy --all-targets` + `cargo fmt --check`

**通过标准：仓库里没有 SQLite 依赖、没有 SQL、没有迁移目录、没有 .db 文件；控制面只使用对象后端。**

## 门禁记录

2026-11-04，切片提交 `3484a89..78ed2b0`（Task 1–9 + 门禁修复），门禁在 `/home/wwwroot/project/oss` 执行：

- `cargo test -p xl-store`：448 passed / 0 failed / 1 ignored（6 个测试目标）。
- `cargo test -p oss-server --no-fail-fast`：834 passed / 2 failed / 2 ignored（77 个测试目标）；两次失败均为 5s 控制面状态探测超时（`站点控制面请求超时（5s）`）：`m8_read_consistency::follower_consistent_read_catches_up_and_carries_applied_index` 与 `raft_lifecycle_cursor_replication::new_leader_continues_from_replicated_cursor`，机器负载 50+ 下的 timing flake（首轮全量另有 `sync_gate::concurrent_delete_bucket_second_is_409` 同类负载时序抖动，复跑通过），隔离重跑分别通过（5.06s / 12.65s）。有效 836 passed / 0 failed / 2 ignored。
- `cargo clippy -p oss-server -p xl-store -p xl-s3 --all-targets`：exit 0，0 warnings。
- `cargo fmt --all -- --check`：exit 0。
- `cargo test -p oss-server --test control_store_ap_olap_diff`：6/6 passed。

结论：切片门禁 PASS；可进入下一切片（lifecycle / inventory / tier）。
