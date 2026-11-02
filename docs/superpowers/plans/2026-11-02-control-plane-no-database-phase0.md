# 控制面去数据库化 — Phase 0（xl-store 基础原语）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 xl-store 中补齐三个基础原语——S3 不可见的保留命名空间、带单调 fencing token 的条件写（CAS）、持久化的锁 token——为后续控制面对象化提供可验证的存储基座；本阶段不改任何控制面业务。

**Architecture:** 保留命名空间复用每盘已有的 `.oss.sys` 目录（S3 桶名校验永远拒绝点开头，因此用户无法寻址）。控制对象以小对象内联进 `xl.meta`，提交仍走现有 quorum 两阶段协议；新增可选的 `fence_token` 字段，提交侧拒绝旧 token，补偿回滚也按 token 收口。CAS 由“读当前版本 + 锁内提交 + expected_version 校验”实现，锁 token 由持久化的单调计数器发放。

**Tech Stack:** Rust 2024、tokio、serde/rmp-serde、tonic/prost、现有 xl-store 两阶段 meta 提交协议。

**Spec:** 会话中已确认的 B 方案设计；关键约束已复制到 Global Constraints 与 Architecture。

## Global Constraints

- 现有测试必须全绿：`cargo test -p xl-store`、`cargo test -p oss-server`。
- 不改变现有数据面对象（用户桶）的读写语义；新原语只对 `.oss.sys` 命名空间生效。
- 不新增外部依赖、数据库或后台服务。
- `xl.meta` 只加 `Option` + `#[serde(default)]` 字段；老解码器必须继续可读。
- 保留命名空间任何 S3 路由不可达；`validate_bucket` 保持拒绝点开头。
- 所有提交路径在 `fsync` 后才算提交；补偿/回滚必须带同一 fencing token，禁止覆盖更新的 token。
- 每个任务结束必须提交；提交信息用 `feat(xl-store): ...` / `test(xl-store): ...`。

## Review Focus

- **旧 token 写者在租约丢失后提交**：必须被提交侧拒绝，且补偿回滚不得覆盖新 token 的已提交版本。
- **两阶段协议崩在 prepare/commit 之间**：重启后不能出现“返回失败但读得到”的写。
- **保留命名空间泄漏到 S3**：ListBuckets、DeleteBucket、key cache、fsck/heal/scrub、reconcile 都不能看到或搬走 `.oss.sys`。
- **多盘/集群/单盘三种提交分支**：fencing 行为必须一致；单盘多盘不得因为“≥1 翻转即提交”绕过 token 检查。
- **stretch 下 `inline_threshold=0`**：控制对象仍必须可内联，否则 12-slot EC 写会引入 placement 循环。

---

### Task 1: 保留命名空间与所有枚举器的豁免

**Files:**
- Modify: `crates/xl-store/src/layout.rs`
- Modify: `crates/xl-store/src/store.rs`（`list_buckets`、`delete_bucket`、`reconcile_bucket_dirs`）
- Modify: `crates/xl-store/src/store/fsck.rs`（`collect_namespace`、bucket 循环）
- Modify: `crates/xl-store/src/store/key_cache.rs`（缓存入口）
- Modify: `crates/xl-store/src/store/list.rs`（如存在系统桶过滤）
- Test: `crates/xl-store/src/store/tests.rs`（新增 `mod control_ns_tests` 或独立文件）

**Interfaces:**
- Produces:
  ```rust
  pub fn is_system_bucket(name: &str) -> bool;
  pub fn object_dir_system(root: &Path, key: &str) -> Result<PathBuf>;
  pub fn object_dir_for(root: &Path, bucket: &str, key: &str) -> Result<PathBuf>;
  ```
  `is_system_bucket` 仅对 `layout::SYS_DIR` 返回 true。`object_dir_system` 跳过桶名校验，仍调用 `validate_key`，路径为 `<root>/.oss.sys/<flattened-key>`。`object_dir_for` 是内部路由：系统桶走 `object_dir_system`，其余走现有 `object_dir`；**不改变 `object_dir` 的 S3 入口校验**。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn system_bucket_is_hidden_from_list_and_delete() {
    let s = test_store().await;
    tokio::fs::create_dir_all(s.cfg.root().join(layout::SYS_DIR)).await.unwrap();
    let buckets = s.list_buckets().await.unwrap();
    assert!(buckets.iter().all(|b| b.name != layout::SYS_DIR));
    assert!(s.delete_bucket(layout::SYS_DIR).await.is_err());
    assert!(layout::object_dir_for(s.cfg.root(), layout::SYS_DIR, "control/manifest").is_ok());
    assert!(layout::object_dir(s.cfg.root(), layout::SYS_DIR, "control/manifest").is_err());
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store system_bucket_is_hidden_from_list_and_delete -v`
Expected: FAIL（`object_dir_for` / `is_system_bucket` 不存在）

- [ ] **Step 3: 实现 `is_system_bucket` / `object_dir_system` / `object_dir_for`，在所有枚举器和内部 meta 路径加豁免**

`list_buckets` 已有 `SYS_DIR` 过滤，补测试；`delete_bucket`、`reconcile_bucket_dirs`、`collect_namespace`、bucket 循环、key cache 入口统一调用 `is_system_bucket`。把所有内部 meta 读写/提交/补偿路径（`read_meta`、`read_meta_cluster`、`write_meta_on`、`write_meta_new_on`、`commit_meta_on`、`mesh_*`、`clear_meta_new`、补偿恢复）里的 `layout::object_dir` 换成 `layout::object_dir_for`；S3 数据面入口保持 `object_dir`。`ensure_bucket` 不得为系统桶建控制面行。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store system_bucket -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/layout.rs crates/xl-store/src/store.rs crates/xl-store/src/store/fsck.rs crates/xl-store/src/store/key_cache.rs crates/xl-store/src/store/list.rs crates/xl-store/src/store/tests.rs
git commit -m "feat(xl-store): reserve system namespace and exempt it from enumerators"
```

---

### Task 2: 控制对象 meta 字段与单机 system-object API

**Files:**
- Modify: `crates/xl-store/src/meta.rs`（`XlMeta` 加 `control_version` / `fence_token` / `control_deleted`）
- Create: `crates/xl-store/src/store/control.rs`
- Modify: `crates/xl-store/src/store.rs`（`mod control;`）
- Test: `crates/xl-store/src/store/tests.rs` 或 `crates/xl-store/src/store/control.rs` 内联测试

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, Clone)]
  pub struct SystemObject {
      pub key: String,
      pub data: Vec<u8>,
      pub version: u64,
      pub fence_token: u64,
      pub deleted: bool,
  }

  impl Store {
      pub async fn put_system_object(
          &self,
          key: &str,
          data: &[u8],
          expected_version: Option<u64>,
          fence_token: u64,
      ) -> Result<u64>;

      pub async fn get_system_object(&self, key: &str) -> Result<Option<SystemObject>>;

      pub async fn delete_system_object(
          &self,
          key: &str,
          expected_version: Option<u64>,
          fence_token: u64,
      ) -> Result<bool>;

      pub async fn list_system_objects(&self, prefix: &str) -> Result<Vec<String>>;
  }
  ```
- `XlMeta` 新字段：
  ```rust
  #[serde(default)] pub control_version: Option<u64>,
  #[serde(default)] pub fence_token: Option<u64>,
  #[serde(default)] pub control_deleted: Option<bool>,
  ```
- 单机实现：`get` 走 `read_meta(SYS_DIR, key)`；`put` 构造单版本 `XlMeta`（`inline: Some(InlineData { bytes })`、`epoch: Some(new_version)`、`part` 兼容位）；`delete` 写 tombstone（`control_deleted = Some(true)`，`inline`/数据为空）。
- 集群分支：本任务先返回 `Error::Unsupported("cluster fenced commit lands in Task 3")`，由 Task 3 替换。

- [ ] **Step 1: 写失败测试（单机）**

```rust
#[tokio::test]
async fn system_object_cas_and_tombstone() {
    let s = test_store().await;
    assert_eq!(s.put_system_object("control/a", b"one", None, 1).await.unwrap(), 1);
    assert_eq!(s.get_system_object("control/a").await.unwrap().unwrap().data, b"one");
    assert!(s.put_system_object("control/a", b"two", Some(0), 2).await.is_err());
    assert_eq!(s.put_system_object("control/a", b"two", Some(1), 2).await.unwrap(), 2);
    assert!(s.delete_system_object("control/a", Some(2), 3).await.unwrap());
    assert!(s.get_system_object("control/a").await.unwrap().is_none());
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store system_object_cas_and_tombstone -v`
Expected: FAIL（接口不存在）

- [ ] **Step 3: 实现 meta 字段与 system-object API（单机分支）**

校验：key 非空、以 `control/` 开头、长度 ≤1024；`data.len() <= meta::MAX_INLINE`；`fence_token` 必须大于当前 meta 的 token（缺失按 0）；`expected_version` 必须等于当前 `control_version`（缺失按 0）。任何不匹配返回 `Error::Conflict`（新增变体或复用 `InvalidArgument`，优先新增 `Error::Conflict(String)` 映射 409）。单机串行化用按 key 的进程内 Mutex（或 `lock_object` 的系统桶变体）；集群 fenced lock 在 Task 5 接入。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store system_object -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/meta.rs crates/xl-store/src/store/control.rs crates/xl-store/src/store.rs crates/xl-store/src/error.rs
git commit -m "feat(xl-store): add control metadata fields and single-node system object API"
```

---

### Task 3: 集群 fenced commit（prepare/commit/补偿）

**Files:**
- Modify: `crates/xl-proto/proto/oss.proto`（`MetaWrite` 加 `optional uint64 fence_token = 4;`；`SlotAck` 加 `optional bool stale = 4;`）
- Modify: `crates/xl-store/src/mesh.rs`（`MetaWrite` / `SlotAck` 字段）
- Modify: `crates/xl-store/src/store/mesh_api.rs`（`write_meta_new_on` / `commit_meta_on` / `mesh_prepare_meta` / `mesh_commit_meta` / `mesh_abort_meta`）
- Modify: `crates/xl-store/src/store.rs`（`write_meta_cluster` 传 token；补偿路径带 token）
- Test: `crates/xl-store/src/store/tests.rs`（集群 harness，参见 `tests_lock.rs` / `tests_replication.rs`）

**Interfaces:**
- Produces:
  ```rust
  pub(in crate::store) enum SlotCommit {
      Committed { hash: [u8; 32] },
      Stale,
      Missing,
  }

  async fn commit_meta_on(
      &self,
      drive: &Path,
      bucket: &str,
      key: &str,
      fence_token: Option<u64>,
  ) -> Result<SlotCommit>;
  ```
- `MetaWrite.fence_token` 为 `None` 时保持旧行为（数据面对象）。`Some(t)` 时：
  - `commit_meta_on` 读取当前 `xl.meta`，解码；若当前 `fence_token >= t`，返回 `Stale`，不 rename。
  - `mesh_commit_meta` 把 `Stale` 映射为 `SlotAck { ok: false, stale: true }`。
  - `write_meta_cluster` 只把 `Committed` 且 hash 匹配的槽计入 `flipped`。
  - 补偿回滚用同 token 的 `restore_meta_fenced`；若当前 token 已更大，跳过恢复并 warn，绝不覆盖新写。
- 单节点多盘分支：`write_meta` 的逐盘 rename 前同样检查 token；`≥1 翻转即提交` 的既有语义保留，但只对 token 合法的翻转计数。

- [ ] **Step 1: 写失败测试（集群/多盘）**

```rust
#[tokio::test]
async fn stale_fence_token_cannot_commit_or_restore() {
    // 两个写者：token 1 先 prepare 后暂停；token 2 完成提交；
    // token 1 恢复后 commit 必须返回 Stale，且 token 2 的内容仍在。
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store stale_fence_token_cannot_commit_or_restore -v`
Expected: FAIL

- [ ] **Step 3: 实现 proto/mesh/commit/补偿的 token 校验**

先改 `oss.proto` 并 `cargo build -p xl-proto` 生成代码；再改 `mesh.rs` / `mesh_api.rs` / `store.rs`。保持 `fence_token=None` 路径逐字节不变。

- [ ] **Step 4: 运行集群与多盘测试**

Run: `cargo test -p xl-store fence_token -v`
Expected: PASS；旧 `tests_replication` / `tests_lock` 全绿。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-proto/proto/oss.proto crates/xl-store/src/mesh.rs crates/xl-store/src/store/mesh_api.rs crates/xl-store/src/store.rs crates/xl-store/src/store/tests.rs
git commit -m "feat(xl-store): enforce monotonic fence tokens in cluster meta commit"
```

---

### Task 4: 持久化单调 fencing token（锁服务）

**Files:**
- Modify: `crates/xl-proto/proto/oss.proto`（`LockReq` 加 `uint64 fence_token = 8;`；`LockResp` 加 `uint64 last_fence_token = 2;`）
- Modify: `crates/xl-store/src/mesh.rs`（`LockReq` / `LockResp` 字段）
- Modify: `crates/xl-store/src/store/lock.rs`（`LockEntry` 加 `fence_token`；`apply` 校验；持久化高水位）
- Modify: `crates/xl-store/src/store/mesh_api.rs`（Lock 服务端 handler 返回 `last_fence_token`）
- Test: `crates/xl-store/src/store/tests_lock.rs`

**Interfaces:**
- Produces:
  ```rust
  pub(in crate::store) struct FencedLockGuard {
      pub token: u64,
      // 复用 ClusterLockGuard 的 lease_lost/续租/释放
  }

  impl Store {
      pub(in crate::store) async fn acquire_fenced_cluster_lock(
          &self,
          bucket: &str,
          key: &str,
      ) -> Result<FencedLockGuard>;
  }
  ```
- 协调者流程：读本地/远端高水位 → `proposed = max(last_token)+1` → 广播 `Acquire { fence_token: proposed }`；节点 `apply` 仅在空闲/过期且 `proposed > 已存 token` 时授予，并在内存 `LockEntry` 记录；协调者收满 `Q_n` 后把 `proposed` 持久化到 `<root>/.oss.sys/locks/<sha256(bucket/key)>.fence`（tmp + fsync + rename）。若授予失败并返回更高 `last_fence_token`，用该值重试。
- 非集群单机：`proposed` 从同一持久化文件读取并 +1。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn fence_token_is_monotonic_across_restart_and_contenders() {
    // 同一 key 连续获取两次，token 严格递增；
    // 重建 Store 后再次获取，仍大于上次；
    // 两个并发获取不会拿到相同 token。
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store fence_token_is_monotonic -v`
Expected: FAIL

- [ ] **Step 3: 实现 token 发放与持久化**

持久化文件只存十进制 u64；写 tmp 后 fsync + rename + 目录 fsync。校验/解析失败 fail-closed（`Error::CorruptMeta`），不得回退到 0。

- [ ] **Step 4: 运行锁测试**

Run: `cargo test -p xl-store tests_lock -v`
Expected: PASS；旧锁测试全绿（原 epoch 逻辑保留）。

- [ ] **Step 5: 提交**

```bash
git add crates/xl-proto/proto/oss.proto crates/xl-store/src/mesh.rs crates/xl-store/src/store/lock.rs crates/xl-store/src/store/mesh_api.rs crates/xl-store/src/store/tests_lock.rs
git commit -m "feat(xl-store): persist monotonic lock fence tokens"
```

---

### Task 5: 集群 system-object 读写与 quorum 读屏障

**Files:**
- Modify: `crates/xl-store/src/store/control.rs`
- Test: `crates/xl-store/src/store/tests.rs`（集群 harness）

**Interfaces:**
- `put_system_object` 在集群分支：先 `acquire_fenced_cluster_lock(SYS_DIR, key)` 拿 token，再 quorum 读当前 meta、校验 `expected_version`、构造新 meta、以 token 调 `write_meta`；提交后释放锁。
- `get_system_object` 在集群分支走 `read_meta_full`（覆盖 ≥ `Q_s`），返回最新 `control_version`；覆盖不足返回 `CorruptMeta`（fail-closed）。
- `delete_system_object` 与 put 同锁/同 CAS，写 tombstone。
- 单机/多盘分支保持 Task 2 语义。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn system_object_read_reflects_committed_write_on_follower() {
    // 3 节点集群：节点 A 写 control/x，节点 B 读必须看到同一版本；
    // 停掉 2 个节点后读必须 fail-closed（QuorumUnmet/CorruptMeta）。
}
```

- [ ] **Step 2: 运行并确认失败**

Run: `cargo test -p xl-store system_object_read_reflects -v`
Expected: FAIL

- [ ] **Step 3: 实现集群分支**

复用现有 `read_meta_cluster(full=true)` 与 `write_meta`；不要另写一套 quorum 逻辑。锁的 key 用 `(SYS_DIR, "control/...")`，与数据面锁表隔离。

- [ ] **Step 4: 运行测试**

Run: `cargo test -p xl-store system_object -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/src/store/control.rs crates/xl-store/src/store/tests.rs
git commit -m "feat(xl-store): cluster quorum read/write for system objects"
```

---

### Task 6: 崩溃/分区故障注入

**Files:**
- Modify: `crates/xl-store/tests/kill9.rs`
- Create: `crates/xl-store/tests/control_faults.rs`
- Test-only; no production files.

**Interfaces:**
- 复用现有 `OSS_TEST_ABORT_META_PREPARE` / `OSS_TEST_ABORT_META_FLIP_K`；新增 `OSS_TEST_ABORT_CONTROL_COMMIT_K`（同款模式）。
- 场景矩阵：
  1. token 1 prepare 后崩溃；token 2 提交；重启 token 1 的节点 → token 2 内容胜。
  2. token 1 翻转 1 个槽后崩溃；重启 → 读覆盖 quorum 仍见 token 2 或旧版本，绝不出现半截。
  3. 锁丢失后旧写者提交 → `Stale`，补偿不覆盖新 token。
  4. 分区 `< Q_s` → 写/读 fail-closed。
  5. stretch 配置下 system object 仍内联且可读写。

- [ ] **Step 1: 写故障测试**
- [ ] **Step 2: 运行并确认失败或暴露缺陷**
- [ ] **Step 3: 修复实现（只改 xl-store 源码，不改测试断言）**
- [ ] **Step 4: 运行全部 xl-store 测试**

Run: `cargo test -p xl-store`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/xl-store/tests/kill9.rs crates/xl-store/tests/control_faults.rs
git commit -m "test(xl-store): crash and partition coverage for fenced control objects"
```

---

### Task 7: Phase 0 集成门禁

**Files:** 无新增；验证所有既有测试。

- [ ] **Step 1: 运行 xl-store 全量测试**

Run: `cargo test -p xl-store`
Expected: PASS

- [ ] **Step 2: 运行 oss-server 控制面相关测试**

Run: `cargo test -p oss-server`
Expected: PASS（现有 409 测试不得回归）

- [ ] **Step 3: 运行格式与静态检查**

Run: `cargo fmt --check && cargo clippy -p xl-store -- -D warnings`
Expected: PASS

- [ ] **Step 4: 记录门禁结果**

把命令与结果追加到本计划末尾的“门禁记录”小节；任何失败不得进入 Phase 1。

- [ ] **Step 5: 提交**

```bash
git add docs/superpowers/plans/2026-11-02-control-plane-no-database-phase0.md
git commit -m "test(xl-store): phase 0 integration gate"
```

---

## 门禁记录

### 2026-11-03 · Phase 0 集成门禁

- HEAD: `a17e5b5bffb2d1e1152991b6dca4736028ff4893`
- 环境：8 vCPU Linux；测试期间机器有其他并发构建负载。

| # | 命令 | 结果 |
|---|------|------|
| 1 | `cargo test -p xl-store` | **PASS** — lib 429 passed / 0 failed / 1 ignored（63.9s）；`tests/control_faults.rs` 5 passed；`tests/kill9.rs` 1 passed；doc-tests 0；exit 0 |
| 2 | `cargo test -p oss-server` | **PASS（最终一次全绿）** — 75 个测试二进制，817 passed / 0 failed / 2 ignored；exit 0。此前 3 次全量运行各有 1 个不同测试偶发失败，隔离重跑均通过（见下） |
| 3 | `cargo fmt --check` | **PASS** — exit 0 |
| 4a | `cargo clippy -p xl-store --all-targets` | **3 warnings（均为本阶段新增，见下）**：`store.rs:1835`、`store.rs:2059` too_many_arguments (8/7)；`store.rs:2237` collapsible_if |
| 4b | `cargo clippy -p xl-store --all-targets -- -D warnings` | **FAIL** — exit 101，3 个 warning 升为 error |
| 4c | `cargo clippy -p xl-store -- -D warnings`（计划原文命令） | **FAIL** — exit 101，同上 3 个 error |

**oss-server 偶发失败记录（隔离重跑均通过，未发现确定性回归）：**

| 全量运行 | 失败测试 | 现象 | 隔离重跑 |
|---|---|---|---|
| 第 1 次（fail-fast，仅 lib） | `ap_aws::tests::maps_source_ip_and_caches_result` | mock EC2 endpoint `connection refused` | 单测 3/3 通过 |
| 第 2 次（`--no-fail-fast`，75 个二进制） | `m8_ap::alias_path_and_host_forms` | PUT 200、GET 200，但 alias ListObjectsV2 未含 `dir/a.txt` | 单测 3/3；整个 `m8_ap` 二进制 23/23 |
| 第 3 次（fail-fast，67 个二进制后停止） | `sync_gate::delete_bucket_waits_for_inflight_writer_and_quiesces_remote` | 在途 PUT 期望 503，实际 404（PUT 调度晚于删桶完成） | 单测 3/3；整个 `sync_gate` 二进制 38/38 |
| 第 4 次（fail-fast，完整） | 无 | — | exit 0，817 passed |

**clippy 基线对比：** 用 `git archive main` 导出基线树，共享 target 运行 `cargo clippy -p xl-store --all-targets` → 0 warnings（exit 0）。因此上述 3 个 warning 均为 Phase 0 新增，非既有告警：`25dd3c6` 引入 `write_meta_fenced_mode`（8 参数）并扩展 `write_meta_cluster` 参数；`771fb86` 引入可折叠 `if`。本阶段对 oss-server 的生产改动仅 `router.rs` 3 行能力位（`fenced_meta`/`control_quorum` 置 false），与上述偶发失败路径无关。

**门禁结论：未通过。** 阻塞项为 clippy：基线干净，本阶段新增 3 个 warning，违反 preflight 裁定的「no new warnings」。xl-store / oss-server 测试未发现确定性回归（偶发失败均为测试自身竞态或本地 mock 环境问题，隔离可复现为绿）；但按计划「任何失败不得进入 Phase 1」，需先消除 clippy 告警并复跑本门禁。

## 后续阶段

Phase 1–5 各自单独出计划，按设计文档第 7 节顺序执行；Phase 0 门禁未过不得开始 Phase 1。Phase 1 的第一个任务是把 `ControlStore` 抽象与 SQLite 双后端差分测试落地，保持现有行为不变。
