# M3 分布式 实现计划（节点间 mesh · quorum 读写 · 故障容忍 · 读修复 · 跨节点锁）

> **For agentic workers:** REQUIRED SUB-SKILL: 逐 Task 执行——每 Task 有独立步骤、伪代码与验收；切片收尾循 m2b 计划的四闸惯例（fmt · clippy 0 · workspace 全绿 · e2e）。

**Goal:** 把 v1.4.0 的单机存储（单机多盘 + EC 8+4 + 子账户权限）升级为**静态拓扑的多节点集群**：任意节点可写（quorum 达成即返回）、任意节点可读（quorum 内可读到最新已提交版本）、拔节点/分区下不损坏、读修复补齐。**按设计文档 §M3 范围行**：节点间 gRPC mesh（xl-proto）· 静态拓扑 · 对象→纠删组哈希 · 跨节点 quorum 读写 · 节点故障容忍 · 读修复 · 节点间命名空间锁。

**范围边界（钉子，防蔓延）**：rebalance/不停机扩容 → M4；heal/scrub 独立命令与**批量补建**（分片/桶目录）→ M4（M3 的「加回节点后补齐」= **fsck 集群化 + 缺键 meta 补建**，Task 7 Step 1b；分片补建靠读时修兜底、批量归 M4；代码内两处旧注释「M3 heal」按此统一，Task 11）；事件总线 → M6；站点复制 → M7/M9；控制面强一致（openraft）→ M9；多 set → M4；**`/api/v1/cluster/*` 控制台 API 与集群状态页 → M4**（§17.1 标 M3/M4，§19 对照归 M4「集群状态页」）；**完整 Prometheus 指标面（§18.2）→ M4**（随面板；项目尚无 `/metrics`）；**M3 最小信号**：`quorum_unmet_total`（写/锁分列）与「本节点视角各节点 up 数」进 `/health/cluster` JSON（AtomicU64，成本≈零）。M3 健康面 = §19 M3「集群健康端点」= `/health/cluster`（+ 强语义 `/health/ready`，见 G）。**无数据中心/区域/可用区层级**（设计 §5.3 拓扑原则）。

## 用户已拍板的两个架构岔路

1. **跨节点锁 = quorum 租约锁**（设计 §4.4 路线，类 MinIO dsync）：任意节点可写；锁请求广播、达标授予、TTL 续租；不做写入侧 fencing（已知边界）。
2. **控制面跨节点 = 读扇出**：AK/SK 与权限查询在本地 miss 时向 peers 尽力扇出（首答胜、无缓存起步）；写入仍只落本节点；强一致归 M9。

## 二轮评审修正速览（2026-10-13，全量并入；出处 m3-planreviewer / m3-factcheck）

锁 quorum 分域（P0）· 提交 quorum + 补偿 + hash 防伪（P1）· 读 quorum 快路径 · 单调 mod_time · LIST 并集 + 成本收口 · 桶三层语义 · delete meta 上抛 · recycle 跨节点 + 先决链 · gc fail-closed · 锁三洞修正 · **否决 4×4 配置** · e2e 断言三口径 · fsck 差集检测 + 补建闭环 · 拓扑指纹 · 188 测基线等事实修正。明细见文末「评审修订记录」。

---

## 第一部分：核心架构（写进代码注释）

### A. 槽位模型——`ShardRef.drive` 从「本机盘下标」升级为「全局槽位下标」（零 meta 变更）

- 集群 = 静态配置的**扁平节点 × 驱动器**（设计 §5.3）。全局槽位表 = 按配置顺序拼接各节点的盘：`slot = 前序节点盘数之和 + 本地盘序`。`shard → slot = shard % slot_count`（`drives.rs:17-18` 的 `drive_index` 公式不变，`n` 换成全局槽位数）。
- **单节点（未配 `[cluster]`）时槽位表 = 本机盘，全链路行为与 v1.4.0 逐字节一致**。两种既有形态保持：单盘（1 份 meta = 提交点）；多盘 ≥12 盘（`oss.toml:9` 口径）。
- `ShardRef.drive: u8` 语义升级**不动 xl.meta 格式**：单机旧 meta 里的 `drive` 即全局槽位 ✓。上限 255 槽位（12 节点 × 12 盘 = 144）。
- 槽位数 ≥ data+parity（12）为集群配置硬校验；`Store::open` 的本地盘数校验（`store.rs:146`）在集群模式下**只做 self 一致性**（本地盘数 == 自己声明的 drive_count），data+parity 校验移到全局 slot_count。
- **data_dirs 顺序 = 拓扑的一部分**（顺序变化 = 槽位静默错位）：首启写 `.oss.sys/topo` 指纹（slot_count + 每盘标识），不一致**大声失败**；README 写明「M3 **不支持单机原地升级为多节点**（无 rebalance，归 M4）」——「前向兼容天然成立」仅对单节点集群成立。

### A2. Quorum 两域（P0 修正——必须分家）

```
槽位域：Q_s = data + 1          （12 槽 = 9）——分片与 meta 的准备/翻转/读快路径
节点域：Q_n = ⌈n/2⌉ + 1        （4 节点 = 3）——跨节点锁的授予/续租
相交证明（各自域内）：|C ∩ R| ≥ 2Q − N > 0
  槽位 12：2×9−12 = 6 > 0     节点 4：2×3−4 = 2 > 0
禁止跨域换算（「锁要 9 票」= P0 缺陷，4 节点下永远拿不到锁）
```

### B. 分层——xl-store 零框架依赖

store 定义传输 trait（`crates/xl-store/src/mesh.rs`），tonic 实现在 xl-client，oss-server 注入。完整 Rust 形状：

```rust
// mesh.rs —— DTO（Rust 侧；与 proto 一一对应）
pub struct ShardWrite  { pub bucket: String, pub key: String, pub uuid: String, pub part: u32,
                         pub shard: u32, pub crc32c: u32, pub data: bytes::Bytes }
pub struct ShardRead   { pub bucket: String, pub key: String, pub uuid: String, pub part: u32, pub shard: u32 }
pub struct VersionDirRef { pub bucket: String, pub key: String, pub uuid: String }
pub struct MetaWrite   { pub bucket: String, pub key: String, pub meta: bytes::Bytes }
pub struct SlotAck     { pub slot: u16, pub ok: bool, pub hash: [u8; 32] }   // hash = 实际写入/翻转的内容
pub struct MetaAck     { pub ok: bool, pub msg: String, pub slots: Vec<SlotAck>, pub slots_ok: u32 }
pub struct MetaSnapshot { pub meta: bytes::Bytes, pub mod_time_nanos: i64, pub slots_ok: u32 }
pub struct ListEntry   { pub key: String, pub size: u64, pub etag: String, pub max_mod_time_nanos: i64 }
pub struct ListLocal   { pub bucket: String, pub prefix: String, pub delimiter: Option<String>,
                         pub marker: Option<String>, pub limit: usize }      // limit = max_keys + 1（见 D）
pub struct ListLocalResp { pub entries: Vec<ListEntry>, pub prefixes: Vec<String>, pub slots_ok: u32 }
pub struct NodeHealth  { pub node_id: String, pub version: String, pub slots: Vec<(u16, bool)>, pub buckets: u64 }
pub struct LockReq     { pub bucket: String, pub key: String, pub epoch_nanos: i64, pub epoch_node: String,
                         pub holder: String, pub ttl_ms: u64, pub op: LockOp }
pub struct LockResp    { pub granted: bool }
pub enum ControlQuery  { AccessKey(String), Permission { uid: i64, bucket: String, action: u8 },
                         OwnedBuckets(i64), VisibleBuckets(i64), BucketOwner(String) }
pub struct ControlRow  { pub uid: Option<i64>, pub is_admin: Option<bool>, pub active: Option<bool>,
                         pub secret: Option<String>, pub flag: Option<bool>, pub names: Option<Vec<String>> }

#[async_trait::async_trait]
pub trait MeshTransport: Send + Sync {
    async fn write_shard(&self, node: &str, w: ShardWrite) -> Result<()>;
    async fn read_shard(&self, node: &str, r: ShardRead) -> Result<Option<bytes::Bytes>>;
    async fn delete_version(&self, node: &str, d: VersionDirRef) -> Result<()>;
    async fn recycle_version(&self, node: &str, d: VersionDirRef) -> Result<()>;
    async fn prepare_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck>;
    async fn commit_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck>;
    async fn abort_meta(&self, node: &str, m: MetaWrite) -> Result<()>;
    async fn read_meta(&self, node: &str, r: MetaRead) -> Result<Option<MetaSnapshot>>;
    async fn ensure_bucket(&self, node: &str, bucket: &str) -> Result<()>;
    async fn list_bucket_dirs(&self, node: &str) -> Result<Vec<String>>;
    async fn list_local(&self, node: &str, l: ListLocal) -> Result<ListLocalResp>;
    async fn health(&self, node: &str) -> Result<NodeHealth>;
    async fn lock(&self, node: &str, req: LockReq) -> Result<LockResp>;
    async fn control_query(&self, node: &str, q: ControlQuery) -> Result<Option<ControlRow>>;
}
```

- `StoreConfig` 增 `pub cluster: Option<Arc<ClusterConfig>>`；`ClusterConfig { pub topo: ClusterTopology, pub transport: Arc<dyn MeshTransport> }`。未配置 = 单节点（现状）。
- **xl-store 对外 mesh 面（pub 入口）**：现有 `stage_payload`/`object_dir`/`write_meta_on` 等均 `pub(in crate::store)`，xl-client 调不到——Task 3 建立 `Store::mesh_*` 命名与可见性约定，各 RPC 实现随所属 Task 落地。
- **语义学分工**（服务端 handler）：node 收到的 MetaWrite 写**该节点自己的全部本地槽**（drive_count 份）；ListLocal 只扫本节点；ReadShard 只读本节点槽；扇出永远由「协调者」（收请求的节点）做。

### C. 写路径（跨节点 quorum）

**C1. 落片**（`stage_payload_multi`，put.rs:241-277）：循环内按 `shard_target(s)` 分派 `Local → 现文件写 / Remote → write_shard RPC（幂等：同 uuid+shard 覆盖写；服务端先验 CRC32C 再落盘）`；`landed` 计数与 quorum 判据（≥ Q_s）不变；远端落盘序 = 与本机同款（create_dir_all + 写 + fsync + sync_dir）。回滚 `rollback_versions`：Local 删目录 / Remote `delete_version`。

**C2. `multi_drive()` 分叉修正**（put.rs:366）：条件改 `cluster.is_some() || self.cfg.multi_drive()`——集群 + 单盘节点必须走槽位路径，否则分片全落本地 tmp、槽位模型失效。

**C3. meta 保持每盘一份，按槽位扩展**（`write_meta` store.rs:504-577 的 drives 循环换成槽位循环——代码增量最小）。签名的必要变更：写路径已持有 `old_meta`（put.rs:362、mpu.rs 同款），传给 `write_meta` 供补偿用：

```rust
// 原：write_meta(&self, bucket, key, m) -> Result<usize>          （返回翻转盘数）
// 新：write_meta(&self, bucket, key, m, old: Option<&XlMeta>) -> Result<usize>
// 单节点分支（drives.len()==1 或 cluster None）逐字节沿用现状。
```

集群模式算法（协调者视角；「本节点槽」= 自己 3 盘，逐节点 = 每 peer 一条 RPC）：

```text
prepare 阶段
  本节点槽：逐盘 drive_up 探测 → 写 xl.meta.new + fsync → 记 (slot, content_hash)
  每 peer：prepare_meta RPC（node 侧同样逐本地槽写 .new）→ MetaAck.slots
  合计 prepared_slots ≥ Q_s ？ 不足 → abort（清已写 .new，best-effort）→ Err(QuorumUnmet)
commit 阶段（先查租约——见 E）
  本节点槽：对每个 .new  读回算 hash → rename → 记 (slot, hash)
  每 peer：commit_meta RPC → MetaAck.slots（node 侧同款读回-rename）
  失败槽位重试一次（rename 幂等，.new 仍在）
  flipped = Σ slots where ack.ok && ack.hash == 本地 self_hash     ← 只计「内容是自己的」的翻转
  flipped ≥ Q_s → 已提交（<prepared 的差集 → warn「M4 heal 补副本」）
  flipped < Q_s → 补偿回滚：
    对每个已翻且 hash==self_hash 的槽：有 old → 写 old 的 .new + rename 恢复；无 old → 删 xl.meta
    补偿失败的槽 → 留痕（tracing error + fsck 报告项）→ 仍返回 Err(QuorumUnmet)（500）
  调用方（put/mpu）：收到 Err → rollback_versions + 清 tmp/空目录（现有异常路径原样复用）
```

**为什么补偿必须做**：8/12 已翻时任一 9-读集必含 ≥5 个已翻副本 → 不补偿会让「已返回失败」的写被读到。**为什么 hash 必须对**：无 fencing 锁失效窗口下，W1 的 CommitMeta 可能 rename 到 W2 写入的 .new（共享路径）——不计入 quorum 即防伪达成。

**C4. mod_time per-key 单调**（一行改动，位置已勘定）：`put.rs:362` 已读 `old_meta`、`:365` 定义 `now`——改为

```rust
let now = match old_meta.as_ref().and_then(|m| m.latest()) {
    Some(v) => now_nanos().max(v.mod_time_nanos + 1),
    None => now_nanos(),
};
```

mpu.rs 的 complete 同理（同一模式）。单调性使 latest-wins 与墙钟脱钩——M2b R8 的正解在 M3 兑现。

**C5. recycle_old 跨节点**（put.rs:317-341，调用点 :455/:470、mpu.rs:365）：本地各盘 rename 进 trash + 远端 `recycle_version`（rename 进对端该盘 trash）；远端失败 warn——孤儿由**该节点**的 clean/fsck 按龄回收。**先决链**：C3 的提交 quorum ⇒ 读集必含已提交副本 ⇒ 旧版本无读者 ⇒ 回收安全。

**C6. 桶三层语义**：
- **强建 fail-closed**：`create_bucket` = 本地各盘 + 广播 `ensure_bucket`；**任一配置节点不可达 → 报错**（静默部分建 = 缺桶节点 404，且分片分布确定（`shard % slot_count`），落不到该节点的桶永不靠懒自愈修复）。
- **懒自愈兜底**：对象落片时远端 `create_dir_all` 连带建桶目录（只覆盖「有分片/meta 落该节点」的桶）。
- **fsck 收敛**：Task 7 增「桶目录跨节点比对」巡检项。
- `delete_bucket`：空检查须**所有节点**做、任一不可达即拒（M2b R2 防复活语义的跨节点对应物）。
- `list_buckets` = 本机各盘并集 ∪ 各节点 `list_bucket_dirs`；creation_nanos 取首个见到的值。

**C7. delete_object**（store.rs:589-629）：逐槽位 Local 删 / Remote `delete_version`；在册预检（fail-closed 扩大坐标域）；**远程 meta 删除失败即上抛**——meta 删除是可见性提交点，best-effort 吞掉会让对象在部分节点「复活」而删除已报成功。

**C8. MPU 会话粘节点**：暂存区在本节点第 0 块盘（put.rs:364、mpu.rs:266）；同一 upload 后续请求须回同一节点（upload_id 无节点信息）。已知边界，文档记明。

### D. 读路径与读修复

**D1. meta 读 = quorum 快路径（覆盖率制）**。`read_meta`（store.rs:416-458）现逐盘扫描、latest 胜、corrupt fail-closed——集群版在其上包一层「扇出 + 覆盖率」：

```text
covered = 0; best = None; corrupt = None
并发发起：本节点逐盘扫描（现逻辑，产出 local_best/local_covered）
         + 每 peer 一条 read_meta RPC（node 侧返回它本地 best + slots_ok=成功读到的盘数）
按完成顺序收：每到一个响应 → covered += resp.slots_ok；用 mod_time 更新 best
covered ≥ Q_s → 提前决策（丢弃/取消掉队者——SIGSTOP 节点不再拖尾延迟）
收完全部仍 covered < Q_s → Err(CorruptMeta("quorum 覆盖不足：covered/9"))   ← fail-closed
决策：best 存在 → Ok(Some)（strict 轮的 500 语义不变）
      best 无 && corrupt 有 → Err(CorruptMeta)（现状语义）
      best 无 && corrupt 无 && covered ≥ Q_s → Ok(None)（缺席可证明：任何已提交副本必在覆盖交集内）
本地盘的 NotFound 计入 slots_ok（「此处无 meta」是确定性观察）；IO/解码错误不计并记 corrupt。
```

- **回退一次**：GET 拿到 meta 后在 strict 数据轮因分片缺失/坏片失败 → 全量扇出（不提前停）重取 meta 一次再读——处理「快路径选中的副本落后于数据实际终态」的极小窗口。
- **否决**「本地优先+版本比对」（提交 quorum 语义下本地副本可能是旧的）；否决等最慢节点的全量扇出。本地优先仅可作为发包顺序。

**D2. read_ec 逐片分派**（read.rs:218-298）：循环内 `shard_target(s)` → Local 现文件读 / Remote `read_shard`。远端连接失败/超时 ≡ **缺失**（入 missing，不 Err）；CRC32C/长度不符 → bad_bytes（现状）；strict 轮语义、重建、SHA256 终验、截断全部原样。并发性：远端读以 `FuturesUnordered` 并发（本地保持顺序读即可；注意 Err 语义只在本地 IO 错误保留）。

**D3. repair_shards 远程化**（read.rs:303-350）：缺失片的 `drive_up` 探活换 `shard_target`——Local 现逻辑；Remote `write_shard`（探活 = 该次 RPC 本身，失败 warn）。

**D4. list_objects 集群强一致 + 成本收口**。现状：`collect_keys` 逐盘扫（list.rs:83-112），`list_objects` 逐键 `read_meta`（list.rs:67）。集群版：

```text
page 请求（bucket, prefix, delimiter, marker, max）
  limit_nodes = max + 1                                  ← 关键：每节点取 max+1 条输出
  本机：现有 list_objects 逻辑，limit = max+1
  每 peer：list_local RPC（同参、limit=max+1）→ entries(key,size,etag,max_mod_time) + prefixes
  合并：按 key 去重（同键取 max_mod_time 最大的条目——其持有者的 meta 即已提交版本，
        尺寸/ETag 直接用它，**不做逐键 meta RPC**）；prefixes 去重
  按 key 序遍历做 max 截断与 cp 去重（复用 list.rs:35-66 的单机规则）→ next_token=被截断处的键
```

- **`limit = max+1` 的完备性证明（写进注释）**：设合并页截断于 K（合并后第 max 个输出）。任一节点 A 已返回其前 max+1 个输出；若 A 有未返回输出其 key < K，则 A 的前 max+1 个输出全 < K，而合并中 < K 的输出至多 max−1 个——矛盾。故所有未返回输出 > K，`next_token=K` 续页不丢键。
- **max_mod_time 单调（C4）是「相等即复用本地」的严格前提**：不引入每键扇出（list.rs:67 的 N 倍 RPC 风暴否决）；只有 `cluster_max > local_max` 的少数键（本节点曾掉线错过提交）用对端条目。
- 内联 meta 可达 128KiB——`ListLocal` **不带 meta 字节**（否则整页百 MiB 级线放大）。

**D5. reap_stale fail-closed**（gc.rs:56-108）：集群模式对象目录清扫前逐节点探活；**任一节点不可达 → 跳过该对象的子目录回收**（保守口径对齐 delete；trash/tmp 按龄回收不受影响）。单机语义不变。

### E. 跨节点命名空间锁（quorum 租约，节点域）

`lock.rs` 状态机：

```rust
struct LockEntry { epoch: (i64, String), holder: String, expires: Instant }
struct LockTable { inner: std::sync::Mutex<HashMap<(String, String), LockEntry>> }  // (bucket,key)

// 授予规则（修正后）——仅在持有者进程内表上执行：
fn try_grant(table, key, req) -> bool:
    entry = table.get(key)
    match entry:
      None                                  → 插入 (req.epoch, req.holder, now+ttl) → true
      Some(e) if e.expires <= now:            // 已过期
          if req.holder == e.holder          → 续期（同持有者重入）
          else if req.epoch > e.epoch        → 替换 → true
          else                               → false   // 延迟到达的旧请求不得抢回
      Some(e) if e.holder == req.holder      → 续期 → true（Renew 走这里）
      Some(_)                                → false   // 活租约：**绝不按 epoch 抢占**
```

**协调者流程**（写路径 lock_object 的第三层）：

```text
acquire(bucket,key):
  epoch = (本机墙钟纳秒, self_node_id)        ← epoch 仅用于「过期替换」定序与日志
  loop（重试上限 10，退避 50–200ms 抖动）:
    并发广播 Acquire 到全节点（含本机表）→ granted 数（含本机）≥ Q_n → 持有
    不足 → 对已授予者发 Release（best-effort）→ 退避重试
  耗尽 → Err(QuorumUnmet)（分区拒写——验收⑤）
LockGuard { key, epoch, lease_lost: Arc<AtomicBool>, renew_task: JoinHandle }
  持有时 spawn 后台续租：每 TTL/3 广播 Renew → granted ≥ Q_n 续成；失败 → lease_lost=true
  Drop：abort 续租任务 + spawn Release 广播（best-effort，不 await）
写入口检查：prepare 前、commit 前各查一次 lease_lost / 剩余 TTL；失效 → 中止 + 补偿（C3）
```

- TTL 默认 10s 可配；**Renew 与 Acquire 同口径 Q_n**（初稿「≥半数」集与 Acquire 不相交 → 双持有者）。
- 接入 `lock_object`（store.rs:258-276）：进程内锁 → flock（现有）→ 集群锁（仅 cluster 配置），顺序恒定防死锁。
- **不做写入侧 fencing（裁决 + 边界）**：兜底 = 版本 uuid 隔离——交错最坏 = 双目录 + meta 最新者胜，败者成孤儿（各节点 clean/fsck 回收），**已提交数据不损坏，代价是回收延迟**。

### F. 控制面读扇出（用户拍板路线）

- `ControlQuery` 白名单（服务端 match **字面量 SQL**；sqlx 0.9 禁动态字符串）：`AccessKey(ak)` → (uid, is_admin, active, secret)（JOIN users.status，与 SigV4Auth 同口径）；`Permission(uid,bucket,action)` → bool；`OwnedBuckets/VisibleBuckets(uid)` → Vec<String>；`BucketOwner(bucket)` → Option<uid>。
- 接入：`SigV4Auth::get_secret_key`（auth.rs:23）与 `Acl::{user_of_key(access.rs:55), can(:83), visible(:117), is_owner(:73)}` 本地 miss → 并发扇出全部 peers、首答胜；**不加缓存起步**（ponytail：5s TTL 升级点与撤销延迟权衡记档）。
- 写入路径仍只落本节点；强一致归 M9。

### G. 健康与可观测

- `GET /health/cluster`（**新建**——现状只有 `/health/live`，router.rs:51）：本节点 + 各 peer 的 `health()` + **最小计数** `quorum_unmet_total`（写/锁分列，AtomicU64）与各节点 up 数。响应示例：
  ```json
  { "node_id": "n1", "version": "1.5.0", "nodes": [ {"id":"n1","up":true,"slots":3},
    {"id":"n2","up":true,"slots":3}, {"id":"n3","up":false,"slots":0} ],
    "quorum_unmet": { "write": 12, "lock": 3 }, "ready": true }
  ```
- `GET /health/ready`（**新建，强语义 = 设计 §18.4**）：本机 slot0 可用 **且** 可达节点数 ≥ Q_n——分区/少数派节点被 LB 摘除。
- **mesh 边界三条（写死口径）**：版本协商——静态同版本部署，Health.version 仅观测；传输——无 TLS（内网假设）明示；启动时校验对端 Health 回传 node_id == 配置 id（防错接线）。
- **错误码映射**：`QuorumUnmet`（含锁拿不到、覆盖不足）→ 500 `InternalError`（§0.2）；验收⑤脚本按 5xx 断言。
- tracing 跨节点传播（设计 D:362）：mesh 注入 `traceparent`、服务端提取（Task 10）。

---

## 第二部分：执行顺序与依赖

```
Task 1 (xl-proto) ─→ Task 2 (拓扑) ─→ Task 3 (mesh 骨架:server+Health+pub 面+fixture) ─→ Task 4 (配置+/health)
                                                                                      │ 切片一四闸
切片二：Task 5a (分片 RPC+落盘+回收) ──┐
        Task 5b (read_ec+repair)       ├─→ Task 6 (meta 层:两阶段/快路径/删除/桶/列表) ─→ Task 7 (fsck+gc)
        （5a/5b 可并行，同 fixture）    │   （6 依赖 5a 的 write_shard/delete_version 就位）      │ 切片二四闸
切片三：Task 8 (锁)    Task 9 (控制面扇出)   —— 无相互依赖，可并行  ─────────────────────────→ Task 10 (e2e)
                                                                                              → Task 11 (收尾)
硬依赖：recycle 远端分派（5a Step 4）与 Task 6 的 commit quorum **同批落地**（先决链）；
        LIST 的「相等即复用」严格依赖 Task 6 Step 3 单调 mod_time；锁失效检查（Task 8）接在 Task 6 的提交检查点。
```

---

## 第三部分：文件结构（改动面）

```
crates/
├── xl-proto/                      [新 #4] proto 定义 + tonic 生成（build.rs）
│   └── proto/oss.proto            完整消息与 service（Task 1 Step 1 全文）
├── xl-client/                     [新 #5] tonic 封装 + 服务端 handler
│   └── src/lib.rs                 impl MeshTransport for TonicMesh；serve(store, addr)
├── xl-store/                      [改] 保持零框架依赖
│   └── src/
│       ├── mesh.rs                [新] trait + DTO（B 节全文）
│       ├── store.rs               [改] StoreConfig.cluster；mesh_* pub 面；write_meta 槽位两阶段；
│       │                          read_meta 覆盖率快路径；mod_time 单调（:362/:365 一行）；lock_object 接集群锁
│       ├── store/drives.rs        [改] ClusterTopology + shard_target（Task 2）
│       ├── store/put.rs           [改] 落片分派；multi_drive 分叉（:366）；回滚；recycle 远端
│       ├── store/read.rs          [改] read_ec 分派（:218-298）；repair_shards（:303-350）
│       ├── store/list.rs          [改] 集群并集合并（:14-78 扩展）
│       ├── store/lock.rs          [新] quorum 租约锁（E 节）
│       ├── store/gc.rs            [改] reap_stale fail-closed（:56-108）
│       ├── store/fsck.rs          [改] 集群巡检 + 差集检测 + 补建
│       └── store/tests.rs         [改] in-process fixture（4×3）
├── xl-s3/                         [改] Acl/SigV4Auth miss→扇出
└── oss-server/                    [改] config.rs / state.rs / router.rs（Task 4）
tests/e2e_m3_cluster.sh            [新] 4 节点 × 3 盘 = 12 槽（Task 10）
```

依赖：xl-proto 引 `tonic 0.12` + `prost 0.13` + `tonic-build`（已在实际编译图——ecat 7 默认带 grpc）；xl-store 增 `async-trait`（0.1.92 已在锁内）。不引新生态依赖。

---

# 切片一：M3-1 干线（proto · 拓扑 · mesh 骨架与 Health · 健康面）

## Task 1: xl-proto crate

**Files:** Create: `crates/xl-proto/{Cargo.toml, build.rs, proto/oss.proto, src/lib.rs}`；Modify: workspace `Cargo.toml`（members 加两项）

- [x] **Step 1:** `proto/oss.proto` 全文：

```proto
syntax = "proto3";
package oss.v1;
service Mesh {
  rpc WriteShard(ShardWrite) returns (WriteResp);
  rpc ReadShard(ShardRead) returns (ShardResp);
  rpc DeleteVersion(VersionDirRef) returns (WriteResp);
  rpc RecycleVersion(VersionDirRef) returns (WriteResp);
  rpc PrepareMeta(MetaWrite) returns (MetaAck);
  rpc CommitMeta(MetaWrite) returns (MetaAck);
  rpc AbortMeta(MetaWrite) returns (WriteResp);
  rpc ReadMeta(MetaRead) returns (MetaResp);
  rpc EnsureBucket(BucketRef) returns (WriteResp);
  rpc ListBuckets(Empty) returns (BucketsResp);
  rpc ListLocal(ListReq) returns (ListLocalResp);
  rpc Health(Empty) returns (NodeHealth);
  rpc Lock(LockReq) returns (LockResp);
  rpc ControlQuery(ControlReq) returns (ControlResp);
}
message Empty {}  message WriteResp { bool ok = 1; string msg = 2; }
message ShardWrite { string bucket=1; string key=2; string uuid=3; uint32 part=4;
                     uint32 shard=5; uint32 crc32c=6; bytes data=7; }
message ShardRead  { string bucket=1; string key=2; string uuid=3; uint32 part=4; uint32 shard=5; }
message ShardResp  { bool found=1; uint32 crc32c=2; bytes data=3; }
message VersionDirRef { string bucket=1; string key=2; string uuid=3; }
message MetaWrite { string bucket=1; string key=2; bytes meta=3; }
message SlotAck { uint32 slot=1; bool ok=2; bytes hash=3; }
message MetaAck { bool ok=1; string msg=2; repeated SlotAck slots=3; uint32 slots_ok=4; }
message MetaRead  { string bucket=1; string key=2; }
message MetaResp  { bool found=1; bytes meta=2; int64 mod_time_nanos=3; uint32 slots_ok=4; }
message BucketRef { string bucket=1; }  message BucketsResp { repeated string names=1; }
message ListReq { string bucket=1; string prefix=2; string delimiter=3; string marker=4; uint32 limit=5; }
message ListEntry { string key=1; uint64 size=2; string etag=3; int64 max_mod_time_nanos=4; }
message ListLocalResp { repeated ListEntry entries=1; repeated string prefixes=2; uint32 slots_ok=3; }
message NodeHealth { string node_id=1; string version=2; repeated SlotState slots=3; uint64 buckets=4; }
message SlotState { uint32 slot=1; bool up=2; }
message LockReq { string bucket=1; string key=2; int64 epoch_nanos=3; string epoch_node=4;
                  string holder=5; uint64 ttl_ms=6; LockOp op=7; }
enum LockOp { ACQUIRE=0; RENEW=1; RELEASE=2; }
message LockResp { bool granted=1; }
message ControlReq { ControlKind kind=1; string a=2; string b=3; uint32 c=4; }
enum ControlKind { ACCESS_KEY=0; PERMISSION=1; OWNED_BUCKETS=2; VISIBLE_BUCKETS=3; BUCKET_OWNER=4; }
message ControlResp { bool found=1; int64 uid=2; bool is_admin=3; bool active=4;
                      string secret=5; bool flag=6; repeated string names=7; }
```

- [x] **Step 2:** `build.rs`：`tonic_build::compile_protos("proto/oss.proto")`；`src/lib.rs`：`tonic::include_proto!("oss.v1");` + `pub const SERVICE: &str` 版本常量供 Health.version。
- [x] **Step 3:** Cargo.toml（`tonic = "0.12"`, `prost = "0.13"`, build-dep `tonic-build = "0.12"`, workspace 声明共享版本）；`cargo test -p xl-proto` 编译通过。
- [x] **Step 4:** 提交 `feat(xl-proto): 节点间 gRPC 协议骨架——分片/meta/版本目录/回收/桶/列表/健康/锁/控制面`。

**验收**：`cargo build -p xl-proto` 通过；`grep -c "rpc " proto/oss.proto` = 14。

## Task 2: 槽位模型与拓扑（xl-store，零 meta 变更）

**Files:** Modify: `crates/xl-store/src/store/drives.rs`、`store.rs`、`store/put.rs`（分叉）

- [x] **Step 1:** `drives.rs` 增（保留现有 `drive_index` 单机语义）：

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSpec { pub id: String, pub addr: String, pub drive_count: u16 }

#[derive(Clone, Debug)]
pub struct ClusterTopology { pub self_idx: usize, pub nodes: Vec<NodeSpec>, bases: Vec<u16>, slot_count: u16 }

impl ClusterTopology {
    pub fn new(self_idx: usize, nodes: Vec<NodeSpec>) -> crate::Result<Self> {
        // 校验：nodes 非空、id 唯一、drive_count ≥ 1、
        // slot_count ≤ 255 且 ≥ ec.data+ec.parity（12）（调用方传入 EcConfig，Err 指明两项数值）
    }
    pub fn slot_count(&self) -> u16;
    pub fn node_of_slot(&self, slot: u16) -> usize;            // bases 二分
    pub fn local_drive_of_slot(&self, slot: u16) -> Option<usize>; // 本节点 → Some(slot - self_base)
    pub fn local_slot_base(&self) -> u16;
    pub fn self_node(&self) -> &NodeSpec;
    pub fn slot_of(&self, shard: u8) -> u16 { shard as u16 % self.slot_count }   // 全局 drive_index
}

/// 分片落点：单节点恒 Local（与现状同路径）；集群按 slot 归属。
pub enum ShardTarget<'a> { Local { drive: &'a Path, slot: u16 }, Remote { node: &'a str, slot: u16 } }

impl Store {
    pub(in crate::store) fn shard_target<'a>(&'a self, s: &ShardRef) -> Option<ShardTarget<'a>> {
        // cluster None：drive_root(&self.cfg.drives, s.drive) → Local（越界 None，同现状语义）
        // cluster Some：topo.slot_of(s.drive) → local_drive_of_slot → Local(本地盘) / Remote(node id)
    }
}
```

- [x] **Step 2:** `StoreConfig` 增 `pub cluster: Option<Arc<ClusterConfig>>`（`ClusterConfig { pub topo: ClusterTopology, pub transport: Arc<dyn MeshTransport> }`，mesh.rs 先落 trait 空壳亦可）；`Store::open`：`cluster: Some` 时校验 `topo.nodes[self_idx].drive_count == cfg.drives.len()`（self 一致性），**跳过 data+parity 的本地盘数校验**（移到 Topology::new）；`cluster: None` 现校验与行为不动。
- [x] **Step 3:** 盘引用重构 = **六处解引用 + N 处逐盘循环**（后者在后续 Task 添集群语义，此处登记不漏改）。解引用：read.rs:238/244、read.rs:322/330、fsck.rs:147/151 改为 `shard_target`；逐盘循环点：store.rs 304 / 341 / 349-371 / 378 / 521 / 582 / 596、put.rs:229-232、gc.rs:59/64、fsck 逐盘扫描。**`multi_drive()` 分叉（put.rs:366）条件改 `cluster.is_some() || multi_drive()`**。`put.rs:242`（drive_index 调用）与 `:248`（object_dir(drives[d])）同改——单节点下 `shard_target` 恒 Local、路径逐字节一致。
- [x] **Step 4:** 测试（drives.rs 内联）：
  - `topology_slot_math_truth_table`：4×3 → slot_of 真值表（14%12=2 → n1 本地盘 2；16%12=4 → n2 盘 1 …）、local/remote 判定；
  - `topology_rejects_bad_config`：self_idx 越界 / id 重复 / slot_count 11（<12）/ drive_count 0 → Err 且消息含数值；
  - 现有 188 测全绿（**单节点路径零变化是本 Task 唯一验收**）。
- [x] **Step 5:** 提交 `feat(xl-store): 槽位模型——ShardRef.drive 升级为全局槽位（单节点零行为变化）`。

## Task 3: Mesh 骨架（tonic server + Health + pub mesh 面）与 in-process 基座

**Files:** Create: `crates/xl-store/src/mesh.rs`（trait+DTO 全文，见 B 节）；Create: `crates/xl-client/src/lib.rs`；Modify: `store.rs`（`pub mod mesh` 导出）、`store/tests.rs`

- [x] **Step 1:** `mesh.rs` 按 B 节落 trait + DTO；`xl-store/Cargo.toml` 增 `async-trait`、`bytes`（已有）、`futures`（已有）。
- [x] **Step 2:** `Store::mesh_*` pub 面骨架（可见性约定，health 先行）：
  ```rust
  impl Store {
      pub async fn mesh_health(&self) -> mesh::NodeHealth;                 // node_id 来自 cluster.topo
      pub async fn mesh_read_meta(&self, b: &str, k: &str) -> Result<Option<mesh::MetaSnapshot>>;
      // 其余 mesh_* 随所属 Task：mesh_write_shard/mesh_read_shard/mesh_*_meta/mesh_delete_version/
      // mesh_recycle_version/mesh_ensure_bucket/mesh_list_local/mesh_list_buckets/mesh_lock/mesh_control_query
  }
  ```
- [x] **Step 3:** **tonic server 骨架 + Health RPC service impl**（切片一必需——Task 4 双节点 `/health/cluster` 自验收依赖 peer 服务端；数据面 RPC 留 Task 5a）：xl-client `TonicMesh { nodes: HashMap<String, (String, Channel)>, secret: String, self_id: String }`；`pub async fn serve(store: Arc<Store>, addr: SocketAddr, secret: String) -> Result<()>`——interceptor 校验 metadata `x-oss-cluster-token`；lazy connect（首用建 channel）、连接超时 1s / 请求超时 5s。
- [x] **Step 4:** in-process 实现 + fixture（tests.rs）：
  ```rust
  pub struct InProcessMesh { peers: Mutex<HashMap<String, Weak<Store>>> }   // 构建后回填破循环
  impl MeshTransport for InProcessMesh { /* 直调对端 Store::mesh_* */ }
  pub struct Cluster { pub stores: Vec<Store>, pub mesh: Arc<InProcessMesh>, pub down: Mutex<HashSet<String>> }
  impl Cluster {
      pub async fn new(4, 3) -> Cluster;                  // 4 节点 × 3 盘 tempdir
      pub fn down(&self, node: &str);  pub fn up(&self, node: &str);       // 摘除/恢复
      pub fn delay(&self, node: &str, d: Duration);       // 慢节点注入（快路径测试）
      pub fn fail_once(&self, node: &str, op: &str);      // 提交失败注入（补偿测试）
  }
  ```
  **fixture 注释写死两条**：(a) in-process handler 不得再向外扇出（真 RPC 只是超时，in-process 是循环 await 死锁）；(b) 不变量断言 `slot_count == data + parity`、EC 参数由 slot_count 推导勿硬编码 8+4（3×2/4×4 会让算术静默坏——4×4 尤坑：16 槽下 `shard % 16` 只落 0..11，第 4 节点零分片、容错反更差）。
- [x] **Step 5:** 测试：Topology↔fixture 真值表；health 双节点往返；`down/up/delay` 注入生效。
- [x] **Step 6:** 提交 `feat(xl-store+xl-client): Mesh 骨架——pub mesh 面、tonic server + Health、in-process 4×3 基座`。

## Task 4: oss-server 配置面与 /health/cluster

**Files:** Modify: `crates/oss-server/src/{config.rs, state.rs, router.rs}`

- [x] **Step 1:** config.rs（`AppConfig` :8，load :82）增：

```rust
#[derive(Deserialize)] pub struct ClusterToml { pub node_id: Option<String>, pub secret: Option<String>,
    #[serde(default)] pub nodes: Vec<NodeToml> }
#[derive(Deserialize)] pub struct NodeToml { pub id: String, pub addr: String, pub drive_count: u16 }
```

```toml
[cluster]                                  # 未配置 = 单机（现状零变化）
node_id = "n1"
secret  = "cluster-internal-token-change-me"
[[cluster.nodes]]
id = "n1"; addr = "127.0.0.1:19101"; drive_count = 3
# n2/n3/n4 同构 → 4 节点 × 3 盘 = 12 槽（单节点故障 = 写零余量，临界口径见 README）
```

- [x] **Step 2:** state.rs（StoreConfig 构造 :56-63）：配置了 cluster 时——校验 `node_id` 在 nodes 中、`data_dirs.len() == 本节点 drive_count`；构造 `ClusterTopology` + `TonicMesh` → `StoreConfig.cluster` 注入；`serve` 起 tonic server（Task 3 Step 3）。
- [x] **Step 3:** router.rs：`/health/cluster`（G 节 JSON）+ `/health/ready`（槽 0 可用 + 可达 ≥ Q_n；G 节）；**启动校验：逐 peer 拉一次 Health，node_id 必须 == 配置 id（不一致拒启）**。
- [x] **Step 4:** 双进程冒烟（依赖 Task 3 Step 3 的 server 骨架）：两节点各起 oss-server（不同端口/data_dirs/DB），`curl /health/cluster` 双方都见 2 节点；`kill` 其一 → `up:false` + ready 语义（2 节点时自身可写无需多数——冒烟只查 JSON 形状）。提交 `feat(m3): cluster 配置面与 /health/cluster（M3-1 收尾）`。

**切片一四闸**：fmt · clippy 0 · `cargo test --workspace` 全绿（单节点 188 测 + 新拓扑/health/in-process 测） · 单机 `tests/e2e_s3.sh` 27 步零差异（cluster 未配置路径）。

---

# 切片二：M3-2 数据面（跨节点 quorum 读写）

## Task 5a: 分片 RPC 与落盘分派（含旧版本回收）

**Files:** Modify: `xl-store/src/store/put.rs`；xl-client（数据面 service impl + pub 入口）

- [x] **Step 1（测试先行，in-process 4×3）**：命名测试——
  - `cluster_write_distributes_by_slot`：写 129KiB（走 EC）→ 断言每槽恰 1 片、`shard i → slot i%12`；
  - `cluster_read_after_node_down`：摘 n2 → 读回一致（重建路径）；
  - `cluster_write_with_node_down_succeeds`：摘 1 节点 → 写成功（9 槽恰好 quorum）；
  - `overwrite_recycles_remote_old_version`：覆写 → 远端槽位 trash 出现旧 uuid 目录。
- [x] **Step 2:** `stage_payload_multi`（put.rs:241-277）落片分派：
  ```rust
  // 现循环：drive_index → drive_root → drive_up → 写 part 文件
  // 改：shard_target(&s)?：
  //   Local {drive,..} → 现逻辑原样（drive_up 探测 + create+write+fsync+sync_dir）
  //   Remote {node,slot} → transport.write_shard(node, ShardWrite{..crc32c..})；失败 warn 计 quorum
  // touched 记 (slot)，供回滚
  ```
- [x] **Step 3:** service impl（xl-client）：WriteShard（**先验 crc32c 再落盘**；幂等覆盖）→ `Store::mesh_write_shard`；ReadShard / DeleteVersion / RecycleVersion 同批；AbortMeta/PrepareMeta/CommitMeta/ReadMeta 骨架留 Task 6 填。
- [x] **Step 4:** `rollback_versions` 分派（put.rs 内）与 `recycle_old` 远端分派（C5）。**与 Task 6 的 commit quorum 同批落地（先决链：回收安全依赖读相交）**。提交 `feat(m3): 跨节点分片写读、旧版本回收与落盘分派`。

## Task 5b: 读路径与读时修

**Files:** Modify: `xl-store/src/store/read.rs`

- [x] **Step 1（测试先行）**：`cluster_read_reconstructs_from_remote`（摘节点读回一致）；`remote_bad_shard_treated_missing`（远端返回坏字节 → bad_bytes 语义）；`repair_writes_back_to_remote_slot`（读时修把缺失片回写远端，幂等）。
- [x] **Step 2:** `read_ec`（read.rs:236-262 循环）分派：`Local → 现读 / Remote → read_shard`；远端失败/超时 ≡ 缺失（missing.push，不 Err；本地 IO 错误的 Err 语义保留）；远端读用 `futures::stream::FuturesUnordered` 并发。strict 轮、重建、SHA256 终验、截断原样。
- [x] **Step 3:** `repair_shards`（read.rs:320-333）分派：Local 现逻辑；Remote `write_shard`（探活 = RPC 本身，失败 warn）。
- [x] **Step 4:** 提交 `feat(m3): read_ec 跨节点分派与远程读修复`。

## Task 6: meta 层跨槽位分派（两阶段提交 · 读快路径 · 删除 · 桶与列表）

**Files:** Modify: `xl-store/src/store.rs`（write_meta:504-577、read_meta:416-458、delete_object:589-629、delete_bucket、create_bucket、list_buckets、clear_meta_new:582、mod_time :362）；`list.rs`；xl-client（Meta/List/Bucket RPC）

- [x] **Step 1（测试先行，in-process 4×3；复用 fixture 的 down/delay/fail_once）**：
  - `meta_latest_wins_keeps_semantics`（现状语义保持）；
  - `meta_mod_time_monotonic_under_skew`：人工把对端 meta mod_time 写超前 → 新版本仍胜；
  - `partial_flip_compensates`：`fail_once` 使 3 槽翻不动 → 客户端 5xx 且全部槽 meta 回到旧值（坏注入清掉后重读验证）；
  - `foreign_content_hash_not_counted`：伪造「翻到他写者内容」→ 不计入 quorum（翻 int 断言）；
  - `read_fast_path_cancels_straggler`：`delay(n3, 500ms)` → 读仍在 ~1 往返完成、结果正确；
  - `read_coverage_below_quorum_fails_closed`：摘 2 节点 → GET 得 500（CorruptMeta），**不是 404**；
  - `read_fallback_full_fanout_once`：选中副本数据轮失败 → 回退成功；
  - `delete_fanout_fails_loud`：远端 delete_version 注入失败 → delete 报错（不吞）；
  - `bucket_create_fail_closed` / `delete_bucket_any_unreachable_rejects`；
  - `list_union_sees_object_written_while_down`：n2 掉线期写入 → n2 恢复后 LIST 仍见（并集）；`list_pagination_no_skip`：造 >max 键、`limit=max+1` 续页枚举完整（防丢键）。
  - **落地实录（f77dd6c）**：已实现并通过 11 条集群测试（分片落位/掉线写读/远端回收/单调/部分翻转补偿零残留/覆盖不足 fail-closed/快路径取消掉队者/桶扇出与守卫/删除全节点/LIST 并集/分页完备）。**两枚微测待补**（逻辑已实现、由相邻用例间接覆盖）：`foreign_content_hash_not_counted`（翻转他人 .new 不计入——需在 prepare/commit 间注入，随 Task 7 批一起补）与 `read_fallback_full_fanout_once`（选中副本数据轮失败 → 全量回退；喂给 Task 10 前的测试补齐）。另：`mesh_read_meta` 初版误调 `self.read_meta` 引发集群循环扇出（in-process 死循环实锤），已改就地本地扫描并写死注释——**服务端 handler 绝不扇出**是硬约定。
- [x] **Step 2:** `write_meta` 槽位化（C3 伪代码逐条落）：新签名 `write_meta(bucket, key, m, old: Option<&XlMeta>)`；prepare/commit 每节点一条 RPC（node 侧写/翻**其本地全部槽**）；hash 只计自己的；重试一次；集群 <Q_s → 补偿（old 恢复 / 无 old 删）；**单节点分支 ≥1 即提交（M2b 原样，kill9 `FLIP_K` 口径不动）**——两分支显式注释。`clear_meta_new` 槽位化。
- [x] **Step 3:** `read_meta` 覆盖率快路径（D1 伪代码逐条落）+ 全量回退一次；`put.rs:362/:365` 单调 mod_time（C4 一行）。
- [x] **Step 4:** 删除/桶/列表（C6/C7/D4）：delete_object 分派 + 失败上抛；create/delete/list_buckets 分派；`list_objects` 并集合并（D4 伪代码 + `limit=max+1` 证明注释）。
- [x] **Step 5:** `lock_object` 预留集群锁位（Task 8 填充：结构上先加 `Option<ClusterLockGuard>` 字段位置）。提交 `feat(m3): meta 层跨槽位分派——quorum 提交（重试+补偿+hash）、读快路径、删除/桶/列表（fail-closed 扩大坐标域）`。

## Task 7: 集群化 fsck + gc fail-closed（验收②本体）

**Files:** Modify: `xl-store/src/store/fsck.rs`、`gc.rs`、`cli.rs` 面（命令落点不变）

- [x] **Step 1:** `fsck`：本机全量 + 对每 peer 先 health（down → 报告 `unreachable` 计项）再扇出分片巡检（v1 用 `read_shard` 逐片；批量 Scrub RPC 留 M4）→ 合并报告。**键集来源（评审）**：缺失检测 = 「本机键集 vs 各 peer `ListLocal` 键并集」**差集**（fsck 本机扫描发现不了缺键）。巡检项：桶目录跨节点比对、meta 缺失（差集）。
- [x] **Step 1b（M3 做掉闭环，~30 行）:** `--repair` 对每个缺键 quorum `read_meta` 取最新 meta **写回本机各槽**（复用 write_meta_on）；分片补建：读时修兜底（Task 5b）、批量 heal → M4。报告 JSON 增 `missing_meta_keys` 字段。
- [x] **Step 2:** `reap_stale`（gc.rs:56-108）集群 fail-closed（D5）：对象目录清扫前任一节点不可达 → 跳过该对象；trash/tmp 按龄回收照旧。
- [x] **Step 3（测试先行）**：
  - `fsck_repairs_node_returned`：摘 n2 写入 → 恢复 → `fsck --repair`（in-process 直调）→ n2 槽位分片 CRC 全一致、缺键 meta 已补；
  - `gc_skips_when_any_node_down`：注入不可达 → 断言 reap_stale 对已提交版本目录零删除；
  - `gc_reclaims_orphan_when_all_up`：全上 → 孤儿按龄回收照旧。
- [x] **Step 4:** 提交 `feat(m3): 集群化 fsck（验收②）与 gc fail-closed——分区期不误删活数据`。

**切片二四闸** + e2e 冒烟：4 节点真进程（写→任意节点读一致→kill 一节点→读写继续→拉回→fsck 补齐）。

---

# 切片三：M3-3 锁与一致性收尾（Task 8/9 无依赖，可并行）

## Task 8: quorum 租约锁（节点域）

**Files:** Create: `crates/xl-store/src/store/lock.rs`；Modify: `store.rs`（lock_object:258-276）、put.rs/mpu.rs 写入口检查点

- [x] **Step 1（测试先行，in-process）**：
  - `lock_single_winner`：4 节点两写者抢同 key → 恰一方持有（Q_n=3）；
  - `live_lease_not_stolen_by_higher_epoch`：活租约 + 更大 epoch 竞争 → 拒；
  - `expired_lease_epoch_ordered`：过期后晚 epoch 胜、迟到的旧 epoch 不抢回；
  - `renew_quorum_intersects_acquire`（参数化 4/5 节点推演：两集必相交）；
  - `holder_paused_ttl_expiry_takeover`：持有者不续租 → TTL 后可取得；
  - `split_brain_rejects_writes`：仅 2 可达（<Q_n）→ acquire 重试耗尽 `QuorumUnmet`，**写被拒**；
  - `slow_write_keeps_lease`：注入超 TTL 的慢写 → 后台续租保锁、写成功。
- [x] **Step 2:** `lock.rs` 状态机（E 节：LockTable/try_grant/Renew/Release + 退避重试）；TTL 10s 可配。
- [x] **Step 3:** `lock_object` 接入：进程内 → flock → 集群锁；`LockGuard` 增 `lease_lost: Arc<AtomicBool>` 与续租任务句柄（Drop abort + Release 广播）；put/mpu 写入口 prepare 前 / commit 前各查一次。
- [x] **Step 4:** 提交 `feat(m3): quorum 租约锁（节点域、后台续租、脑裂拒写）——验收④⑤`。

## Task 9: 控制面读扇出

**Files:** Modify: `xl-s3/src/{auth.rs, access.rs}`、`oss-server/src/{router.rs, state.rs}`

- [x] **Step 1（测试先行）**：`ak_resolves_via_peer`（节点 B 建号/签 AK → 打节点 A 的 S3 → 200）；`permission_fanout`（权限行同样）；`peer_down_local_ok_remote_403`（B 掉线：A 本地凭据不受影响、纯远程凭据 403 fail-closed）。
- [x] **Step 2:** `SigV4Auth`/`Acl` 构造点加 `Option<(Arc<dyn MeshTransport>, String /*self_id*/)>`；miss 时：
  ```rust
  // 并发扇出全部 peers（futures 并集），首 Some 胜；全 miss → 按现状报错/拒
  // ControlQuery::AccessKey(ak) / Permission{uid,bucket,action} / ...
  ```
  服务端 `mesh_control_query` 实现（字面量 SQL，白名单 match）。
- [x] **Step 3:** 提交 `feat(m3): 控制面读扇出——同一 AK 任意节点可用（写仍本地，M9 收口）`。

## Task 10: 4 节点真进程 e2e 与混沌

**Files:** Create: `tests/e2e_m3_cluster.sh`；Modify: `README`、tracing 传播

- [x] **Step 1:** 脚本骨架（bash，与 e2e_s3.sh 同风格：彩色步标 + `set -euo pipefail`）：

```text
布局：RUN=$(mktemp -d)；每节点 $RUN/n{i}/{data0,data1,data2,oss.db,oss.toml}
配置：4× oss.toml（node_id=n{i}、addr=127.0.0.1:1910{i}、drive_count=3、nodes 四条同构、secret 统一）
      oss-server 端口 900{i}（9000–9003）——起服务前检查端口空闲（既有 e2e 教训）
启动：OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw 各起一进程 → 逐节点 curl /health/live 就绪
建号：节点 1 控制台 API 建 AK（控制面扇出后任意节点可用——Task 9 的实证点）
清理：trap EXIT kill 全部 pid + rm -rf $RUN
```

- [x] **Step 2:** 验收映射脚本段（断言与口径逐条写死）：
  - **①+③**：写 10MiB（任意节点）→ 4 节点各读 `cmp` 一致 → `kill -9` n4（剩 9 槽=恰好 quorum；**写零余量 = 临界**，README 注明）→ 写+读继续 → 拉回重启 → 读仍一致；
  - **②**：掉节点期间写入若干 → 拉回 → `oss-server fsck --repair`（打任一节点）→ 报告 `missing=0` 且缺键 meta 已补、抽查分片 CRC；
  - **④**：10 进程并发写同一 key ×100 轮 → 读回必为某次完整写（SHA 命中某轮内容）、无交错；覆写旧版本回收：远端槽位 trash 出现或经 clean 回收（留一断言）；
  - **⑤（三口径）**：`kill -STOP` n3+n4（剩 6 槽 <9、节点 2<3）→ ①写必须失败（**5xx**）；②同期 GET **亦不可达**——500 `CorruptMeta`（present=6≠0，**不是 404**，脚本注明「读同败为预期」）；③**时间预算**：SIGSTOP 进程不响应、每失败请求等满 RPC 超时，并发/重试轮次配预算防脚本假死。`kill -CONT` → 断言「写恢复 + 之前已提交对象读回一致 + fsck missing=0」；游荡 RPC 残骸孤儿=预期由 fsck/clean 收；
  - **⑤-positive**：`kill -STOP` **1** 节点（3/4 ≥ Q_n）→ **写必须成功**（多数派正测）。
- [x] **Step 3:** tracing：mesh 注入 `traceparent`、服务端提取（双节点日志同一 trace_id 抽查）。
- [x] **Step 4:** README 集群运行手册：配置样例、启动顺序、fsck 用法、**临界口径（停 1：写 9/9 全须成功、读 9≥8 余 1；停 2：GET=500 CorruptMeta 非 404）**、备选 6×2、**运维：每节点定期 `clean` + `fsck --repair`（远端孤儿只由该节点自己回收）**、**`fsck --repair` 不得与在途 MPU 并跑**、**M3 不支持单机原地升级为多节点**、配置反例 **4×4**（`shard % 16` 只落 0..11）、已知边界：MPU 粘节点、无 fencing、控制面读扇出、节点间无 TLS（内网假设）、**命名勿混淆**：oss-server 的 aetherupload `staging_dir`（config.rs:19）≠ store 的 `.oss.sys/tmp`。提交 `test(m3): 4 节点集群 e2e 与混沌段（验收①–⑤全覆盖）`。

## Task 11: 收尾

- [x] **口径注释对齐**：put.rs:448「已提交，M3 heal 补副本」、drives.rs:22-23「多 set 是 M3 的事」与计划口径统一（heal 命令→M4；验收②由 fsck 集群化承担；多 set→M4）。
- [x] 设计文档状态注记；feature-spec §19 对照更新；README.md:17 与 docs/assets/architecture.svg:4/:14 的「三 crate」文案 + README.md:44-48 crate 职责表补 xl-proto/xl-client 两行。
- [x] 提交 `docs: M3 落地后同步边界清单` + 本计划定稿记录。

---

## 四闸与验收（各切片收尾统一跑）

- fmt 无 diff · clippy `--workspace --all-targets -D warnings` 0 · `cargo test --workspace -j 2` 全绿 · 相关 e2e（单机 27 步零差异）+ 集群段（切片二起）。
- 设计 §M3 验收 5 条映射：

| 验收 | 覆盖（测试名/脚本段） |
|---|---|
| ① 4 节点拔 1 读写不中断 | `cluster_write_with_node_down_succeeds` / `cluster_read_after_node_down` + 脚本①段（kill -9；**4×3 下写零余量=临界**，README 注明） |
| ② 加回节点后 scrub 补齐 | `fsck_repairs_node_returned` + 脚本②段（fsck --repair 实弹） |
| ③ 任意节点读一致（哈希对拍） | `read_fast_path_cancels_straggler` + 脚本③段（4 节点 cmp） |
| ④ 并发写同 key 无交错 | `lock_single_winner` + 脚本④段（10 并发 ×100 轮） |
| ⑤ 分区下 quorum 不足拒写 | `split_brain_rejects_writes` + 脚本⑤段（SIGSTOP 2 节点：写 5xx；读同败=500 CorruptMeta 非 404；+ 1 节点多数派正测） |

- 二轮评审专项：部分翻转补偿 / hash 防伪 / 单调 mod_time / LIST 并集续页（Task 6）；gc fail-closed（Task 7）；续租与授予规则（Task 8）。

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| **4×3 单节点故障 = 写零余量（临界）** | 写死进计划与 README；客户端重试收敛；要余量用备选 6×2（12 槽，停 1 余 1） |
| tonic/prost 与 ecat 7 传递依赖冲突 | 复用 lock 既有版本（tonic 0.12.3 / prost 0.13.5，已在编译图）；冲突时以 lock 为准 |
| 锁无 fencing | 已知边界；后台续租 + 写入口两查；兜底=版本 uuid 隔离（孤儿回收延迟，数据不损坏） |
| meta 每槽一份 ⇒ 每 PUT 2×远端槽数条小 RPC（4×3 下 ≈18 条 prepare/commit） | 元数据小、与分片并行；备选（per-node meta + Q_n）记档，实测尾延迟超标再切 |
| 时钟漂移 | 数据裁决已脱钩（单调 mod_time）；epoch 仅用于过期替换定序；残余：TTL 判定用本机墙钟（M9 逻辑时钟） |
| GC/回收 | gc fail-closed（Task 7）；recycle 远端失败留孤儿 → 该节点 clean 按龄回收；MPU 暂存本机闭环 |
| MPU 会话粘节点 | 文档化；`fsck --repair` 清暂存区=杀在途 MPU（README 标注不得与在途 MPU 并跑） |
| in-process fixture 循环回填 | 构建后回填（Mutex<HashMap>）；**handler 不得再扇出**（死锁）注释写死 |
| SIGSTOP 分区模拟 | 语义等价（连接超时 vs 丢包）；脚本配时间预算；iptables 备选记档 |
| 集群配置错误 | open 校验 self 一致性 + slot_count；对端不符由 health 暴露（槽位解引用失败 → 该槽 down + warn） |
| 拓扑错位（data_dirs 顺序变化 = 槽位静默错位） | 首启写 `.oss.sys/topo` 指纹，不一致大声失败；README 写明 M3 不支持单机→多节点原地升级 |

## 定稿记录

### 切片一（M3-1 干线）定稿（2026-10-13）

- **提交**：`839a663`（槽位模型）→ `8144fa5`（xl-proto）→ `a11d7aa`（mesh 基座）→ `f2b8327`（xl-client）→ `cf774ac`（配置面与健康端点）。
- **四闸实跑**：`fmt --all` 净 · `clippy --workspace --all-targets -D warnings` **0** · `cargo test --workspace` 全绿（22 个测试二进制：xl-store 109+kill9、xl-client 2、oss-server 全套） · 单机 `e2e_s3.sh` **27 步零差异**（1–15h 全过，脚本零改动）。
- **双进程冒烟**（2 节点 × 6 盘 = 12 槽）：双方 `/health/cluster` 互认（slots:6）且 ready 200；`kill` n2 → n1 立即标 down + `/health/ready` **503**「可达节点 1/2 < 节点多数 2」；滚动启动探活只 warn（不拒启）。
- **偏差与笔记**：① `e2e_s3.sh` 需 `ADMIN_PASSWORD=pw`（库中 erik 口令由 Playwright 引导轮设定；脚本默认 `changeme` 是 README 首启口径——环境使然，非回归）；② 本机 9001 端口被**外部服务**占用，冒烟改 9051/9052（与代码无关）；③ 实现顺序微调：mesh.rs trait 随 Task 2 先落（ClusterConfig.transport 需要），tonic server + Health 落在 Task 3 Step 3 一次完成。

### 切片二（M3-2 数据面）定稿（2026-10-13）

- **提交**：`f77dd6c`（跨节点数据面与 meta 层，Task 5a/5b/6 核心）→ `273a7a7`（xl-client 补齐 ensure_bucket/list_bucket_dirs/list_local 接线）→ `e86a8ef`（Task 7：集群化 fsck + gc 保守门 + 读回退 + 拓扑指纹）→ 本记录。
- **四闸实跑**：fmt --all 净 · clippy --workspace `-D warnings` 0 · workspace 全绿（xl-store 125+kill9；xl-client 3 测含真 gRPC 往返映射覆盖）· 单机 `e2e_s3.sh` 27 步零差异。
- **4 节点真进程冒烟（切片二验收门）**：写 2MiB → 4 节点下载 `cmp` 逐字节一致；`kill -9` n4 → 写（1MiB）+ 读继续（9 槽=恰好 quorum）；拉回 n4 → `fsck --repair`（n4 视角差集检出缺键并补建）→ n4 读新旧两对象均一致；`/health/cluster` 四节点全 up。
- **实现要点与新教训**：
  - `write_meta_cluster` 三分支（集群 ≥Q_s+补偿 / 单盘 / 多盘 ≥1）落地；补偿含 DeleteMeta（无旧版本）与 prepare+commit 旧内容（有旧）两式。
  - read_meta 覆盖率快路径 + `read_meta_full` 回退一次（get_object/get_object_range 第 2 轮）。
  - **循环扇出教训（实锤）**：`mesh_read_meta` 初版调 `self.read_meta` → 集群下服务端再扇出 → in-process 测试死循环挂起。修复=就地本地扫描；「服务端 handler 绝不扇出」为硬约定（头注+测试注释多处写死）。
  - **冒烟逮住陈旧二进制**：tonic 侧三条接线晚于上次 build，真进程立刻报 `tonic mesh 未实现：list_bucket_dirs` → 500——**in-process 测试盖不到的 proto↔DTO 映射层缺口，只有真进程 e2e 能逮**（Task 10 的存在理由实证）。
  - 两枚延期微测已补齐（foreign .new 不计翻转 / full 收满全部源）。
- **偏差**：控制面读扇出（Task 9）未落地前，4 节点冒烟用「同 AK/SK 逐节点种入」过渡；Task 9 完成后此过渡即多余。

### 切片三（M3-3 锁与一致性收尾）定稿（2026-10-14）

- **提交**：`05c351b`（Task 8 quorum 租约锁）→ `b88f4d1`（Task 9 控制面读扇出 + traceparent）→ 本记录（含 `tests/e2e_m3_cluster.sh`）。
- **四闸实跑**（高负载下分 crate 串行）：clippy `-D warnings` 0（四 crate）· xl-store 130+kill9 · xl-s3 19/2/23 · xl-client 4 · oss-server 全套 · fmt 净 · 单机 `e2e_s3.sh` 27 步零差异。
- **4 节点真进程混沌 e2e 全绿**（`tests/e2e_m3_cluster.sh`，端口动态分配、各段带时间预算）：控制面扇出（AK 只在 n1 铸造 → n4 直用）→ ①③ 10MiB 四节点 `cmp` 一致 → ① `kill -9` n4 读写续跑（9 槽=恰好 quorum，临界口径）→ ② 拉回 `fsck --repair` 差集检出+补建+复检清零 → ④ 同键 10 并发 ×100 轮无交错 → ⑤ SIGSTOP 双节点：写 5xx、**读同败（500 CorruptMeta 非 404）**、`/health/ready` 503 → ⑤-positive 单停（3/4 多数）写必成 → 恢复读写一致。
- **实现要点**：lock.rs 授予规则（仅空闲/过期可授、活锁不抢、旧 epoch 不抢回、Renew 同口径 Q_n）+ 后台续租 + 写入口提交点前复查；ControlBackend 注入（判权复用 Acl）+ ControlMesh（并发、首答胜、失败落 warn）；traceparent W3C 注入/提取。
- **实弹教训**：① AK/SK 手拼连踩两发「≥16 位」400（AK 15 位、SK 15 位均为静默失败）——e2e 改服务端自动生成 + 铸造断言；② 裸 `wait` 把四个常驻 server 也等上（④ 挂死实锤）——只等 aws 子进程数组；③ `pkill -f` 自匹配自杀两次（模式串出现在自己命令行里）——改 `pkill -x`。
- **验收①–⑤映射**：全部 in-process 单测 + 本脚本实弹**双覆盖**（对应表见「四闸与验收」节；⑤ 含多数派正测）。
- **Task 11 收尾**：已随 `251a2b0`（扇出失败落 warn）与 `2ac9670`（全仓口径统一：注释两处+同款、README 五-crate 与 M3 进度、architecture.svg、设计文档 M3 落地注记与 ecat-lock 定稿、feature-spec §3.6/§19）完成——M3 计划闭环。

## 评审修订记录（2026-10-13，二轮）

- 事实核查（m3-factcheck）：行号引用逐一属实（stage_payload_multi:241-277、write_meta:504-577、read_meta:416-458、read_ec:236-262、repair_shards:303-350、lock_object:258-276、delete_object:589-629）；修正 189→188、「六处+N 处」、`/health/ready` 不存在、`config.rs` 属实、MPU 暂存 `drives[0]` 属实。
- 设计批判（m3-planreviewer）维度 1–8 全量并入：锁 quorum 分域（P0）· 提交 quorum+补偿+hash（P1）· 读快路径与覆盖语义 · 单调 mod_time · recycle 跨节点+先决链 · gc fail-closed · 锁三洞 · 桶三层语义 · delete meta 上抛 · **否决 4×4**（16 槽只落 0..11）· e2e ⑤ 三口径+多数派正测 · fsck 差集检测+M3 补建闭环 · LIST 并集与 `limit=max+1` 完备性 · 拓扑指纹 · mesh 三边界 · QuorumUnmet→500 · ShardWrite 服务端 CRC · 顺序重切（server 骨架回切片一、5 拆 6 并、pub mesh 面）。
