# M2b 存储层重构 实现计划（EC · 多盘 · 内联 · 断电一致性）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地设计文档 M2 的**存储层**部分——EC 8+4 纠删码 · 单机多盘 · 小对象内联（< 128 KiB 进 `xl.meta`）· 断电一致性复核 · fsck/scrub 起步——把「单盘直存」升级为「单机真·对象存储」。协议面（Multipart/Range/元数据/GC）已由 M2a 完成，本计划不碰。

**范围切分（三个可独立交付的切片，各自四闸）**：

| 切片 | 内容 | 关键验收 |
|---|---|---|
| **M2b-1 EC 单盘** | store.rs 拆分 → EC 编解码 → `xl.meta` 演进 → 单盘 EC 写/读 → 小对象内联 | 设计 §M2 验收 #4（2 KB 不产生 13 文件）、#5（翻转分片字节读报错） |
| **M2b-2 多盘** | `drives[]` + crc32c 对象→组映射 + quorum 写 + 拔盘容忍 + 断电一致性 | 设计 §M2 验收 #1（拔 2 盘读写照常）、#2（kill -9 无脏数据） |
| **M2b-3 运维面** | fsck/scrub 起步 + 桶级互斥 + flock 跨进程锁 + 收尾 | 设计 §4.4 锁承诺（M1–M2 flock）、既有 ponytail 注释收口 |

**明确不做（M3+ 边界）**：跨节点 gRPC mesh / 网络 quorum / 节点间锁 / rebalance / 读修复的网络侧 / 分层 / 事件总线。单机内的「拔盘→回插→scrub 重建」是 M2b-2 的范围；**跨节点**的同类能力是 M3。

---

## 执行前必读：四件事

1. **依赖已逐条核实（crates.io 现状，2026-10-12）**：
   - `reed-solomon-erasure = "6.0.0"`（设计 §2 指定；MIT）。已读源码核实 API：`galois_8::ReedSolomon::new(data, parity) -> Result<Self, Error>`（core.rs:445）、`encode(&self, shards: T) where T: AsRef<[U]> + AsMut<[U]>, U: AsRef/Elem`（core.rs:597，吃 `&mut Vec<Vec<u8>>` 与 `Vec<&mut [u8]>`）、`reconstruct(&self, slices: &mut [T]) where T: ReconstructShard<F>`（core.rs:680，缺失片以容器内 `None`/空表示——**容器形态以编译器为准**）。
   - `crc32c = "0.6.8"`（硬件加速 CRC32C + 软件回退；设计 §4.3 要求的分片校验和）。
   - **备选**：`reed-solomon-simd 3.1.0`（O(n log n) + SIMD，MSRV 1.82，维护中）——rs-erasure 6.0.0 树里带 parking_lot 0.11 / lru 0.7 等老依赖，若与现代树冲突或编译不过，切它，API 形态不同但顶层用法（encode/reconstruct）同类。判据写进风险表。
2. **五条铁律（设计 §4.2）本切片不得破坏**：① 元数据跟对象走盘（无中心库）；② `xl.meta` 唯一提交点，写序不可变（分片→候选 meta→rename→meta 覆盖→trash）；③ 展平 + 整对象一个目录（转正永远同盘 rename）；④ 小对象内联不经 EC（持久性由 meta 复制承担）；⑤ **分片位置只存拓扑下标 `(drive_idx, part, shard_idx)`，绝不存绝对路径**——rebalance 与换盘的前提。
3. **M1/M2a 数据兼容裁决：就地兼容，不迁移**。`Version.data_layout: Option<DataLayout>`——`None` = M1/M2a 旧形态（`part`/`parts` 单文件路径），`Some` = M2b EC 形态；读路径按字段分发，写路径 M2b 起一律新形态。**升级方向只保证「新解旧」**（旧代码读新对象会在 `part.N` 找不到文件——接受，发布与部署锁步）。golden 字节将再次变更（有新字段），按 meta.rs 既有「有意识更新」流程走。
4. **本机编译成本**：编译一次 ≈ 5–20 分钟（高负载时更久）。所有需要编译验证的臆测合并；每个 Task 的「确认失败」步骤在编译成本不可承受时可与实现合并为一次编译，但**必须在定稿记录中如实注明**（M2a 已有先例）。

---

## 文件结构（改动面）

```
crates/xl-store/src/
├── meta.rs            [改] Version 加 data_layout/inline（Option，前向兼容契约）；golden 更新
├── layout.rs          [改] shard 文件名编码（part.<part>.<shard>）；drives 感知的路径推导
├── store.rs           [拆+改] 569 行已超 500 上限：拆出 put.rs / read.rs；StoreConfig 加 drives/ec/inline 阈值
├── store/
│   ├── ec.rs          [新] EC 编解码（encode/reconstruct + CRC32C/SHA256 校验）
│   ├── drives.rs      [新] 盘集合、set 映射（§5.3）、quorum 判定、存活探测
│   ├── put.rs         [新] 提交协议（自 store.rs 迁入并加 EC/内联分支）
│   ├── read.rs        [新] get/range/head（EC 重建 + 校验）
│   ├── mpu.rs         [改] complete 走统一 EC 路径（暂存零件流读 → 编码 → 分片转正）
│   ├── gc.rs          [改] 多盘遍历；fsck 报告钩子
│   └── list.rs        [改] 多盘遍历（每 set 一份命名空间副本，枚举 set 0 即可——裁决见 Task 7）
crates/oss-server/src/
├── config.rs          [改] oss.toml：data_dirs（数组，data_dir 兼容单盘）；ec.data/parity；inline_threshold
├── cli.rs             [改] clean/groups 适配多盘
└── state.rs / aether.rs [改] StoreConfig 构造点
tests/e2e_s3.sh        [改] 拔盘/断电为人工长跑入口；既有 17 步回归不变（实况计数，含 11b/11c/11d 子步）
```

依赖：`reed-solomon-erasure = "6.0.0"`、`crc32c = "0.6"`（workspace 根声明；xl-store 引用）。

---

# 切片一：M2b-1（EC 单盘）

## Task 1: store.rs 拆分（纯搬家，零行为变化）

**Files:** Modify: `crates/xl-store/src/store.rs`；Create: `crates/xl-store/src/store/put.rs`、`store/read.rs`

- [x] **Step 1:** `put.rs`：迁入 `put_object` / `put_object_with_meta` / `put_object_stream` / `put_object_stream_with_meta` / `write_meta` / `read_meta`（`read_meta` 同时被 gc/mpu 用——迁走后需 `pub(in crate::store)` 或留在 store.rs；**裁决：read_meta/write_meta 留 store.rs**，其余迁 put.rs）。
- [x] **Step 2:** `read.rs`：迁入 `get_object` / `get_object_range` / `head_object` / `load_object_info`。
- [x] **Step 3:** store.rs 保留：`open`（自举/自检）、`root/lock_for/ensure_bucket`、桶操作、`delete/copy_object`、`now_nanos`。目标 store.rs ≤ 300 行。
- [x] **Step 4:** `cargo test -p xl-store` 全绿（7d64681；store.rs 576→267 行）（**行为零变化**是本 task 唯一验收）；提交 `refactor(xl-store): store.rs 拆分——put/read 迁出，pub 面不变`。

## Task 2: EC 编解码模块（`store/ec.rs`）

**Files:** Modify: `Cargo.toml`（workspace）、`crates/xl-store/Cargo.toml`；Create: `crates/xl-store/src/store/ec.rs`；Modify: `store.rs`（`mod ec;`）

- [x] **Step 1: 写失败的测试**（`ec.rs` 内 `#[cfg(test)]`）：

```rust
#[test]
fn ec_roundtrip_8_4_survives_up_to_4_losses() {
    let cfg = EcConfig { data: 8, parity: 4 };
    let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let shards = encode(&payload, cfg).unwrap();
    assert_eq!(shards.len(), 12);
    // 丢任意 ≤4 片可重建
    for lost in [[0usize, 3, 7, 11], [1, 2, 9, 10]] {
        let mut view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        for &i in &lost { view[i] = None; }
        assert_eq!(reconstruct(&view, cfg).unwrap(), payload, "丢 {lost:?}");
    }
    // 丢 5 片必败
    let mut view: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
    for i in [0, 1, 2, 3, 4] { view[i] = None; }
    assert!(reconstruct(&view, cfg).is_err());
}

#[test]
fn flipped_shard_byte_is_detected_by_crc32c() {
    let cfg = EcConfig { data: 8, parity: 4 };
    let payload = vec![7u8; 1024];
    let mut shards = encode(&payload, cfg).unwrap();
    shards[2][0] ^= 0x01; // 手工翻转
    assert!(shards_valid(&shards, cfg).is_err(), "翻转字节必须被 CRC32C 抓到");
}
```

- [x] **Step 2: 确认失败**（`encode` 未定义）。
- [x] **Step 3: 实现**。形态：

```rust
//! EC 编解码与分片校验。M2b 先整份进内存（ponytail: 大对象内存峰值为 payload + 分片；
//! M2b-3 换分块 encode —— rs-erasure 支持 ShardByShard 增量编码，升级路径已留）。

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EcConfig { pub data: u8, pub parity: u8 }   // 默认 8/4；可配 4+2 / 6+3 / 12+4

/// 返回 12 片（前 data 片为数据片），每片长度 = ceil(len/data) 对齐。
pub fn encode(payload: &[u8], cfg: EcConfig) -> Result<Vec<Vec<u8>>>;
/// 缺失片（None/空）用 EC 重建；片数与配置不符 → Err。
pub fn reconstruct(shards: &[Option<Vec<u8>>], cfg: EcConfig) -> Result<Vec<u8>>;
/// 逐片 CRC32C 校验（crate crc32c；存 meta 的值与实读比对由调用方传入）。
pub fn crc32c_of(shard: &[u8]) -> u32;
```

- [x] **Step 4:** `cargo test -p xl-store ec` 全绿；提交 `feat(xl-store): EC 8+4 编解码模块（reed-solomon-erasure 6.0.0 + crc32c）`。

## Task 3: `xl.meta` 演进（DataLayout / Inline）

**Files:** Modify: `crates/xl-store/src/meta.rs`

- [x] **Step 1:** 新字段（**只加 Option，前向兼容契约**；`sample()` 补 `data_layout: None, inline: None`）：

```rust
pub struct Version {
    …现状全保留（part/parts 成为旧形态的兼容位）…
    /// M2b：Some = EC 布局（本版本以分片为准）；None = M1/M2a 旧形态（读 part/parts）。
    #[serde(default)]
    pub data_layout: Option<DataLayout>,
    /// M2b：小对象内联字节（< inline_threshold 时数据就在 meta 里，不经 EC）。
    #[serde(default)]
    pub inline: Option<InlineData>,
}

pub struct DataLayout {
    pub data: u8,
    pub parity: u8,
    pub shard_size: u64,              // 每片定长（含末片补零对齐）
    pub shards: Vec<ShardRef>,
}
pub struct ShardRef {
    pub drive: u8,                    // 拓扑下标（铁律 ⑤；单盘恒 0）
    pub part: u16,                    // M2b 恒 1（多 part 分片的升级位与 parts 对齐）
    pub shard: u8,                    // 0..data+parity
    pub size: u64,
    pub crc32c: u32,
}
pub struct InlineData { pub bytes: Vec<u8> }   // 上限 inline_threshold，读取端复核
```

- [x] **Step 2:** decode 白名单扩展（inline ≤ MAX_INLINE=128KiB；
  data_layout 分片数须等于 data+parity——后者为评审式补强）：`data_layout.shards` 无 uuid 字段（纯下标），但 `inline.bytes` 长度必须 `<= MAX_INLINE`（防解码端被超大分配打爆——**信任边界**）。
- [x] **Step 3:** golden 摘要按流程重算（第三次：f28d2848…）；旧文件→None、超限/不自洽拒绝用例（连续第二个有意识变更）；补用例：旧文件（无新字段）解码 → None；新文件 + 旧解码器（FutureVersion 模式）→ 跳过。
- [x] **Step 4:** 提交 `feat(xl-store): xl.meta 增 DataLayout/Inline——EC 布局与内联的前向兼容字段`。

## Task 4: 单盘 EC 写路径

**Files:** Modify: `store/put.rs`、`layout.rs`（shard 文件名）、`store.rs`（StoreConfig）

- [x] **Step 1: StoreConfig 演进**：

```rust
pub struct StoreConfig {
    pub root: PathBuf,          // 单盘形态（drives 长度 1 时二者等价；M2b-2 引入 drives 后 root() = drives[0]）
    pub fsync: bool,
    pub ec: EcConfig,           // 新；默认 EcConfig { data: 8, parity: 4 }
    pub inline_threshold: u64,  // 新；默认 128 * 1024（设计铁律 ④）
}
```
（`StoreConfig::new` 保持既有签名，新字段给默认值——调用点零波及。**落地补强**：`open` 拒绝 `inline_threshold > MAX_INLINE`——超过的话写出的 meta 重启后会被自己的解码白名单拒收。）

- [x] **Step 2: 写路径分支**（`put_object_stream_with_meta` 的 staged 段）：

```rust
// 现流程：流式写 tmp/<uuid>/part.1。M2b 分叉：
if size < self.cfg.inline_threshold（实际上需边写边判，写完整份再分叉） {
    // 内联：不写 part 文件；Version.inline = Some(bytes)
} else {
    // EC：payload → ec::encode(cfg) → 逐片写 tmp/<uuid>/part.1.<shard>（每片 fsync）→
    //      Version.data_layout = Some(DataLayout{ …, shards: [ShardRef{ drive:0, part:1, shard:i, size, crc32c }] })
    // part 兼容位：part.uuid = uuid、part.part = 1、part.size = 总长、part.sha256 = 整对象 SHA256（其余读方依赖）
}
// 候选 meta → rename → 提交点，与现流程完全一致（铁律 ② 不动）
```

- [x] **Step 3:** 测试（`store/tests.rs` 追加）：① 1 MiB 对象 → 盘上 `find . -name 'part.*' | wc -l == 12`；② 读回字节一致；③ inline_threshold 边界（127 KiB 内联 / 129 KiB EC——**用 `-` 与 `+` 两例钉死 `>=` 还是 `>` 的分支语义**）。
- [x] **Step 4:** 提交 `feat(xl-store): 单盘 EC 写路径——12 分片落盘，提交协议不变`。（**与 Task 5 合并为一次提交**：读形态是写的验收前提，分开提交会让 81 测中的读用例在中间态全红——编译成本约束下的合并，遵 §1.4 如实注明。）

## Task 5: 单盘 EC 读路径（重建 + 校验）

**Files:** Modify: `store/read.rs`

- [x] **Step 1: 测试先行**：① 全片在 → 读回一致；② 删 1/2/4 片（`remove_file`）→ 读回一致；③ 删 5 片 → `Err`（信息含「分片不足」）；④ 翻转一片一个字节 → `Err`（CRC32C 抓到，**不得静默返回脏数据**——设计验收 #5）；⑤ 翻转后整对象 SHA256 兜底（CRC 挡不住的场景由 SHA256 收口）。（**落地改判**：④ 在 EC 冗余下纠正——坏片被 CRC32C 抓到后**视同缺失进入重建**，若存活片 ≥ data 则返回**正确的重建数据**（自愈读优于报错，且同样满足「不得静默返回脏数据」）；只有重建输入不足才 Err。⑤ 用 meta 移植（翻转字节 + 同步改掉 meta 里该片 CRC32C）实测 SHA256 终验拦下。）
- [x] **Step 2: 实现**（分支骨架）：

```rust
// get_object：v.data_layout 分支
//   Some(l) => {
//      读每片（drive 暂恒 0）：Ok(bytes) => crc32c 校验存 ShardRef 比对 → 不符 = 坏片（视同丢失，进 reconstruct 输入 None）
//      存活片 < data → reconstruct 必败 → Err(CorruptMeta 语义的「分片不足」变体，新增 Error::ShardsUnavailable)
//      重建 → 整对象 SHA256（PartRef.sha256）校验 → 不符 Err(CorruptMeta「校验和不符」)
//   }
//   None => 现路径（effective_parts 单文件/多段）
// ponytail: M2b 重建整份进内存；M2b-3 的 range 按需只重建命中块（rs-erasure ShardByShard 的分块能力）。
```

- [x] **Step 3:** `get_object_range` 同分支（M2b 先重建整份再切片，ponytail 注同上）；`head_object` 不变（只读 meta）。（**落地补充**：内联与 EC 的 `get_object_range` 分派同挂此处；内联/EC 都不满足「重读 meta 重试」的场景——全部分片缺失返回 `Ok(None)` → 重读一轮 → NoSuchKey，与旧单文件路径同语义。）
- [x] **Step 4:** 提交 `feat(xl-store): EC 读路径——存活片直读 + 重建 + CRC32C/SHA256 双校验`。（并入 Task 4 提交。）

**落地裁决（Task 4/5 合并轮）：**
1. **不采纳「收全再分叉」**：任务书写「需边写边判，写完整份再分叉」，但那样会取消 M2a 的**流式落盘**语义（`put_streams_without_buffering_whole_body` 握手测试与 README 承诺）。落地为「**流式写 part.1（不变）→ 收全后转形态**（内联读回/EC 编码后删 part.1）」。内存峰值仍只在 encode 一步（payload + 12 片），与风险表同口径；流式语义与测试全保。
2. **`Error::ShardsUnavailable` 不新增**：沿用 `CorruptMeta("分片不足…")`——convert.rs 已映射 5xx 泛化，新增变体只多一个无差别分支（YAGNI）。
3. **兼容位回填**：EC 版本的 `part.uuid/part.part/part.size/part.sha256` 按任务书写全（sha256 = 整对象 SHA256，读路径终验直接用）。
4. **内联的镜像性质**（测试钉死）：内联数据在 meta 里，版本目录被移走也照读——无跨文件竞态窗口；EC 对象则与旧路径同窗口（全片缺失 → 重试 → NoSuchKey）。
5. **信任边界补强**（评审式）：内联长度 ≠ 对象长度、EC 重建长度 < 对象长度 → `CorruptMeta` fail-closed（防手工构造 meta 触发 panic/越界切片）；`open` 拒绝超限 `inline_threshold`。

## Task 6: 小对象内联

**Files:** Modify: `store/put.rs`、`store/read.rs`

- [x] **Step 1: 测试先行**（设计验收 #4 的本地版）：2 KB 对象 PUT 后，对象目录内 `find` 计数 == 1（只有 `xl.meta`）；读回一致；覆写为 EC 尺寸再读（形态切换）；mpu 小对象同样内联（complete 分支）。（落地：内联 find 计数用例 = `put_then_head`（11 B）+ `inline_threshold_boundary`（127 KiB）+ `multipart_single_small_part_completes`（5 B，MPU 分支）；形态切换 = `inline_threshold_boundary` 尾段（同键 内联→EC→内联 覆写）；设计验收 #4 的「13 小文件」在 EC 侧 = 12 片（parity 计入）。）
- [x] **Step 2: 实现**：内联字节直接进 `Version.inline`；读路径 `inline.is_some()` 优先于 `data_layout`/parts。（已于 Task 4/5 轮随写读分派落地。）**注意**：内联不经 EC——单盘形态（M2b-1）下即单份，冗余由 M2b-2 的 meta 多盘复制提供（风险表记录此边界）。
- [x] **Step 3:** MPU `complete_multipart_upload` 收口：**统一裁决**——complete 时按顺序流读暂存 `part.N` 拼成整对象 → 走与单段 PUT 相同的「内联或 EC」落盘 → 暂存文件清理；`Version.parts` 字段保留供 M2a 旧对象读取，新写入不再使用（裁决记入定稿记录）。（落地：形态核心提取为 `put.rs::stage_payload`（单段 PUT 与 MPU complete 共用）；MPU 装对象 = `mpu.rs::concat_stream` 按声明顺序 64 KiB 块流式拼 `part.N`；完成提交后暂存区**整目录清理**（`mpu.meta` 生死门先改名 `mpu.meta.done`，删到一半崩溃的残骸由 `reap_stale` 按 mtime 回收）；`abort`/`list_parts` 在 complete 后仍报 `NoSuchUpload`（断言在测试里）。）
- [x] **Step 4:** 提交 `feat(xl-store): 小对象内联（< 128 KiB 进 xl.meta）；MPU complete 统一走 EC/内联`。

**落地裁决（Task 6 轮）：**
1. **complete 期间并发重传防护**：`upload_part` 不持对象锁（允许并发传不同 part——M2a 裁决不变），因此 complete 拼装出的字节数与声明的各段总和可能不一致（同一 part 被并发重传）。fail-closed：不一致 → `InvalidPart`（可重试语义），不提交不一致的 etag/长度。**评审补强（F2）**：仅比总长度不够——同长度不同内容会静默提交「数据≠etag」。改为 `concat_stream_verified` 在拼装中对每段做字节级核验（md5+sha256+长度 与 sidecar 快照全等），核验对象正是即将提交的字节，竞态在核验点收口；`multipart_complete_rejects_tampered_part_bytes` 用例钉死（含可重试性）。
2. **暂存区不再原地转正**：旧设计（staging 目录 rename 成版本目录）与「统一 EC/内联形态」不兼容；新流程 = 拼装进 `tmp/<新 uuid>` → rename → 提交点 → 清理暂存区。代价 = 一次全量数据双写（暂存 + 转换产物并存到提交点之后），换来形态统一；暂存区在 complete 成功后立即释放。
3. **旧 parts 形态回归用例**：`legacy_parts_form_still_readable`（手工构造 M2a meta + 多段 part 文件）钉死「升级不失读」。

### 评审一轮（切片一收尾前，评审者只读，7 项发现 → 4 修 1 记录）

- **F1（高）修**：EC 读的「半截视图」误报 500——分片逐个读，并发删除/覆写的中间态会呈现「前缀片在、后缀片没」，与真实损坏单轮内不可区分。修法：`read_ec(strict)`——非末轮「存活不足」返回 `Ok(None)` 走既有「重读 meta 一轮」；末轮：全片 ENOENT 且未读到坏片字节 → NoSuchKey；读到过任何坏片字节或存活不全但不空 → `CorruptMeta`（fail-closed）。附带：并发撕裂用例（`concurrent_puts_and_deletes…`）载荷 64 B→129 KiB——64 B 现在走内联、数据在 meta 里，该用例已退化为空转。
- **F2（中）修**：见裁决 1（逐段字节核验）。
- **F3（中）部分修 + 如实记档**：EC 读峰值 ≈ 4× 对象（shards 1.5× + reconstruct 克隆 1.5× + 拼接 1×）；已把「截断拷贝」改为 `truncate` + move（省 1×）；完整流式化（ShardByShard）维持「M2b-3 之后」。complete 峰值 ≈ 2.5× 对象，**单段 PUT 的 `max_put_bytes` 约束对 complete 不成立**（xl-s3 只限单 part），此边界补记入风险表。
- **F4（低）修**：`inline_threshold=0` 时 0 字节对象落入 EC → rs 拒收空片 500；修为 `size == 0` 恒内联 + 用例。
- **F5（低）修**：12 片全坏与全缺不可区分（全坏会 404）；随 F1 的 `bad_bytes` 信号一并修 + 用例钉死「全坏 = 损坏（500 谱系）」。
- **F6（低）修**：读路径核验 `bytes.len() == ShardRef.size`（不符视同坏片），片长字段不再形同虚设。
- **F7（低，M2a 遗留，本切片未加宽）**：`upload_part` 不持锁 vs abort/complete 的 `remove_dir_all` 窗口 → 裸 IO 500 而非 NoSuchUpload。记入遗留清单（M2b-2/3 或独立小修）。

**e2e 实弹（e16f481）**：`tests/e2e_s3.sh` 17/17 步全绿（脚本实况 17 步头 = 编号 1–14 含 11b/11c/11d 子步；此前文档里的「14 步/19 步」为计数漂移，以脚本实况为准），脚本零改动；额外人工探针：1024 B/127680 B → 内联（目录仅 xl.meta），131072 B/1 MiB → 12 片 EC，四种形态读回/ETag/Range/CopyObject 全对，6 路并发覆写无撕裂。

### 切片一收尾
- [x] 四闸：fmt · clippy 0 告警 · `cargo test --workspace -j 2` 全绿（151 测）· `tests/e2e_s3.sh` 17 步回归（S3 面行为不变；修复轮后复跑见下）
- [x] 控制台 e2e（四闸之外的本项目惯例）：Playwright 4/4 ×2 轮全绿；另用浏览器实弹传 300 KiB → `part.1.0..11` 12 片（8×38400B=307200）+ xl.meta，下载 sha256 逐字节一致——aether ingest 的 EC 写路径端到端成立
- [x] 提交切片总结（并入本文档定稿记录，见下）

---

# 切片二：M2b-2（多盘）

## Task 7: `drives[]` 与对象→组映射

**Files:** Modify: `store.rs`（StoreConfig/Store/root/open）、Create: `store/drives.rs`、`layout.rs`、`config.rs`（oss-server）

- [x] **Step 1: 设计裁决（写进代码注释）**：（已落地：`StoreConfig.drives`/`root()=drives[0]`；`set_index` 公式+测试留形不留行为；每盘完整命名空间+拓扑下标寻址；盘数裁决见 open——「enabled=false 仅单盘 / 单盘 EC 合法 warn / 2..11 盘 Err」，与原文的差异见 Task 7 落地裁决。）
  - `StoreConfig { drives: Vec<PathBuf>, … }`；单盘形态 = `vec![root]`；`root()` 返回 `drives[0]`（既有调用点零波及）。
  - **set 划分**：M2b 单机单 set = 全部盘（`sets.len() == 1`）；`set_index = crc32c(bucket + "/" + key) % sets.len()` 先落公式但取模恒 0——**为 M3 的多 set 留形不留行为**（设计 §5.3 的确定性哈希从第一天就在，rebalance 才需要 >1 set）。
  - 每盘持有完整命名空间（对象目录 + meta 副本）；EC 分片按 `ShardRef.drive` 分发到 `drives[i]` 下**同构路径**：`<drive>/<bucket>/<展平键>/<uuid>/part.1.<shard>`——换挂载点/换盘不影响寻址（铁律 ⑤）。
  - **启动校验（含单盘向上兼容裁决——评审补）**：`drives.len() < data + parity` 时不得无条件 Err——单盘/少盘是 M1/M2a 的既有部署形态，升级即破不可接受。裁决：`[ec] enabled` 可配（默认 true）——**enabled=true 且盘数不足 → 构造期 Err**（大声失败，设计 §4.3 的 8+4 需要 ≥12 盘位，测试用 12 个 tempdir）；**enabled=false → 单文件旧形态继续（新写仍走 `data_layout: None`），并 warn 提示无冗余**。README/迁移说明必须写清这条开关。
- [x] **Step 2:** 实现 `drives.rs`：`enum DriveState { Up, Missing }`、存活探测（写探针文件）、`shard_path(drives, &ShardRef, bucket, key, uuid)`、quorum 判定。（已落地为 `drives.rs`（`drive_index`/`drive_up`/`shard_path`/`drive_root`）+ put.rs 的 landed/prepared/flipped quorum；差异：存活探测未写独立探针文件，复用 open 建的 `.oss.sys` 信标——见落地裁决 2。）
- [x] **Step 3:** `oss-server/config.rs`：`data_dirs: Option<Vec<PathBuf>>`（有它用数组；否则 `data_dir` 单盘兼容）；`[ec] data/parity`、`inline_threshold` 可配；`state.rs` 构造点同步。（`[ec] enabled` 一并可配，见落地裁决。）
- [x] **Step 4:** gc/clean 多盘遍历（每盘各自的 `.oss.sys`）。（与 Task 8 合并为一次提交。）提交 `feat(xl-store): 多盘——drives[]、对象→set 映射（单 set）、分片按拓扑下标分发`。

**落地裁决（Task 7 轮）：**
1. **盘数校验的「升级不破」细化**（任务书原文「enabled=true 且盘数不足 → Err」会打断 M2b-1 已发布的单盘 EC 默认配置）：落为——单盘恒合法（enabled=true 时即 M2b-1 形态，warn「无冗余」；enabled=false 时即 M1 旧形态）；**多盘（≥2）且 < data+parity → Err**（半冗余误配大声失败）；enabled=false + 多盘 → Err（旧形态无多盘语义）。
2. **盘存活探测是真需求不是仪式**（测试暴露）：挂载点掉线后挂载点**目录**仍在父文件系统上，`create_dir_all` 会把数据盲写进根盘且误报成功。`drives::drive_up` = 检查该盘 `.oss.sys` 存在（`open` 时建的探针信标，不再写额外探针文件）；多盘落盘、meta 准备、读时修三处落盘前必探。
3. **桶命名空间镜像**（测试暴露）：`create/delete_bucket` 在各盘同建同删（半途失败回滚已建）；`ensure_bucket/bucket_exists` 任一在册盘在即视为存在。列举仍以 drive0 为枚举源（ponytail: drive0 掉线时列举降级；M3 多 set 时升级）。
4. **`set_index` 留形不留行为**：公式与测试落地，实现中未接（单 set 恒 0）；`#[allow(dead_code)]` 标注。

## Task 8: quorum 写与拔盘容忍

**Files:** Modify: `store/put.rs`、`store/read.rs`、`store/drives.rs`

- [x] **Step 1: quorum 裁决（写进注释）**：写成功条件 = **成功落盘分片数 ≥ data + 1**（即至少 9/12；可容忍 parity−1=3 片并发失败；已成功片数在 [9,12] 之间即提交）。低于阈值 → 回滚（删已写片）→ `Err(Error::QuorumUnmet)`。读侧容忍 `≤ parity` 片缺失并重建。
- [x] **Step 2:** 测试（拔盘模拟 = 把某 drive 目录替换为不可写/移走）：全部落地于 `multi_drive_pull_two_then_five_drives` 与 `multi_drive_read_repairs_missing_shard`。
- [x] **Step 3:** meta 复制：每盘的 `<uuid>` 候选 meta 与提交后 meta 在各盘对象目录 **各自写一份**；读 meta 取任意可用盘，**多盘不一致时以 `mod_time_nanos` 最新者胜**（设计铁律 ② 的读侧，M3 的 heal 前置形态）。落地为**两阶段提交**（见下述裁决 1），非「尽力而为计数」。
- [x] **Step 4:** 提交 `feat(xl-store): 写 quorum（≥ data+1）与拔盘容忍——读重建 + 读时修`。（与 Task 7 合并为一次提交。）

**落地裁决（Task 8 轮）：**
1. **meta 提交 = 两阶段**（任务书「meta quorum = 全部在册盘；单盘失败不阻提交——与分片 quorum 同阈值」在「部分翻转后回滚」上语义自相矛盾——commit point 翻转不可逆。两阶段消解）：阶段一全盘写 `xl.meta.new`+fsync，**准备数 ≥ data+1 才进入翻转**；不足 → 清 `.new`（一盘未翻转）→ `QuorumUnmet` → 数据安全回滚。阶段二 rename 近乎瞬时，失败仅 warn（≥1 翻转即已提交；全失败仍属未提交，调用方回滚）。
2. **多盘写协议没有跨盘 rename**：数据分片直写各盘版本目录（`<drive>/…/<uuid>/part.1.<i>`），meta 翻转即各盘提交点；单盘流程（tmp→rename→write_meta）逐字节不变。
3. **读时修**：重建成功后 best-effort 回写缺失/坏片（含存活探测；不做 quorum 校验、无后台 heal——M4 补）。
4. **内联多盘 = meta 复制即数据复制**：连版本目录都不落（数据纯在 meta 里）——`multi_drive_inline_replication_survives_all_but_one_drive` 钉死「只剩 1 盘仍可读」，收口 M2b-1 记录的「单盘内联无冗余」边界。
5. **拔盘即探测**：见 Task 7 裁决 2——读时修同样先探（否则会把「拔走」的盘原地重建，测试曾实际复现）。

## Task 9: 断电一致性（目录 fsync 复核 + kill -9）

**Files:** Modify: `store/put.rs`（fsync 序）、`store/tests.rs`、`tests/`（进程级用例）

- [x] **Step 1: 复核现有假设（代码注释已明确挂着）**：「ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可；**没有目录 fsync**」。M2b 处置：`fsync=true` 时在**两处 rename 之后**补目录 fsync（封装 `sync_dir(path)`：`File::open(dir)?.sync_all()`，两处 = tmp→对象目录、meta.new→meta）；并在文档标注：目录 fsync 在部分文件系统上返回 EINVAL（忽略并 warn）——物理保证需按盘上文件系统复核。（**落地扩展**：多盘直写路径在每片 fsync 后补版本目录 fsync——分片目录项须先于 meta 翻转持久；`stage_payload` 收尾补 tmp 目录 fsync。）
- [x] **Step 2: kill -9 用例**（集成测试形态）：`crates/xl-store/tests/kill9.rs`——同一测试二进制两段子进程重入：段 1 写 v1 正常退出；段 2 以 `OSS_TEST_ABORT_AFTER_SHARD=8` 写 v2、第 8 片落盘后 `std::process::abort()`（SIGABRT）。父进程断言：子进程死于信号 6、重启后 **v1 可见 / v2 无影**、`reap_transient` 清空 tmp、半提交分片被 `reap_stale` 回收且回收后 v1 仍可读。（比任务书的 oss-server 子进程版更直接：绕开 HTTP 层，注入点即提交点前的最后一步。）
- [x] **Step 3:** 把设计验收 #3（rclone 并发 100×100 MB + 中断重试）写为 `tests/e2e_s3.sh` 的可选长跑段（`E2E_LONG=1` 门控，默认跳过——高负载机器上分钟级）。**已由 e2e 子代理落地**（13b 段：生成 N×MB → 慢传中断 kill -9 → 并发续跑 → 逐对象 md5 比对；微缩版 8×8MB 实跑通过，默认 100×100 MB 人工触发）。
- [x] **Step 4:** 提交 `feat(xl-store): 目录 fsync 复核 + kill -9 断电用例；长跑并发验收入 e2e 门控段`。（与 Task 7/8 合并为一次提交；Step 3 见上。）

### 切片二收尾
- [x] 四闸 + e2e（12 盘 tempdir 真服务冒烟 ×2 轮：9dc7070 与终版 5e91fe4 各一次，17 步全绿、零行为差异、脚本零改动；E2E_LONG 段落地（13b，微缩版实跑通过，默认跳过）；终版加验三项：`fsck`/`fsck --repair` 实跑（报告 JSON + 盘上回写证据）、拔盘 rm fail-closed（3×500 QuorumUnmet + 插回即通）、拔盘 ListObjects 完整）

---

# 切片三：M2b-3（运维面）

## Task 10: fsck/scrub 起步 + 锁收口

- [x] **Step 1: fsck**：`oss-server fsck [--repair]` 与启动自检共用口径：扫全盘（每 drives[i]）——(a) tmp/trash 条目计数（清理由 clean 负责，fsck 只报数）；(b) 孤儿版本目录（未被任何版本引用）；(c) EC 分片 CRC32C 全检（缺/坏计数）。`--repair`：删孤儿 + 对缺/坏对象走一次 get_object（触发读时修）。报告 JSON 落 `.oss.sys/fsck/<时间戳>.json`（xl-store 出结构体，serde_json 序列化在 oss-server cli——内核不引新依赖）。**裁决**：独立 `scrub` 仍归 M4。
- [x] **Step 2: 桶级互斥**：写者「桶窗口」（桶锁内 ensure + 对象目录先落）与 `delete_bucket`/`create_bucket`（全程桶锁）互斥——删桶见非空必拒、窗口外的迟到写者撞 NoSuchBucket，「桶复活」窗口关闭；回归（删除）为竞态用例（30 轮断言 put 与 delete_bucket 不可能同时成功）。
- [x] **Step 3: flock 跨进程锁**：`lock_object` = 进程内 tokio 互斥 → `<drive0>/.oss.sys/locks/<sha256(bucket/key)[..16]>.lock` 的 `File::lock()`。**落地修正**：flock 是阻塞调用，放进 `spawn_blocking`——直接在内联 await 里调用会 parked 执行器线程，多任务争用时可饿死持锁任务（实测挂死，用例 1.68s 转绿即为证）。用例：两个 Store 实例并发写/删同键，读只见完整写或 NoSuchKey。
- [x] **Step 4:** 提交 `feat(xl-store): fsck --repair 起步、桶级互斥、flock 跨进程锁`。（与 M2b-2 评审修复轮合并提交。）

**评审二轮（M2b-2，评审者只读，8 项发现 → 6 修 2 记录）——全部落地：**

1. **R1（高）盘身份/初始化**：原 open 无条件自建 `.oss.sys`，把「挂载点掉线的空目录」自证为在册盘（写入落父 fs、假 quorum）。修：首启（全盘无 sys）才初始化并盖部署标识 `.oss.sys/drive-id`；已有在册盘时缺席盘**不创建**、按不在册处理（插回自动重新在册，无需重启）；标识不一致（插错盘）→ 大声失败。盲区如实记录：首启时全盘同时掉线不可与全新盘区分（部署文档要求首启确认挂载）。
2. **R2（高）删除无 tombstone**：拔盘时 delete 静默成功 → 插回复活（内联对象 200 已删字节、EC 对象永久 500）。修（tombstone-lite）：delete 前逐盘探测，缺席即 `QuorumUnmet` fail-closed。另把 delete 改为**meta 先行**（删 xl.meta = 可见性提交点，数据清理随后 best-effort）——与写协议对偶，同时关闭「并发删/写时读到 CorruptMeta」的裂口（并发撕裂用例在 EC 载荷下暴露后收敛）。
3. **R3（中）列举只看 drive0**：拔 d0 时 List 误报 NoSuchBucket/空。修：collect_keys/list_buckets 各盘取并集。
4. **R4（中）配置面无校验**：重复 data_dirs（quorum 按条目假达成）、空数组越界 panic、ec 0 值、u8 相加溢出。修：state.rs 去重+非空；open 校验 data/parity ≥1 且和 ≤255（u16 运算）。
5. **R6（低）read_meta 单盘 IO 错误上抛**：降级为跳过该盘（仍保留「全盘无副本且见过错误 → fail-closed」）。
6. **R7（低）.new 残骸**：翻转全失败/半途遗留的 `xl.meta.new` 无人清。修：flipped==0 分支 clear_meta_new + reap_stale 按 mtime 回收对象目录内的 .new。
7. **R5（记录）**：读时修可能把已删/已覆写版本的缺片写回（uuid 不复用 → 不会污染新版本；残骸由 GC 回收）。接受，M4 heal 收口。
8. **R8（记录）**：「mod_time 最新者胜」依赖墙钟单调（NTP 回拨 + 翻转失败组合可让旧副本胜出，表现为已提交对象 404；读不核 quorum 的边界同条）。**写入设计文档为已知边界**；正解 = per-key 单调代数（M3 heal/epoch 顺延）。
9. **kill9 加固**（评审专节）：注入点扩为四处——分片半写（段 2）、meta 准备完成未翻转（段 3）、翻转中途（段 4）——覆盖两阶段提交的两个核心崩溃窗口；补显式证据断言（v2 版本目录存在、.new 存在、翻转中途崩后 v2 必须胜出）。
10. **实测挂死修复**：flock 内联阻塞调用 parked 执行器线程（并发用例挂死 15 分钟+）→ 挪入 `spawn_blocking`；相关竞态用例关 fsync（与锁语义无关的提速）。

## Task 11: 文档与收尾

**Files:** `README.md`、`docs/superpowers/specs/2026-10-10-minio-clone-design.md`（状态注记）、`docs/superpowers/specs/2026-10-10-feature-spec.md`（§3.2 EC 重建措辞、阶段表）、本计划

- [x] README：架构段加「单机多盘 + EC 8+4」；已知限制更新（多盘运维边界、首启确认挂载、拔盘删除 fail-closed、mod_time 边界、目录 fsync 物理假设、`E2E_LONG` 门控）；`oss.toml` 示例补 `data_dirs`/`[ec]`；运维命令加 `fsck`。
- [x] 设计文档：M2 行的「已落地」标记（验收 5 条逐条映射 + 实现边界）；§4.4 锁表更新为「已实现」（flock + 桶级互斥）；盘布局注释同步（locks/drive-id/fsck）。功能规格：`fsck` 行与阶段总对照同步。
- [x] 提交 `docs: M2b 落地后同步 README/设计文档边界清单`。（23563a1。）

### 切片三收尾
- [x] 四闸 + 全量 e2e：Task 10 已并入切片二终版（5e91fe4）并过全部闸（172 测 + clippy 0 + 12 盘 e2e 17/17 + fsck 实跑 + 拔盘加验）；切片三本体为纯文档（23563a1），fmt 无 diff、workspace clippy 0 复跑通过，代码未动故 e2e 结论沿用 5e91fe4 轮
- [x] 定稿记录：见下（三个切片的四闸实跑、设计 §M2 验收映射、全部裁决与偏差）

---

## 四闸与验收（各切片收尾统一跑）

- fmt 无 diff · clippy 0 告警 · `cargo test --workspace -j 2` 全绿 · 相关 e2e（控制台 Playwright + `tests/e2e_s3.sh`）
- 设计 §M2 验收的自动化映射：
  | 设计验收 | 本计划覆盖 |
  |---|---|
  | #1 拔 2 盘读写照常 | Task 8 的移盘用例（本地版；e2e 长跑可加真挂载点） |
  | #2 kill -9 无脏数据/无孤儿 | Task 9 的提交点注入用例 + 启动自检 |
  | #3 rclone 并发 100×100 MB | `E2E_LONG=1` 门控长跑（人工触发） |
  | #4 2 KB 不产生 13 小文件 | Task 6 的 find 计数用例 |
  | #5 翻转分片字节读报错 | Task 5 的 CRC32C/SHA256 用例（含翻转走 CRC 与走 SHA256 两条路） |

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| rs-erasure 6.0.0 老依赖（parking_lot 0.11/lru 0.7 等）与现代树冲突或编译不过 | **已实测结案（Task 2）**：6.0.0 及其老树（ahash 0.7/parking_lot 0.11/lru 0.7/instant/libm）在本工作区 2024 edition 工具链下编译通过，**不切** simd 备选（备选与判据保留供将来参考） |
| EC 全内存（写：payload+12 片；读：重建整份） | ponytail 注释明示；`inline_threshold` 与「单次 PUT ≤ max_put_bytes」共同限定峰值；M2b-3 之后（M2c/M3）用 `ShardByShard` 分块 encode/decode + range 按需重建。**评审量化（F3）**：读峰值 ≈ 4× 对象（已用 truncate+move 省 1×）；**MPU complete 峰值 ≈ 2.5× 对象且不受 `max_put_bytes` 约束**（xl-s3 只限单 part）——P 级大对象的 complete 是已知悬崖，优先随 ShardByShard 收口 |
| 写 quorum 语义（≥ data+1）在并发失败下的回滚复杂性 | 回滚 = 删已写片目录（未提交前不可见，天然安全）；用例覆盖 8 片成功回滚 |
| 目录 fsync 在非 ext4/XFS 上语义不一（EINVAL 等） | 忽略并 warn；文档标注「断电零丢失需按文件系统复核」 |
| golden 字节第二次变更（data_layout/inline 入编码） | meta.rs 既有「有意识更新」流程；摘要常量 + 注记 |
| 旧代码读 M2b 新对象会找不到 part.N | 方向契约明确：只保证「新解旧」；发布/部署锁步（升级即全量新写） |
| 单盘形态的 inline 无冗余（meta 只有一份） | 声明为 M2b-1 边界；M2b-2 的 meta 多盘复制落地后冗余成立 |
| 12 盘起步的测试环境成本（tempdir × 12/用例） | 用例用 12 个 `tempfile::tempdir()`（轻量）；真服务 e2e 用 12 个子目录 |
| MPU 与新布局的兼容（parts 字段的存废） | **已结案（Task 6）**：新写入统一 EC/内联，`parts` 降为只读兼容位；`legacy_parts_form_still_readable` 回归用例钉死；complete 并发重传 fail-closed |
| `set_index` 公式在单 set 下恒 0（为 M3 留形） | 注释明示「留形不留行为」；M3 引入多 set 时公式行为随拓扑生效 |
| **R8：meta「最新 mod_time 者胜」依赖墙钟单调**（NTP 大步回拨 + 翻转失败的组合可让旧副本胜出；读不核 quorum，只采纳达副本） | 已知边界，写入设计文档（Task 11）；正解 = per-key 单调代数（与 M3 heal/epoch 一并） |
| **R5：读时修可能把已删/已覆写版本的缺片写回**（uuid 每版本全新、不复用 → 不污染新版本；残骸由 GC 按 mtime 回收） | 接受为 M2b-2 边界；M4 heal 任务收口 |
| **首启盲区：全盘同时掉线不可与全新盘区分**（R1 初始化路径） | 部署文档要求首启确认各挂载点已挂载（Task 11 README） |
| flock 阻塞调用的执行器风险 | 已验证：入 spawn_blocking（内联实测挂死）；跨进程争用等待发生在阻塞池 |

## 定稿记录

### 切片一（M2b-1，单盘 EC + 内联）——2026-10-12

- **提交序列**：Task 1 拆分（7d64681）→ Task 2 EC 模块（9bfd57e）→ Task 3 meta 扩展 + golden 第三次重算 f28d2848…（c70887c）→ Task 4+5 写读路径 + 内联（55a206d，合并提交理由见 Task 4 Step 4 注）→ Task 6 MPU 收口（e16f481）→ 评审修复轮（F1–F6）。
- **四闸**：fmt 无 diff；clippy `-p xl-store --all-targets -- -D warnings` 0 告警；`cargo test --workspace -j 2` 全绿（151 测，含评审轮新增 3 例）；`tests/e2e_s3.sh` 17/17 步 ×2 轮（e16f481 与修复轮 be10706 各一轮，行为零差异；e2e 子代理实弹，脚本零改动，四种形态人工探针 + inline↔EC 12 轮覆写并发读 40 次无撕裂）；控制台 Playwright 4/4 ×2 轮 + 浏览器 300 KiB 上传 EC 探针（aether ingest 端到端）。
- **与计划差异**（均有落地裁决段详述）：不收全再分叉（保流式语义）；不加 ShardsUnavailable；Task 4/5 合并编译轮；Task 6 改为「拼装 → 新目录」并加逐段字节核验；评审轮 F1/F2/F4/F5/F6 修复。
- **遗留**：F7（upload_part 无锁窗口，M2a 已存在）；complete 的 P 级内存悬崖（随 ShardByShard）；README 存储形态说明待 M2b-2（多盘真冗余落地）后一并更新——单盘 EC 无冗余收益，提前宣传会误导。
- **下一步**：切片二（M2b-2 多盘：Task 7–9，含 `[ec] enabled=false` 单盘兼容裁决的落地）。

### 切片二（M2b-2，多盘 + quorum + 断电 + 运维锁）——2026-10-13

- **提交序列**：Task 7+8+9 核心（9dc7070：drives[]/直写 quorum/两阶段 meta 提交/读重建+读时修/目录 fsync/kill9）→ Task 10 + 评审二轮 R1-R7（5e91fe4：fsck、桶锁、flock、盘身份、删除 fail-closed+meta 先行、列举并集、配置校验、挂死修复、kill9 扩四注入点）。
- **四闸**：fmt 无 diff；clippy `--workspace --all-targets -D warnings` 0 告警；`cargo test --workspace -j 2` 全绿（172 测）；12 盘真服务 e2e 17/17 ×2 轮 + 三项加验（fsck 实跑、拔盘删除 fail-closed、拔盘列举完整）+ E2E_LONG 段落地（微缩实跑通过）。
- **关键裁决**（均有专节详述）：盘数校验「单盘恒合法、多盘须 ≥ data+parity、enabled=false 仅单盘」；meta 两阶段提交取代计数式；quorum = data+1；读侧最新 mod_time 者胜；内联多盘 = meta 复制；delete meta 先行（可见性提交点）；盘身份 = `.oss.sys/drive-id` 部署标识；flock 入阻塞池。
- **实测修复**（测试/评审暴露）：读时修缺存活探测、桶操作只看 drive0、flock 执行器饿死挂死、put 桶窗口间隙被 delete 抢先、kill9 目录计数含 meta 文件。
- **遗留**：R5/R8/首启盲区入风险表；E2E_LONG 默认参数（100×100 MB）人工触发；README/设计文档同步归 Task 11。
- **下一步**：切片三（Task 11 文档收尾；Task 10 已完成并入 5e91fe4）。

### 切片三（Task 10 并入切片二；Task 11 文档）——2026-10-13

- **提交**：23563a1（docs：README / 设计文档 / 功能规格 / oss.toml 四件套同步）。
- **四闸**：纯文档变更——fmt 无 diff、workspace clippy 与测试在 5e91fe4 上已全绿且代码未动，e2e 结论沿用（12 盘 17/17 + fsck 实跑 + 拔盘加验）。
- **设计 §M2 验收五条全部闭合**（映射见设计文档 M2 节；#3 的长跑段已落 `E2E_LONG` 门控、微缩版实跑通过、默认参数人工触发）。
- **M2b 整体收束**：单盘（M2b-1：EC 8+4 落盘/重建 + 小对象内联）→ 多盘（M2b-2：drives[]/quorum/两阶段 meta 提交/拔盘容忍/读时修/断电用例/fsck/锁）→ 文档。全部裁决、偏差、评审两轮（M2b-1 F1-F7、M2b-2 R1-R8）与遗留均已入档。**M2 阶段（M2a + M2b）完成**；后续按设计阶段表进入 M3（分布式）。
