# M2a 单盘可用性切片 实现计划

> **历史文档（2026-10-23 注）**：本文件记录当时的计划与边界。部分“未实现/非目标/后置”
> 已在 2026-10-23 能力清理批次或更早落地；以实现代码、README 与
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md` 为准。


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 M1 明确的用户面缺口补上——`x-amz-meta-*` 元数据往返、Range GET、流式 PutObject、孤儿/垃圾回收（启动自检 + clean 扩展）、Multipart Upload——让 `aws-cli`/`rclone` 的大文件默认路径（≥8 MiB 自动 multipart）与断点重试的原生习惯直接可用。

**范围切分（与设计文档 M2 的关系）**：设计文档的 M2 把「存储层重构」（EC 8+4、多盘、小对象内联、Shards/Part 布局）与「协议面补齐」（Multipart、Range、元数据、GC）放在同一阶段。本计划**只做协议面与单盘运维**，存储层重构显式留给 **M2b**（`xl.meta` 的 `DataLayout/Shards/Part` 是 M2b 的形态；本计划的 parts 表示是单盘、无分片的过渡形态，落盘演进预留见 Task 5）。

**Architecture:** 继续三 crate 单向依赖。改动面：`xl-store`（put 签名加元数据、Range 有界读、`store/mpu.rs` 新模块、`store/gc.rs` 新模块、`meta.rs` 加 `parts: Option<Vec<PartRef>>`）、`xl-s3`（Service 五处新方法 + get/put 改造）、`oss-server`（aether ingest 调用点、cli clean 接线）。不新增依赖。

**Spec:** `docs/superpowers/specs/2026-10-10-feature-spec.md` §3.2（Range）、§3.6 附近 multipart 小节（InvalidPart / InvalidPartOrder / EntityTooSmall / 末段 5 MiB 例外）、§18 clean；设计文档 §4（提交协议五铁律）、§5.1（xl.meta 前向兼容规则）。

---

## 执行前必读：三件事

1. **s3s 0.17 的 DTO/ErrorCode 已逐条核实**（来源：`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/s3s-0.17.0/`），本计划引用的字段名如下——若与编译器不符，**以编译器为准**：
   - `GetObjectInput.range: Option<s3s::dto::Range>`（`src/dto/range.rs`，枚举 `Int{first,last:Option}` / `Suffix{length}`，自带 `parse` / `check(full_len) -> Result<std::ops::Range<u64>, RangeNotSatisfiable>`；`RangeNotSatisfiable: Into<S3Error>` = `InvalidRange`）。
   - `GetObjectOutput.content_range: Option<ContentRange>`（`ContentRange = String`，generated.rs:3266）；`metadata: Option<Metadata>`，`Metadata = Map<String,String>`（`Map<K,V> = std::collections::HashMap`，dto/mod.rs:47-48）。
   - `PutObjectInput.metadata: Option<Metadata>`；`PutObjectOutput.e_tag: Option<ETag>`。
   - Multipart（generated.rs 字段）：`CreateMultipartUploadInput{ bucket,key,content_type,metadata,.. }` → `CreateMultipartUploadOutput{ bucket,key: Option, upload_id: Option<MultipartUploadId>, .. }`；`UploadPartInput{ bucket,key,upload_id: MultipartUploadId, part_number: PartNumber(=i32), body: Option<StreamingBlob>, content_length: Option<ContentLength(=i64)>, .. }` → `UploadPartOutput{ e_tag: Option<ETag>, .. }`；`CompleteMultipartUploadInput{ bucket,key,upload_id, multipart_upload: Option<CompletedMultipartUpload{ parts: Option<CompletedPartList(=Vec<CompletedPart>)> }>, .. }`，`CompletedPart{ part_number: Option<i32>, e_tag: Option<ETag>, .. }`；`AbortMultipartUploadInput{ bucket,key,upload_id,.. }`；`ListPartsInput{ bucket,key,upload_id,max_parts: Option<MaxParts(=i32)>,part_number_marker,.. }` → `ListPartsOutput{ parts: Option<Parts(=Vec<Part>)>, is_truncated, .. }`，`Part{ part_number: Option<i32>, e_tag: Option<ETag>, size: Option<Size(=i64)>, .. }`。
   - Trait 方法均已是**带默认实现**的 `async fn`（默认 `Err(s3_error!(NotImplemented, ...))`，见 `src/s3_trait.rs:100/269/4722/5052/8178`），实现即覆盖，签名统一为 `async fn x(&self, req: S3Request<XInput>) -> S3Result<S3Response<XOutput>>`。
   - 非 200 响应：`S3Response::with_status(output, StatusCode::PARTIAL_CONTENT)`（`src/protocol.rs:208`）。
   - 错误码齐备：`S3ErrorCode::{EntityTooSmall, InvalidPart, InvalidPartOrder, InvalidRange, NoSuchUpload}`（`src/error/generated.rs:434/731/737/761/1078`）。
2. **本机编译极慢（≈20 分钟一次 `cargo check`）**。所有需要编译验证的臆测**合并到一次探头**：本计划已把签名写实；开工前先跑一次 `cargo check --workspace` 确认基线，之后按 Task 节制编译次数，评审/收尾轮聚合验证。
3. **铁律提醒**（设计 §4.2 五条，本切片不得破坏）：
   - `xl.meta` 仍是唯一提交点（MPU 的数据在 part 上传时已进对象目录但**未被 meta 引用 → 不可见**，complete 写 meta 仍是唯一可见性翻转；仍走 `write_meta` 的 `xl.meta.new` + rename）。
   - 客户端可控字符串进路径前必须校验：**`upload_id` 必须 `Uuid::parse_str` 通过**（防 `../` 穿越）；part 号 `1..=10000`。
   - `xl.meta` 演化只加 `Option` 字段（本计划 Task 5 加 `parts: Option<Vec<PartRef>>`）。
   - 不新增依赖；`ponytail:` 注释标简化天花板。

---

## 文件结构（改动面）

```
crates/xl-store/src/
├── meta.rs            [改] Version 加 parts: Option<Vec<PartRef>>；encode/decode 复用
├── meta_tests.rs?     （不新增；测试在 meta.rs 内既有 tests 模块）
├── store.rs           [改] put_object/put_object_stream/copy_object 签名加 user_metadata；
│                           get_object 走 version_parts；新增 get_object_range；
│                           open() 增加启动自检调用
├── store/
│   ├── list.rs        （不动）
│   ├── tests.rs       [改] 既有调用点补参数 + 新用例
│   ├── mpu.rs         [新] Multipart 暂存区：create/upload_part/complete/abort/list_parts
│   └── gc.rs          [新] reap_transient（启动）/ reap_stale（clean 按 mtime）/ SweepReport
crates/xl-s3/src/
├── service.rs         [改] put_object 流式 + metadata；get_object Range/206/metadata；
│                           head_object metadata；copy metadata 携带；新增 5 个 multipart 方法
└── convert.rs         [改] store Error 新变体（NoSuchUpload/InvalidPart/EntityTooSmall/InvalidPartOrder
                           的映射——或走 s3s::s3_error! 直报，见 Task 5 裁决）
crates/oss-server/src/
├── aether.rs          [改] IngestSink 调用点补 user_metadata 参数
└── cli.rs             [改] clean 接 store 的 reap_stale；reap_stale_tmp 并入/退役
crates/oss-server/tests/…
tests/e2e_s3.sh        [改] 增 multipart/range/metadata 用例；删 multipart_threshold 变通
README.md              [改] Task 6 收尾
```

---

## Task 1: `x-amz-meta-*` 用户元数据往返

**Files:**
- Modify: `crates/xl-store/src/store.rs`（put 签名）
- Modify: `crates/xl-store/src/store/tests.rs`
- Modify: `crates/xl-s3/src/service.rs`、`crates/oss-server/src/aether.rs`

- [x] **Step 1: 写失败的测试**（`store/tests.rs` 追加）

```rust
#[tokio::test]
async fn put_and_read_back_user_metadata() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let mut md = std::collections::BTreeMap::new();
    md.insert("foo".to_string(), "bar".to_string());
    let info = s
        .put_object_with_meta("alpha", "k", Bytes::from_static(b"x"), None, md.clone())
        .await
        .unwrap();
    assert_eq!(info.user_metadata, md);
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.user_metadata, md);
    // 覆写不带元数据 = 清空（S3 PUT 语义：metadata 全量替换，不是合并）
    let info2 = s
        .put_object_with_meta("alpha", "k", Bytes::from_static(b"y"), None, Default::default())
        .await
        .unwrap();
    assert!(info2.user_metadata.is_empty());
}

#[tokio::test]
async fn copy_carries_user_metadata() {
    // copy 默认 metadata-directive=COPY：元数据随对象走
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let mut md = std::collections::BTreeMap::new();
    md.insert("k".to_string(), "v".to_string());
    s.put_object_with_meta("alpha", "src", Bytes::from_static(b"x"), None, md)
        .await
        .unwrap();
    s.copy_object("alpha", "src", "alpha", "dst").await.unwrap();
    assert_eq!(
        s.head_object("alpha", "dst").await.unwrap().user_metadata.get("k"),
        Some(&"v".to_string())
    );
}
```

- [x] **Step 2: 跑测试确认失败**——`cargo test -p xl-store user_metadata 2>&1 | tail`，预期编译错误（`put_object_with_meta` 未定义）。

- [x] **Step 3: 实现**。裁决：**新增 `put_object_with_meta` / `put_object_stream_with_meta`，旧函数保留为取 `Default::default()` 的薄壳**——比改签名波及 15+ 调用点（含全部测试）小得多，`xl.meta` 的 `user_metadata` 字段与 `ObjectInfo` 均已有：

```rust
pub async fn put_object_with_meta(
    &self, bucket: &str, key: &str, body: Bytes,
    content_type: Option<String>,
    user_metadata: std::collections::BTreeMap<String, String>,
) -> Result<ObjectInfo> {
    self.put_object_stream_with_meta(bucket, key,
        Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
        content_type, user_metadata).await
}

pub async fn put_object_stream_with_meta(
    &self, bucket: &str, key: &str, mut body: ByteStream,
    content_type: Option<String>,
    user_metadata: std::collections::BTreeMap<String, String>,
) -> Result<ObjectInfo> {
    // 与现 put_object_stream 同体，唯二差异：
    //   ① Version.user_metadata = user_metadata.clone()（不再 Default::default()）
    //   ② 返回的 ObjectInfo.user_metadata 同值
}

// 旧壳：
pub async fn put_object(&self, b, k, body, ct) -> Result<ObjectInfo> {
    self.put_object_with_meta(b, k, body, ct, Default::default()).await
}
```

`copy_object` 内部改为把 `info.user_metadata` 传给 `put_object_with_meta`（默认 COPY 语义；REPLACE 指令不在本切片）。
`xl-s3 service.rs::put_object` 增补：

```rust
let metadata = req.input.metadata.unwrap_or_default();   // Metadata = HashMap<String,String>
// s3s 已按 x-amz-meta- 前缀解析；是否已小写化以实测为准（e2e 用 aws-cli --metadata 验证）
let info = self.store.put_object_with_meta(&bucket, &key, Bytes::from(buf), content_type,
        metadata.into_iter().collect()).await.map_err(to_s3_error)?;
```

`get_object` / `head_object` 输出补 `metadata: Some(info.user_metadata.clone().into_iter().collect())`（空 map → `Some(空)` 与 `None` 的取舍：给 `Some(空)`，AWS 语义下无差别，客户端按空处理）。
`oss-server/src/aether.rs` 的 `IngestSink`：显式调 `put_object_stream`（旧壳）**不改**——aether 链路无元数据来源，旧壳意图更清晰。

- [x] **Step 4: 跑测试**——`cargo test -p xl-store meta 2>&1 | tail`；再 `cargo test -p xl-s3 2>&1 | tail`。
- [x] **Step 5: s3 面集成用例（改为 xl-s3 service 层直调，见定稿记录）**（`crates/oss-server/tests/s3_over_http.rs` 内已有直连用例的骨架，沿用其模式）：PUT 带 `x-amz-meta-foo: bar` → HEAD 断言响应头 `x-amz-meta-foo: bar` 且 GET 同。s3s 对响应头的前缀回写方式以实测为准（断言用 `resp.headers().get("x-amz-meta-foo")`）。
- [x] **Step 6: 提交**——`git add -A && git commit -m "feat: x-amz-meta-* 元数据 PUT/GET/HEAD/Copy 全链路往返"`

---

## Task 2: Range GET（206 / Content-Range / 416）

**Files:**
- Modify: `crates/xl-store/src/store.rs`（新增 `get_object_range`）
- Modify: `crates/xl-store/src/store/tests.rs`
- Modify: `crates/xl-s3/src/service.rs`

- [x] **Step 1: 写失败的测试**

```rust
#[tokio::test]
async fn get_object_range_reads_exact_slice() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"0123456789"), None).await.unwrap();
    // 半开区间 [2,5) -> "234"
    assert_eq!(&s.get_object_range("alpha", "k", 2..5).await.unwrap()[..], b"234");
    assert_eq!(&s.get_object_range("alpha", "k", 0..10).await.unwrap()[..], b"0123456789");
    // 出界：start == size / end > size 一律 InvalidKey 语义之外——越界由调用方（s3s range.check）先拦，
    // 内核仍要防呆：start >= size 或 end > size → Err
    assert!(s.get_object_range("alpha", "k", 10..11).await.is_err());
    assert!(s.get_object_range("alpha", "k", 0..11).await.is_err());
    assert!(s.get_object_range("alpha", "k", 5..3).await.is_err());
}
```

- [x] **Step 2: 跑测试确认失败**（`get_object_range` 未定义）。

- [x] **Step 3: 实现**。内核按 meta 定位 part 文件偏移；本任务对象都是单 part（Task 5 的跨 part 用例另测）：

```rust
/// 有界读：`range` 为半开区间且必须满足 0 <= start <= end <= size（调用方已校验越界）。
/// 读路径重试语义与 get_object 相同（并发覆写时重读 meta 一轮）。
pub async fn get_object_range(&self, bucket: &str, key: &str, range: std::ops::Range<u64>)
    -> Result<Bytes> {
    // 同 get_object 的两轮循环：读 meta → version_parts(v) → 逐 part 截取命中段 concat；
    // 单 part 时 = open + seek(start) + read_exact(len)
    // 读者防呆：start > end、end > size → Error::InvalidKey(format!("range {range:?} 越界"))（内部错误，service 层不该走到）
}
```

`xl-s3 service.rs::get_object` 改造（**`head_object` 的 Range 不支持**——HEAD 带 Range 返回 206/部分 Content-Length 是 S3 行为，但 aws-cli/rclone 均不依赖；标为已知边界不实现）：

```rust
let (start, end) = match req.input.range {
    Some(r) => { let rr = r.check(info.size)?; (rr.start, rr.end) }  // 越界 → InvalidRange(416) via From
    None => (0, info.size),
};
let bytes = if start == 0 && end == info.size {
    self.store.get_object(...)?.1
} else {
    self.store.get_object_range(&bucket, &key, start..end).await.map_err(to_s3_error)?
};
let content_range = (req.input.range.is_some())
    .then(|| format!("bytes {}-{}/{}", start, end.saturating_sub(1), info.size));
let resp = GetObjectOutput {
    body: Some(StreamingBlob::from_bytes(bytes)),
    content_length: Some((end - start) as i64),
    content_range,
    accept_ranges: Some(AcceptRanges::from_static(AcceptRanges::BYTES)),
    metadata: …（Task 1 的字段）,
    ..同现状
};
Ok(match req.input.range { Some(_) => S3Response::with_status(resp, StatusCode::PARTIAL_CONTENT), None => S3Response::new(resp) })
```

**多区间 `bytes=0-9,20-29`**：s3s 的 `Range::parse` 直接拒绝（ParseRangeError）→ 在 DTO 反序列化层即 400；具体错误码探头核实一次（预期 `InvalidArgument`，规格措辞是 `InvalidRequest`——若不符记录为 Minor，不为此写补丁）。

- [x] **Step 4: 跑测试**（内核用例如上；再加 `bytes=-N` 语缀区间与空对象的 `check` 行为边界，均通过 s3s 语义覆盖）。
- [x] **Step 5: s3 面集成用例（改 service 层直调，见定稿记录）**：PUT 100 字节 → GET `Range: bytes=10-19` 断言 206 + `content-range: bytes 10-19/100` + 该段字节；`Range: bytes=1000-` 断言 416 + `InvalidRange`。
- [x] **Step 6: 提交**——`git commit -m "feat: GetObject Range——206/Content-Range/416，内核有界读"`

---

## Task 3: 流式 PutObject（去掉整份内存）

**Files:**
- Modify: `crates/xl-s3/src/service.rs`
- Modify: `crates/oss-server/tests/s3_over_http.rs`

- [x] **Step 1: 写测试要点**（xl-s3 层难做「驻留内存」判别，改测**语义等价与失败路径**）：
  - 大于单次 read 的体（如 3 MiB）PUT 后 GET 回内容与 md5 一致（走真实 HTTP，沿用 s3_over_http 模式）；
  - `content_length` 声明超 `max_put_bytes` → 立即 413 `EntityTooLarge`（无需读体）；
  - 流中途错误（客户端断连）→ 4xx `IncompleteBody`，且 `.oss.sys/tmp` 无残留（复用 store 既有 tmp 清理路径）。

- [x] **Step 2: 实现**。`StreamingBlob` 本身就是 `Stream<Item = Result<Bytes, _>>`（`src/dto/streaming_blob.rs:76`），直接映射错误后交给内核流式写盘：

```rust
let body = req.input.body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;
if let Some(cl) = req.input.content_length {
    if cl < 0 { return Err(s3s::s3_error!(InvalidRequest, "negative content-length")); }
    if cl as usize > self.max_put_bytes {
        return Err(s3s::s3_error!(EntityTooLarge, "object exceeds max_put_bytes"));
    }
}
// 计数上限在流上兜底（content-length 可缺省/不可信）
let mut seen = 0usize;
let limited = body.map(move |chunk| {
    let chunk = chunk.map_err(|e| {
        tracing::warn!(error = ?e, "读取 PUT 请求体失败");
        xl_store::error::Error::Io(std::io::Error::other(format!("request body: {e}")))
    })?;
    seen += chunk.len();
    if seen > max { Err(EntityTooLarge 包装) } else { Ok(chunk) }
});
let info = self.store.put_object_stream_with_meta(&bucket, &key, Box::pin(limited) as ByteStream,
    content_type, metadata).await.map_err(to_s3_error)?;
```

  注意同 Task 1 的 metadata 一并接入（两任务触碰同一函数，谁先落地谁先改；本计划顺序 Task 1 → 3，Task 3 在此处直接复用 `put_object_stream_with_meta`）。
  错误映射抉择：流上「超限」错误内核只会当 `Io` 上报 → 4xx 会退化成 5xx。**裁决**：xl-s3 层在 `seen > max` 时先把标志置位、让内核收到一个良性截断错误，随后 service 层检查标志返回 `EntityTooLarge`（实现时用 `Arc<AtomicBool>`），避免把 413 语义沉进内核。骨架：`let exceeded = Arc::new(AtomicBool::new(false));`（闭包 clone 进 map），finally 分支 `if exceeded.load() { return Err(s3s::s3_error!(EntityTooLarge, ...)) }`。
  `max_put_bytes` 的语义变化：仍是**单次 PUT（含单 part）**上限；multipart 大对象不再受它约束（Task 5 的 part 各自限）。README 措辞随之更新（Task 6）。

- [x] **Step 3: 跑测试**；Step 4: 提交——`git commit -m "feat: PutObject 流式化——请求体直通内核，去掉整份内存封顶"`

---

## Task 4: 孤儿与垃圾回收（启动自检 + clean 扩展）

**Files:**
- Create: `crates/xl-store/src/store/gc.rs`
- Modify: `crates/xl-store/src/store.rs`（`open` 调 `reap_transient`）、`crates/xl-store/src/lib.rs`（若需 re-export）
- Modify: `crates/oss-server/src/cli.rs`（clean 接线）
- Modify: `crates/oss-server/tests/ops_cli.rs`

**两步划分（防竞态的裁决）**：
- **启动自检 → `reap_transient()`**：清空 `.oss.sys/tmp/` 与 `.oss.sys/trash/` 全部条目（启动时进程内无在途写入；tmp 里只有崩溃残留、trash 里都是无人引用的旧版本）。**不碰对象目录**——uuid 形状目录可能是客户端仍在续传的 MPU 暂存区（S3 客户端可跨服务重启续传），启动即删会造成在途上传失败；对象目录的垃圾交 mtime 判定（下一步）。
- **`clean <days>` → `reap_stale(due)`**：按 mtime 回收 (a) trash/ 条目、(b) `.oss.sys/tmp/` 条目、(c) **每个对象目录下未被 xl.meta 引用的子目录**（含 MPU 暂存区——规格 §multipart「未完成上传占用磁盘，clean 按 mtime 回收（默认 7 天）」）。判定：读该对象 xl.meta（**读失败/损坏 → 跳过不删**，保守优先）；引用集 = 所有版本当前形态的 `v.part.uuid`（**Task 5 落地后改为 `effective_parts()`**——Task 4 先于 Task 5 提交，此处按当时代码形态写）；子目录名 ∈ 引用集 → 保留；否则看 mtime < due → 删除。mtime 用目录自身（MPU 每次 part 上传会更新目录 mtime——part 文件创建/覆盖会刷新父目录 mtime ✓；判定窗口外长挂上传被删则上传方收 NotFound/NoSuchUpload，与 S3 一致，文档化）。

- [x] **Step 1: 写失败的测试**（`store/tests.rs`）：

```rust
#[tokio::test]
async fn open_reaps_tmp_and_trash_transient() {
    let (s, d) = store().await;
    // 摆 tmp 孤儿与 trash 残留（含一个「活引用」的对照：活版本目录在对象目录里，不受影响）
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"one"), None).await.unwrap();
    let tmp = layout::sys_dir(d.path(), "tmp").join("orphan");
    let trash = layout::sys_dir(d.path(), "trash").join("old");
    tokio::fs::create_dir_all(&tmp).await.unwrap();
    tokio::fs::create_dir_all(&trash).await.unwrap();
    let s2 = Store::open(StoreConfig::new(d.path())).await.unwrap(); // 重开 = 重启
    assert!(!tmp.exists() && !trash.exists());
    assert_eq!(s2.head_object("alpha", "k").await.unwrap().size, 3); // 活数据不动
    drop(s);
}

#[tokio::test]
async fn reap_stale_removes_unreferenced_dirs_by_mtime() {
    // 对象目录里摆：① 被引用版本目录（活）② 无引用旧目录 mtime 拨老 ③ 无引用旧目录 mtime 新鲜
    // 断言：只有 ② 被删；①③ 全在
}

#[tokio::test]
async fn reap_stale_skips_object_with_corrupt_meta() {
    // xl.meta 损坏的对象的子目录一律保留（保守不删）
}
```

- [x] **Step 2: 实现 `gc.rs`**：

```rust
//! 垃圾回收：启动瞬时回收（tmp/trash）与 clean 的按龄回收（trash/tmp/未引用子目录）。
pub struct SweepReport { pub transient: usize, pub stale: usize }

impl Store {
    pub(crate) async fn reap_transient(&self) -> Result<usize> { /* 清空 tmp/ 与 trash/ 目录内容 */ }
    pub async fn reap_stale(&self, due: std::time::SystemTime) -> Result<usize> {
        // 1) trash/ 与 tmp/ 下 mtime < due 的条目
        // 2) 每桶每对象目录：读 meta → 引用集 → 未引用且 mtime < due 的子目录删除
        //    列举入口复用 store/list.rs 的 collect_keys（跳过坏目录的既有语义）
    }
}
```

`store.rs::open`：建目录后调 `self.reap_transient().await?`，失败仅 warn 不阻启动？——**裁决：失败向上传播**（启动即异常应当可见；tmp 清不掉通常意味着盘坏了）。
`lib.rs` re-export `SweepReport`。`cli.rs`：clean 分支改为 `staging 扫描（现状）+ state.store.reap_stale(due)`，输出两者计数；`reap_stale_tmp` 保留（staging 盘在 store 之外）但 `.oss.sys/tmp` 部分移除（并入内核，防两处口径漂移）。

- [x] **Step 3: 跑测试** + `ops_cli` 集成用例更新（clean 现在还应删 trash 与未引用子目录——补断言）。
- [x] **Step 4: 提交**——`git commit -m "feat: 孤儿回收——启动自检清 tmp/trash；clean 按 mtime 回收未引用版本目录与未完成 MPU 暂存区"`

---

## Task 5: Multipart Upload（本切片最大的一块）

**Files:**
- Modify: `crates/xl-store/src/meta.rs`（`parts: Option<Vec<PartRef>>`）
- Create: `crates/xl-store/src/store/mpu.rs`
- Modify: `crates/xl-store/src/store.rs`（`get_object`/`get_object_range` 走 `version_parts`）
- Modify: `crates/xl-s3/src/service.rs`（5 个方法）
- Modify: `crates/oss-server/tests/s3_over_http.rs`、`tests/e2e_s3.sh`

### 5.1 `xl.meta` 演进

```rust
pub struct Version {
    …现状全保留…
    pub part: PartRef,                                   // 兼容位：单段 PUT 恒有值
    /// Multipart：Some(parts) 时以本列表为准（按 part 号升序）；
    /// None = M1 单段语义（读 part 字段）。只加 Option 字段 = 前向兼容契约（meta.rs 顶部注释）。
    #[serde(default)]
    pub parts: Option<Vec<PartRef>>,
}

impl Version {
    /// 统一的 parts 视图：单段 → vec![part.clone()]，多段 → 原列表。
    pub fn effective_parts(&self) -> Vec<PartRef> { … }
}
```

单段 PUT 写入时保持 `parts: None`（旧文件同样的形态）；MPU complete 写 `parts: Some(...)`。**黄金字节测试会破**（新字段进入编码）——按 meta.rs 既有「更新流程」注记：确认有意 → 打印新摘要 → 更新常量（golden_bytes_stable 的断言值）。
`PartRef.uuid`：多段时所有 part 的 uuid 相同（= 暂存目录名 = upload_id），`part.part` = part 号。

### 5.2 内核 `store/mpu.rs`

**布局（按规格 §multipart：「在对象目录下建 `<uploadId>/` 暂存区」）**：

```
<bucket>/<对象展平目录>/<upload_id>/
├── mpu.meta            create 时写（rmp：{content_type, user_metadata, created_nanos}；存在即「这是一个 MPU 暂存区」）
├── part.1  part.2 …    各 part 数据（与版本目录里的 part.N 同名同形——complete 后目录原地转正，零拷贝）
└── part.1.meta …       sidecar：rmp {etag, sha256, size}（part 级独立写入，无并发共享文件）
```

```rust
pub struct MpuPartInfo { pub part_number: u16, pub size: u64, pub etag: String }

impl Store {
    pub async fn create_multipart_upload(&self, bucket: &str, key: &str,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>) -> Result<String>;
        // ensure_bucket → validate_key → lock_for → upload_id = Uuid::new_v4()
        // mkdir 对象目录/<id>/ → 写 mpu.meta；对象已有旧版本不动（complete 才提交）

    pub async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str,
        part_number: u16, body: ByteStream) -> Result<String>;
        // validate_upload_id（Uuid::parse_str）→ 目录存在（无 mpu.meta → Error::NoSuchUpload）
        // part_number 1..=10000（调用方转好 u16）
        // 流式写 part.N（先 ./part.N.new 再 rename？——重传同 part 允许覆盖：直接 create 截断写 + 末尾 rename part.N.new→part.N 防半截文件，采用后者）
        // 边写边算 md5+sha256；写 part.N.meta；返回 etag（hex md5）

    pub async fn complete_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str,
        parts: &[(u16, String)]) -> Result<ObjectInfo>;
        // lock_for(bucket,key)（与 PUT/DELETE 串行）
        // 校验顺序（对齐规格）：目录/mpu.meta 在 → 非空且 ≤10000 → 严格升序（否则 InvalidPartOrder）
        //   → 逐 part：文件在 + sidecar 存在 + 声明 etag 与 sidecar 一致（否则 InvalidPart）
        //   → 非末段 size ≥ 5 MiB（否则 EntityTooSmall）
        // etag = hex(md5(concat(每 part 的 md5 原始 16 字节))) + "-{n}"
        // 构造 Version{ part: 首段副本（兼容位）, parts: Some(...), size: 总和, etag, content_type, user_metadata } 
        // write_meta（唯一提交点；xl.meta.new + rename）→ best-effort 删 mpu.meta 与 *.meta sidecar
        // 旧版本仍按既有流程 → trash

    pub async fn abort_multipart_upload(&self, bucket, key, upload_id) -> Result<()>;
        // 校验后 remove_dir_all（幂等性：不存在 → NoSuchUpload）

    pub async fn list_parts(&self, bucket, key, upload_id) -> Result<Vec<MpuPartInfo>>;
        // 读 part.N.meta 列举（升序；max_parts 分页不做——≤10000 全量，is_truncated=false，规格未要求）
}
```

`Error` 增 4 个变体：`NoSuchUpload(String)` / `InvalidPart(String)` / `InvalidPartOrder(String)` / `EntityTooSmall(String)`，`convert.rs` 映射到同名 `S3ErrorCode`（4xx 保留原文，沿用既有口径）。

**并发裁决**：part 上传不取对象锁（文件名按 part 号互斥、sidecar 独立）；complete/abort 取 `lock_for`（与 PUT/DELETE 串行，防「complete 期间被 PUT 覆写」的交错）；complete 与 abort 之间的竞态：complete 先 rename 走 mpu.meta？——裁决：**complete 提交成功后把 mpu.meta 移入版本目录（改名为 `mpu.meta.done`）**，abort 以 mpu.meta 是否存在为门（不存在 → NoSuchUpload），天然互斥。

### 5.3 读路径

`get_object` 与 `get_object_range` 改为按 `v.effective_parts()` 累计偏移逐 part 读取：

```rust
// 整读：各 part 顺序 read + concat（ponytail: M2a 仍整份进内存，与既有限制同口径；M2b 流式）
// Range：start/end 映射到 part 序列的局部区间，只读命中段（首尾 part 各读一段，中间 part 整读）
```

### 5.4 `xl-s3 service.rs` 五个方法

```rust
async fn create_multipart_upload(&self, req) -> … {
    let b = &req.input.bucket; let k = &req.input.key;
    let id = self.store.create_multipart_upload(b, k, req.input.content_type, metadata).await
        .map_err(to_s3_error)?;
    Ok(S3Response::new(CreateMultipartUploadOutput { bucket: Some(b.clone()), key: Some(k.clone()),
        upload_id: Some(id), ..Default::default() }))
}

async fn upload_part(&self, req) -> … {
    let pn = req.input.part_number;
    if !(1..=10_000).contains(&pn) { return Err(s3s::s3_error!(InvalidArgument, "part number out of range")); }
    let body = req.input.body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;
    // 流式直通（同 Task 3 的 map 包装；part 上限沿用 max_put_bytes 预检 + 计数兜底）
    let etag = self.store.upload_part(&req.input.bucket, &req.input.key, &req.input.upload_id,
        pn as u16, Box::pin(body.map(…))).await.map_err(to_s3_error)?;
    Ok(S3Response::new(UploadPartOutput { e_tag: Some(ETag::Strong(etag)), ..Default::default() }))
}

async fn complete_multipart_upload(&self, req) -> … {
    // multipart_upload.parts 缺失/空 → s3s::s3_error!(InvalidRequest, "missing parts")
    // 逐项：part_number 与 e_tag 缺失 → InvalidPart（裁决：规格只写了 ETag 不一致，缺字段一并按 InvalidPart）
    let info = self.store.complete_multipart_upload(…, &pairs).await.map_err(to_s3_error)?;
    Ok(S3Response::new(CompleteMultipartUploadOutput { bucket, key, e_tag: Some(ETag::Strong(info.etag)),
        ..Default::default() }))   // location 不给（客户端不依赖）
}

async fn abort_multipart_upload / list_parts  // 直通；ListPartsOutput 填 bucket/key/upload_id/parts/is_truncated:Some(false)
```

`UploadPartCopy` / `ListMultipartUploads` / GET 的 `partNumber` 参数：**本切片不做**（保持默认 NotImplemented / 忽略），在功能规格对应行加 M2b 注记（Task 6）。

### 5.5 测试与验收

- [x] 内核用例（`store/tests.rs`）：create→1 part 小文件（<5MiB 末段允许）complete；3 part（中间段 ≥5MiB 用 5×1MiB×… 直接构造 size）complete 后 etag 形如 `<32hex>-3`；乱序 → InvalidPartOrder；ETag 不符 → InvalidPart；中间段 <5MiB → EntityTooSmall；abort 后目录消失且再次 abort → NoSuchUpload；upload_id 含 `../` → InvalidKey/Uuid 校验拒；complete 后 `get_object` 拼接内容 == 原三段拼接；`get_object_range` 跨 part 命中断言；并发：同 part 重传覆盖幂等、complete 进行中 PUT 同键串行（lock 断言可用现有 concurrent 测试模式）。
- [x] `xl.meta` 用例：golden 摘要更新（e7f386e4…）；`effective_parts` 单段/多段；旧文件（无 parts 字段）decode → None。
- [x] s3 面集成：**改 service 层直调**（multipart_flow_through_service；真实 HTTP 由 e2e 脚本覆盖）（create→3×upload_part→complete→GET 断言）；错误码断言（416 之外的 404 NoSuchUpload / 400 InvalidPart 等按 XML `<Code>`）。
- [x] **e2e 脚本**（`tests/e2e_s3.sh`）：删 `multipart_threshold` 环境变通行；新增第 N 步：`aws s3 cp` 一个 ≥8 MiB 随机文件（自动走 multipart）+ `aws s3 cp` 下载 `cmp` 一致；`aws s3api get-object --range`（206 内容比对）；`aws s3 cp --metadata foo=bar` 后 `head-object` 可见；rclone 同款 ≥8 MiB 上传下载。
- [x] 提交——`git commit -m "feat: Multipart Upload——五操作全链路，xl.meta parts 演进，clean 回收未完成上传"`

---

## Task 6: 文档与限制清单收尾

**Files:** `README.md`、`docs/superpowers/specs/2026-10-10-feature-spec.md`（状态注记）、本计划

- [x] README「已知限制」更新：删 `Multipart 与 Range 未实现`、`x-amz-meta-* 静默丢弃`、`单段 PUT 上限 256 MiB` 的旧措辞；改留：HEAD 带 Range 不支持、`UploadPartCopy`/`ListMultipartUploads` 未实现（M2b）、GET `partNumber` 未实现、单 part/单 PUT 仍受 `max_put_bytes` 限、多区间 Range → 400。
- [x] 功能规格 multipart 小节把 `UploadPartCopy`/`ListMultipartUploads` 标注 M2b；§3.2 Range 的「M2」标注改为已实现。
- [x] 提交——`git commit -m "docs: M2a 落地后同步 README 与功能规格的边界清单"`

---

## 四闸与验收（收尾轮统一跑）

- [x] fmt 无 diff · clippy 0 告警 · `cargo test --workspace -j 2` 全绿（128 测）
- [x] `tests/e2e_s3.sh` 对真服务实跑全过（含新增 multipart/range/metadata 步；14 步、退出码 0）
- [x] 控制台 Playwright e2e × M2a 后端**补跑两轮 3/3 全绿**（29.0s / 12.8s，零重试；用 M2a 二进制直起，测毕即停；建桶/上传/列表/下载字节一致/删除/密钥/秒传全过——M2a 未回归控制台路径）

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| s3s 字段名/错误码与本文档不符 | 全部已按 0.17.0 源码核实（行号见「执行前必读」）；仍以编译器为准 |
| `Range::parse` 对多区间的错误码与规格措辞（InvalidRequest）可能不符（预期 400 InvalidArgument） | 记录为 Minor，不为措辞写补丁 |
| complete 是唯一「非原子可见」操作（拼接/校验期间不可见） | 规格明说如此；读路径只信 meta 的语义下天然成立 |
| complete 与 abort 的竞态 | mpu.meta 存在性为门 + lock_for；complete 成功后先改名 `mpu.meta.done` 再收尾 |
| clean 与进行中 MPU 的 mtime 竞态 | 窗口（`days`）内不删；超窗口删除 = 上传方 NoSuchUpload（与 S3 行为一致），README 文档化 |
| 损坏 xl.meta 让 reap_stale 误删活版本 | 读 meta 失败一律跳过该对象（保守不删） |
| `parts: Option<Vec<PartRef>>` 改变 golden 字节 | 按 meta.rs 既有「有意识更新」流程：核对有意后更新摘要常量 |
| part sidecar（part.N.meta）在版本目录里残留 | complete 后 best-effort 删除；残留无害（读路径只认 xl.meta），M2b fsck 兜底 |
| 单 part 上限未强制 5 GiB；ListParts 不分页 | 记录在案；客户端不依赖（aws-cli/rclone 的 part 均 ≤ 常规值） |
| `put_object_with_meta` 新函数 vs 改签名的取舍 | 新函数 + 旧壳（波及面最小）；M2b 重构存储层时可收拢 |
| 流式 PUT 后 `max_put_bytes` 语义（单 PUT/单 part） | README 明示；multipart 大对象不受该值约束 |
| 对象在 MPU 进行中被 DELETE：对象目录（含 MPU 暂存区）整体移除 → 该 upload 的 complete/abort 收 NoSuchUpload | 与既有 delete_object 语义一致（删除即目录移除）；AWS 下 MPU 独立于对象、此处随目录消亡——**M2a 边界，文档化**；客户端（aws-cli/rclone）不依赖该场景 |

## 定稿记录

（实现完成后补：每 Task 实际形态、四闸实跑、e2e 输出、与本文档的差异裁决。）

### Task 1（x-amz-meta-* 往返）

- 实际形态与本计划一致（`put_object_with_meta` / `put_object_stream_with_meta` + 旧壳；
  `copy_object` 携带 `info.user_metadata`；service 层 GET/HEAD 输出补 `metadata`）。
- **偏差**：Step 5 的「s3_over_http.rs 走真实 HTTP 断言响应头」**未做**——该文件现无 SigV4 签名 helper，
  签名级验证按仓库既有分工属 `tests/e2e_s3.sh`（Task 6 新增 `aws s3 cp --metadata` + `head-object` 用例）。
  本任务以 `xl-s3/tests/object_ops.rs::metadata_roundtrip_through_service`（service 层直调，含覆写清空语义）替代。
- 实跑：`cargo test -p xl-store user_metadata` 2/2 ✓；`cargo test -p xl-s3 metadata_roundtrip` 1/1 ✓；
  fmt/clippy 随提交前跑。

### Task 2 + Task 3（Range GET / 流式 PutObject，合并提交）

- **合并提交原因**：两任务改动集中在 `xl-s3/src/service.rs` 的相邻函数（`get_object` / `put_object`），
  无法按文件拆分提交；任务边界以本记录与提交信息说明。
- Task 2 实际形态：内核 `get_object_range`（seek + 有界读，tokio `read_exact` 返回 `Result<usize>`
  ——首轮编译即暴露，已修）；service 层 `Range::check` → 206 + `Content-Range` + `accept-ranges: bytes`，
  越界 → `InvalidRange(416)`；显式全段 Range 仍回 206（走整读）。
- Task 2 **偏差**：Step 2「确认失败」与实现合并编译（编译成本）；Step 5 同 Task 1——HTTP 级留给 Task 6 e2e，
  本任务以 `object_ops.rs::get_object_range_returns_206_and_416`（含语缀区间 bytes=-5 与 416）替代。
- Task 3 实际形态：`StreamingBlob` 经 `StreamExt::map` 直通内核；`content_length` 已知超限不读体直接 413；
  流上计数兜底用 **两个标志位**（`exceeded` → 413；`body_err` → 保留 M1 的 `IncompleteBody` 4xx 口径，
  不退化成 500）。测试含本地 `Chunks` 假流（多分块、超限、中途出错三条路径）+ tmp 无残留断言。
- 依赖面：`http` 从 xl-s3 的 dev-deps 提升为直接依赖（s3s 不 re-export StatusCode；http 本就在依赖树，非新增 crate）。
- 实跑：xl-store 56/56 ✓、xl-s3 27/27 ✓（均真实退出码 0）；fmt/clippy 随提交前跑。

### Task 4（孤儿/垃圾回收）

- 实际形态：新增 `store/gc.rs`——`reap_transient`（open 时清 tmp/trash，失败向上传播）+
  `reap_stale(due)`（trash/tmp 按 mtime + 逐对象目录「未引用且过期」子目录；无 meta 按零引用、
  损坏 meta 整对象跳过）。**偏差**：未引入 `SweepReport` 结构——两个 usize 直接返回，CLI 分开打印，
  少一层没有第二实现的抽象。
- `cli.rs`：clean 分支接 `store.reap_stale`；`reap_stale_tmp` 与其单测**退役**（职责并入内核，
  防两处口径漂移）；等价用例已在 `xl-store` 侧（`open_reaps_tmp_and_trash_transient` /
  `reap_stale_removes_unreferenced_dirs_by_mtime` / `reap_stale_skips_object_with_corrupt_meta`，
  mtime 用 std `File::set_modified`，不再依赖 `touch`）。`ops_cli` 的 clean 用例未改（行为等价）✓。
- 实跑：xl-store 59/59 ✓、oss-server 各目标全绿（含 ops_cli clean 路径经新内核回收）✓；退出码 0。

### Task 5（Multipart Upload）

- 实际形态与本计划一致。实现中的关键裁决/偏差：
  - `complete` 的 part 一致性校验在**提交前**完成（etag/sidecar/大小/顺序），提交后只做转正与清理；
    组合 ETag = `hex(md5(逐段原始 md5 字节拼接))-N`，手算核对（单段 `md5("hello")` 的组合值为
    `62109206880d38a4010a98e11243924a-1`，测试初稿曾按明文 md5 断言——测试错、实现对）。
  - `decode` 的 uuid 白名单从 `part.uuid` **扩展覆盖 `parts[].uuid`**（多段 uuid 同样进读路径，
    属计划执行中新发现的信任边界缺口，已补测试）。
  - GC 引用集改用 `effective_parts()`（不再依赖「多段共享同一目录」的巧合）。
  - golden 摘要按既有流程重算：`e7f386e4…`（parts 字段进入编码，有意识变更）。
  - e2e 脚本补三组用例（multipart 自动/强制分片、Range 206 字节比对、--metadata 往返）。
- 实跑：xl-store 67/67、xl-s3 19/19（含全链路）、oss-server 全目标绿、`cargo test --workspace -j 2` 128 测全绿；clippy 0 告警。

### Task 6（文档收尾）

- README：进度段升为「M1 + M2a」；已知限制替换为 M2a 边界（UploadPartCopy/ListMultipartUploads/GET
  partNumber → M2b；多区间 Range → 400；HEAD 带 Range 不支持；单 PUT/单 part 仍受 max_put_bytes）。
- 功能规格：§3.1/3.2/3.5/3.6/3.7 与阶段表逐处标注 M2a 已落地与 M2b 边界。
- 四闸实跑：fmt 无 diff · clippy 0 告警 · workspace 128 测全绿 · **e2e_s3.sh 对真服务实跑 14 步全过**
  （10 MiB aws-cli 自动 multipart + cmp、multipart ETag 带 -N、Range 206 Content-Range + 字节比对、
  --metadata 往返、rclone 压 cutoff 强制分片 + cmp；退出码 0）。
