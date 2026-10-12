# S3 协议补全 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把功能规格里已写死、实现尚未落地的 S3 协议面补齐——**条件请求**（If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since）、**`response-content-*` 响应覆盖参数**、**`Content-MD5` 与 `x-amz-checksum-*` 校验和**——并收口两处边界（多区间 Range 错误码对齐、HEAD 带 Range 的取舍）与控制台对 2.0 问题响应的 e2e 覆盖。

**Architecture:** 改动集中在 `xl-s3`（service 层的条件求值与响应覆盖、流包装层的校验和）、`xl-store`（`xl.meta` 加 `Option` 校验和字段 + 提交前校验语义）、web e2e（用例）。**不新增第三方依赖**（校验和走 `s3s::checksum::ChecksumHasher`，时间格式化走 `s3s::dto::Timestamp::format`——均已核实为公开 API）。

**Spec:** `docs/superpowers/specs/2026-10-10-feature-spec.md` §3.1（Content-MD5 / `x-amz-checksum-*`）、§3.2（response-content-* / 条件请求 / 多区间）、§3.3（HeadObject 的错误口径）。

---

## 执行前必读：四件事

1. **s3s 0.17 相关面已逐条核实**（来源 `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/s3s-0.17.0/`）。字段名不符时**以编译器为准**：
   - 条件字段：`GetObjectInput` 与 `HeadObjectInput` 均有 `if_match: Option<IfMatch>` / `if_none_match: Option<IfNoneMatch>` / `if_modified_since: Option<IfModifiedSince>` / `if_unmodified_since: Option<IfUnmodifiedSince>`（generated.rs，两者同形；HeadObjectInput 另有 `range` 与 `part_number`，本计划不实现后者的语义）。
   - `IfMatch` 等 = `ETagCondition`（`dto/etag_condition.rs`）：`enum { ETag(ETag), Any }`；`ETag` 为 `Strong(String)/Weak(String)`，取值用 `.value()`。
   - `IfModifiedSince` / `IfUnmodifiedSince` / `ResponseExpires` = `Timestamp`（`dto/timestamp.rs`）：**derive 了 PartialOrd/Ord**（直接比较）；`From<SystemTime>` ✓；`pub fn format(&self, TimestampFormat, &mut impl io::Write)`，`TimestampFormat::{DateTime, HttpDate, EpochSeconds}`（`format` 的签名带 `&mut impl io::Write`，对 String 用 `&mut w`）。
   - `response-content-*` 输入字段全为 `String`（generated.rs:30302-30310）；`response_expires` 由 s3s 按 **HttpDate** 解析（`ops/generated/get_object.rs:51-52`）。输出侧 `GetObjectOutput` 有对应 `cache_control/content_disposition/content_encoding/content_language/content_type: Option<String>` 与 `expires: Option<Expires(=String)>`。
   - **头部解析失败一律 `InvalidArgument`**：`http/de.rs:30` 的 `invalid_header` 证据——多区间 Range 被 s3s 拒绝时的实测错误码即来自此（Task 4 据此对齐规格）。
   - 校验和：`s3s::checksum::ChecksumHasher`（`src/checksum.rs`，**无 feature 门**，`pub mod checksum`）单遍算 crc32/crc32c/sha1/sha256/sha512/xxhash 并出 base64 `dto::Checksum`；`x-amz-checksum-*` 头由 s3s 解析进 `PutObjectInput.checksum_*` 字段（`ops/generated/put_object_annotation.rs:24-38`）。
   - 错误码齐备：`BadDigest` / `InvalidArgument` / `NotModified` / `PreconditionFailed`（`error/generated.rs:22/98/154/209`）。
2. **规格与实现的现状差**（开工事实）：
   - `Content-MD5` 规格标 ✅（§3.1）但**全仓零引用**（未实现）——本计划补齐，规格注记。
   - 条件请求、response-content-* 规格已写、未实现——本计划补齐。
   - `If-Range` 不在规格内，**不做**（记入风险表边界）。
3. **铁律**（设计 §4.2）不破坏：`xl.meta` 演化只加 `Option` 字段（本计划加 `checksum`，golden 摘要随之有意识更新）；校验必须在**提交点之前** fail-closed（Task 3 的流包装设计即为此）；不新增依赖。
4. 邻居事项（**不在本计划**）：`vite.config.ts` 的 vitest `startupTimeout` 已由主线程落盘；M2b（EC/多盘/流式读）另立计划。

---

## Task 1: 条件请求（GetObject + HeadObject）

**Files:**
- Modify: `crates/xl-s3/src/service.rs`（新增条件求值 helper；`get_object`/`head_object` 接线）
- Modify: `crates/xl-s3/tests/object_ops.rs`
- Modify: `tests/e2e_s3.sh`

- [x] **Step 1: 写失败的测试**（`object_ops.rs` 追加）

```rust
#[tokio::test]
async fn conditional_requests_follow_rfc9110() {
    let (s3, _d) = service().await;
    // …建桶、PUT "cond"（内容任意，取回 etag 与 last_modified）
    let put = s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(), key: "cond".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"payload"))),
        ..Default::default()
    })).await.unwrap().output;
    let etag = put.e_tag.unwrap().value().to_string();

    let get = |if_match: Option<IfMatch>, if_none: Option<IfNoneMatch>| {
        // 闭包不便异步泛型；实现时写成逐用例手写（本骨架示意字段）
    };

    // ① If-Match 命中 → 200；错值 → 412 PreconditionFailed
    // ② If-None-Match 命中 → 304 Not Modified（body 无、content_length 不设）
    // ③ If-None-Match: * 对已存在对象 → 304
    // ④ If-Match: Any(*) 对已存在对象 → 200
    // ⑤ If-Modified-Since 拨到未来 → 304；If-Unmodified-Since 拨到过去 → 412
    // ⑥ 条件先于 Range：If-None-Match 命中 + Range → 304（不是 206）
    // ⑦ HEAD 同口径：head_object --if-match 错值 → 412（§3.3 的「错误一律 404」只罩读错误，
    //    条件结果 412/304 照常返回——用例注记）
}
```

- [x] **Step 2: 跑测试确认失败**（`cargo test -p xl-s3 conditional 2>&1 | tail`，预期断言失败/字段未接线）。

- [x] **Step 3: 实现**。在 `service.rs` 加模块级 helper：

```rust
/// RFC 9110 §13.2.2 求值顺序：If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since，
/// 任一决定即短路。返回 `Err(Some(status))` 表示条件失败（304/412），`Ok(())` 继续。
/// `If-None-Match` 命中 → 304（GET 无 body；HEAD 同）；其余失败 → 412。
fn eval_preconditions(
    info: &ObjectInfo,
    if_match: Option<&ETagCondition>,
    if_unmodified_since: Option<Timestamp>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<Timestamp>,
) -> Result<(), StatusCode> {
    let etag_matches = |c: &ETagCondition| match c {
        ETagCondition::Any => true, // 实体存在（能走到这里即存在）
        // 值比较、不区分强弱：我们的 ETag 恒为 Strong 无引号 hex，客户端回传任意强弱都按值比
        // ponytail: 弱校验语义（W/ 前缀差异）不细分；有真实互操作诉求时按 RFC 9110 §8.8.3 收严
        ETagCondition::ETag(e) => e.value() == info.etag,
    };
    let last = last_modified(info.last_modified_nanos); // 复用手头 helper → Timestamp
    if let Some(c) = if_match {
        if !etag_matches(c) { return Err(StatusCode::PRECONDITION_FAILED); }
    } else if let Some(t) = if_unmodified_since {
        if last > t { return Err(StatusCode::PRECONDITION_FAILED); }
    }
    if let Some(c) = if_none_match {
        if etag_matches(c) { return Err(StatusCode::NOT_MODIFIED); }
    } else if let Some(t) = if_modified_since {
        if last <= t { return Err(StatusCode::NOT_MODIFIED); }
    }
    Ok(())
}
```

`get_object` 接线：先把 `info` 拿到（**现有代码在无 Range 分支才拿 info**——需重排：统一先 `head_object` 取 info，再求值条件，再 Range/整读；Range 分支现有的 head 调用可并入这一次）。`Err(304)` → `Ok(S3Response::with_status(GetObjectOutput::default(), StatusCode::NOT_MODIFIED))`；`Err(412)` → `Err(s3s::s3_error!(PreconditionFailed, "precondition failed"))`。
`head_object` 同样先取 info → 求值 → 304: `HeadObjectOutput::default()` with_status；412 同上。
注意：条件失败**不读盘上数据**（求值只用 info）。

- [x] **Step 4: 跑测试**（全 `-p xl-s3`；含既有 19 个不得回归）。

- [x] **Step 5: e2e 补充**（11b 段；aws-cli 输出形态由 e2e 代理实跑核准）（`tests/e2e_s3.sh`，插在步骤 11 之后）：
  ```bash
  echo "== 11b. 条件请求 =="
  COND_ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key one.bin --query ETag --output text | tr -d '"')
  # 304：If-None-Match 命中（aws-cli 对 304 的行为以实测为准——预期报 304/无 body，捕获退出码与输出）
  aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$BUCKET" --key one.bin \
    --if-none-match "\"$COND_ETAG\"" "$WORK/none.bin" 2>&1 | grep -q "304" || true
  # 412：If-Match 错值
  aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key one.bin \
    --if-match '"deadbeef"' >/dev/null 2>&1 && { echo "If-Match 错值应 412"; exit 1; } || true
  ```
  （aw-cli 对 304 的输出形态若怪，用 `curl -H 'If-None-Match: …'` 直测并断言空 body + 304 状态行，实现时以实测选一种。）

- [x] **Step 6: 提交**（706bd9c）——`git commit -m "feat(s3): 条件请求——If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since → 304/412"`

---

## Task 2: `response-content-*` 覆盖参数

**Files:** Modify: `crates/xl-s3/src/service.rs`（`get_object`）、`crates/xl-s3/tests/object_ops.rs`、`tests/e2e_s3.sh`

- [x] **Step 1: 写失败的测试**

```rust
#[tokio::test]
async fn response_content_params_override_headers() {
    // PUT "rc" → GET 带 response_content_type / response_content_disposition / response_cache_control /
    // response_content_language / response_content_encoding / response_expires
    // 断言 output 对应字段被覆盖；未带时回到对象自有 content_type / None
}
```

- [x] **Step 2: 跑测试确认失败**（`cargo test -p xl-s3 response_content 2>&1 | tail`，预期字段未被覆盖而断言失败）。

- [x] **Step 3: 实现**（`get_object` 组装 output 处，最省一档）：

```rust
// response-content-* 是响应头覆盖：客户端给什么值就原样下发（含 &filename= 的 RFC 5987
// 编码由客户端负责——规格 §3.2 措辞据此澄清，服务端无编码逻辑）。
let over = req.input; // 借用即可，逐字段搬运
let mut out = GetObjectOutput { /* 现有字段 */ ..Default::default() };
if let Some(v) = over.response_content_type { out.content_type = Some(v); }
if let Some(v) = over.response_content_disposition { out.content_disposition = Some(v); }
if let Some(v) = over.response_cache_control { out.cache_control = Some(v); }
if let Some(v) = over.response_content_language { out.content_language = Some(v); }
if let Some(v) = over.response_content_encoding { out.content_encoding = Some(v); }
if let Some(ts) = over.response_expires {
    let mut s = String::new();
    ts.format(TimestampFormat::HttpDate, &mut s).expect("HttpDate 格式化到 String 不会失败");
    out.expires = Some(s);
}
```

- [x] **Step 4: 跑测试** + e2e 补一步：`aws s3api get-object --response-content-disposition 'attachment; filename="dl.txt"' --response-content-type 'text/x-custom'` 后比对输出 JSON 的 `ContentType`/`ContentDisposition` 字段。

- [x] **Step 5: 提交**（e2e 覆盖并入收尾轮）——`git commit -m "feat(s3): response-content-* 查询参数覆盖响应头（含 response-expires 的 HttpDate 格式化）"`

---

## Task 3: `Content-MD5` 与 `x-amz-checksum-*`

**Files:**
- Modify: `crates/xl-store/src/meta.rs`（`Version` 加 `checksum: Option<ChecksumRecord>`）
- Modify: `crates/xl-store/src/error.rs`（加 `BadDigest(String)`）
- Modify: `crates/xl-store/src/store.rs`（`put_object_stream_with_meta` 增 `checksum` 参数；`load_object_info` 带回）
- Modify: `crates/xl-store/src/store/mpu.rs`（complete 的 `Version` 构造补 `checksum: None`）
- Modify: `crates/xl-store/src/store/tests.rs`、`crates/xl-store/src/meta.rs`（golden 常量）
- Modify: `crates/xl-s3/src/service.rs`（流包装校验；GET/HEAD 回写）
- Modify: `crates/xl-s3/src/convert.rs`（`BadDigest` 映射）
- Modify: `crates/oss-server/src/aether.rs`（调用点补 `None`）、`crates/oss-server/tests/*`（如涉调用点）
- Modify: `crates/xl-s3/tests/object_ops.rs`、`tests/e2e_s3.sh`

**设计裁决（必须照做）**：校验在 **xl-s3 的流包装层**完成、以「**流末尾注入错误**」让内核在提交点前 fail-closed——现有 `put_object_stream_with_meta` 的失败路径本就清 tmp、不写 meta，故**无需改提交协议**。校验和落盘仅在 store 侧作为**数据**（不做校验逻辑）。

- [x] **Step 1: 写失败的测试**（store 层：meta 往返含 checksum；service 层：见 Step 4）

```rust
// store/tests.rs
#[tokio::test]
async fn meta_roundtrips_checksum_record() {
    // put_object_stream_with_meta(..., Some(ChecksumRecord{ algorithm: "SHA256".into(), value_base64: "…".into() }))
    // → head_object 带回同值；get/head 读取路径一致
}
```

- [x] **Step 2: 实现 store 侧**：

```rust
// meta.rs
/// 单段校验和记录（M 阶段：仅客户端声明的那一种；multipart 组合校验和留 M2b）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChecksumRecord {
    /// s3s 的 ChecksumAlgorithm 字符串（"CRC32C" / "SHA256" / …）
    pub algorithm: String,
    /// base64 编码的摘要
    pub value_base64: String,
}
// Version 加：#[serde(default)] pub checksum: Option<ChecksumRecord>,
```

`put_object_stream_with_meta` 增尾参 `checksum: Option<ChecksumRecord>`，写进 `Version` 与返回的 `ObjectInfo`（`ObjectInfo` 亦加 `checksum` 字段）；旧壳传 `None`；aether 调用点传 `None`。**golden 摘要破**→ 按 meta.rs 既有更新流程重算。

- [x] **Step 3: 实现 xl-s3 侧**：

```rust
/// 校验和尾流：边转发边算声明算法的摘要；inner 结束后比对，不匹配则**在流末尾注入错误**
/// 让内核在提交点前中止（tmp 清理走既有失败路径）。
struct ChecksumTailStream { inner: StreamingBlob, hasher: Option<ChecksumHasher>, expected_base64: String, done: bool }
impl Stream for ChecksumTailStream {
    type Item = Result<Bytes, xl_store::error::Error>;
    fn poll_next(...) -> Poll<Option<Self::Item>> {
        if self.done { return Poll::Ready(None); }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => { self.hasher.as_mut().unwrap().update(&chunk); Poll::Ready(Some(Ok(chunk))) }
            Poll::Ready(Some(Err(e))) => { self.done = true; Poll::Ready(Some(Err(io_err(e)))) }
            Poll::Ready(None) => {
                self.done = true;
                let got = base64(self.hasher.take().unwrap().finalize());
                if got != self.expected_base64 {
                    return Poll::Ready(Some(Err(xl_store::error::Error::BadDigest(format!(
                        "checksum mismatch: expected {}, got {}", self.expected_base64, got)))));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
```

`put_object` 接线：从 `req.input` 取——`content_md5`（对应算法 MD5）与 `checksum_algorithm`+`checksum_*` 字段（以 s3s 实际填充为准：显式头已核实；aws-chunked **trailing** 路径见风险表，第一步先覆盖显式头；若 s3s 把 trailer 也合并进字段则白拿）。声明了校验和时：`ChecksumHasher` 只启用对应算法；同时把 `ChecksumRecord` 传进 `put_object_stream_with_meta` 落盘。冲突声明（如 Content-MD5 与 `x-amz-checksum-md5` 并存且不同值）→ `InvalidRequest`（记风险表）。
`GET/HEAD` 回写：`info.checksum` 按 `algorithm` 映射到 `checksum_crc32c/…/checksum_md5/checksum_sha256` 输出字段。
`convert.rs`：`StoreError::BadDigest(_) => S3ErrorCode::BadDigest`（400 保留原文）。

- [x] **Step 4: 测试**：object_ops（正确 crc32c 通过并落盘；**错值 → BadDigest 且对象不存在**（fail-closed 断言：随后 head 应 404）；Content-MD5 正确/错误两态；GET 回读字段）。

- [x] **Step 5: e2e**（11c 段；aws-chunked trailer 形态由 e2e 代理实跑核验）：`aws s3api put-object --checksum-algorithm SHA256 --body …` → `get-object` 输出含 `ChecksumSHA256`；改一字节故意不匹配（用 `--checksum-sha256` 伪造）→ `BadDigest` 原文断言。

- [x] **Step 6: 提交**（ec20d8d）——`git commit -m "feat: Content-MD5 与 x-amz-checksum-*——流尾校验（提交前 fail-closed）、xl.meta 落盘、GET/HEAD 回写"`

---

## Task 4: 边界收口（多区间错误码 / HEAD 带 Range / 规格同步）

**Files:** `crates/xl-s3/tests/object_ops.rs`、`tests/e2e_s3.sh`、`README.md`、`docs/superpowers/specs/2026-10-10-feature-spec.md`

- [x] 多区间：object_ops 加用例（改置 e2e/HTTP 层——11d，未签名 403 AccessDenied / 带签 400 InvalidArgument 两契约）（`GetObjectInput{range}` 走不到——多区间在 s3s 的**头部解析层**就拒了，服务层拿到的是解析错误；故该用例的正确位置是 **e2e/HTTP 层**：`curl -H 'Range: bytes=0-1,3-4'` → 断言 `400` + `<Code>InvalidArgument</Code>`（证据：`http/de.rs:30` 统一映射））。规格 §3.2「多区间 → InvalidRequest」改为 **InvalidArgument（400）**，并注明该判定在 s3s 解析层。
- [x] HEAD 带 Range：**不做**（aws-cli/rclone 不依赖）；加一条契约用例「HEAD 带 range 参数仍返回 200 + 全量 Content-Length」（钉住现行为，防未来静默变化）；规格 §3.2 既有「HEAD 带 Range 不支持」保留，README 已知限制同步。
- [x] 规格 §3.1 的 `Content-MD5 ✅` 补「（实现于本计划 Task 3）」；`x-amz-checksum-*` 行从 `❌（M2）` 改为 `✅`。
- [x] 提交——`git commit -m "docs+test: 多区间错误码对齐 InvalidArgument；HEAD+Range 边界钉契约；规格同步"`

---

## Task 5: 控制台问题响应 e2e（web）

> **已落地（评审注记，2026-10-12）**：本 Task 由主线程随「小口子收口」提交 `5ec7e11` 完成——
> `web/e2e/console.spec.ts` 已加「上传超限 → 问题响应 title」用例（实跑修正：声明超限在
> preprocess 即被 400 invalid_parameter 拒 → 断言钉「错误：请求参数非法」全角冒号版，兼作 3.0.0
> i18n 变更的实测回归；413 invalid_resource_size 仅对少报大小可达，不在此路径）；e2e 实跑验证中。本 Task 若仍有出入
> （如用例措辞与规格对齐），只做微调，不重复建设。

**Files:** `web/e2e/console.spec.ts`（追加用例）

- [ ] 现状（已核对源码，无需改前端）：vendored 脚本自带 `parseProblem`——失败响应把本地化 `title` 写进 `#aetherupload-output`（`aetherupload-all.js` 229/334/368 行），409 `chunk_out_of_order` 还带 `expected_index` 续传特判。
- [ ] 用例：选超过目标分组 `max_size` 的文件上传 → 断言 `#aetherupload-output` 出现 `「错误：」` 前缀的本地化 title（413 payload_too_large）；这是问题响应链路的第一个端到端断言（e2e 代理标注的覆盖缺口）。
- [ ] 跑 `npm run e2e`（高负载用 `--workers=1`；vitest 的 `startupTimeout` 已在配置里）。
- [ ] 提交——`git commit -m "test(web): 控制台 e2e 覆盖 2.0 问题响应——超限上传的本地化 title 显示"`

---

## 四闸与验收（收尾轮统一跑）

- [x] fmt 无 diff · clippy 0 告警 · workspace 全绿（132 测）· web vitest 全绿（本轮未动 web 代码面）
- [x] `tests/e2e_s3.sh` 对真服务实跑 **19 步全绿 rc=0**（条件/覆盖参数/校验和/多区间步全含）
- [x] 控制台 Playwright e2e **3/3 绿**
- [x] `xl.meta` golden 按「有意识更新」流程重算并记录（2af7eeed…）
- [x] 交叉验证闭环：评审 I-1（206 带校验和）与 e2e 实跑发现互为独立复现；修复经 e2e 在干净重建二进制上复验（206 无 x-amz-checksum*、全量 GET 仍带且客户端校验通过）

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| ETag 强弱比较简化（值比较、不分 W/ 前缀） | `ponytail:` 注释标注；有真实互操作诉求时按 RFC 9110 §8.8.3 收严 |
| RFC 求值顺序的 edge（如 If-Match 与 If-Unmodified-Since 并存、304 与 Range 组合） | 按「If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since」短路；e2e 以 aws-cli 实测为准 |
| aws-chunked **trailing** checksum（aws-cli 新版默认路径） | s3s 有 `stream::aws_chunked_stream` 解码器与 `x-amz-trailer` 解析；**先探针抓包**确认 trailer 值是否进 `PutObjectInput.checksum_*` 或 `req.trailing_headers`，再决定补读位置；本轮先保证显式头路径正确，trailer 路径以实测补 |
| `Content-MD5` 与显式 checksum 并存且值冲突 | `InvalidRequest`（S3 语义未定义交叉校验；明确拒绝优于静默取一） |
| `response_expires` 的 Timestamp→String 格式化 | 走 `Timestamp::format(HttpDate)`（s3s 公开 API），单测钉格式 |
| `ChecksumRecord` 进 xl.meta 改变 golden | 有意识更新流程（Task 3 Step 2 注记） |
| multipart 的组合校验和语义（composite） | **不做**（CompleteMultipartUpload 的 algorithm 协商留 M2b）；README/规格边界注明 |
| 304 响应的 header 细节（无 body、Content-Length 处理） | 用 `GetObjectOutput::default()` + `with_status(304)`；e2e 断言状态行与空 body |
| 条件请求对 aetherupload 上传面（2.0 协议）无影响 | 上传面不经 s3s 的 Get/Head——无需改动，e2e 顺带回归 |

## 评审收尾轮（reviewer 代理，区间 d055dd1..HEAD）

- **I-1（已修）**：206 分段响应原会回写整对象校验和（SDK 响应校验拿片段验整对象必失败）——
  改 `status != PARTIAL_CONTENT` 门控回写 + 用例；**e2e 代理在修复前的二进制上独立复现了同一缺陷**
  （aws-cli 实测 `Expected checksum … did not match calculated checksum`），其复现路径已转为 11c 的
  回归断言（eb55457，带校验和对象做 range GET 须通过客户端默认校验）；**M-1（已修）**：同值双声明去重放行、异值仍拒；
  **M-3（已修）**：ChecksumRecord 解码白名单（algorithm ≤32 / value_base64 ≤128 / 可见 ASCII）；
  **M-2（已标注）**：part 级校验和「未校验」进 README/规格；**O-3（留痕）**：时间条件秒/纳秒精度。
- 未采（记录在案）：O-1 GET/HEAD 状态码口径两套（等价且有用例注释）；O-2 GET 净增一次 meta 读
  （M2b 流式读时合并）。
- 无问题的项（评审核对）：求值序/短路/`*` 语义、校验和包装序与全部流边界分支、golden 记录、
  11b/11c 断言形态。

## 定稿记录

（实现完成后补：每 Task 实际形态、四闸实跑、e2e 输出、与本文档的差异裁决。）

### Task 1（条件请求）

- 形态与计划一致。实现中三处偏差/裁决：
  - 测试断言「200」须写 `Some(StatusCode::OK)` 而非 `None`——get_object 统一走 `with_status`（初稿断言 None 系笔误，编译/运行两轮修正）。
  - 测试闭包用 `let s3 = &s3;` 借用捕获（否则 `async move` 把 s3 搬进 future，闭包成 FnOnce 只能调用一次）。
  - `eval_preconditions` 的两处 else-if 用 let-chains 收拢（clippy `collapsible_if`，-D warnings 门）。
- e2e 实跑（e2e 代理）：**e2e_s3.sh 17 步全绿 rc=0**（含 11b）。两处实跑裁决：
  ① aws-cli 2.37 对 HEAD 的 304 **不当错误**（退出 0、输出空壳仅 `"Metadata":{}`）——
  11b 的 304 判别按「成功且无 ContentLength/ETag」写（首版按「非零退出+grep」是假红，已修 471f381）；
  ② 412 实测报错退出、stderr 含 `(412) … Precondition Failed`；GET 路径同验（304 空壳 / 412 报错）。
- 实跑：xl-s3 20/20（七场景）、workspace 全绿、clippy 0 告警。

### Task 2（response-content-*）

- 形态与计划一致（可变 out + 六字段覆盖）。**计划勘误**：`Timestamp::format` 吃 `io::Write`，
  `String` 只实现 `fmt::Write`——计划里「对 String 用 &mut w」写错，实现改 `Vec<u8>` 缓冲再转 String。
- 实跑：xl-s3 21/21、workspace 全绿、clippy 0 告警；e2e 覆盖并入收尾轮统一跑。

### Task 3（Content-MD5 与 x-amz-checksum-*）

- 形态与计划一致。实现中的裁决与勘误：
  - 包装序定为 **limit_stream 内、ChecksumTailStream 外**——否则限流层的 `body_err` 会把注入的
    BadDigest 吞成 IncompleteBody（计划未写明次序，为本次实现裁决）。
  - `declared_checksum` 在解构 `req.input` **之前**求值（解构会部分搬走 metadata 等字段，
    之后借用 req.input 会 E0382）。
  - `ChecksumAlgorithm` 是枚举（含 const 字符串与 From<String>），测试里用 `.to_string().into()`。
  - 「只声明算法无值 → InvalidRequest」写进用例与注释（CLI 恒带值；无值不猜）。
  - golden 第二次重算：`2af7eeed…`。
- 实跑：xl-store 68/68、xl-s3 22/22、workspace 全绿、clippy 0 告警；**e2e 18 步全绿 rc=0**（11c 一次通过）。
- **最大不确定性结案（e2e 代理实抓 --debug 请求原文 + botocore 元数据）**：aws-cli 2.37 的
  `--checksum-algorithm SHA256` **走请求头**（`x-amz-checksum-sha256` + `x-amz-sdk-checksum-algorithm`，
  均在 SigV4 签名头清单），**不走 aws-chunked/trailer**——「trailer 值不进 `PutObjectInput.checksum_*`」
  的情形在当前 CLI 下不发生。实跑输出：正确值 head `ChecksumSHA256` 回写一致；错值 →
  `(BadDigest) … checksum mismatch: expected …, got …`；ck-bad.bin 事后 404（fail-closed 落盘验证）。
