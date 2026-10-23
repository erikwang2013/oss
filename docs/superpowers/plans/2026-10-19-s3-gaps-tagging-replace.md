# S3 缺口：对象标签 + CopyObject REPLACE 实施计划（工作令）

> **历史文档（2026-10-23 注）**：本文件记录当时的计划与边界。部分“未实现/非目标/后置”
> 已在 2026-10-23 能力清理批次或更早落地；以实现代码、README 与
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md` 为准。


> 功能规格 §3.5 / §3.7；设计文档 M4 规格缺口（2026-10-18 记）；README「已知缺口」147–148。
> **定位**：M8（S3 控制面服务）前置切片——把 M4 遗留的两项 S3 面缺口补齐，不触碰 M8 范围。
> **验收**（本切片自定）：
> ① `Put/Get/DeleteObjectTagging` 与 `x-amz-tagging` 完整往返（含 `versionId` 点名旧版本）；
> ② CopyObject `x-amz-metadata-directive: REPLACE` 用请求头替换元数据、`COPY` 继承源；
> ③ `x-amz-tagging-directive` COPY/REPLACE 两轴独立，Copy 到自身（同桶同键 + REPLACE）追加新版本；
> ④ 旧 `xl.meta`（无 tags 字段）可读，标签非法输入按 S3 错误码拒绝。
>
> **基线**：HEAD `7c4dd73`（M1–M7 完成）。本工作令只写文档；实施与测试由后续会话按切片执行，主会话统一跑测试。
>
> **v2 修订（2026-10-19，按 verifier-s3-gaps-plan 评审 B1/B2/N1–N8/R1–R5）**：
> 切片 0 拆为 0a/0b/0c；黄金摘要作第九次更新（实施后主会话回填）；调用点按实测 **40**
> 修正；补 5 个 `Version` 字面量与 `ObjectTag` 派生；marker 错误改 **405（Error::DeleteMarker，
> 与 GET/HEAD/AWS 一致）**；补 `x-amz-tagging-count`；`REPLICA` 摄入显式忽略标签；list 降级
> 条目 tags 固定空；e2e 新建 `TAG_BUCKET` 并登记 trap；切片 0 闸加 ③；补 R2/R4 测试与边界。
> 评审报告：`/tmp/plan-review-s3-gaps.md`。

## 钉子（防蔓延）

- **不做**（归后续/M8）：标签跨界复制（M6 已记档「锁/标签不复制」——本切片**不改变**该边界）；
  生命周期按标签过滤（M4 生命周期只做前缀，标签过滤未实现）；`s3:ExistingObjectTag` 条件键（策略引擎
  目前只有 Principal/Action/Resource，无条件求值）；对象通知的 `s3:ObjectTagging:Put/Delete` 事件；
  MPU 的 `x-amz-tagging`（CreateMultipartUpload 头，另一缺口）；控制台标签 UI / `ReplaceTags` 批量接口；
  独立标签表或索引；标签分页（上限 10，无分页）；ACL / 存储类。
- **不改**：`xl.meta` `FORMAT_VERSION`（只加 `Option` 字段，前向兼容契约）；复制入队逻辑与出站头；
  鉴权三档模型（Read/Write/Delete）；SSE-C 数据钥要求（标签是元数据操作，不要求客户钥——与 legal-hold
  既有口径一致）。
- **单写者**：切片串行；每片一个提交；发布/推送待用户口令。

## 关键裁决（先行定案，实施照此）

| # | 问题 | 裁决 | 理由 |
|---|------|------|------|
| A1 | 标签存哪 | **每个 `Version` 记录内**：`Version.tags: Option<Vec<ObjectTag>>`（`#[serde(default)]`）。删除 marker 恒 `None` | 与锁/加密/复制状态同模式；版本不可变 + 逐版本标签是 S3 语义；独立表要跨 meta/DB 两套提交点，且 E2E 需双写一致性 |
| A2 | 标签类型 | `ObjectTag { key: String, value: String }`；`None`/空 vec 统一在写端归一为 `None` | 保留客户端顺序；10 条上限内无需 map；空集与无标签在 S3 响应上等价 |
| A3 | 旧 meta 兼容 | 旧文件缺字段 → `None`（`serde(default)`）；解码器**跳过未知字段**的既有契约不变 | 与 is_delete_marker/lock/encryption 完全同路径，无需迁移 |
| A4 | 版本解析（无 `versionId`） | 取**链尾**；链尾是 marker → `NoSuchKey`（404）。**不向前穿透**到旧版本 | 与 GET/HEAD、legal-hold 既有口径一致；AWS 对已删对象的默认版本操作即 404 |
| A5 | 点名 marker 版本 | `Put/Get/DeleteObjectTagging` + `versionId`=marker → `Error::DeleteMarker`（**405**） | AWS/本仓库 GET/HEAD 口径；与 legal-hold 的 400 差异记档（verifier R1） |
| A6 | 未知 `versionId` | `NoSuchKey`（不引入 `NoSuchVersion`） | 除 A5 的 `DeleteMarker` 外，未知版本沿用 NoSuchKey；避免新增错误码翻译 |
| A7 | 字符集/长度 | `key` 1..=128、`value` 0..=256（按 **Unicode 标量值计数**，非字节）；允许字符 = Unicode 字母/数字 + 空格 + `+ - = . _ : / @`；重复键、控制字符 → 拒 | AWS 文档的字符表 + Unicode 类；`is_alphanumeric()` 覆盖中文；控制字符/换行天然被排除 |
| A8 | `x-amz-tagging` 解析 | 复用 `crate::sts::parse_form`（`&` 分段、`+`→空格、`%XX` 解码）；解码后做 A7 校验 | 现成且语义正确（URL Query 编码）；`crate::sts::parse_form` 已 `pub` |
| A9 | 非法标签错误码 | `InvalidTag`（400），消息带首条违规原因 | s3s 已有该码；AWS 口径 |
| A10 | 空 TagSet | 视为**清空标签**（PUT body 空集 / `x-amz-tagging` 空串 / DELETE 标签）；GET 回空 TagSet | MinIO 同口径；不新增错误分支 |
| A11 | 权限映射 | `GetObjectTagging` → `Gate::Need(Read)`；`Put/DeleteObjectTagging` → `Gate::Need(Write)`。桶策略 `s3_action`：Get→`s3:GetObject`，Put/Delete→`s3:PutObject`；补 R2 测试（桶策略/会话策略折叠后的 Allow/Deny 各一例） | 标签是对象元数据读写，不是对象删除；沿用现有粗粒度 Action，**不新增** `s3:Get/Put/DeleteObjectTagging` 词表（记档） |
| A12 | 审计/事件 | 不新增专用审计事件：`check()` 已对每个 op 落 `audit_log`（op 名即事件名）。通知事件不做 | 零代码换覆盖；S3 事件面记档 |
| A13 | Copy 元数据轴 | `metadata_directive` 缺省/`COPY` → 继承源 `content_type` + `user_metadata`；`REPLACE` → 用请求的 `content_type`/`metadata`，**未提供即空**（不继承） | 本实现只建模这两个字段（cache-control 等未落 meta，无需清） |
| A14 | Copy 标签轴 | `tagging_directive` 缺省/`COPY` → 继承源**被读版本**的 tags；`REPLACE` → 用 `x-amz-tagging`（缺省=空集）。两轴独立 | AWS 语义；COPY 时携带 `x-amz-tagging` 头按忽略处理（不报错，记档） |
| A15 | 指令非法值 | `metadata_directive`/`tagging_directive` 非 `COPY`/`REPLACE`（大小写敏感）→ `InvalidArgument` | s3s 不校验枚举（newtype 字符串）；必须自检 |
| A16 | Copy 到自身 | 同桶同键且无 `versionId`：仅当 `metadata_directive=REPLACE` 放行（版本桶追加新版本；非版本桶单版本覆盖）；其余仍 `InvalidRequest`。`tagging_directive=REPLACE` 单独**不解锁** | 规格 §3.5 的「无 REPLACE」例外；标签改写走 PutObjectTagging，不该借 copy-to-self 绕过 |
| A17 | 标签随版本 | PutObject 的 `x-amz-tagging` 落在**新版本**；Copy COPY 的标签来自**源版本**；`REPLICA` 摄入**显式忽略** `x-amz-tagging`（`replica` 判定后置空，实施 N7） | 保持 M6「标签不复制」边界 |
| A18 | SSE-C | 标签三 API 不要求客户钥（与 legal-hold 的元数据面口径一致，记档差异） | s3s 的 Tagging DTO 无 SSE-C 头；标签非秘密 |

## 切片总表（串行）

| # | 切片 | 交付 | 验收锚 |
|---|------|------|--------|
| 0a | 存储模型 | `ObjectTag` + `Version.tags` + 解码校验 + `validate_tags`；黄金摘要第九次更新；全部 `Version` 字面量补 `tags: None` | xl-store 编译 + meta 测试（摘要由主会话回填） |
| 0b | ObjectInfo + 调用点 | `ObjectInfo.tags`；`put_object_stream_with_meta` 新参数；**40 个调用点**（生产/薄壳 5 + 测试 35）补 `None`；list/mpu/read/version 字面量 | 全仓编译；xl-store + oss-server 测试（闸③） |
| 0c | store 标签方法 | `set_object_tags`/`get_object_tags`；marker 405；`tests_tags.rs`（≥8 例） | xl-store 测试全绿 |
| 1 | S3 标签 API | `Put/Get/DeleteObjectTagging` 三方法；PutObject `x-amz-tagging`（REPLICA 忽略）；`classify`/`s3_action` 映射；GET/HEAD `x-amz-tagging-count`；xl-s3 测试 | 标签往返 + 非法矩阵 + 版本点名 + tag-count |
| 2 | CopyObject DIRECTIVE | `metadata_directive` / `tagging_directive` 解析；REPLACE 元数据/标签；copy-to-self 放行条件；测试矩阵 | COPY/REPLACE 两轴矩阵 + 自身复制 |
| 3 | e2e + 文档 | `tests/e2e_s3.sh` 步骤 20（新建 `TAG_BUCKET` + trap）；README/设计/规格落档；全七闸 | e2e PASS（主会话跑） |

每片结束跑**七闸**（定义见文末）；0a 至少 ①⑥⑦；0b/0c 至少 ①③④⑥⑦；切片 1–2 至少 ①③④⑥⑦；
切片 3 全七闸 + e2e。每片一个本地提交，提交信息按仓库既有风格
（`feat(store): …` / `feat(s3): …` / `test(e2e): …` / `docs: …`）。

## 切片 0a：存储模型（meta.rs）

**交付**

1. `crates/xl-store/src/meta.rs`
   - 常量 `MAX_TAGS = 10`、`MAX_TAG_KEY_CHARS = 128`、`MAX_TAG_VALUE_CHARS = 256`。
   - `pub struct ObjectTag { pub key: String, pub value: String }`，派生
     `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`（`Version`/`ObjectInfo` 的派生要求）。
   - `Version` 末尾加 `#[serde(default)] pub tags: Option<Vec<ObjectTag>>`。
   - `pub fn validate_tags(tags: &[ObjectTag]) -> Result<(), String>`：A7 全部规则（键非空/Unicode 标量
     长度/字符集/重复键/总数）。解码 `XlMeta::decode` 对每个 `v.tags` 调用并映射为 `CorruptMeta`
     （信任边界，防手工构造的超大/非法集合原样回给客户端）。
   - 文件头注释补一行「标签 = 版本内 Option 字段」。
2. **全部 `Version` 全量字面量补 `tags: None`（7 处，无 `..Default`）**：
   - `meta.rs:404`（`sample()`）、`meta.rs:642`（`parts_tests::version()`）
   - `store/put.rs:669`、`store/version.rs:167`（marker）、`store/mpu.rs:493`
   - `store/tests.rs:1753`、`store/tests.rs:2189`
   （`meta.rs` 测试局部的 `FutureVersion/OldVersion` 结构体不动。）
3. **黄金摘要第九次更新（B1）**：`sample()` 加 `tags` 后 `golden_bytes_stable` 必然变化。实施者把
   `meta.rs` 注释“第八次”改为“第九次（M8 前置：Version.tags）”；主会话跑
   `cargo test -p xl-store golden -- --nocapture` 后回填新摘要（计划验收锚改为“除 golden 摘要外，
   旧用例零改断言”）。
4. 测试（meta 内）：旧无 tags `Version` 解码 → None；解码拒 11 条 / 129 字符键 / 重复键 / 控制字符；
   正向边界 10 条 / 128 字符键 / 256 字符值（含中文按标量计数）通过。

**涉及文件**：`meta.rs` + `store/{put,version,mpu,tests}.rs`（5 个字面量）。**验收锚**：
`cargo test -p xl-store`（0a 阶段允许 golden 待回填，其余绿）。

## 切片 0b：ObjectInfo + 调用点

**交付**

1. `crates/xl-store/src/store.rs`：`ObjectInfo` 加 `pub tags: Vec<ObjectTag>`（空 = 无标签）。
2. `store/read.rs::load_object_info`、`store/version.rs::version_info`：
   `tags: v.tags.clone().unwrap_or_default()`。
3. 其余 `ObjectInfo` 字面量（实际 5 处）补 tags：`store/put.rs:799`（返回体用归一后的 tags）、
   `store/version.rs:739`、`store/list.rs:255`（**降级条目固定 `Vec::new()`**，N6；`Some(info)` 分支已整体
   clone）、`store/mpu.rs:605`（Complete 取该版本 tags）、`store/read.rs:53`（第 2 项已列）。
4. `store/put.rs`：`put_object_stream_with_meta` 增加末参 `tags: Option<Vec<meta::ObjectTag>>`
   （已有 `#[allow(clippy::too_many_arguments)]`，不引入 builder）。
   - 入口先 `validate_tags`（非法 → `Error::InvalidArgument`）；`let tags = tags.filter(|t| !t.is_empty());`
     归一。
   - **移动顺序（N3）**：`Version { …, tags: tags.clone() }`；返回的 `ObjectInfo` 用
     `tags.unwrap_or_default()`（不要先 move 再 clone）。
   - 两个内部薄壳 `put_object_stream(_with_meta)` 传 `None`。
5. 全仓 **40 个调用点**（生产/薄壳 5 + 测试 35，N2）补 `None`，编译器驱动：
   `xl-s3/src/service.rs` 2、`oss-server/src/aether.rs` 1、`put.rs` 薄壳 2；测试
   `tests_crypto.rs` 22、`tests_lock.rs` 4、`tests_replication.rs` 1、`store/tests.rs` 3、
   `oss-server/tests/{lifecycle.rs 1, replication.rs 2, site_sync.rs 2}`。

**涉及文件**：上述。**验收锚**：`cargo test -p xl-store` + `cargo test -p oss-server`（闸③，N8）。

## 切片 0c：store 标签方法

**交付**

1. `crates/xl-store/src/store/version.rs` 新方法（模板 = `set_object_legal_hold` / `get_legal_hold`）：
   ```rust
   /// 写某版本标签（空 = 清除）。返回解析到的版本号（"null" 或 uuid）。
   pub async fn set_object_tags(&self, bucket: &str, key: &str,
       version_id: Option<&str>, tags: Vec<ObjectTag>) -> Result<String>;
   /// 读某版本标签。None = 链尾；链尾 marker → NoSuchKey；点名 marker → Error::DeleteMarker（405）。
   pub async fn get_object_tags(&self, bucket: &str, key: &str,
       version_id: Option<&str>) -> Result<(String, Vec<ObjectTag>)>;
   ```
   `set` 路径：`ensure_bucket` → `lock_object` → `read_meta`（无 → NoSuchKey）→ 解析版本（A4/A5/A6；
   **点名 marker → `Error::DeleteMarker`**）→ `validate_tags` → `v.tags = (!tags.is_empty()).then_some(tags)`
   → 链尾 `epoch = old.max_epoch()+1` → `refresh_latest_flags` → `write_meta(…, Some(&old))`。
   `get` 路径首行同样 `ensure_bucket`（缺桶 → NoSuchBucket），再解析版本但不取对象锁、不写。
   tag 写**不新增版本**。
2. 新测试 `crates/xl-store/src/store/tests_tags.rs`（样式照 `tests_replication.rs`，≥8 例）：
   - 版本桶：v1 写 tags → v2 写不同 tags → 逐版本读回互不污染；`versionId` 点名；
   - 非版本桶：写 tags → 覆盖写不带 tags → 旧 tags 不残留；`"null"` 版本可写读；
   - 清除（空 vec）后 GET 为空；无标签对象 GET 为空；
   - 链尾 marker：默认 GET/PUT → NoSuchKey；点名 marker → **405（DeleteMarker）**；
   - **tag 写不新增版本**（版本数不变）；
   - 旧 meta：手工编码一个无 `tags` 字段的 `Version` → 解码成功、tags 为 None；
   - 解码/写入拒非法：11 条 / 129 字符键 / 重复键 / 控制字符；正向 10/128/256 通过；
   - epoch 抬升：tag 写后 `max_epoch` 递增；
   - tags 与 legal-hold / replication-status 写互不覆盖（同一 meta 重写路径）。

**涉及文件**：`store/version.rs`、`store/tests_tags.rs`。**验收锚**：`cargo test -p xl-store` 全绿
（新增 ≥8 例，golden 已回填）。

## 切片 1：S3 标签 API

**交付**

1. `crates/xl-s3/src/service.rs`
   - 私有 helper（文件尾部，与其他 free fn 并列）：
     ```rust
     fn parse_tagging_header(raw: &str) -> S3Result<Vec<meta::ObjectTag>>;   // sts::parse_form(raw.as_bytes()) + 校验
     fn tagging_from_dto(t: &Tagging) -> S3Result<Vec<meta::ObjectTag>>;     // XML → 结构
     ```
     `validate_tags` 的 `Err` → `s3s::s3_error!(InvalidTag, "{reason}")`。XML 里
     `Tag.key == None || key.is_empty()` → InvalidTag（`<Key></Key>` 是 `Some("")`，N5）；
     `value == None` → 空串。`<TagSet></TagSet>`（空 vec）→ 清空；零字节 body 由 s3s
     `MissingRequestBodyError` 拒（不在本层处理）。
   - `put_object`：解构补 `tagging`；**先判 `replica`，若 `replica` 则 tags 置空（A17/N7）**，否则在读 body
     之前解析+校验（fail-fast）；把 tags 传给 `put_object_stream_with_meta`。`PostObject`（s3s 默认委托
     put_object）自动覆盖，补一条往返测试。
   - 三个 trait 方法：
     - `put_object_tagging`：`tagging_from_dto` → `store.set_object_tags(...)` →
       `PutObjectTaggingOutput { version_id: Some(vid) }`；
     - `get_object_tagging`：`store.get_object_tags(...)` → `GetObjectTaggingOutput { tag_set:
       Vec<Tag{key:Some,value:Some}>, version_id: Some(vid) }`；
     - `delete_object_tagging`：`let vid = set_object_tags(..., Vec::new()).await?` →
       `DeleteObjectTaggingOutput { version_id: Some(vid) }`（r2 补：客户端可见版本号）。
   - **GET/HEAD `x-amz-tagging-count`（R3）**：在 `get_object`（约 1146）与 `head_object`（约 1259）的
     响应构造处各一行：`tag_count = (!info.tags.is_empty()).then_some(info.tags.len() as i32)`
     （`TagCount = i32`）。
   - 错误翻译走既有 `to_s3_error`；点名 marker 由 store 返回 `Error::DeleteMarker` → 405（A5/R1）。
2. `crates/xl-s3/src/access.rs`
   - `classify`：读组加 `"GetObjectTagging"`；写组加 `"PutObjectTagging"`、`"DeleteObjectTagging"`。
     **必须与 service 方法同片落地**——`classify` 返回 None 对非管理员是 fail-closed 403（管理员走旁路
     到 501）。
   - 单测列表同步加三 op。
3. `crates/xl-s3/src/policy.rs`
   - `s3_action`：`"GetObjectTagging" => "s3:GetObject"`；
     `"PutObjectTagging" | "DeleteObjectTagging" => "s3:PutObject"`（A11，记档粗粒度折叠）。
   - **R2 测试**：桶策略 `Deny s3:GetObject` 对 GetObjectTagging 生效、`Allow s3:PutObject` 对 PutTagging
     生效；会话策略交集各一例（收紧行为需钉住）；"已知边界 4" 补一句折叠影响。
4. `crates/xl-s3/tests/object_ops.rs` 追加（复用现有 `service()`/`req()`）：
   - Put 带 `tagging: Some("a=1&b=2")` → Get 回两条；URL 编码 `k%20ey=v%2B1` / 字面量 `+`→空格解码正确；
   - Put 无 tagging → Get 空 TagSet；PostObject 表单 tagging 一条；
   - PutObjectTagging → Get → DeleteObjectTagging → Get 空；tag 写后版本数不变；
   - 版本桶：对旧 versionId 打标签，GET 默认版本不受影响；`versionId="null"` 在非版本桶可用；
   - **正向边界**：恰好 10 条、128 字符键、256 字符值、128 个中文字符键（按标量计数）→ 成功；
   - **非法矩阵**（`S3ErrorCode::InvalidTag`）：11 条、129 字符键、257 字符值、重复键、空键
     （XML `<Key></Key>`）、控制字符（header `%0A`）、非法字符（`!`）、坏转义 `%zz`；
   - 链尾 marker：默认 Get/Put → NoSuchKey；点名 marker → **405**；DeleteObjectTagging 同样覆盖；
   - GET/HEAD 有标签时回 `x-amz-tagging-count`，无标签时不回；
   - 缺桶 → NoSuchBucket；缺键 → NoSuchKey。

**涉及文件**：`service.rs`、`access.rs`、`policy.rs`、`tests/object_ops.rs`（+ policy 相关测试）。
**验收锚**：`cargo test -p xl-s3` 全绿；既有测试除新行为断言外零回归。

## 切片 2：CopyObject DIRECTIVE

**交付**（全部在 `crates/xl-s3/src/service.rs::copy_object`，2408 行起）

1. **先解析指令再判自身**（顺序重要，A15/A16）：
   ```rust
   let md = req.input.metadata_directive.as_ref().map(|d| d.as_str()).unwrap_or("COPY");
   let td = req.input.tagging_directive.as_ref().map(|d| d.as_str()).unwrap_or("COPY");
   // 非 COPY/REPLACE → InvalidArgument
   ```
   注意 `copy_source` 的 `ref` 借用仍在作用域内：指令/metadata/content_type/tagging 一律 `.clone()` 取用，
   不要移动 `req.input` 字段。
2. 自身复制放行：`version_id.is_none() && 同桶同键 && md != "REPLACE"` → 保持 `InvalidRequest`；
   `md == "REPLACE"` 放行（A16）。
3. 源读取后选择目标元数据/标签（**N4 类型**：`content_type` 保持 `Option<String>`，直接传；
   tags 包 `Some(...)`）：
   - `md == "REPLACE"` → `content_type = req.input.content_type.clone()`，
     `user_metadata = req.input.metadata.clone().unwrap_or_default().into_iter().collect()`；
   - 否则继承 `src_info.content_type` / `src_info.user_metadata`。
   - `td == "REPLACE"` → `tags = Some(parse_tagging_header(req.input.tagging.as_deref().unwrap_or(""))?)`
     （缺省 = 空集 → 清空）；否则 `tags = Some(src_info.tags.clone())`（源为点名版本时即该版本标签）。
4. 调 `put_object_stream_with_meta(..., content_type, user_metadata, checksum, dst_versioned, dst_sse,
   dst_lock, tags)`；其余（事件/复制入队/响应）不动。
5. 测试（`crates/xl-s3/tests/object_ops.rs` 追加）：
   - 默认 COPY：源带 content-type + metadata + tags → 目标三者全继承；
   - `REPLACE` + 仅 content-type：目标 metadata 清空、tags 仍继承（两轴独立）；
   - `REPLACE` + 仅 metadata：目标 content-type 为空（不继承）；
   - `tagging_directive=REPLACE` + `x-amz-tagging`：目标用请求标签；REPLACE 但不带 header → 清空；
     COPY 时携带 `x-amz-tagging` → 忽略、继承源（A14）；
   - **从旧 `versionId` Copy**：继承该版本 tags（非最新版本）；
   - copy-to-self：版本桶 `md=REPLACE` → 返回新 version_id、旧版本仍在且 tags 不变、新版本为新值；
     非版本桶同操作 → 单版本覆盖；`md=COPY` → InvalidRequest；仅 `td=REPLACE` → InvalidRequest；
   - 非法指令值 `replace`/`COPYX` → InvalidArgument；
   - `REPLACE` 标签非法 → InvalidTag，且目标对象**未产生**（先校验后写）。

**涉及文件**：`crates/xl-s3/src/service.rs`、`crates/xl-s3/tests/object_ops.rs`。
**验收锚**：`cargo test -p xl-s3` 全绿；copy 既有用例（bytes/同源自拒/缺源 404）零回归。

## 切片 3：e2e + 文档 + 定稿

1. `tests/e2e_s3.sh` 追加 **步骤 20（对象标签 + CopyObject REPLACE）**，沿用 `aws s3api`：
   - **新建 `TAG_BUCKET="tag-$(date +%s)"` 并加入脚本顶部 trap 清理列表**（B2；不复用步骤 16 已删的
     `$VB`），步骤内 `mb` + `put-bucket-versioning Status=Enabled`；
   - `put-object --tagging "env=prod&team=%E4%B8%AD%E6%96%87"` → `get-object-tagging` 断言两条且解码正确；
   - `put-object-tagging --tagging 'TagSet=[{Key=a,Value=1},{Key=b,Value=2}]'` → GET；`delete-object-tagging`
     → GET 空；
   - 版本桶：对旧 versionId 打标签 → 默认版本标签不受影响；tag 写后版本数不变；
   - Copy：源带 metadata+tags → 默认 copy 全继承；`--metadata-directive REPLACE --content-type text/x-t
     --metadata k=v` → 元数据替换、tags 仍继承；`--tagging-directive REPLACE --tagging "x=9"` → 标签替换；
     `--tagging-directive REPLACE` 不带 tagging → 清空；
   - copy-to-self（版本桶）`--metadata-directive REPLACE` → 新 version-id 且 `list-object-versions` 多一条；
     不带 REPLACE → 非零退出；
   - GET/HEAD 有标签时的 `x-amz-tagging-count` 断言；
   - 非法：11 条标签、129 字符键、重复键、`%0A` 控制字符 → 均非零退出。
   - 步骤编号 20 起（现尾步 19）；错误分支用 `if aws …; then echo …; exit 1; fi`，与既有步骤同风格。
2. 文档：
   - `README.md` 147–148 两条「未实现」改为已实现并给用法；若「已知缺口」小节因此清空，保留小节标题并写
     「无」或按现状收拢（不做无关重排）。
   - `docs/superpowers/specs/2026-10-10-minio-clone-design.md` M4 缺口段（474–475）更新：标签与 REPLACE
     已补，剩余缺口为控制台桶策略/审计 UI；
   - `docs/superpowers/specs/2026-10-10-feature-spec.md` §3.5 / §3.7 标注「已落地」+ 边界一句
     （标签不跨界复制、MPU 标签与控制台标签未做）。
3. 全七闸 + `tests/e2e_s3.sh`（主会话执行；实施者只写不跑）。
4. 定稿记录：七闸计数、e2e EXIT、树锚 md5；发布待口令。

**验收锚**：e2e 步骤 20 全绿；文档不再把两项列为缺口；七闸全绿。

## 校验/错误矩阵（实施对照表）

| 输入 | 结果 |
|------|------|
| 标签 >10 条 / 键 0 或 >128 字符 / 值 >256 字符 / 重复键 / 控制字符 / `+-=._:/@` 与字母数字空格之外 | `InvalidTag` 400 |
| `x-amz-tagging` 非法百分号转义（解码后残留 `%`） | `InvalidTag`（字符集校验兜底） |
| `metadata_directive` / `tagging_directive` 非 COPY/REPLACE | `InvalidArgument` 400 |
| 同桶同键 copy，无 versionId，`md != REPLACE` | `InvalidRequest` 400 |
| 同桶同键 copy，无 versionId，`md == REPLACE` | 成功：版本桶追加版本 / 非版本桶覆盖 |
| 标签 API 缺桶 | `NoSuchBucket` |
| 标签 API 缺键 / 未知 versionId / 默认版本为 marker | `NoSuchKey` |
| 标签 API 点名 marker 版本 | `Error::DeleteMarker` → **405**（与 GET/HEAD/AWS 一致；legal-hold 为 400，差异记档） |
| 标签 API 空 TagSet / 空 header | 成功（清空；零字节 body 由 s3s 拒） |
| Copy `td=COPY` 且带 `x-amz-tagging` | 成功，头被忽略（标签继承源） |

## 已知边界（落档）

1. **复制不携带标签**：M6/M7 出站 PUT 不含 `x-amz-tagging`，站点/远端副本的标签为空；标签变更
   （PutObjectTagging）也不触发复制。与本切片前的边界一致。
2. **生命周期标签过滤未实现**：规则解析仍只支持前缀；本切片不做。
3. **`s3:ExistingObjectTag` 条件键未实现**：策略引擎无 condition 求值；桶策略只能按 Action/Resource。
4. **动作词表折叠**：不识别 `s3:PutObjectTagging`/`s3:GetObjectTagging`/`s3:DeleteObjectTagging`；分别
   折为 `s3:PutObject`/`s3:GetObject`。只写 `s3:PutObjectTagging` 的策略在本实现中不生效（记档）。
   **反向影响**：既有 `s3:GetObject`/`s3:PutObject` 的 Allow/Deny 从此也覆盖标签读写（显式 Deny 对管理员
   同样生效）；会话策略交集同理（收紧，R2 测试钉住）。
5. **通知事件**：不发 `s3:ObjectTagging:*`。
6. **MPU 标签**：CreateMultipartUpload/CompleteMultipartUpload 的 `x-amz-tagging` 未接线，Complete 产物
   标签为空。
7. **PutObjectTagging 的 Content-MD5 不校验**（s3s 已消费 body；全局无此校验面）。
8. **SSE-C**：标签三 API 与 CopyObject 的标签轴不需要客户钥（元数据面）；数据面复制仍按现状要求。
9. **copy-to-self 的其它解锁条件**（仅换 SSE/存储类/website-redirect）仍 400；本切片只补 metadata REPLACE。
10. **REPLACE 未提供的头**：只有 content-type 与 `x-amz-meta-*` 被建模；cache-control 等从未落 meta。
11. 标签顺序保留客户端顺序；GET 不排序（S3 不保证顺序）。
12. `PutObjectTagging` 空集合按清除处理（AWS 未明确禁止）。
13. **marker 错误码**：标签 API 点名 marker → 405；legal-hold 仍 400（历史口径差异），不改 legal-hold。
14. **`x-amz-tagging-count`**：GET/HEAD 在标签非空时返回（实现于切片 1）；TagSet 本体只在 GetObjectTagging。

## 七闸（每片收口）

① `cargo test -p xl-store`（lib，含 kill9 两例）· ② kill9 专项 · ③ `cargo test -p oss-server`（lib + 全部
集成二进制）· ④ `cargo test -p xl-s3` · ⑤ `cargo test -p xl-client -p xl-proto` ·
⑥ `cargo clippy --workspace --all-targets -- -D warnings` · ⑦ `cargo fmt --all --check`。
切片 3 另跑 `tests/e2e_s3.sh`（需预起后端，按既有高负载跑法错峰）。

## 风险与对策

- **改动面大**：`put_object_stream_with_meta` 新参数触及 **40 个调用点**（生产/薄壳 5 + 测试 35）。对策：
  编译器驱动一次性补 `None`，纯机械；不建 builder（避免更大 churn）。
- **黄金摘要**：`Version.tags` 入编改变 `golden_bytes_stable` 的摘要（第九次）；主会话跑测试后回填，
  实施者不自行猜测摘要。
- **403 陷阱**：slice 1 若只加 service 方法漏 `classify`，请求会 fail-closed 403。对策：同片同提交 + 单测钉住。
- **自身复制内存**：copy 仍整对象读内存（既有实现），大对象复制非本切片目标；不新增流式改造。
- **epoch 抬升**：标签写抬链尾 epoch（照 legal-hold），集群合并语义一致；切片 0 用测试钉住。
- **旧开发库**：无 tags 字段的 meta 直接可读（A3）；无需迁移脚本。
