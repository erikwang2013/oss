# 自研分布式对象存储系统 —— 功能规格

- 日期：2026-10-10
- 定位：**功能层规格**。总体设计见 `2026-10-10-minio-clone-design.md`（架构/盘布局/数据结构）；本文件定义**每个功能的行为、接口、边界与错误**，是实现与验收之间的契约。
- 阶段标注：`M1`…`M9` 对应总体设计的阶段划分。

---

## 0. 全局约定

所有功能都建立在这五条之上，各功能章节不再重复。

### 0.1 三种入口，三套认证

| 入口 | 路径 | 认证 | 授权 |
|---|---|---|---|
| S3 协议 | `/`（catch-all fallback） | SigV4（Header 或 Query 预签名） | 策略求值（M4 起） |
| 控制台 API | `/api/v1/*` | JWT（`Authorization: Bearer`） | 角色 + 资源属主（M4 起） |
| 浏览器上传 | `/aetherupload/*` | 暂由会话 Cookie/JWT 覆盖层（M1 起） | 分组 → 目标桶映射 |

### 0.2 S3 错误响应格式

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/bucket/key</Resource>
  <RequestId>...</RequestId>
</Error>
```

**常用错误码与本项目的触发条件**（这是实现与测试的对照表）：

| Code | HTTP | 触发条件 |
|---|---|---|
| `NoSuchBucket` | 404 | 桶目录不存在 |
| `NoSuchKey` | 404 | `xl.meta` 不存在或没有可用版本 |
| `NoSuchVersion` | 404 | 指定 `versionId` 不存在 |
| `BucketAlreadyExists` / `BucketAlreadyOwnedByYou` | 409 | 创建已存在的桶 |
| `BucketNotEmpty` | 409 | 删除非空桶 |
| `InvalidBucketName` | 400 | 桶名不合规（§2.1 规则） |
| `KeyTooLongError` | 400 | 键 > 1024 字节 |
| `InvalidArgument` | 400 | 键含保留分隔符 / 路径穿越段（message 说明原因） |
| `EntityTooLarge` | 400 | 超过 `max_put_bytes`（M1）或 5 TiB 上限 |
| `EntityTooSmall` | 400 | 多段上传的某个 part < 5 MiB（非末段） |
| `AccessDenied` | 403 | 认证通过但策略拒绝 |
| `InvalidAccessKeyId` / `SignatureDoesNotMatch` | 403 | AK 不存在 / 验签失败 |
| `ExpiredToken` | 403 | STS 会话过期 |
| `RequestTimeSkewed` | 403 | 请求时间偏差 > 15 分钟 |
| `NoSuchUpload` | 404 | `uploadId` 不存在或已完成 |
| `InvalidPart` / `InvalidPartOrder` | 400 | 多段上传的 part 缺失/乱序 |
| `InvalidRange` | 416 | Range 越界 |
| `PreconditionFailed` | 412 | `If-Match` / `If-Unmodified-Since` 不满足 |
| `InvalidObjectState` | 403 | 对象在归档层且未取回 |
| `SlowDown` | 503 | sync 复制的远端不可达（§10） |
| `InvalidRequest` | 400 | 参数组合非法（含 S3 Select 语法不支持） |
| `NotImplemented` | 501 | 明确不支持的操作（如 S3 Object Lambda 的写路径） |
| `InternalError` | 500 | 盘错误、元数据损坏、quorum 不足 |

### 0.3 控制台 API 约定

- 所有响应 `application/json`
- 分页统一 `?limit=&cursor=`，响应带 `next_cursor`
- 错误体 `{ "error": { "code": "...", "message": "..." } }`
- 时间一律 Unix 纳秒（`i64`），前端负责格式化
- 未认证 → 401；已认证无权限 → 403

### 0.4 通用限制（可配，此处为默认值）

| 项 | 默认 | 可配 |
|---|---|---|
| 单对象上限 | 5 TiB | 否（S3 协议上限） |
| 单段 PUT 上限 | 256 MiB（M1 内存上限；M2 起为 5 GiB） | 是 |
| 多段 part 数 | 1..=10000 | 否 |
| 多段 part 大小 | 5 MiB .. 5 GiB（末段可小于 5 MiB） | 否 |
| `max-keys` | 1000 | 否 |
| 桶数 | 不限 | 是（每用户配额，M4） |
| 预设 URL 有效期 | 默认 1 小时，最大 7 天 | 是 |
| 纠删码参数 | 8+4 | 是（按盘数推导） |
| 小对象内联阈值 | 128 KiB | 是 |

### 0.5 一致性模型

| 操作 | 保证 |
|---|---|
| 新对象 PUT → 可读 | **强一致**（提交点在 `xl.meta`，读路径只认它） |
| 覆盖 PUT → 可读 | 强一致（同 key 的写由对象锁串行化） |
| DELETE → 不可见 | 强一致 |
| LIST | 强一致（直接走盘，不是缓存索引） |
| 跨节点（M3） | 写 quorum 达成即返回；读 quorum 内可读到最新已提交版本 |
| sync 桶跨站点（M9） | 远端确认后才返回；远端不可达 → 503 |

---

## 1. 认证与身份

### 1.1 SigV4 认证 —— `M1`

**做什么**：校验 S3 请求签名。s3s 完成规范请求串构造、签名比对、时间偏差检查；我们只提供「AK → SK」的解析。

**行为**
- 支持 Header 认证（`Authorization: AWS4-HMAC-SHA256 ...`）与 Query 预签名（`X-Amz-Signature`）
- 支持 `x-amz-security-token`（M5 起为 STS 会话）
- 时间偏差 > 15 分钟 → `RequestTimeSkewed`
- AK 不存在 / 非 active / 已过期 → `InvalidAccessKeyId`
- 签名不符 → `SignatureDoesNotMatch`
- 匿名请求：默认拒绝（除非桶策略显式允许匿名，M4）

**接口（控制台）**
| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/access-keys` | 列出当前用户的 AK |
| POST | `/api/v1/access-keys` | 创建 AK/SK（SK 只在此响应里出现一次） |
| DELETE | `/api/v1/access-keys/{ak}` | 删除 |
| PATCH | `/api/v1/access-keys/{ak}` | 启用/禁用 |

**边界**：SK 在 M1 明文存库（M5 改为加密存）；AK 全局唯一；一个用户可持有多把 AK。

### 1.2 控制台登录 —— `M1`

**行为**：用户名 + 口令 → JWT（默认 12 小时）。`sub` = 用户 id。无注册接口，管理员用环境变量引导。

**边界**：口令哈希在 M1 是 SHA-256 + 固定前缀盐（**不是** Argon2/bcrypt，M4 换）；无失败次数限制（M4 加）。

### 1.3 STS 临时凭证 —— `M5`

**做什么**：签发有时效的临时 AK/SK + SessionToken，供 SDK / 前端直传使用。

**操作**：`AssumeRole`、`GetSessionToken`、`AssumeRoleWithWebIdentity`。由 s3s 的 `S3Route` 前置层拦截（它们不是标准 S3 操作）。

**行为**
- 返回 `AccessKeyId` / `SecretAccessKey` / `SessionToken` / `Expiration`
- 会话落 `access_keys` 表：`expires_at` 非空 + 指向父密钥
- 请求带 `x-amz-security-token`：SigV4 验签通过后**还要校验会话未过期**，过期 → `ExpiredToken`
- **会话策略与用户策略取交集**（不能通过 AssumeRole 提权）
- 默认最长 1 小时，可配；超过用户策略上限的请求被拒

**边界**：不做 MFA；不做外部 IdP 的 OIDC 发现（WebIdentity 只校验我们信任的签发者列表）。

**落地记录（M5 切片 5-6，2026-10-17）**：`AssumeRole` / `GetSessionToken` 落地（s3s `S3Route`
前置拦截、Query/XML 形态）；`AssumeRoleWithWebIdentity` **裁剪延后**（成本评估见 M5 计划——
JWKS 拉取需出站 TLS 客户端新依赖栈；接入点已留）。语义落点：**AssumeRole 限管理员调用**
（「角色 = 用户别名」，无角色信任策略时防子账户自铸 admin 会话）；会话行落 `access_keys`
（`kind='session'`、`parent_ak` 父链一层、`expires_at` **必非空**、`session_policy` 可选）；
**会话策略与用户策略取交集**（在表三档 × 桶策略之后叠加：Deny → 拒 / 无匹配 → 拒 / Allow 不可
抬升）；`x-amz-security-token` 头随签名携带但 **token 本体不落库不校验**（AK 即凭据——s3s 的
S3Auth 接口只有 AK→SK，无头面；AWS 信任链差异记档）；过期会话 → `ExpiredToken`（§0.2 表）。
密文口径：会话 SK 与长时键同为 `enc:v1:` 信封加密存（切片 6）。

### 1.4 策略求值 —— `M4`

**做什么**：判断「这个身份对这个资源做这个操作」是否允许。

**求值顺序**：显式 Deny 优先 → 身份策略 Allow → 桶策略 Allow → 资源属主（同账号）→ 默认拒绝。

**边界**：支持 S3 策略的 Action / Resource / Principal / Condition 子集；不支持的条件键明确列出并拒绝（不静默忽略 —— 静默忽略一个 Condition 等于越权）。

---

## 2. 桶管理

### 2.1 创建桶 —— `M1`

**操作**：`CreateBucket`（PUT `/{bucket}`）、控制台 `POST /api/v1/buckets`

**桶名规则**（不合规 → `InvalidBucketName`）：3–63 字节；仅小写字母、数字、`-`、`.`；首尾不得为 `-` 或 `.`；不含 `..`；**不得以 `xn--` 开头**（S3 保留前缀）；不与已有桶重名；不同时形如 IP 地址。

**行为**：内核建目录 + 控制面插 `buckets` 行（owner = 当前用户）。已存在 → `BucketAlreadyOwnedByYou`。

**边界**：M1 无配额；M4 加每桶字节/对象数配额，超限 → `QuotaExceeded`（控制台 API）/ `SlowDown`（S3）。

### 2.2 删除桶 —— `M1`

**行为**：桶必须为空（含未完成的多段上传，M2 起）。非空 → `BucketNotEmpty`。成功则删目录 + 删 `buckets` 行（级联删桶策略/生命周期/复制/通知配置）。

**边界**：`force` 语义不存在 —— S3 要求客户端先清空。

### 2.3 列举桶 —— `M1`

**操作**：`ListBuckets`、`GET /api/v1/buckets`

**行为**：返回当前身份可见的桶（M1：全部；M4 起：按策略过滤），按名字典序。`creation_date` 取盘上目录的创建/修改时间。

### 2.4 桶配置类操作（各自的章节）

`HeadBucket`（M1，存在性探测）· `GetBucketLocation`（M1，恒返回配置的 region）· 版本控制（§6）· 对象锁（§7）· 加密（§8）· 生命周期（§9）· 复制（§10）· 通知（§11）· 清单（§15）· 标签（M4；2026-10-19 作为 M8 P0 补齐）· 策略（M4）· ACL（**不做**，仅支持 `private` 与 `bucket-owner-full-control` 两种 canned ACL，其余 → `NotImplemented`）

---

## 3. 对象读写

### 3.1 PutObject —— `M1`

**操作**：`PutObject`、`POST /api/v1/buckets/{bucket}/objects/{*key}`（控制台）

**行为**
1. 校验桶存在、键合法
2. 流式写数据到 `.oss.sys/tmp/<uuid>/part.1`，边写边算 MD5 + SHA256 + size
3. 写候选 `xl.meta` → **同盘 rename** 数据就位 → 覆盖 `xl.meta`（提交点）
4. 老版本目录进回收站
5. 发 `s3:ObjectCreated:Put` 事件（M6）

**响应头**：`ETag`（单段 = 整份 MD5，带引号）、`x-amz-version-id`（M4 起）

**请求头语义**
| 头 | M1 | 说明 |
|---|---|---|
| `Content-Type` | ✅ | 存 meta，GET 时回 |
| `x-amz-meta-*` | ✅（M2a 已落地） | 用户元数据，总计 ≤ 2 KiB；全量替换语义（非合并） |
| `x-amz-storage-class` | ❌（M8） | 默认 STANDARD |
| `x-amz-server-side-encryption*` | ❌（M5） | |
| `x-amz-tagging` | ❌（M4） | |
| `x-amz-object-lock-*` | ❌（M5） | |
| `Content-MD5` | ✅（协议补全计划 Task 3 落地） | 与计算值不符 → `BadDigest`（提交点前 fail-closed，对象不落盘） |
| `x-amz-checksum-*` | ✅（协议补全计划 Task 3 落地；单段 PUT 校验并回写，CRC32C/SHA1/SHA256/SHA512/CRC64NVME，206 不回写；**part 级校验和未校验、组合语义留 M2b**） | |

**边界**：M2a 起流式直通内核（不再整份读内存），单次 PUT/单 part 仍受 `max_put_bytes` 上限。

### 3.2 GetObject —— `M1`（Range：M2a 已落地）

**行为**：读 `xl.meta` → 定位版本目录 → 流式返回。响应带 `Content-Length`、`Content-Type`、`ETag`、`Last-Modified`。

**`response-content-*` 查询参数**（覆盖响应头；协议补全计划 Task 2 已落地）：`response-content-type`、`response-content-disposition`（含 `&filename=` 的 RFC 5987 编码）、`response-cache-control` 等。

**Range（M2a 已落地）**：`bytes=0-99`、`bytes=100-`、`bytes=-100`；越界 → `InvalidRange`（416）；**多区间（`bytes=0-9,20-29`）在 s3s 头部解析层即拒 → 400 `InvalidArgument`**（实测，协议补全计划 Task 4 对齐）；响应 `206` + `Content-Range`；内核按 part 序列跨段映射、只读命中段。HEAD 带 Range 不支持（仍回 200 + 全量长度，契约有用例钉住）。EC 重建的按需解码随 M2b。

**条件请求（协议补全计划 Task 1 已落地）**：`If-Match` / `If-None-Match`（ETag）/ `If-Modified-Since` / `If-Unmodified-Since` → 不满足时 `412` 或 `304`（`If-None-Match` 命中 → `304 Not Modified`，无 body；RFC 9110 §13.2.2 求值序短路；ETag 按值比较不分强弱）。

### 3.3 HeadObject —— `M1`

同 GetObject 但不返回 body。**注意语义**：任何错误一律 404（指对象/桶不存在的读语义；键不合规仍按 §0.2 报 400；不返回具体错误码），这是 S3 的既有行为，客户端（如 rclone）依赖它。

### 3.4 DeleteObject / DeleteObjects —— `M1`

**行为**
- 删不存在的键 → **成功**（幂等）
- 无版本控制的桶：删对象目录
- 有版本控制的桶（M4）：插入 delete marker；带 `versionId` 则永久删该版本
- 发 `s3:ObjectRemoved:Delete` / `DeleteMarkerCreated` 事件（M6）

**DeleteObjects（批量）**：一次最多 1000 个；部分失败时 `Errors` 数组列出失败项，成功的仍在 `Deleted` 里。**不做事务回滚**。

### 3.5 CopyObject —— `M1`（COPY 指令 M2a 落地；REPLACE 两轴 2026-10-19 落地）

**行为**：`x-amz-copy-source` 形如 `/{src-bucket}/{src-key}`（可带 `?versionId=`）。M1 实现为「读出源字节 → 写入目标」（真实复制）；M2 起改为**服务端分片级复制**（不经过内存）。

**`x-amz-metadata-directive`**：`COPY`（默认，保留源元数据）——M2a 已落地；`REPLACE`（用请求头里的元数据）未实现。

**边界**：源 = 目标（同桶同键、无 REPLACE、无存储类变更）→ `InvalidRequest`（S3 语义）；跨桶复制需要目标桶的写权限（M4 起）。

### 3.6 多段上传 —— `M2a 已落地`（UploadPartCopy / ListMultipartUploads 留 **M4+**——与 README 的 Multipart 边界口径一致，具体批次随 M4 计划排）

| 操作 | 行为 |
|---|---|
| `CreateMultipartUpload` | 返回 `uploadId`；在对象目录下建 `<uploadId>/` 暂存区 |
| `UploadPart` | 写 `<uploadId>/part.N`；每个 part 独立算 MD5；part 号 1..=10000；返回该 part 的 ETag |
| `UploadPartCopy` | 从已有对象复制区间作为 part（`x-amz-copy-source-range`）——**M4+** |
| `CompleteMultipartUpload` | 按 part 号升序拼接；**校验每个 part 的 ETag 与请求里声明的一致**（不一致 → `InvalidPart`）；末段 < 5 MiB 允许，中间段 < 5 MiB → `EntityTooSmall`；乱序 → `InvalidPartOrder` |
| `AbortMultipartUpload` | 删 `<uploadId>/` 全部内容 |
| `ListParts` | 列举已上传的 part（M2a 不分页：≤10000 全量，`isTruncated=false`） |
| `ListMultipartUploads` | 列举未完成的上传——**M4+** |

**多段 ETag 格式**：`<所有 part 的 MD5 拼接后再取 MD5>-<part 数>`（这是 S3 的定义，客户端靠它判断对象是否完整）。**注意**：因此多段上传的对象 ETag **不是**整份 MD5 —— 控制台要如实展示，不要假装它是校验和。

**边界**：未完成的上传占用磁盘；`clean` 命令按 mtime 回收（默认 7 天）。`CompleteMultipartUpload` 是所有上传中唯一**非原子可见**的操作：拼接期间对象不可见，拼接完成后一次性提交 `xl.meta`。

### 3.7 对象标签与元数据 —— `M2`/`M4`（标签 2026-10-19 落地；边界：不跨界复制、MPU/控制台标签未做）

`PutObjectTagging` / `GetObjectTagging` / `DeleteObjectTagging`（M4）—— 最多 10 个标签，键 ≤ 128 字符，值 ≤ 256 字符。`x-amz-meta-*` 用户元数据（**M2a 已落地**）—— 总计 ≤ 2 KiB（大小写归一化行为以 s3s 实测为准）。

---

## 4. 列举

### 4.1 ListObjectsV2（推荐）/ V1 —— `M1`

**参数**：`prefix`、`delimiter`、`max-keys`（默认 1000，上限 1000）、`continuation-token`（V2）/ `marker`（V1）、`start-after`、`encoding-type=url`、`fetch-owner`

**行为**
- 键按 **UTF-8 字节序** 升序返回（不是字典序，中文会不同 —— 测试要覆盖）
- `delimiter` 命中时，公共前缀聚合成 `CommonPrefixes`，不返回其中的对象
- 截断时返回 `NextContinuationToken`（M1 实现为「本页未返回的第一个键」，恢复时包含该键本身；token 不透明，客户端不该解析）
- `encoding-type=url` 时键做百分号编码（含 XML 非法字符的键必须编码）

**响应字段**：`Contents[].{Key,LastModified,ETag,Size,StorageClass,Owner}`、`KeyCount`、`IsTruncated`、`CommonPrefixes[]`

### 4.2 ListObjectVersions —— `M4`

按版本列举，含 `DeleteMarkers` 与 `IsLatest`。参数 `key-marker` / `version-id-marker`。

---

## 5. 上传（aetherupload 链路）

这一节是**浏览器路径**，与 §3 的 S3 路径相互独立、最终写同一套 `xl.meta`。

### 5.1 预处理 —— `M1`

**路由**：`POST /aetherupload/preprocess`（默认路径，可配）

**请求字段**：`resource_name`（原始文件名）、`resource_size`、`group`（上传分组）、`resource_hash`（整份 MD5，秒传用）

**响应 JSON**：`error`（"0" 表示成功）、`chunkSize`、`resourceTempBaseName`、`groupSubDir`、`resourceExt`、`savedPath`（秒传命中时非空）

**行为**：校验分组与参数 → 生成临时名与子目录 → **秒传判定**（命中则一个分块都不用传）→ 建空 `.part` → 写 `chunkIndex=0` 到 `_header`

**错误一律 HTTP 200 + `error` 字段**（协议与 PHP 版一致，前端按 `error` 真值判断 —— 不要改成 HTTP 错误码）。

### 5.2 分块 —— `M1`

**路由**：`POST /aetherupload/uploading`

**每块的顺序固定**：校验 → 追加 → 回写 `chunkIndex`。重发同一序号幂等跳过；序号跳变或分块被截断**只返回错误、不清理进度**（弱网下客户端可持续重试）。

**末块**：校验整份大小 → 按真实内容复核 MIME → 触发「上传完成前」事件 → 流式重算整份 MD5 → 与客户端声明的 hash 比对（`lax_mode` 关闭时）。不一致 → **整份丢弃**。

### 5.3 秒传 —— `M1`

**索引**：`instant_index` 表（键格式 `aetherupload:resource:<分组>_<hash>`，每条独立 TTL，默认 7 天）

**行为**：命中时预处理阶段直接返回 `savedPath`，零分块传输。

**边界**：**索引与实际文件是两份数据** —— 删除资源必须走 `Runtime::delete_resource()`（联动清索引），否则留死链；建议每天跑一次 `build` 重建。

### 5.4 原生 ingest —— `M1`

**做什么**：`upload_complete` 事件里把成品文件装进存储内核。

**映射规则**：分组 → 目标桶（`oss.toml` 的 `[[groups]]`）→ 对象键 `<groupSubDir>/<md5>.<ext>`。

**ETag 白拿**：aetherupload 已算完整份 MD5 → 直接就是单段 PUT 的 ETag，不重算。

**行为**：读暂存文件 → `store.put_object` → 写 `uploads` 表（saved_path → bucket/key/size/etag）。

**错误语义**：ingest 失败 = 上传失败（事件里抛错等同于上传失败 —— 这是 aetherupload 的契约）。**不允许静默吞掉**：宁可让用户重传，也不能返回成功却没落进对象存储。

**边界**：暂存盘与数据盘是两个目录；暂存文件在 ingest 成功后**不删除**（aetherupload 的显示/下载路由仍要读它）。由 `clean` 按 mtime 回收。

### 5.5 分组与限制 —— `M1`

分组 = 上传档位。每组独立的：目标桶、大小上限（0 = 不限）、扩展名白名单、两个事件开关。

**分组名不能含下划线**（它参与 `savedPath` 的三段式编码，含下划线会让解码错位、该分组资源永久 404）。配置与上传两阶段都拒。

---

## 6. 版本控制 —— `M4`

### 6.1 配置

`PutBucketVersioning` / `GetBucketVersioning` —— 状态 `Off` / `Enabled` / `Suspended`。**开了不能真正关回 Off**（S3 语义：只能 Suspended）。

### 6.2 行为

| 场景 | Enabled | Suspended |
|---|---|---|
| PUT | 新版本，`version_id` = uuid | 新版本覆盖 `null` 版本 |
| DELETE（无 versionId） | 插入 delete marker，`version_id` 新 uuid | 删 `null` 版本 |
| DELETE（带 versionId） | 永久删该版本（对象锁可能阻止） | 同左 |
| GET（无 versionId） | 返回最新非 delete marker 版本 | 同左 |
| GET（带 versionId） | 返回指定版本 | 同左 |

**响应头**：所有写操作返回 `x-amz-version-id`。

**`xl.meta` 的 `versions` 数组**在这一阶段从「恒一条」变为多条，靠 `is_latest` 与 `mod_time_nanos` 排序。**格式版本号已在 M2 预留，不需要数据迁移**。

### 6.3 列举与分页

`ListObjectVersions`，分页键是 `(key, version_id)` 二元组。

---

## 7. 对象锁 —— `M5`

**前置**：**必须启用版本控制**（没有版本就没有「保留某个版本」的概念）。

### 7.1 配置

`PutObjectLockConfiguration` / `GetObjectLockConfiguration` —— 桶级开关 + 默认保留模式与时长。**只能在建桶时或未启用版本控制时开启**（S3 语义）。

### 7.2 保留与合法保留

| 操作 | 说明 |
|---|---|
| `PutObjectRetention` / `GetObjectRetention` | 设/查 `retain_until_date` 与模式 |
| `PutObjectLegalHold` / `GetObjectLegalHold` | 设/查合法保留（`ON`/`OFF`） |
| PUT 时带 `x-amz-object-lock-*` | 一次性设置 |

**模式**
- **GOVERNANCE**：持有 `s3:BypassGovernanceRetention` 权限者可带 `x-amz-bypass-governance-retention: true` 提前解除
- **COMPLIANCE**：**任何人**（含管理员、root）在到期前都无法删除或缩短保留期 —— 这是合规审计的意义所在

### 7.3 拒绝规则

| 尝试 | 结果 |
|---|---|
| 删除被保留版本的 DELETE | `AccessDenied` |
| 覆盖被保留版本（Suspended 桶里的 `null` 版本） | `AccessDenied` |
| 缩短 `retain_until_date` | `AccessDenied` |
| 合法保留 `ON` 时的任何删除 | `AccessDenied` |

**边界**：对象锁**不阻止**读。Compliance 模式的保留**不可逆** —— 设置界面必须有明确的二次确认文案。

**落地记录（M5 切片 4，2026-10-17）**：桶级开关语义按 AWS 修正——`PutObjectLockConfiguration`
要求**版本控制已 Enabled**（否则 400 `InvalidBucketState`；建桶旗标
`x-amz-bucket-object-lock-enabled` 会一并自动开版本控制），**Enabled 只能开不能关**
（二次 Put 只许改默认保留规则）；本条原「只能在建桶时或未启用版本控制时开启」表述作废。
`PutObjectLegalHold` / `GetObjectLegalHold` 已落地（`put` 须点名版本且桶已开锁——未开锁 400；`get` 为元数据面读，SSE-C 对象不要求客户钥，与 AWS 须带数据钥的差异记档）；**`Put/GetObjectRetention`
未接线**（延后，见 M5 计划定稿记档）。拒绝规则均已落地：点名删版本 / 批量删 / 非版本化覆盖写
回收一致判定（到期可删；COMPLIANCE 含管理员一律拒；GOVERNANCE 仅 bypass 头放行；
Legal Hold ON 一律拒），拒删 = 403 `AccessDenied`。无版本号 DELETE 照打 delete marker
（锁定版本天然留存）。控制台锁状态展示延后（S3 API 面完整）。

---

## 8. 服务端加密（SSE） —— `M5`

**硬约束：先加密，再纠删码。** 写 = 明文按 part 流式加密 → EC 编码 → 落盘；读 = EC 重建 → 解密。这样单个分片流出（不足 quorum）不含任何明文信息。

### 8.1 三种模式

| 模式 | 密钥来源 | 盘上存什么 | 请求头 |
|---|---|---|---|
| SSE-S3 | 服务端主密钥（配置/KMS） | 包装后的 DEK | `x-amz-server-side-encryption: AES256` |
| SSE-KMS | 外部 KMS（含文件型实现） | 包装后的 DEK + `key_id` | `...: aws:kms` + `x-amz-server-side-encryption-aws-kms-key-id` |
| SSE-C | 客户端每次请求携带 | **只存 HMAC 用于校验密钥正确性** | `x-amz-server-side-encryption-customer-*` |

### 8.2 行为

- 每对象一个 DEK；主密钥只做包装（envelope）→ **换主密钥不重写数据**，只重新包装 DEK
- nonce 由 `nonce_seed` + part 号派生，保证同一 DEK 下不重用
- 读 SSE-C 对象：密钥不匹配 → `AccessDenied`（不泄露「密钥错了」还是「对象不存在」）
- 响应回显 `x-amz-server-side-encryption` 头
- **`HEAD` 也回显**加密头（客户端靠它判断是否需要带密钥）

### 8.3 边界

- **不做**：客户端侧加密（CSE）、S3 代理加密、加密上下文的复杂条件
- 元数据本身**不加密**（不含秘密；用户元数据里的敏感信息由用户自己负责 —— 写入文档）
- 换主密钥（`key_id`）后旧对象仍可读（DEK 未变，只是包装主密钥变了）
- KMS 不可达 → 拒写（`InternalError`），**不降级为不加密**

**落地记录（M5 切片 1-3，2026-10-16/17）**：§8.2 的「nonce 由 `nonce_seed` + part 号派生」
落为**块对齐单元号**派生（nonce = 种子‖BE32(单元号)，单元号把（EC 分片, 块）双射到密文单元，
65520B 明文 → 65536B 密文，DARE 式；同 DEK 下 nonce 不重用这一目标不变，且 Range 读与
分片级重编码天然成立）。§8.1 的「SSE-C 只存 HMAC」落为 **`base64(MD5(key))`**（AWS
customer-key-MD5 同款）。§8.2 的「读 SSE-C 密钥不匹配 → `AccessDenied`」落为
**400 `InvalidArgument`**（AWS 对 SSE-C 错钥即 4xx 客户端错，非 403；格式不符/缺头同 400）。
SSE-KMS 显式带 CMK（`…-aws-kms-key-id`）→ **400 拒绝**（多主密钥轮换编排未做）。
桶级默认加密（`PutBucketEncryption`）未实现。aetherupload 内部摄取不带加密（明文入盘，记档）。

---

## 9. 生命周期 —— `M4`（分层转换在 `M8`）

### 9.1 配置

`PutBucketLifecycleConfiguration` / `GetBucketLifecycleConfiguration` —— 最多 1000 条规则，每条：`ID`、`Status`（Enabled/Disabled）、`Filter`（前缀/标签/大小）、`Expiration`、`Transition`、`NoncurrentVersionExpiration`、`AbortIncompleteMultipartUpload`

### 9.2 行为

- 规则由**作业引擎**驱动（§16）：每日扫描，按规则生成删除/转换任务
- `Expiration.Days` / `.Date`、`ExpiredObjectDeleteMarker`
- `Transition` 只支持同层内与「转到 tier 后端」（§14），存储类名须在允许列表内，否则 `InvalidStorageClass`
- `AbortIncompleteMultipartUpload.DaysAfterInitiation`
- 转换与过期都会发事件（`s3:LifecycleTransition` / `s3:LifecycleExpiration:*`）

### 9.3 边界

- 规则**最小粒度是对象**，不是版本（`NoncurrentVersionExpiration` 除外）
- 到期删除**不是精确时刻**：命中窗口由扫描周期决定（默认每日），文档要写清楚

---

## 10. 复制

### 10.1 桶复制 —— `M6`

**配置**：`PutBucketReplication` / `GetBucketReplication` / `DeleteBucketReplication` —— 需要**版本控制已启用**。每条规则：`ID`、`Priority`、`Filter`（前缀/标签）、`Destination`（桶 + 存储类 + 账号）、`DeleteMarkerReplication`、`ExistingObjectReplication`。

**行为**
- 复制由**持久化队列**驱动（`repl_outbox` 表）：对象提交成功后写队列 → worker 异步推远端 → 更新 `x-amz-replication-status`
- 状态流转：`PENDING` → `COMPLETED` | `FAILED`（失败进重试队列，指数退避）
- 远端过滤出的对象标记为 `REPLICA`，**不参与再复制**（防循环）
- 删除标记默认**不复制**（`DeleteMarkerReplication: Disabled` 是 S3 的默认值 —— 别搞反）
- 需要目标桶的写权限与目标桶的版本控制

**限速**：可配带宽上限（否则一条规则能打满出口）。

### 10.2 跨站点复制 —— `M7`（异步）/ `M9`（同步）

**异步（M7，默认）**：双向；版本不可变 → 实际是追加式传播；桶/策略/IAM 变更也复制（控制面
日志）。桶删除等命名空间变更按**站点优先级**裁决。

> **实现注记（2026-10-18）**：M7 收缩为单节点站点对；控制面冲突实际采用 LWW
> `(ts, priority, origin_site_id, seq)` —— **ts 优先，priority 只在同秒裁决**，再以 origin 字典序与
> seq 破同值。上述「按站点优先级裁决」的早期表述以本注记为准。范围收缩与已知边界（非版本桶
> 删除不跨界、非空桶删除不生效、版本 UUID 两端不同、锁/标签/SSE 不复制、明文 http、同主密钥
> 约束等）见设计文档 §7.3 与 M7 落地状态。

**同步（M9，按桶 opt-in）**：`replication_mode: sync`。提交 = 本地 staging/tee + 写 intent →
远端 token PUT 确认**同一 origin version id** → 本地 meta 提交。删除 remote-first；
`DeleteObjects` 远端不可达/超时/5xx 时整体 503（已成功的键已经提交），确定性 4xx 保留为
逐键 Errors；无 versionId 的 marker 为 at-least-once（对象不可见幂等，版本列表可能多
marker）。**sync 与 stretch 互斥**：
`[stretch]` 集群拒绝 sync 桶，stretch 也不能与既有 sync 桶混用。

**CAP 选择必须显式**：sync 桶在远端不可达时**写返回 503**。**不做自动降级** —— 降级了「强一致」这个词就没有意义。此约束必须出现在桶创建界面。

**同步控制面（M9）**：IAM / 桶 / 策略 / 访问点用 `openraft` 跨站点强一致（状态小、变更低频，Raft over WAN 成立）。生产形态 3 voters：A/B 两个数据站点 + C 控制面 witness，quorum 2；C 不存对象、不在 `sites` 表。

> **实现注记（2026-10-22，S0–S9 已实施；残余证据见下）**：模式/配置守卫、sync 写门
> （同 origin version id、tee 0600/TTL、写 intent compensate-only）、remote-first 删除
> （内部 token 路由、删桶 quiesce、删除 intent）与 stretch 2×6 数据面已落地并有测试；
> openraft 0.9.25 控制面已接 server/router（11 表单命令 apply、启动模式断言、witness
> 独立运行时、3-voter 成员/配对、快照、cutover/export/force-recover、`sync-reconcile
> [--repair]`）。`RAFT_READY`/`SYNC_READY`/`STRETCH_READY=true`；`production_ready` 需
> ≥3 voters。本机验收：strict e2e_m9 71 PASS/0 FAIL/1 预期 SKIP、e2e_stretch ALL PASS、
> M7 e2e_sites PASS、七闸全绿。残余：stretch+Raft 配对组合证据未归档
> （B′/cutover/repair 专用 e2e 见 `tests/e2e_m9_recovery.sh`；11 表并发 diff 见 `tests/e2e_m9.sh`）。
> stretch 限制：2×6、EC 8+4、`Q_s=9`、单站点不可
> 读写、不支持站点容灾、WAN 吞吐受限、inline=0、禁用 M7 异步站点复制、拒绝 sync 桶、
> 仅全新空库、`self_addr` 为独立 mesh gRPC 端口。

---

## 11. 事件通知 —— `M6`

### 11.1 配置

`PutBucketNotificationConfiguration` —— 每个桶最多 100 条，指向一个 Kafka 目标（`arn:oss:sqs::<id>:kafka` 形式，沿用 MinIO 的 ARN 惯例）。每条含 `Id`、`Events[]`、`Filter`（前缀/后缀）。

### 11.2 事件类型

`s3:ObjectCreated:{Put,Post,Copy,CompleteMultipartUpload}` · `s3:ObjectRemoved:{Delete,DeleteMarkerCreated}` · `s3:ObjectAccessed:{Get,Head}` · `s3:ObjectRestore:{Post,Completed}` · `s3:LifecycleTransition` · `s3:LifecycleExpiration:{Delete,DeleteMarkerCreated}` · `s3:Replication:{OperationFailedReplication,OperationMissedThreshold,OperationReplicatedAfterThreshold}`

### 11.3 投递语义

- 事件先落 `event_outbox` 表，worker 异步投递 → **Kafka 宕机不丢事件**，恢复后按 seq 续发
- **至少一次**投递（可能重复，消费者须幂等）—— 这是行业惯例，文档必须写明
- 记录结构含 `eventVersion`、`eventName`、`eventTime`、`s3.bucket`、`s3.object`（含 `key`、`size`、`eTag`、`versionId`、`sequencer`）、`source`（含 `host`、`port`、`userAgent`）
- 投递失败重试带退避，超过次数进死信（控制台可见）

### 11.4 边界

- 不做 webhook / AMQP / SNS 目标（**但底层用 `ecat-mq` 的 `MessageQueue` trait 抽象**，加后端只需新 contrib crate）
- `ObjectAccessed` 事件**默认关闭**（GET 频率极高，打开前先算量）

---

## 12. S3 Select —— `M6`

### 12.1 操作

`SelectObjectContent`（POST `/{bucket}/{key}?select&select-type=2`）

### 12.2 支持的输入

| 格式 | 说明 |
|---|---|
| CSV | 分隔符、引号、转义、表头行、注释字符、记录分隔符可配 |
| JSON | `Document`（每行一个 JSON 对象）与 `Lines`；需配 `Type` 与可选的 `CompressionType` |
| Parquet | **后置**（需 arrow 依赖，作为 stretch） |

**压缩**：`NONE` / `GZIP` / `BZIP2`（`CompressionType` 参数）。**不支持** lz4/zstd（AWS 也不支持）。

### 12.3 SQL 子集（AWS 方言，**不是真 SQL**）

```
SELECT <列表达式列表 | *> FROM S3Object [alias] [WHERE <布尔表达式>] [LIMIT n]
```

- 列引用：CSV 用 `s._1`；JSON 用 `s.name`、嵌套 `s.a.b`
- 支持：比较（`= <> < <= > >=`）、`AND/OR/NOT`、`BETWEEN`、`IN`、`LIKE`、`IS NULL`、`CAST`
- 函数子集：`substring`、`trim`、`lower`、`upper`、`char_length`、`abs`、`ceil`、`floor`、`round`、`cast`、聚合 `count(*)`、`sum`、`min`、`max`、`avg`
- 明确列表之外 → `InvalidRequest`（**不静默返回错结果**）

### 12.4 输出

AWS event-stream 帧（prelude + headers + payload + CRC32）：`Records`（分片流式）、`Progress`（每约 1 MiB 一次）、`Stats`（`BytesScanned`/`BytesProcessed`/`BytesReturned`）、`End`。

**边界**：`ScanRange` 支持（`Start`/`End` 字节区间）；支持 `RequestProgress.Enabled`（按 §12.4 的 ~1 MiB 节奏发 `Progress` 帧——设计文档 M6 验收依赖 `--request-progress` 的帧格式）；不做其他交互式查询；**不解压后随机寻址**（GZIP 必须顺序解压）。

---

## 13. Access Points 与 Object Lambda —— `M8`

> **落地状态（2026-10-20）**：S1 路径式 AP 与 S2 Object Lambda 均已落地（HTTP 变换器默认、
> WASM `olap-wasm` feature 门控）；`<name>-s3alias` 主机名、匿名/跨账号 AP 与写变换不做。

### 13.1 Access Points

**机制**：**新建访问点 CRUD 不在 S3 协议里**（`CreateAccessPoint` 属于 S3 Control API，独立服务、独立端点、独立签名）→ 走控制台 API；而**使用**走 `S3Route` 前置层，与标准 S3 请求共用同一条解析路径。

**寻址**：`/ap/<name>/<key>` 路径前缀（可选主机名形式 `<name>-s3alias`，需通配 DNS + 证书）。

**策略求值**：访问点策略 ∩ 桶策略 ∩ 身份策略，**取交集**（三者都允许才放行）。

**控制台 API**：`GET/POST /api/v1/access-points`、`GET/DELETE /api/v1/access-points/{name}`、`PUT .../{name}/policy`

**边界**：仅路径式 `/ap/<name>/<key>`（无 `<name>-s3alias` 主机别名）；不做网络来源限制
（VPC endpoint 概念不适用）；不做匿名/跨账号访问点。

### 13.2 Object Lambda

**做什么**：读路径的响应变换。

**变换器形态**
| 形态 | 默认 | 说明 |
|---|---|---|
| HTTP 变换器 | ✅ | POST 对象字节 + 元数据到外部端点，收变换结果。**无重依赖** |
| WASM 变换器 | cargo feature 门控 | `wasmtime` 沙箱内执行，窄 ABI：读对象字节+meta → 写变换后字节/头 |

**行为**：GET 经 `/ap/<name>/<key>` → 取对象 → 变换 → 返回。`Content-Length` 与 `ETag` 语义要正确（变换后长度可能变化 → 重新算或改用 chunked）。

**落地形态（S2）**：HTTP 变换器 `POST <endpoint>`（body = 原始字节；头 `x-oss-bucket/
version-id/etag/size/key-b64`），响应体为变换后字节、`Content-Type` 可覆盖；`Content-Length`/
`ETag` 按变换结果重算。条件头 304/412 透传，Range → 400，HEAD 直通，写操作 405。限额：输入
`OSS_OLAP_MAX_INPUT_BYTES`（64 MiB）、输出 `max_response_bytes`（默认 64 MiB，上限 1 GiB）、
`timeout_ms` 1–30s、每进程 16 并发；HTTP 仅 http/https，拒内网/保留地址与重定向（每次解析钉扎）。
WASM 模块 ≤16 MiB、保存即编译、燃料限额；未以 `olap-wasm` 构建时保存 400。

**边界**：**只做读变换** —— AWS 标准 Object Lambda 也没有写路径，自造协议任何客户端都不认。变换器超时 → `InternalError`；配置无试跑；变换失败返回 500，不静默直通原文。

---

## 14. 存储分层与 Restore —— `M8`

> **落地状态（2026-10-20）**：S4a 元数据（`storage_class`/`TierState`/phase gate/MPU class/
> `TierGcSink`）与 S4b（远端 S3 tier 后端、生命周期 Transition、`RestoreObject`、到期 reaper、
> 启动对账）均已落地；仅远端 tier（http），无本地冷盘池。

### 14.1 存储类

`STANDARD` · `STANDARD_IA` · `ONEZONE_IA` · `INTELLIGENT_TIERING` · `GLACIER` · `GLACIER_IR` · `DEEP_ARCHIVE` · `REDUCED_REDUNDANCY`

存储类记录在 `xl.meta` 的 `Version.storage_class`。**协议语义完整实现，物理实现是可插拔 tier 后端**：

| tier 后端 | 说明 | 状态 |
|---|---|---|
| 远端 S3 兼容端点 | `[[tiers]]` 静态配置（`id/endpoint/bucket/access_key/secret_key/region`），仅 http；全内存搬运，单对象 `OSS_TIER_MAX_BYTES`（默认 1 GiB） | 已落地 |
| 本地冷盘池 | 不同 EC 参数（如热池 8+4、冷池 12+4），可能更慢的介质 | 未实施（设计保留） |

**不模拟磁带** —— 那不在对象存储范围内，MinIO 的 ILM tiering 也是同一模型。

### 14.2 Restore

| 操作 | 说明 |
|---|---|
| `RestoreObject` | `x-amz-restore-request: days=<n>,tier=Standard\|Bulk\|Expedited`；创建异步作业 |
| `HEAD`/`GET` 状态 | `x-amz-restore: ongoing-request="true"` → 进行中；完成后 `ongoing-request="false", expiry-date="..."` |
| 归档中直接 GET | `InvalidObjectState`（403） |
| 恢复副本过期 | 到期后自动回收，对象回到 `Archived` 状态 |

### 14.3 生命周期驱动

`Transition` 规则 → 作业引擎生成 tier 转换任务（复制到 tier 后端 + 更新 meta + 回收本地分片）。
落地：S3 `Put/Get/DeleteBucketLifecycleConfiguration` 与控制台 lifecycle 共用校验，tier 在写入
时解析并持久化；sweep 由 `OSS_TIER_SWEEP_INTERVAL_SECS`（默认 3600）周期执行 +
`POST /api/v1/tiers/sweep` 强制；按对象去重、仅当前非加密非分层版本、单次 ≤10000 对象。
到期回收 reaper 间隔 `OSS_TIER_REAPER_INTERVAL_SECS`（默认 300，精度约 300s）。

---

## 15. Inventory —— `M8`

> **落地状态（2026-10-20）**：S5 已落地——S3 `Put/Get/List/DeleteBucketInventoryConfiguration`
> 与控制台薄封装、Daily/Weekly 调度、force-run、manifest + CSV.gz（MD5/大小为 gzip 后字节）。

**配置**：`PutBucketInventoryConfiguration` —— 源桶、目标桶+前缀、频率（`Daily`/`Weekly`）、输出格式（`CSV`；`Parquet` 后置）、是否含版本、可选字段列表、可选 Filter（前缀）。

**输出布局**（对齐 AWS v1，让既有工具能直接读；偏离见下）：
```
<dest-prefix>/<source-bucket>/<config-id>/<run_key>/manifest.json
<dest-prefix>/<source-bucket>/<config-id>/<run_key>/data/<uuid>.csv.gz
```
`run_key`：Daily = `YYYY-MM-DD`（UTC），Weekly = `YYYY-Www`，force-run = `manual-<epoch>`。
`manifest.json` 含 `sourceBucket`、`destinationBucket`、`version`、`creationTimestamp`、`fileFormat`、`fileSchema`、`files[{key,size,md5Checksum}]`；`size`/`md5Checksum` 对应 gzip 后字节数与 MD5（base64）。

**列**（`fileSchema`）：必含 9 列 `Bucket`、`Key`、`VersionId`、`IsLatest`、`DeleteMarker`、
`Size`、`LastModifiedDate`、`ETag`、`StorageClass`；可选 6 列按固定顺序追加
`ReplicationStatus`、`EncryptionStatus`、`ObjectLockRetainUntilDate`、`ObjectLockMode`、
`ObjectLockLegalHoldStatus`、`ChecksumAlgorithm`。

**控制台**：`GET/PUT /api/v1/buckets/{b}/inventory`、`DELETE .../{id}`、
`POST .../{id}/run`（force-run → 202 `{"run_key","job_id"}`）。调度 worker
`OSS_INVENTORY_INTERVAL_SECS`（默认 60；0 关闭），同槽唯一去重，停机只补最近一槽。

**边界**：不做 `hive/` 分区布局（后置）；不做 S3 Inventory 的跨账号投递；全量扫描、单数据
文件（gzip 后 ≤5 GiB）、不做增量；`IsMultipartUploaded`/`IntelligentTieringAccessTier`
不支持。

---

## 16. Batch Operations 与作业引擎 —— `M8`

> **落地状态（2026-10-20）**：S0 作业引擎与 S3 Batch 均落地（`jobs/job_tasks`、领取/重试/
> 超时/报告、控制台详情与 pause/resume/cancel/retry-failed、1000+1 ReplaceTags 与 retry-failed
> 子作业）；Transition/Restore/Inventory 已作为任务动作接入。M4 生命周期过期删除与 M6 复制
> 重试不迁入本引擎（M8 钉，各自 worker/outbox 保留）。

### 16.1 作业引擎（一个引擎，五个消费者）

**消费者**：Batch Operations · 生命周期转换 · Glacier Restore · Inventory 生成
（复制积压重试**不迁入**：仍由 M6 outbox worker 负责）

**模型**
```
Job     { id, kind, state, spec, progress, report_bucket, report_key, priority, created_at, finished_at }
JobTask { job_id, task_id, target(bucket,key[,version]), state, result_code, result_string, attempts }
```

**行为**：worker 池从 `job_tasks` 拉任务（`SELECT ... FOR UPDATE SKIP LOCKED`，SQLite 下单写者模式）；失败重试带指数退避与最大次数；完成后写报告对象。

**状态机**：`Queued` → `Running` → `Completed` | `Failed` | `Cancelled`。支持暂停/恢复/取消。

### 16.2 Batch Operations

**CreateJob**：清单（桶内 CSV，列为 `bucket,key[,versionId]`）+ 操作 + 报告配置 + 优先级。

| 操作 | 说明 |
|---|---|
| `CopyObject` | 批量复制到目标桶+前缀 |
| `DeleteObject` | 批量删除 |
| `ReplaceTags` / `PutObjectTagging` | 批量改标签 |
| `RestoreObject` | 批量取回 |
| `InvokeLambda` | 批量走 Object Lambda 变换器 |

**报告**：`results.csv` 写回指定桶，列 `taskId, bucket, key, versionId, resultCode, resultString, startTime, endTime`（`resultCode` 沿用 AWS 的 `Succeeded`/`PermanentFailure` 语义）；仅已启动任务入报告，时间 `%Y-%m-%dT%H:%M:%SZ`。`retry-failed` 建子作业（`parent_job_id`，仅失败任务），父报告不改写。

**落地限额**：`OSS_JOB_MAX_INFLIGHT_PER_USER`（100）/`OSS_JOB_MAX_INFLIGHT_GLOBAL`（1000）；
创建期逐行授权并去重；作业终态 `done`/`error`/`cancelled`（有失败即 `error`）。

**边界**：不做作业优先级抢占（队列按优先级排序即可）；不做跨账号清单；InvokeLambda 仅 HTTP 变换器。

---

## 17. 控制台 API 与 Web 界面

### 17.1 API 总表

| 域 | 端点 | 阶段 |
|---|---|---|
| 认证 | `POST /api/v1/login` | M1 |
| 密钥 | `GET/POST /api/v1/access-keys`、`DELETE/PATCH /api/v1/access-keys/{ak}` | M1 |
| 桶 | `GET/POST /api/v1/buckets`、`DELETE /api/v1/buckets/{bucket}` | M1 |
| 对象 | `GET/POST /api/v1/buckets/{b}/objects`、`GET/DELETE /api/v1/buckets/{b}/objects/{*key}` | M1 |
| 上传记录 | `GET /api/v1/uploads`、`POST /api/v1/uploads/register` | M1 |
| 用户策略 | `GET/POST /api/v1/users`、`/api/v1/policies` | M4 |
| 生命周期 | `GET/PUT /api/v1/buckets/{b}/lifecycle` | M4 |
| 集群 | `GET /api/v1/cluster/status`、`/disks`、`/erasure-sets`、`/scrub` | M3/M4 |
| 审计 | `GET /api/v1/audit`（`from/to/uid/op/bucket/key/decision` 过滤） | M4/M8 |
| 复制 | `GET/PUT /api/v1/buckets/{b}/replication`、`GET /api/v1/remotes` | M6 |
| 站点 | `GET/POST /api/v1/sites`、`/api/v1/sites/{id}/join` | M7 |
| 通知 | `GET/PUT/DELETE /api/v1/buckets/{b}/notifications`、`GET /api/v1/events/dead`、`POST /api/v1/events/{id}/redeliver` | M6/M8 |
| Select | `POST /api/v1/buckets/{b}/select`（工作台，代理 `SelectObjectContent`） | M6/M8 |
| 加密状态 | `GET /api/v1/encryption`（管理员，只读 SSK/S3-KMS 状态） | M5/M8 |
| 策略模板 | `GET /api/v1/policy-templates`（静态 IAM 预设） | M4/M8 |
| 访问点 | `GET/POST /api/v1/access-points`、`GET/DELETE .../{name}`、`PUT .../{name}/policy` | M8（S1 已落地） |
| OLAP | `GET/PUT /api/v1/access-points/{name}/transform` | M8（S2 已落地） |
| 作业 | `GET/POST /api/v1/jobs`、`GET /api/v1/jobs/{id}`、`POST .../{id}/{pause,resume,cancel,retry-failed}` | M8（S0/S3 已落地） |
| 清单 | `GET/PUT /api/v1/buckets/{b}/inventory`、`DELETE .../{id}`、`POST .../{id}/run` | M8（S5 已落地） |
| 分层 | `GET /api/v1/tiers`、`POST /api/v1/tiers/sweep`（admin） | M8（S4b 已落地） |
| 对象标签 | `GET/PUT /api/v1/buckets/{b}/tags/{*key}`（控制台） | M8（S6 缺口页） |
| 桶加密 | `GET/PUT/DELETE /api/v1/buckets/{b}/encryption` | M8（默认加密） |
| 健康 | `/health/live`、`/health/ready`、`/health/cluster` | M1/M3 |

### 17.2 Web 页面

| 页面 | 功能 | 阶段 |
|---|---|---|
| 登录 | 用户名口令 → JWT | M1 |
| 概览 | 容量/对象数/请求速率/集群健康摘要 | M4（依赖指标端点与集群面板，M1 无数据支撑） |
| 桶列表 | 建/删桶、配额、用量条 | M1 |
| 对象浏览器 | 面包屑前缀导航、虚拟滚动列表、上传（拖拽）、下载、删除、复制/重命名、标签、元数据面板、分享链接（预设 URL）、版本列表、对象锁状态、加密状态、存储类、恢复按钮 | M1（基础）/M4-M8（逐项） |
| 上传面板 | 分组选择、进度条、断线续传提示、秒传命中提示、上传历史 | M1 |
| 访问密钥 | AK/SK 列表、创建、禁用、一次性展示 SK、STS 会话列表 | M1/M5 |
| 用户与策略 | 用户、策略文档编辑、绑定 | M4 |
| 生命周期 | 规则可视化编辑器 | M4 |
| 集群状态 | 盘用量与健康、纠删组、scrub/heal 进度、rebalance | M3/M4 |
| 审计日志 | 按时间/身份/资源/动作过滤 | M4/M8 |
| 复制与站点 | 桶复制规则、站点拓扑图、积压深度、站点加入向导 | M6/M7 |
| 事件通知 | 桶通知配置、死信查看与重投 | M6/M8 |
| Select 工作台 | 选对象、写 SQL、看结果流与字节统计 | M6/M8 |
| 访问点与 OLAP | 访问点 CRUD、策略、变换器配置与回读 | M8（S1/S2 已落地） |
| 作业中心 | Batch 创建、列表/进度、暂停/恢复/取消、重跑失败项、报告路径 | M8（S0/S3 已落地） |
| Inventory | 桶页配置弹窗、force-run | M8（S5 已落地） |
| 分层 | tier 只读状态（健康/哨兵键）、强制 sweep | M8（S4b 已落地） |
| 对象页 | 标签编辑、存储类、恢复状态与 Restore、默认加密入口 | M8 |
| 设置 | 上传分组、限制、SSE/KMS 只读状态（材料只在 env） | M2/M5/M8 |

**前端技术**：React 19 + Vite + TypeScript + antd v5 + `@tanstack/react-query`；上传复用 aetherupload 自带的 `aetherupload-all.js`（含 zepto + spark-md5），React 侧包一个 `useUpload` hook 驱动它 —— **不重写分片逻辑**。

---

## 18. 运维与可观测

### 18.1 运维命令

| 命令 | 作用 | 阶段 | 实现状态（M1 收尾） |
|---|---|---|---|
| `groups` | 建分组目录（**不建就一定上传失败**） | M1 | 已实现：启动自举（`aether::build_runtime`）+ `oss-server groups` 双入口 |
| `build` | 按磁盘现状重建秒传索引 | M1 | 已实现：`oss-server build` |
| `clean <days>` | 按 mtime 回收临时文件（`_header` 与 `*.part`） | M1 | 已实现：`oss-server clean <days>`；另回收内核 `.oss.sys/tmp/` 的孤儿上传目录 |
| `fsck` | 启动自检：孤儿版本目录、缺失分片、meta 不一致 | M2 | **M2b 已落地**：`oss-server fsck [--repair]`——孤儿版本目录 / EC 分片 CRC32C 缺坏 / tmp-trash 计数；`--repair` 删孤儿 + 读时修分片；报告落 `.oss.sys/fsck/<时间戳>.json`。独立深度 `scrub` 仍归 M4 |
| `scrub [--repair]` | 逐分片校验和比对，可选修复 | M4 | 未实现 |
| `rebalance` | 按新拓扑重新分布 | M4 | 未实现 |
| `heal` | 补齐缺失分片（读到即修 + 主动扫描） | M4 | 未实现 |

建议 cron：`clean 1` 与 `build` 每日各一次；`scrub --repair` 每周一次。

### 18.2 指标（Prometheus）

每盘用量/健康 · 纠删组状态 · **quorum 达成率**（最重要的一个：这是可用性的先行指标）· 读写延迟直方图（按操作）· 复制积压深度 · 秒传命中率 · 作业进度 · S3 每操作计数与错误计数（按错误码）· 事件投递成功率

### 18.3 审计

`audit_log` 表记录：时间、身份（AK 或用户 id）、动作、目标资源、来源 IP、结果、详情。控制台可查询导出。**记录所有写操作与所有被拒的请求**（被拒的请求是安全信号）。

### 18.4 健康端点

`/health/live`（进程活着）· `/health/ready`（能服务：拓扑加载完成、至少一个 EC 集合 quorum 可用）· `/health/cluster`（节点列表、每个节点的盘状态、复制积压）

---

## 19. 功能 ↔ 阶段 总对照

| 阶段 | 功能 |
|---|---|
| **M1** | SigV4 认证 · 控制台登录 · 桶 CRUD · 对象 PUT/GET/HEAD/DELETE/批量删 · CopyObject · ListObjectsV2/V1 · aetherupload 预处理/分块/秒传/原生 ingest/分组 · 控制台 API（认证/桶/对象/上传/密钥/分组）· 登录/桶/对象/访问密钥四页（上传面板并入对象页） |
| **M2** | 多段上传全套 · Range 与条件请求 · 用户元数据 · 流式大对象 · 纠删码 · 小对象内联 · 多盘 · 断电一致性 · fsck（**协议面与孤儿回收部分 → M2a 已落地；存储层（EC 8+4、内联、多盘 quorum、拔盘容忍、读时修、fsck、kill -9 用例）→ M2b 已落地**） |
| **M3** | 多节点 mesh · 静态拓扑 · 对象→纠删组哈希 · quorum 读写 · 节点故障容忍 · 读修复 · 集群健康端点 · 节点间 quorum 租约锁（**已落地 2026-10-14**；控制面强一致归 M9） |
| **M4** | 版本控制 · 生命周期 · 策略求值与 IAM · 用户与策略页 · 审计 · scrub/heal/rebalance · 集群状态页 · 对象标签（2026-10-19 M8 P0 补齐：含 Copy 两轴指令） |
| **M5** | SSE-S3/KMS/C · 对象锁 · STS · 加密主密钥与 KMS 设置页 |
| **M6** | 事件总线 + Kafka 通知（含 `ecat-mq-kafka`）· S3 Select · 桶复制（含 `ecat-data-s3` 扩展）· 事件页 · Select 工作台 |
| **M7** | 跨站点异步双向复制 · 站点身份与管理 API · 配置/IAM 日志复制 · 站点页 |
| **M8** | Access Points · Object Lambda · 作业引擎 · Batch Operations · Inventory · 存储分层与 Restore · 作业中心页 · 分层页（**已落地 2026-10-20（S0–S6）**；非目标：`AssumeRoleWithWebIdentity`、SSE-KMS 显式 CMK/多钥轮换、AP 主机别名/匿名/跨账号；M9 另列） |
| **M9** | 控制面 `openraft` 跨站点强一致 · sync 桶 · stretch EC · 站点故障语义 |

**与总体设计的分工**：总体设计回答「系统长什么样」（架构、盘布局、数据结构、阶段划分）；本文件回答「每个功能做什么」（行为、接口、边界、错误）；`2026-10-10-m1a-backend-slice.md` 与 `2026-10-10-m1b-frontend-console.md` 回答「M1 前后端怎么写」（逐任务代码与测试）。
