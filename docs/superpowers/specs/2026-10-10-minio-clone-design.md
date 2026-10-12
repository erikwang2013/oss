# 自研分布式对象存储系统（MinIO 替代）— 总体设计

- 日期：2026-10-10
- 状态：已确认范围，待 M1 实现计划
- 框架：e-cat 7 · 上传：aetherupload-rust 1.0.3 · 协议：s3s 0.17 · 前端：React 19 + antd v5

---

## 1. 目标与非目标

### 1.1 目标

一个 S3 协议兼容、分布式、支持纠删码的自研对象存储系统（MinIO 的等价实现），具备：

- **S3 协议全量数据面**：桶/对象 CRUD、Multipart、Range、Copy、批量删除、版本控制、生命周期、对象锁、Storage Class 与 Restore、Access Points、Object Lambda、Batch Operations、Inventory、S3 Select、桶复制、事件通知
- **纠删码存储引擎**：8+4 默认，元数据随对象落盘，无中心元数据库，无共识依赖（除 M9 控制面）
- **浏览器大文件上传**：切片、断线续传、秒传、去重、完整性校验，由 aetherupload-rust 提供，复用其自带前端脚本
- **分布式**：多节点、静态拓扑、对象按哈希分派到纠删组、quorum 读写、节点故障容忍、自愈
- **多站点**：异步双向复制（对齐 MinIO 语义）+ 控制面同步强一致（自研选项）
- **Web 管理台**：React + antd，覆盖桶/对象/上传/权限/密钥/集群状态/各阶段功能界面

### 1.2 非目标

| 不做 | 原因 |
|---|---|
| 磁带/离线介质归档 | Storage Class 的 tier 后端是「远端 S3 端点」或「本地冷盘池」，不模拟磁带 |
| 多站点同步写默认开启 | sync 是 per-bucket 显式 opt-in；默认 async，与 MinIO 语义一致 |
| S3 Object Lambda 的写路径 | AWS 标准 Object Lambda 只做读变换；写路径需要自造协议，不兼容任何客户端 |
| FIPS/国密合规认证 | 加密算法用标准 AES-256-GCM / ChaCha20-Poly1305；国密 SM 系列不做 |
| 跨站点自动降级 | sync 桶在远端不可达时拒绝写（503），不自动转 async —— 见 §7.3 |

---

## 2. 技术选型（版本均已核实）

| 用途 | 选型 | 版本 | 说明 |
|---|---|---|---|
| 应用框架 | `ecat` | 7.0.0 | `App::builder()` + `HttpServer::new(addr).router(r)` + `LifecycleHook`（`HttpServer` 属 `ecat-transport-http`，经 `ecat::transport_http` 再导出） |
| HTTP 传输 | `ecat-transport-http` | 7.0.0 | 底层即 **axum 0.8** —— 与 aetherupload 的 `ecat` feature 完全对齐 |
| S3 协议 | `s3s` | 0.17.0 | `S3Service` 是 `tower::Service`，**114 个 S3 操作**，SigV4/XML/流式全包 |
| 上传 | `aetherupload-rust` | 1.0.3 | feature `ecat` → 返回 `axum::Router<()>`，直接挂载 |
| 纠删码 | `reed-solomon-erasure` | 6.0.0 | GF(2^8)，已验证 620 万下载 |
| 控制面 DB | `ecat-data-sqlx` | 7.0.0 | 开发 SQLite / 生产 PostgreSQL，同一 trait |
| 缓存（可选） | `ecat-data-redis` | 7.0.0 | 仅当外部化秒传索引时才需要；默认不用 |
| S3 客户端（复制/分层） | `ecat-data-s3` | 7.0.0（**需扩展**） | 现仅 put/get/delete/list，需补流式 multipart / copy / 版本头 |
| 认证 | `jwt-rust` | 1.0.1 | 控制面 `/api/v1` 与上传口 `/aetherupload` 的 JWT 校验/签发（axum `JwtLayer` + `encode`，12h）；S3 面走 SigV4。`ecat-auth` 的定位不变，留 M4 策略层（桶策略/IAM） |
| 分布式锁（M3+） | `ecat-lock` | 7.0.0 | 节点间命名空间锁；后端选型在 M3 定 |
| 可观测 | `ecat-metrics` · `ecat-tracing` · `ecat-logging` | 7.0.0 | Prometheus + OTLP |
| 中间件 | `ecat-middleware` · `ecat-security` | 7.0.0 | 超时/限流/攻击检测 |
| 加密（M5） | `aes-gcm` · `chacha20poly1305` | 最新 | RustCrypto |
| 共识（M9） | `openraft` | 0.9.25 | 仅控制面跨站点强一致 |
| Kafka（M6） | `rskafka` | 最新 | **纯 Rust，免 librdkafka C 工具链**；抽成 `ecat-mq-kafka` contrib |
| S3 Select 解析（M6） | `sqlparser` | 最新 | 只取 AST，求值器自写（AWS 方言不是真 SQL） |
| WASM 变换器（M8） | `wasmtime` | 最新 | **cargo feature 门控**，默认走 HTTP 变换器 |
| 前端 | React 19 · Vite · TypeScript · antd v5 · `@tanstack/react-query` | — | 复用 `aetherupload-all.js`（含 zepto + spark-md5） |

**关于 `s3s` 的边界**：114 个操作覆盖全部数据面与桶配置面，但 **Access Points 的 CRUD 不在其中** —— `CreateAccessPoint` 属于 S3 Control API（独立服务、独立端点、独立签名）。这部分由本项目的控制台 API 自建，Access Point 的**使用**（寻址 + 策略解析）走 s3s 的 `S3Route` 前置层。

---

## 3. 系统架构

### 3.1 三入口单 Router

```
  浏览器                          aws-cli / rclone / boto3 / s3fs / minio-go
    │  aetherupload-all.js                   │  SigV4
    ▼                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  e-cat App (ecat 7)                                                 │
│  HttpServer::new(":9000").router(router)          ← axum 0.8 Router │
│                                                                     │
│   /aetherupload/*   aetherupload-rust (feature=ecat)                │
│   /api/v1/*         控制台 API        ← jwt-rust JwtLayer           │
│   /ap/<name>/*      Access Point 寻址 ← S3Route 前置改写层 (M8)      │
│   /metrics          ecat-metrics (Prometheus)                       │
│   fallback          s3s::S3Service    ← 114 个 S3 操作               │
│                                                                     │
│  中间件链：ecat-middleware(超时/限流) + ecat-tracing + ecat-security │
│  LifecycleHook：拓扑加载 · 启动自检(fsck) · 后台作业引擎 · 后台 heal │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
        ┌──────────────────────┴───────────────────────┐
        │                                              │
   s3s 的 S3 trait 实现                        aetherupload EventSink
   （协议 → 内核的翻译层）                      （上传完成后原生 ingest）
        └──────────────────────┬───────────────────────┘
                               ▼
        ┌──────────────────────────────────────────────────────┐
        │  xl-store 存储内核                                    │
        │   命名空间锁 · 纠删码 · xl.meta 提交点 · 拓扑哈希     │
        │   事件总线 · 作业引擎 · scrub/heal · 加密 · 分层      │
        │  【不依赖 axum / s3s / e-cat —— 可独立测试】          │
        └──────────────────────┬───────────────────────────────┘
                               ▼
            盘0      盘1      盘2 …          (M3: 远端节点)
```

### 3.2 crate 拓扑（依赖方向单向，反向不成立）

```
oss/
├── Cargo.toml              workspace
├── crates/
│   ├── xl-store/           存储内核：盘布局 · EC · xl.meta · 拓扑 · 锁 · 事件 · 作业 · 自愈
│   ├── xl-s3/              s3s 的 S3 trait 实现 · S3Route 前置层 · SigV4 凭据 · 策略求值
│   ├── oss-server/         e-cat 应用：router 组装 · 控制台 API · aetherupload 接线 · 钩子
│   ├── xl-proto/           (M3) 节点间 gRPC 协议 (prost + tonic-build)
│   └── xl-client/          (M3) 节点间客户端封装（用于测试与工具）
├── web/                    React 控制台
├── deploy/                 docker-compose(dev) · systemd(prod) · nginx(TLS + X-Accel-Redirect)
└── docs/
```

依赖方向：`oss-server` → `xl-s3` → `xl-store`；`xl-store` 不依赖任何框架。M1 只建三个 crate（`xl-proto` / `xl-client` 到 M3 才出现）。

---

## 4. 盘上布局与提交协议

### 4.1 布局

```
<盘根>/
├── .oss.sys/
│   ├── tmp/            未提交数据（同盘 rename 的源）
│   ├── trash/          待异步清理的版本目录
│   ├── locks/          <sha256(key)[..16]>.lock —— 命名空间锁（M2b 已实现）
│   ├── drive-id        盘部署标识（M2b：防插错盘 / 掉线盘识别）
│   └── fsck/           fsck 巡检报告（M2b 已启用）
└── <bucket>/
    └── a__XLDIR__b__XLDIR__c.txt/     ← 对象名展平（MinIO 同款）
        ├── xl.meta                    元数据（唯一提交点）
        └── <uuid>/part.1 part.2 …     每次写入一个版本目录
            └── xl.meta（候选）        提交时写入的元数据副本（rename 覆盖上层后仍保留在版本目录里）
```

### 4.2 五条铁律

1. **元数据跟着对象走盘。** 没有中心元数据库，没有 Raft（控制面除外）。桶与对象的存在性由盘上的目录与 `xl.meta` 决定。这是「无中心、静态拓扑」成立的原因，也是与 Garage（LMDB 索引）的根本分野。

2. **`xl.meta` 是唯一提交点，且必须写 quorum 块盘。** 写入顺序：
   - 分片落 `.oss.sys/tmp/<uuid>/part.N.<shard>` → fsync
   - 候选 `xl.meta` 落 tmp → fsync
   - 同盘 rename：`tmp/<uuid>` → `<bucket>/<obj>/<uuid>`
   - `xl.meta` 以 rename 方式覆盖（先写 `<obj>/xl.meta.new` 再 rename）
   - 旧版本目录 → `trash/`，异步删

   读路径**只信 `xl.meta`**，未被引用的版本目录都是可回收垃圾。断电最多留下 tmp 孤儿，启动自检清掉。

   **`xl.meta` 写 quorum 而非单盘**：小对象的字节内联在 meta 里，meta 的复制就是它的持久化机制；同时保证不同盘对「最新版本是谁」的认知一致。读 quorum 且发现 meta 不一致时，**以 mod_time 最新者胜出**并触发 heal。

3. **对象名展平 + 整对象一个目录。** 于是「转正」永远是同盘 rename，原子性不依赖任何锁，也不需要跨目录事务。

4. **小对象内联进 `xl.meta`（< 128 KiB）。** 头像/文档/缩略图是绝大多数请求，避免「一个 2 KB 对象产生 13 个小文件」。内联对象不经纠删码——持久性由铁律 2 的 meta quorum 提供。

5. **分片位置只存拓扑下标，不存绝对路径。** `(drive_idx, part, shard_idx)` 三元组，磁盘换挂载点、目录搬家、节点重排都不影响寻址。这是 rebalance 与 drive 替换能成立的前提。

### 4.3 纠删码

- 默认 **8+4**（1.5x 开销，可丢 4 盘），按可用盘数推导且可配（4+2 / 6+3 / 12+4）
- 每分片 **CRC32C**（硬件加速），整对象 **SHA256** 存 meta，用于 scrub 与读校验
- 读路径：读 quorum 内的分片 → 缺失分片用 EC 重建 → 整对象 SHA256 校验
- 写路径：计算 EC 分片 → 并发写各盘 → **写 quorum 达成即返回成功**，不足 quorum 则回滚

### 4.4 命名空间锁

| 阶段 | 机制 |
|---|---|
| M1–M2（单机） | `.oss.sys/locks/<sha256(key)[..16]>.lock` 上的 `flock`（跨进程，**M2b 已实现**，阻塞等待入 spawn_blocking 池）+ 进程内 per-object 互斥（`HashMap<Key, Arc<Mutex>>`，M1 已实现）；M2b 另加**桶级互斥**（写者「桶窗口」与删桶/建桶互斥，防桶复活） |
| M3+（分布式） | 节点间 quorum 锁（类 MinIO dsync）。跨节点 rename 不原子，锁是唯一保证。优先评估 `ecat-lock` 的后端能力 |

---

## 5. 关键数据结构

### 5.1 `xl.meta` v1（首字节即格式版本号，前向兼容解码）

```rust
/// 编码：`[1 字节格式版本][msgpack named 载荷]`。版本字节是前向兼容的锚点：
/// 后续阶段只往 `Version` 里加 `Option` 字段；用 named（字段名→值映射）而非位置数组，
/// 老解码器才能靠 serde「跳过未知字段名」继续可读。
struct XlMeta {
    format_version: u8,              // = 1
    object_key: String,
    versions: Vec<Version>,          // v1 仅一条；M4 版本控制起多条
}

struct Version {
    version_id: Uuid,                // 未启用版本控制时为 Uuid::nil()；S3 协议层映射为字面量 "null"（M4 起为每版本真实 uuid）
    is_delete_marker: bool,          // M4
    is_latest: bool,                 // M4
    mod_time: i64,                   // unix nanos —— meta 冲突时的裁决依据
    size: u64,                       // 逻辑大小（明文）
    etag: String,                    // 单段 = MD5（aetherupload 白拿）；多段 = "<md5-of-md5s>-<n>"
    content_type: String,
    user_metadata: BTreeMap<String, String>,   // x-amz-meta-*
    tags: BTreeMap<String, String>,
    storage_class: String,           // STANDARD | STANDARD_IA | GLACIER …（M8）
    data: DataLayout,
    checksum: Option<Checksum>,      // SHA256，M2 起写入
    encryption: Option<Encryption>,  // M5
    lock: Option<ObjectLock>,        // M5
    replication: Option<ReplStatus>, // M6
    tier: Option<TierState>,         // M8
}

enum DataLayout {
    Inline { bytes: Vec<u8> },                       // < 128 KiB，不经 EC
    Shards { parts: Vec<Part>, ec: EcParams, set: u16 },
}

struct Part {
    number: u16,
    size: u64,
    shards: Vec<Option<ShardLoc>>,   // 长度 = data + parity；None = 该分片缺失，靠 EC 重建
}

struct ShardLoc { drive: u16, size: u64, crc32c: u32 }   // 只有拓扑下标，没有路径

struct Encryption {                  // M5
    kind: SseKind,                   // SseS3 | SseKms | SseC
    algo: String,                    // AES-256-GCM | ChaCha20-Poly1305
    wrapped_dek: Vec<u8>,            // 主密钥包装后的 DEK
    key_id: Option<String>,          // SSE-KMS
    nonce_seed: [u8; 8],             // 每 part 派生的 nonce 基
    sse_c_hmac: Option<Vec<u8>>,     // SSE-C：只存校验，不存密钥
}

struct ObjectLock { mode: Governance | Compliance, retain_until: Option<i64>, legal_hold: bool }  // M5

struct TierState { state: Hot | Archived | RestorePending | Restored, restore_expires: Option<i64> }  // M8
```

### 5.2 控制面 SQL 模型（19 张表，全部是薄表）

**对象数据永远不进 SQL。** 这里只有控制面状态。

| 表 | 用途 | 阶段 |
|---|---|---|
| `users` | 用户、密码哈希、是否管理员 | M1 |
| `access_keys` | AK/SK（SK 加密存）、状态、过期、父密钥、会话策略 | M1（M5 扩展为 STS 会话载体） |
| `policies` · `user_policies` | 策略文档与绑定 | M4 |
| `buckets` | 桶名、创建时间、版本控制状态、对象锁开关、配额、属主 | M1（字段随阶段扩展） |
| `bucket_policies` | 桶策略文档 | M4 |
| `access_points` · `olap_configs` | 访问点与 Object Lambda 变换配置 | M8 |
| `lifecycle_rules` | 生命周期规则 | M4 |
| `replication_rules` · `remotes` | 桶复制规则、远端端点与凭据、tier 后端 | M6/M8 |
| `jobs` · `job_tasks` | **作业引擎**：Batch Ops / 生命周期转换 / Restore / Inventory / 复制重试共用 | M4（雏形）· M8（完整） |
| `event_configs` · `event_outbox` | 桶通知配置与**持久化事件队列**（Kafka 挂了不丢） | M6 |
| `repl_outbox` | 复制积压队列（含跨站点，含重放） | M6/M7 |
| `instant_index` | aetherupload 秒传索引（替代 Redis，带 TTL） | M1 |
| `uploads` | `savedPath → bucket/key/owner` 映射 | M1 |
| `audit_log` | 审计 | M4 |

### 5.3 对象键 → 纠删组（确定性，无共识）

```
set_index = crc32c(bucket + "/" + key) % sets.len()
```

拓扑来自静态配置（`node/drive` 列表，MinIO 的 `VOLUMES` 同款）。配置变更 = rebalance（M4 起支持）。**确定性哈希 = 不需要共识即可让所有节点对「对象在哪」达成一致**，这是整个分布式设计能站住的地基。

> **拓扑原则（按 MinIO 的设计，不引入「数据中心」概念）**：集群 = 一组扁平节点 × 驱动器，没有
> DC / Region / AZ 层级抽象；不做「同 DC 就近」这类路由。跨地理的复制用 MinIO 原生的**站点
> （site）复制**语义表达（M7 异步 / M9 同步，见 §7.3），站点是复制关系的对端，不是拓扑层级。
> 后续所有阶段（M3+）的设计不得引入 DC/Region 维度。

---

## 6. 请求路径与集成

### 6.1 三种入口的具体接线

```rust
// 1) 存储内核：无框架依赖
let store = Arc::new(xl_store::Store::open(Config::from_file("oss.toml")?).await?);

// 2) S3 协议层：s3s 的 S3Service 挂 fallback
// s3s 0.17 的 builder 是 &mut self 风格，不能链式调用
let mut s3_builder = s3s::service::S3ServiceBuilder::new(S3Impl::new(store.clone()));
s3_builder.set_auth(SigV4Auth::new(store.clone()));         // AK/SK 查库 + STS 会话校验
s3_builder.set_access(PolicyAccess::new(store.clone()));    // 策略求值（M4）
s3_builder.set_route(AccessPointRoute::new(store.clone())); // AP / Object Lambda 改写层（M8）
let s3 = s3_builder.build();

// 3) e-cat 应用：一个 Router 三种入口
let router = axum::Router::new()
    .nest("/api/v1", console_api(store.clone()))              // 控制台 API
    .merge(aetherupload::integrations::ecat::routes(runtime)) // 四条上传路由
    .nest("/ap", access_point_layer(store.clone()))           // M8
    .fallback_service(HandleError::new(s3, handle_s3_error)); // S3 协议全量

App::builder()
    .name("oss")
    .server(HttpServer::new("0.0.0.0:9000").router(router))
    .on_start(Hooks::startup(store.clone()))   // 拓扑加载 · fsck · 作业引擎 · heal
    .on_stop(Hooks::shutdown(store.clone()))
    .build()?
    .run()
    .await?;
```

### 6.2 aetherupload 集成（少一跳，秒传与 ETag 天然对齐）

- 四条路由挂 `/aetherupload/*`；`Config.root_dir` 指向**暂存盘**（`/var/lib/oss/staging`），**不是数据盘**
- 分组 = 上传档位（`image` / `video` / `file`），各自独立的上限与扩展名白名单
- 末块校验通过 → `EventSink::upload_complete` → **原生 ingest 进内核**（Rust API 调用，不绕 HTTP）
  - **ETag 白拿**：aetherupload 已算完整份 MD5，单段 PUT 的 S3 ETag 定义就是 MD5 → 直接写 `xl.meta`，`rclone` / `aws-cli` 看到的 ETag 正确，无需重算
  - **秒传不引 Redis**：`InstantStore` 是 trait，实现到 `instant_index` 表（TTL 语义一致）。要外部 Redis 时换回 `ecat-data-redis` 即可
- `uploads` 表记录 `savedPath → bucket/key/owner`（aetherupload 的无状态寻址只到「在盘上哪」）
- 前端不重写分片逻辑：复用 aetherupload 自带 `aetherupload-all.js`，React 侧包一个 `useUpload` hook 驱动

**S3 多段上传与 aetherupload 是两条独立路径**：S3 客户端的多段由 s3s 直接驱动内核；aetherupload 只服务浏览器。两者最终写同一套 `xl.meta`。

---

## 7. 横切子系统

### 7.1 作业引擎（M8 引入，但 M4 的生命周期转换就有雏形）

一个引擎，五个消费者：**Batch Operations · 生命周期存储类转换 · Glacier Restore · Inventory 生成 · 复制积压重试**。

```
Job { id, kind, state, spec, progress, report_bucket, report_key, priority, created_at, finished_at }
JobTask { job_id, task_id, target(bucket,key[,version]), state, result_code, result_string, attempts }
```

- worker 池从 `job_tasks` 拉任务（`SELECT … FOR UPDATE SKIP LOCKED` 或 SQLite 单写者模式）
- 失败重试带指数退避与最大次数；终态写回任务表
- 完成报告 = 一个 CSV object 写回用户指定桶（**S3 Batch Operations 的 completion report 格式**）
- 作业进度进 Prometheus 指标 + 控制台实时面板

### 7.2 事件总线（M6 引入，M7 复用）

写路径在提交成功后发出事件：`ObjectCreated:*` / `ObjectRemoved:*` / `ObjectAccessed:*` / `Replication:*` / `LifecycleTransition`。

- **三个消费者共用一份事件**：Kafka 通知（`event_outbox`）、桶复制（`repl_outbox`）、跨站点复制（复用 `repl_outbox` 的跨站点目标）
- **持久化队列**：事件先落 outbox 表，worker 异步投递。Kafka 宕机不丢事件，恢复后按 seq 续发（MinIO 的做法是队列满即丢，我们做得更好且成本只是两张表）
- Kafka 后端以 `ecat-mq` 的 `MessageQueue` trait 抽象 → **新建 `ecat-mq-kafka` contrib**（e-cat 生态当前只有 rabbitmq/nats/mqtt）。首选 `rskafka`（纯 Rust），需要 SASL/SSL 高级特性时换 rdkafka，只动一个 crate

### 7.3 多站点一致性（M7 异步 + M9 同步）

| 层 | 机制 | 阶段 |
|---|---|---|
| 对象数据（默认） | 异步双向复制，对齐 MinIO site replication 语义。版本不可变 → 实际是追加式传播；桶删除等命名空间变更按站点优先级裁决 | M7 |
| 对象数据（opt-in） | `replication_mode: sync` 桶：提交 = 本地 EC quorum + 远端站点 meta 确认。支持 **stretch EC**（分片与校验块跨站点分布，单站点不足以独立读出） | M9 |
| 控制面（IAM/桶/策略/AP） | `openraft` 跨站点强一致。状态小、变更低频，Raft over WAN 成立；配置分叉是最难修的数据事故，值得强一致 | M9 |

**CAP 选择必须显式**：sync 桶在远端站点不可达时**写返回 503**。不做「自动降级为 async」——那会让「强一致」这个承诺失去意义。写延迟下限 = 跨站点 RTT，因此 sync 只适合合规桶与小对象。此约束必须出现在桶创建界面的文案里。

### 7.4 存储分层（M8）

- Storage Class 记录在 `xl.meta` 的 `Version.storage_class`
- **tier 后端是可插拔的**：远端 S3 兼容端点（扩展后的 `ecat-data-s3`）或本地冷盘池（不同 EC 参数，如 12+4）
- 协议语义完整实现：`x-amz-storage-class` 请求头、`x-amz-restore: ongoing-request="true"; expiry-date="…"` 响应头、`RestoreObject` 异步作业、恢复副本到期回收
- 生命周期规则驱动转换（`lifecycle_rules` → 作业引擎）

### 7.5 可观测性

- `ecat-metrics`：每盘用量/健康、纠删组状态、quorum 达成率、读写延迟直方图、复制积压深度、作业进度、S3 每操作计数
- `ecat-tracing`：OTLP，跨节点传播 trace（M3 起）
- 审计：`audit_log` 表 + 控制台查询
- 健康端点：`/health/live`、`/health/ready`、`/health/cluster`（M3）

### 7.6 加密与密钥管理（M5）

**顺序是硬约束：先加密，再纠删码。** 写路径 `明文 → 按 part 流式加密（AES-256-GCM）→ EC 编码 → 落盘`；读路径反向。这样单个分片流出（不足 quorum）不含任何明文信息，parity 块同理。反过来做（先 EC 再加密）会让每个分片成为独立的密文单元，parity 参与运算的是密文，虽也可行但密钥管理与重建逻辑更乱，且失去「分片泄露无信息」这一性质。

| 模式 | 密钥来源 | 盘上存什么 |
|---|---|---|
| SSE-S3 | 服务端主密钥（配置文件/KMS） | 包装后的 DEK |
| SSE-KMS | 外部 KMS（先落一个文件型实现，预留 HTTP KMS trait） | 包装后的 DEK + `key_id` |
| SSE-C | 客户端每次请求携带 | **只存 HMAC 用于校验密钥正确性，绝不存密钥** |

- 每对象一个 DEK，主密钥只做包装（envelope encryption）；换主密钥不重写数据，只需重新包装 DEK
- nonce 由 `nonce_seed` + part 号派生，保证同一 DEK 下不重用 nonce
- `xl.meta` 中 `Version.encryption` 记录算法与包装结果；元数据本身不加密（不含秘密）

### 7.7 Access Point 与 Object Lambda 的改写层（M8）

两者是同一个机制：**请求到达 S3 服务之前的改写**。

```
请求 → S3Route::is_match（识别 /ap/<name>/ 路径 或 <name>-s3alias 主机名）
     → 解析出 (目标桶, 访问点策略, 可选变换器)
     → check_access：访问点策略 ∩ 桶策略 ∩ 身份策略，取交集
     → call：命中变换器 → 取对象 → 变换 → 返回；无变换器 → 直通 S3 服务
```

- **Access Point CRUD 自建**（`/api/v1/access-points`），因为 `CreateAccessPoint` 属于 S3 Control API，不在 s3s 的 114 个操作里；而**使用**走 `S3Route`，与标准 S3 请求共用同一条解析路径
- Object Lambda 的变换器两种形态：**HTTP 变换器**（默认，POST 对象字节+元数据到外部端点，收变换结果，无重依赖）与 **WASM 变换器**（`wasmtime`，cargo feature 门控，沙箱内执行，窄 ABI：读对象字节+meta → 写变换后字节/头）
- AWS 的标准 Object Lambda 只做读变换；我们不做写路径变换（那需要自造协议，任何客户端都不认）

---

## 8. 阶段划分与验收标准

每个阶段独立可交付、可停下。验收标准全部是可执行动作，不是「应该能用」。

### M1 垂直切片（2-3 周）

**范围**：workspace 骨架 · e-cat 起服务 · s3s 挂 fallback · 单盘直存（无 EC）· 桶/对象核心 API（含 CopyObject 与批量删除）· SigV4 凭据 · JWT 控制台登录 · aetherupload 四条路由 + 原生 ingest · React 控制台（登录/桶列表/对象浏览/上传/下载/访问密钥）

**验收**
1. `aws-cli s3 mb/ls/cp/rm` 与 `rclone` 增删查列全部通过
2. 浏览器上传 1 GB 文件：有进度条、拔网线后自动续传成功
3. 同一文件二次上传秒传命中（0 分块传输），`uploads` 表与对象均正确
4. 控制台里能看到、下载、删除该文件
5. `curl` 直接打 `/aetherupload/preprocess` 返回协议一致的 JSON

### M2 真·单机对象存储（3-4 周）——**已落地（M2a 协议/回收面 + M2b 存储面，2026-10-13）**

**范围**：正式 `xl.meta` v1（含格式版本）· EC 8+4 · 小对象内联 · 单机多盘 · Multipart Upload · Range · 断电一致性 · tmp/trash 孤儿回收

**验收**（M2b 收尾时的自动化映射见实现计划 `plans/2026-10-12-m2b-ec-multidisk.md` 定稿记录）
1. 拔掉 2 块盘（8+4 的 parity 预算内）读写照常，日志显示分片重建 —— ✅ 单测（12 盘 tempdir 移盘）+ 真服务 e2e 加验；拔 4 盘读边界、拔 5 盘 fail-closed、拔盘写入 quorum = data+1
2. 在任意提交点 `kill -9` 后重启：自检无脏数据、无孤儿外泄 —— ✅ `tests/kill9.rs` 四注入点（分片半写 / meta 准备后 / 翻转中途，SIGABRT）+ 启动自检；`rclone check` 于 e2e 覆盖
3. `rclone` 并发上传 100 个 100 MB 文件 + 中断重试，全部校验和一致 —— ✅ `tests/e2e_s3.sh` 的 `E2E_LONG=1` 门控段（微缩版实跑通过；默认参数人工触发）
4. 2 KB 对象不产生 13 个小文件（内联生效，`find` 计数验证）—— ✅ 内联阈值用例（find 计数 0）
5. 分片 CRC32C / 整对象 SHA256 校验：手工翻转一个分片字节，读返回错误而非静默脏数据 —— ✅ CRC 层坏片视同缺失进重建（自愈）；能过 CRC 的错配由 SHA256 终验 fail-closed（专测）

**实现边界（M2b 定稿记录）**：EC 读写整份进内存（ShardByShard 流式化顺延）；meta「最新副本胜」依赖墙钟单调（M3 epoch/heal 收口）；读时修为最小版、无后台 heal；rclone 长跑默认参数人工触发。

**授权模型提前落地（2026-10-13，原计划 M4 的「角色/策略」最小版）**：子账户 × 桶 三档
（读/写/删）+ 桶主全权 + 管理员全量；S3 面（s3s `S3Access::check`）、控制台、aetherupload
上传面执行同一份判定（`xl-s3/src/access.rs::Acl`）。细粒度策略（桶策略 JSON、预设模板、
STS/AssumeRole）与对象锁仍随 M4/M5；实施记录见
`plans/2026-10-13-subaccounts-permissions.md`。

### M3 分布式（4-6 周）

**范围**：节点间 gRPC mesh（`xl-proto`）· 静态拓扑 · 对象→纠删组哈希 · 跨节点 quorum 读写 · 节点故障容忍 · 读修复 · 节点间命名空间锁

**验收**
1. 4 节点集群，拔掉 1 个节点读写不中断
2. 加回节点后 scrub 补齐，逐分片校验和比对一致
3. 从任意节点读同一对象结果一致（哈希对拍）
4. 并发写同一 key 无交错损坏
5. 脑裂场景（人为网络分区）下，quorum 不足的一侧拒绝写而非产生分叉

### M4 运维与 IAM（3-4 周）

**范围**：heal/scrub · rebalance · **版本控制** · 生命周期 · 桶策略/IAM · 审计 · 集群面板

**验收**
1. 不停机扩容（加盘/加节点）后数据按新拓扑重新分布，读路径全程可用
2. 版本控制：PUT 三次得三个版本，`list_object_versions` 正确，删除产生 delete marker，可回滚
3. 生命周期规则到期自动转换/删除（`jobs` 表可见进度）
4. 策略拒绝：无权限的 AK 访问被拒且审计表有记录

### M5 安全与锁定（2-3 周）

**范围**：SSE-S3 / SSE-KMS（含文件型 KMS 实现）/ SSE-C · 对象锁（Governance + Compliance + Legal Hold）· STS（AssumeRole / GetSessionToken / AssumeRoleWithWebIdentity）

**验收**
1. 三种 SSE 各下一单：`aws-cli` 带对应参数写入后 `head-object` 看到正确的加密头；盘上分片无反推的明文（`grep` 明文特征串）
2. 先加密再 EC：手工取单个分片（不足 quorum）无法还原任何明文
3. 对象锁：Compliance 模式下删除版本被拒（含管理员）；到期后可删
4. STS：AssumeRole 拿到临时凭据，`aws-cli` 用它能访问；过期后拒绝；会话策略与用户策略取交集生效
5. SSE-KMS 换主密钥后旧对象仍可读（DEK 未变）

### M6 数据流（3-5 周）

**范围**：事件总线 + Kafka 通知（含新建 `ecat-mq-kafka`）· S3 Select（CSV + JSON）· 桶复制（含扩展 `ecat-data-s3`）

**验收**
1. Kafka 消费者收到 `ObjectCreated` 事件，字段与 S3 通知格式一致
2. 关掉 Kafka 期间产生的上传，在 Kafka 恢复后全部补投，不丢不重
3. `aws-cli s3api select-object-content` 对 CSV 与 JSON 各返回正确记录流；`--request-progress` 事件帧格式正确
4. 桶复制：源桶写入后远端出现同 ETag 对象，`x-amz-replication-status` 从 PENDING → COMPLETED
5. 远端不可达期间积压进 `repl_outbox`，恢复后自动追平
6. Select 的 SQL 子集边界测试：不支持的语法返回明确的 `InvalidRequest`，而非错误结果

### M7 多站点异步复制（4-6 周）

**范围**：站点身份 · 站点间管理 API · 对象双向异步复制 · IAM/桶/策略变更的日志复制 · 断点续传与重放

**验收**
1. 两站点互建关系后，任一站点写入在秒级出现在对端（含版本）
2. 任一站点的桶/策略变更在对端可见
3. 断开 WAN 一段时间后恢复：积压全部重放，双向数据一致（逐 ETag 对拍）
4. 两端同时写同一 key 不产生损坏（版本不可变，追加式传播）

### M8 S3 控制面服务（4-6 周）

**范围**：Access Points（CRUD 自建 + `S3Route` 寻址）· Object Lambda（读变换：HTTP 变换器默认 + WASM 可选）· Batch Operations（作业引擎）· Inventory（CSV.gz + manifest.json，列对齐 AWS v1 schema）· 存储分层与 Restore

**验收**
1. 建 AP 后经 `/ap/<name>/<key>` 访问，且 AP 策略与桶策略取交集生效
2. Object Lambda：GET 经变换器返回变换后内容，`Content-Length`/`ETag` 语义正确
3. Batch Operations：1000 对象批量打标签，报告 CSV 写回指定桶，字段含 taskId / resultCode / resultString，失败项可重跑
4. Inventory：每日生成 manifest.json + data/*.csv.gz，列覆盖 AWS v1 schema（含加密状态、复制状态、对象锁字段）
5. 生命周期把对象转到 GLACIER 层：`head-object` 显示存储类，GET 返回 `InvalidObjectState`，`RestoreObject` 后返回 `x-amz-restore: ongoing-request` 最后可读，到期自动回收恢复副本

### M9 同步强一致跨站点（6-10 周）

**范围**：控制面 `openraft` 跨站点 · `replication_mode: sync` 桶 · stretch EC · 站点故障语义

**验收**
1. 控制面：两个站点同时改 IAM/策略，强一致收敛，无分叉
2. sync 桶：写入在远端确认后才返回成功；断开远端后写返回 503（**不降级**）
3. stretch EC：单站点全部盘取完仍不足以还原对象，双站点联合可读
4. 站点恢复后自动追平，元数据一致（控制面状态逐字段对拍）

---

## 9. 测试策略

| 层 | 手段 |
|---|---|
| `xl-store` 单元 | EC 编解码、分片损坏重建、**proptest 随机损坏 N 个分片断言重建成功**、键→组哈希的确定性 |
| 崩溃一致性 | 故障注入：在提交协议的每个步骤后 `kill -9`，重启跑 fsck 断言无脏数据、无半提交 |
| **S3 兼容性** | `aws-cli` · `rclone` · `s3fs` · `boto3` · `minio-go` 对拍 —— 这是「兼容 S3」的唯一可信证据 |
| 浏览器上传 | aetherupload 自带 e2e 测试的接线形态 + Playwright 跑真实上传（含断网续传与秒传） |
| 混沌（M3+） | 随机杀节点 / 随机延迟 / 网络分区，断言可用性与数据一致性 |
| 前端 | vitest 单测 + Playwright 关键路径（登录/上传/浏览/分享/权限） |

**每阶段结束前必须真跑一遍验收清单并把输出贴进 PR 描述**，不允许「应该可以」。

---

## 10. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 分布式写冲突 / 脑裂 | quorum 不足时**拒绝写**（M3 验收第 5 条专门测这个）；绝不「尽力而为地写」 |
| 宕机后修复的正确性 | 故障注入测试从 M2 起就在 CI 里；scrub 逐分片校验和比对 |
| EC × 加密 × 版本 × 复制 × 分层的组合爆炸 | `xl.meta` 从 M2 起带格式版本号 + 前向兼容解码；组合场景写矩阵测试 |
| M9 的 Raft over WAN 是最大未知 | 先做控制面（状态小）再考虑对象数据；M9 之前不承诺任何同步语义 |
| 工期 | 单人 8-11 个月。M5/M6/M8 之间基本无耦合，可并行；M1→M2→M3→M4→M7→M9 必须串行。三人成组可压到 5-6 个月 |
| 上游缺口 | `ecat-data-s3` 需扩展（流式 multipart、copy、版本头）；`ecat-mq-kafka` 需新建。两者都是对 e-cat 生态的正向贡献，而非本项目私有的胶水 |

---

## 11. 工期

| 阶段 | 工作量 | 可并行性 |
|---|---|---|
| M1 垂直切片 | 2-3 周 | 串行起点 |
| M2 单机对象存储 | 3-4 周 | 串行 |
| M3 分布式 | 4-6 周 | 串行 |
| M4 运维与 IAM | 3-4 周 | 串行（M5/M6 可与其并行） |
| M5 安全与锁定 | 2-3 周 | **可并行** |
| M6 数据流 | 3-5 周 | **可并行** |
| M7 多站点异步 | 4-6 周 | 依赖 M6 |
| M8 S3 控制面服务 | 4-6 周 | **可并行**（依赖 M4） |
| M9 同步强一致 | 6-10 周 | 收尾 |

单人全职 **31-47 周（约 8-11 个月）**；三人并行 **约 5-6 个月**。

---

## 12. 部署、CI 与许可证（跨阶段）

| 项 | 阶段 | 内容 |
|---|---|---|
| CI | 已就位（M1a Task 1 起生效） | `.github/workflows/ci.yml`：Rust（fmt / clippy / test）与 web（build / vitest）两个 job；对应文件（`Cargo.toml` / `web/package.json`）出现前自动跳过，规划阶段不红 |
| 部署 | **M2** | `deploy/docker-compose.yml`（dev：oss + minio 对照）· `deploy/systemd/oss.service` · `deploy/nginx.conf`（TLS + X-Accel-Redirect 大文件下载；M1b 的 `web/` 构建产物由 nginx 静态服务）· `Dockerfile` |
| 许可证 | 即刻 | Apache-2.0（根 `LICENSE` 已就位；workspace `Cargo.toml` 的 `license` 字段与之一致） |

M1 期间的浏览器验收不依赖部署：前端跑 Vite dev server，经代理直连 `oss-server`（见 `plans/2026-10-10-m1b-frontend-console.md`）。
