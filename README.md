![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

![吞吞的 12 种彩色动态表情：笑、欢乐、哭、沮丧、惊恐、愤怒、撒娇、卖萌、讨好、敲打、捂脸、吐舌头](docs/assets/mascot-expressions.svg)

单张彩色动态 SVG 见 [`docs/pet-expressions/`](docs/pet-expressions/)（12 种形态，均带动画）。

# oss（吞吞）—— 自研分布式对象存储系统

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研对象存储（MinIO 的等价实现）。目标形态是分布式 + 纠删码；
当前完成 **M1（单盘垂直切片）+ M2a（可用性切片：Multipart Upload、Range GET、
`x-amz-meta-*` 元数据、流式 PUT、孤儿回收）+ M2b（存储切片：单机多盘 + 纠删码 8+4、
quorum 写、拔盘容忍与读时修、`fsck` 巡检、kill -9 断电一致性用例）+ M3（分布式切片：
节点间 gRPC mesh、静态槽位拓扑、跨节点 quorum 读写、节点故障容忍与读修复、quorum 租约锁、
混沌 e2e）+ M4（运维与 IAM：版本控制、生命周期、桶策略、审计、heal/scrub、rebalance、
多 set）+ M5（安全与锁定：SSE 三模式、对象锁、STS 临时凭据、SK 加密存）**——`aws-cli`/`rclone`
的大文件默认路径（≥8 MiB 自动 multipart）与断点习惯直接可用，浏览器能分块上传，控制台能看见。

- **五个 crate 单向依赖 `oss-server → xl-s3 / xl-client → xl-store`（`xl-client → xl-proto`）**：
  `xl-store` 是不依赖任何框架的存储内核（提交协议与 `xl.meta` 格式按最终设计落盘）；`xl-s3` 把
  s3s 的 S3 trait 实现到内核上；`xl-proto` 是节点间 gRPC 协议消息与 service（prost + tonic-build
  生成）；`xl-client` 是 mesh 的 tonic 实现（`TonicMesh` 客户端 + 服务端 handler）；`oss-server`
  用 e-cat 组装一个 axum Router，同时承载控制台 API、aetherupload 分块上传与 S3 服务。
- **两条客户端入口**：`aws-cli` / `rclone` 走 S3 面（SigV4 认证）；
  浏览器控制台（React 19 + antd，M1b）走 `/api/v1` 与 aetherupload 自带脚本（分块 / 断线续传 / 秒传）。
- **技术栈**：Rust 2024 · ecat 7.0 · s3s 0.17 · aetherupload-rust 1.0.4 · axum 0.8 · sqlx / SQLite · serde + rmp-serde。

## 当前进度

已实现（M1）：单盘直存内核（无纠删码）· S3 桶/对象 CRUD · ListObjectsV2 · CopyObject ·
批量删除 · SigV4 认证 · aetherupload 分块上传与原生 ingest · 控制台 API。
已实现（M2a）：Multipart Upload（五操作）· Range GET（206/416）· `x-amz-meta-*` 元数据往返 ·
流式 PutObject（不再整份读内存）· 启动自检与 `clean` 按龄续回收（trash/tmp/未引用版本目录）。
已实现（M2b）：多盘存储（`data_dirs`，每盘一份命名空间镜像）· EC 8+4 分片（quorum 写
≥ data+1、拔盘重建、读时修）· 小对象内联（< 128 KiB 进 `xl.meta`）· meta 两阶段多盘提交
（最新副本胜）· `fsck [--repair]` 巡检 · 桶级互斥与 flock 跨进程锁 · 目录 fsync 与
kill -9 断电用例（SIGABRT 四注入点）。
已实现（权限体系）：子账户与桶级**读/写/删**三档授权（管理员在控制台「子账户」页创建、
管理、授权）· 三面统一执行（S3 的 AK/SK、控制台 JWT、aetherupload 上传认证）· 子账户可建
自有桶（桶主全权）· 上传/下载/删除均按权限门。
已实现（M3）：分布式切片——节点间 gRPC mesh（`xl-proto`/`xl-client`）· 静态扁平拓扑（全局
槽位表 + 拓扑指纹）· 跨节点 quorum 读写（提交 quorum + 补偿、读覆盖率快路径）· 节点故障容忍
（kill -9 / SIGSTOP 混沌 e2e 实证）· 读修复（read-repair + fsck 集群化差集补齐）· 节点间
quorum 租约锁（节点域 Q_n）· `/health/cluster` 集群健康端点。
已实现（M4 + 扩展包 C1-C5）：版本控制（版本链 / delete marker / 回滚 / 点名版本读删）·
生命周期到期自动删（jobs 进度可见）· 桶策略（显式 Deny 优先；CopyObject 源/目标双轴判定；
批量删逐键授权）· 审计日志与查询（C5 保留期自动清理）· `heal`/`scrub` 独立命令 · rebalance
追加式扩容（C4 同盘 hardlink 快路径）· 多 set 横向扩容（桶钉 set；C2 控制台 sets 视图；
C3 建桶缺省自动指派负载最小的 set）· 登录限速（C1：账号 10 分钟 5 败锁定 + 全局 60 秒 120 败
暂拒，429 + Retry-After）。
已实现（M5 安全与锁定）：集群控制面行一致性广播（切片 0）· 主密钥密钥库（切片 1，单钥文件 +
多钥 KMS 目录）· 加密数据面（切片 2，先加密后 EC 的 DARE 式块对齐 AES-256-GCM）· S3 面
SSE-S3 / SSE-KMS / SSE-C 全链（切片 3）· 对象锁 GOVERNANCE / COMPLIANCE / Legal Hold +
桶级默认保留规则（切片 4）· STS 临时凭据 AssumeRole/GetSessionToken + 会话策略交集（切片 5）·
`access_keys.secret_key` 信封加密 + 存量透明迁移（切片 6）。**WebIdentity 经成本评估裁剪延后**
（需出站 TLS 客户端新依赖栈；验收 ①–⑤ 不涉；记档于 M5 计划——用户知悉项）。
已实现（M6 数据流）：事件通知（S3 通知配置 + Kafka，至少一次 outbox）· S3 Select（CSV/JSON、
SQL 白名单、ScanRange、Progress/Stats 帧）· 桶复制（规则 API、9 写入口入队、SigV4 出站、
PENDING→COMPLETED/FAILED、停机积压追平）。
已实现（M7 多站点异步复制）：单节点站点对（`[site]` 唯一开关 + 管理 API 配对/ping/status）·
控制面 changelog（SQLite 触发器捕获、LWW `(ts,priority,origin,seq)` + 本地写门控 + apply 抑制）·
配对快照（单事务、入钟、只本地胜者）· 对象复制复用 `repl_outbox`（逐 peer 入队、发送前
`GET ?versioning` 预检、metadata 透传、REPLICA 防环）· 停机积压追平、死信/游标/状态面。
**未实现**（见设计文档的阶段表）：访问点 / Object Lambda（M8）· 跨站点强一致（M9）·
细粒度 IAM 预设模板 · 复制带宽限速。

## 架构

![系统架构](docs/assets/architecture.svg)

M6 数据流（事件通知 / S3 Select / 桶复制）：

![M6 数据流：事件 / Select / 复制](docs/assets/m6-dataflow.svg)

M7 多站点异步复制（控制面 changelog + 对象 outbox）：

![M7 多站点复制](docs/assets/m7-sites.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` 编解码（版本链 / 加密记录 / 锁记录）、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock、密钥库（`kms.rs`）与块对齐信封加密（`crypto.rs`） |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`）、SSE 与对象锁头解析/响应装配 |
| `crates/xl-proto` | 节点间 gRPC 协议：`oss.proto` 消息与 service 定义（prost + tonic-build 全量生成） |
| `crates/xl-client` | mesh 的 tonic 实现：`TonicMesh` 客户端（懒连接 / 超时 / token 拦截器 + traceparent 注入）与服务端 handler（令牌校验，只操作本节点本地槽） |
| `crates/oss-server` | ecat + axum 组装：控制台 API（JWT）、aetherupload 接线、S3 fallback、运维 CLI |
| `web` | 控制台前端（M1b）：React 19 + antd + Vite，Playwright e2e |
| `tests/e2e_s3.sh` | aws-cli / rclone 端到端验收脚本（19 步，含 M6 Select/Kafka/复制；+ `E2E_LONG=1` 门控长跑） |
| `tests/e2e_replication.sh` | 桶复制双实例验收（源/目标真进程：同 ETag、PENDING→COMPLETED、停机积压→追平、marker） |
| `tests/e2e_sites.sh` | 多站点复制双实例验收（双向配对/快照、建桶立刻写、断连积压、marker、并发同 key、重启不回退） |

磁盘布局速记（M2b 多盘）：每个盘各持一份**完整命名空间**——`<盘>/<桶>/<展平键>/` 下是
`xl.meta`（该盘副本）与版本目录 `<uuid>/part.1.<shard>`（落到本盘的那些分片；单盘形态即全部分片）。
**提交永远是「数据先到位 → meta 翻转」**：单盘 = 同盘 rename；多盘 = 各盘 meta 两阶段提交
（全盘 `xl.meta.new` 准备 ≥ quorum 才逐盘转正；读取**最新副本**胜）。小对象（< 128 KiB）内联进
`xl.meta`、不落数据文件。启用加密（M5）后数据文件存的是**密文分片**（先加密后 EC），
包裹的 DEK 与 `key_id` 记在 `xl.meta` 内（元数据本身不含密钥明文）。临时区在每盘
`.oss.sys/{tmp, trash, locks}`，盘身份标识 `drive-id` 防插错盘。
控制面元数据（用户/密钥/桶/上传记录/秒传索引）在 SQLite。

## 请求周期

![请求周期](docs/assets/request-flow.svg)

两条入口（S3 直传、浏览器分块上传）在 xl-store 的**提交协议**处汇合：
暂存 → 同盘 rename 数据就位 → `xl.meta` 覆盖即提交；提交之前的一切对客户端不可见。
EC 写/读（含区间读）已分块流式：内存峰值 ≈ 块缓冲（MiB 级），与对象大小无关
（96 MiB 对象实测 VmHWM 107 MiB）。

## 对象生命周期

![对象生命周期](docs/assets/object-lifecycle.svg)

读路径只信 `xl.meta`：无有效 meta 的目录永不被服务；崩溃只会留下不可见的残骸
（孤儿版本目录 / trash 残留 / 未完成的 multipart 暂存区）。服务启动时自检清 `tmp`/`trash`，
`oss-server clean <days>` 按 mtime 回收其余残骸（M2a 已落地）。

## 跑起来

```bash
# 1. 起服务（首次启动会自动建数据目录、暂存目录、桶目录与管理员）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server

# 2. 登录拿 token（M1 还没有注册界面，账号来自上面的环境变量）
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"changeme"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

# 3. 建一对访问密钥给 S3 客户端用（显式指定，与下面验收段的 AK/SK 一致；重复执行会提示「access key 已存在」，幂等）
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"access_key":"AKIA_TEST_ACCESS_KEY","secret_key":"SK_TEST_SECRET_KEY_0001"}'
```

> 注意：暂存盘下的 `_header/` 与各分组目录都由 `aether::build_runtime` 在启动时创建
> （分组目录经 `aetherupload::console::list_groups`，`_header/` 单独补建——list_groups
> 只在自建根目录时顺带建它）；若预处理报笼统的上传错误，先检查暂存盘目录是否被清理。

> 已知限制：
> - `/aetherupload/*` 四条路由**需 JWT**：登录后带 cookie `oss_token` 或 `Authorization: Bearer <token>` 访问
>   （前端在登录成功时种 cookie，M1b）；缺/坏/过期一律 401。彻底的角色/策略模型仍随部署形态（M4+）。
> - ListObjects **v1 未实现（501）**；rclone 用 `provider=Other` 时须设 `LIST_VERSION=2`（验收脚本即这么做）。
> - 单次 PUT / 单个 part 仍受 `max_put_bytes`（默认 256 MiB）上限；multipart 大对象整体不受该值约束。
> - Multipart 边界：`UploadPartCopy`、`ListMultipartUploads`、GET 的 `partNumber` 参数未实现（M4+）；
>   多区间 Range（`bytes=0-9,20-29`）→ 400 `InvalidArgument`（s3s 解析层拦截）；HEAD 带 Range 不支持。
> - **对象标签未实现（501）**：`Put/Get/DeleteObjectTagging` 与 PUT/COPY 的 `x-amz-tagging*` 头未接线（工作令未排；功能规格 §3.7 要求）。
> - **CopyObject `REPLACE` 未实现**：`x-amz-metadata-directive: REPLACE` 与 `x-amz-tagging-directive` 被忽略——始终按 COPY 继承源元数据（规格 §3.5 标注「待后续」，此处显式记档）。
> - **控制台页面缺口**：M5 加密/KMS 设置页、M6 事件页与 Select 工作台、M4 桶策略编辑与审计查询 UI 未做；对应后端 API 均已完整（可用 aws-cli / 控制台 API 操作）。
> - Multipart **part 级** `x-amz-checksum-*` 不校验（静默忽略，M2b 接组合语义）；单段 PUT 的
>   Content-MD5 / `x-amz-checksum-*` 已在提交点前校验（不符 → `BadDigest`，对象不落盘）。
> - 控制台「上传记录」是登记历史：对象或暂存文件被删后，记录行仍在（按设计，作审计线索）。
> - `oss.toml` 默认 `listen = "0.0.0.0:9000"` 与占位 `jwt_secret` 仅限本机开发；对外部署前必须换成回环绑定与随机密钥
>   （占位密钥可被用来伪造控制台与 `/aetherupload/*` 的 token；与 0.0.0.0 绑定合用即 LAN 风险面）。
> - **多盘（M2b）运维边界**：
>   - `data_dirs` 配 2..11 个盘会在启动时**报错**（EC 8+4 的冗余需要 ≥ 12；单盘可运行但无冗余，启动 warn）。
>     `[ec] enabled=false` 恢复 M1 旧形态（只允许单盘）。
>   - **首启前确认各盘挂载点已挂载**：首启时「未挂载的挂载点目录」与全新盘不可区分，会被当作新盘初始化。
>     运行时掉线的盘会被自动判为不在册（写按 quorum 绕行、删对象 fail-closed、插回自动恢复），
>     但它**必须曾在挂载状态下启动过**（盘上有 `.oss.sys` 与 `drive-id`）。
>   - 掉线盘在场时 **删对象/删桶返回 500**（防「插回即复活」）；这是有意为之的 fail-closed，插回后重试即可。
>   - 读 meta 副本择新自 M4 起按 `(epoch, mod_time)` 排秩——写端 epoch 单调 +1，NTP 回拨不再产生「旧副本凭大钟胜出」；
>     epoch 随 `xl.meta` 前向兼容（老文件按 0 参与排秩）。
>   - 目录 fsync 的断电零丢失保证按 ext4/XFS 假设；其它文件系统上目录 fsync 失败仅 warn，需自行复核。
> - 长跑验收（rclone 并发 100×100 MB + 中断重试，设计验收 #3）在 `tests/e2e_s3.sh` 的
>   `E2E_LONG=1` 门控段（默认跳过，人工触发；`E2E_LONG_N`/`E2E_LONG_MB` 调规模）。
> - **权限体系边界**：
>   - 授权模型 = 每（子账户 × 桶）三档（读/写/删）+ 桶主全权 + 管理员全量；**无行 = 无权限**。
>     S3 面（AK/SK）、控制台、aetherupload 上传三个入口执行同一份判定。
>   - 子账户可在控制台/ S3 建**自有桶**（自动全权）；删桶限桶主或管理员；历史遗留桶
>     （无归属记录）仅管理员可见。
>   - 管理员可**停用/启用**账号（Users 页）：停用即全链路失效（登录 401、既有 JWT 下一请求 401、
>     AK 403、上传面 401；无主动吊销名单，靠下一请求时校验）。任何人可在「访问密钥」页**自助改密**
>     （验旧口令）；管理员也可为他人重置口令。
>   - 控制台上传只能进「`[[groups]]` 分组映射的目标桶」——子账户要往自有桶传，走 S3 面。
>   - 撤回授权对**在途分块上传**不追溯（preprocess 已放行的会传完）；新请求即时生效。
>   - 建子账户 API 固定为非管理员（防提权）；口令至少 8 位；删除用户级联清其密钥与授权。
>   - 口令存 **Argon2id**（m=19456, t=2, p=1）；历史 sha256 口令在**成功登录时透明升级**（无需重置）。
>   - **登录限速（C1）**：账号维度 10 分钟窗口失败 ≥5 次 → 锁 10 分钟；全局维度 60 秒窗口
>     失败 ≥120 次（跨用户名，防随机用户名洪水刷 Argon2）→ 全体暂拒（429 + `Retry-After`）。
>     单进程内存态：每节点独立、重启清零（限速非持久配额）。只护登录口；已认证的改密不在此列。
>   - **桶策略只在 S3 面生效**：控制台与 aetherupload 上传面仍走三档 permissions。
>   - **删桶连带清空该桶策略与生命周期规则**（S3 面与控制面删桶都清）——同名重建不继承，防旧规则误删。
> - **多 set（M4）边界**：
>   - 多 set 下**数据面每 op +1 次本地 DB 查询**（桶 → set；本地 miss 才扇出对端；单 set 零新增）。
>     解析 fail-closed：传输错 / set 越界 / 对端答复不一致一律上抛，**绝不静默回落 set0**。
>   - **跨 set 桶名唯一性在并发建桶下是 best-effort**（既有跨节点查重 TOCTOU 同款）。
>   - **`[[groups]]` 分组的目标桶钉 set0**（控制台上传面不参与新 set）。
>   - **加盘只能加在最后一个 set**（前滚约束）：往中间 set 加盘 → 启动指纹拒启，不可行。
> - **加密与对象锁（M5）边界**：详见「加密（M5）」「对象锁（M5）」两节记档段；跨节点要点——
>   桶锁状态列（`buckets.object_lock*`）随控制面行广播传播，属**最终一致**：窗口期内非版本化
>   覆盖写/回收路径 **fail-closed 拒删**（不静默放行）；行同步强一致归 M9。**多节点加密须先
>   把同一把主密钥（文件或 KMS 目录整份）分发到全部节点**再开写，否则本节点读不了别节点写的对象。

## Docker（compose）

**单机**（oss-server + 控制台 nginx）：

```bash
docker compose up --build        # 控制台 http://localhost:8080 · S3/API http://localhost:9000
```

**M3 集群**（4 节点 × 3 盘 = 12 槽；服务名即节点地址）：

```bash
docker compose -f docker-compose.cluster.yml up --build
# S3/控制面入口：n1:9000 · n2:9001 · n3:9002 · n4:9003（任意节点等价）；控制台 :8080 反代 n1
```

- 配置：单机 `docker/oss.toml`；集群 `docker/cluster/n{1..4}/oss.toml`（静态拓扑，注释在文件里）。
- **密钥自动生成**：`jwt_secret` / 集群 `secret` / 管理员口令**没填**就首次启动自动生成（64 hex），
  落盘宿主 `./docker/secrets/generated.env`——0644 直接 `cat` 看管理员口令，已 gitignore；
  集群四节点共享同一份（flock 串行化，并发首启收敛到同一份）。**轮换 = 删掉该文件重启**；
  想用自有值：在 `oss.toml` / `OSS_ADMIN_PASSWORD` 里显式填写（显式值优先，绝不覆盖）。
- 数据/暂存在具名卷（`docker compose down -v` 才清）；集群每节点独立卷。
- S3 客户端**直连节点端口**（SigV4/流式不经 nginx）；控制台走 :8080 的反代。
- 镜像：`Dockerfile`（oss-server release 构建，含 protoc）与 `docker/Dockerfile.web`（vite 构建 + nginx）。
- 集群 compose 的节点 `addr` 用服务名（主机名）——引导解析绑定 + 对端按名拨号（同一份配置两用）。

## 控制台（M1b）

```bash
# 1. 起后端（同 M1a）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173 ，用上面的账号登录
```

- 上传复用 aetherupload 自带脚本（分块 / 断线续传 / 秒传），前端不含分片逻辑
- **子账户**（管理员菜单）：创建/删除子账户、逐桶勾选 读/写/删 授权；子账户登录后只见
  授权桶（菜单无「子账户」页），上传与删除按钮按权限显隐（后端 403 兜底）
- **集群**（管理员菜单）：模式 / 槽位 / quorum 未达计数 + 节点表 + 拓扑指纹（15s 自刷新）；单机显示单机模式提示
- **上传落点**：按上传分组落桶（`oss.toml` 的 `[[groups]]`，如 `file` → `files`）；对象名 = 内容的整份 MD5 +
  扩展名，子目录 = UTC 年月（如 `202610/<md5>.bin`）。所以在对象浏览器里是根据内容哈希找文件，不是原始文件名
- 秒传提示「上传成功（秒传）」表示 0 分块传输
- e2e：`cd web && npx playwright install chromium && npm run e2e`。Playwright 自带双 webServer
  （`cargo run` + `vite`），后端按 `web/playwright.config.ts` 里的 `pw` 自启；**已有后端在跑时会被复用**
  （`reuseExistingServer`），此时运行中实例的管理员口令必须也是 `pw`，否则登录 401——口令不一致就先停掉
  自起的服务，让 Playwright 用 `pw` 起自己的（「跑起来」段用的 `changeme` 只是示例占位）。
- 1 GB 上传与断线续传为**人工验证项**（不在 e2e 里）：
  `head -c 1073741824 /dev/urandom > /tmp/1g.bin` → 面板上传观察进度推进 → 中途 DevTools → Network → Offline
  （或拔网线）→ 输出区出现「网络故障，正在重试……」→ 恢复网络 → 自动继续至成功；下载后 `cmp` 与本地一致。
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传未排期；当前限同一页面会话）

## 运维命令

`oss-server [serve|groups|build|clean <days>|fsck [--repair]|heal|scrub|rebalance [--dry-run|--apply]]`（无参数 = `serve`）。运维命令读当前目录的
`oss.toml`，并走与服务端相同的启动自举（建目录/连库/开存储；管理员与桶的引导是幂等的）：

```bash
oss-server groups        # 建分组目录（幂等；启动自举也会做，上传报笼统错误时先跑它确认）
oss-server build         # 按磁盘现状重建秒传索引（先清旧索引，再全量扫描暂存盘）
oss-server clean <days>  # 按 mtime 回收 days 天前的残骸：
                         #   暂存盘 `_header/` 与 `*.part`；数据盘 `.oss.sys/{tmp,trash}` 孤儿
                         #   与各对象目录下未被引用的版本目录（含未完成的上传暂存区）
oss-server fsck          # 全盘巡检：孤儿版本目录 / EC 分片缺坏（CRC32C）/ tmp-trash 计数 /
                         #   stray 分片文件（rebalance/崩溃残留；--repair 删除）；
oss-server fsck --repair #   同上 + 最小修复（删孤儿 + 读时修分片）。报告落
                         #   `.oss.sys/fsck/<时间戳>-<kind>.json`
oss-server heal          # fsck --repair 全量 + 缺失桶目录补建（集群差集）+ 逐版本修复
                         #   （历史版本同样点名读触发回写）；每节点各自执行——本地语义：
                         #   补的是本机盘上的缺口，集群收敛 = 各节点各跑一次
                         #   禁忌同 fsck --repair：不得与在途上传/写入并跑（读时修/孤儿
                         #   清理与在途版本目录竞争）
oss-server scrub         # 全量深度巡检（只读）：每个 EC 版本逐块读取 + 逐块 12×W 对偶校验
                         #   （可定位坏片；常量内存不组装整份对象）；历史版本全覆盖；
                         #   健康对象计 ok_versions；可重建的坏/缺块不退出非零（数据仍在
                         #   EC 冗余内、读路径自愈），仅深度失败（存活不足/对偶不过）exit 1（cron 可盯）
oss-server rebalance     # 扩容搬迁（默认 dry-run）：存量对象迁到展开放置（placement=v2；
                         #   未开则空跑）；逐对象 copy→锁内翻转→清老片，幂等可重跑；
                         #   报告 `<时间戳>-rebalance[-dry].json`（操作序见「扩容」节）
```

分工：`fsck` = 整片 CRC32C 快检（轻）；`scrub` = 块级深度验证（慢、可定位坏片）。

建议 cron（每日 `clean 1` 与 `build` 各一次，每周 `fsck --repair` 一次）：

```cron
17 4 * * * cd /path/to/oss && ./oss-server clean 1
47 4 * * * cd /path/to/oss && ./oss-server build
23 4 * * 0 cd /path/to/oss && ./oss-server fsck --repair
```

- `clean` 按 mtime 判定：上传持续时长在回收窗口内时不会被误删；超出窗口的长挂上传（仅 S3 直传可达，
  aether 与本地上传是分钟级）可能在仍进行时被判过期——本次写入失败、无数据损失；`days` 限 1..=36525
- `build` 先清空秒传索引再按盘重建：重建瞬间的并发上传会从「秒传命中」回退为正常上传，不丢数据
- 未知子命令或非法参数打印用法并 exit 2；命令失败 exit 1
- 配置文件路径固定为当前目录的 `oss.toml`（原先的 `oss-server <配置路径>` 用法已移除）

## 集群模式（M3）

**静态拓扑**：扁平 node × drive，配置即拓扑（无 DC/区域层级）。未配 `[cluster]` = 单机（本机行为与 v1.4.0
逐字节一致）；配置要求 **槽位数 ≥ data+parity（12）**。

```toml
[cluster]
node_id = "n1"
secret  = "cluster-internal-token-change-me"   # 节点间 gRPC 共享令牌（明文，内网假设）

[[cluster.nodes]]
id = "n1"
addr = "127.0.0.1:19101"   # 该节点 gRPC 监听（HTTP 端口另配）
drive_count = 3
# …n2/n3/n4 同构
```

启动：各节点在自己的目录放好 `oss.toml`（`data_dirs` 三条 = drive_count 3）后 `oss-server serve`；
顺序无所谓（滚动启动探活只告警；可达对端 node_id 与配置不符会拒启，防错接线）。

**临界口径（4 节点 × 3 盘 = 12 槽，EC 8+4）**：
- 停 1 节点（剩 9 槽 = 恰好 Q_s）：**写零余量**（9 片全须成功，客户端重试收敛）、读 9≥8 有余；
  本配置**单节点故障即临界**——要余量用 6×2（12 槽，停 1 余 1）。
- 停 2 节点（剩 6 槽 <9）：写被拒（锁/meta quorum 未达 → 5xx）；**读同不可达**——
  GET 报 `500 CorruptMeta`（**不是 404**，不许把「quorum 覆盖不足」伪装成「不存在」）。

**配置反例**：4×4 = 16 槽**勿用**——EC 只有 12 分片，`shard % 16` 只落槽 0..11，第 4 节点零分片，
停任一持片节点剩 8<9 反而更容易不可写。

**运维口径**：
- 每节点定期 `clean` + `fsck --repair`（同前 cron）；**远端孤儿只由该节点自己回收**——
  掉线期间其他节点删不掉它的残骸。分片缺失靠读时修兜底 + fsck 巡检；缺键 meta 由
  `fsck --repair` 从对端差集检出并补建（「加回节点后补齐」；批量口径用 `heal`——差集补建 + 桶目录补建 + 逐版本修复）。
- 节点掉线期间 `clean` 的对象目录回收会**保守空转**（不可逆删数据前全节点探活）；`trash/tmp` 照常。
- **`fsck --repair` 不得与在途分块上传并跑**（无龄期删孤儿会清掉暂存区）。
- **M3 不支持单机原地升级为多节点**（无 rebalance/搬迁，归 M4）：单机数据要进集群请重新灌入。
- `data_dirs` 顺序是拓扑的一部分：错位会在启动时被 `.oss.sys/topo` 指纹拒绝（大声失败）。

**观测**：`GET /metrics`（Prometheus 文本；**公开**，注释已写明与 `/health/cluster` 同量级信息）——`oss_quorum_unmet_total{op="write"|"lock"}` ·
`oss_node_up{node=…}` · `oss_drive_count` · `oss_slot_count`。控制台「集群」页（管理员菜单：模式 / 槽位 / quorum 未达计数 +
节点表 + 拓扑指纹，15s 自刷新；单机显示单机模式提示）与 `GET /api/v1/cluster/status`（admin；单机优雅降级 `mode:"single"`）同源。

```yaml
scrape_configs:
  - job_name: oss
    static_configs: [{ targets: ['oss:9000'] }]
```

**已知边界（M3）**：
- 分块上传（MPU）会话**粘节点**：同一 upload 的后续请求须回到发起节点。
- 节点间锁无写入侧 fencing：租约 TTL 远大于单写操作；兜底是版本 uuid 隔离（交错写最多留孤儿目录，
  由各节点 clean 回收，已提交数据不损坏）。
- 控制面（账号/权限）**读扇出**：任一节点铸造的 AK 全集群可用；写入仍只落本节点
  （跨节点强一致归 M9）。
- 节点间 gRPC **无 TLS**（内网假设）；traceparent 轻量传播（`RUST_LOG=debug` 下可按同一 trace id
  在双节点日志抽样；完整 OTLP 归 M4）。
- 命名勿混淆：oss-server 的 aetherupload `staging_dir` ≠ 存储内核的 `.oss.sys/tmp`。
- **升级须整栈同版本**（M4 起）：分片 RPC 带分块字段；新旧混跑的写会安全失败（新协调者→旧节点被旧端 CRC 门拒收，
  该片失联、可能 quorum 不过导致写失败），跨界读按缺失降级（EC 兜底但白读）。**不要滚动升级**：先停后换或按整栈重建。
- **EC 区间读语义**（M4 切片 2）：Range 读不再做整片 CRC32C / 对象级 SHA256（改逐块 12×W 对偶校验，坏片可定位自愈）；
  **范围外的损坏不再触发读时修**（整读仍会），批量修复归 `heal`（M4 切片 4）。

## 扩容（rebalance，M4）

追加式扩容：放置开关（`placement = "v2"`）+ 尾部追加拓扑 + 逐对象搬迁。**操作序**（顺序不可乱）：

1. **全节点升级二进制**（placement 仍默认 v1，行为不变）。
2. 各节点 `oss.toml` 加 `placement = "v2"`——**全节点同步**。混版窗口内**部分写入会降级/失败**（可诊断）：
   旧节点发出/转发的分片请求没有 slot（proto 未知字段 → 0），新服务端按「slot 0 属主」校验而拒（除 slot 0
   属主那台）；反向（新→旧）旧服务端忽略未知字段按 v1 重算 = v1 下结果正确。**升级窗口只做升级、不承载业务写；
   全节点升级完再开 `placement="v2"`。**
3. 追加拓扑（加盘/加节点，见下）。
4. 逐节点 `oss-server rebalance --dry-run` 看计划。
5. `oss-server rebalance --apply`（幂等，可重跑续搬；同盘搬迁走 **hardlink 快路径**，C4——
   免整片重拷、崩溃安全，跨设备自动回落分块 copy）。

**加盘**：只能往**最后一个节点**追加 `data_dirs`（中间节点加盘会位移其后节点槽位，启动时指纹拒启）。
新盘必须先「入册」——在已挂载的新盘上
`mkdir -p <盘>/.oss.sys && cp <任一旧盘>/.oss.sys/drive-id <盘>/.oss.sys/`
（防遮挂载点 impostor 的既有策略不允许自动初始化非首启盘）。
注意：**单节点盘数不得超过 parity**（默认 8+4 即每节点 ≤4 盘，超过启动拒启）——加盘是 3→4 而非 3→5；
更大容量走**加节点**（新节点同样 ≤4 盘）。

**加节点**：`cluster.nodes` 尾部追加 + 该节点自备配置启动。**加节点后也要全节点 placement=v2 + rebalance**
（v1 恒等放置永不使用新槽）。

**单机多盘**：超过 12 盘时自动 v2（无需配置）；旧对象用 `rebalance` 搬迁。

**边界**：不支持缩容（删节点/盘）；**配置回退也不可行**（旧槽位引用 fail-closed 500），只能前滚；
同刻只跑一个 rebalance；`rebalance` 与 `fsck`/`heal` **不得并跑**——fsck 的 stray 扫描会把搬迁中
「已 copy 未 flip」的半成品新片当作残骸删除（对象级自愈可重跑，但白搬一轮）；搬迁残窗与老片清理
兜底归 `fsck`（stray 分片扫描，跑在 rebalance 停后）。

## 版本控制（M4）

`aws s3api put-bucket-versioning --versioning-configuration Status=Enabled` 开启；`list-object-versions` 枚举；
GET / HEAD / DeleteObject 带 `--version-id` 点名版本；`copy-object --copy-source 'bucket/key?versionId=<旧>'` 即**回滚**
（旧版本 → 新版本）；无版本号 DELETE = delete marker（GET 404、对象列表隐身、版本列表可见）。`tests/e2e_s3.sh` 第 16 节实弹覆盖。

**v1 边界（诚实记档）**：
- 版本控制状态**单向**：只能 Off→Enabled；已启用的桶拒绝改回 Off（501，防整链替换丢历史），Suspended 亦 501（未实现）；
  未开过版本的桶 `get-bucket-versioning` 回 `Off`（AWS 留空）。
- 所有写响应透出 `x-amz-version-id`（PUT / Complete / Copy / DeleteObject；DeleteObject 另带 `x-amz-delete-marker: true`）；
  GET / HEAD 也回实际版本号。点名 GET / HEAD marker 版本 → 405。
- 对象列表（ListObjectsV2）只见「链尾非 marker」的键；区间读对历史版本同 S3 语义。
- `ListObjectVersions` v1 **忽略 delimiter**（不产 CommonPrefixes）；续点（key-marker / version-id-marker）不匹配链时
  返回 400 InvalidArgument（fail-loud）。
- 集群：版本追加仍走 write_meta quorum（整份 meta 重写）；`list-object-versions` 集群路径为逐键 quorum 读
  （低频运维面，量级上去再做分页 RPC——记档）。
- 对象锁（M5）：开锁桶须版本控制 Enabled（建桶旗标会一并自动开）；锁定版本的删除语义见「对象锁（M5）」节。

## 生命周期（M4）

控制台 API `PUT /api/v1/buckets/{bucket}/lifecycle`（body `[{prefix, expire_days, enabled}]`，**全量替换**；
桶主或管理员）配置，`GET` 同路径回读。后台 worker 按 `OSS_LIFECYCLE_INTERVAL_SECS`（默认 60s；显式 `0`
关 worker）扫描，到期判定 `now - last_modified ≥ expire_days×24h`（**0 = 立即到期**，测试/激进清理用）
自动删除：版本控制桶 = 打 **delete marker**（不物理删；noncurrent 版本过期未实现，记档），非版本桶 = 整键删除。

进度：`GET /api/v1/jobs`（admin）看最近作业（scanned / deleted / skipped / state / error，逐规则一条）。

**边界**：
- 转换/分层归 M8。
- **同刻建议只跑一个 serve 实例**：每实例各挂 worker（多实例会重复执行；幂等但白跑）。
- **大桶性能**：worker 每周期对该桶全前缀列举一次——前缀很大时建议调大 `OSS_LIFECYCLE_INTERVAL_SECS`。
- **饥饿边界**：单轮单规则 ≤10 万键（100 页 × 1000）；超出即**截停记 jobs.error**（`state=error`，不静默）——
  但**扫描位点不跨轮持久化**，前缀下若长期 >10 万「未到期」键，尾部的到期键可能长期扫不到（调小 prefix 拆规则可解）。
- **陈旧作业行**：进程崩溃可能留下 `state='running'` 的 jobs 行（无自动回收，展示层可按 updated_at 过旧识别）。

## 桶策略与审计（M4）

控制台 API `GET/PUT /api/v1/buckets/{bucket}/policy`（桶主或管理员；**桶不存在 = 404**——防给
尚不存在的桶预置策略、同名桶日后创建即继承）：`PUT` 为**全量替换**（≤64 KiB、≤100 条 statement、
顶层必须对象、`Effect` 必须为 `Allow`/`Deny` 且 Principal/Action/Resource 非空；越出支持子集或
拼错即 400，不静默空转），`GET` 回读原文（未配 = `null`）。S3 面判定口径：**显式 Deny 优先 →
Allow 提升 → 无匹配回落三档 permissions**；策略读/解析失败 fail-closed 拒。`CopyObject`
**源/目标两轴独立判定**：目标桶写策略与源桶读策略各判各的，**源桶 Deny 压过目标桶 Allow**
（否则有目标桶写权者可绕过源桶 Deny 拷走机密），且**源桶 Allow 不能替代目标桶写权限**。
`DeleteObjects` **逐键授权**：批量删的键在请求体、`check` 阶段不可见，授权下放 handler 逐键判定
（与单键删同口径——策略 Deny → 拒 / Allow → 放 / 无匹配 → 回落三档）；Deny 键以 `AccessDenied`
记入响应 `errors[]` 并落审计，其余键照删（§3.4 部分失败语义；否则前缀级 Deny 会被一条批量删
请求整体绕过）。

支持子集（`crates/xl-s3/src/policy.rs`）：`Effect` · `Principal.AWS`（`"*"` | AK | `"uid:<n>"`）·
`Action`（精确或尾部 `*` 通配，如 `s3:Get*`）· `Resource`（桶级 `arn:aws:s3:::<bucket>` /
对象级 `arn:aws:s3:::<bucket>/<prefix>*`，**按 op 类别单轴匹配**——桶级 op 只见桶 ARN、
对象 op 只见对象 ARN，与 AWS 一致）。**明确不做**：Conditions / NotAction / NotResource /
Principal 的 ARN 形态 / **匿名（无凭据）放行**——`Principal:"*"` 只对任意**有效 AK** 生效，
真正匿名需认证层改造。显式 Deny 对桶主与管理员同样生效（AWS 语义，e2e 实测）——管理员只旁路
三档 permissions，不旁路桶策略 Deny。

审计：S3 面每次授权判定（允许与拒绝都）落一行 SQLite `audit_log`，`detail` 记策略来源
（`policy:allow` / `policy:deny` / `policy:error`）；**保留期清理（C5）**：`OSS_AUDIT_RETENTION_DAYS`
（默认 0 = 不清理）开启后每小时清 `ts` 超出保留期的行（best-effort，失败只 warn）。
（`policy:allow` / `policy:deny` / `policy:error`）；`GET /api/v1/audit?limit=&decision=allow|deny`
（仅管理员；limit 默认 100、钳 1..=1000；`decision` 只认 allow/deny，其它值视为不过滤，最新在前）。
`tests/e2e_s3.sh` 第 17 节实弹覆盖（Deny 优先 / 校验 400 / 审计查询 / 清策略恢复 / 删桶清策略 /
批量删逐键授权）。

## 多 set（M4）

单部署多 EC 组（set）的横向扩容形态：新 set 加容量，**老 set 零重排**；单 set（k=1）与既有部署
逐字节一致（RPC 缺省字段、指纹、放置全等价；原地升级 = 末尾追加 `set=1` 条目，老段不动）。

**配置**：`[[cluster.nodes]]` 新增可选 `set`（缺省 0）。同一物理节点可在多个 set 各出一条
（同 `id`、**`addr` 必须一致**——节点间 RPC 只按 id 路由，不一致拒启）；set 号必须从 0 连续
（缺号 fail-closed）。**`data_dirs` 顺序契约（硬）**：按 set 升序、set 内按节点声明序拼接；
本节点各 set 的 `drive_count` 之和 == `data_dirs.len()`（open 自校验）。每个 set 独立走全套拓扑
校验（槽 ≤255、≥ data+parity、多节点时每节点盘数 ≤ parity；flat 并集另受单节点总盘数 ≤255 约束）。

```toml
[[cluster.nodes]]          # set0 段（缺省，可不写 set）：n1..n3 各一条
id = "n1"
addr = "127.0.0.1:19101"
drive_count = 3
# n2 / n3 同构…

[[cluster.nodes]]          # set1 段：同物理节点再出一条（原地升级 = 末尾追加，老段在前不动）
id = "n1"
addr = "127.0.0.1:19101"   # 同 id 多 set 的 addr 必须一致
drive_count = 3
set = 1
# n2 / n3 同构…
```

**桶钉 set**：控制台建桶（`POST /api/v1/buckets`）可选 `set`（校验 < 组数；**缺省自动指派**
到桶数最少的 set，C3）；S3 面 CreateBucket（协议无 set 形参）缺省同样**自动指派**到负载最小的
set（单 set 恒 0）。归属存 `buckets.set_id` 列（老库启动幂等补列，存量 = 0）。
**set 一经创建不可变**——没有改 set 的 API，已有桶不跨 set 搬迁。

**隔离口径**：set 只约束**数据分片**（放置公式与分片 RPC 写/读/删）；**桶目录与 `xl.meta`
跨全部 set 镜像**（命名空间面：每盘一份完整命名空间副本，全盘迭代）——空桶目录与元数据副本
出现在非属主 set 的盘上属预期，不是串组。

**拓扑指纹**：每盘一条。set0 段盘写 `v2 node=… slot=…`（与单 set 部署逐字节一致、不重写），
set>0 段盘写 `v3 set=… node=… slot=…`（slot 为**组内槽**）；`v3` 且 `set=0` = 拒启（写路径永不
产出该形态）。

**扩容约束**：加盘只允许加在**最后一个 set**（往中间 set 加盘会位移其后段 → 启动指纹拒启）；
跨 set 不搬迁已有桶——多 set 是给新桶加容量，不是对存量重分布。

**观测**：`GET /api/v1/cluster/status`（admin）新增 `sets` 数组（每 set 一条：组内槽数、节点表、
本节点是否属该组）；顶层 `nodes` / `slot_count` 为按 id 去重的节点并集（命名空间面口径）。
**各 set 的槽号是独立槽域，跨 set 会重号——按 set 分组读**（控制台「集群」页已按 set 分组展示，
C2）。

## 加密（M5）

**顺序是硬约束：先加密，再纠删码**——写路径 `明文 → 块对齐流式加密（AES-256-GCM，DARE 式
65520B 明文单元 → 65536B 密文单元）→ EC 编码 → 落盘`，读路径反向；因此**单个分片（不足 quorum）
不含任何明文信息**，parity 块同理（验收 ②）。

三种模式由请求头选定（**env 一个不配 = 加密整体不启用，零行为变化**）：

| 模式 | 启用 / 请求 | 密钥来源 | 盘上存什么 |
|---|---|---|---|
| SSE-S3 | env `OSS_MASTER_KEY_FILE=<路径>`；头 `x-amz-server-side-encryption: AES256` | 单主密钥文件（缺省 `<data_root>/.oss.sys/master.key`，0600 首启自动生成；**多节点须分发同一把**） | 包装后的 DEK，`key_id="file"` |
| SSE-KMS | env `OSS_KMS_DIR=<目录>`；头 `aws:kms` | 多钥目录 `key-<n>.bin`（当前钥 = 最大 n） | 包装后的 DEK + `key_id="k<n>"` |
| SSE-C | 头 `x-amz-server-side-encryption-customer-{algorithm,key,key-MD5}`（每次请求带 32B 客户端钥） | 客户端 | **只存 `base64(MD5(key))` 校验值（AWS 同款 24 字符头），密钥绝不落盘** |

- 每对象一把 32B DEK（OsRng 生成）；单元 nonce = 随机种子(8B) ‖ BE32(单元号)，单元号把
  （EC 分片, 块）双射到密文单元——同一 DEK 下 nonce 不重用。
- 响应头：PUT / HEAD / GET 回 `x-amz-server-side-encryption`（SSE-C 另回 customer-key-MD5 等
  校验头），`aws-cli` 的 `sse` 参数直连可用。
- **口径**：ETag / Content-MD5 / SHA256 全部对**明文**计算（协议语义不变）；分片 CRC32C 对**密文**；
  SSE-KMS 请求若**显式带 CMK**（`…-aws-kms-key-id`）→ 400 拒绝（多主密钥轮换编排未做，
  明确拒优于静默忽略）；读/删 SSE-C 对象必须带同一把钥（错钥 → 400，校验值即判据）。
- MPU：SSE 意图在 CreateMultipartUpload 记录（含 SSE-C 校验值），Complete 统一落地；
  SSE-C 的 UploadPart / Complete 须全程带同名钥；读写响应带版本/加密头。
- **换主密钥不重写数据**（验收 ⑤）：DEK 的包裹体在 `xl.meta`，只要旧 `key_id` 的钥仍在即可解——
  轮换 = 往 `OSS_KMS_DIR` 加入更大的 `key-<n>.bin`（新写用新钥，旧对象照读）；
  单钥文件 provider 直接换文件会让旧 `key_id="file"` 的包裹不可解（**先迁到 KMS 目录再轮换**）。
- **SK 加密存（M5）**：`access_keys.secret_key` 同样信封加密（`enc:v1:` 前缀，AAD 域分隔，
  每行独立 DEK；kms 目录优先、单钥文件兜底）——**两处写点**（控制台建键、STS 签发）与**读侧
  透明迁移**（存量明文行被读到即原地重写为密文）；集群下密文过网、消费端解密（mesh 不出现
  明文 SK）。**运维硬约束**：多节点须分发同一把主密钥；**撤掉 `OSS_MASTER_KEY_FILE`/`OSS_KMS_DIR`
  环境变量会让既有密文行全部 500（fail-closed）**——变更前先规划。**威胁模型边界（记档）**：
  加密只防**只读**泄露（备份/快照/误拷贝）；对数据库有写权限者可写入自选明文 SK 接管任意 AK
  （切片前有 DB 写权本就全权，非新增面）。
- **边界（记档）**：桶级默认加密（`put-bucket-encryption`）未实现；aetherupload 内部摄取
  不带加密（存入为明文）；未配对应 env 时请求该模式 → 500（fail-closed，不静默落明文）；
  HTTP KMS 未实作（`KeyProvider` trait 已留接入点，文件型 KMS 即多钥目录）。
- `tests/e2e_s3.sh` 第 18 节（随 M5 收尾落地）实弹覆盖「写入 → head 头断言 → 盘上 grep
  明文特征串」。

## 对象锁（M5）

**桶级**：`CreateBucket` 带 `x-amz-bucket-object-lock-enabled: true` 建桶即开锁（**自动开版本
控制**，AWS 语义）；或对已开版本控制的桶 `PutObjectLockConfiguration` 开锁并设**默认保留规则**
（`Days`/`Years`，归一为天存储，上限 36500 天；`Enabled` 只能开不能关——二次 Put 只许改规则）。
默认规则对**未带锁头**的 PUT 生效（显式锁头优先，不叠加）。

**对象级头**（PutObject / CopyObject 目标 / CreateMultipartUpload；MPU 在 create 时记录、
complete 时落地）：

- `x-amz-object-lock-mode: GOVERNANCE | COMPLIANCE` + `x-amz-object-lock-retain-until-date`
  —— 必须**成对**且 until 在未来（否则 400）；桶未开锁带锁头 → 400。
- `x-amz-object-lock-legal-hold: ON | OFF` —— 独立于 retention。

**删除守卫**（点名删版本 / 批量删 / 覆盖写回收 / 控制台删路径统一）：`until ≤ now`（到期）可删；
未到期 **COMPLIANCE 一律拒（含管理员）**；未到期 **GOVERNANCE 仅
`x-amz-bypass-governance-retention: true` 放行**（调用方须已鉴权）；**Legal Hold ON 一律拒**。
拒删 = 403 `AccessDenied`（批量删逐键记入响应 `errors[]`，其余键照删）。无版本号 DELETE 照常打
delete marker（锁定版本天然留存）；生命周期删除同理。
Legal Hold 开关 API：`put/get-object-legal-hold`（`put` 须点名版本且**桶已开锁**，否则 400；
`get` 为元数据面读——SSE-C 对象不要求客户钥）。
响应面：GET / HEAD 回 `x-amz-object-lock-mode` / `-retain-until-date` / `-legal-hold`。

**边界（记档）**：`Put/GetObjectRetention`（改保留期）未接线（延后）；控制台尚无锁状态展示
（S3 API 面完整可用）；`buckets` 表锁列跨节点的**行广播**传播，窗口期内非版本化回收路径
fail-closed 拒删（强一致归 M9，见「已知限制」）。

## STS 临时凭据（M5）

标准 STS 协议（Query/XML）由 s3s 前置路由拦截（`POST /` + form 编码；**须 SigV4 签名**，
不覆写 `check_access`）。实现两种操作：

```bash
# AssumeRole（调用者须**管理员**——最小模型无角色信任策略，防子账户自铸 admin 会话提权；
# 「角色 = 用户别名」，ARN 形如 arn:oss:iam::oss:role/<用户名>）
aws --endpoint-url http://127.0.0.1:9000 sts assume-role \
  --role-arn arn:oss:iam::oss:role/erik --role-session-name s1 --duration-seconds 900 \
  [--policy '{"Version":"2012-10-17","Statement":[...]}']
# GetSessionToken（任意长时键自派生，TTL 900..=129600 秒，默认 12h）
aws --endpoint-url http://127.0.0.1:9000 sts get-session-token --duration-seconds 900
```

- 响应含 `AccessKeyId`（`ASIA…`）/ `SecretAccessKey` / `SessionToken` / `Expiration`；
  临时凭据对 S3 面**全集群可用**（会话行随 AK 扇出）。
- **会话策略（仅 AssumeRole 的 `Policy` 参数，≤4096B）与用户权限取交集**：在表三档 × 桶策略
  融合**之后**再叠——会话 Deny → 拒；**无匹配 → 拒**（未 Allow 即拒）；会话不能抬升权限。
  `DeleteObjects` 逐键同样叠会话判定（防批量删绕过）；审计 detail 缀 `;session:<allow|deny>`。
- **过期**：会话行过期 → `ExpiredToken`（长时键过期维持 `InvalidAccessKeyId`）；**父链一层**：
  父键（或父属主账号）删/停/过期 → 子即失效（父须为长时键；会话不能作父）。
- **边界（记档）**：`SessionToken` 本体不落库不校验（AK 即凭据——s3s 认证接口无头面，AWS
  信任链差异）；错误响应用 S3 错误 XML（非 STS `ErrorResponse` 形态，成功路径 aws-cli 直用）；
  GET query 形（`?Action=`）不接；时长越界报 400 `InvalidArgument`（AWS 为 `ValidationError`）；
  会话**只对 S3 面**生效（控制台/aether 走 JWT，无 AK 入口）；过期会话行不自动清理（自然不可用）。
- **AssumeRoleWithWebIdentity：裁剪延后**（成本评估：JWKS 拉取需出站 TLS 客户端新依赖栈；
  验收 ①–⑤ 不涉及；记档于 M5 计划，接入点已留——`StsRoute` 动作分发 + `check_access` 匿名覆写）。

## 事件通知（M6）

桶通知配置走 S3 协议（`PutBucketNotificationConfiguration` / `GetBucketNotificationConfiguration`）；
事件写入本地 outbox（`event_outbox`，至少一次）后由 worker 投递。目标只支持 Kafka：

```toml
# oss.toml：通知目标静态配置（id 与桶配置 ARN 对应）
[[kafka.targets]]
id = "t1"
brokers = ["127.0.0.1:9092"]
topic = "oss-events"
```

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-notification-configuration \
  --bucket bkt --notification-configuration '{
    "QueueConfigurations":[{
      "Id":"all","QueueArn":"arn:oss:sqs::t1:kafka",
      "Events":["s3:ObjectCreated:Put","s3:ObjectRemoved:Delete"],
      "Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"logs/"}]}}
    }]}'
```

- 事件名白名单：`s3:ObjectCreated:{Put,Post,Copy,CompleteMultipartUpload}` ·
  `s3:ObjectRemoved:{Delete,DeleteMarkerCreated}` · `s3:LifecycleExpiration:{Delete,DeleteMarkerCreated}`。
  事件 JSON 为 AWS 通知形状（`eventVersion 2.1`、`eventSource oss:s3`、urlencoded key、sequencer）。
- 投递语义：**至少一次**（稳态不重；崩溃窗可能重投）。永久错（鉴权/协议）>10 次入死信；
  连接类错误永不放弃（退避封顶 300s）。Topic 需**预创建**（部署前置；UTOP 不判死）。
- 无 Kafka 时的调试旁路：`OSS_EVENT_SINK=file:/<dir>`（落文件，测试用）；worker 间隔
  `OSS_EVENT_INTERVAL_SECS`（默认 1；0=关）。
- 边界：SASL/SSL 未接线（内网假设）；`s3:ObjectAccessed:*` 未实现（配置返回 400）。

## S3 Select（M6）

`SelectObjectContent` 支持 CSV / JSON（LINES/DOCUMENT）× 输出 CSV / JSON，压缩 NONE/GZIP：

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api select-object-content \
  --bucket bkt --key data.csv \
  --expression "SELECT s._1, s._2 FROM S3Object WHERE s._2 > 15 LIMIT 10" --expression-type SQL \
  --input-serialization '{"CSV":{},"CompressionType":"NONE"}' \
  --output-serialization '{"CSV":{}}' /tmp/out.csv
```

- SQL 子集（白名单外一律 `InvalidRequest`，不静默错结果）：`SELECT 列|* FROM S3Object [s]
  [WHERE …] [LIMIT n]`；比较、AND/OR/NOT（SQL 三值逻辑）、BETWEEN/IN/LIKE/IS NULL、CAST；
  函数 `substring/trim/lower/upper/char_length/abs/ceil/floor/round`；聚合
  `count(*)/sum/min/max/avg`（不支持 GROUP BY；聚合须独占投影）。CSV 列 `s._1`；JSON 路径 `s.a.b`。
- 输入：CSV 分隔符/引号/转义/记录分隔符/注释（缺省 `#`）/表头可配；`AllowQuotedRecordDelimiter`
  缺省 false。ScanRange 按「记录首字节落在区间内则整条处理」；仅给 End = 扫描末尾 N 字节；
  GZIP 与 ScanRange 互斥；JSON DOCUMENT 不支持 ScanRange。
- 输出：Records ≤256KiB 分片 + Progress（约 1MiB 节奏，`--request-progress`）+ Stats + End；
  Stats 三计数独立（`BytesScanned` 原始/压缩后、`BytesProcessed` 送入求值器、`BytesReturned` 输出）。
- 上限/边界（记档）：对象 ≤256MiB、GZIP 解压 ≤512MiB（全量缓冲，无并发预算）；BZIP2 返回 400
  （后置）；Parquet 未支持；CRLF 保留 `\r`；空 CSV 字段进 sum/avg 报 400；LIKE 模式 ≤4096 字节。

## 桶复制（M6）

源桶须**版本控制 Enabled**；目标用 `[[remotes]]` 静态配置，规则 `Destination.Bucket` 写成
`<remote-id>/<桶名>`：

```toml
[[remotes]]
id = "r1"
endpoint = "http://10.0.0.2:9000"   # 仅 http（TLS 未接线，启动期拒绝 https）
access_key = "..."                   # 目标实例上铸造的 AK
secret_key = "..."
region = "us-east-1"
```

```bash
aws --endpoint-url http://127.0.0.1:9000 s3api put-bucket-replication --bucket srcbucket \
  --replication-configuration '{"Role":"arn:oss:iam::oss:role/replication","Rules":[{
    "ID":"logs","Status":"Enabled","Filter":{"Prefix":"logs/"},
    "Destination":{"Bucket":"r1/dstbucket"},
    "DeleteMarkerReplication":{"Status":"Enabled"}}]}'
```

- 覆盖全部写入口：PUT / MPU complete / CopyObject / 单删与批量删 marker / aether 摄取 /
  控制台删除 / 生命周期删除。对象另带 `x-amz-replication-status`：源 `PENDING`→`COMPLETED`
  （任一目的地永久失败超限 → `FAILED`）；目标为 `REPLICA`（防环，REPLICA 不再入队）。
- 出站为 SigV4 + http1 PUT（`x-amz-replication-status: REPLICA` 纳入签名）；worker 300s 租约 +
  R1 分级重试（连接/408/429/5xx 永不放弃、其余 4xx 永久、>10 次死信）；积压写入 `repl_outbox`，
  目标恢复后自动追平。间隔 `OSS_REPL_INTERVAL_SECS`（默认 1；0=关）。
- 边界（记档）：目标桶/主题需**预创建**；SSE-C 源对象不可复制（客户钥不落盘）；版本定向删除
  与 `Priority` 不生效（不同目的地都会复制）；MPU 源 ETag 目标重算（不做 ETag 断言）；
  单对象整读入内存（≤16 并发）；`Role` 不参与鉴权。

## 多站点复制（M7）

两个**单节点**实例建立站点对，双向异步复制控制面与版本化对象。`[site]` 是唯一开关（与
`[cluster]` 互斥；未配置时站点 API 一律 404）：

```toml
[site]
name = "site-a"
region = "ap-east-1"     # 配对时对端校验一致
priority = 10            # 同秒 LWW 的裁决优先级
token = "<32–128 位十六进制>"   # 控制面通道共享密钥；缺省首启随机生成
```

```bash
# 管理员面：本站身份（含 token）→ 在 A 上配对 B（B 端也需配对 A 才是双向）
curl -s $A/api/v1/site -H "authorization: Bearer $JWT"
curl -s -X POST $A/api/v1/sites -H "authorization: Bearer $JWT" -H 'content-type: application/json' -d '{
  "peer_id":"b","endpoint":"http://10.0.0.2:9000","region":"eu-west-1","priority":10,
  "s3_access_key":"<B 上铸造的 AK>","s3_secret_key":"<SK>",
  "peer_site_token":"<B 的 site token>"}'
curl -s $A/api/v1/sites/b/status -H "authorization: Bearer $JWT"   # lag/dead/outbox
```

- **控制面**：`buckets`（列级 versioning/owner/lock）、`bucket_policies`、`permissions`、
  `users`、长时 `access_keys` 的变更由 SQLite 触发器写入 `site_changelog`；worker 逐 peer
  按 cursor 出货（≤100 条/批，`OSS_SITE_INTERVAL_SECS` 默认 1s）。冲突用 LWW
  `(ts, priority, origin, seq)`：ts 新者胜，同秒按 priority，再按 origin 字典序；本地写若旧于
  已知时钟会被 BEFORE 门控拒绝（409/500 直到追上）。**管理员行不复制**、STS 会话键不复制。
- **配对快照**：`POST /sites` 在单事务内写 `sites` + cursor + 五类实体快照，快照同事务入本地
  时钟；只快照「本地时钟无行或 origin=本站」的实体，避免同时配对互相回显。
- **对象**：复用 `repl_outbox`（`target_kind='site'`），每个 peer 一行；仅版本化数据写与
  marker 入队，删除要求源桶 versioning=Enabled。worker 发送前签名 `GET /{bucket}?versioning`
  （10s 缓存；404/非 Enabled → 可重试），PUT 透传 content-type 与 `x-amz-meta-*`、要求目标返回
  真实 version-id（`null` → 重试）。同版本多目标聚合 PENDING→COMPLETED/FAILED。
- **边界（记档）**：仅 http（TLS 未接线）；单节点站点对；版本 UUID 两端不一致、版本定向删除
  不复制；非空桶删除不生效（保留行与数据，记死信）；锁/标签/SSE 不复制，SSE-C 源永久错；
  AK 密文需两端同主密钥（配对只校验 vault 开关一致，不校验同一把钥）；控制面最终一致（无强一致，
  归 M9）；端点/凭据明文存储；`priority` 仅在**同秒**裁决（新 ts 的低优先级仍胜旧 ts 的高优先级——
  与早期规格「按优先级裁决」有偏差）；解配期间的本地控制面改动不被捕获，且对端快照可能在重配对时
  覆盖这些改动（上次由对端胜出的实体）——需在重配对后重做；`x-amz-replication-status: REPLICA`
  由任意已认证写者提供，可被用于抑制复制（M6 既有信任面，M7 的 DELETE 防环依赖同一头）；桶删除时
  若策略/授权行的 LWW 时钟更新，会按 L7 保留，同名桶重建可能继承这些残留行（跨站点并发删/改窗口）。

## 验收



覆盖范围：S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）见下方脚本；控制台的登录/上传/秒传/密钥常规路径见
「控制台（M1b）」的 Playwright e2e；**1 GB 上传与断线续传为人工验证项**（指引同 M1b 段）。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
# M5 全量（步骤 18）：服务须以密钥库启动（OSS_MASTER_KEY_FILE/OSS_KMS_DIR），可选：
#   E2E_DATA_DIR=<data_dir>  盘上明文特征 grep（含明文对照键的判别性断言）
#   E2E_DB=<sqlite 路径>     SK 落库密文前缀（enc:v1:）断言
```

M6 全量（§19：Select 对当前 ENDPOINT + Kafka（docker redpanda）+ 复制双实例）：

```bash
cargo build -p oss-server && AK=... SK=... ENDPOINT=... tests/e2e_s3.sh
tests/e2e_replication.sh        # 仅复制：自动起源/目标两实例（约 1–2 分钟）
tests/e2e_kafka.sh              # 仅 Kafka 三阶段（redpanda docker；约 1 分钟）
tests/e2e_sites.sh              # M7 多站点：自动起 A/B 两实例并双向配对（约 2–4 分钟）
```

集群验收（M3；4 节点真进程 + 混沌段，覆盖「拔节点/分区/并发写/拉回补齐/控制面扇出」）：

```bash
cargo build -p oss-server && tests/e2e_m3_cluster.sh   # 端口全自动取空闲；约 8–15 分钟（含 §⑤ 超时预算）
# 含 §0c：桶行广播接线（建/删桶后各节点即时一致，免重启——M5 切片 0 遗留断言）
```

## 参考文档

- 设计文档：`docs/superpowers/specs/2026-10-10-minio-clone-design.md`
- 功能规格：`docs/superpowers/specs/2026-10-10-feature-spec.md`
- M1 实现计划：`docs/superpowers/plans/2026-10-10-m1a-backend-slice.md`（前端：`…-m1b-frontend-console.md`）
- M2a 实现计划：`docs/superpowers/plans/2026-10-12-m2a-usability-slice.md`
- M2b 实现计划（多盘 / EC / 断电一致性）：`docs/superpowers/plans/2026-10-12-m2b-ec-multidisk.md`
- 子账户与权限体系实施记录：`docs/superpowers/plans/2026-10-13-subaccounts-permissions.md`
- M3 分布式实现记录：`docs/superpowers/plans/2026-10-13-m3-distributed.md`
- M4 运维与 IAM 实现记录：`docs/superpowers/plans/2026-10-14-m4-ops-iam.md`
- M5 安全与锁定实施记录（进行中）：`docs/superpowers/plans/2026-10-16-m5-security-lock.md`

---

© 2026 erik.xyz · 以 Apache-2.0 许可发布

<!-- assets-license:start -->
## 视觉资产授权 / Visual Assets License

本项目的项目宠物、图片、图标、表情（包括 `docs/pet-expressions/` 等目录下的
SVG / PNG）**不是开源资产**，版权归作者所有。未经书面许可不得复制、修改、再分发或
用于商业用途；商业使用需取得商业授权，联系 **erik@erik.xyz**。详见
[ASSETS-LICENSE.md](ASSETS-LICENSE.md)。

> 代码部分仍遵循仓库原有 `LICENSE`；第三方素材（如明确标注的 Go Gopher 等）仍遵循其原许可证。
<!-- assets-license:end -->

<!-- pet-expressions:start -->
## 项目宠物表情形态 · 吞吞

12 种彩色动态形态：笑 / 欢乐 / 哭 / 沮丧 / 惊恐 / 愤怒 / 撒娇 / 卖萌 / 讨好 / 敲打 / 捂脸 / 吐舌头。每个 SVG 自带表情、情绪特效与整体动效，单张文件见 [`docs/pet-expressions/`](docs/pet-expressions/)。

| 形态 | 代码 | 形态 | 代码 | 形态 | 代码 |
| --- | --- | --- | --- | --- | --- |
| [笑](docs/pet-expressions/smile.svg) | `:emoji-smile:` | [欢乐](docs/pet-expressions/joy.svg) | `:emoji-joy:` | [哭](docs/pet-expressions/cry.svg) | `:emoji-cry:` |
| [沮丧](docs/pet-expressions/dejected.svg) | `:emoji-depressed:` | [惊恐](docs/pet-expressions/terrified.svg) | `:emoji-terrified:` | [愤怒](docs/pet-expressions/angry.svg) | `:emoji-angry:` |
| [撒娇](docs/pet-expressions/spoiled.svg) | `:emoji-coquettish:` | [卖萌](docs/pet-expressions/cute.svg) | `:emoji-cute:` | [讨好](docs/pet-expressions/plead.svg) | `:emoji-pleasing:` |
| [敲打](docs/pet-expressions/knock.svg) | `:emoji-knocking:` | [捂脸](docs/pet-expressions/facepalm.svg) | `:emoji-facepalm:` | [吐舌头](docs/pet-expressions/tongue.svg) | `:emoji-tongue:` |
<!-- pet-expressions:end -->
