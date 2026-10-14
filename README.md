![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

![吞吞的表情形态：哭、笑、沮丧、惊恐、得意](docs/assets/mascot-expressions.svg)

# oss（吞吞）—— 自研分布式对象存储系统

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研对象存储（MinIO 的等价实现）。目标形态是分布式 + 纠删码；
当前完成 **M1（单盘垂直切片）+ M2a（可用性切片：Multipart Upload、Range GET、
`x-amz-meta-*` 元数据、流式 PUT、孤儿回收）+ M2b（存储切片：单机多盘 + 纠删码 8+4、
quorum 写、拔盘容忍与读时修、`fsck` 巡检、kill -9 断电一致性用例）+ M3（分布式切片：
节点间 gRPC mesh、静态槽位拓扑、跨节点 quorum 读写、节点故障容忍与读修复、quorum 租约锁、
混沌 e2e）**——`aws-cli`/`rclone`
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
未实现（见设计文档的阶段表）：版本控制 · 加密 · 复制 · 对象锁/STS 等
完整 IAM（细粒度策略、预设模板留 M4+）。

## 架构

![系统架构](docs/assets/architecture.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` v1 编解码、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`） |
| `crates/xl-proto` | 节点间 gRPC 协议：`oss.proto` 消息与 service 定义（prost + tonic-build 全量生成） |
| `crates/xl-client` | mesh 的 tonic 实现：`TonicMesh` 客户端（懒连接 / 超时 / token 拦截器 + traceparent 注入）与服务端 handler（令牌校验，只操作本节点本地槽） |
| `crates/oss-server` | ecat + axum 组装：控制台 API（JWT）、aetherupload 接线、S3 fallback、运维 CLI |
| `web` | 控制台前端（M1b）：React 19 + antd + Vite，Playwright e2e |
| `tests/e2e_s3.sh` | aws-cli / rclone 端到端验收脚本（17 步 + `E2E_LONG=1` 门控长跑） |

磁盘布局速记（M2b 多盘）：每个盘各持一份**完整命名空间**——`<盘>/<桶>/<展平键>/` 下是
`xl.meta`（该盘副本）与版本目录 `<uuid>/part.1.<shard>`（落到本盘的那些分片；单盘形态即全部分片）。
**提交永远是「数据先到位 → meta 翻转」**：单盘 = 同盘 rename；多盘 = 各盘 meta 两阶段提交
（全盘 `xl.meta.new` 准备 ≥ quorum 才逐盘转正；读取**最新副本**胜）。小对象（< 128 KiB）内联进
`xl.meta`、不落数据文件。临时区在每盘 `.oss.sys/{tmp, trash, locks}`，盘身份标识 `drive-id` 防插错盘。
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
>   - 读 meta 取**最新副本**（`mod_time` 墙钟）：多盘间该值正常单调；NTP 大步回拨叠加写盘故障属已知边界（M4 的 heal/epoch 收口）。
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

`oss-server [serve|groups|build|clean <days>|fsck [--repair]]`（无参数 = `serve`）。运维命令读当前目录的
`oss.toml`，并走与服务端相同的启动自举（建目录/连库/开存储；管理员与桶的引导是幂等的）：

```bash
oss-server groups        # 建分组目录（幂等；启动自举也会做，上传报笼统错误时先跑它确认）
oss-server build         # 按磁盘现状重建秒传索引（先清旧索引，再全量扫描暂存盘）
oss-server clean <days>  # 按 mtime 回收 days 天前的残骸：
                         #   暂存盘 `_header/` 与 `*.part`；数据盘 `.oss.sys/{tmp,trash}` 孤儿
                         #   与各对象目录下未被引用的版本目录（含未完成的上传暂存区）
oss-server fsck          # 全盘巡检：孤儿版本目录 / EC 分片缺坏（CRC32C）/ tmp-trash 计数；
oss-server fsck --repair #   同上 + 最小修复（删孤儿 + 读时修分片）。报告落
                         #   `.oss.sys/fsck/<时间戳>.json`（独立 scrub 深度巡检是 M4）
```

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
  `fsck --repair` 从对端差集检出并补建（「加回节点后补齐」）。
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

## 验收

覆盖范围：S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）见下方脚本；控制台的登录/上传/秒传/密钥常规路径见
「控制台（M1b）」的 Playwright e2e；**1 GB 上传与断线续传为人工验证项**（指引同 M1b 段）。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
```

集群验收（M3；4 节点真进程 + 混沌段，覆盖「拔节点/分区/并发写/拉回补齐/控制面扇出」）：

```bash
cargo build -p oss-server && tests/e2e_m3_cluster.sh   # 端口全自动取空闲；约 8–15 分钟（含 §⑤ 超时预算）
```

## 参考文档

- 设计文档：`docs/superpowers/specs/2026-10-10-minio-clone-design.md`
- 功能规格：`docs/superpowers/specs/2026-10-10-feature-spec.md`
- M1 实现计划：`docs/superpowers/plans/2026-10-10-m1a-backend-slice.md`
- M2a 实现计划：`docs/superpowers/plans/2026-10-12-m2a-usability-slice.md`
- M2b 实现计划（多盘 / EC / 断电一致性）：`docs/superpowers/plans/2026-10-12-m2b-ec-multidisk.md`
- 子账户与权限体系实施记录：`docs/superpowers/plans/2026-10-13-subaccounts-permissions.md`

---

© 2026 erik.xyz · 以 Apache-2.0 许可发布
