![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

![吞吞的表情形态：哭、笑、沮丧、惊恐、得意](docs/assets/mascot-expressions.svg)

# oss（吞吞）—— 自研分布式对象存储系统

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研对象存储（MinIO 的等价实现）。目标形态是分布式 + 纠删码；
当前完成 **M1（单盘垂直切片）+ M2a（可用性切片：Multipart Upload、Range GET、
`x-amz-meta-*` 元数据、流式 PUT、孤儿回收）+ M2b（存储切片：单机多盘 + 纠删码 8+4、
quorum 写、拔盘容忍与读时修、`fsck` 巡检、kill -9 断电一致性用例）**——`aws-cli`/`rclone`
的大文件默认路径（≥8 MiB 自动 multipart）与断点习惯直接可用，浏览器能分块上传，控制台能看见。

- **三个 crate 单向依赖 `oss-server → xl-s3 → xl-store`**：`xl-store` 是不依赖任何框架的存储内核
  （提交协议与 `xl.meta` 格式按最终设计落盘）；`xl-s3` 把 s3s 的 S3 trait 实现到内核上；
  `oss-server` 用 e-cat 组装一个 axum Router，同时承载控制台 API、aetherupload 分块上传与 S3 服务。
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
未实现（见设计文档的阶段表）：分布式（多节点）· 版本控制 · 加密 · 复制 · 权限。

## 架构

![系统架构](docs/assets/architecture.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` v1 编解码、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`） |
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
>   （前端在登录成功时种 cookie，M1b）；缺/坏/过期一律 401。彻底的角色/策略模型仍随部署形态（M2/M4）。
> - ListObjects **v1 未实现（501）**；rclone 用 `provider=Other` 时须设 `LIST_VERSION=2`（验收脚本即这么做）。
> - 单次 PUT / 单个 part 仍受 `max_put_bytes`（默认 256 MiB）上限；multipart 大对象整体不受该值约束。
> - Multipart 边界：`UploadPartCopy`、`ListMultipartUploads`、GET 的 `partNumber` 参数未实现（M3+）；
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
>   - 读 meta 取**最新副本**（`mod_time` 墙钟）：多盘间该值正常单调；NTP 大步回拨叠加写盘故障属已知边界（M3 的 heal/epoch 收口）。
>   - 目录 fsync 的断电零丢失保证按 ext4/XFS 假设；其它文件系统上目录 fsync 失败仅 warn，需自行复核。
> - 长跑验收（rclone 并发 100×100 MB + 中断重试，设计验收 #3）在 `tests/e2e_s3.sh` 的
>   `E2E_LONG=1` 门控段（默认跳过，人工触发；`E2E_LONG_N`/`E2E_LONG_MB` 调规模）。

## 控制台（M1b）

```bash
# 1. 起后端（同 M1a）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173 ，用上面的账号登录
```

- 上传复用 aetherupload 自带脚本（分块 / 断线续传 / 秒传），前端不含分片逻辑
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
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传在 M2）

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

## 验收

覆盖范围：S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）见下方脚本；控制台的登录/上传/秒传/密钥常规路径见
「控制台（M1b）」的 Playwright e2e；**1 GB 上传与断线续传为人工验证项**（指引同 M1b 段）。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
```

## 参考文档

- 设计文档：`docs/superpowers/specs/2026-10-10-minio-clone-design.md`
- 功能规格：`docs/superpowers/specs/2026-10-10-feature-spec.md`
- M1 实现计划：`docs/superpowers/plans/2026-10-10-m1a-backend-slice.md`
- M2a 实现计划：`docs/superpowers/plans/2026-10-12-m2a-usability-slice.md`
- M2b 实现计划（多盘 / EC / 断电一致性）：`docs/superpowers/plans/2026-10-12-m2b-ec-multidisk.md`

---

© 2026 erik.xyz · 以 Apache-2.0 许可发布
