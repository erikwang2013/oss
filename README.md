![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

# oss（吞吞）—— 自研分布式对象存储系统

[English](README.en.md) | 简体中文

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研分布式对象存储（MinIO 的等价实现）。支持多盘纠删码 8+4、跨节点 gRPC mesh、版本控制、生命周期、桶策略与审计、SSE 三模式与对象锁、STS 临时凭据、事件通知/Kafka、S3 Select、桶复制与多站点复制、访问点/Object Lambda、Batch/Inventory/tier、sync 桶与 3-voter Raft 控制面。`aws-cli`/`rclone` 的默认大文件路径（≥8 MiB 自动 multipart）与断点习惯直接可用，浏览器控制台可分块上传并管理。

**最新发布：v1.27.0（2026-10-31）。** 分布式存储与升级安全版：feature level 8 交付账户硬配额配置与动态 VPC 端点注册表 Raft 命令；跨版本 MPU 支持 wire-1 有界回退（v1.16+），并补齐裸 UUID 收尾与幂等 Complete；Level C 交付 Ext 类型能力协商与在途 MPU 的 CMK 引用；AP 新增 attested-proxy 端点身份；Raft 快照 >256 KiB 分块、日志 entry 压缩与分段；新增停写蓝绿 preflight、v1.16 数据面下限/MPU 排空检查与单机→集群工具化。能力边界见[使用文档](https://oss.erik.xyz/usage.html#boundaries)。

## 功能

| 领域 | 能力 |
|---|---|
| 存储内核 | 多盘纠删码 8+4、跨节点 gRPC mesh、quorum 提交、`xl.meta` 版本链、小对象内联、多盘自愈、全对象 GET 流式直出、分片并行写、列举键缓存 |
| S3 API | 对象/桶/版本/标签、Multipart、partNumber、Range、Copy 指令、分块上传与断线续传 |
| 生命周期 | 过期、非当前版本、转换规则，Storage Class、分层归档与 Restore |
| 安全 | SSE-S3/KMS/C、密钥库、对象锁与保留期、STS、桶策略与审计（异步批量队列）、TLS/mTLS、子账户三档权限、Argon2id、账户软/硬字节与对象数配额 |
| 数据流 | 事件通知/Kafka、S3 Select、桶复制（Prefix/Tag/And 过滤）、多站点复制 |
| 控制面 | 访问点、Object Lambda、Batch Operations、Inventory、作业引擎与终态保留（JobPurge）、恢复后续状态/调度租约复制（feature 7）、硬配额/VPC 端点命令（feature 8）、控制台页面 |
| 强一致 | sync 桶、stretch 2×6、3-voter Raft（witness）、能力级别 1..8 |
| 运维 | `fsck` / `heal` / `scrub` / `clean` / `rebalance` / `cluster-shrink plan/apply/verify/commit/rollback` |
| 客户端 | `aws-cli` / `rclone`；React 控制台（分块、断线续传、秒传） |

## 架构

![系统架构](docs/assets/architecture.svg)

数据流（事件通知 / S3 Select / 桶复制）：

![数据流：事件 / Select / 复制](docs/assets/m6-dataflow.svg)

多站点异步复制（控制面 changelog + 对象 outbox）：

![多站点复制](docs/assets/m7-sites.svg)

控制面（访问点与 OLAP / 作业与 Batch / Inventory / 分层与 Restore / 控制台页面）：

![控制面](docs/assets/m8-control-plane.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` 编解码（版本链 / 加密记录 / 锁记录 / 标签 / storage_class / tier）、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock、密钥库（`kms.rs`）与块对齐信封加密（`crypto.rs`） |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`）、SSE 与对象锁头解析/响应装配；含访问点路由（`ap.rs`）、Object Lambda（`olap.rs`）、Restore/生命周期（`restore.rs`/`lifecycle.rs`）、TierGc/删桶清理契约 |
| `crates/xl-proto` | 节点间 gRPC 协议：`oss.proto` 消息与 service 定义（prost + tonic-build 全量生成） |
| `crates/xl-client` | mesh 的 tonic 实现：`TonicMesh` 客户端（懒连接 / 超时 / token 拦截器 + traceparent 注入）与服务端 handler（令牌校验，只操作本节点本地槽） |
| `crates/oss-server` | ecat + axum 组装：控制台 API（JWT）、aetherupload 接线、S3 fallback、运维 CLI；含作业引擎（`jobs.rs`）、Batch（`job_actions*.rs`）、访问点/OLAP（`access_points_api.rs`）、Inventory（`inventory*.rs`）、分层/恢复（`tier*.rs`）与 0003 迁移 |
| `web` | 控制台前端：React 19 + antd + Vite，Playwright e2e；含访问点/作业/分层/清单/通知/审计/设置页面 |

磁盘布局速记（多盘）：每个盘各持一份**完整命名空间**——`<盘>/<桶>/<展平键>/` 下是
`xl.meta`（该盘副本）与版本目录 `<uuid>/part.1.<shard>`（落到本盘的那些分片；单盘形态即全部分片）。
**提交永远是「数据先到位 → meta 翻转」**：单盘 = 同盘 rename；多盘 = 各盘 meta 两阶段提交
（全盘 `xl.meta.new` 准备 ≥ quorum 才逐盘转正；读取**最新副本**胜）。小对象（< 128 KiB）内联进
`xl.meta`、不落数据文件。启用加密后数据文件存的是**密文分片**（先加密后 EC），
包裹的 DEK 与 `key_id` 记在 `xl.meta` 内（元数据本身不含密钥明文）。临时区在每盘
`.oss.sys/{tmp, trash, locks}`，盘身份标识 `drive-id` 防插错盘。
控制面元数据（用户/密钥/桶/上传记录/秒传索引）在 SQLite。

**技术栈**：Rust 2024 · ecat 7.0 · s3s 0.17 · aetherupload-rust 3.0.0 · axum 0.8 · sqlx / SQLite · serde + rmp-serde。

## 请求周期

![请求周期](docs/assets/request-flow.svg)

两条入口（S3 直传、浏览器分块上传）在 xl-store 的**提交协议**处汇合：
暂存 → 同盘 rename 数据就位 → `xl.meta` 覆盖即提交；提交之前的一切对客户端不可见。
EC 写/读均已分块流式：内存峰值 ≈ 块缓冲（MiB 级），与对象大小无关（96 MiB 对象
区间读实测 VmHWM 107 MiB）。无 Range 的全对象 GET 直接流式输出（加密对象按 unit 解密，
至多缓存一个 unit）；远端 EC 分片按 `OSS_SHARD_READ_BATCH_BYTES`（默认 1 MiB，上限 16 MiB）
批量范围读取，减少 RPC 次数。Range/partNumber 仍有界缓冲，上限由 `OSS_MAX_BUFFERED_GET_BYTES`
控制（默认 512 MiB，0=不限），EC 数据盘缺失时的整体重建回退同样受该上限约束。
单次列举最多收集 `OSS_LIST_MAX_KEYS` 个键（默认 100 万，0=不限），大桶请用 prefix 翻页；
首次列举后每节点按桶缓存键集（`OSS_KEY_CACHE_MAX_KEYS`/`OSS_KEY_CACHE_MAX_TOTAL_KEYS`
默认各 200 万，`OSS_KEY_CACHE_TTL_SECS` 默认 300 秒，0 关闭周期重建；超限回退逐页扫描）。
审计日志默认保留 90 天（`OSS_AUDIT_RETENTION_DAYS=0` 关闭）；过期秒传索引每小时清理。
审计写入经有界异步队列（单 worker 每 100 ms 或满 256 行批量落库）；队列满时丢弃该行并累计
丢弃计数（首次及每 1000 条告警一次）——审计是 best-effort，不阻塞也不改变请求结果。
`jobs`/`job_tasks` 运行态随 Raft 快照进入 `m8.runstate`（硬上限 64 MiB）；终态作业默认
保留 30 天（`OSS_JOB_RETENTION_DAYS` 修改，0 关闭，上限 3650），每小时由 leader 经
feature level 6 的 `JobPurge` 命令分批清理，`queued`/`running`/`preempted` 永不删除。
启用 feature 6 前需全集群升级：旧节点 advertise ≤5，effective level 停在 5，`JobPurge`
在 append 前被拒（单向门，激活后不可回滚）。feature level 7 的恢复后续状态/调度租约命令
同样需全集群升级；旧节点 advertise ≤6，effective 停在 6，相关命令在 append 前被拒。
账户硬配额配置 API 在 standalone 与 Raft（feature level 8）均可用；Raft 模式需全集群先升级到 level 8，旧节点在 append 前被拒。配额执行在限额写入后生效。

## 对象生命周期

![对象生命周期](docs/assets/object-lifecycle.svg)

读路径只信 `xl.meta`：无有效 meta 的目录永不被服务；崩溃只会留下不可见的残骸
（孤儿版本目录 / trash 残留 / 未完成的 multipart 暂存区）。服务启动时自检清 `tmp`/`trash`，
`oss-server clean <days>` 按 mtime 回收其余残骸。

桶级生命周期规则负责过期删除、非当前版本清理、Storage Class 转换与分层 Restore；归档对象需
Restore 后读取。

## 安装

### 源码运行

```bash
# 1. 起服务（首次启动会自动建数据目录、暂存目录、桶目录与管理员）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server

# 2. 登录拿 token
TOKEN=$(curl -s -X POST localhost:9000/api/v1/login -H 'content-type: application/json' \
  -d '{"username":"erik","password":"changeme"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

# 3. 建一对访问密钥给 S3 客户端用（重复执行提示「access key 已存在」，幂等）
curl -s -X POST localhost:9000/api/v1/access-keys -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"access_key":"AKIA_TEST_ACCESS_KEY","secret_key":"SK_TEST_SECRET_KEY_0001"}'
```

> `oss.toml` 默认 `listen = "127.0.0.1:9000"`（仅本机）与占位 `jwt_secret`，仅限本机开发；
> 若把 listen 改成非回环地址，占位密钥会**拒绝启动**——对外部署前必须换成随机 `jwt_secret`。

### Docker

单机（oss-server + 控制台 nginx）：

```bash
docker compose up --build        # 控制台 http://localhost:8080 · S3/API http://localhost:9000
```

集群（4 节点 × 3 盘 = 12 槽；服务名即节点地址）：

```bash
docker compose -f docker-compose.cluster.yml up --build
# S3/控制面入口：n1:9000 · n2:9001 · n3:9002 · n4:9003（任意节点等价）；控制台 :8080 反代 n1
```

- 配置：单机 `docker/oss.toml`；集群 `docker/cluster/n{1..4}/oss.toml`（静态拓扑，注释在文件里）。
- `jwt_secret` / 集群 `secret` / 管理员口令未填时首次启动自动生成，落盘
  `./docker/secrets/generated.env`（容器内权限 **0600**、属主 `oss`，目录 0700）；轮换 = 删除
  该文件重启。查看口令：`docker compose exec oss cat /secrets/generated.env`（集群用
  `docker compose -f docker-compose.cluster.yml exec n1 cat /secrets/generated.env`）；
  宿主机上仅 root 可读：`sudo cat docker/secrets/generated.env`。
- 端口默认只绑 `127.0.0.1`（9000 S3/API、8080 控制台）。需要对外暴露时，去掉 compose 端口
  映射里的 `127.0.0.1:` 前缀，并自行加 TLS/反向代理与访问控制；容器间仍按服务名
  `oss:9000` 互通，不受宿主绑定影响。
- 控制台镜像沿用 `nginx:alpine` 默认以 root 主进程监听 :80；如需非 root，需换非特权基础
  镜像/端口并同步 compose 与 healthcheck（本次未改）。
- 数据/暂存在具名卷（`docker compose down -v` 才清）；集群每节点独立卷。

### 控制台

```bash
# 1. 起后端（同「源码运行」）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173，用上面的账号登录
```

> 完整的 TLS、集群、强一致与生产部署说明见[安装文档](https://oss.erik.xyz/install.html)。

## 使用方法

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
E=http://127.0.0.1:9000

aws --endpoint-url $E s3 mb s3://photos
aws --endpoint-url $E s3 cp ./cover.jpg s3://photos/cover.jpg
aws --endpoint-url $E s3 ls s3://photos/
aws --endpoint-url $E s3 presign s3://photos/cover.jpg --expires-in 3600
aws --endpoint-url $E s3 rm s3://photos/cover.jpg
```

- 大文件自动走 multipart；Range、`x-amz-meta-*`、Storage Class 与对象标签按 S3 语义处理。
- 浏览器控制台覆盖桶、对象、用户、访问密钥、权限、访问点、作业、Inventory、分层与 Raft 集群状态。
- 版本控制、生命周期、加密、对象锁、STS、事件通知、复制、强一致与运维命令详见[使用文档](https://oss.erik.xyz/usage.html)。
- 完整 API 与协议边界见 [API 文档](https://oss.erik.xyz/api.html)。

## 升级与回滚

本版数据面升级按**停写蓝绿**执行，不做滚动升级：新二进制在原数据目录启动，由运维切换流量；旧二进制回退只在没有新格式写入时成立。

1. 升级前跑只读预检：`scripts/upgrade-preflight.sh --data-dir <盘...> --health-json <保存的 /health/cluster> --strict --require-no-mpu`。
2. 停客户端流量并停旧服务，确认没有写入者。
3. 冷备每块数据盘与控制库，保留到验收完成。
4. 用新二进制在原数据目录启动；检查 `/health/ready` 与 `/health/cluster`：`data_plane_compatible: true`，且所有 peer `data_wire_version >= 2`。
5. 切流；备份继续保留。
6. 回滚 = 停新二进制、还原冷备、起旧二进制；仅当未发生新格式/CMK 写入时有效，否则按下方 CMK 迁移处理或保持新二进制。

`scripts/blue-green-preflight.sh` 把同一流程的检查做成只读闸门：`--data-dir`、`--backup`、`--config`、`--health-json`、`--health-url`、`--new-binary`、`--min-version`、`--strict`、`--json`；它只检查、不修改数据、不停止服务。

**CMK 激活是不归点。** DARE 的 AAD 绑定 `key_id`，只改元数据无法让 pre-CMK 二进制读 CMK 数据；仓库不提供明文/不安全降级。`oss-server cmk rewrap` 只在**同一个 CMK** 内把旧版本重加密到该 CMK 当前版本，不能把目标换成 legacy `file`/`k<n>` key。若已有 CMK 对象或桶默认加密、而必须回到 pre-CMK 二进制：先停写，逐节点跑 `oss-server cmk scan --name <cmk> --json`（`references`、`mpu_references` 与 `bucket_defaults`，含在途 MPU）并 Complete/Abort 所有 MPU、清除桶默认加密，使引用归零；然后要么用 legacy provider 重写对象数据，或迁移到新集群（如 `scripts/single-to-cluster.sh` 的离线重新灌入，只迁当前版本），再 `oss-server cmk delete --name <cmk> --yes`（集群需在全站逐节点核实后加 `--force`）；要么保持新二进制。**绝不能删除 CMK 来「强制」降级**；迁移完成前，回滚只能退回仍能读 CMK 的新二进制。

**跨版本 MPU。** 跨节点 UploadPart 转发支持 v1.16+（wire 1 有界回退）与本版 wire 2 流式；≤v1.15 没有 MPU RPC，无法支持。升级/降级前必须 Complete/Abort/排空所有在途 MPU，并用 `upgrade-preflight.sh --require-no-mpu --health-json` 强制门槛（`data_wire_version >= 2`、`data_plane_compatible: true`）。

**Raft 日志格式（wave-4）。** 新版本把 ≥1024 B 的日志 entry 压缩为 `z1:`，并把连续 ≥256 条压成 `raft_log_segment`；旧二进制无法读取新格式（fail-closed）。因此新二进制一旦写入新日志格式，就不能回退旧二进制——升级仍按停写蓝绿执行，回滚只在新格式/CMK 写入前成立。

**访问点端点身份。** `OSS_AP_ENDPOINT_VERIFIER=trusted-proxy` 按可信直连网段 + 声明头断言；`attested-proxy` 进一步要求可信网段内的终止代理用 HS256 JWT 签署 `vpce`/`vpc`/`account`（`x-oss-endpoint-attestation`，默认 60 秒有效），需要 `OSS_AP_ATTESTATION_SECRET`（≥32 字节）与 `OSS_AP_TRUSTED_PEER_CIDRS`。直连客户端无法伪造；验证后仍执行端点注册表 registered/state/VPC/account 检查。真实 AWS PrivateLink API 与 mTLS 证书→端点映射仍未实现（需要证书/端点注册表），不伪造身份。

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

![吞吞的 12 种彩色动态表情：笑、欢乐、哭、沮丧、惊恐、愤怒、撒娇、卖萌、讨好、敲打、捂脸、吐舌头](docs/assets/mascot-expressions.svg)

| 形态 | 代码 | 形态 | 代码 | 形态 | 代码 |
| --- | --- | --- | --- | --- | --- |
| [笑](docs/pet-expressions/smile.svg) | `:emoji-smile:` | [欢乐](docs/pet-expressions/joy.svg) | `:emoji-joy:` | [哭](docs/pet-expressions/cry.svg) | `:emoji-cry:` |
| [沮丧](docs/pet-expressions/dejected.svg) | `:emoji-depressed:` | [惊恐](docs/pet-expressions/terrified.svg) | `:emoji-terrified:` | [愤怒](docs/pet-expressions/angry.svg) | `:emoji-angry:` |
| [撒娇](docs/pet-expressions/spoiled.svg) | `:emoji-coquettish:` | [卖萌](docs/pet-expressions/cute.svg) | `:emoji-cute:` | [讨好](docs/pet-expressions/plead.svg) | `:emoji-pleasing:` |
| [敲打](docs/pet-expressions/knock.svg) | `:emoji-knocking:` | [捂脸](docs/pet-expressions/facepalm.svg) | `:emoji-facepalm:` | [吐舌头](docs/pet-expressions/tongue.svg) | `:emoji-tongue:` |
<!-- pet-expressions:end -->
