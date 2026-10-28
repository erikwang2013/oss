![吞吞（OSS 项目宠物）：一只装着三块对象立方体的存储桶，戴着绿色状态灯](docs/assets/mascot.svg)

# oss（吞吞）—— 自研分布式对象存储系统

[English](README.en.md) | 简体中文

![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg) ![Rust](https://img.shields.io/badge/rust-2024%20edition-dea584.svg) ![Release](https://img.shields.io/github/v/release/erikwang2013/oss) ![S3](https://img.shields.io/badge/S3-compatible-blueviolet.svg)

**项目中文名：吞吞　英文名：oss　项目宠物：吞吞（TunTun）**——一只把对象块吞进桶里的小家伙。

S3 兼容的自研分布式对象存储（MinIO 的等价实现）。支持多盘纠删码 8+4、跨节点 gRPC mesh、版本控制、生命周期、桶策略与审计、SSE 三模式与对象锁、STS 临时凭据、事件通知/Kafka、S3 Select、桶复制与多站点复制、访问点/Object Lambda、Batch/Inventory/tier、sync 桶与 3-voter Raft 控制面。`aws-cli`/`rclone` 的默认大文件路径（≥8 MiB 自动 multipart）与断点习惯直接可用，浏览器控制台可分块上传并管理。

**最新发布：v1.20.1（2026-10-28）。** 补丁版：账户软字节配额（只读用量端点 + 独立配置路由 + level-3 Raft 命令，仅告警不阻写）、缩容 4→3 真实进程 loopback e2e（97/0），并修复 v1.20.0 源码归档中 `Cargo.lock` 的 `either` 被误标为 1.20.0 导致无法构建的问题。硬字节配额与对象数配额仍未交付；边界见[使用文档](https://oss.erik.xyz/usage.html#boundaries)。

## 功能

| 领域 | 能力 |
|---|---|
| 存储内核 | 多盘纠删码 8+4、跨节点 gRPC mesh、quorum 提交、`xl.meta` 版本链、小对象内联、多盘自愈 |
| S3 API | 对象/桶/版本/标签、Multipart、partNumber、Range、Copy 指令、分块上传与断线续传 |
| 生命周期 | 过期、非当前版本、转换规则，Storage Class、分层归档与 Restore |
| 安全 | SSE-S3/KMS/C、密钥库、对象锁与保留期、STS、桶策略与审计、TLS/mTLS、子账户三档权限、Argon2id、账户软字节配额告警 |
| 数据流 | 事件通知/Kafka、S3 Select、桶复制、多站点复制 |
| 控制面（M8） | 访问点、Object Lambda、Batch Operations、Inventory、作业引擎、控制台页面 |
| 强一致（M9） | sync 桶、stretch 2×6、3-voter Raft（witness）、能力级别 1..3 |
| 运维 | `fsck` / `heal` / `scrub` / `clean` / `rebalance` / `cluster-shrink plan/apply/verify/commit/rollback` |
| 客户端 | `aws-cli` / `rclone`；React 控制台（分块、断线续传、秒传） |

## 架构

![系统架构](docs/assets/architecture.svg)

M6 数据流（事件通知 / S3 Select / 桶复制）：

![M6 数据流：事件 / Select / 复制](docs/assets/m6-dataflow.svg)

M7 多站点异步复制（控制面 changelog + 对象 outbox）：

![M7 多站点复制](docs/assets/m7-sites.svg)

M8 控制面（访问点与 OLAP / 作业与 Batch / Inventory / 分层与 Restore / 控制台页面，S0–S6）：

![M8 控制面](docs/assets/m8-control-plane.svg)

| crate / 目录 | 职责 |
|---|---|
| `crates/xl-store` | 存储内核：盘布局与键校验（`__XLDIR__` 展平）、`xl.meta` 编解码（版本链 / 加密记录 / 锁记录 / 标签 / storage_class / tier）、提交协议、EC 编解码、多盘分发与 quorum、对象/桶锁与 flock、密钥库（`kms.rs`）与块对齐信封加密（`crypto.rs`） |
| `crates/xl-s3` | s3s 的 `S3` trait 实现：DTO 翻译、错误映射、SigV4 认证（AK → SK 查 SQLite `access_keys`）、SSE 与对象锁头解析/响应装配；M8 增访问点路由（`ap.rs`）、Object Lambda（`olap.rs`）、Restore/生命周期（`restore.rs`/`lifecycle.rs`）、TierGc/删桶清理契约 |
| `crates/xl-proto` | 节点间 gRPC 协议：`oss.proto` 消息与 service 定义（prost + tonic-build 全量生成） |
| `crates/xl-client` | mesh 的 tonic 实现：`TonicMesh` 客户端（懒连接 / 超时 / token 拦截器 + traceparent 注入）与服务端 handler（令牌校验，只操作本节点本地槽） |
| `crates/oss-server` | ecat + axum 组装：控制台 API（JWT）、aetherupload 接线、S3 fallback、运维 CLI；M8 增作业引擎（`jobs.rs`）、Batch（`job_actions*.rs`）、访问点/OLAP（`access_points_api.rs`）、Inventory（`inventory*.rs`）、分层/恢复（`tier*.rs`）与 0003 迁移 |
| `web` | 控制台前端（M1b）：React 19 + antd + Vite，Playwright e2e；M8 增访问点/作业/分层/清单/通知/审计/设置页面 |

磁盘布局速记（M2b 多盘）：每个盘各持一份**完整命名空间**——`<盘>/<桶>/<展平键>/` 下是
`xl.meta`（该盘副本）与版本目录 `<uuid>/part.1.<shard>`（落到本盘的那些分片；单盘形态即全部分片）。
**提交永远是「数据先到位 → meta 翻转」**：单盘 = 同盘 rename；多盘 = 各盘 meta 两阶段提交
（全盘 `xl.meta.new` 准备 ≥ quorum 才逐盘转正；读取**最新副本**胜）。小对象（< 128 KiB）内联进
`xl.meta`、不落数据文件。启用加密（M5）后数据文件存的是**密文分片**（先加密后 EC），
包裹的 DEK 与 `key_id` 记在 `xl.meta` 内（元数据本身不含密钥明文）。临时区在每盘
`.oss.sys/{tmp, trash, locks}`，盘身份标识 `drive-id` 防插错盘。
控制面元数据（用户/密钥/桶/上传记录/秒传索引）在 SQLite。

**技术栈**：Rust 2024 · ecat 7.0 · s3s 0.17 · aetherupload-rust 3.0.0 · axum 0.8 · sqlx / SQLite · serde + rmp-serde。

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

> `oss.toml` 默认 `listen = "0.0.0.0:9000"` 与占位 `jwt_secret` 仅限本机开发；对外部署前必须改成
> 回环/受控绑定与随机密钥。

### Docker

单机（oss-server + 控制台 nginx）：

```bash
docker compose up --build        # 控制台 http://localhost:8080 · S3/API http://localhost:9000
```

M3 集群（4 节点 × 3 盘 = 12 槽；服务名即节点地址）：

```bash
docker compose -f docker-compose.cluster.yml up --build
# S3/控制面入口：n1:9000 · n2:9001 · n3:9002 · n4:9003（任意节点等价）；控制台 :8080 反代 n1
```

- 配置：单机 `docker/oss.toml`；集群 `docker/cluster/n{1..4}/oss.toml`（静态拓扑，注释在文件里）。
- `jwt_secret` / 集群 `secret` / 管理员口令未填时首次启动自动生成，落盘
  `./docker/secrets/generated.env`；轮换 = 删除该文件重启。
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
