# OSS —— 自研分布式对象存储系统

S3 兼容的自研对象存储（MinIO 的等价实现）。设计文档见
`docs/superpowers/specs/2026-10-10-minio-clone-design.md`。

## 当前进度：M1（后端垂直切片）

已实现：单盘直存内核（无纠删码）· S3 桶/对象 CRUD · ListObjectsV2 · CopyObject ·
批量删除 · SigV4 认证 · aetherupload 分块上传与原生 ingest · 控制台 API。
未实现（见设计文档的阶段表）：纠删码 · 多盘 · 分布式 · 版本控制 · 加密 · 复制 · 权限。

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
> - 单段 PUT 上限 256 MiB（`max_put_bytes`）。
> - Multipart 与 Range 未实现（M2）：≥8 MiB 的 `aws-cli s3 cp` 默认走 multipart → 501；Range 被忽略（200 + 全量）。
>   变通：`aws configure set default.s3.multipart_threshold 268435456`。
> - `x-amz-meta-*` 用户元数据**静默丢弃**（M2 接上）。
> - `oss.toml` 默认 `listen = "0.0.0.0:9000"` 与占位 `jwt_secret` 仅限本机开发；对外部署前必须换成回环绑定与随机密钥
>   （占位密钥可被用来伪造控制台与 `/aetherupload/*` 的 token；与 0.0.0.0 绑定合用即 LAN 风险面）。

## 控制台（M1b）

```bash
# 1. 起后端（同 M1a）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173 ，用上面的账号登录
```

- 上传复用 aetherupload 自带脚本（分块 / 断线续传 / 秒传），前端不含分片逻辑
- 秒传提示「上传成功（秒传）」表示 0 分块传输
- e2e：`cd web && npx playwright install chromium && npm run e2e`。Playwright 自带双 webServer
  （`cargo run` + `vite`），后端按 `web/playwright.config.ts` 里的 `pw` 自启；**已有后端在跑时会被复用**
  （`reuseExistingServer`），此时运行中实例的管理员口令必须也是 `pw`，否则登录 401——口令不一致就先停掉
  自起的服务，让 Playwright 用 `pw` 起自己的。上面「跑起来」的 `changeme` 只是示例占位，与 e2e 无关
  （改不改都不影响 e2e）。
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传在 M2）

## 验收

覆盖范围：S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）；浏览器上传与控制台界面见
「控制台（M1b）」的 Playwright e2e。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
```
