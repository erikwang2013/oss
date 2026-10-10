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
> - M1 的 `/aetherupload/*` 四条路由**无认证层（匿名可写）**，勿暴露公网；认证覆盖层随部署形态（M2）落地。
> - ListObjects **v1 未实现（501）**；rclone 用 `provider=Other` 时须设 `LIST_VERSION=2`（验收脚本即这么做）。
> - 单段 PUT 上限 256 MiB（`max_put_bytes`）。
> - Multipart 与 Range 未实现（M2）：≥8 MiB 的 `aws-cli s3 cp` 默认走 multipart → 501；Range 被忽略（200 + 全量）。
>   变通：`aws configure set default.s3.multipart_threshold 268435456`。
> - `x-amz-meta-*` 用户元数据**静默丢弃**（M2 接上）。
> - `oss.toml` 默认 `listen = "0.0.0.0:9000"` 与占位 `jwt_secret` 仅限本机开发；对外部署前必须换成回环绑定与随机密钥
>   （与上面 `/aetherupload/*` 匿名可写合用即 LAN 风险面）。

## 验收

覆盖范围：仅 S3 面（aws-cli + rclone 的建桶/增删查列与内容比对）；浏览器上传与控制台界面在 M1b。
前置：本机装了 aws-cli 与 rclone。

```bash
AK=AKIA_TEST_ACCESS_KEY SK=SK_TEST_SECRET_KEY_0001 ENDPOINT=http://127.0.0.1:9000 tests/e2e_s3.sh
```
