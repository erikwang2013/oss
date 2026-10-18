# M7 多站点异步复制 实施计划（工作令）

> 设计文档 §7.2（事件总线复用）/ §7.3（多站点一致性）/ §M7；功能规格 §10.2 / §19。
> **验收**（设计文档 §M7）：
> ① 两站点互建关系后，任一站点写入在秒级出现在对端（含版本）
> ② 任一站点的桶/策略变更在对端可见
> ③ 断开 WAN 一段时间后恢复：积压全部重放，双向数据一致（逐 ETag 对拍）
> ④ 两端同时写同一 key 不产生损坏（版本不可变，追加式传播）

## 钉子（防蔓延）

- **不做**（归 M9/M8）：sync 桶 / stretch EC / 控制面 openraft 强一致 / 访问点复制 / 自动配对（自动交换凭据）/ 站点级过滤（站点关系 = 全桶双向）/ 带宽限速 / TLS（跨站点一律 http，配置信任同 M6 `[[remotes]]`）/ 控制台「站点页」（后端管理 API 完整，UI 延续 M4/M5/M6 记档口径）/ **多站点（>2）扇出**——M7 只支持**站点对**（每站点至多 1 个 peer；多站点归 M9 前扩展）。
- **与 M6 的关系**：对象复制**复用 `repl_outbox`**（新增 `target_kind='site'`），出站沿用 SigV4 + hyper + `x-amz-replication-status: REPLICA`（防环）；控制面复制走**独立 changelog**（不入事件总线/Kafka）。
- **版本语义**：版本不可变 → 追加式传播；**版本 UUID 站点本地生成，不保证两端一致**；以内容/ETag/版本数语义为准（验收③逐 ETag 对拍）。非版本桶不参与对象站点复制（建桶与版本控制变更本身会复制）。
- **删除语义**：对象删除以 **delete marker** 传播（S3 语义）；控制台/生命周期整键硬删在版本化目标上同样表现为 marker（与 M6 一致）；桶删除只复制控制面行（LWW 裁决），**不清理对端对象数据**（记档）。
- **冲突裁决**：控制面条目按 `(priority, ts, origin_site_id, seq)` 全序 LWW——**priority 数值越大越权威**（站点关系建立时配置），同级由时间戳、来源 id、序号定序；apply 端持久化 `site_entity_clock` 以幂等去旧。对象层天然无冲突（版本追加）。
- **凭据模型**：peer 行存两套凭据——`s3_access_key/secret_key`（在对端铸造的 AK，用于对象复制 SigV4）+ `site_token`（成对共享的随机串，用于控制面 changelog 通道 `X-OSS-Site-Token`；仅 http 明文传输，内网信任模型，记档）。**不做自动配对**：两端各用管理 API 添加对方。
- **密钥互操作**：`access_keys.secret_key` 按**原样密文**复制 → 跨站点 IAM 生效要求两端同一主密钥配置（同一 `OSS_MASTER_KEY_FILE`/KMS）或均未启用 vault（e2e 用后者）；记档/README 写明。
- **changelog 捕获**：SQLite 触发器捕获 `buckets` / `bucket_policies` / `permissions` / `users`（基表列）；`access_keys` 因 `kind/parent_ak/session_policy` 为运行时 `ensure_columns` 补列（迁移期触发器不可引用），改为**应用层记账**（长时键创建/删除点；STS 会话键**不复制**——站点本地临时凭据）。对象锁桶默认规则（M5 补列）不在触发器捕获面，记档。
- 发布/推送：只在用户明确口令时执行（本地提交为止）。

## 切片总表（串行；每片 = 代码 → 测试 → 七闸 → verifier 核查 → 定稿 → 本地提交）

| # | 切片 | 交付 | 验收锚 |
|---|------|------|--------|
| 0 | 站点身份 + 关系 API | `site_config`/`sites` 迁移与 bootstrap；`GET /api/v1/site`；`GET/POST/DELETE /api/v1/sites`；`POST /api/v1/sites/{id}/ping`；校验矩阵 | 切片自验收 |
| 1 | 控制面 changelog | `site_changelog`/`site_entity_clock`/`site_cursors`/`apply_ctx` + 触发器 + access_keys 应用记账；`POST /api/v1/site/changelog`（token、批量、幂等、LWW）；shipping worker + 游标；`GET /api/v1/sites/{id}/status` | 验收② |
| 2 | 对象站点复制 | `repl_outbox.target_kind` 迁移；SqlReplSink 对每个 peer 入队（marker 与版本化写）；worker 按 peer 动态出站；bucket 未就绪 404 可重试；REPLICA 防环；状态聚合 | 验收①④ |
| 3 | e2e + 文档 + 图 + 定稿 | `tests/e2e_sites.sh`（双实例：对象双向/桶与策略/IAM 传播/停机重放/并发写/marker/ETag 对拍）；README「多站点复制（M7）」；设计/规格落地状态；`docs/assets/m7-sites.svg` + 架构图 M1–M7；计划定稿 | 验收①–④ |

## 切片 0：站点身份 + 关系管理 API

- 迁移 `0002_m7_site.sql`（新文件——0001 已随 v1.10.0 发布，不得再改）：
  `site_config(id PK, name, region, priority, site_token, created_at)`（单行）；
  `sites(peer_id PK, name, endpoint, priority, s3_access_key, s3_secret_key, site_token, created_at, last_seen, last_error)`。
- 配置新增 `[site]`（可选）：`name` / `region`（默认 us-east-1）/ `priority`（默认 0）；bootstrap 首启生成
  uuid v4 + 32 hex site_token 落 `site_config`（幂等；重启不变）。
- 管理 API（控制台 JWT；**仅管理员**）：
  - `GET /api/v1/site` → 本站点身份（id/name/region/priority）
  - `GET /api/v1/sites` → peer 列表（**不返回** secret/token，返回 has_credentials）
  - `POST /api/v1/sites` {peer_id,name,endpoint,priority,s3_access_key,s3_secret_key,site_token}
    ——校验：peer_id 白名单 `[A-Za-z0-9._-]{1,64}`、≠ 本机 id、endpoint 仅 http+host、AK/SK/token 非空且 token ≥16、
    upsert；**至多 1 个 peer**（M7 两站点，超出 400）。
  - `DELETE /api/v1/sites/{peer_id}`
  - `POST /api/v1/sites/{peer_id}/ping` → 以 `X-OSS-Site-Token` 调对端 `GET /api/v1/site`，更新 last_seen/last_error。
- 测试：bootstrap 幂等/配置校验；API 矩阵（非管理员 403、校验 400、peer 上限、列表脱敏）。

## 切片 1：控制面变更日志（触发器 + 应用 + 出货）

- 迁移补充：`apply_ctx(id=1, active)`；`site_changelog(seq PK AUTOINCREMENT, origin_site_id, entity,
  entity_id, op, payload, priority, ts, created_at)`；`site_entity_clock(entity, entity_id, priority, ts,
  origin_site_id, seq, PK(entity,entity_id))`；`site_cursors(peer_id PK, last_seq, updated_at)`。
- 触发器（AFTER INSERT/UPDATE/DELETE，`WHEN NOT EXISTS (SELECT 1 FROM apply_ctx WHERE id=1 AND active=1)`）：
  - `buckets`：upsert 载荷 name/owner_id/creation_date/versioning/object_lock/set_id；delete 载荷 name。
  - `bucket_policies`：upsert 载荷 bucket/policy_json/updated_at；delete。
  - `permissions`：entity_id=`<username>:<bucket>`（触发器内 `(SELECT username FROM users WHERE id=?)`）；
    载荷 username/bucket/can_*；delete。
  - `users`：entity_id=username；载荷 username/password_hash/is_admin/status/created_at；delete。
  - `access_keys`：**应用层**记账（console 建键/删键点；载荷 access_key/secret_key(原样)/username/status/
    expires_at/created_at；**不复制** `kind != 'long-term'`）。
- 应用端 `POST /api/v1/site/changelog`（**无 JWT**，`X-OSS-Site-Token` 与任一已配置 peer 的 token 恒定时间比对；
  失败 401）：body `{entries:[...]}`（≤500）；单事务且 `apply_ctx.active=1` 抑制触发器回环：
  - 对每条：与 `site_entity_clock` 比较 LWW 键 `(priority, ts, origin_site_id, seq)`，旧/等 → 跳过（幂等）；
    新 → 应用各实体（用户按 username、权限按 username+bucket、AK 按 access_key 定位本地行；用户 upsert
    保本地 id）；更新 clock。
  - 事务提交后：对新增桶调用 `store.create_bucket`（幂等；失败记 last_error 并回 500 让对端重试）。
  - 桶删除只删 buckets 行（对象数据保留，记档）；权限/策略/用户/AK 删除直接删行。
- 出货 worker（`OSS_SITE_INTERVAL_SECS` 默认 1；0=关）：逐 peer 读 `site_changelog` 中 seq > cursor（≤100），
  POST 到 `<peer.endpoint>/api/v1/site/changelog`（token 头）；200 → 推进 cursor；失败 → sites.last_error，
  下 tick 重试（控制面小、无死信；退避以轮询间隔为准）。
- 测试：触发器载荷形状矩阵（四表 + 抑制环）；LWW 矩阵（priority/ts/origin 定序、删除 vs 更新）；应用幂等
  （重复批次零副作用）；token 校验（错 token/未知 peer 401）；cursor 推进与重放；用户/权限跨站点登录可用。

## 切片 2：对象站点复制（复用 repl_outbox）

- 迁移：`ALTER TABLE repl_outbox ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'remote'`；索引补
  `(target_kind, remote_id, state, next_retry_at)`。
- 入队（`SqlReplSink::on_write`）：除本地桶规则外，对**每个 peer** 追加一行 `target_kind='site',
  remote_id=<peer_id>, dest_bucket=<源桶名>`：
  - 数据写：仅版本化且非 "null"、非 REPLICA（同 M6 门控）；
  - 删除写：marker 必入（硬删无版本行同样入队，`version_id=''`，不维护状态）。
- worker：`ReplRow.target_kind`；site 行按 `sites` 表动态取 endpoint/AK/SK 构造客户端（新增
  `ReplClient::single(endpoint,ak,sk,region)`）；数据 PUT 带 REPLICA 头、marker DELETE；**目标 404 →
  可重试**（等 changelog 建桶，不吃死信预算）；其余分级沿用 M6（408/429/5xx 可重试、4xx 永久 >10 dead）；
  状态聚合按 `(bucket,key,version_id)` 跨 remote/site 行（沿用 `settle_version_status`）。
- REPLICA 防环：peer 摄入时标记 REPLICA 且 sink 跳过（已实现）。
- 测试：入队矩阵（peer 存在/无 peer、marker、REPLICA、null 版本）；worker site 行（mock 对端：REPLICA 头/
  同 ETag/404 重试/连接断开积压→恢复）；双规则+site 混合状态聚合。

## 切片 3：e2e + 文档 + 图 + 定稿

- `tests/e2e_sites.sh`（两实例 A/B，均无 vault）：
  1. 双向添加 peer（互相铸造 AK + 共享 token）→ ping 绿；
  2. A 建桶 + 版本控制 Enabled → B 可见（changelog）；A 写对象 → B 同 ETag/内容；B 写 → A 同；
  3. A 设桶策略/建用户/建 AK → B 可见且凭据可用（B 登录/签名）；
  4. 停 B → A 写 N 条（repl_outbox 积压）→ 起 B → 全部追平（逐 ETag 对拍）；同窗口反向验证；
  5. 删除对象 → 两端 DeleteMarker；两端同时写同 key → 两版本均保留、内容各自一致；
  6. 桶删除控制面传播（对象数据保留——断言行消失但数据仍在，记档口径）。
- README「多站点复制（M7）」节（配置、API、冲突/凭据/同主密钥约束、边界）；设计文档 §M7 落地状态；
  功能规格 §10.2 注记；新增 `docs/assets/m7-sites.svg`（两站点双向流：对象 repl_outbox + 控制面 changelog）；
  架构图 subtitle 更新 M1–M7。
- 计划定稿（逐片勾选 + 证据链）；**发布待用户口令**。

## 执行模型

- 与 M4/M5/M6 同款：主会话唯一写者；verifier 每片对抗核查（计划阶段先经 verifier-m7-plan 评审）；
  七闸沿用防伪脚本（任一 gate 失败 → `GATES-FAILED`，全绿才 `GATES-DONE`）；
  证据日志入 `.superpowers/sdd/2026-10-18-m7-multisite/`。
