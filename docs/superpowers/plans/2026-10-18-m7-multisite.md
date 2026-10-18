# M7 多站点异步复制 实施计划（工作令 v2）

> 设计文档 §7.2（事件总线复用）/ §7.3（多站点一致性）/ §M7；功能规格 §10.2 / §19。
> **验收**（设计文档 §M7）：
> ① 两站点互建关系后，任一站点写入在秒级出现在对端（含版本）
> ② 任一站点的桶/策略变更在对端可见
> ③ 断开 WAN 一段时间后恢复：积压全部重放，双向数据一致（逐 ETag 对拍）
> ④ 两端同时写同一 key 不产生损坏（版本不可变，追加式传播）
>
> **v2 修订（2026-10-18，按 verifier-m7-plan 第一轮 B1–B12）**：本地写时钟、0002 执行钩子与幂等补列、
> 触发器捕获 access_keys、owner/set 本地语义、admin 排除、建桶副作用对账、删除版本化门控、控制面
> 毒丸死信、token 方向性与获取路径、peer region、配对时控制面快照、「站点仅单节点」正式收缩。
> 评审报告：`.superpowers/sdd/2026-10-18-m7-multisite/plan-review.md`。

## 钉子（防蔓延）

- **不做**（归 M9/M8）：sync 桶 / stretch EC / 控制面 openraft 强一致 / 访问点复制 / 自动配对 / 站点级过滤
  （站点关系 = 全桶双向）/ 带宽限速 / TLS（跨站点一律 http，配置信任同 M6 `[[remotes]]`）/ 控制台「站点页」
  （后端管理 API 完整——**规格偏差，切片 3 落档设计/规格**）/ **多站点（>2）与多节点站点**——M7 只支持
  **单节点站点成对**：`[site]` 与 `cluster` 同时配置 → 启动 fail-closed；>1 peer → API 400。
  （多节点/多站点扇出归 M9 前扩展，正式写入设计文档 M7 范围。）
- **与 M6 的关系**：对象复制复用 `repl_outbox`（新增 `target_kind='site'`），出站沿用 SigV4 + hyper +
  `x-amz-replication-status: REPLICA`（防环）；控制面复制走独立 changelog（不入事件总线/Kafka）。
- **版本语义**：版本不可变 → 追加式传播；**版本 UUID 站点本地生成，不保证两端一致**；以内容/ETag/版本数
  语义为准（验收③按**版本集合 + 每版本 ETag/内容**对拍，不比 latest GET/HEAD）。非版本桶不参与对象站点复制
  （建桶与版本控制变更本身会复制）。
- **删除语义**：对象删除以 delete marker 传播；**删除入队按源桶 `versioning='Enabled'` 门控**（非版本桶的
  硬删永不跨界——防止删掉对端独立数据）；控制台/生命周期整键硬删在版本化目标上表现为 marker（对端历史版本
  保留——记档）；桶删除：apply 先尝试「本端空桶则物理删除（`store.delete_bucket`）+ 行删除」，非空则只删行并
  记 `last_error`（**记档：磁盘桶仍可见，tombstone 归后续**）。
- **冲突裁决（控制面）**：LWW 元组 **`(ts, priority, origin_site_id, seq)`** 字典序（ts 秒级；priority 数值越大
  越权威、作为同秒裁决；origin/seq 破平）。
  - **本地写也入钟**：触发器在写 changelog 后同步 upsert `site_entity_clock`（本地写不被静默跳过）。
  - **本地写门控**：BEFORE 触发器比较候选元组与 clock；候选 < clock → `RAISE(ABORT)`（低优先级同秒写直接失败，
    不产生永久分叉）。API 层把该 DB 错误映射为 409（能定位的端点）；其余记 500。
  - apply：条目元组 > clock 才应用；≤ 跳过（幂等）。收敛性论证：clock 单调不减、任一侧只接受 ≥clock 的写，
    两端最终持有同一最大元组值。
- **凭据模型**：`sites` 行存 `peer.site_token`（**出站时呈现给对端**）+ `s3_access_key/secret_key`
  （对端铸造的 AK，SigV4）+ `region`。入站 `X-OSS-Site-Token` 恒与**本站** `site_config.site_token`
  恒定时间比对。站点 token：`[site].token` 可选（管理员预共享）；缺省随机 32 hex。管理员 `GET /api/v1/site`
  可见本站 token（便于配对）；`GET /api/v1/sites` 脱敏。README 写明 http 明文信任边界。
- **密钥互操作**：`access_keys.secret_key` 原样密文复制 → 跨站点 AK 生效要求两端同一主密钥（同一
  `OSS_MASTER_KEY_FILE`）或均未启用 vault；`GET /api/v1/site/identity` 暴露 `has_vault` 供配对期发现不对称。
- **changelog 捕获面**：`buckets`（owner 以 **username** 表达；`set_id` 不复制，apply 按本地自动指派 0）、
  `bucket_policies`、`permissions`（username+bucket）、`users`（**排除 is_admin=1**）、`access_keys`
  （触发器 + `kind='long-term'` 过滤；STS 会话键不复制）。对象锁桶默认规则（ensure_columns 补列）不捕获，记档。
- **毒丸策略**：apply 逐条校验；永久坏条目（未知 entity/op、schema 未知、未知 username、payload 超限）写入
  `site_apply_dead` 并**越过**（cursor 推进）；DB 忙/写冲突 → 整批 500 重试。`site status` 暴露 dead 数。
- 发布/推送：只在用户明确口令时执行。

## 切片总表（串行；每片 = 代码 → 测试 → 七闸 → verifier 核查 → 定稿 → 本地提交）

| # | 切片 | 交付 | 验收锚 |
|---|------|------|--------|
| 0 | 站点身份 + 关系 API + 迁移底座 | 0002 执行钩子 + `site_config`/`sites`/`site_*` 表 + `target_kind` 幂等补列；`[site]` 配置；bootstrap 次序；`GET /api/v1/site`、`GET /api/v1/site/identity`（token）、`GET/POST/DELETE /api/v1/sites`、`POST /api/v1/sites/{id}/ping`；JWT except | 切片自验收 |
| 1a | changelog 捕获 + apply | clock 表 + 四组 AFTER 触发器（buckets/policies/permissions/users）+ access_keys 触发器 + BEFORE 本地门控；`POST /api/v1/site/changelog`（token、批量、LWW、毒丸、幂等）；桶物理对账 | 验收② |
| 1b | 出货 + 初始快照 + status | shipping worker + `site_cursors` + 配对时控制面快照（既有 users/AK/permissions/buckets/policies 入 changelog）；`GET /api/v1/sites/{id}/status`（cursor lag/outbox/dead/last_error） | 验收②③ |
| 2a | 对象复制入队 + 迁移 | `repl_outbox.target_kind`（幂等补列）+ 夹具 DDL；SqlReplSink 逐 peer 入队（版本化数据 + 版本化桶删除；非版本桶删除不入）；marker/REPLICA 门控 | 验收①④ |
| 2b | 动态 peer 出站 + 重试 | `ReplClient::single`；site 行 404/版本控制未就绪 → 可重试（仅 site 行）；peer 删除 → 行丢弃记档；metadata/content-type 透传；状态聚合 | 验收①③ |
| 3 | e2e + 文档 + 图 + 定稿 | `tests/e2e_sites.sh`（双向/桶/策略/IAM/初始快照/停机重放/版本集合对拍/并发写/marker）；README、设计/规格落地状态与偏差落档；`docs/assets/m7-sites.svg` + 架构图 M1–M7；计划定稿 | ①–④ |

## 切片 0：站点身份 + 关系 API + 迁移底座

- **迁移落地（B2）**：`db.rs::connect` = 0001 重放 → `ensure_columns()`（新增 `repl_outbox.target_kind`
  探测-补列）→ `raw_sql(include_str!("../migrations/0002_m7_site.sql"))`（表/触发器全 IF NOT EXISTS，每次启动
  重放幂等）→ 返回。**触发器必须在 ensure_columns 之后创建**（引用 `kind`/`status`/`object_lock` 等补列）。
  补「v1.10.0 老库升级」测试（老库无 0002 无 target_kind → 启动后全部就位，二次启动不报错）。
- 0002 表：`site_config(id PK, name, region, priority, site_token, created_at)` 单行；
  `sites(peer_id PK, name, endpoint, region, priority, s3_access_key, s3_secret_key, peer_site_token,
  created_at, last_seen, last_error)`；`apply_ctx(id=1,active)`；`site_changelog`、`site_entity_clock`、
  `site_cursors`、`site_apply_dead`（见下）；`repl_outbox.target_kind` 由 ensure_columns 补。
- 配置：`[site]` 可选 `name`/`region`（默认 us-east-1）/`priority`（默认 0）/`token`（≥32 hex；缺省随机生成）。
  **bootstrap 在 `AppState::new` 最前**（先于管理员引导与 groups 建桶——否则触发器按本地写重复入日志；
  `site_config` 无触发器，种子本身安全）：无行则 INSERT uuid v4 + token。`[site]` 与 `[cluster]` 同配 →
  `AppState::load` 返回错误（M7 单节点站点）。
- 管理 API（控制台 JWT；仅管理员；`who/admin_of`）：
  - `GET /api/v1/site` → 本站身份 + site_token（管理员可见；README 警告）
  - `GET /api/v1/site/identity`（**JWT except**，`X-OSS-Site-Token` 校验本站 token）→ id/name/region/
    priority/has_vault（不含 token）
  - `GET /api/v1/sites` → peer 列表（脱敏：has_credentials，绝不回显 SK/token）
  - `POST /api/v1/sites` {peer_id,name,endpoint,region,priority,s3_access_key,s3_secret_key,peer_site_token}
    ——校验：peer_id 白名单、≠本机、endpoint 仅 `http://host[:port]`（无 path/query/userinfo）、region 非空、
    AK/SK 非空、token ≥32 hex；**至多 1 peer**；插入后触发**控制面快照**（1b）并把 cursor 置 0。
  - `DELETE /api/v1/sites/{peer_id}` → 删 peer（object outbox site 行同事务丢弃 + 记日志；cursor 删除）
  - `POST /api/v1/sites/{peer_id}/ping` → 以 peer_site_token 调对端 `GET /api/v1/site/identity`；更新
    last_seen/last_error；region 不一致 → 400 提示。
- JWT except（`state.rs` JwtConfig.except）追加 `/site/identity`、`/site/changelog`（后者 1a 启用）。
- 测试：迁移幂等/老库升级；bootstrap 先于 admin/groups 且 idempotent；集群+site fail-closed；API 矩阵
  （非管理员 403、校验 400、peer 上限、脱敏、token 校验、ping region 不一致）。

## 切片 1a：changelog 捕获 + apply

- 表：
  - `site_changelog(seq PK AUTOINCREMENT, origin_site_id, entity, entity_id, op, payload, priority, ts,
    created_at)`
  - `site_entity_clock(entity, entity_id, ts, priority, origin_site_id, seq, PRIMARY KEY(entity,entity_id))`
  - `apply_ctx(id=1, active)`（种子 0）
  - `site_apply_dead(peer_id, seq, entity, entity_id, error, payload, created_at, PRIMARY KEY(peer_id,seq))`
- 触发器（0002）：每表 BEFORE（本地门控）+ AFTER（写 changelog + upsert clock；`apply_ctx.active=1` 时跳过）：
  - 元组候选：`(strftime('%s','now'), local_priority, local_site_id, (SELECT COALESCE(MAX(seq),0)+1 FROM
    site_changelog))`；BEFORE 用行值比较 `(new_ts,prio,self_id) < (SELECT ts,priority,origin_site_id FROM
    site_entity_clock WHERE ...)` → `RAISE(ABORT,'site replication conflict')`（无 clock 行则放行）。
  - `buckets`：entity_id=name；载荷 `name/owner_username/creation_date/versioning/object_lock`
    （owner 以 `(SELECT username FROM users WHERE id=?)`，无则 NULL；**set_id 不复制**）；DELETE 载荷 name。
  - `bucket_policies`：bucket + policy_json + updated_at。
  - `permissions`：entity_id=`COALESCE(username,'')||':'||bucket`；载荷 username/bucket/can_*。
  - `users`：`WHEN NEW.is_admin=0`（UPDATE 需 `OLD.is_admin=0 AND NEW.is_admin=0`；DELETE `OLD.is_admin=0`）；
    载荷 username/password_hash/status/created_at（**不复制 is_admin**）。
  - `access_keys`：`WHEN NEW.kind='long-term'`（DELETE `OLD.kind='long-term'`）；载荷 access_key/
    secret_key(原样)/username/status/expires_at/created_at。
  - AFTER 写 changelog 用 `last_insert_rowid()` 作 seq，再 `INSERT ... ON CONFLICT DO UPDATE` clock（仅当候选 > 旧）。
- apply `POST /api/v1/site/changelog`（token=本站；失败 401）：`{entries:[...]}` ≤500 条、单条 payload ≤64KiB。
  单事务、`apply_ctx.active=1`；逐条：
  - origin == 本站 → 跳过（环）；未知 entity/op/字段超限 → `site_apply_dead` 并继续；
  - 元组 ≤ clock → 跳过；> → 应用：
    - bucket upsert：`INSERT INTO buckets(name,owner_id,creation_date,versioning,object_lock,set_id)
      VALUES(..., <本地 owner username 解析>, ..., 0) ON CONFLICT(name) DO UPDATE SET ...`（versioning/
      object_lock/owner 更新；set_id 保持本地值）；delete：行删除（物理删除见下）。
    - policy upsert/delete；permission upsert/delete（username→本地 id，未知用户 → dead 跳过）；
    - user upsert：按 username；不存在则 INSERT（password_hash/status/created_at），存在则 UPDATE 这三列；
      **绝不改 is_admin**；delete：按 username 解析本地 id，若本地为 admin → dead 跳过；否则级联
      `access_keys`/`permissions`/`buckets.owner_id=NULL` 后删 user；
    - access_key upsert：按 access_key；username 未知 → dead；若本地同 AK 属另一 user → dead（不静默改属主）；
      delete：按 access_key 删（long-term 才有条目）。
    - 更新 clock。
  - 事务提交后**对批内出现的每个 bucket 实体（upsert 与 delete 均算）幂等对账**：upsert → `store.create_bucket`
    （已存在 no-op）；delete → 若本地桶为空则 `store.delete_bucket`，非空只删行并记 sites.last_error。
    即使 clock 全部跳过也执行（B6 幂等；失败 → 500 让对端重试）。
- API 错误映射：`RAISE(ABORT,'site replication conflict')` → 409（能定位的 console 端点；其余 500 记档）。
- 测试：触发器形状矩阵（5 表 × I/U/D + apply_ctx 抑制）；本地门控（低元组写 409/ABORT）；LWW 收敛模拟
  （双向并发写同一实体 → 两端最终同值）；apply 幂等（重复批次）；未知用户/坏载荷 → dead 且 cursor 可越过；
  admin 行不被捕获/注入；用户删除级联；建桶副作用失败→重试成功；owner username 映射正确。

## 切片 1b：出货 + 初始快照 + status

- shipping worker（`OSS_SITE_INTERVAL_SECS` 默认 1；0=关）：逐 peer 读 seq > cursor（≤100）→ POST；2xx 推进
  cursor（含 dead 越过）；失败记 last_error、下 tick 重试。**控制面不设死信**，但 dead 条目由 apply 端返回并在
  status 暴露。
- 配对快照：`POST /api/v1/sites` 成功后，把本端当前 `users(is_admin=0)/access_keys(kind='long-term')/
  permissions/buckets/policies` 以 upsert 条目**直接插入 site_changelog**（origin=self，ts=now，独立 seq），
  peer 的 cursor=0 → 首轮全部送达（LWW 幂等）。重复添加 peer（upsert）不重复快照（仅在新建时）。
- `GET /api/v1/sites/{id}/status`（管理员）：peer 基本信息 + 控制面 `last_seq-cursor` lag + dead 计数 +
  `repl_outbox` 按 state 计数（`target_kind='site' AND remote_id=id`）+ last_error/last_seen。
- 测试：cursor 推进/重放；快照覆盖既有资源（配对前建桶/用户/AK → 对端可见可用）；status 数字正确；
  worker 间隔 0 关闭。

## 切片 2a：对象复制入队 + 迁移

- `ensure_columns` 补 `repl_outbox.target_kind TEXT NOT NULL DEFAULT 'remote'`；索引
  `(target_kind, remote_id, state, next_retry_at)`（0002 内 CREATE INDEX IF NOT EXISTS）。
- 夹具 DDL（`tests/replication.rs` 等自建表）同步加列（P10）。
- `SqlReplSink::on_write`：本地桶规则照旧；再对每个 peer（≤1）追加 site 行：
  - 数据写：仅 `version_id != "null"` 且非 REPLICA；
  - 删除写：**先查源桶 versioning 是否 Enabled**，未启用 → 不入队（B7）；启用 → 入队（marker 带版本号；
    硬删无版本行 `version_id=''`）。
  - 状态 PENDING 同 M6（单调写）。
- 测试：入队矩阵（peer/无 peer、marker、REPLICA、null 版本、非版本桶删除不入）；夹具迁移。

## 切片 2b：动态 peer 出站 + 重试

- `ReplClient::single(endpoint, ak, sk, region)`；worker 对 `target_kind='site'` 行按 `sites` 表取
  endpoint/AK/SK/region（每行构造；peer 不存在 → 丢弃行 + 警告，不改对象状态——P12）。
- 源对象属性透传：PUT 带 `content-type` 与全部 `x-amz-meta-*`（自源 info；对象锁/标签/SSE **不复制**——P3 记档）。
- 重试：site 行 **404 / 目标桶 versioning 未就绪 → 可重试**（仅 site 行生效——P8）；其余沿用 M6 分级。
- 状态聚合沿用 `settle_version_status`（跨 remote/site 行）；site 行丢弃时若该版本其余行已完成 → 正常 COMPLETED。
- 测试：mock 对端（REPLICA 头/同 ETag/metadata 透传/404 重试/versioning 未就绪重试/连接断开积压→恢复）；
  混合规则 + site 聚合；peer 删除后 in-flight 行丢弃。

## 切片 3：e2e + 文档 + 图 + 定稿

- `tests/e2e_sites.sh`（两实例 A/B，均无 vault；A `[site].region` 非默认覆盖一次 B10 路径）：
  1. 配对前：A/B 各建桶/用户/AK；双向 POST /sites（token 从各自 `GET /api/v1/site` 取）→ 快照同步，
     ping 绿；
  2. A 建新桶 + 版本控制 Enabled → 等 B 可见且 versioning Enabled（**再写对象**）；A 写 → B 版本集合
     含对应版本（`x-amz-version-id` 存在）且逐版本 ETag/内容一致；反向同理；
  3. A 设桶策略（Principal `*` 或 AK）→ B GET 一致；A 建用户/长时 AK → B 可登录/可签名（SK 同主密钥口径）；
  4. 停 B → A 写 N 条（repl_outbox 积压，状态 PENDING）→ 起 B → 轮询至 COMPLETED（预算 ≥300s 退避上限）；
     B→A 方向同窗口验证；
  5. 删除对象（版本桶）→ 两端 DeleteMarker；两端同时写同 key → 各自版本均在对端出现、内容对应；
  6. 非版本桶：两端各写同 key；A 删除 → **B 对象仍在**（B7 回归）；桶删除（空桶）→ 两端行消失（物理删除）。
- 文档：README「多站点复制（M7）」；设计文档 §M7 落地状态 **并正式写入范围收缩**（站点对、单节点、
  无站点页、无 /sites/{id}/join 形态、非版本桶对象不复制、控制面最终一致冲突门控）；功能规格 §10.2 注记；
  §5.2 表清单补 `site_*`/`repl_outbox.target_kind`；新增 `docs/assets/m7-sites.svg`（双向对象流 + 控制面
  changelog/快照/LWW），架构图 subtitle M1–M7。
- 计划定稿（逐片勾选 + 证据链）；发布待用户口令。

## 已知边界（记录在案）

1) 控制面最终一致：LWW `(ts,priority,origin,seq)` + 本地写门控；跨站时钟偏差可致「较旧 ts 胜出」，强一致归 M9；
2) 版本 UUID 不保证一致；版本定向删除不复制（M6 记档）；>5G/MPU 对象 ETag 口径不同（验收对拍限单段小对象）；
3) 对象锁/标签不复制；SSE-S3/KMS 源以**明文过 http** 落到对端（不保持加密，内网假设）；SSE-C 源不可复制；
4) 桶删除：空桶物理删除，非空只删控制面行（磁盘桶仍可见）；tombstone 归后续；
5) 用户/权限按 username 映射；`Policy` 里 `uid:<id>` Principal 不重映射（跨站点策略请用用户名/AK/*）；
6) `access_keys` 密文互操作要求相同主密钥或均未启用 vault；STS 会话键不复制；
7) 管理员账号（is_admin=1）不复制——两端各自管理；同名管理员口令以各自引导为准；
8) changelog 无裁剪、peer 删除后 cursor 删除；重新配对会重放历史 + 新快照（LWW 幂等）。
