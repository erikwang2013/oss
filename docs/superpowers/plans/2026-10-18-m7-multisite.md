# M7 多站点异步复制 实施计划（工作令 v3）

> 设计文档 §7.2 / §7.3 / §M7；功能规格 §10.2 / §19。
> **验收**（设计文档 §M7）：
> ① 两站点互建关系后，任一站点写入在秒级出现在对端（含版本）
> ② 任一站点的桶/策略变更在对端可见
> ③ 断开 WAN 一段时间后恢复：积压全部重放，双向数据一致（逐 ETag 对拍）
> ④ 两端同时写同一 key 不产生损坏（版本不可变，追加式传播）
>
> **v3 修订（2026-10-18，按 verifier-m7-plan 第二轮 R1–R8）**：apply 期抑制覆盖 BEFORE；种子落点
> 与触发器激活条件（仅 `[site]` 且有 peer）；桶对账按最终行/物理空态而非批内 op；快照入本地钟且
> 只快照本地胜者；admin 排除补全（upsert/触发器/快照）；site 数据发送前校验对端 versioning；
> `[site]` 作为唯一开关（cluster 节点不可配对）；桶触发器列级捕获 + `upsert_bucket_set` 条件更新。
> 评审报告：`plan-review.md`（v1）、`plan-review-v2.md`（v2）。

## 钉子（防蔓延）

- **不做**（归 M9/M8）：sync 桶 / stretch EC / 控制面 openraft 强一致 / 访问点复制 / 自动配对 / 站点级过滤 /
  带宽限速 / TLS（跨站点一律 http，配置信任同 M6 `[[remotes]]`）/ 控制台「站点页」（规格偏差，切片 3 落档）/
  **多站点（>2）与多节点站点**——M7 = **单节点站点成对**：`[site]` 存在且 `[cluster]` 存在 → 启动 fail-closed；
  `[site]` 是 M7 唯一开关（无 `[site]` 的集群节点不挂站点路由、不配对）。
- **与 M6 的关系**：对象复制复用 `repl_outbox`（`target_kind='site'`），出站沿用 SigV4 + hyper +
  `x-amz-replication-status: REPLICA`；控制面复制走独立 changelog。
- **版本语义**：版本不可变 → 追加式；版本 UUID 站点本地生成（不保证一致）；验收③按**版本集合 + 每版本
  ETag/内容**对拍。非版本桶不参与对象站点复制。
- **删除语义**：对象删除以 marker 传播；**删除入队按源桶 `versioning='Enabled'` 门控**（Suspended/Off 不入队，
  记档）；版本化桶的整键硬删在对端表现为 marker（历史版本保留）。桶删除：**仅当本端物理桶为空且
  `store.delete_bucket` 成功**才删行并清理桶级表（lifecycle/event_configs/replication_rules/policies/
  permissions）；非空 → 不删行、保持现状并记 `last_error`（**不做 tombstone**；记档）。
- **控制面冲突（LWW）**：元组 **`(ts, priority, origin_site_id, seq)`**，`ts` 秒级 INTEGER。**ts 优先**
  （较新写胜出；priority 作同秒裁决）——与规格「按站点优先级裁决」的字面差异在切片 3 正式落档。
  - **本地写入钟**：AFTER 触发器插入 changelog（`last_insert_rowid()` 为 seq）后，条件 UPSERT clock
    （仅当候选元组 > 旧）。
  - **本地写门控**：BEFORE 触发器（**同一 apply_ctx 抑制条件**）候选元组 < clock → `RAISE(ABORT)`；
    无 clock 行放行；apply 期间不门控（否则合法条目被整批 ABORT）。
  - **偏移保护**：apply 条目 `ts > now + 300s` → 入 dead（偏移上限保护活性）；apply 端时钟偏差记档。
  - **收敛条件**：每侧只接受 ≥clock 的本地写与远端条目；clock 单调；两端最终收敛到最大元组值。快照与
    内部维护写必须同规则入钟（见下）。
- **触发器激活条件**：所有站点触发器 WHEN = `NOT apply_ctx.active AND EXISTS(SELECT 1 FROM sites)
  AND EXISTS(SELECT 1 FROM site_config)`。**未配对期间零捕获**（既有测试/行为零扰动）；配对时快照补齐
  历史状态。触发器在 0002 中以 `DROP TRIGGER IF EXISTS` + `CREATE TRIGGER` 每次启动重建（升级路径）。
- **捕获面**：`buckets` 用 **`AFTER UPDATE OF versioning, owner_id, object_lock` + OLD≠NEW**（`set_id`/
  启动回填不触发——R8）；`bucket_policies`、`permissions`、`users`（**排除 is_admin=1**）、`access_keys`
  （`kind='long-term'` 且所属用户 `is_admin=0`；UPDATE 需 OLD/NEW 均 long-term）。`upsert_bucket_set`
  加 `WHERE set_id <> excluded.set_id`（R8 兜底）。对象锁桶默认规则不捕获（记档）。
- **凭据**：`sites` 存 peer endpoint/region/优先级/`peer_site_token`（出站呈现）/S3 AK/SK；入站
  `X-OSS-Site-Token` 与**本站** `site_config.site_token` 恒定时间比对。`[site].token` 可选（≥32 hex，
  缺省随机）。管理员 `GET /api/v1/site` 可见本站 token；`GET /api/v1/sites` 脱敏。
- **密钥互操作**：AK `secret_key` 原样密文复制 → 要求两端同主密钥或均未启用 vault；identity 暴露 `has_vault`。
- **毒丸**：apply 逐条；永久坏条目（未知 entity/op、未知 username、本地 admin 冲突、payload >64KiB、
  ts 超偏移、bucket 非空删除失败）→ `site_apply_dead` 并越过（cursor 推进）；DB 忙 → 整批 500 重试。
  dead 载荷截断 4KB；status 暴露 dead 计数（对端 dead 不可见——记档）。
- 发布/推送：只在用户明确口令时执行。

## 切片总表

| # | 切片 | 交付 | 验收锚 |
|---|------|------|--------|
| 0 | 站点身份 + 关系 API + 迁移底座 | 0002 执行钩子（DROP/CREATE 触发器）+ 表 + `target_kind` 幂等补列；`Db::connect` 种子 site_config；`[site]` 开关/校验；`GET /site`、`GET /site/identity`、`GET/POST/DELETE /sites`、`POST /sites/{id}/ping`（同步校验 identity/region）；JWT except | 切片自验收 |
| 1a | changelog 捕获 + apply | 触发器（激活条件/列级/admin 过滤）+ clock + BEFORE 门控；`POST /site/changelog`（token/批量/LWW/毒丸/幂等）；桶删除「空桶物理删+清理表/非空跳过」；最终行对账 | 验收② |
| 1b | 出货 + 快照 + status | shipping worker + cursors；配对快照（单事务、只本地胜者、入钟）；`GET /sites/{id}/status` | ②③ |
| 2a | 对象复制入队 + 迁移 | `target_kind` + 夹具；逐 peer 入队（版本化数据/版本化桶删除；非版本/Suspended 不入） | ①④ |
| 2b | 动态 peer 出站 + 重试 | `ReplClient::single`；**发送前 `GET ?versioning` 就绪校验（缓存）**；404/未就绪 → 可重试（仅 site 行）；metadata 透传；peer 删除丢行；状态聚合 | ①③ |
| 3 | e2e + 文档 + 图 + 定稿 | `tests/e2e_sites.sh`（含 R3b/R4/R6/R8 场景）；README/设计/规格落档（含 priority-vs-ts 偏差与范围收缩）；`m7-sites.svg` + 架构图；定稿 | ①–④ |

## 切片 0：站点身份 + 关系 API + 迁移底座

- **迁移**：`db.rs::connect` = 0001 → `ensure_columns()`（含 `repl_outbox.target_kind` 探测补列）
  → `raw_sql(0002_m7_site.sql)`（表 `IF NOT EXISTS`；触发器 `DROP+CREATE`；索引 IF NOT EXISTS）
  → `INSERT OR IGNORE site_config`（uuid v4、name="site"、region us-east-1、priority 0、random token 32hex）
  → 返回。**种子在 connect 内完成**，保证 `AppState::load` 的两轮桶行对账（state.rs:269/297）与测试
  `AppState::new` 路径都有 site_config（R2）。
- `[site]` 配置（`config.rs`）：`name`/`region`（默认 us-east-1）/`priority`（默认 0）/`token`（可选）。
  `AppState::load` 在 `Db::connect` 后立即 `db.apply_site_config(&cfg.site)`（覆盖 name/region/priority/token）
  ——**早于桶对账与管理员引导**；`[site]` 与 `[cluster]` 同配 → 返回错误。`[site]` 为 None → M7 关闭。
- 0002 表：`site_config`、`sites(peer_id PK, name, endpoint, region, priority, s3_access_key, s3_secret_key,
  peer_site_token, created_at, last_seen, last_error)`、`apply_ctx`、`site_changelog(ts INTEGER)`、
  `site_entity_clock`、`site_cursors`、`site_apply_dead`。
- 管理 API（仅 `[site]` 存在时挂载；控制台 JWT）：
  - `GET /api/v1/site`（管理员）→ 本站身份 + site_token；
  - `GET /api/v1/site/identity`（JWT except；token 校验）→ id/name/region/priority/has_vault；
  - `GET /api/v1/sites`（管理员，脱敏）、`POST /api/v1/sites`（校验 peer 上限 1/id/endpoint
    http+host:port 无 path/region 非空/token ≥32hex；**同步调用对端 identity 校验 region 与可达性**，
    失败 400）、`DELETE /sites/{id}`（同事务丢 site outbox 行 + 删 cursor）、`POST /sites/{id}/ping`。
  - JWT except 追加 `/site/identity`、`/site/changelog`。
- 测试：connect 幂等/老库升级并**实写一次**（触发器列必须可用）；种子存在且 apply_site_config 覆盖；
  cluster+site 拒绝；无 `[site]` 不挂站点路由（404）/不可配对；API 矩阵（403/400/脱敏/ping region）。

## 切片 1a：changelog 捕获 + apply

- 触发器（0002，DROP+CREATE；WHEN 统一含 `NOT apply_ctx.active AND EXISTS(sites) AND EXISTS(site_config)`）：
  - `buckets`：BEFORE INSERT/UPDATE OF(versioning,owner_id,object_lock)/DELETE 门控；AFTER 同列（UPDATE
    需 OLD≠NEW）写条目；载荷 name/owner_username/creation_date/versioning/object_lock（**set_id 不写**）；
    DELETE 载荷 name。
  - `bucket_policies`：I/U/D；载荷 bucket/policy_json/updated_at。
  - `permissions`：entity_id=`username||char(31)||bucket`（无歧义）；载荷 username/bucket/can_*；只捕获
    所属用户 `is_admin=0`（子查询）。
  - `users`：仅 `is_admin=0`（I/U/D）；载荷 username/password_hash/status/created_at。
  - `access_keys`：`kind='long-term'` 且所属用户 `is_admin=0`（UPDATE 需 OLD/NEW 均满足）；载荷
    access_key/secret_key(原样)/username/status/expires_at/created_at。
  - AFTER：插 changelog（seq=last_insert_rowid）→ 条件 UPSERT clock（候选 > 旧才更新）。
  - BEFORE 门控：候选 `(now_ts, local_priority, local_site_id)` 与 clock `(ts,priority,origin)` 行值比较，
    候选小 → `RAISE(ABORT,'site replication conflict')`；`apply_ctx.active=1` 时跳过。
- apply `POST /api/v1/site/changelog`（token=本站；401）：≤500 条、单条 ≤64KiB；**单事务**：
  `apply_ctx.active=1`（事务内；失败回滚即复位 0——P12）→ 逐条：
  - origin==本站 / 元组 ≤ clock → 跳过；未知 entity/op/未知 username/admin 冲突/超偏移 → dead 并继续；
  - bucket upsert：按 username 解析 owner；`ON CONFLICT DO UPDATE` 更新 owner/versioning/object_lock
    （set_id 保持本地）；delete：先 `store.delete_bucket`（**非空 → Err** → 不删行，记 last_error 并作为
    dead 条目）→ 成功后删行 + 清理 lifecycle_rules/event_configs/replication_rules/bucket_policies/
    permissions；
  - policy/permission/user/AK 应用规则同 v2（用户删除按 username 走完整级联：access_keys/permissions/
    uploads.owner_id=NULL/buckets.owner_id=NULL；本地 admin 目标 → dead）；
  - 更新 clock。
  - 提交后**按最终状态对账**：对批内出现的每个 bucket 实体，若 `buckets` 行存在 → `store.create_bucket`
    （幂等）；行不存在 → no-op（物理删除已在条目应用时完成）。**不得按批内 op 决策**（R3a）。
    create 失败 → 500 让对端重试；重试时仍按键存在对账（幂等）。
- 409 映射：console 建桶/策略等端点捕获 `site replication conflict` → 409 并清理已做物理副作用
  （console 建桶先 DB 行后物理，失败回滚物理——P2）。
- 测试：触发器矩阵（5 表 × I/U/D × 激活/未激活/apply 期/UPSERT/INSERT OR IGNORE）；本地门控（ABORT/409）；
  双向并发收敛（含快照后）；apply 幂等；毒丸越过；admin 同名不被覆盖（upsert/delete）；用户删除级联含
  uploads；桶删除空桶成功/非空跳过且重启不复活；最终行对账；`set_id`-only 更新不产生条目。

## 切片 1b：出货 + 快照 + status

- shipping worker（`OSS_SITE_INTERVAL_SECS` 默认 1，0=关）：逐 peer 读 seq > cursor（≤100）→ POST；
  2xx 推进 cursor（dead 越过）；失败记 last_error 下 tick 重试。
- **配对快照**（`POST /sites` 新建 peer 后，单事务）：对 `users(is_admin=0)`、`access_keys(kind='long-term'
  且用户非 admin)`、`permissions(用户非 admin)`、`buckets`、`bucket_policies` 中**本地 clock 无行或
  origin=本站**的实体插入 changelog（origin=self，ts=now）**并同步 upsert 本地 clock**（R4；避免回显与
  同时配对分叉）。peer cursor=0 → 首轮全量送达；LWW 幂等。
- `GET /api/v1/sites/{id}/status`：cursor lag、dead 计数（本端应用 dead 表 + peer 状态未知记档）、
  site outbox 状态计数、last_seen/last_error。
- 测试：cursor/重放；快照覆盖既有资源；同名实体双方同时配对的不同值 → 收敛到最大元组；快照不回显；
  status 数字；worker 0 关闭。

## 切片 2a：对象复制入队 + 迁移

- `ensure_columns` 补 `target_kind`；索引；测试夹具 DDL 同步。
- `SqlReplSink::on_write`：对每个 peer 追加 site 行：
  - 数据写：仅版本化（非 "null"）且非 REPLICA；
  - 删除写：源桶 versioning 必须 `Enabled`（Off/Suspended 不入队——记档）；marker 带版本；硬删无版本行
    `version_id=''`。
  - PENDING 单调写（M6 实现复用）。
- 测试：入队矩阵（peer、marker、REPLICA、null、Suspended、非版本桶删除不入）。

## 切片 2b：动态 peer 出站 + 重试

- `ReplClient::single(endpoint, ak, sk, region)`；worker 按 `sites` 取参数；peer 不存在 → 丢行 + 警告
  （不改状态）。
- **发送前置**：site 数据/marker 行发送前，对目标桶做签名 `GET /{bucket}?versioning`：
  404 或状态非 Enabled → 可重试；结果按 `(peer,bucket)` 缓存 10s（R6）。仅 site 行生效（P8）。
- metadata 透传：PUT 带 `content-type` + `x-amz-meta-*`；锁/标签/SSE 不复制（记档）。
- 状态聚合沿用 `settle_version_status`；peer 删除属丢行路径。
- 测试：mock 对端（versioning 探询/REPLICA/ETag/metadata/404/未就绪/连接断开积压→恢复）；混合规则聚合。

## 切片 3：e2e + 文档 + 图 + 定稿

- `tests/e2e_sites.sh`（A/B 无 vault；A 非默认 region）：
  1. 配对前各建桶/用户/AK → 双向配对（token 从 `GET /site`）→ 快照同步 + ping；
  2. **建桶后立刻写对象**（不等待）→ 断言对端最终有该版本且 `version_id` 存在、ETag/内容一致；
     反向同理；
  3. 桶策略（`*` 或 AK Principal）与用户/长时 AK 传播可用；同名普通用户/管理员场景安全；
  4. 停 B → A 写 N 条 PENDING → 起 B → 追平（预算 ≥300s）；反向同窗；
  5. 版本桶删除 → 双端 marker；双端并发写同 key → 版本集合互见；
  6. **R3b**：非空桶删除请求 → 对端不删行/桶仍在；空桶删除 → 两端行消失；**R8**：A 配对后 B 开
     versioning，A 重启（触发启动回填）→ 版本控制不回退；
  7. 非版本桶删除不跨界。
- 文档：README「多站点复制（M7）」；设计 §M7 落地状态 + **范围收缩正式落档**（单节点站点对、无站点页、
  无 /sites/{id}/join、非版本桶不复制、ts-first vs priority 规格偏差、明文 http、同主密钥约束）；
  功能规格 §10.2 注记与 §5.2 表清单补 `site_*`；`docs/assets/m7-sites.svg` + 架构图 M1–M7。
- 定稿；发布待口令。

## 已知边界（在案）

1) 控制面最终一致：LWW ts-first + 本地写门控；**远端未来 ts 会让本地写暂时 409/500 直到钟追上**（上限
   保护：>now+300s 入 dead）；强一致归 M9；2) 版本 UUID 不一致、版本定向删除不复制、MPU/大对象 ETag 口径；
3) 锁/标签不复制；SSE 源明文过 http；SSE-C 源不可复制；4) 非空桶删除不生效（保留行与数据，记 last_error）；
5) `uid:<id>` Principal 不重映射；6) AK 密文需同主密钥；STS 会话键不复制；7) 管理员行不复制；
8) changelog 无裁剪/重配对全量重放；9) dead 载荷截断 4KB 且对端 dead 本端不可见；10) 端点/凭据明文存储、
   无 token 轮换。
