# M9 强一致控制面、同步桶与 Stretch EC 实施计划（v5）

> **历史文档（2026-10-23 注）**：本文件记录当时的计划与边界。部分“未实现/非目标/后置”
> 已在 2026-10-23 能力清理批次或更早落地；以实现代码、README 与
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md` 为准。


> 设计文档 §M9 / §7.3（`docs/superpowers/specs/2026-10-10-minio-clone-design.md:348-358,612-621,646-664`）；
> 功能规格 §3.3 / §10.2 / §里程碑（`docs/superpowers/specs/2026-10-10-feature-spec.md:87-92,491-506,815-821`）。
> 基线：`b160e08`（M8 / v1.12.0 已发布，工作树干净）。本文档是计划；当前任务不改源码，
> 实施阶段按 §4 跑 preflight/cargo。
> 评审输入：`/tmp/m9-plan-review.md`（v1 NO-GO：6 P0 + 11 P1 + 15 non-blocking）；
> `/tmp/m9-plan-review-v2.md`（v2 NO-GO：4 new P0 + 10 P1 + 14 non-blocking）；
> `/tmp/m9-plan-review-v3.md`（v3 NO-GO：3 new P0 + 8 P1 + 13 non-blocking）；
> `/tmp/m9-plan-review-v4.md`（v4 NO-GO：4 new P1，B1–B4，均为契约修正）。
>
> **产品裁决（2026-10-20，binding；v1 的 2-voter、删除 501、③ 延后方案均不再适用）**
> A. 控制面固定 **3 voters**：数据站点 A、B + 只承载控制面的 witness C；quorum 2。
> B. sync 桶 **本轮实现 remote-first deletes**：DeleteObject / DeleteObjects / 版本删除 / 生命周期过期。
> C. **Stretch EC 2×6 本轮实现**：两站点 × 6 盘、EC 8+4、站点感知放置、`Q_s=9`、`inline_threshold=0`、
>    meta 镜像 12 槽、读写均需两站点；不承诺单站点容灾（明确文档化）。
>
> **验收（设计 §M9）**
> ① 3-voter 控制面并发写收敛，无分叉；单数据站点宕机后控制面仍可写（quorum 2）。
> ② sync 桶：远端确认后才返回成功；断连 503；不降级；**写与删都 remote-first**。
> ③ Stretch EC：2 站点 × 6 盘、8+4；单站点 6 片 < data=8，读必须联合两站点；
>    写要求 9 个槽位；不承诺单站点故障后仍可读（见 §2.5）。
> ④ 站点恢复后追平；复制实体业务字段逐字段相等（代理键按自然键归一化）。
>
> 计划状态：**v5（2026-10-20）**，冻结待实施。行号均为 `b160e08`；实现时若漂移以函数名为锚。
> 这是 multi-week program：preflight 7 d，切片基础 **65 人日** + **3 d 应急** = **68 人日**
> （含 preflight 共 **75 人日**）；单工程师约 15 周，双轨约 11–12 周。
> v4 重估吸收了 v3 评审列出的 5–8 人日修复量；v5 只收口 B1–B4 契约修正（<0.5 d），
> 估算维持 **75 人日**（增量已含在应急内）。

## 0. 验收映射与评审响应

### 0.1 验收映射

| 验收 | 落点 | 证据 |
|---|---|---|
| ① 3-voter 控制面无分叉 | S5–S8 | `raft_three_voter.rs`：A/B 并发写 IAM/AK/bucket/policy；kill B 后 A+C 仍可写；kill C 后 A+B 仍可写；kill A+B 后 503；重启追平；归一化 diff=0 |
| ② sync 写/删 remote-first | S1–S3 | `e2e_m9.sh`：PUT 200 仅远端 ack 后；SIGSTOP B → PUT/DELETE 503、本地不可见/未删；SIGCONT → 200/204；版本删除、DeleteObjects、生命周期过期；无 outbox |
| ③ Stretch EC 2×6 | S4 + S9 | `e2e_stretch.sh`：两站点各 6 盘、8+4；写入成功需 9 槽；停 B 后 GET/PUT 非 2xx；两站点齐时内容相等；meta 镜像 12、提交 ≥9；`inline=0` |
| ④ 恢复追平/字段相等 | S7 + S8 | 快照安装、kill -9、3-voter 重启、M7→M9 cutover；`raft_applied`/membership 相等；业务字段归一化 diff=0 |
| M7 回归 | 全程 | `[raft]` 缺失时 `e2e_sites.sh` 原样通过；M7 触发器/worker 行为不变；`[raft]` 下 async 桶对象 worker 仍工作 |

### 0.2 评审响应矩阵（v1 17 blocking + 15 N；v2 14 blocking + 14 N；v3 11 blocking + 13 N；v4 4 P1）

计数：v1 **6/6 P0、11/11 P1、15/15 non-blocking** 已落点；v2 **4/4 P0、10/10 P1、
14/14 non-blocking** 已落点；v3 **3/3 P0、8/8 P1** 已落点，另应用 **N2/N5/N6/N7/N13**；
v4 **4/4 P1（B1–B4）** 已在 v5 落点。累计 **88 项中 80 项**有明确落点
（v1 32 + v2 28 + v3 11 阻塞 + 5 非阻塞 + v4 4）；v3 的 N4（sync+stretch 规格收窄）
以显式范围修订处理，N11（估算）已重估，N1/N3/N8/N9/N10/N12 保留为非阻塞实施注意项
（由 S2/S3/S5/S9 覆盖）。
本计划不再使用“全部闭环/无未决阻塞”的总括说法。

**v1 矩阵（保留可追溯性）**

| 项 | 处置 | 落点 |
|---|---|---|
| P0-1 降级守卫不执行 | `prepare_raft_mode(present)` 在 `Db::connect` 后**无条件**调用；`raft-cutover` 写 `raft_mode=on`；无 `[raft]` + on → 拒绝启动 | §2.1, S0, S7 |
| P0-2 单边配对/self endpoint/RPC 认证/M7 行 | `[raft].advertise_endpoint`；`POST /sites` + 对端 `POST /api/v1/site/raft/join` 双边落行；共享 `cluster_token`；Raft 模式 adopt 已有 M7 `sites` 行 | §2.6, S7 |
| P0-3 StoredMembership 未持久化 | `raft_applied` 存完整 `LogId` JSON + `membership_json`；快照 meta 同带；apply/安装同事务 | §2.3, S0, S5–S7 |
| P0-4 补偿 DELETE 被 store 501 拒 | 删除裁决上移 S3 service 层；token 认证的站点删除走本地直删；普通客户端删除走 remote-first | §2.4, S3 |
| P0-5 本地 quorum 表述不可实现 | 删除“flipped >= data+1 / 不写 meta / QuorumUnmet”分支；明确 staging 的 `data+1` 与 meta `prepared >= data+1`、首 flip 可见、heal 兜底 | §2.4, S1 |
| P0-6 能力开关早翻 | `SYNC_READY` 在 S3 出口翻；`RAFT_READY` 在 S6 出口翻；未就绪时 `RAFT_READY=false` 控制写 503、`SYNC_READY=false` sync 创建 400；guard 失败 5xx 不 204 | §2.3, S0, S3, S6 |
| P1-1 tag 变更绕过 gate | sync 桶 Put/DeleteObjectTagging、Batch tag 返回 501；仅 PUT 携带 tags | §2.4, S2 |
| P1-2 SSE-C 明文复制 | sync 写（PUT/MPU/Copy 目标）SSE-C 返回 400；SSE-S3/KMS 目标本地；明文 WAN 文档化 | §2.4, S2 |
| P1-3 sync 无 Raft 也允许 | sync 创建要求 `[raft]` + `RAFT_READY` + 恰好 1 个数据 peer；无 M7 create barrier | §2.4, S0, S1 |
| P1-4 启动顺序/物理对账/spawn | 固定顺序 + `reconcile_bucket_dirs` 每次 Raft 启动；`raft::spawn` 公开；side effect 重试 | §2.1, S5, S6 |
| P1-5 `[[groups]]` 被 guard 拒 | `[raft] + [[groups]]` 启动即拒；跳过 `upsert_bucket_set`（set_id=0） | §2.3, S6 |
| P1-6 create 写入不完整/204 | `db`/`access`/`console` 三个 create writer 统一为全列 `create_bucket_tx`；mode 冲突 409；guard ABORT→5xx/409 | §2.3, S0, S6 |
| P1-7 REPLICA 丢 tags/class | token 验证的 sync REPLICA 保留 tags/storage_class；普通 replica 保持旧行为 | §2.4, S2 |
| P1-8 cutover 丢桶列 | `raft-export` 导出 buckets 全列（含 lock 默认值/default_encryption）；冲突默认中止 | §2.6, S7 |
| P1-9 桶删除 apply 语义 | `buckets.deleting` 标志 + active-writer 归零 + 两站点 quiesce 后查空；命令只删行；各站点目录原子 rename 为 tombstone；非空回滚 flag + 409；失败 intent 重试 | §2.3/§2.4, S3/S6 |
| P1-10 openraft 适配/尺寸 | `features=["serde","storage-v2"]`（v4 修正）；v2 traits 直连、无 `Adaptor`、原生 async fn；module base64；append/vote 32 MiB；快照分块 | §2.3, S5 |
| P1-11 0004 不可重放 | 表 `IF NOT EXISTS`、触发器先 `DROP IF EXISTS`、ctx `INSERT OR IGNORE`；连续 connect 测试 | §2.3, S0 |
| N1 tee 临时文件位置 | `staging_dir/tee/<uuid>.plain`（版本目录外）；成功/失败/超时/取消均清理 | S1 |
| N2 超时歧义可能远端可见 | `oss_sync_ambiguous_total`；S3 对超时做一次 HEAD 探测，能定位则补偿；文档 | S2/S3 |
| N3 转发超时歧义 | 文档：命令可能已提交；重试按 Conflict/NotFound 幂等；测试 | S8 |
| N4 follower 授权陈旧 | 安全章节明示：撤销在 follower 应用前仍可认证 | S8/S9 |
| N5 Raft 下对象 worker | 只停 M7 changelog worker；async 对象 repl worker 保留；回归 e2e | S6/S9 |
| N6 删 peer 与 sync 桶 | 有 sync 桶时 `DELETE /sites` 409；force-recovery runbook | S7/S9 |
| N7 sync 桶 Transition | sync 桶 lifecycle 仅允许 Expiration；Transition 400（避免两站点独立 tier） | S2 |
| N8 identity 字段来源 | `advertise_endpoint` 配置；`admin_username` 由 `OSS_ADMIN_USER` 首启写入 `schema_meta`；缺省 null | S0/S7 |
| N9 快照/session AK | 安装后删除 user 已不存在的 session AK；保留 admin 会话；admin 权限行清空并文档化 | S7 |
| N10 guard 加固 | `users` 用 `OLD/NEW.is_admin`；`access_keys` 按 session kind/admin 属主；`permissions` 目标 admin；其余 8 表无例外 | S0 |
| N11 签名头 | `x-oss-site-token` 纳入 SigV4 SignedHeaders（ReplClient 签名）；仅内部路由校验，剥离/错误 → 403；普通 S3 路由不读该头（v3 N2 修正） | S1/S3 |
| N12 ReplClient 超时回归 | 共享 client 不设默认超时；sync 每调用 10s；async worker 自设大超时 | S1 |
| N13 估算/并行 | v1 时为 58 人日、S6 10 d、S7 9 d；v4 重估见 §3；S0 预留 `gate/control/raft/stretch` 字段与 init stub | §3, §8 |
| N14 永久 voter 丢失 | 3-voter 替换 witness/数据站点 + 离线 `raft-force-recover` runbook；禁止手改 `raft.db` | §2.6, S9 |
| N15 节点身份持久化 | `schema_meta` 存 `raft_node_id/bootstrap/initialized`；变更拒绝启动 | S0/S5 |

**v2 矩阵（本轮新增阻塞项）**

| 项 | 处置 | 落点 |
|---|---|---|
| v2 P0-1 stretch 仍跑 M7 异步对象复制 | `SqlReplSink` 增 `site_data_replication: bool`；stretch 为 false，`enqueue_sites` 同时拒绝 sync/stretch 桶；S1 落通用 gate，S4 只翻配置；e2e 断言一次 PUT 每站点恰好 1 个版本、无 `target_kind='site'` 行 | §2.4/§2.5, S1, S4, S9 |
| v2 P0-2 版本映射单向 | 所有 token-auth sync 写与 marker 带 `x-oss-origin-version-id`，接收端**采用该 UUID 作为副本 `Version.version_id`** 并写 `remote_version_id`；发起端响应 id 必须相等；marker UUID 预生成；`sync-reconcile --repair`；双站版本/marker 删除测试 | §2.4, S1–S3, S9 |
| v2 P0-3 写 ack/本地 meta 崩溃留下远端孤儿 | `sync_write_intents` 在远端调用前落盘；状态 `remote_pending/remote_done`（commit 后删行）；启动 + 周期恢复选择**补偿远端**（或本地已提交则清理）；kill -9 ack/commit 边界测试 | §2.4, S0/S1/S3, S9 |
| v2 P0-4 stretch 节点顺序矛盾/远端分片写失败 | 规范顺序按 `node_id` 排序（与 self 无关），两端同一 `nodes` 顺序并派生 `self_idx`；配对交换并校验实际 6 盘/8+4/placement/mesh secret；真实 `mesh_write_shard` 到 peer 测试 | §2.5, S4, S7 |
| v2 P1-1 marker 不能原子携带远端 id | `delete_object_versioned_ext` 增 `DeleteMarkerSpec{marker_uuid, remote_version_id}`；一个 meta 事务写 marker + 映射；服务预生成 UUID | §2.4, S3 |
| v2 P1-2 删除 intent 仅启动恢复/语义错 | `sync_delete_intents` 改 `id` 主键、无空串唯一键；周期 worker（复用 job worker）；`delete_version` 与 `create_marker` 分开恢复；marker 存在则完成本地而非重造；本地失败不重启测试 | §2.4, S3 |
| v2 P1-3 成员操作非 leader-aware | `POST /api/v1/site/raft/membership` 任意节点转发 leader；CLI 从 `/api/v1/site/raft/status` 发现 leader；集合取自 `Raft::membership()`；C 为 leader 的 add/replace/remove 测试 | §2.6, S7 |
| v2 P1-4 B' 替换破坏映射 | B' 以 A 为源按同 id 重推（`sync-reconcile --repair` 可续跑），刷新双侧映射；清理 stale intents；B' 替换 e2e 从两侧删旧对象 | §2.4/§2.6, S3/S7/S9 |
| v2 P1-5 force recovery 非 openraft 操作 | 停全部 → 导出状态机业务行 → **新建空 `raft.db`** → `initialize({N})` 单节点 → 加 learner/改成员；不写“epoch”、不手改旧 `raft.db`；测试单存活节点路径 | §2.6, S7 |
| v2 P1-6 DeleteBucket 与 PUT 竞态 | `buckets.deleting` 控制命令 + 每桶 active-writer 屏障；两站点 quiesce 后再查空；非空回滚 flag + 409；失败保 flag 由 intent 重试；并发 PUT/DeleteBucket 测试；不再宣称“空检查保证” | §2.4, S3/S6 |
| v2 P1-7 stretch 配置/部署缺口 | `[stretch].mesh_secret`；`peer_addr` 规范化（去 `http://`、拒 https/path）；identity 校验真实盘数/EC/placement；stretch 仅允许全新空 store；已有 sync 桶拒绝启动 | §2.5, S0/S4/S7 |
| v2 P1-8 preflight 未覆盖新 P0/出口计数错 | 新增 6a 规范顺序/真实 mesh、6b 抑制异步复制、8a origin-id 双站删除、8b kill -9 写意图、1a leader 成员；出口改为 10 个结果 | §4 |
| v2 P1-9 快照漏 `permissions` 重映射 | 快照逻辑化列表加 `permissions(username,bucket,can_read,can_write,can_delete)`，安装方按 username 重解析 `user_id`；源/目标 id 乱序 + witness owner 0 测试 | §2.3/§5 S7 |
| v2 P1-10 所有权表与 S4 冲突 | S0 落全部 `[stretch]` 配置/校验，S1 落 `replication.rs` 通用 gate，S4 只碰拓扑/`state.rs` 分支；S4 依赖 S0+S1；表内明确 | §8 |
| v2 N1 reconcile 只读 | 默认只读报告；`--repair` 变更为可续跑修复（同 id 重推/匹配 ETag+size/清 legacy），报告 ambiguous | §2.4, S3 |
| v2 N2 DeleteObjects 503 | 明确保持“任一远端失败 → 503，成功键已提交；对象可见性幂等，无 versionId 的键可能再生成 marker”；文档 + `sync_delete_partial_total` + 测试 | §2.4, S3 |
| v2 N3 零字节 stretch | 单站点隔离测试覆盖零字节对象（meta quorum 9 仍失败），文档标明 inline 例外 | §2.5, S4 |
| v2 N4 单站延迟 | 首个远端分片失败即 fail-fast，不等 6×5 s；e2e 记录上限 | §2.5, S4 |
| v2 N5 production_ready 仅建议 | 启动 warning + console banner；`/raft/status` 保留 | §2.6, S8/S9 |
| v2 N6 witness `site_bootstrap` 无条件种子 | `Db::connect` 拆 `connect(seed_site=true)`/`connect_witness(seed_site=false)`；witness identity 用配置；测试无 `site_config` 行 | §2.1/§2.6, S5 |
| v2 N7 估算 57/58 不一致 | v2 时为切片基础 58 + 3 应急 = 61、含 preflight 67；v4 已重估为 **65 + 3 + 7 = 75**（§3），以 v4 表为准 | 头部/§3 |
| v2 N8 陈旧 progress ledger | 本计划 v3 为准；派发前由 lead 更新 `.superpowers/sdd/.../progress.md`（本任务不改计划外文件） | 头部 |
| v2 N9 cutover 导出范围 | `raft-export` 覆盖 11 张复制表（buckets 全列 + 其余 10 表），不只 M8 六表；逐表 diff 进 precheck | §2.6/§5 S7 |
| v2 N10 快照 256 MiB 可能不足 | 默认 `snapshot_max_bytes=512 MiB`（可配，最大 1 GiB）；分块 4 MiB；preflight 测最坏 OLAP；错误给可操作提示 | §2.3/§4, S5 |
| v2 N11 S4/S5 估时紧 | S4 8 d、S5 5 d，另加 3 d 应急；正式接受上浮 | §3 |
| v2 N12 tee 崩溃孤儿 | 启动/周期按 mtime 清理 `staging_dir/tee/*.plain` 孤儿（有 intent 的由恢复处理，无 intent 且超过 1 h 删除） | §2.4, S1/S3 |
| v2 N13 `raft-add-witness` 硬编码 | 读取当前 membership，计算目标集合；替换先加新 node 再移除旧 node | §2.6, S7 |
| v2 N14 M7 `sites` adopt 缺测试 | S7 测 adopt/update、不同 peer 409、无 raft 列旧行补齐 | §5 S7 |

**v3 矩阵（本轮新增阻塞项 + 指定非阻塞项）**

| 项 | 处置 | 落点 |
|---|---|---|
| v3 P0-1 openraft 缺 `storage-v2` | 所有依赖行改 `features=["serde","storage-v2"]`；openraft v2 impl 用原生 `async fn`（**不用 `async-trait`**）、无 `Adaptor`、关闭 `generic-snapshot-data`；S5 入口/出口清单 + workspace 编译 preflight | §1/§2.3, S5, §4 |
| v3 P0-2 stretch 数据面不可启动 | `[stretch].self_addr` + `StretchToml`；`cli.rs` 增 stretch mesh gRPC server 分支；`self_addr` 绑定/规范化；真实 `oss-server` 两站点 e2e。**v4 发现 config/identity 残留（B1/B2），v5 已补齐主 HTTP/`advertise_endpoint` 分离与 identity 字段** | §2.5, S0/S4, §8 |
| v3 P0-3 Batch/console 删除绕过 remote-first | 新增共享 `SyncDeleteService`，S3 service/lifecycle/Batch/console 对象/console 桶删除全部经它；四类外部 caller 在 sync 桶禁止直接 `store.delete_*`，接收端内部路由仅用 local-only 方法（v5 B4）；M7 行为不变 | §2.4, S3/S6, §8 |
| v3 P1-1 openraft 运行时/API 条件 | `[raft]` 增 `install_snapshot_timeout_ms`/WAN heartbeat/election/`max_in_snapshot_log_to_keep=0`/`purge_batch_size`/`write_timeout_ms`；`client_write` 应用层 deadline；`PayloadTooLarge` 拆分；apply 每条命令一个 `R`；`ClientWriteResponse.data` 单个 `R`；`Cursor<Vec<u8>>` 快照接收 | §2.3, S5/S6 |
| v3 P1-2 快照分块 vs JSON body limit | 4 MiB 原始分块 JSON 约 16 MiB；快照路由 `DefaultBodyLimit=32 MiB`（append/vote 同 32 MiB）；preflight/S5 增完整 HTTP build/upload/install 测试 | §2.3, S5, §4 |
| v3 P1-3 permissions 快照键错 | S7 改 `(username,bucket,can_read,can_write,can_delete)`；导出按 username、安装回填 `user_id`；admin 目标映射保留；非 admin/admin/AP/Inventory owner 测试 | §2.3/§2.7, S7 |
| v3 P1-4 写意图恢复自相矛盾 | 统一为 **compensate-only**：本地 meta 不存在 → 远端同 id token 删除；存在 → 只清理 intent/tee；不“从 tee 完成本地提交”；MPU tee 限制显式化 | §2.4, S1/S2/S8, R24 |
| v3 P1-5 DeleteBucket 未分模式/counter 未定义 | 新流程仅 sync 桶 + Raft 模式；M7 保持 `service.rs:804-858` 本地删除/冲突补偿；`Store` 增每桶 active-writer 计数 + deleting 集合，同一锁内 check/increment，RAII 减计数；四类写/删 caller 均取 guard | §2.3/§2.4, S3/S6 |
| v3 P1-6 marker 恢复 HEAD 405 | `ReplClient::head_object_version`（token）；200 或 405/DeleteMarker = 存在→重试 token 删除；404 = 不存在→完成本地；`delete_version`/`create_marker` 同一规则；kill 后 marker 版本测试 | §2.4, S3 |
| v3 P1-7 sync MPU 无明文 tee | MPU Complete 在拼接阶段同时 tee 到 `staging_dir/tee/<uuid>.plain`；gate 用该文件；成功/失败/取消/孤儿清理；tee 缺失时远端调用前 500；kill-9 测试 | §2.4, S1/S2 |
| v3 P1-8 所有权表缺项 | 增 `job_actions.rs`、`lifecycle.rs`/`lifecycle_store.rs`、console 删除 handler、`cli.rs` S4 mesh server；明确 `service.rs`/`sync.rs` 串行顺序 | §8 |
| v3 N2 token 剥离 | 不再依赖 S3 DELETE 头区分；站点删除走独立内部路由 `/api/v1/site/delete-object`（token 必填、只本地执行），普通 S3 DELETE 永远走客户端 remote-first；剥离/错误 token 在内部路由 403 | §2.4, S3 |
| v3 N5 C' 替换缺 add_learner | §2.6 改为：读当前 voters → `add_learner(N, blocking)` → 追平 → `change_membership((voters-{3})∪{N})` → 移除旧 3 | §2.6, S7 |
| v3 N6 死 `shard%2` 回退 | 从 §0.3/§2.5/S4/preflight 全部删除；只保留 `placement_v2` 的 12 槽证明 | §0.3/§2.5, S4, §4 |
| v3 N7 重复 marker | 明确 at-least-once：无 versionId 的重试可能产生多个 delete marker；保证对象不可见与同 id 版本删除，不保证版本列表逐项相同 | §2.4/§9, S9 |
| v3 N13 witness 导出源 | force recovery 优先从最全数据站点导出；仅 C 存活时 `raft-export --witness` 读 `state_db`（仅控制面），对象从备份恢复；S9 runbook 写明 | §2.6, S9 |

**v4 矩阵（本轮 4 个 P1 契约修正，v5 落点）**

| 项 | 处置 | 落点 |
|---|---|---|
| B1 主 HTTP 与 mesh gRPC 同端口 | 样例增 `listen`，`advertise_endpoint` 改主 HTTP `a:9000`，`self_addr=a:9100`；`stretch_validate` 拒绝 `advertise_endpoint` host:port == `self_addr`；S4/S9 e2e 断言 B 经 A 的 `advertise_endpoint` 访问 `/api/v1/raft/status` | §2.5, S0/S4, S9 |
| B2 stretch identity 缺字段 | identity 增 `stretch {node_id, self_addr, peer_node_id, drive_count, ec, placement, mesh_secret_fingerprint}`；S4/S7 接线；六类字段不匹配测试 | §2.5, S4/S7 |
| B3 quiesce 无 bucket/无等待契约 | `POST /api/v1/site/bucket-quiesce {bucket}` 同锁置 deleting 并等对端 active-writer 归零后 200；本地/远端均 30 s 上限，超时 409/503 + 回滚 deleting；in-flight PUT barrier/SIGSTOP 测试 | §2.3/§2.4, S3/S6, S9 |
| B4 内部接收端无 local-only 路径 | `SyncDeleteService` 增 `delete_version_local`/`create_marker_local`/`delete_bucket_local`；仅 token 路由使用；直接 store 禁令仅约束四类外部 caller；no-loop + 四类 caller 测试 | §2.4, S3 |

### 0.3 ③ 可实现性与显式设计修订

可实现，且不需要新写一套 EC 算法：2 节点 × 6 盘 = 12 槽，正好是现有 `[cluster]` 的槽位模型；
EC 8+4、`placement_v2`、`Q_s=data+1=9`、meta 镜像/两阶段提交都已存在。需要新增的是 **stretch
预设校验**：恰好 2 节点 × 6 盘、跳过 `drive_count <= parity` 的单节点上限（该上限只为“丢节点
可重建”，而 stretch 明确不承诺站点故障）、强制 `inline_threshold=0`、站点感知放置证明
（每站点恰好 6 片，6 < data=8）、`[stretch].self_addr` 与 mesh server 启动、以及 `[site]` 与
`[cluster]` 互斥的例外。`placement_v2 = (hash+shard)%12` 对 shard 0..11 是双射，必为 6/6；
preflight 只验证该结论，**不再保留 `shard%2` 回退**（该回退在当前 `place` 中无法表达）。

本计划有**两处显式设计修订/收窄**，不再宣称“零设计变更”：
1. stretch 模式引入**两站点失败域**（设计 §5.3 的扁平拓扑只适用于普通 `[cluster]`）；
   这是验收 ③ 的直接要求。
2. stretch 下**禁止 sync 桶**（设计 §7.3 / feature spec §10.2 曾写 sync 支持 stretch EC）。
   这是为避免“跨站 EC 分片 + 整对象 sync 复制”双重语义的显式收窄；S9 同步更新设计/规格文档，
   发布前由产品负责人确认该范围。v2 的四个 P0 在 v3 已落点；v3 的三个 P0 在 v4 核心落点，
   v4 复核发现的 B1/B2（stretch 配置/identity）、B3/B4（quiesce/local-only）在 v5 收口。

## 1. 钉子与全局约束

- **3 voters / quorum 2**：A、B 为数据站点，C 为 witness（只跑控制面，不存对象）。
  任一单站点（A/B/C）宕机，控制面仍可写；A+B 同时宕机 → 503。sync 写仍要求 A/B 都在
  （数据面 peer 不可用 → 503），但控制面不被拖死。**无自动降级、无自动摘除 voter**。
- **不做线性一致 follower 读**：follower 只读本地已应用状态；`/raft/status` 标 `lag`/`stale`；
  `wait_applied(index)` 仅测试/转发读己之写，不进 S3/console 读路径。
- **管理员站点本地**：`users.is_admin=1`、管理员长期 AK、`kind='session'` AK 不复制；
  两站点 admin 用户名必须相同；`admin_username` 首启写入 `schema_meta` 并由 identity 暴露。
- **TLS 不做**：v1 只支持 `http://`；sync/ReplClient/TonicMesh/raft RPC 均为明文，要求私有网络。
- **不复用 M7 changelog**：Raft RPC 走 `/api/v1/site/raft/*`；`/api/v1/site/changelog` 在 Raft 模式 409；
  `site_*` 触发器启动时 drop；`site` worker 不 spawn。对象 repl worker 保留（async 桶）。
- **不复制 `buckets.set_id`**：`[raft]` 要求 `[site]`；`[site]` 与 `[cluster]` 仍互斥，
  `[stretch]` 是唯一例外；Raft 模式下 `set_id` 恒 0，apply/快照不写该列。
- **Raft 模式禁止 apply 外控制面 SQL 写**：`schema_meta('raft_mode')` + `raft_apply_ctx` +
  守卫触发器 fail-stop；管理员 bootstrap / session AK / 本地表例外显式列举。
- **sync 无 outbox、无降级**：sync 写/删不写 `repl_outbox` 的 site 行；远端失败即 503；
  不转 async、不后台重试；`sync_write_intents`/`sync_delete_intents` 只是同步崩溃恢复的
  本地意图表，不是发送队列。
- **sync 单数据 peer**：每个数据站点的 `sites` 表只有 1 行数据 peer（A 上是 B、B 上是 A），
  witness C 不在 `sites`；0 或 >1 行 → sync 写/删 503；sync 桶创建要求恰好 1 行。
- **sync 范围（本轮）**：PUT、MPU Complete、Copy、DeleteObject（marker）、DeleteObject(version)、
  DeleteObjects、生命周期 Expiration、DeleteBucket（deleting + quiesce 后两站点空）。
  Tagging API、Batch tag 501；
  SSE-C 400；object lock 409；lifecycle Transition 400。
- **sync 版本 id 双向对称**：token-auth sync 写的 `x-oss-origin-version-id` = 发起端预生成
  UUID；接收端把它作为副本的 `Version.version_id` 并写 `Version.remote_version_id`；
  因此任意一端都能用同一个 id 删版本/marker。非 token 的 M6/M7 REPLICA 不采用此规则。
- **stretch 钉子**：恰 2 数据站点 × 6 盘；EC 8+4；`inline_threshold=0`；`Q_s=9`；meta 镜像 12 槽；
  任一次写需 9 槽；每站点 6 片，6 < 8，读写都需两站点；**不承诺单站点故障容灾**；
  `[stretch].self_addr` 必填并由 `cli.rs` 启动本站 mesh gRPC server（`xl_client::serve`）；
  `[stretch]` 下禁止 `replication_mode=sync`，且**关闭 M7 异步站点对象复制**
  （`SqlReplSink.site_data_replication=false`；不产生 `target_kind='site'` 行）；
  `[stretch]` 只允许全新空存储（无 bucket 行/对象目录），禁止在既有部署上原地开启。
- **stretch 节点顺序**：`[stretch]` 的两条 `NodeSpec` 一律按 `node_id` 升序排列，与“谁是
  self”无关；两端 `self_idx` 由同一顺序派生；配对时校验对端 id/盘数/EC/placement/mesh secret。
- **不新增依赖**：仅 `openraft = { version = "=0.9.25", features = ["serde", "storage-v2"] }`；
  base64 已在 workspace（`Cargo.toml:43`）。openraft v2 impl 用原生 `async fn`，不用
  `async-trait`，不加 `Adaptor`，关闭 `generic-snapshot-data`（见 S5 入口清单）。
- **删除入口唯一**：S3 service、lifecycle、Batch Delete、console 对象/桶删除在 sync 桶上
  全部经 `SyncDeleteService`；站点间删除走独立 token 路由（只调 local-only 方法，无回环），
  普通 S3 DELETE 永不携带站点 token。
- **不做**：TLS、3+ 数据站点、stretch 站点容灾、Raft 多 group、跨站 ACL 统一、M8 数据面强一致、
  M6 规则改造、sync exactly-once、tag 变更复制。

## 2. 架构裁决

### 2.1 模式矩阵与启动顺序

| 配置 | 数据面 | 控制面 | 说明 |
|---|---|---|---|
| 无 `site/cluster/stretch` | 单节点 | 本地 SQLite | M1/M2 现状 |
| `[site]` | M7 站点对（async） | 本地 SQLite | M7 现状 |
| `[cluster]` | 扁平多节点 EC | 本地 SQLite | M3/M4 现状 |
| `[site]` + `[raft]` | M7 站点对 | 3-voter Raft（A/B/C） | sync 桶生产形态 |
| `[site]` + `[stretch]` + `[raft]` | 2×6 stretch EC | 3-voter Raft（A/B/C） | ③ 形态 |
| `[site]` + `[cluster]` / `[stretch]` + `[cluster]` / `[stretch]` 无 `[raft]` | — | — | 启动失败 |

- `[raft]` 必须有 `[site]`；`[stretch]` 必须有 `[site]` + `[raft]`；`[cluster]` 与两者互斥
  （改 `config.rs:353-375` 的 `site_validate`，新增 `stretch_validate`）。
- `[stretch]` 不是新 EC 实现，而是现有 `ClusterSets` 的 2×6 预设：`NodeSpec` 两条、各 6 盘、
  `placement_v2=true`、`ClusterConfig.stretch=true`；复用 `TonicMesh`、`write_meta_cluster`、
  `read_meta_cluster`、`Q_s`（`drives.rs:59,320`；`store.rs:1000,753`）。
- witness C 不在 `[stretch]` 拓扑、不在 `sites`、不存对象；只应用控制命令。

**启动顺序（`state.rs::load` 240-489）**
1. `Db::connect`（主库 schema/ensure_columns/SCHEMA_M9）；
2. `apply_site_config`；
3. **无条件** `db.prepare_raft_mode(cfg.raft.is_some())`：无 `[raft]` + `raft_mode=on` → 拒绝启动
   （恢复提示）；无 `[raft]` + off → drop 全部 `raft_guard_*`（M7 控制写不受影响）；
   有 `[raft]` → 写 on、drop 全部 `site_*` 触发器、保留/重建 `raft_guard_*`、复位 `apply_ctx`；
4. `Store::open`（含 stretch 校验）；
5. AP/OLAP cache、bucket cleanup sink；
6. `SiteSyncGate` + `Raft`（注入 side-effect sink）→ `raft::spawn` 公开；
7. `reconcile_bucket_dirs`（row→dir；无 row 的空目录删/非空目录移 tombstone）；
8. `sync::recover_write_intents` + `sync::recover_delete_intents`（依赖 Store/tee/token 客户端，
   在 serve 前完成首轮；随后由周期 worker 接管）+ 孤儿 tee 清理（>1 h 且无 intent）；
9. 启动回填/`[[groups]]`/admin bootstrap（Raft 下规则见 S6）；
10. serve。周期 intent worker 与 job worker 同生命周期；不阻塞 serve。
witness 走 `oss-server witness --config` 独立路径：不构造 `Store`/S3，只
`Db::connect_witness(state_db)`（`seed_site=false`，不种 `site_config`）+ `raft.db` + 控制面 SM +
最小路由（`/health`、`/api/v1/site/identity`、`/api/v1/site/raft/*`）。

### 2.2 复制实体（Raft v1）

| 实体 | 键 | 复制列/说明 |
|---|---|---|
| `users` | `username` | 仅 `is_admin=0`；`password_hash` 在 leader 算 |
| `access_keys` | `access_key` | 仅非管理员 + `kind='long-term'`；secret 在 leader 封装/明文 |
| `permissions` | `(username,bucket)` | 全量替换 |
| `buckets` | `name` | `owner_username`、`creation_date`、`versioning`、`object_lock`、`object_lock_default_mode/days`、`default_encryption`、`replication_mode`、`deleting`；不含 `set_id` |
| `bucket_policies` | `bucket` | 原始 `policy_json` 字节 |
| `lifecycle_rules` | `(bucket,prefix)` | 全量规则行（稳定 `rule_id`）；apply 按 bucket 先删后插 |
| `event_configs` | `bucket` | 规范化 `config_json` 字节 |
| `replication_rules` | `bucket` | 规范化 `rules_json` 字节 |
| `access_points` | `name` | 全字段；owner 用 username |
| `olap_configs` | `access_point` | 全字段；`module` base64 传输 |
| `inventory_configs` | `(bucket,id)` | 全字段；owner 用 username |

本地实体（不进命令/快照）：`site_config`、`sites`、`site_changelog`、`site_entity_clock`、
`site_cursors`、`site_apply_dead`、`apply_ctx`、审计表、`jobs`/`job_tasks`、`uploads`、
`instant_index`、`inventory_runs`、`tier_restores`、`event_outbox`、`repl_outbox`、
`sync_write_intents`、`sync_delete_intents`、`kind='session'` AK、管理员用户/AK、
管理员 permissions 行、`buckets.set_id`、vault 材料、`raft.db`/witness `state_db`。

### 2.3 控制写路由、确定性与守卫

- **路由**：`ControlPlane::{Local,Raft}`。console 控制写全部走 `state.control.*`；S3 桶配置写经
  `BucketControlSink`（M7 下 `None`，走现有 `SqlAcl`，零行为变化）。Raft 分支 follower 转发
  `POST /api/v1/site/raft/write`（cluster token），无 leader → 503 + `x-oss-leader-site`；
  leader `client_write`；follower `wait_applied(index)`（有界）后返回。
- **成员操作 leader-aware**：新增 `POST /api/v1/site/raft/membership`（cluster token），
  任意节点接受后转发 leader；请求 `{op:AddLearner{node_id,endpoint}|Change{voters}|Remove{node_id}}`；
  当前集合取自 `Raft::membership()`，不硬编码 `{A,B,C}`；CLI 先 `/api/v1/site/raft/status` 找 leader，
  遇 `ForwardToLeader` 重试一次。`POST /sites`/`DELETE /sites`/`raft-add-witness` 全走此入口。
- **确定性**：命令携带所有不可重放值（Argon2 hash、vault 封装 secret、时间戳、AK/secret、
  lifecycle 行、event/replication JSON、inventory id/UUID）。apply 只做 SQL，不调用
  `hash_password`/`seal`/`now()`/`Uuid::new_v4()`/`pick_least_loaded_set`。
- **代理键**：命令携带 username/自然键；apply 本地解析 `user_id`。`buckets.owner_id` 管理员写 NULL；
  `access_points.owner_id`/`inventory_configs.owner_id`（NOT NULL）在数据站点解析为本地 id
  （管理员解析为本地管理员 id，`inventory.rs:257` 运行期需要）。witness 没有管理员用户，
  对 admin 属主写 `0` 占位；数据站点若本地管理员尚未 bootstrap 也先写 0，bootstrap 后启动修复
  按 `users.is_admin=1` 重写为本地 id；witness 不运行 inventory/console，0 不参与业务。
  验收按自然键归一化，代理键数值可不同。
- **apply 失败 = fail-stop**：返回 StorageError 并退出；不跳过命令。
- **物理副作用后置**：apply 事务只写 SQL；桶目录 create/delete、AP/OLAP cache 失效在 commit 后
  执行；每个站点执行。失败重试 3 次并日志；启动 `reconcile_bucket_dirs` 兜底。
- **隐藏写**：旧 hash 升级、SK 重封、启动桶回填在 Raft 模式抑制（读可用，写不落库）。
- **`[[groups]]`**：`[raft] + [[groups]]` 启动即拒（v1 裁决）；`upsert_bucket_set` 跳过（set_id=0）。
- **create 写入**：`db.rs::insert_bucket`/`access.rs::record_bucket_owner_set`/
  `console.rs::upsert_bucket_owner_set` 统一为全列 writer（`versioning`、`replication_mode`、
  `deleting`、lock 默认值、`default_encryption`）；ON CONFLICT 全列更新但**不写 set_id**；
  重建时 mode 变化 → 409。guard ABORT/行写失败在 create 路径映射 5xx/409，禁止 204
  （修 `console.rs:1717-1749` 的 warn+204）。
- **桶删除 quiesce（仅 sync 桶 + Raft 模式）**：`SetBucketDeleting{name,bool}` control
  命令写 `buckets.deleting`；sync 写 gate 见 `deleting=1` → 503。删除流程先在两站点
  quiesce，再查空；非空回滚 flag + 409；失败保留 flag 由
  `sync_delete_intents(op='delete_bucket')` 周期重试。空检查本身不构成串行化保证。
  **M7 async 桶不走此流程**，保持 `service.rs:804-858` 的本地删除 + 冲突补偿，行为零变化。
  **quiesce 契约（B3）**：`POST /api/v1/site/bucket-quiesce {bucket}`；接收端在同一锁内
  置 deleting，然后**等待本站 active-writer 计数归零后才返回 200**（只拒绝 `deleting=1`
  之后的新写不够——已过 `begin_write` 的 PUT/MPU 必须被计入并等待）。等待上限 **30 s**：
  超时由**接收端先清掉自己的 in-memory deleting** 再返回 409/503；调用端中止删除并回滚
  本地 deleting + `SetBucketDeleting{false}`（两端都不遗留锁死状态）；本地
  `begin_delete(bucket)` 用同一 30 s 上限和同一回滚路径。`bucket-empty` 只在 quiesce
  200 后调用。
  **active-writer 定义**：`Store` 增 `active_writers: Mutex<HashMap<String,usize>>` 与
  `deleting: Mutex<HashSet<String>>`；`begin_write(bucket)` 在同一把锁内检查 deleting 并
  自增，返回 RAII `WriterGuard`（Drop 时减计数）；`begin_delete(bucket)` 在同一锁内置
  deleting，再 poll 计数归零（有界）。PUT/MPU/Copy/Delete 及 Batch/console 路径都在入口
  取 guard；M7 从不置 deleting，计数只增加少量内存操作，不改变语义。
- **StoredMembership**：`raft_applied(id=1, applied_log_id TEXT, membership_json TEXT)`；
  `applied_state()` 返回 `(Option<LogId>, StoredMembership)`；apply 与快照安装同事务更新；
  快照 meta 同带。不得只存 index/term。
- **命令编码/尺寸**：`ControlCommand` serde JSON；`olap_configs.module` 以 base64 `String`
  传输（16 MiB BLOB → ~21.4 MiB JSON）；单命令上限 24 MiB，超限 4xx；
  append/vote 路由 `DefaultBodyLimit::max(32 MiB)`；快照按 4 MiB 原始分块，serde JSON
  的 `Vec<u8>` 数组约 **16 MiB/块**，故快照路由同样 `DefaultBodyLimit::max(32 MiB)`
  （不是 4 MiB）；总上限 `[raft].snapshot_max_bytes` 默认 **512 MiB**，可配上限 1 GiB。
- **快照自然键**：11 表快照全部逻辑化；`permissions` 按
  `(username,bucket,can_read,can_write,can_delete)` 记录，安装方重新解析本地 `user_id`；
  `users` 按 username，`access_keys` 按 access_key/username，buckets/AP/Inventory owner
  按 username；witness 管理员 owner 保留 0 占位。
- **openraft**：`openraft = { version="=0.9.25", features=["serde","storage-v2"] }`；
  实现 v2 `RaftLogStorage` + `RaftStateMachine` 直传 `Raft::new`，**不用 `Adaptor`**
  （`Adaptor` 只适配旧 `RaftStorage`）；v2 impl 用原生 `async fn`（不用 `async-trait`）；
  关闭 `generic-snapshot-data`；`begin_receiving_snapshot` 接收 `Cursor<Vec<u8>>`。
- **openraft 运行时**：`[raft]` 增 `install_snapshot_timeout_ms`（WAN 默认 300_000）、
  `heartbeat_interval_ms`（默认 1_000）、`election_timeout_ms`（默认 5_000）、
  `max_in_snapshot_log_to_keep=0`、`purge_batch_size`（默认 1_024）、`write_timeout_ms`
  （默认 10_000）、`snapshot_max_chunk_size`（原始字节，默认 4 MiB，上限 4 MiB）。每次 `client_write` 外层 `tokio::time::timeout`，超时 503（保证 A+B
  宕机在 deadline 内返回）；append 遇 `RPCError::PayloadTooLarge` 按字节上限拆批，单条
  命令 >24 MiB 在入口 4xx（不可拆）；`apply()` 每条 entry 只返回一个 `R`，
  `ClientWriteResponse.data` 是单个 `R` 而非 `Vec`。
- **守卫**：`0004_m9.sql` 对 11 张复制表各建 INSERT/UPDATE/DELETE guard；先判
  `raft_apply_ctx.active=1`，否则按表放行例外，其余
  `RAISE(ABORT,'raft: control write outside apply')`：
  - `users`：UPDATE/DELETE 用 `OLD.is_admin=1`；INSERT 用 `NEW.is_admin=1`（仅 bootstrap 管理员）；
  - `access_keys`：`kind='session'`，或属主 `username` 是本地 admin 的长期 AK；
  - `permissions`：目标 `username` 是本地 admin（管理员权限本地化）；
  - 其余 8 表无例外（apply 外一律 ABORT）。
- **节点身份持久化**：`schema_meta` 存 `raft_node_id`、`raft_bootstrap`、`raft_initialized`、
  `admin_username`；重启值变化 → 拒绝启动（防第二集群/非投票节点）。

### 2.4 sync 数据面

**统一版本 id（v3 核心裁决）**
- token-auth sync 写（PUT/MPU Complete/Copy）在 staging 前预生成本地版本 UUID；远端 PUT 带
  `x-oss-origin-version-id: <uuid>`（纳入 SigV4 SignedHeaders）。接收端在 sync 桶 + 合法
  `x-oss-site-token` 下**采用该 UUID 作为副本 `Version.version_id`**，并在同一 meta 写
  `Version.remote_version_id=Some(uuid)`。
- 远端响应 `x-amz-version-id` 必须等于该 UUID；不等 → 502 `sync version id mismatch`，
  本地不提交，intent 留待人工/repair。发起端同样记录 `remote_version_id=<uuid>`。
- 由此双向对称：A 的版本 id = B 的版本 id；从任一端 `DELETE ?versionId=<id>` 都能在对端找到并删除；
  marker 同理。不需要反向映射表。非 token 的 M6/M7 REPLICA 保持旧行为，不采用此规则。
- 接收端幂等：同 key 已存在该 version id 且 size/ETag 相同 → 返回既有版本，不重复落盘；
  不同 → 409。这样写意图恢复/repair 重推可安全重试。
- store 契约：`put_object_stream_with_meta` 与 MPU complete 增 `forced_version_id: Option<Uuid>`；
  marker 创建增 `DeleteMarkerSpec { marker_uuid, remote_version_id }`（`version.rs:159-235`）；
  三者在 meta 写入时一次落 `version_id` + `remote_version_id`。

**写顺序（PUT/MPU/Copy）**
1. staging 完成（不可见，无 `xl.meta`）；sync 时 tee 明文到 `staging_dir/tee/<uuid>.plain`
   （版本目录外；成功/失败/超时/取消均清理）。**MPU Complete 在拼接 part 流进
   `stage_payload_multi` 的同时写同一 tee 文件**，否则 `GateRequest.plaintext` 无文件可读；
   tee 写失败在远端调用前 500。tee 与 staging 都在 intent 前完成。
2. **写意图先落盘**：`sync_write_intents(id UUID, bucket,key,local_version_id,remote_version_id,
   tee_path, commit_json, state, created_at, updated_at)`；`remote_version_id` 预期等于本地 id；
   `commit_json` 记录 content-type/user-metadata/tags/storage-class/checksum/SSE/lock/MPU etag/size。
   `state='remote_pending'`；intent 插入使用 `synchronous=FULL`，先于任何远端调用。
   因为远端 id 已知等于本地 id，即使崩溃发生在 ack 与 intent 更新之间也无需 HEAD 猜测。
3. `gate.before_commit`：远端 token PUT（`x-oss-site-token` + REPLICA + origin id），要求 2xx
   且响应 id == local id；更新 intent 为 `remote_done`（仍在本事务外，允许崩溃窗口）。
4. 本地 `Version`/meta 提交（使用 forced id + remote id），`replication_status=COMPLETED`；
   删除 intent + tee。`ObjectInfo` 透传 remote id。
5. 失败/崩溃恢复（启动 + 周期 worker）：
   - 远端失败 → intent 删除、staging/tee 清理、503，无本地 meta。
   - 本地提交失败（可捕获）→ 立即 token DELETE `?versionId=<local id>` 补偿；intent 保留到
     补偿成功/404，然后清理；503。
   - kill -9 在远端 ack 与本地 meta 之间 → 重启后按 local id 查本地 meta：不存在则
     **补偿远端**（token DELETE；404 视为成功），不重推、不创建第二版本；存在则只清理
     intent/tee。远端暂时不可达 → intent 留 `remote_done`，周期重试；期间远端该版本可见，
     但客户端从未收到 200。
   - 选择“补偿”而非“从 tee 续提交”：避免 MPU etag 重建与半写 meta；客户端重试即可。
   - **MPU 限制**：tee 是 Complete 时一次写出的拼接明文，不能从已存 part 安全重建
     ETag/元数据；因此恢复绝不“从 tee 续提交 MPU”，只按同 id 补偿远端。tee 文件权限
     0600，成功/失败/取消即删；无 intent 的孤儿超过 1 h 清理。
- 本地耐久性按现有 M2b：staging 已要求 `data+1` 落地；meta `prepared >= data+1`，首个 flip 即可见，
  其余由 heal 修复。**不新增 `flipped >= data+1` / 不写 meta / `QuorumUnmet` 分支**（P0-5）。
- sync 数据面用本站 `[ec]`（默认 8+4，盘数受 `Store::open` 校验）；跨站是对象级复制，不是
  跨站 EC 分片；`[stretch]` 下禁止 sync（且关闭异步站点复制，见 §2.5）。
- `Error::Unavailable` → 503；`Error::Unsupported` 仅用于明确的 501 能力边界。
- 大小：`[sync].max_object_bytes` 默认 64 MiB，staging 前 400；超时：`[sync].timeout_ms` 默认
  10 s，**仅 sync 调用**；共享 `ReplClient` 不设默认超时（async worker 不回归）。N12 的
  有效包络：64 MiB 在 10 s 内需 ≥52 Mbps 上行；部署低于该带宽时必须下调
  `max_object_bytes` 或提高 `timeout_ms`，文档写明（默认值面向小对象/合规）。

- 元数据：content-type/user-metadata/tags/storage-class 复制；远端 REPLICA 摄入保留
  tags/storage_class（token 验证的 sync replica）；checksum 远端重算；SSE-C 400；
  SSE-S3/KMS/默认加密目标本地决定；object lock 409；Tagging API/Batch tag 501。
- 版本规则：sync 创建即 `versioning=Enabled`；`Suspended` 409；lifecycle 只允许 Expiration，
  Transition 400；`replication_mode` 创建后不可变。
- 模式：sync 创建要求 `[raft]` + `RAFT_READY` + 恰好 1 个数据 peer；无 M7 create barrier。
- 缓存：mode 10 s TTL + 创建/删除/apply 失效。
- 孤儿 tee：启动/周期扫描 `staging_dir/tee/*.plain`，有 intent 的交给 intent worker；
  无 intent 且 mtime > 1 h 删除；不删除正在写的文件（按 intent/活跃计数保护）。

**remote-first 删除（本轮）**
- **唯一入口 `SyncDeleteService`**（`oss-server/src/sync.rs` 或 `sync/delete.rs`）：S3
  `delete_object`/`DeleteObjects`、lifecycle Expiration、Batch Delete、console 对象/桶删除
  全部经它。sync 桶由它执行 remote-first；async/M7 桶委托现有 store 路径，行为不变。
  **直接 store 删除禁令只约束四类外部 caller**（S3 service、lifecycle、Batch、console）：
  它们在 sync 桶上不得直接 `store.delete_*`；接收端内部路由是唯一例外（见下）。
- **local-only API（B4）**：`SyncDeleteService::{delete_version_local, create_marker_local,
  delete_bucket_local}`（或同一服务的 `LocalOnly` 模式）。语义是“只做本地 store/meta 删除
  或本地 marker，**永不发起 ReplClient/站点调用、不写 remote_pending intent**”；只允许
  token 认证的接收端内部路由使用，
  且该 handler 不得调 `delete_version`/`delete_object`/`delete_key_all_versions`/
  `delete_bucket` 这些 remote-first 方法，否则会 A→B→A 回环。
- **站点间删除走独立内部路由**，不复用 S3 DELETE 的 token 语义：`POST
  /api/v1/site/delete-object`（body `{bucket,key,version_id?,marker_id?}`，token 必填，
  JWT except，只调 local-only 操作，永不转发）。普通 S3 `DELETE` 永不携带站点 token，
  永远走客户端 remote-first；因此代理剥离 `x-oss-site-token` 不会把站点请求降级成客户端
  路径（N2）。ReplClient 的 `delete_object_version`/`delete_marker` 调该路由。
- **Batch Delete**：`job_actions.rs:339-347` 的每个目标版本/对象改调
  `SyncDeleteService::delete_version`/`delete_object`；sync 桶任一远端失败 → 503/job error，
  不直接 `store.delete_object_versioned`。async 桶保持原逻辑。
- **console 对象删除**：`console.rs:1909-1922` 的硬删除改为
  `SyncDeleteService::delete_key_all_versions`：先列版本，逐版本 remote-first 删除
  （marker 按同 id），全部远端 ack 后才允许本地硬删；async 桶保持原 `store.delete_object`。
- **console 桶删除**：`console.rs:1759-1778` 走 `SyncDeleteService::delete_bucket`（见下）；
  async/M7 桶保持现有 `store.delete_bucket`。
- **版本删除**：本地版本 id 与远端 id 相同（§“统一版本 id”）→ 内部路由
  `POST /api/v1/site/delete-object {version_id}`；远端 2xx/404 视为成功；再本地删版本；本地失败留
  intent 周期重试。
- **DeleteObject（无 versionId）**：service 预生成 marker UUID `m`，先写
  `sync_delete_intents(op='create_marker', marker_version_id=m, remote_version_id=m)`；
  内部路由 `POST /api/v1/site/delete-object {marker_id: m}`，接收端强制创建 marker `m` 并返回
  `x-amz-delete-marker` + id `m`；再本地创建 marker `m`（同一 meta 写 remote id=m）；
  本地失败由 worker 幂等完成本地 marker，不重造远端 marker。**重试契约 at-least-once**：
  同一次 DeleteObject 若在远端 ack 后重试且未复用 `m`，会再生成一个 marker；对象不可见性
  成立，但 `ListObjectVersions` 可能出现多个 delete marker（N7，明确文档化）。
- **DeleteObjects**：逐键 remote-first；任一远端失败 → 503（成功的键已提交；重试在对象
  可见性层面幂等，但无 versionId 的键可能再生成一个 marker）；指标
  `sync_delete_partial_total`；文档明示这是本实现的契约（不是逐键 200+Error），且
  “幂等”仅指对象不可见/同 id 版本删除，不指 marker 列表逐项相同（N7）。
- **生命周期 Expiration**：`lifecycle.rs`/`lifecycle_store.rs` 的 `sweep_once` 调同一
  `SyncDeleteService`；远端失败记 job error，下一轮重试；不本地直删。两站点 worker 可能各
  产生 marker（**不同 id/时间戳**），这是 at-least-once 语义；只保证对象不可见、版本可按
  同 id 删除，不保证两站点版本列表逐项相同（替代“两边一致”的说法）。
- **DeleteBucket（仅 sync 桶 + Raft 模式）**：先置 `buckets.deleting=1`（S6 起为
  `SetBucketDeleting` 复制命令）→ 本站 `Store::begin_delete` 置 deleting 并等 active-writer
  计数归零（**30 s 上限**，超时回滚 `deleting=0` + 409/503）→ 对端 token
  `POST /api/v1/site/bucket-quiesce {bucket}`（接收端同一锁置 deleting、等本站计数归零后
  才 200；**30 s 上限**，超时 409/503，调用端回滚）→ 两站点
  `GET /api/v1/site/bucket-empty`；非空回写 `deleting=0` + 409；空则内部路由
  `POST /api/v1/site/delete-bucket`（token）→ 本地删；失败保留 deleting 与
  `delete_bucket` intent 周期重试，API 503。删除中的 PUT/MPU/Copy/
  Delete → 503。**M7 async 桶完全不进此流程**，仍走 `service.rs:804-858` 的本地删除与
  冲突补偿（P1-5）。
- **intent 表** `sync_delete_intents(id TEXT PRIMARY KEY, bucket,key,op,local_version_id,
  remote_version_id,marker_version_id,state,created_at,updated_at)`：每操作一行 UUID，不再用
  空串版本做唯一键；远端调用前 `remote_pending`，ack 后 `remote_done`，本地成功后删行。
- **周期恢复**（job worker，默认 30 s；启动先跑一轮）：新增
  `ReplClient::head_object_version(bucket,key,version_id)`（token，返回 200/404/405）。
  `delete_version` 按 `remote_pending` HEAD：**200 或 405/DeleteMarker = 远端版本/marker
  仍存在 → 重试内部路由删除；404 = 已不存在 → 本地删**；`remote_done` 直接本地删。
  `create_marker` 同规则：200/405 视为远端 marker 已在，只补本地 marker；404 重试创建。
  `delete_bucket` 重新 quiesce + 空检查 + 两端删除。本地瞬时失败不再依赖重启。
- **token/签名**：内部站点路由（`/api/v1/site/delete-object`、`/api/v1/site/delete-bucket`、
  `/api/v1/site/bucket-quiesce {bucket}`、`/api/v1/site/bucket-empty`）的 `x-oss-site-token` 值取
  `sites.peer_site_token`，与 `site_config.site_token` 常量时间比较，并纳入 SigV4
  SignedHeaders（ReplClient 签名）。内部路由缺 token/错误/签名失败一律 403，且**只做本地
  操作、永不转发**；普通 S3 DELETE 不读取该头、永远走客户端 remote-first。因此“代理剥离
  token”只可能让内部路由 403，不会把请求降级为客户端路径。不再声称 S3 DELETE 能凭剥离
  的头返回 403（N2）。
- **`sync-reconcile`**：默认只读报告；`--repair` 可续跑修复：按本地 version id 查远端同 id →
  写映射；缺失则按 ETag+size 匹配远端 legacy 版本；仍无则从本地明文按 origin id 重推（B'
  替换场景），旧 legacy 远端版本列为 ambiguous，不静默删除。修复后可安全从两侧删版本。
- **版本映射缺失**：M9 前/cutover 对象无 `remote_version_id` → 409
  `InvalidRequest: sync version mapping missing`，提示跑 `sync-reconcile --repair`（不是只读报告）；
  无 versionId 的删除仍可创建 marker。

### 2.5 Stretch EC 2×6

**配置（每个数据站点）**
```toml
listen = "a:9000"            # 主 HTTP 监听；Raft append/vote/status 路由在此
[site]                       # 复用 M7 站点身份；A/B 各一份
name = "A"
region = "r1"
token = "<64 hex>"

[stretch]                    # 与 [cluster] 互斥；必须有 [site]+[raft]
node_id = "A"                # 数据面拓扑 id（非 Raft u64）
self_addr = "a:9100"         # 本站 mesh gRPC 监听地址；host:port 或 http://host:port
peer_node_id = "B"
peer_addr = "b:9100"         # 对端 mesh 地址；启动时规范化
peer_drives = 6
placement = "v2"             # 固定；其他值启动失败
mesh_secret = "<64 hex>"     # A/B 共享；TonicMesh 认证
mesh_timeout_ms = 5000       # TonicMesh 请求超时（WAN 可调）

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 0         # stretch 强制；非 0 启动失败

[[data_dirs]] ... 6 条        # 本站点恰好 6 盘

[raft]
node_id = 1                  # u64；A=1,B=2,C=3
role = "voter"
bootstrap = true             # 仅 A
advertise_endpoint = "http://a:9000"  # 主 HTTP；不能与 self_addr(mesh gRPC) 同端口
cluster_token = "<64 hex>"
db_path = "/var/lib/oss/raft.db"
```
- `self_addr`/`peer_addr` 校验：接受 `host:port` 或 `http://host:port`；启动时去掉
  `http://` 后交给 `TonicMesh`（现有 `new` 会再拼 `http://`）；拒绝 `https://`、path/query、
  空 host/port；两者都必填且规范化后不相等。`self_addr` 是本站 `xl_client::serve` 的绑定
  地址，也是本地 `NodeSpec.addr`；不能留空（`TonicMesh::new` 会为包括 self 在内的每个节点
  建 client，`xl-client/src/lib.rs:223-243`）。
- `stretch_validate` **必须拒绝 `advertise_endpoint` 的 host:port（规范化后）等于
  `self_addr`**：主 HTTP（`listen`）承载 `/api/v1/raft/*`，mesh 是独立的 gRPC 监听；
  同端口会 bind 冲突或把 Raft RPC 发到 gRPC。示例：`listen/advertise_endpoint=a:9000`，
  `self_addr=a:9100`。
- `mesh_secret` 只用于 A/B mesh；不写日志。配对时用该 secret 调对端 mesh health，
  401/超时即拒绝；identity 的 `stretch` 对象为
  `{node_id, self_addr, peer_node_id, drive_count, ec, placement, mesh_secret_fingerprint}`
  （fingerprint 为 SHA-256，不可逆；不暴露 secret 本身），S4/S7 共用同一形状。

**拓扑与放置（规范顺序）**
- 规范顺序：`nodes = [local, peer]` 后按 `node_id` 升序排序（两端得到同一 `[A(6),B(6)]`）；
  `self_idx = nodes.iter().position(|n| n.id == self_id)`；不得使用 `[self,peer]` 顺序。
  这是 P0-4 的修复：槽号由 `nodes` 配置顺序决定（`drives.rs:83-135`），反序会让一半
  分片被远端 `mesh_api.rs:71-89` 判为“不属本节点”。
- `ClusterSets::new_stretch(self_id, nodes_sorted, ec)`：校验 `nodes.len()==2`、每条
  `drive_count==6`、`data==8 && parity==4`、总槽 12、self 在表中；直接调用现有
  `ClusterTopology::build(self_idx, nodes, ec, false)`（`drives.rs:87`；`build` 已有
  `check_per_node_parity: bool`，**不新增参数**；`new` 仍传 `true`），跳过
  `drive_count <= parity`（`drives.rs:112`）；注释说明“stretch 明确不承诺站点故障，
  该上限不适用”。`new_stretch` 同时填 `sets`/`self_ranges=[(0,0,6)]` 与同构 `flat`。
- `state.rs`：`[stretch]` 分支只构造两条 `NodeSpec`（本地 6 盘，`addr=self_addr`；peer 6 盘，
  `addr=peer_addr`）→ 按 node_id 排序 → `new_stretch` →
  `TonicMesh::new_with_timeouts(self_id, mesh_secret, nodes, 1 s, mesh_timeout_ms)`。
  不得在 `state.rs` 另拼 `[self,peer]`。`cli.rs` 负责用 `self_addr` 起 `xl_client::serve`。
- `cli.rs`（S4 交付，不是测试 harness）：`cli.rs:132-197` 目前只在
  `if let Some(cl) = state.cfg.cluster.as_ref()` 下启动 mesh server；增加 `[stretch]` 分支，
  用与 cluster 相同的 host 解析/端口探测逻辑绑定 `self_addr`，调用
  `xl_client::serve(store.clone(), addr, mesh_secret)`，bind 失败即启动失败；`serve` 增
  `mesh_secret` 参数并在 gRPC interceptor 校验 `x-oss-mesh-token`，缺失/错误
  `UNAUTHENTICATED`，`[cluster]` 旧路径保持无 secret。`[cluster]`、`[stretch]` 互斥，
  不会双起；M7 无 mesh server，行为不变。
- `placement_v2=true`：`place(hash, shard) = (hash + shard) % 12`（`drives.rs:196`）。
  shard 0..11 两两互异 → 每站点恰好 6 槽；12 片里只有 4 parity，故每站点至少 2 个数据片。
  **6 < data=8，任何单站点都不能重建对象；读写都必须两站点。** preflight 只验证该结论；
  不存在 `shard%2`/`shard/2` 回退（当前 `place` 也无法表达），不得在 S4 实现该分支。

**关闭 M7 异步站点复制（P0-1）**
- `SqlReplSink` 构造增 `site_data_replication: bool`；stretch 站点传 `false`。
  `enqueue_sites`（`replication.rs:183-241`）首行 `if !self.site_data_replication { return false; }`，
  并对 sync 桶直接返回 false（stretch 由该 flag 整体关闭）；不写 `target_kind='site'` 行。
- S1 落通用 gate（sync + stretch 两种桶），S4 只传 `false`；`replication.rs` 归 S1 所有，
  S4 不改该文件。M6 规则复制仍照常（外部 remote，不是站点对）。
- 启动断言：stretch 下 `repl_outbox WHERE target_kind='site'` 计数必须 0；>0 拒绝启动并提示清理。
- e2e：一次 PUT 后每站点 `ListObjectVersions` 恰好 1 个新版本，B 无异步副本；`repl_outbox`
  无 site 行。sync 桶同样断言（S1/S9）。

**全新空存储（P1-7）**
- `[stretch]` 只允许全新部署：首次启用时 `schema_meta('stretch_initialized')` 不存在，
  且 `buckets` 表为空、所有 `data_dirs` 无 bucket 目录（`.oss.sys` 除外）、无
  `replication_mode=sync` 行；通过后写 `stretch_initialized=1`，之后正常启动不再要求空库。
  未标记且非空 → 拒绝启动并提示先导出/重建；不提供原地异步→stretch 迁移。
- `[stretch]` 下创建 sync 桶 400；若发现既有 sync 桶（例如复制来的 DB）启动失败。
- `[stretch]` 的 identity/配对额外校验：两端 `node_id` 不同且与配置一致、各 6 盘、EC 8+4、
  `placement=v2`、`peer_node_id` 互为对端、对端 identity 的 `self_addr` 与本地配置的
  `peer_addr` 规范化后相等、对端 `peer_node_id` 与本地 `node_id` 相等、对端
  `drive_count`/`ec`/`placement` 与本端配置一致、`mesh_secret_fingerprint` 相等；
  不一致 409。不信任本地 `peer_drives` 声明，也不允许“各算各的槽序”。测试逐项构造
  `self_addr`/`peer_node_id`/`drive_count`/`ec`/`placement`/fingerprint 六类不匹配。

**meta / quorum / inline**
- `Q_s = data+1 = 9`（`store.rs:1009`，`read_meta_cluster:761`）。meta 镜像到 12 槽；
  写 prepare 全部可达槽位，commit 计数 hash 匹配的 flip，`>=9` 即提交；读覆盖 `>=9`。
- 单站点只有 6 槽 → prepare/read 覆盖 6 < 9，写 `QuorumUnmet`、读 `CorruptMeta`（fail-closed，
  S3 映射 500/503）；这就是“两站点必须都在”的实现点。
- `inline_threshold=0`：对象字节只进分片，不进 `xl.meta`；meta 仍含 key/tags/user-metadata/
  content-type（元数据可镜像，验收只要求对象内容不可单站点读取）。`Store::open` 在
  `ClusterConfig.stretch` 时强制 0 并拒绝非 0。
- 零字节对象：`stage_payload_multi` 仍会 inline size 0（`put.rs:305-318`）；meta quorum 9
  仍要求两站点，单站点读/写失败。S4 显式测零字节 PUT + 停 B 读非 2xx，文档列为 inline 例外。
- `write_meta_cluster`/`mesh_prepare_meta` 已按每盘镜像；不新增 meta 协议。

**数据/读路径**
- `stage_payload_multi` 已要求 `data+1=9` 个分片落地（`put.rs:287`）；单站点 6 片 → 写失败。
  首个远端分片失败即 fail-fast（不再等剩余最多 6×5 s），整体 5xx 有界；日志记失败节点。
- `read_ec_range` 需要 `>=8` 片；单站点 6 片 → 读失败；`read_meta_cluster` 先 fail-closed。
- `TonicMesh` 复用（`xl-client/src/lib.rs:227-230`）：64 KiB/块、每块每远端分片一次 RPC；
  stretch 只面向合规/小对象，文档明示吞吐上限；`new_with_timeouts` 透传 `mesh_timeout_ms`。
- heal/fsck：站点恢复后 `fsck --repair`/`heal` 通过 mesh 修复缺失分片；不新增站点感知调度，
  但读修复路径已能写远端槽。WAN 修复限速/窗口在 runbook 说明。
- 真实 mesh 测试：B 本地盘序第二（`state.rs` 在 B 构造 `[A,B]` 后取 `self_idx=1`），
  A 用 `mesh_write_shard` 写 B 的 6 个远端槽；必须 6/6 成功；再断言 A 的 6 个本地槽。
  不能只做进程内拓扑断言。

**失败语义**
- 单站点宕机：读/写失败（503/500），**不承诺容灾**；控制面若另一数据站点 + witness C 可达，
  仍可写（quorum 2）。这是接受的产品语义，不是缺陷。
- 两站点都在但单盘坏：EC 8+4 可重建；`Q_s=9` 仍要求至少 9 槽可达。
- `[stretch]` 下 sync 桶 400；stretch 自身跨站 EC 提供“两站点齐全才可读”，且 M7 异步站点
  复制已关闭，**不存在第二份整对象副本**。

### 2.6 3-voter witness

**角色与配置**
```toml
# witness.toml（独立主机；无 [storage]/[site]/S3）
[raft]
node_id = 3
role = "witness"
bootstrap = false
advertise_endpoint = "http://witness:9100"
cluster_token = "<64 hex>"
db_path = "/var/lib/oss-witness/raft.db"
state_db = "/var/lib/oss-witness/control.db"
```
- `oss-server witness --config witness.toml`：解析独立 `WitnessConfig`；`Db::connect_witness(state_db)`
  （`seed_site=false`，不调用 `site_bootstrap`，无 `site_config` 行）应用 M9 schema；
  `prepare_raft_mode(true)`（置 `raft_mode=on`，无 M7 触发器）；构造控制面 SM；
  不建 `Store`、不启 S3/console/lifecycle/对象 worker；只暴露
  `/health`、`/api/v1/site/identity`、`/api/v1/site/raft/{append,vote,snapshot,status,membership}`。
  `Db::connect` 保留数据站点路径（`seed_site=true`），witness 不用 `state.site`。
- witness 的 `/api/v1/site/identity` 用 `WitnessConfig` 的 node_id/role/advertise_endpoint，返回
  `site_uuid=null`、`stretch=null`、`admin_username`（从导入的控制状态读）；不能读 `site_config`。
- witness 应用全部 11 张复制表（控制状态），但桶目录/对象副作用为 no-op；可服务快照。
- 每个数据站点的 `sites` 只有 1 行数据 peer（A 上是 B、B 上是 A）；witness 不在 `sites`，
  sync gate 的“恰好 1 个 peer”按行数计，不受 witness 影响。

**成员与 bootstrap（全部 leader-aware）**
- A：`[raft].bootstrap=true`，启动 `initialize({A})`；A 的 `BasicNode.addr = advertise_endpoint`。
- B：`bootstrap=false`，等待 leader；`POST /api/v1/sites` 完成双边注册（§S7），
  成员操作走 `POST /api/v1/site/raft/membership`（任意节点转发 leader）：
  `add_learner(B, blocking=true)` → B 追平 → `change_membership({A,B})`。
- C：`oss-server raft-add-witness --config a.toml --node-id 3 --endpoint http://witness:9100`
  → 读 `Raft::membership()` 当前 voters → `add_learner(3)` → `change_membership(current ∪ {3})`；
  C 的 `state_db` 由快照追平。**不硬编码 `{A,B,C}`**。
- 成员操作可在 C 为 leader 时执行：`POST /sites`、`raft-add-witness`、`DELETE /sites` 均先查
  `/api/v1/site/raft/status`，遇 `ForwardToLeader` 带 cluster token 重试；测试覆盖 kill A 后 C 为 leader
  仍能加/替换/移除节点。
- 恰好一侧 `bootstrap=true`；两 true/两 false → 配对 409；node_id 唯一；admin_username 相同；
  `cluster_token` 三节点一致；vault probe 一致（若启用）。
- 生产就绪要求 3 voters；单节点/双节点仅开发/测试：启动 warning + console banner +
  `/raft/status production_ready=false`，不只状态字段。
- RPC 认证：三节点共享 `[raft].cluster_token`，所有 `/api/v1/site/raft/*` 校验 `x-oss-raft-token`
  （常量时间）；`/api/v1/site/raft/join` 同时校验对端 `site_token`。JWT except。

**quorum/可用性**
- quorum 2：A+B、A+C、B+C 均可写；单站点（含 witness）宕机不影响控制面；A+B 同时宕机 → 503。
- 单数据站点宕机时，sync 写/删仍 503（数据面需要 A/B 双方），但 IAM/桶策略等控制写可继续。
- 无自动降级、无自动摘除 voter；follower 读仍本地/可能陈旧。

**永久丢失/force recovery（runbook 摘要，S9 完整版）**
- **C 永久丢失**：A+B 仍 quorum 2；部署 C'（新 node_id = 当前最大+1，首次为 4，避免与旧 3
  冲突），`raft-add-witness --node-id <N>` → 读当前 voters →
  `add_learner(N, blocking)` → 等待 C' 追平 → `change_membership((voters - {3}) ∪ {N})`
  → 移除旧 3。**不直接对 voter 集做一次替换**。控制面全程可写；C' 不在 `sites`。
- **B 永久丢失且有 sync 桶**：A+C quorum 2；部署 B'，**复用旧 B 的 site UUID/`peer_id`/token
  （从备份/`site_config` 种子导入）但使用新 Raft `node_id=<N>=当前最大+1`（首次示例 4；
  旧 B=2 不复用）**，使 A 上 `POST /sites` 命中已有行 adopt/update；否则不同 peer 会 409，
  S9 runbook 在 A 本地 `sites` 行做离线替换（只改本地表，不碰 `raft.db`）。成员操作走
  leader-aware 端点：`add_learner(<N>, blocking)` → B' 快照/控制追平 →
  `change_membership(voters - {2} + {<N>})`。B' 起来后**必须**跑
  `sync-reconcile --repair`（A 为源、B' 为目标）：按本地 version id 重推所有 sync 版本/marker，
  刷新 A 的 `remote_version_id` 与 B' 的 `Version.remote_version_id`，清理指向旧 B 的
  delete/write intents；确认后才把 B' 标为可服务。`DELETE /sites` 在有 sync 桶时 409，
  必须走此流程。A 永久丢失时 B 侧对称执行（A' 新 node_id=当前最大+1）。
- **两节点永久丢失（无 quorum）**：**新集群，不写旧 epoch**。
  1. 停全部节点；**优先**从状态最全的存活数据站点 `raft-export`（11 表业务行、自然键、
     无 LogId 语义）。若只剩 witness C，`raft-export --witness --config witness.toml`
     读 C 的 `state_db`，只导出控制面 11 表；对象数据必须从备份恢复（C 无对象目录）。
     导出源与 `--config`/`state_db` 必须显式记录在 runbook。
  2. 归档旧 `raft.db`；`oss-server raft-force-recover --config <survivor> --from <export>
     --node-id <N> --yes`：建**全新空 `raft.db`**，把业务行单事务导入 `control.db`，
     重置 `raft_applied` 与 `raft_apply_ctx`。
  3. `raft-force-recover` 在同一命令中对**全新空日志**执行 `Raft::initialize({N})`（单节点
     quorum），再启动该节点；启动配置 `bootstrap=false` 只加载既有 membership，**不会**再
     initialize，也不会等待旧 leader。
  4. 其他存活节点回来后 `add_learner` → 追平 → `change_membership(最终 voters)`；
     只有一个存活节点时保持单节点，后续再加 learner。
  5. 对象数据不在此导出内；从备份/异地副本恢复。sync 桶恢复对端后跑
     `sync-reconcile --repair`；stretch 需两站点数据都从备份恢复。
  未提交写丢失；**禁止手改旧 `raft.db`**。若 A+B 全丢只剩 C：C 有控制状态但无对象数据，
  force recovery 只重建控制面。测试：kill 两节点 → 单存活节点恢复 → 写控制面 → 加回 learner。
- **witness 存储**：`raft.db` + `state_db` 持久盘、`synchronous=FULL`；不存对象；备份只需控制状态。

### 2.7 已裁决问题

| # | 裁决 |
|---|---|
| R1 | NodeId = `[raft].node_id`（u64）；site UUID 只做站点身份；两者都在 identity 暴露。 |
| R2 | 生产 3 voters：A/B 数据站点 + C witness；成员变更仅 `POST /sites`（B）+ `raft-add-witness`（C）；无自动摘除。 |
| R3 | sync 删除本轮 remote-first 实现；无 501 追加写语义。 |
| R4 | sync 创建要求 `[raft]` + `RAFT_READY` + 1 个数据 peer；创建即 versioning=Enabled，mode 不可变。 |
| R5 | sync 对象上限 64 MiB（可配）；MPU Complete 超限 400。 |
| R6 | 共享 `ReplClient` 无默认超时；sync 每调用 `[sync].timeout_ms`（10 s）；async worker 自设大超时。 |
| R7 | sync mode 缓存 10 s + 创建/删除/apply 失效；mode 不可变。 |
| R8 | 删除裁决在 S3 service 层；store 删除保持通用；token 认证的站点删除只本地执行。 |
| R9 | Raft 模式 drop M7 触发器、不 spawn site worker；`/api/v1/site/changelog` 409；对象 repl worker 保留。 |
| R10 | 旧 hash 升级/SK 重封在 Raft 模式抑制；管理员密码/AK 本地可写。 |
| R11 | admin 用户名两站点相同；首启写入 `schema_meta('admin_username')`，identity 暴露。 |
| R12 | vault 兼容用密封探针；配对/cutover 预检。 |
| R13 | `snapshot_max_chunk_size`=4 MiB 原始（JSON 约 16 MiB）；快照路由 32 MiB，append/vote 32 MiB；总上限默认 512 MiB（可配 ≤1 GiB）；命令 base64 module、单命令 24 MiB。 |
| R14 | 复制实体相等 = 自然键 JOIN 后业务字段比较；代理键按 username 归一化；快照也按自然键逻辑化（含 `permissions(username,bucket,can_read,can_write,can_delete)`）、安装方重分配本地 id；`set_id`/本地表排除。 |
| R15 | cutover 导出 11 张复制表（buckets 全列含 lock 默认值/default_encryption/replication_mode/deleting）；冲突默认中止。 |
| R16 | 降级不支持：`raft_mode=on` + 无 `[raft]` 无条件拒绝启动；runbook 给恢复路径。 |
| R17 | witness 是 voter、控制面专用；不在 `sites`、不存对象；`cluster_token` 认证；witness 不种 `site_config`。 |
| R18 | `[stretch]` = 2×6 预设 + 站点感知放置；与 `[cluster]` 互斥；必须有 `[site]`+`[raft]`；禁止 sync、关闭 M7 异步站点复制、只允许全新空存储。 |
| R19 | 成员移除：有 sync 桶 409；无 sync 桶可 `change_membership` 移除；所有成员操作 leader-aware；替换流程见 §2.6。 |
| R20 | force recovery 只离线、显式 `--yes`、建全新 `raft.db` + `initialize({N})` 后加 learner；不手改旧 `raft.db`。 |
| R21 | `[stretch]` 不承诺站点容灾；单站点宕机读写失败是预期；控制面靠 witness 保持。 |
| R22 | `[raft]+[[groups]]` 启动拒绝；`upsert_bucket_set` 跳过。 |
| R23 | sync 版本 id 双向对称：token-auth 写/ marker 的 origin UUID = 接收端 `Version.version_id`；接收端写 `remote_version_id`；响应 id 必须相等。 |
| R24 | sync 写/删均先落本地 intent；写崩溃恢复**只补偿远端**（本地 meta 不存在时远端同 id token 删除；存在时只清 intent/tee），不从 tee 完成本地提交；intent 不是 outbox。 |
| R25 | stretch 两条 `NodeSpec` 按 node_id 规范排序；`self_addr` 为本站 mesh 监听/`NodeSpec.addr`、`peer_addr` 为对端；配对数、盘数、EC、placement、mesh secret/地址双端校验。 |

## 3. 切片表、估算与顺序

| 片 | 范围 | 依赖 | 估时 | 验收锚 |
|---|---|---|---|---|
| P | **Preflight/spike**（§4） | — | 7 d | 所有片的前置 |
| S0 | 契约/迁移/配置/UI 文案；`[raft]`(voter/witness)/`[sync]`/`[stretch]`（含 `self_addr`/mesh_secret/openraft 运行时字段）；`replication_mode`/`deleting`/`remote_version_id` 契约；`sync_write_intents`；全列 create writer；guard；能力开关 false | P | 3 d | ①②③ 前置 |
| S1 | sync 写核心：`CommitGate` + PUT tee + **写 intent** + origin-id 同 id 规则 + `SiteSyncGate` + 503/补偿（compensate-only） + 单 peer + 站点复制 gate | S0 | 6 d | ② 写 |
| S2 | sync MPU/Copy/元数据/版本规则/不可变；**MPU 拼接 plaintext tee**；origin-id 透传；tags/class 保留；SSE-C/tag 501/lock/lifecycle 裁决 | S1 | 4 d | ② |
| S3 | sync remote-first 删除：**共享 `SyncDeleteService`（S3/lifecycle/Batch/console 四类 caller）**、内部 token 路由、marker/version/DeleteObjects/桶删 quiesce、active-writer counter、`head_object_version`、周期 intent、repair | S1,S2 | 6 d | ② 删 |
| S4 | **Stretch EC 2×6**：`self_addr` + `cli.rs` mesh server、规范节点序、2×6 拓扑、placement、inline=0、Q_s、mesh secret/timeout、关闭异步站点复制、全新 store guard、真实 `oss-server` mesh 测试 | S0,S1 | 9 d | ③（最高风险） |
| S5 | Raft 基座单节点 + witness runtime：**openraft `storage-v2`/原生 async fn**、运行时参数/客户端 deadline、LogId+membership、base64 命令、512 MiB 分块快照 + 32 MiB 路由、HTTP 快照测试、witness 子命令、connect_witness | S0 | 6 d | ① 前置 |
| S6 | 命令模型 + 单节点路由：全部控制写、apply/tx（每条一个 R）、guard、create writer、`SetBucketDeleting`（仅 sync）、groups、物化对账、PayloadTooLarge、RAFT_READY | S5 | 11 d | ① |
| S7 | 3-voter 成员/双边配对/快照/cutover：leader-aware membership、advertise、join、sites adopt、C 加入（add_learner 先行）、快照（permissions 三列）、cutover 11 表、force recovery | S6 | 10 d | ①④ |
| S8 | 故障/读语义/可观测：单站点宕机控制可写、A+B 宕 503、重启追平、follower 读、指标、compensate-only/MPU/marker 405/DeleteBucket M7 回归、验收 ①④ | S7 | 5 d | ①④ |
| S9 | `e2e_m9.sh` + `e2e_stretch.sh` + console 3-voter/witness 卡 + 文档/force runbook（含 witness 导出源） | S3,S4,S8 | 5 d | ①②③④ |

- 切片基础 **65 人日**（S0–S9），加 **3 d 应急**（S4/S5 上浮）= **68 人日**；含 preflight 7 d
  共 **75 人日**。关键路径控制轨 `S0→S5→S6→S7→S8→S9` = 3+6+11+10+5+5 = **40 d**；数据轨
  `S1→S2→S3` = 6+4+6 = **16 d**，`S4` = **9 d**（依赖 S1 的复制 gate），可并行。单工程师
  ≈ 15 周；双工程师 ≈ 11–12 周（S6/S7 是控制轨瓶颈，S4 是数据轨/风险瓶颈）。
- **首个可实施片 = S0**（单写手），但必须先做 P 的 8 组/12 项 spike（§4）。
- S4/S5/S6 都碰 `state.rs`（`Cargo.toml` 只有 S5）：S0 预置 `gate/control/raft/stretch`
  字段和 `sync::init`/`raft::init`/`stretch::init` stub；S4 只填 stretch 分支 + `cli.rs`
  mesh server，S5/S6 填 raft 分支，由 integration owner 串行合并 `state.rs`。S4 的
  `replication.rs` 改动由 S1 先落，S4 只传 `site_data_replication=false`，不越界。

## 4. Preflight / spike（7 d，先于 S0）

1. **openraft 0.9.25 + leader 成员**（1.5 d）：workspace 精确 manifest 用
   `features=["serde","storage-v2"]` 跑 `cargo check --workspace`；v2
   `RaftLogStorage`+`RaftStateMachine` 直传、原生 `async fn`（无 `async-trait`）、无
   `Adaptor`、`generic-snapshot-data` 关闭；`initialize({self})`、`client_write`、重启、
   快照 build/install、learner+`change_membership` 到 3 voters；记录
   `install_snapshot_timeout_ms`/heartbeat/election/`max_in_snapshot_log_to_keep=0`/
   `purge_batch_size`/`write_timeout_ms`/`snapshot_max_chunk_size` 常量、`PayloadTooLarge` 拆分、apply 单 R/
   `ClientWriteResponse.data` 形状、`Cursor<Vec<u8>>` 接收类型；记录 16 MiB wasm 经 base64
   JSON 的命令与快照尺寸。**1a**：kill A 让 C 成为 leader，通过
   `POST /api/v1/site/raft/membership` 完成 add/replace/remove；CLI 发现 leader 并重试。
   产出 S5/S7 的 API/尺寸/成员操作常量。
2. **启动/迁移**（0.5 d）：无条件 guard；`0004` 连续两次 `Db::connect`；`[raft]+[[groups]]` 拒绝；
   rows-to-dirs 对账；M7 `sites` 行 adopt；witness `connect_witness` 无 `site_config` 行。
3. **cutover 列审计**（0.5 d）：枚举 **11 张复制表**（buckets 全列含 `object_lock_default_mode/days`、
   `default_encryption`、`replication_mode`、`deleting`；其余 10 表）与 owner_id→username /
   `permissions(username,bucket,can_read,can_write,can_delete)` 映射；决定
   export/merge/dry-run；M7 双边 fixture；逐表 diff。
4. **配对**（0.5 d）：`advertise_endpoint`；双边 `POST /sites`+`/api/v1/site/raft/join`；cluster token；
   B-originated console/sync 写；M7 旧行 adopt 与不同 peer 409。
5. **尺寸 + HTTP 快照**（0.5 d）：base64 module、append/vote 32 MiB、快照 4 MiB 原始分块
   （JSON 约 16 MiB）、快照路由 32 MiB、总 512 MiB；构造最坏 OLAP 16 MiB module 场景确认
   不超限；**走真实 HTTP 路由完成一次 build/upload/install**（含 `install_snapshot_timeout`
   与 WAN 模拟延迟），记录实际 wire size；openraft in-process spike 不算该证据。
6. **stretch 拓扑 / 顺序 / mesh server**（1.5 d）：用现有 cluster 代码搭 2×6；**6a** B 的
   `self_idx=1` 且两端 `[A,B]` 规范顺序；`[stretch].self_addr` 规范化并作为本地
   `NodeSpec.addr`，**由真实 `oss-server` 进程（不是测试 harness）**启动 `xl_client::serve`；
   `listen`/`advertise_endpoint`（主 HTTP）与 `self_addr`（mesh）端口分离，B 经
   `advertise_endpoint` 访问 `/api/v1/raft/status` 成功；A 通过真实 `mesh_write_shard`
   写 B 的 6 个远端槽 6/6 成功；确认 `placement_v2` 每站点
   6 片、`Q_s=9`、inline=0、零字节、停一站 fail-closed；`peer_addr`/`self_addr` 规范化与
   `mesh_secret` 认证。**只验证 6/6，不保留 `shard%2` 回退**。
7. **抑制异步站点复制**（0.5 d）：**6b** `[stretch]` 一次 PUT → 每站点恰好 1 版本、
   `repl_outbox target_kind='site'` 0 行；sync 桶同样；M6 外部规则仍工作。
8. **sync 映射/崩溃/删除**（1.5 d）：**8a** `x-oss-origin-version-id` 往返，接收端同 id +
   `remote_version_id`，从 B 发起版本删除和 marker 删除；**8b** kill -9 在远端 ack 与本地 meta
   之间，重启后写 intent **只补偿远端**（不完成本地提交）、版本列表相等；**8c** 删除 intent
   周期 worker（不重启恢复）、marker HEAD 200/405/404 三态、DeleteBucket quiesce/并发 PUT；
   **8d** MPU Complete plaintext tee 存在且可被 gate 读取；**8e** Batch Delete、console
   对象删除、console 桶删除在 sync 桶上走 `SyncDeleteService`（或按裁决 409），async 桶不变。

**Preflight 出口**：**12 项结果**写入本计划（storage-v2 workspace 编译/原生 async、
openraft 3-voter + C-leader 成员与运行时常量、命令/HTTP 快照尺寸、0004 双 connect、
cutover 11 表 diff + permissions 三列、配对 adopt、stretch 6/6 + 真实 `oss-server` mesh、
异步复制抑制、origin-id 双站删除、写 intent kill-9 compensate-only、删除 intent 周期恢复 +
marker HEAD 405、Batch/console 删除路由 + MPU tee）；任一失败则停下改设计，不进入 S0。
S5 派发前必须用精确 manifest 重跑 storage-v2 编译；S4 生产接线前必须用真实二进制跑通
`self_addr` mesh server。

## 5. 切片详情

### S0 契约/迁移/配置/UI 文案（3 d）

**迁移**
- **不改已应用的 `crates/oss-server/migrations/0001/0002/0003`**（`0001_init.sql:22-30` 保持原样）。`buckets.replication_mode`、
  `sites.raft_node_id` 由 `db.rs::ensure_columns`/`ensure_columns_m9` 用 `PRAGMA table_info` 幂等添加；
  新库 init 后同样走 ensure。M7 bucket 触发器 payload **保持原样**：M7 下 mode 恒为 async，
  cutover 从本地 `buckets` 全列导出，不依赖 changelog。
- 新 `crates/oss-server/migrations/0004_m9.sql`（唯一 M9 迁移，**可重放**）：
  - `CREATE TABLE IF NOT EXISTS schema_meta(key,value)`、`raft_applied(id=1,applied_log_id,membership_json)`、
    `raft_apply_ctx(id=1,active)`；
  - `sync_write_intents(id TEXT PRIMARY KEY,bucket,key,local_version_id,remote_version_id,
    tee_path,commit_json,state,created_at,updated_at)`；
  - `sync_delete_intents(id TEXT PRIMARY KEY,bucket,key,op,local_version_id,remote_version_id,
    marker_version_id,state,created_at,updated_at)`；**不要**用空串版本字段做 UNIQUE；
  - 11 张表 guard：`DROP TRIGGER IF EXISTS` 后 `CREATE TRIGGER`；例外按 §2.3（users 的
    OLD/NEW.is_admin、access_keys 的 session/admin 属主、permissions 的 admin 目标）；
    `prepare_raft_mode(false)` 启动时 drop 这些 `raft_guard_*`，M7 模式可正常写；
  - `INSERT OR IGNORE` 初始化 ctx；不建列（列走 ensure_columns）。
- `db.rs`：`SCHEMA_M9`；`connect`（230-249）M8 后执行；`ensure_columns`（291-430）补
  `buckets.replication_mode`、`buckets.deleting`；`ensure_columns_m9` 补 `sites.raft_node_id`；
  `prepare_raft_mode(bool)` 无条件调用（见 §2.1）：false 时 drop `raft_guard_*`，
  true 时保留并 drop `site_*`；`create_bucket_tx` 全列 writer；
  `insert_bucket` 委托 async 默认；`insert_peer` 增 `raft_node_id`（784-795）；
  新增 `connect_witness(url)`（不调 `site_bootstrap`，witness 无 `site_config` 行）。
- `reconcile_bucket_dirs`：row→dir、空 orphan dir 删除、非空移 `.oss.sys/orphan/`。
- M7 `site.rs`（`apply_bucket` 321-418、`insert_peer_with_snapshot` 838-865）本轮不改；
  其 payload 无 mode，按默认 async 应用。

**配置**
- `config.rs`：`RaftToml { node_id:u64, role: RaftRole, bootstrap:bool, advertise_endpoint:String,
  cluster_token:String, db_path:Option<PathBuf>, election_timeout_ms, heartbeat_interval_ms,
  rpc_timeout_ms, install_snapshot_timeout_ms, max_in_snapshot_log_to_keep,
  purge_batch_size, write_timeout_ms, snapshot_max_chunk_size, snapshot_max_bytes }`；
  `SyncToml { max_object_bytes, timeout_ms }`；`StretchToml { node_id, self_addr, peer_node_id, peer_addr, peer_drives,
  placement, mesh_secret, mesh_timeout_ms }`；`AppConfig` 增 `raft/sync/stretch`。校验：raft
  必须 site、advertise、node_id>0、cluster_token；witness role 走独立 `WitnessConfig`；sync
  上限；stretch 必须 site+raft、2×6、8+4、inline=0、placement 只认 v2、mesh_secret 非空、
  **`self_addr`/`peer_addr` 都必填且规范化（去 `http://`，拒绝 https/path，二者不相等），
  且 `advertise_endpoint` 的 host:port 不等于 `self_addr`（主 HTTP 与 mesh gRPC 分离）**；
  `site_validate` 允许 `site+stretch`，仍禁止 `site+cluster`/`stretch+cluster`。stretch
  校验在 S0 落完整字段/函数，S4 只接线，`config.rs` 之后只读。
- 能力开关：`RAFT_READY=false`、`SYNC_READY=false`；`RAFT_READY=false` 时控制写 503
  （`[raft]` 可启动供 S5 测试），`SYNC_READY=false` 时 sync 桶创建 400。
- 节点身份：`schema_meta` 写 `raft_node_id/bootstrap/initialized/admin_username`；变化拒绝启动。

**API/UI**
- console `list_buckets`（1620-1672）返回 `replication_mode`；`CreateBucketBody` 增 mode；
  create（1683+）校验 sync 需 `RAFT_READY` + 1 peer + versioning Enabled + no lock。
- S3 `create_bucket`（`service.rs:733-790`）读 `req.headers`（s3s 0.17 `protocol.rs:126-141`）
  `x-oss-replication-mode`；统一 `create_bucket_tx`；行写失败回滚物理桶；guard 失败 5xx/409。
- `raft/mod.rs` stub + `router.rs:88-96` `GET /api/v1/raft/status`（S5/S8 扩展）。
- web `api.ts`/`Buckets.tsx`：mode 列/弹窗；文案更新为“3-voter 控制面；sync 写/删均远端确认；
  单数据站点宕机控制面可写但 sync 503；stretch 两站点缺一不可”。
- 文档占位：`README.md` 模式矩阵、`[stretch]`、witness、force recovery。

**xl-store 契约**
- 新 `xl-store/src/gate.rs`：`CommitGate/GateRequest/GateAck/GateError` 定义（trait `Debug`，
  无实现）；`StoreConfig` 增 `gate: Option<Arc<dyn CommitGate>>`、`sync_max_bytes`；
  `Store::sync_mode` stub。S1 只填 `sync.rs`/`open` 注入，S4 只加 stretch 分支。

**测试**
- `m9_contract.rs`：旧库补列、0004 连续两次 connect、mode 列、guard 例外
  （users/access_keys/permissions）、M7 启动后 `raft_guard_*` 不存在、Raft 启动后存在、
  `RAFT_READY=false` 控制写 503、sync 创建 400、S3 头；`[stretch]` 缺 `self_addr`/地址
  非规范化拒绝启动；`[raft]` 运行时字段默认值。
- `site_changelog.rs`：M7 payload 不变、mode 默认 async；M7 行为回归。
- `xl-s3`：CreateBucket 头/回滚/guard 5xx。

### S1 sync 写核心（6 d）

**store 契约（新 `crates/xl-store/src/gate.rs`）**
```rust
pub struct GateRequest<'a> { bucket, key, local_version_id: &'a str, plaintext: &'a Path,
  size, etag, content_type, user_metadata, tags, storage_class,
  commit_json: &'a str /* store 预构造：checksum/SSE/lock/MPU etag 等 */ }
pub struct GateAck { pub remote_version_id: Option<String> }
pub enum GateError { Unavailable(String), TooLarge, Rejected(String) }
// 这里的 async-trait 只用于 xl-store 既有 trait 风格，与 openraft 无关；
// S5 的 RaftLogStorage/RaftStateMachine impl 用原生 async fn，不得加 async-trait。
#[async_trait] pub trait CommitGate: Send + Sync + std::fmt::Debug {
  async fn is_sync(&self, bucket: &str) -> Result<bool, GateError>;
  async fn before_commit(&self, req: GateRequest<'_>) -> Result<GateAck, GateError>;
  async fn abort(&self, ack: &GateAck);
}
```
- `StoreConfig`（`store.rs:41`）的 `gate: Option<Arc<dyn CommitGate>>`、`sync_max_bytes: u64`
  由 S0 预置；S1 只填实现。`StoreConfig` 保留 `Debug+Clone`，故 `CommitGate: Debug`；
  `SiteSyncGate` 手写/derive Debug（只打 bucket 计数，不打 tee 路径之外的敏感值）。
  `Store::sync_mode(bucket)` 薄壳。
- `Error` 增 `Unavailable(String)`、`Unsupported(String)`；`convert.rs:6-40` 映射 503/501。
- `Version`（`meta.rs:29`）增 `remote_version_id: Option<String>`（serde/default，旧 meta
  None）；`put_object_stream_with_meta`/MPU complete 增 `forced_version_id: Option<Uuid>`；
  sync 路径用预生成 UUID 作为 version id 和版本目录名；已存在同 id 时返回既有版本
  （size/ETag 相同）或 409。

**Put/MPU 插入**
- `put_object_stream_with_meta`（`put.rs:585-843`）：入口取 sync 并预生成 `uuid`；tee 明文到
  `staging_dir/tee/<uuid>.plain`（不在 `tmp/` 版本目录内）；超限 staging 前 400；
  staging 结束 `put.rs:686` / `Version {` 前 `put.rs:688` 插 gate（锚点 `687`）；
  gate 请求带 `local_version_id=uuid`；成功后删 tee。
- **写 intent**：S1 在 `sync.rs` 实现 `sync_write_intents`（S0 建表）：tee 完成后、远端调用前
  插入 `remote_pending`（`remote_version_id=local id`, `commit_json=req.commit_json`
  全元数据/SSE/lock/MPU etag）；
  ack 后写回 `ack.remote_version_id`（必须等于 local id）并置 `remote_done`；
  本地 commit 后删行。启动恢复 + 30 s 周期 worker：
  本地同 id 版本存在 → 清理；否则 token DELETE 同 id 补偿；远端不可达则留 intent 重试。
  `synchronous=FULL` 用于 intent 插入。
- `ObjectInfo`（`store.rs:95-120`）与 `XlMeta::Version` 透传 `remote_version_id`；
  `put.rs:837` 硬编码 `replication_status: None` 改为 sync 的 `COMPLETED` + remote id。
- 失败：gate Err → 现有 staging 清理；本地 commit 失败且 ack 存在 → `gate.abort` + intent 保留。
- `mpu.rs:525`（staging 结束/`Version` 前）同样插 gate；`ObjectInfo`（642-663）透传；
  MPU 的 `forced_version_id` 同样预生成。
- `replica: bool` 参数：REPLICA 摄入不调用 gate；service 后置 `REPLICA` 保持
  （`service.rs:994-1007`）。token-auth sync REPLICA 例外：采用 origin id + 写 remote id。
- **不新增严格 flip 分支**：按 §2.4 的 M2b 语义；sync 依赖 staging 的 data+1 与 meta prepared。

**oss-server `sync.rs`**
- `is_sync`：`buckets.replication_mode` + 10 s cache；创建/删除/apply 失效；`deleting=1` → 503。
- `before_commit`：`[site]` + 恰好 1 个 `sites` 行（witness 不计）；读 tee 文件（≤64 MiB）；
  `ReplClient::single(...)`；`put_object_with_meta(..., require_version=true, site_token,
  origin_version_id, tags, storage_class)`；响应 id 必须等于 local id；返回 remote id。
- `abort`：`delete_object_version`（token + `?versionId=`），404 成功；失败指标。
- `repl_client.rs`：`put_object_with_meta` 返回 `Result<Option<String>, ReplError>`；
  增 `site_token`、`origin_version_id`、`delete_object_version`、`delete_marker`；
  `send_with_timeout`（sync 10 s，共享 client 无默认超时）；
  `x-oss-site-token`/`x-oss-origin-version-id` 纳入 SigV4 SignedHeaders。
- `replication.rs::SqlReplSink::new` 增 `site_data_replication: bool`；`enqueue_sites`
  （183-241）首行拒绝 `!site_data_replication`，并对 `buckets.replication_mode='sync'`
  返回 false；`on_write`（106）跳过 sync。stretch 由 flag 整体关闭。M6 规则仍照常。
  S4 只传 false。
- `state.rs`：S0 预置 `sync::init` 调用与 `OssS3.local_site_token` 字段，S1 只填 `sync.rs`；
  `service.rs` PUT（894+）token 校验：sync 桶 REPLICA 无/错 token → 403；token 正确 →
  本地摄入（不 gate），带 origin id 时强制同 id。

**测试/出口**
- `xl-store/tests_sync.rs`：mock gate 断序（staging 后、Version 前）；gate 失败无 meta；
  `replica=true` 不调用；tee 清理；MPU 同；forced id 幂等/冲突。
- `sync_gate.rs`：mock 远端 axum（复用 `tests/replication.rs:683`）→ 200+远端+COMPLETED；
  远端 500/超时 → 503+本地 404+无 outbox；token 缺失/错误 → 403；空/异 id → 502；
  0/2 peer → 503；tee 文件不存在；写 intent kill -9 → 补偿/无孤儿。
- `replication.rs`：stretch/sync 桶一次 PUT 无 site 行；async 桶仍入队。
- S1–S3 的 sync 桶 fixture 用 DB 直插 `replication_mode='sync'`（API 创建要求
  `RAFT_READY`，S6 后才全绿）；mock gate/远端即可测数据面。
- 出口：七闸；**`SYNC_READY` 仍 false**（S3 出口才翻）。

### S2 sync MPU/Copy/元数据/边界（4 d）

- Copy：`service.rs::copy_object`（3074-3260）走完整 `put_object_stream_with_meta`，
  确认 `dst_versioned`（3219）；补 tags/storage_class；sync Copy 预生成 version UUID，
  经 gate 带 `origin_version_id`；远端同 id 落副本。
- MPU：`complete_multipart_upload_versioned` 的 tags/storage_class 入 gate；complete 预生成
  version UUID；`forced_version_id` 用于本地与远端同 id；**在拼接 part 流进
  `stage_payload_multi` 的同时 tee 到 `staging_dir/tee/<uuid>.plain`**，`GateRequest.plaintext`
  指向该文件；tee 写失败在远端调用前 500；成功/失败/取消删除，无 intent 孤儿 >1 h 清理。
  远端整对象 ETag 与本地 MPU ETag 不比对（`is_single_part_etag` 口径），但 origin-id
  响应必须一致；GET ETag 两站点可能不同，文档化。
- ReplClient：`x-amz-tagging`（复用现有 URL 编码）、`x-amz-storage-class`、
  `x-oss-origin-version-id`；远端 REPLICA 摄入保留 tags/class（修 `service.rs:934,950-951`
  的 `None`；仅 token-verified sync replica），并采用 origin id。
- 接收端幂等：token sync PUT/MPU 的 origin id 已存在且 size/ETag 相同 → 返回既有 id；
  不同 → 409；不重复创建版本。
- 版本：远端 bucket 必须存在且 Enabled；复用 `site_versioning_ready`（`replication.rs:266-297`）；
  未就绪 503；sync `Suspended` 409；mode 无更新 API。
- 边界：SSE-C 对 PUT/MPU/Copy dest → 400；object lock/EnableBucketLock → 409；
  Tagging API/Batch tag（`service.rs:3268,3313`；`console.rs:2003,2025`；`job_actions.rs:383`）
  sync → 501；lifecycle Expiration 允许、Transition 400；标签/存储类仅随 PUT/MPU/Copy 复制。
- 远端超时歧义：origin id 已知；一次 HEAD `?versionId=<local id>` 能定位则 `abort`，否则
  `oss_sync_ambiguous_total` + 写 intent 周期补偿；文档“可能远端可见”。
- 测试：MPU/Copy sync（本地/远端同一 version id）、MPU tee 存在且内容等于对象明文、
  远端调用前 tee 缺失 500、kill -9 ack/commit 边界按补偿收敛、tags/class 远端 GET、
  幂等重推、SSE-C 400、tag 501、lock 409、Suspended 409、lifecycle Transition 400、
  转移失败 503。

### S3 sync remote-first 删除（6 d）

- **meta**：`Version`（`meta.rs:29`）已有 `remote_version_id`（S1 加）；list/get 透传。
  `delete_object_versioned_ext` 增 `DeleteMarkerSpec` 参数（或新 `_with_marker` 包装）：
  `{ marker_uuid: Uuid, remote_version_id: Option<String> }`；marker 的 id/remote id 在
  同一 meta 写落盘（`version.rs:159-235` 的 UUID 生成/`write_meta` 之间）。
- **共享 `SyncDeleteService`（P0-3）**：在 `sync.rs` 定义 remote-first 方法
  `delete_version(bucket,key,version_id)`、`delete_object(bucket,key)`（marker）、
  `delete_key_all_versions(bucket,key)`、`delete_bucket(bucket)`；内部先判断
  `is_sync(bucket) && raft_mode`，否则委托现有 store 路径。以下四类 caller 全部改走它：
  1. S3 `service.rs:1780+` `delete_object` 与 `:1984+` `DeleteObjects`；
  2. `lifecycle.rs`/`lifecycle_store.rs` 的 Expiration；
  3. `job_actions.rs:339-347` Batch Delete（逐版本）；async 桶保持原逻辑；
  4. `console.rs:1909-1922` 对象删除（`delete_key_all_versions`）与
     `console.rs:1759-1778` 桶删除（`delete_bucket`）。
  同一服务再定义 **local-only** 方法 `delete_version_local`/`create_marker_local`/
  `delete_bucket_local`（或 `LocalOnly` 模式），仅供接收端内部路由调用；它们绝不发起
  ReplClient/站点调用。**直接 store 删除禁令仅约束上述四类外部 caller**；接收端内部路由
  是唯一允许绕过 remote-first 的调用方（B4）。
- **内部站点路由**：`router.rs` + `sync.rs` 增 `POST /api/v1/site/delete-object`
  （body `{bucket,key,version_id?,marker_id?}`）与 `POST /api/v1/site/delete-bucket`
  （`{bucket}`）；`x-oss-site-token` 必填、常量时间校验、JWT except；handler 只调
  `*_local` 方法做本地删除/本地 marker，**永不转发、永不调 remote-first 方法**。普通 S3
  `DELETE` 不带/不读站点 token，永远走客户端 remote-first；代理剥离 token 只会让内部
  路由 403，不会退化成客户端路径（N2）。测试断言接收端路径无任何 outbound 调用。
- **service 删除路径（客户端）**：
  - versionId：本地/远端同 id；`ReplClient::delete_object_version` 调内部路由 → 2xx/404 →
    本地删版本；本地失败留 intent；无映射（M9 前对象）→ 409
    `InvalidRequest`（提示 `--repair`）。
  - 无 versionId：service 预生成 marker UUID `m`，写 intent；
    `ReplClient::delete_marker` 调内部路由 `{marker_id:m}`；远端返回 marker id `m`；
    本地 marker `m` + remote id `m`；本地失败由 worker 幂等补齐。
  - `DeleteObjects`：逐键；任一远端失败 → 503，成功键已提交；`sync_delete_partial_total`；
    文档/测试固定该契约；marker 重试 at-least-once（不承诺列表逐项相同）。
- **lifecycle**：`lifecycle.rs`/`lifecycle_store.rs` 的 Expiration 走同一
  `SyncDeleteService`：当前版本过期按“无 versionId”→远程 marker（预生成 marker UUID），
  非当前版本过期按同 id 删版本；失败记 job error 下一轮重试；不本地直删。两站点 worker
  可能各产生 marker（**不同 id/时间戳**），at-least-once；验收只断言对象不可见与同 id
  版本删除，不断言两站点版本列表逐项相同（替代“两边一致”）。
- **DeleteBucket（P1-5）**：
  - 新增内部 token 路由 `GET /api/v1/site/bucket-empty?bucket=`、
    `POST /api/v1/site/bucket-quiesce {bucket}`、`POST /api/v1/site/delete-bucket`
    （`router.rs` + `sync.rs`；`site.rs` 保持 M7 不动）。quiesce 处理器在同一锁内置
    deleting，**等本站 active-writer 计数归零后才 200**，上限 30 s；超时先清本地
    deleting 再 409/503，调用端同时回滚本地 deleting + `SetBucketDeleting{false}`。
  - **仅当 `is_sync(bucket) && raft_mode` 走新流程**；M7 async 桶保持
    `service.rs:804-858` 本地删除 + 冲突补偿，不调用 quiesce/远端删除。
  - `Store` 增 `active_writers: Mutex<HashMap<String,usize>>` 与
    `deleting: Mutex<HashSet<String>>`；`begin_write(bucket)` 同一锁内检查 deleting 并
    自增，返回 RAII guard；`begin_delete(bucket)` 同一锁内置 deleting 后等计数归零，
    **上限 30 s**；超时回滚 deleting 并 409/503。
    PUT/MPU/Copy/Delete 及 Batch/console 路径都在入口取 guard。
  - 流程：写 `buckets.deleting=1`（S6 改由 `SetBucketDeleting` 命令承载）→
    `begin_delete` 等本站计数归零（30 s）+ 对端 quiesce（30 s）→ 两端 bucket-empty；
    任一步超时回滚 `deleting=0` + 409/503；非空回写 `deleting=0` + 409；空则
    `POST /api/v1/site/delete-bucket` → 本地删；失败保留 deleting +
    `delete_bucket` intent 周期重试，API 503。
  - PUT/MPU/Copy/Delete 在 `deleting=1` 时 503；quiesce 期间新写拒绝；不再把“两站点空检查”
    当作无竞态保证。
- **intent**：S0 表；每操作 `id` UUID 主键，版本字段允许空串但**不做唯一键**：
  `sync_delete_intents(id,bucket,key,op,local_version_id,remote_version_id,marker_version_id,
  state,created_at,updated_at)`；远端前 `remote_pending`，ack 后 `remote_done`，本地成功后删。
  周期 worker（30 s，启动先跑）：新增 `ReplClient::head_object_version`（token，返回
  200/404/405）。`delete_version` 按 `remote_pending` HEAD：**200 或 405/DeleteMarker →
  重试内部路由删除；404 → 本地删**；`remote_done` 本地删。`create_marker` 同规则（200/405
  = 远端 marker 已在，只补本地；404 重试创建）。`delete_bucket` 重跑 quiesce/空检查/
  两端删除。本地瞬时失败不重启也恢复；指标 `sync_delete_recovered_total`。
- **token**：仅内部路由读取 `x-oss-site-token`，与 `site_config.site_token` 常量时间比较，
  并纳入 SignedHeaders；缺失/错误/签名失败 → 403，且内部路由只本地执行。普通 S3 DELETE
  不读取该头、永远走 remote-first；非 sync 桶保持 M7 行为。不再依赖“剥离头 → 403”的
  不可实现说法（N2）。
- **`sync-reconcile`**：默认只读报告（本地 version id ↔ 远端 HEAD/ListObjectVersions，
  输出 missing/ambiguous）；`--repair` 可续跑修复：同 id 存在则写映射；缺失则按 ETag+size
  匹配 legacy；仍无则从本地明文按同 origin id 重推；旧 legacy 远端版本列为 ambiguous，
  不静默删除。B' 替换必须跑此命令后才标可服务。
- **测试**：e2e 对端停 → DELETE/DeleteObjects/版本删 503 且本地 GET 仍 200；恢复 → 204，
  对象不可见/同 id 版本删除成功（不比较版本列表）；从 B 发起版本删除/marker 删除；
  marker HEAD 200/405/404 三态恢复；生命周期双 marker at-least-once；内部路由 token 错
  403/S3 路由无 token 仍 remote-first；本地 commit 失败 → 周期 intent 恢复（不重启）；
  远端 404 幂等；桶删并发 PUT 503/非空 409/quiesce；**B3 在途写**：B 上用一个
  barrier/SIGSTOP 卡住已过 `begin_write` 的 PUT，A 发起 DeleteBucket，断言 A 在 B 计数
  归零前不删、quiesce 30 s 超时返回 409/503 且 A 回滚 `deleting=0`；**B4 no-loop**：
  接收端内部路由只调 local-only 操作，断言不发起任何 outbound `ReplClient` 调用（无
  A→B→A）；四类外部 caller 断言从不直接调 `store.delete_*`；**Batch Delete、console
  对象删除、console 桶删除在 sync 桶上走共享服务，对端停时 503/409 且不本地直删；
  async 桶 M7 行为不变**；`sync-reconcile --repair` 修复 pre-M9 映射。
- 出口：**翻 `SYNC_READY=true`**；七闸 + `sync_gate` 全量 + `e2e_sites.sh` M7 回归。

### S4 Stretch EC 2×6（9 d，最高风险）

- **配置/校验**：`config.rs::stretch_validate`：必须 `[site]`+`[raft]`；`[cluster]`/`[[groups]]`
  缺失；`nodes.len()==2`、node_id 唯一、每节点 6 盘、`ec 8+4`、`inline_threshold==0`、
  `placement=="v2"`、`mesh_secret` 非空、**`self_addr`/`peer_addr` 必填且规范化后不相等，
  `advertise_endpoint` host:port 与 `self_addr` 不相等**；主 HTTP（`listen`）承载 Raft 路由，
  mesh gRPC 独立端口；`[stretch]` 与 sync mode 互斥（创建 sync 桶 400）。
  `site_data_replication=false` 由 `[stretch]` 强制；若配置文件显式为 true，启动报错而不是
  静默覆盖。`[stretch]` 首次启用只接受**全新空库**：`schema_meta('stretch_initialized')`
  不存在时检查 `buckets` 为空、`data_dir` 无对象/无 meta、无 sync 桶；通过后写标记，后续
  正常启动不要求空库。配对产生的 `sites` 行允许存在（控制面需要），但 M7
  `site_changelog`/`site_*` 状态必须为空；未标记且非空直接拒绝（不迁移、不混跑）。
- **拓扑/顺序（P0-4）**：
  - `drives.rs` 增 `ClusterSets::new_stretch(self_id, nodes, ec)`；入口先
    `nodes.sort_by_key(|n| n.node_id)`，再以排序后的位置求 `self_idx`；**构造前不信任调用方
    顺序**（A 站点输入 `[A,B]`、B 站点输入 `[B,A]` 都必须得到同一 `[A,B]` 槽序）。
  - 复用 `ClusterTopology::build(..., check_per_node_parity=false)`（`drives.rs:87`；`new` 仍
    `true`），注释“stretch 不承诺站点故障，逐节点 parity 上限不适用”；构造 `sets`/
    `self_ranges`/`flat`；`place` 复用 v2。
  - `ClusterConfig` 增 `stretch: bool`；`new_stretch` 断言 12 槽、每站 6、`self_idx` 与
    `self_ranges` 一致、每节点至少 2 个 data 片（6 槽只有 4 parity）。
  - 连接后互校：`/api/v1/site/identity`（S7 字段）的 `stretch` 对象返回
    `{node_id, self_addr, peer_node_id, drive_count, ec, placement, mesh_secret_fingerprint}`；
    逐项对比本端配置，任一不一致 fail-fast，不允许“各算各的槽序”。
  - **B1 e2e**：B 用 A 的 `advertise_endpoint`（主 HTTP `a:9000`）请求
    `/api/v1/raft/status` 成功；`self_addr`（`a:9100`）只用于 mesh gRPC。
- **Store**：`Store::open` 在 `cluster.stretch` 时强制 `inline_threshold==0`、
  `nodes.len()==2 && drives.len()==12`；`Q_s=9` 沿用（`store.rs:1009`）；
  `write_meta_cluster`/`read_meta_cluster` 不改协议。零字节对象仍写 12 槽 meta、commit ≥9，
  但无 payload；读路径对零字节不得尝试远端 data shard。
- **state.rs / cli.rs**：
  - `[stretch]` 分支构造 `Vec<NodeSpec>`（本地 6 盘，`addr=self_addr`；peer 6 盘，
    `addr=peer_addr`）→ 排序 → `ClusterSets::new_stretch`
    → `TonicMesh::new_with_timeouts(self_id, mesh_secret, sorted_nodes, connect_ms, timeout_ms)`
    （`xl-client`；`new` 保持无 secret 的 M7 默认，stretch 必须走带 secret 的新构造）。
  - `cli.rs` 增 `[stretch]` mesh gRPC server 分支：当前 `cli.rs:132-197` 只在
    `cfg.cluster` 下启动；S4 必须在 `cfg.stretch` 下用 `self_addr` 绑定并调用
    `xl_client::serve(store, addr, mesh_secret)`（secret 由 interceptor 校验，见 §2.5），
    bind 失败启动失败。**这是产品二进制
    交付，不是 e2e harness 绕过**；`[cluster]`/`[stretch]` 互斥，不会双起。
  - `self_addr`/`peer_addr` 规范化：接受 `http://host:port`、`host:port`；拒绝 `https://`、
    带 path/query、末尾 `/`；缺失端口报错；写进 `NodeSpec.addr` 前统一格式，确保两端
    `TonicMesh` 一致。
  - **P0-1**：`SqlReplSink::new(pool, store, site_data_replication=false)`（S1 已加参数）；
    `[stretch]` 下对象写入不得调用 `enqueue_sites`；`cli.rs` 不 spawn M7 `site` 复制 worker；
    启动断言 `stretch && site_data_replication` 为配置错误。e2e 断言每站点只有 1 个版本、
    `replication_outbox` 无 `target_kind='site'` 行。
  - 与 `[raft]` 构造并列；witness 不在此拓扑。
- **读写语义/失败测试**：
  - 12 槽、每站点 6：对 1000 个 hash 断言每个 node 恰好 6 片，且每 node 至少 2 个 data 片，
    故单站点 <8；B 本地为第二节点的输入顺序也断言同一槽序。
  - 写入成功 → 12 槽 meta，commit ≥9；停 B → `stage_payload_multi` 6 片 <9 → 写失败；
    `read_meta_cluster` 6<9 → 读失败。
  - **mesh 写失败**：逐节点/逐分片注入远端 5xx/超时，断言首个失败即返回错误、不产生
    commit、已写临时分片清理；不得把远端 6 片当本地成功（`mesh_api.rs:71-89` 的归属拒绝
    不得出现在正常写入路径）。
  - `inline_threshold=0`：1 byte 对象 meta 无 inline 数据；零字节对象无 payload（文档例外）；
  - 两站点齐 → GET 内容/ETag 相等；range read；删除/覆盖成功；
  - 停 B 时删除返回错误，恢复后重试收口（不承诺跨站原子删除）；
  - 单盘/单分片缺失 → 8/12 重建；`fsck --repair` 经 mesh 修复；WAN 超时/慢盘 fail-closed。
- **e2e**：S4 用真实 `oss-server` 二进制起两站点（`[stretch]` + `[raft]` 单节点 dev 即可），
  验证 `cli.rs` 的 `self_addr` mesh server 真正监听、对端 `xl_client::serve` 可达、A 通过
  真实 `mesh_write_shard` 写 B 的 6 个远端槽 6/6 成功；不得只用测试 harness 直接调
  `xl_client::serve` 绕过产品启动路径。`e2e_stretch.sh` 记录单站点 != 2xx、meta 镜像
  12/提交 ≥9、每站点 6 片、B 本地为第二节点、无 site outbox；完整三节点 Raft 在 S9 跑。
- **风险**：WAN 64 KiB/块、6 远端分片/块；只承诺合规/小对象；无站点容灾；S4 估时可能上浮 2–3 d。

### S5 Raft 基座单节点 + witness runtime（6 d）

- `Cargo.toml` workspace `openraft = { version="=0.9.25", features=["serde","storage-v2"] }`；
  `oss-server` 引用；integration owner 管 lockfile。**S5 入口清单**：精确 manifest 下
  `cargo check --workspace` 绿；v2 `RaftLogStorage`/`RaftStateMachine` 直接传给 `Raft::new`；
  不加 `Adaptor`；**不用 `async-trait`**（原生 `async fn`）；`generic-snapshot-data` 关闭；
  两者均持久化完整 `LogId` 与 `StoredMembership`。**S5 出口**：上述条件仍在，且 HTTP
  快照 build/upload/install 通过。
- `raft/{mod.rs,store.rs,net.rs,api.rs,snapshot.rs}`：
  - `SqliteLogStore`：`raft.db` WAL + `synchronous=FULL` + 5 s busy_timeout；`raft_log`/
    `raft_meta`；实现 v2 `RaftLogStorage`；
  - `SqliteStateMachine`：v2 `RaftStateMachine`；apply 写主库 + `raft_applied`（完整 LogId +
    membership JSON）同事务；`applied_state()` 返回 `(Option<LogId>, StoredMembership)`；
  - `net.rs`：HTTP/1.1 到 `advertise_endpoint`/peer；`x-oss-raft-token`；append/vote 32 MiB；
    `install_snapshot` 走 4 MiB 原始分块，JSON 约 16 MiB/请求，路由 `DefaultBodyLimit=32 MiB`；
    `install_snapshot_timeout_ms` 用 WAN 值；`begin_receiving_snapshot` 接收
    `Cursor<Vec<u8>>`；`client_write` 外层应用 deadline；`RPCError::PayloadTooLarge` 拆批；
  - `snapshot.rs`：11 表 + membership + applied LogId + checksum；分块上传/下载；默认上限
    512 MiB，配置 `raft.snapshot_max_bytes` 可调，硬上限 1 GiB；记录每块实际 wire size；
    超限明确报错；
  - `api.rs`：`GET /api/v1/raft/status`、`/api/v1/site/raft/status`；
  - `mod.rs`：`RaftHandle`、`init_single()`、`pub fn spawn()`（测试可调）。
- `state.rs`：`AppState` 增 `raft: Option<Arc<RaftHandle>>`（S0 占位）；`load` 在 Store/caches 后
  构造；`cli.rs:199-256` spawn（witness 不 spawn 对象 worker）。
- `router.rs`：append/vote/join/write/snapshot 路由；`DefaultBodyLimit` **32 MiB
  （append/vote 与 snapshot chunk 一致）**；`state.rs:605-610` JWT except。
- witness：`witness.rs` + `cli.rs::witness`；`WitnessConfig` 读
  `[raft].role="witness"`、`node_id=3`、`bootstrap=false`、`advertise_endpoint`、
  `cluster_token`、`db_path`（raft.db）、`state_db`（control.db）；
  `Db::connect_witness` **跳过 `site_bootstrap`/`site_config`**（`db.rs:230-275` 不能复用
  `Db::connect`）；控制面 SM/no-op 副作用；routes `/health`、`/api/v1/site/identity`、
  `/api/v1/site/raft/{append,vote,snapshot,status,membership}`；`role=witness` 不走
  `AppState::load`，不创建 Store/对象目录；C 不写入 `sites` 表。
- bootstrap：`bootstrap=true` `initialize({self})`；`bootstrap=false` 且 `raft_applied` 无
  membership → 等待 join/leader；已有 membership（含 force recovery 产物）→ 直接恢复，**不**
  再次 initialize；单节点 dev 允许；3-voter 由 S7 完成。持久化
  node_id/bootstrap/initialized；变更拒绝启动。
- **测试**：`raft_store.rs`：append/truncate/purge/vote/重启/单节点 snapshot/status；
  `raft_witness.rs`：witness 启动、应用命令、快照、无对象目录、`site_config` 为空、
  `/api/v1/site/identity` 正确；`raft_membership.rs`：restart 后 `applied_state().1` 保留
  （S7 扩展 3 voters）；`raft_snapshot_http.rs`：真实路由 build/upload/install 4 MiB 分块
  （约 16 MiB JSON）在 32 MiB limit 下通过，21.4 MiB 命令与 `PayloadTooLarge` 拆分；
  workspace `cargo check` 证明 `storage-v2` 生效、无 `async-trait`/`Adaptor`。
- 出口：七闸；S0 的 `RAFT_READY` **仍 false**（S6 出口才翻）。

### S6 命令模型 + 单节点路由（11 d，控制轨瓶颈）

- `raft/command.rs`：`ControlCommand`（serde）：
  - User：`Create{username,password_hash,created_at}`、`Delete`、`SetPassword`、`SetStatus`、
    `SetPermissions{username,rows}`；
  - AK：`Create{access_key,secret,username,created_at,expires_at}`、`Delete`、`SetStatus`；
  - Bucket：`Create{name,owner_username,creation_date,versioning,object_lock,
    object_lock_default_mode,object_lock_default_days,default_encryption,replication_mode,
    deleting:false}`、`Delete{name}`、`SetVersioning`、`SetObjectLock`、`SetLockRule`、
    `SetDefaultEncryption`、`ClearDefaultEncryption`、`SetNotification`、`ClearNotification`、
    `SetLifecycle`、`ClearLifecycle`、`SetReplicationRules`、`ClearReplicationRules`、
    `SetPolicy`、`DeletePolicy`、`SetBucketDeleting{name,deleting}`；
  - AP/OLAP/Inventory：`Put`/`Delete`（全字段；module base64）。
  `ControlResponse::{Ok,BucketNotEmpty,NotFound,Conflict,Invalid}`；命令上限 24 MiB。
- `raft/apply.rs`：抽 `db.rs` 控制写 SQL 为 `control_tx::*`（Local/Raft 共用）；delete_user 级联；
  bucket UPSERT 不写 set_id；apply 失败 fail-stop；返回 side effects；**每条 entry 只返回
  一个 `R`**（不是 `Vec<R>`），`ClientWriteResponse.data` 类型为单个 `R`。
- `raft/control.rs`：Local/Raft；admin 用户/AK 本地直写；follower 转发（cluster token）；
  无 leader 503 + hint；`wait_applied` 有界；**每次 `client_write` 外层
  `tokio::time::timeout(write_timeout_ms)`，超时 503**；append 遇
  `RPCError::PayloadTooLarge` 按字节上限拆批（单命令不可拆，入口 4xx）；转发超时文档 +
  重试 Conflict/NotFound。
- 路由覆盖：console IAM/AK/permissions/bucket/policy/AP/OLAP/Inventory/Lifecycle/Events；
  S3 桶配置 `BucketControlSink`（create/delete/versioning/object_lock/lock_rule/
  default_encryption/notification/lifecycle/replication_rules/policy）；M7 下 `None`。
- create：`db`/`access`/`console` 三个 create writer 统一为全列 `control_tx::create_bucket`
  （含 `deleting` 列，S0 迁移）；mode 冲突 409；guard 5xx；
  console 不再 warn+204。`[[groups]]` + raft 启动拒绝；`upsert_bucket_set` 跳过。
- 桶删除：**仅 sync 桶 + Raft 模式**。API 先 `SetBucketDeleting{true}`（经 Raft 复制）→
  `Store::begin_delete` 置 deleting 并等本站 active-writer 计数归零（30 s 上限）+ token
  `POST /api/v1/site/bucket-quiesce {bucket}` 冻结对端写入并等对端计数归零（30 s 上限）
  → 两端 bucket-empty 检查；任一步超时 → `SetBucketDeleting{false}` + 409/503；非空 →
  `SetBucketDeleting{false}` + 409；空 → `POST /api/v1/site/delete-bucket` → `Delete{name}` 命令只
  删行；本地 side effect 原子 rename tombstone（`bucket/.oss.sys/trash/<uuid>`）。失败保留
  `deleting=1` 与 `sync_delete_intents(delete_bucket)` 周期重试，API 503。**M7 async 桶不
  走此流程**，保持 `service.rs:804-858` 行为；active-writer 定义见 §2.3（同一锁
  check/increment + RAII）。不再使用“两站点空检查保证”作为无竞态论据（P1-5）。
- 启动：回填跳过复制表；admin bootstrap 本地；隐藏写抑制；`reconcile_bucket_dirs`；
  side effects 重试；对象 repl worker 保留（`cli.rs:212`），只停 `cli.rs:207-210` site worker。
- 守卫测试：直接 INSERT/UPDATE 复制表 ABORT；update 把 `is_admin=0` 改 1 被拒；
  session AK 例外；M7 模式 guard 不生效。
- 路由测试：recon-control §2 每条 handler 一条命令；M7 行为不变；隐藏写不落库；
  未路由 writer 返回 5xx 不是 204；桶删除仅 follower 有对象 → 409；**M7 async 桶删除
  仍走原本地路径且不调用 quiesce/内部路由**。
- openraft API/尺寸：`apply` 单 entry 单 `R`、`ClientWriteResponse.data` 为单 `R` 的编译
  断言；21.4 MiB module 命令通过 24 MiB cap、超过 24 MiB 4xx；append 批在
  `PayloadTooLarge` 后拆分重试；`write_timeout_ms` 到期返回 503。
- 快照/重启：`reconcile_bucket_dirs` 补建 row 缺目录、tombstone orphan；kill 在 apply 与
  mkdir 之间后重启可恢复。
- 出口：七闸；**翻 `RAFT_READY=true`**；单节点 3 表命令矩阵全绿。

### S7 3-voter 成员/双边配对/快照/cutover（10 d）

- **identity**：`site_json`（`console.rs:2521-2534`）增 `advertise_endpoint`、`raft_node_id`、
  `raft_bootstrap`、`raft_initialized`、`raft_role`、`admin_username`、`vault_probe`、`raft_enabled`；
  `[stretch]` 时再增 `stretch {node_id, self_addr, peer_node_id, drive_count, ec, placement,
  mesh_secret_fingerprint}`（S4 已消费，S7 只是接线与校验）。配对预检逐项比对
  `self_addr`/`peer_node_id`/`drive_count`/`ec`/`placement`/fingerprint，不匹配 409。
- **双边配对**：`POST /api/v1/sites`（`console.rs:2602+`）：
  1. 拉对端 identity；预检 bootstrap 恰好一侧、node_id 唯一、admin 名/vault/region；
  2. 本地写/更新 `sites` 行（`raft_node_id`、endpoint、peer token、AK/SK）；
  3. 调对端 `POST /api/v1/site/raft/join`（cluster token + 本站 site_token），对端写/更新
     本站行；失败回滚/重试幂等；
  4. leader `initialize({self})`（未初始化）→ `add_learner(peer, blocking)` →
     `change_membership({self,peer})`；
  5. M7 已有 `sites` 行：adopt/update，不再 `count_sites>=1` 拒绝（`console.rs:2645`）；
     不同 peer → 409。
- **witness 加入**：`raft-add-witness`（S5 CLI）→ `add_learner(3)` → `change_membership({A,B,C})`；
  替换用新 node_id 先加 learner → `change_membership` → 移除旧节点；C 不在 sites。
- **leader-aware membership（P1-3）**：`POST /api/v1/site/raft/membership` 与
  `raft-add-witness` 不假设本机是 leader：非 leader 返回 `leader_endpoint` 或由 CLI 用
  `Raft::membership()` 发现 leader 后转发；仅 leader 执行 `change_membership`；结果以
  `Raft::membership()` 实际返回为准，禁止硬编码 `{A,B,C}` 或 `{A,B}`。C 可当选 leader：
  测试 C 为 leader 时 A 发起的加/换成员请求经转发成功。
- **快照**：只含 11 张复制表 + membership + applied LogId；单事务安装：ctx→清 11 表（保留
  admin/session AK/本地表）→插入→membership/applied→commit→cache 失效/桶对账；
  安装后删除 user 不存在的 session AK；admin 权限行清空并文档；checksum 失败拒绝。
  快照行按自然键逻辑化：users 按 username（不含本地 id）、**permissions 按
  `(username,bucket,can_read,can_write,can_delete)`（不是 resource/action；admin 目标
  映射）**、buckets/AP/Inventory owner 按 username（admin 标记）、access_keys 按
  access_key/username；安装方重分配本地 `user_id`，witness 的 `owner_id=0` 视作 admin。
  避免源/目标自增 id 不同导致 ACL 错主或 PK 冲突；测试覆盖非 admin 用户权限、admin
  权限行、AP/Inventory owner 重映射，并逐列断言三个权限位不丢失。
- **B' 替换（P1-4）**：新数据站点 B' 用新 node_id 加入后，对每个 sync 桶跑
  `sync-reconcile --repair`：按同 origin id 从 A 重推缺失对象/版本，刷新
  `remote_version_id` 映射，清理指向旧 B 的 `sync_write_intents`/`sync_delete_intents`
  与旧站点 outbox；repair 完成前 B' 不标可服务。M7 async 对象按备份/重放恢复，不在
  repair 中伪造。测试：B 永久删除后 B' 加入、repair、两端版本删除。
- **force recovery（P1-5）**：`raft-force-recover` 只接受“全新空 `raft.db` + 11 表业务导出”
  输入：导入业务表 → 新 `raft.db` → `initialize({N})` 单节点 → 逐台 `add_learner` →
  `change_membership`；不写“epoch”，不编辑/复用旧 `raft.db`，不手改 log；对象数据从备份
  恢复；缺一即拒绝。测试：两节点永久丢失后从导出重建，membership 与业务字段一致。
- **follower 转发**：`POST /api/v1/site/raft/write`（cluster token）；无 leader 503 + hint；深度 1。
- **cutover**：
  - `raft-export` 导出 **11 张复制表**（含 buckets 全列；users/permissions/access_keys 导出
    username 关系，AP/OLAP/Inventory/lifecycle/events/replication 全字段）；owner_id 同时导出
    username，cutover 按目标本地 id 重映射（admin→NULL/0，按表约束）；避免两站点自增 id
    不同导致 ACL 错主；
  - `raft-cutover`：M7 lag=0、`site_apply_dead` 空、版本相同、admin/vault 一致、无 cluster；
    源优先/冲突中止/`--dry-run`/`--overwrite`；写 `schema_meta('raft_mode')='on'`、
    `raft_cutover`；确保无 `[raft]` 启动也被 guard 拒；
  - 启动 A（bootstrap）→ B（false）→ 配对 → 加 C；A/B/C 三成员 applied 后写 done
    （C 由快照追平）。快照与 cutover 都按自然键重映射 permissions/owner。
- **成员移除**：`DELETE /sites` 有 sync 桶 409；无 sync 桶可 `change_membership` 移除对端并删行；
  `raft-remove-node` 不允许 <2 voters；替换/永久丢失按 B' 修复或 §2.6 force recovery。
- **测试**：`raft_three_voter.rs`：3 voters、A/B/C 身份、kill B 后 A+C 写、kill C 后 A+B 写、
  A+B 宕 503、重启 membership 保留、快照安装后 election、C 为 leader 时成员变更；
  `raft_pairing.rs`：双边行、B-originated 写、M7 adopt、self endpoint、错 token 403；
  `raft_cutover.rs`：11 表全列 audit/冲突/dry-run/permissions
  `(username,bucket,can_read,can_write,can_delete)` 重映射；
  `raft_force_recover.rs`：全新 raft.db + initialize({N}) + learners；`raft_repair.rs`：
  B' 同 id 重推/mapping 刷新/旧 intent 清理。
- 出口：七闸；`RAFT_READY` 已 true；e2e_sites M7 仍绿。

### S8 故障/读语义/可观测 + 验收 ①④（5 d）

- 单数据站点宕机：控制写走 quorum 2（A+C/B+C）继续；sync 写/删 503；重启 log/快照追平。
- witness 宕机：A+B 继续；无 witness 时单数据站点宕机 → 503（明确告警）。
- A+B 同时宕机：C 无 quorum，控制写 503；恢复后追平。
- 成员/leader：C 可当选 leader；follower 收到的成员变更转发到 leader；`Raft::membership()`
  与实际 voter 集一致；kill leader 后新 leader 完成未决写。
- follower 读：本地应用状态；status `lag`/`stale`；不做线性读；撤销在 follower 应用前仍有效
  （安全章节）。
- 转发超时：文档“可能已提交”；重试返回 Conflict/NotFound；测试。
- **sync 写 intent 崩溃**：在远端 ack 后、本地 meta commit 前 `kill -9`；重启
  `recover_write_intents` **只补偿远端**：本地 meta 不存在 → 远端同 id token 删除（404
  视为成功）；存在 → 只清 intent/tee；**不得从 tee 完成本地提交**；断言客户端在 kill 后
  永远看不到该版本、不产生双版本/半版本，重试幂等；在 commit 后、intent 清理前 kill 也
  不重复。MPU 同规则，并覆盖 tee 存在但本地无 meta 的场景。
- **sync 删除 intent**：远端 ack 后 kill；周期 worker 不重启也能补齐本地删除；marker HEAD
  的 200/405/404 三态都收敛；DeleteObjects 部分成功 503 契约（marker 列表 at-least-once）。
- **删除入口一致性**：Batch Delete、console 对象删除、console 桶删除在 sync 桶上不产生
  “本地已删/远端仍在”；对端停时返回 503/409；M7 async 桶行为与 `e2e_sites.sh` 一致。
- **sync 映射对称**：A 发起的版本/marker 删除在 B 执行；B 发起的在 A 执行；S1 前/切换期
  对象经 `sync-reconcile --repair` 后同 id 可删。
- **bucket 删除竞态**：sync 桶 deleting 标志下并发 PUT/MPU/Copy 503；quiesce 后空桶删除；
  非空 409；远端失败保留 intent 且可重试；**B3 在途写**：B 卡住已过 `begin_write` 的 PUT，
  A 的 DeleteBucket 必须等 B 计数归零或 30 s 超时回滚；**B4**：接收端内部路由无 outbound
  调用（no loop），四类外部 caller 无直接 store 删除；**M7 async 桶删除仍走原本地路径，
  不置 deleting、不调 quiesce/内部路由**（回归测试）。
- **stretch**：每站点仅 1 个版本、`replication_outbox` 无 site 行；B 本地为第二节点时槽序
  正确；远端分片失败 fail-fast；零字节/1 字节 meta 12/9。
- **快照/权限**：非 admin 权限行、admin 权限、AP/Inventory owner 在安装后仍指向正确用户；
  B' 替换 repair 后同 id 版本删除成功。
- 指标：`oss_raft_role`、`oss_raft_term`、`oss_raft_last_applied`、`oss_raft_members`、
  `oss_raft_production_ready`、`oss_raft_leader_changes_total`、`oss_raft_apply_errors_total`、
  `oss_raft_forward_total`、`oss_raft_snapshot_bytes`；`oss_sync_acked_total`、
  `oss_sync_failed_total`、`oss_sync_write_intent_recovered_total`、`oss_sync_delete_partial_total`、
  `oss_sync_delete_recovered_total`、`oss_sync_ambiguous_total`；`oss_stretch_slots_ok`。
- 验收测试：① 3-voter 并发写 + 归一化 diff；④ kill/restart/snapshot/cutover + membership
  相等；② 与 S3 共用；③ 与 S4 共用；上述故障注入全部纳入 S8 清单。
- 出口：七闸；验收 ①④ 全绿。

### S9 e2e + console + 文档（5 d）

- `tests/e2e_m9.sh`（3 进程 A/B/C）：
  1. 生成本地 toml（A bootstrap、B false、C witness；`[sync].timeout_ms=1000`）；
  2. 双边配对/加 witness → status 3 voters、`production_ready=true`；从 follower 发起
     membership 查询/转发；C 为 leader 的用例；
  3. A/B 并发控制写 → 归一化 diff=0；kill B → A+C 控制写 200；kill C → A+B 200；
     kill A+B → 503；重启追平；
  4. sync：PUT 200 + 远端 GET tags/class/内容；SIGSTOP B → PUT 503/本地 404/无 outbox；
     DELETE/DeleteObjects/版本删/lifecycle 过期 → 503 且本地仍在；SIGCONT → 200/204，
     断言对象不可见/同 id 版本删除成功（**不比较版本列表逐项相同**，lifecycle 可能多 marker）；
     Batch Delete、console 对象删除、console 桶删除在 sync 桶上同样 503/409 且不本地直删；
     从 B 发起版本/marker 删除；`sync-reconcile --repair` 后 M9 前对象同 id 可删；
     远端 ack 后 kill -9 的写 intent 按补偿恢复、客户端看不到版本；桶删除并发 PUT 503/非空 409；
  5. sync 边界：versioning Suspended 409、object lock 409、SSE-C 400、tag 501、
     lifecycle Transition 400；内部路由缺/错 `x-oss-site-token` → 403，S3 DELETE 不带 token
     仍走 remote-first（代理剥离不会降级）；async 桶在 Raft 下仍复制（对象 worker 保留）；
  6. 收尾：解配/停进程/临时目录清理。

- `tests/e2e_m9_recovery.sh`（灾备）：M7 export/cutover 11 表全列 diff、permissions
  `(username,bucket)` 与 `can_read/can_write/can_delete` 三列、owner 重映射、B' 替换后
  repair，并从 A/B' 两侧删除同一旧版本；force-recover 后 `bootstrap=false` 重启。
- `tests/e2e_stretch.sh`：不依赖 witness C（S4 数据面专用；三 voter/witness 用例在 `e2e_m9.sh`）；
  两站点 × 6 盘、8+4、inline=0；**真实 `oss-server` 二进制**用 `[stretch].self_addr` 启动
  mesh gRPC server（不是测试 harness），两站点 mesh 互通；真实 mesh 分片写/读/range；
  停 B → GET/PUT 非 2xx；恢复 → `fsck --repair`；断言每站点 6 片、meta 镜像 12/提交 ≥9、
  B 本地为第二节点、每站点仅 1 个版本、`replication_outbox` 无 site 行（M7 async 未双发）；
  单站点故障不承诺可用；至少一个用例断言 mesh 端口在启动日志/health 中可用；
  **B 必须能通过 A 的 `advertise_endpoint`（主 HTTP，非 `self_addr`）请求
  `/api/v1/raft/status` 成功**（B1）。
- `e2e_sites.sh`（M7 回归，不改脚本）+ `e2e_m8.sh`。
- force recovery 证据：kill 两节点 → 从存活数据站点 `raft-export` → 新空 `raft.db` +
  `initialize({N})` + learners；**仅 C 存活**用例跑 `raft-export --witness`（读 `state_db`），
  断言只恢复控制面 11 表、对象数据标记为需从备份恢复。
- console `Cluster.tsx` 增 3-voter/witness 卡（role/leader/term/applied/members/lag/
  production_ready/sync_available/stretch_available）；`api.ts` 类型；Playwright。
- 文档：README 模式矩阵（site/cluster/stretch/raft）、witness 部署、sync 写/删语义
  （同 origin version id、remote-first、内部 token 路由、写 intent compensate-only、
  DeleteObjects 503 部分成功 + marker at-least-once、MPU tee 明文与 0600/TTL、
  `sync-reconcile [--repair]`）、stretch 限制（无站点容灾/WAN 慢/inline=0/禁用 M7 async/
  禁 sync/仅全新空库/`self_addr` mesh server）、force recovery runbook（新 raft.db +
  `initialize({N})`，仅 C 存活时 `raft-export --witness` + 对象从备份）、TLS 私网要求；
  设计 §7.3/§M9 与 feature spec §10.2 增实现裁决、③ 2×6 说明，并**同步修正 sync+stretch
  的规格收窄**（设计 §7.3 / feature spec §10.2）。
- 出口：七闸 + web test/build + 五个 e2e 全绿；最终验收证据填入 §7。

## 6. 风险登记

### P0（阻断/数据正确性）

| 风险 | 缓解 |
|---|---|
| 漏接控制写 → 分叉 | 11 表 guard fail-stop；recon-control §2 清单逐项打勾；`raft_routing` 每条 handler；e2e 归一化 diff |
| apply 非确定（hash/seal/now/uuid/set_id） | 命令预计算；`control_tx` 共用；双库业务字段相等；`set_id=0` 断言；module base64 |
| 0004 重放失败/guard 被绕过 | `IF NOT EXISTS`/`DROP TRIGGER IF EXISTS`/`INSERT OR IGNORE`；连续 connect；OLD.is_admin 加固 |
| 降级/配置误启 M7 + Raft 双写 | 无条件 guard；`raft_mode` 持久化；无 `[raft]` 拒绝；启动断言：Raft 下 `site_*`=0、`raft_guard_*`>0；M7 下 `raft_guard_*`=0 |
| M7 被 Raft guard 误伤 | `prepare_raft_mode(false)` 启动时 drop `raft_guard_*`；`e2e_sites.sh` 回归；0004 只建不删 |
| StoredMembership 丢失 → 重启丢 voter | 完整 LogId+membership 同事务；快照 meta；3-voter 重启/选举测试 |
| sync 本地先可见 / 远端未确认 | gate 固定在 Version 前（`put.rs:687`/`mpu.rs:525`）；SIGSTOP e2e；失败清理 |
| sync 写 ack 后本地崩溃 → 远端孤儿/双版本 | `sync_write_intents` 远端前落盘（tee+metadata/SSE intent）；同 id 重放；恢复**只补偿远端**（本地 meta 不存在即删远端，存在即清 intent/tee）；kill -9 边界测试 |
| sync 版本映射不对称 → 另一端不能删 | 同 origin UUID 为两端 version id；`x-oss-origin-version-id` 签名；marker 预生成；两端删除测试；`--repair` |
| sync 删除本地先删 / 补偿被拒 | S3 层 remote-first；token 认证本地直删；`sync_delete_intents` 周期恢复；e2e 503/204 |
| stretch 写被 M7 async 再发一份 | `[stretch]` 强制 `site_data_replication=false`；`SqlReplSink` 不调 `enqueue_sites`；e2e 每站 1 版本/无 site outbox |
| stretch 节点顺序不一致 → 分片错位/写失败 | `new_stretch` 按 node_id 排序后求 `self_idx`；两端 identity 互校；B 本地第二的测试；真实 mesh 分片写 |
| stretch 单站点可读 / Q_s 不达 | 2×6+placement 证明 6/6；inline=0；Q_s=9；停站 e2e；preflight 放置验证 |
| stretch 配置误用（6 盘被 parity 拒/非 8+4） | `new_stretch` 硬校验；启动失败；配置文档 |
| witness 单点误解 → 两站点宕仍写 | quorum 2 语义测试；status 明示；A+B 宕 503 |
| 快照半途 → 分叉 | 单事务 + ctx + checksum + membership；崩溃点测试 |
| openraft 依赖缺 `storage-v2` → S5 不编译 | 精确 manifest 改 `features=["serde","storage-v2"]`；S5 入口 `cargo check --workspace`；原生 async fn/无 Adaptor |
| stretch mesh 数据面不可启动 | `[stretch].self_addr` 必填；`cli.rs` stretch 分支真实启动 `xl_client::serve`；真实二进制 e2e；identity 校验 self/peer 地址 |
| 快照 4 MiB 分块 → ~16 MiB JSON 被 4 MiB 路由拒绝 | 快照路由 32 MiB；preflight/S5 走真实 HTTP build/upload/install 并记录 wire size |
| Batch/console 删除绕过 remote-first → 静默分叉 | 共享 `SyncDeleteService`；四类外部 caller 全改且禁直接 store 删除；接收端只调 local-only（无回环）；对端停 503/409 测试 |
| MPU 无 plaintext tee → gate 无法远端写 | Complete 拼接时写 tee；tee 缺失在远端调用前 500；清理/孤儿；kill-9 测试 |
| permissions 快照键错 → ACL 错主/丢位 | `(username,bucket,can_read,can_write,can_delete)`；安装回填 `user_id`；三列逐项测试 |
| DeleteBucket 误伤 M7/无 counter → 竞态 | 新流程仅 sync+Raft；M7 原路径回归；`Store` 每桶 active-writer + deleting 同一锁/RAII；并发 PUT 测试 |

### P1（可用性/边界）

| 风险 | 缓解 |
|---|---|
| witness 永久丢失 | A+B 仍 quorum；新 node_id 加 learner → 改成员 → 移除旧节点；runbook |
| 数据站点永久丢失 + sync 桶 | 替换站点/以存活站点为源补数据；有 sync 桶时 DELETE 409；runbook |
| 两节点永久丢失 | 离线 `raft-force-recover`；优先从存活数据站点导出；仅 C 存活时 `raft-export --witness` 只导控制状态、对象从备份；禁止手改 raft.db；明确丢未提交写 |
| stretch WAN 性能/超时 | 64 KiB/块、6 远端片/块；仅合规/小对象；`mesh_timeout_ms`；preflight 测量 |
| stretch 无站点容灾 | 文档/UI/验收明示；不做虚假承诺 |
| sync 删除超时歧义 | intent + `head_object_version` 200/405/404 三态 + `oss_sync_ambiguous_total`；内部 token 路由；文档 |
| sync marker 重试重复 | 明确 at-least-once：对象不可见/同 id 版本删除幂等；不承诺 `ListObjectVersions` 列表逐项相同；文档 + e2e 断言 |
| ReplClient 超时回归 async | 共享 client 无默认；sync 每调用；async 大超时 |
| 转发/命令超时歧义 | 文档 + 重试 Conflict/NotFound；测试 |
| tags/class/SSE-C 边界 | token-verified sync replica 保留；SSE-C 400；tag 501；测试 |
| follower 授权陈旧 | 文档明示；不做“即时撤销”承诺 |
| openraft 0.9.25 API/尺寸 | `features=["serde","storage-v2"]` 精确 manifest 编译；原生 async fn/无 Adaptor；install timeout/WAN/purge/PayloadTooLarge/单 R/Cursor；HTTP 快照与 16 MiB module；pin；失败即停 |
| 3-voter 成员变更竞态 | learner 追平后 change_membership；单写手 CLI；幂等 join；超时回滚；leader-aware 转发，C leader 测试 |
| cutover 丢列 | 11 表全列 export/audit；permissions/owner 自然键重映射；冲突中止；dry-run |
| 桶删除与并发写竞态 | 仅 sync+Raft 走新流程；M7 原路径不变；`Store` 每桶 active-writer + deleting 同一锁/RAII；对端 quiesce；并发 PUT 503；非空 409；失败 intent 重试 |
| B' 替换后旧映射/旧 intent | `sync-reconcile --repair` 同 id 重推、刷新 remote id、清理旧站点 intent；repair 前不服务 |
| force recovery 误用旧 raft.db | 仅接受新空 raft.db + 业务导出；`initialize({N})` + learners；拒绝编辑/复用旧 log |
| 快照 permissions/owner 错主 | `(username,bucket,can_read,can_write,can_delete)` 自然键安装；回填 `user_id`；admin 映射；非 admin 权限/AP/Inventory 测试 |
| stretch 配置漏项 | `self_addr`/`peer_addr` 规范化、mesh server 启动、`mesh_secret`/6 盘/8+4/inline 0/全新空库/禁 sync 全部硬校验 |

### P2（体验/运维）

| 风险 | 缓解 |
|---|---|
| sync mode 缓存陈旧 | TTL 10 s + 创建/删除/apply 失效；mode 不可变 |
| `sites` 端点陈旧 | status 显示；配对/成员变更刷新 |
| 快照总量大 | 分块/默认 512 MiB 上限（可配 ≤1 GiB）；清理/导出提示 |
| 指标基数 | 按 site/结果，不带 key |
| 时钟偏差 | 命令时间戳 leader 生成，apply 不取 now |
| 运维复杂度（3 节点/6 盘/stretch） | 部署文档、状态卡、preflight 清单 |

## 7. 验证闸门与最终证据

**七闸（每片收口，沿用 M8）**
1. `cargo test -p xl-store`（lib，含 kill9）
2. `cargo test -p oss-server --lib`
3. `cargo test -p oss-server`（全部集成）
4. `cargo test -p xl-s3`
5. `cargo test -p xl-client -p xl-proto`
6. `cargo clippy --workspace --all-targets -- -D warnings`
7. `cargo fmt --all --check`

另列：`cargo test -p xl-s3 --features olap-wasm`；`npm --prefix web test`、`build`；
`e2e_m9.sh`、`e2e_m9_recovery.sh`、`e2e_stretch.sh`、`e2e_sites.sh`、`e2e_m8.sh`。

**分片闸门矩阵**

| 片 | 额外必跑 |
|---|---|
| P | 12 项 spike：storage-v2 编译/openraft v2 运行时/尺寸/C leader、HTTP 快照、0004 双 connect、配对、stretch 2×6 + `self_addr` 真实二进制 mesh、删除 intent + marker 405、write intent kill-9 compensate-only、version 同 id、MPU tee、Batch/console 删除路由、witness 无 `site_config` |
| S0 | `m9_contract`、`site_changelog`、xl-s3 create、M7 e2e |
| S1 | `xl-store` sync、`sync_gate`、`replication`、`site_data_replication=false`、503/403 |
| S2 | MPU/Copy、**MPU plaintext tee + kill-9**、tags/class、SSE-C/tag/lock/lifecycle、`replication_ops`、origin id |
| S3 | 删除 marker/version/DeleteObjects/lifecycle、**Batch/console 对象/console 桶四类 caller**、内部 token 路由 403、**B3 in-flight quiesce/30 s 回滚、B4 no-loop/local-only + 四 caller 禁直删**、marker HEAD 200/405/404、写/删 intent（含周期）、bucket quiesce/active-writer、`sync-reconcile --repair`、`sync_gate` 全量 |
| S4 | stretch 2×6 放置/Q_s/inline、节点排序、**`self_addr` + 真实 `oss-server` mesh server**、真实 mesh 写失败、停站、无 site outbox、**B1 advertise/self 端口分离 + B2 identity 六字段 mismatch**、`e2e_stretch` |
| S5 | **storage-v2 workspace 编译/原生 async**、`raft_store`、`raft_witness`、restart/membership、HTTP 快照 4 MiB/32 MiB、snapshot 512 MiB、运行时参数 |
| S6 | `raft_commands`、`raft_guard`、`raft_routing`、apply 单 R/`ClientWriteResponse`、`PayloadTooLarge`、`client_write` deadline、M7 回归、groups 拒绝、SetBucketDeleting（仅 sync） |
| S7 | `raft_three_voter`、`raft_pairing`、`raft_cutover`、leader-aware membership、C leader、B' repair、force recovery、permissions 三列、**stretch identity 六字段 mismatch** |
| S8 | 验收 ①④、故障注入（写 intent kill-9 **compensate-only**、删 intent/marker 405、MPU tee、Batch/console 删除、bucket 竞态/M7 回归、stretch 无 async）、指标 |
| S9 | 五个 e2e、web unit/build/Playwright、七闸全量 |

**最终验收证据**
- 七闸 exit 0 + 测试计数；clippy/fmt 无输出。
- `e2e_m9.sh`：3 voters、单站点宕控制写、A+B 宕 503、C leader 成员变更、sync
  PUT/DELETE 503/恢复、从 B 发起版本/marker 删除、写 intent kill -9 **compensate-only**
  恢复、marker 405 恢复、MPU tee、Batch/console 对象/桶删除、内部 token 403/剥离不降级、
  桶删竞态、async 回归；`FINAL FAIL=0`。
- `e2e_m9_recovery.sh`：cutover 11 表 + permissions 三列、`--repair`、witness/force-recover；
  `FINAL FAIL=0`。
- `e2e_stretch.sh`：两站点 6 片/9 槽/inline=0；**真实 `oss-server` 由 `self_addr` 启动
  mesh server**、真实 mesh 写；B 本地第二；每站 1 版本、无 site outbox；停站非 2xx；
  **B 经 A 的 `advertise_endpoint` 访问 `/api/v1/raft/status` 成功**；恢复修复；`FINAL FAIL=0`。
- 验收 ①：A/B/C 3 voters；单站点宕控制写仍 200；A+B 宕 503；并发写后 11 表按自然键归一化
  业务字段 diff=0；membership/LogId 重启保留；permissions
  `(username,bucket,can_read,can_write,can_delete)` 与 owner 在快照/cutover 后正确。
- 验收 ②：sync PUT 200 仅远端 ack 后；DELETE/版本删/DeleteObjects/生命周期 503 时本地未删；
  恢复后对象不可见/同 id 版本删除成功（**不要求版本列表逐项一致**）；A/B 两端删除同一
  对象版本均成功；无 outbox；`replication_mode=sync`；四类删除 caller 均无本地直删；
  写/删 intent 在崩溃后收敛（写只补偿远端）；`sync-reconcile --repair` 修复 M9 前映射。
- 验收 ③：2×6、8+4；每站点 6 片；`Q_s=9`；`inline=0`；停一站读写非 2xx；两站齐内容相等；
  每站仅 1 个对象版本、无 M7 site outbox；明确“无站点容灾”。
- 验收 ④：kill -9/重启/快照安装/cutover 后 `raft_applied`/membership 相等、业务字段归一化 diff=0；
  force recovery 只用新 `raft.db` + `initialize({N})` + learners；仅 C 存活时
  `raft-export --witness` 只导控制状态、对象从备份恢复。

## 8. 执行顺序与文件所有权

**阶段**
1. **P preflight**（7 d，单写手）：8 组/12 项 spike；不通过则停下改设计。
2. **S0**（3 d，单写手）：契约/迁移/配置/UI/占位字段/stub；能力开关 false。
3. **并行两轨**（S0 后）：
   - 数据面：S1 → S2 → S3（6+4+6=16 d）；S4 stretch（9 d）依赖 S0+S1，可在 S2/S3 期间
     并行，但只碰拓扑/`state.rs`/`cli.rs` mesh 分支/配置，不改 `replication.rs`；
   - 控制面：S5 → S6 → S7 → S8（6+11+10+5=32 d）；
4. **S9**（5 d）：S3/S4/S8 全绿后。
- `state.rs`/`Cargo.toml` 由 integration owner 串行合并；S0 预置字段/stub，S1/S4/S6 不互相等。

**文件所有权（同一时刻一个写手）**

| 文件/目录 | 归属顺序 |
|---|---|
| `crates/oss-server/migrations/0004_m9.sql`、`db.rs` | S0 → S5（`connect_witness`）→ S6 → S7 |
| `config.rs` | S0（之后只读；S4 只读） |
| `state.rs` | S0 → S1（`site_data_replication` 参数）→ S4（stretch）→ S5/S6/S7/S8 → S9（integration owner 串行） |
| `router.rs` | S0 → S3（bucket-empty/quiesce）→ S5 → S7（membership）→ S8 |
| `console.rs` | S0 → **S3（对象/桶删除 handler）** → S6 → S7 → S9 |
| `job_actions.rs` | **S3（Batch Delete 路由）** → S6（Batch 控制） |
| `lifecycle.rs` / `lifecycle_store.rs` | **S3（Expiration 走 `SyncDeleteService`）** |
| `replication.rs` | S1（之后只读；S4 不改） |
| `repl_client.rs` | S1（origin/token 头）→ S2 → S3 |
| `sync.rs`（新，含 `SyncDeleteService`/内部路由/`sync-reconcile`） | S1 → S2 → S3 |
| `raft/`（新） | S0 stub → S5 → S6 → S7 → S8 |
| `witness.rs`（新） | S5 |
| `crates/xl-store/src/store/{gate,put,mpu,error}.rs` | S1 → S2 → S3 |
| `crates/xl-store/src/meta.rs` | S1（`remote_version_id`）→ S3（`DeleteMarkerSpec`） |
| `crates/xl-store/src/store/version.rs` | S3（forced marker UUID/remote id） |
| `crates/xl-store/src/store.rs` | S0（StoreConfig gate/sync 字段 + `sync_mode` stub）→ S3（active-writer/deleting 计数）→ S4（stretch 分支） |
| `crates/xl-store/src/store/{drives,cluster}.rs` | S4 |
| `crates/xl-client/src/lib.rs`（`TonicMesh::new_with_timeouts` + `serve` secret/interceptor） | S4 |
| `xl-s3/`（convert/access/replication） | S0 → S1 → S2 → S3 → S6 |
| `crates/xl-s3/src/service.rs` | S0 → S1 → S2 → S3（删除/bucket）→ S6（sink/命令） |
| `cli.rs` | **S4（stretch mesh server/`self_addr`）** → S3（`sync-reconcile`）→ S5（witness/spawn）→ S7 → S9 |
| `Cargo.toml`/lock | integration owner（S5） |
| `web/` | S0 → S9 |
| `tests/e2e_m9.sh`、`e2e_stretch.sh`、`web/e2e/m9-*` | S9 |

**串行化**
- S4 依赖 S0+S1（`SqlReplSink` 参数与 `state.rs` 构造先落地）；S4 只碰 `drives.rs`/
  `store.rs` 拓扑分支、`state.rs` 的 stretch 分支与 `cli.rs` 的 mesh server 分支；S1–S3
  只碰 `gate.rs`/`put.rs`/`mpu.rs`/`meta.rs`/`version.rs`/`error.rs`；`StoreConfig` 字段
  S0 一次加齐。S4 不改 `replication.rs`，只传 `false`。`cli.rs` 的 S4 改动先合，S3 的
  `sync-reconcile` 与 S5 的 witness/spawn 后合。
- `service.rs`：S0/S1/S2/S3 数据轨，S6 控制轨；S3 先落 `SyncDeleteService` 与四类 caller
  （`service.rs`/`lifecycle*`/`job_actions.rs`/`console.rs` 删除 handler），S6 的桶配置
  sink 必须等 S3 合并后再动 `service.rs`；其余 S6（命令/apply/console IAM）可先做。
- `state.rs`/`cli.rs`：S4 stretch 分支先合，S5/S6 raft 分支后合；`cli.rs` 的 S4 mesh
  server 必须先于 S5 witness/spawn 合入；急时 S0 stub 已隔离字段。
- `console.rs`：S3（删除 handler）先合，S6（IAM/桶写）再合，S7（配对）后合，S9 只加卡片。
- `job_actions.rs`：S3 只改 Batch Delete 路由，S6 再改 Batch 控制；同一时刻一个写手。
- `lifecycle*`：S3 独占；S6 不动。
- 每写手独立 worktree/branch，按表顺序合并；合并前跑该片闸门；禁止两人同时改同一文件。
- 提交粒度：一片一提交（S6/S7 可子任务多提交），提交信息带片号；不混片。

## 9. 已知边界与后续

- **M9 不做**：TLS/加密传输、3+ 数据站点、stretch 站点容灾、Raft follower 线性读、
  sync exactly-once、跨站加密/对象锁强一致、M6 规则改造、M7 与 Raft 混跑。
- **stretch**：只承诺 2×6/8+4 的“单站点不可读、两站点联合可读”；单站点宕机读写失败；
  删除/覆盖沿用 M3 集群协议，单站点不可达时删除返回错误、恢复后需重试收口（可能本地 meta
  已删而远端仍在），**不承诺跨站原子删除**；WAN 每 64 KiB/分片 RPC，适合合规/小对象；
  `inline_threshold=0`；meta 仍镜像元数据；**M7 异步站点复制关闭**（每站仅一份版本，
  无 site outbox）；`self_addr` 必须有产品二进制 mesh server，否则无法形成两站点数据面；
  只允许全新空存储，禁止 sync 桶（对设计 §7.3/feature spec §10.2 的显式收窄，S9 更新规格）；
  stretch 引入两站点失败域，是对设计 §5.3 扁平拓扑的显式修订。
- **sync**：at-least-once；两端采用同一 origin version id，删除可双向；写/删 intent 是本地
  崩溃恢复日志，不是 outbox/发送队列；写崩溃恢复**只补偿远端**；MPU Complete 的 plaintext
  tee 是恢复/远端写所必需，文件 0600 且成功/失败即删，残留 >1 h 清理；站点间删除走内部
  token 路由（`/api/v1/site/delete-object` 等），普通 S3 DELETE 永不携带站点 token；删除超时可能
  有远端孤儿/重复 marker；生命周期两站点可能各产生 marker（不同 id），只保证对象不可见；
  版本映射缺失（M9 前/cutover/B' 替换）需 `sync-reconcile --repair`；DeleteObjects 任一远端
  失败 → 503（成功键已提交；重试在对象可见性层面幂等，无 versionId 的键可能再产生 marker，
  不保证版本列表逐项相同）；Batch/console 删除在 sync 桶上同样 remote-first；Tagging
  API/Batch tag 501；SSE-C 400；lifecycle Expiration 支持、Transition 不支持。
- **witness**：控制面 voter，不存对象；`Db::connect_witness` 不建 `site_config`；单数据站点
  宕机控制面可写但 sync 503；两节点永久丢失需离线 force recovery：优先从存活数据站点导出，
  仅 C 存活时 `raft-export --witness` 读 `state_db` 只导控制面 11 表，对象从备份恢复；
  新空 `raft.db` + `initialize({N})` + learners，未提交写丢失；A+B 同时丢失时 C 无对象数据，
  只能重建控制面；禁止手改/复用旧 `raft.db`。
- **运维**：3 节点私网、vault 密钥同源、时钟同步、stretch 6 盘/站点、force runbook；
  `DELETE /sites` 有 sync 桶 409。
