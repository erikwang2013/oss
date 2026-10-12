# 子账户与权限体系实施记录（2026-10-13）

> 需求：**管理员可以创建、管理不同的子账户并设置权限；上传认证可以使用不同的子账户。**
> 产品取舍（用户拍板）：子账户**可建自有桶**（桶主自动全权、删桶限桶主/管理员）；
> 权限**三档**（每子账户 × 每桶：读 / 写 / 删）；控制台视野 = 授权桶浏览+上传 +
> 自己的密钥 + 自己的上传记录，管理员多「子账户」页。
> 另：CI 触发改为仅手动（`workflow_dispatch`），push/PR 不再自动跑。

## 权限模型（唯一事实源）

`permissions(user_id, bucket, can_read, can_write, can_delete)`（0001_init.sql 追加，
IF NOT EXISTS 幂等演进；用桶名非 FK——删桶时 `Acl::forget_bucket` 清行，防重建同名桶继承旧授权）。

求值（`crates/xl-s3/src/access.rs::Acl`，三面共用）：匿名拒；admin 全允许；`buckets.owner_id == uid`
（桶主）全允许；否则查对应能力位，**无行 = 无权限（fail-closed）**。归类：read = 列举/下载/HEAD/
ListParts/HeadBucket；write = PUT/MPUT 四操作/**Abort**/**PostObject**；delete = DeleteObject(s)；
CopyObject = 源桶 read + 目标桶 write；CreateBucket 任何已认证用户（建后记账 owner）；DeleteBucket 桶主/管理员。
（Abort 归写、HeadBucket 归读是**有意口径**。）

## 三面落地

| 面 | 插入点 | 关键裁决 |
|---|---|---|
| S3（SigV4） | `s3s::access::S3Access::check`（反序列化前唯一入口） | `AuthedUser(uid,is_admin)` 经 `extensions_mut` 塞进请求（handler 读）；**PostObject 必须单独映射**（trait 默认实现委托 put_object，op 名是 "PostObject"，是不映射就漏的点）；未知 op fail-closed；CopyObject 用 `s3s::dto::CopySource::parse`（header 是 URL 编码、有 `/` 前导与 `?versionId`，别自己 split）；ListBuckets 在 handler 过滤（磁盘列表权威——历史桶可能无 DB 行） |
| 控制台（JWT） | `console.rs` 各 handler + 新 `/me`、`/users*` | 403 用纯文本（前端 resp.text() 直显）；`/me` 是前端角色唯一依据（JWT 只签 sub）；建子账户 API **恒 is_admin=false**（防提权）、口令 ≥8；删用户事务级联（FK ON：先删 keys/permissions、owner 置 NULL；不能删自己/最后管理员）；register_upload 修「任意挂名」洞（判 bucket 写权限 + 非管理员只能认领无归属/自己的记录） |
| aetherupload（JWT） | **自建四 handler**（`aether_gate.rs`），弃用 crate 的 `ecat::routes` | crate 的 handler 读不到身份。写路径按「分组→桶」判 write；display/download 判 read；403 走 crate 的 `Problem`/`problem_response`（JS 取 title 渲染）。表单解析照抄 crate axum 适配层（私有函数，逐字复刻）；`DefaultBodyLimit` 用 `body_limit(runtime)` 同值。**安全性依赖**：`save_chunk` 无 partial 直接 409 ⇒ 写数据必经 preprocess（门在其前）；秒传命中分支排在 partial 检查前——信息面同被前置门拦掉。归属登记在 preprocess/save_chunk 成功后 best-effort `set_upload_owner` |
| 前端 | `App.tsx`（/me → 菜单按角色、选中态改前缀匹配）、`Users.tsx`（用户表+建号+权限 Modal）、`Buckets/Objects` 按能力位显隐按钮 | — |

## 验证（全部实跑）

- 单测/集成：`Acl` 真值表 5 例 + op 分类表驱动 1 例、db 权限与归属全流程（含级联删用户）、
  控制台权限矩阵 4 例（/me 与角色、用户 CRUD 保护、桶可见性与三档门、自有桶、register 防挂名）、
  aether 门（未授权 403 problem+json 含「无权」/授权后 200/display read 门槛）。
- 前端：vitest 18/18（`--no-file-parallelism`，vitest 5 的 worker 启动超时为硬编码 60s）；
  `npm run build`（tsc -b）通过；Playwright **5/5 ×2 轮**——含新增 `web/e2e/subaccount.spec.ts`
  （admin 建号授权 → 子账户登录：菜单无「子账户」、只见授权桶、上传面 403 拒绝路径实弹）。
- e2e：`tests/e2e_s3.sh` 新增「15. 子账户权限」段（15a–15g：ls/get 放行、只授读上传 403、
  未授权桶 403、匿名 403、补授写后放行、ListBuckets 过滤、清理）；终版 **17 步 + 15 段全
  绿**（a6588d0，脚本零改动）。

## 实弹事故与修复（e2e 15 段逮住，管理员旁路曾掩盖）

- **症状**：子账户一切**对象级** S3 op（Get/Head/Put/Delete/Copy/MPU）403；桶级 op（列举等）
  正常；连桶主写自建桶也被拒。日志特征：`授权拒绝 op="HeadObject" bucket=None`。
- **根因**：`crates/xl-s3/src/access.rs` 用 `cx.s3_path().as_bucket()` 取桶名——s3s 0.17 的
  `as_bucket()` **只匹配 `S3Path::Bucket` 变体**，对象路径 `Object{bucket,key}` 恒返回 None →
  `can_opt(.., None)` 一律拒。修复：`get_bucket_name()`（Bucket|Object 都返回桶名，e7d2d50），
  并把 op 分类抽成纯函数 `classify()`/`Gate` 补表驱动单测。
- **教训**：授权层单测覆盖了真值表但覆盖不到「s3s 上下文→桶名」这段（S3AccessContext 字段
  不可外部构造）；**管理员的 check 早退跳过该段**——正是子账户实弹 e2e 的唯一价值所在。

## 已知问题（遗留清单）

- `oss-server` 在 `listen` 端口被占用时 **bind 失败不退出**（进程活着但无监听，systemd 视作
  假活）。e2e 期间顺带观察（9001 被占时发生）；按 service 化之前需改成启动失败退出。
- 权限撤销对已过 preprocess 的在途上传不追溯；控制台上传受 `[[groups]]` 映射约束（见边界节）。

## 明确不做（本期）

子账户自助改密码（API 归管理员）；用户启用/禁用（需 ALTER TABLE）；桶策略 JSON / 预设模板；
permissions 缓存（每请求 1-2 次 SQLite 读，与既有 ponytail 同口径）。

## 已知边界

- 控制台上传只能进「`[[groups]]` 映射的目标桶」（子账户的自有桶走 S3；要突破需给 preprocess
  表单加目标桶字段，属更大改动）。
- 权限撤销即时生效于新请求；**已通过 preprocess 的在途分块上传**会完成（revocation 的最终一致性窗口）。
- 历史桶（无 DB 行，owner NULL）仅管理员可见；`state.rs` 启动回填补行（INSERT OR IGNORE）。
