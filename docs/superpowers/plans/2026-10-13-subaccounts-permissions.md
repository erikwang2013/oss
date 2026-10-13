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

- ~~`oss-server` 在 `listen` 端口被占用时 bind 失败不退出~~（**已修**，见「清遗留轮」；
  残余毫秒级竞态窗口已记档）。
- 权限撤销对已过 preprocess 的在途上传不追溯；控制台上传受 `[[groups]]` 映射约束（见边界节）。
- 停用账号的既有 JWT 在**下一请求**才失效（无主动吊销名单）——接受，token 12h 自然过期。

## 明确不做（本期）

桶策略 JSON / 预设模板（属新特性，随 M4 细粒度 IAM）；permissions 缓存（每请求 1-2 次
SQLite 读，与既有 ponytail 同口径，无实测瓶颈不加）。

## 清遗留轮（2026-10-13 追加，commit 43c4bbd）

- **自助改密**：`POST /api/v1/me/password`（验旧口令、新口令 ≥8）；前端 Keys 页「修改口令」弹窗。
  已从「本期不做」清单移出。
- **账号启停用**：`users.status`（0001 带列 + `ensure_columns` 对老库幂等补列——SQLite 版极小迁移
  助手：PRAGMA 探测 + ALTER，字面量 SQL）；`PUT /users/{id}/status`（防呆：不能停用自己/最后一个
  在册管理员）。**停用即全链路失效**：登录 401、既有 JWT 下一请求 401（console who()/me）、
  S3 的 AK 403（SigV4Auth 与 Acl::user_of_key 双层 JOIN `users.status='active'`）、上传面 401
  （aether_gate）。前端 Users 页「停用/启用」按钮 + 「已停用」标签。已从「本期不做」清单移出。
- **bind 假活修复**：ecat 把 server 丢 `tokio::spawn`、`start()` 失败只记日志而 `run()` 继续等
  信号 → 端口被占时进程活着无监听（上轮 e2e 观察）。`cli.rs` serve 分支加**绑定期检**：失败即
  非零退出。残余：探测释放与 ecat 复绑之间有毫秒级竞态窗口（命中的概率极低且届时退化为原假活
  行为，由 ecat 日志暴露）；根治需 transport 支持外部 listener（上游增强项）。

### 清遗留轮评审修复（leftover-reviewer，P1–P5）

- **P1（高）**：`create_bucket`/`list_access_keys`/`create_access_key` 三端点只过 `uid_of()`
  （不查库）→ 被停用/已删账号的旧 JWT 仍可建桶、铸新 AK。修：三处改走 `who()`；`/groups`
  同补（P4）。集成回归：停用后四个端点必须 401 且未铸出任何桶。
- **P2（中）**：启停用/删除的「先读计数再写」并发互停可锁死到 0 在册管理员。修：守卫与写入
  合并为**单条条件语句**（`set_user_status` 的停用分支；`delete_user` 的事务首条无副作用守卫写
  `UPDATE users SET id=id WHERE …`，既取写锁串行化又原子判定，不通过则一字节不改）。
- **P3（低）**：`ensure_columns` 多进程首开老库可撞 duplicate column。修：ALTER 失败后重查列，
  已补上则视同成功。
- **P5（低）**：前端 `status === 'disabled'` 显式判定（缺字段不误报）；api 类型改可选；自己那行
  「停用」按钮禁用（后端防呆之外的前端 UX）。
- P6（bind 期检）经评审确认成立；注释中「毫秒级窗口」含建 router 与调度、略宽于字面，量级不变。

## 已知边界

- 控制台上传只能进「`[[groups]]` 映射的目标桶」（子账户的自有桶走 S3；要突破需给 preprocess
  表单加目标桶字段，属更大改动）。
- 权限撤销即时生效于新请求；**已通过 preprocess 的在途分块上传**会完成（revocation 的最终一致性窗口）。
- 历史桶（无 DB 行，owner NULL）仅管理员可见；`state.rs` 启动回填补行（INSERT OR IGNORE）。
