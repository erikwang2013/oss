# M5 安全与锁定 实施计划（工作令）

> 设计文档 §7.6 / §M5：**范围** = SSE-S3 / SSE-KMS（含文件型 KMS）/ SSE-C · 对象锁（Governance +
> Compliance + Legal Hold）· STS（AssumeRole / GetSessionToken / AssumeRoleWithWebIdentity）。
> **验收**（设计文档 §M5）：
> ① 三种 SSE 各下一单：aws-cli 带参数写入 → head-object 见正确加密头；盘上分片 grep 不到明文特征串
> ② 先加密再 EC：单分片（不足 quorum）无法还原任何明文
> ③ 对象锁：Compliance 下删除版本被拒（**含管理员**）；到期后可删
> ④ STS：AssumeRole 临时凭据可用；过期拒绝；会话策略 ∩ 用户策略生效
> ⑤ SSE-KMS 换主密钥后旧对象仍可读（DEK 未变）

## 钉子（防蔓延）

- **不做**：HTTP KMS 真实实现（只留 trait）；KMS 多主密钥轮换编排；SSE 与桶复制/M6 的交互（M6 再议）；
  对象锁的 MFA delete；锁定期生命周期保留策略（AWS 语义「锁定版本跳过删除」，实现 + 记档）。
- 依赖新增（设计文档 §依赖表钦定）：`aes-gcm`、`chacha20poly1305`（RustCrypto）；
  `jsonwebtoken` 仅在 M5-7 WebIdentity slice 落地时引入（若实现，否则记档裁剪）。
- **加密顺序硬约束**（设计 §7.6）：明文 → part 级流式加密 → EC 编码 → 落盘；读路径反向。
  分片泄露 = 无明文信息（验收 ②）。
- 发布/推送：仍只在用户明确口令时执行。

## 切片总表（串行；每片 = 代码 → 测试 → 七闸 → verifier 核查 → 定稿 → 本地提交）

| # | 切片 | 交付 | 验收锚 |
|---|------|------|--------|
| 0 | 集群行一致性（P4-1） | 桶行 set 变更广播（关删桶重建的陈旧行窗口） | 回归 |
| 1 | 密钥库 | KeyProvider trait + 文件主密钥 + KMS 文件实现 + DEK wrap | 单测 |
| 2 | 加密数据面（子设计先行） | 先加密后 EC 全管线 + nonce 派生 + Range/MPU/版本 | 验收 ①②⑤ |
| 3 | S3 面 SSE 接线 | 三模式请求/响应头 + meta 落盘 + SSE-C 校验 | 验收 ① |
| 4 | 对象锁 | 桶开关 + 对象头 + 删除守卫（Compliance 含 admin）+ Legal Hold | 验收 ③ |
| 5 | STS | GetSessionToken + AssumeRole + 会话策略∩ + SigV4 token 链 | 验收 ④ |
| 6 | SK 加密存 | access_keys SK 密文 + 存量透明迁移 | 安全收口 |
| 7 | WebIdentity（条件式）+ 收尾 | e2e §18 / 文档 / 定稿（WebIdentity 视成本决定做或记档） | 全量 |

## 切片 0：集群行一致性（P4-1）

- [ ] proto：`BucketRef` 复用或新消息承载「桶行 set 变更」——控制面**写** RPC（现 ControlBackend 只读；
      新增 `ControlOp` 面或走既有 mesh 服务新方法；方案随实现定，评审过为准）
- [ ] `create_bucket`/`delete_bucket`（S3 面 + 控制面四条路径）**广播**行变更（best-effort + warn，
      镜像面同款口径）；接收端仅写本地 DB
- [ ] 启动回填对账保持（双保险）；测试：3 节点内存集群——A 建桶 set1 → B 立即 resolve 即 set1（无需重启）
- **定稿**：§

## 切片 1：密钥库（KeyProvider）

- [ ] `xl-store` 新模块 `crypto.rs`（或 `kms.rs`）：`KeyProvider` trait
      `wrap_dek(dek) -> (wrapped, key_id) / unwrap_dek(wrapped, key_id) -> dek`
- [ ] 文件主密钥：`OSS_MASTER_KEY_FILE`（默认 `<data_root>/.oss.sys/master.key`，0600，32B 随机，
      首启生成）；**多节点同一主密钥**（运维分发的不是文件内容而是同一把——记录于 README）
- [ ] KMS 文件实现（`key_id` 多主密钥：目录内 `key-<id>.bin`；`key_id` 记录用于换钥后旧 DEK 仍可解——
      验收 ⑤）；HTTP KMS：trait + 明确 NotImplemented 桩（钉子）
- [ ] DEK：每对象 32B（OsRng）；wrap 算法 AES-256-GCM（wrap 层独立 nonce 全随机，非派生）
- [ ] 单测：wrap/unwrap 往返、错 key_id 拒绝、换主密钥后旧 wrap 仍解（模拟 ⑤）、主密钥文件权限与
      生成幂等
- **定稿**：§

## 切片 2：加密数据面（**执行前先出子设计交 verifier 评审**）

子设计要点（待评审细化）：
- 流式插入点：`put.rs` 的 `harvest_body` 之后、`ec::ChunkWriter.encode_block` 之前，**逐块**加密
  （块 = 现有 64KiB 粒度；GCM tag 16B 附加；末块补零在加密**后**……顺序待评）；总长 = 明文 + 每块 tag。
- nonce：`nonce_seed[8] || block_no[4]` 派生（GCM 96-bit）；`nonce_seed` 每 part 随机、落 meta；
  同 DEK 下不重用（part 号参与或 seed 每 part 新——评估）。
- EC 参数与 crc32c：分片 CRC 对**密文**（落盘态）；ETag/checksum 对**明文**（读端解密后原生校验，
  设计决策记录）。
- Range 读：命中密文块 → 解密 → 切片（块内 tag 位置换算）；逐块对偶校验在解密后进行。
- inline 对象：加密后 inline？还是 inline 不加密（记档）？——给结论。
- MPU/版本：每 part 独立 DEK 或对象级单 DEK 多 part？（AWS 语义 + 重建简洁性权衡）
- 旧对象（无 encryption 字段）零回归；加密对象在 `ec.enabled=false` 旧形态的行为。
- **定稿**：§

## 切片 3：S3 面 SSE 接线

- [ ] PutObject/CreateMPU/UploadPart/CompleteMPU/CopyObject/GET/HEAD 的 SSE 头全链
      （s3s DTO 已备 `server_side_encryption`/`sse_customer_*` 字段）
- [ ] `head-object`/`get-object` 回 `x-amz-server-side-encryption*`（SSE-C 必回算法；S3/KMS 回 S3/KMS+key_id）
- [ ] SSE-C 错 key → 拒绝（HMAC 校验，`sse_c_hmac` 落 meta；密钥**绝不落盘**）
- [ ] 桶级默认加密（PutBucketEncryption/GetBucketEncryption + buckets 列）——评：是否本片（AWS 常用）
- **定稿**：§

## 切片 4：对象锁

- [ ] 桶级：`PutObjectLockConfiguration`/`GetObjectLockConfiguration`（s3s DTO 核实）+ `buckets.object_lock`
      列启用（须开版本控制，AWS 语义）；控制台可读
- [ ] 对象级：PutObject 的 `x-amz-object-lock-mode` / `-retain-until-date` / `-legal-hold` 头
      （桶未开启 → 400）
- [ ] 守卫：删除（点名删版本/批量删/生命周期/覆盖写）检查 `Version.lock`——Governance：拒，除非
      `x-amz-bypass-governance-retention: true`（且非匿名）；**Compliance：一律拒（含 admin）**；
      Legal Hold 独立于 retaining
- [ ] 到期：`retain_until < now` 即可删；时钟依赖 NTP（引用切片 4/epoch 口径）
- [ ] 测试：Compliance 拒删（含 admin）→ 拨表到期 → 可删；Governance + bypass 头放行路径；
      LegalHold 开关；批量删逐键守卫
- **定稿**：§

## 切片 5：STS

- [ ] `access_keys` 扩展：`parent_ak` / `session_policy` / `kind`（long-term|session）——ensure_columns 幂等
- [ ] `GetSessionToken`（自凭据派生更短 TTL）与 `AssumeRole`（角色先落「角色=用户别名」最小模型：
      RoleSessionName/Policy 参数/时长上限；ARN 形态 `arn:oss:iam::<acct>:role/<name>`——账号单租户简化）
- [ ] SigV4 链：`x-amz-security-token` 头解析 → auth.rs 校验会话 AK + 过期 + 父链
- [ ] 会话策略 ∩ 用户策略：check()/Acl 带出会话策略，在既有融合后**再叠**（会话 Deny 优先 + 会话
      未 Allow 即拒——交集语义）；审计 detail 标 session
- [ ] 测试：签发→用→过期拒；会话策略比用户更窄时生效；凭据对 S3 面与控制面各面的行为口径
- **定稿**：§

## 切片 6：SK 加密存

- [ ] `access_keys.secret_key` 信封加密（复用切片 1 的 KeyProvider；`enc:v1:` 前缀识别，明文存量
      读到即透明重写）
- [ ] auth.rs / 控制面展示（列表只回 AK）一致改动；测试：存量迁移、密文库启动可用
- **定稿**：§

## 切片 7：WebIdentity（条件式）+ 收尾

- [ ] AssumeRoleWithWebIdentity：验 RS256 JWT（jsonwebtoken）+ JWKS 拉取缓存（`OSS_OIDC_ISSUER` 配置）
      ——**成本评估后决策**：做则全链测试；不做则计划记档裁剪（设计文档范围项，需用户知悉）
- [ ] e2e_s3.sh 新 §18：SSE 三模式写入→head 头断言→盘上 grep 明文特征串（验收 ①②）；锁头部；
      STS 临时凭据调用（验收 ③④）
- [ ] README：加密/锁/STS 章节 + 主密钥运维（分发/轮换口径）+ 已知边界
- [ ] 计划定稿（逐片勾选 + 证据链）；发布待用户口令

## 执行模型

- 与 M4 同款：主会话唯一写者；verifier 每片对抗核查；e2e-runner 关键节点真进程；docs-fixer 只碰 docs。
- 七闸 = xl-store lib / kill9 / oss-server / xl-s3 / xl-client+xl-proto / clippy -D warnings / fmt
  （+ web 三项当触及 web 时）。
