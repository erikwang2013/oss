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

- [x] proto：`ControlWrite` RPC + `ControlWriteReq`（kind 白名单，目前唯一 BUCKET_ROW_SET；服务端
      `u16::try_from` fail-closed，不静默截断；加法兼容，旧节点回 Unimplemented → 广播 warn 不炸）
- [x] `create_bucket`/`delete_bucket`（S3 面 + 控制面四条路径）**广播**行变更（best-effort + warn，
      镜像面同款口径）；接收端仅写本地 DB（set 覆盖、owner 不碰）；发起端两条建桶行写改**覆盖**语义
      （与接收端同文——verifier 阻断 1：原 INSERT OR IGNORE 在陈旧行上整条 no-op，容忍失败路径下
      本节点按旧 set 路由、旧 owner 判权，与已收广播的 peer 静默分叉）
- [x] 启动回填对账保持（**双保险为真**：磁盘面 + 表行面，均走**权威**扇出解析——verifier 高 2：
      原回填走数据面本地先行解析，把本地陈旧值原样写回，对「行存在且 set 已变」收敛不成立）
- **定稿（2026-10-16）**：七闸全绿（xl-store 143+1ignored / kill9 2 / oss-server 67 全二进制含新增 3 枚
  [`control_write_bucket_row_reconverges_stale_row`、`authoritative_resolve_overrides_stale_local_row`、
  `upsert_bucket_owner_set_overrides_stale_row`] / xl-s3 52（含 owner 覆盖语义 2 枚）/ xl-client+xl-proto 5 /
  clippy --workspace --all-targets -D warnings / fmt；最终树 `git diff | md5sum` = `6a650fbd`，相对
  全闸树 `c4d60f92` 仅 mesh_api.rs 三行注释、xl-store lib 复跑于终树）。verifier **两轮对抗**：
  第 1 轮 阻断 1（发起端 IGNORE 不对称分叉）+ 高 2（回填假兜底）→ 全修；第 2 轮复验 **可收口**
  （证据 /tmp/m5s0-verifier-report.md + /tmp/m5s0-verifier-recheck.md，其对 c4d60f92 逐处重核对并
  定点重跑三枚新测试）。工作令「B 立即 resolve 即 set1」以三段钉：①传输分发（xl-store 3 节点内存：
  广播达全部 peer 后端、宕机 peer 不扰其余、本机不自发）；②接收端语义（oss-server：陈旧行覆盖 +
  owner 不碰 + 缺行补插 + 删行）；③四条调用点读码（AuthedUser 恒插入前提核）+ 权威解析四态单测。
  **记档（非阻断）**：①方向性角落——「容忍失败/漏播 + 漏收建桶广播 + 创建者重启」双故障下对端
  共识可覆盖更新行（行无定序元数据；修「本地异议即 Err」会复现主 bug）→ M9 行 epoch/时间戳收口；
  ②接线层（四条广播调用点 + 两回填接线）零测试覆盖（突变存活）→ 切片 7 e2e 断言补；③启动开销
  多 set 两对账面 2×桶数×(N-1) 逐桶扇出（桶内并发；行面去重磁盘面名字可减半，未做）；
  ④接收端广播不覆盖 owner（陈旧节点「上一世 owner」本地 is_owner 仍真——HEAD 即存在，未恶化）；
  ⑤「对端全 None 保守不动」正确（本节点建桶若对端全漏播，删行会把活桶 404）。提交 `41d760d`。

## 切片 1：密钥库（KeyProvider）

- [x] `xl-store` 新模块 `crypto.rs`（或 `kms.rs`）：`KeyProvider` trait
      `wrap_dek(dek) -> (wrapped, key_id) / unwrap_dek(wrapped, key_id) -> dek`
- [x] 文件主密钥：`OSS_MASTER_KEY_FILE`（默认 `<data_root>/.oss.sys/master.key`，0600，32B 随机，
      首启生成）；**多节点同一主密钥**（运维分发的不是文件内容而是同一把——记录于 README）
- [x] KMS 文件实现（`key_id` 多主密钥：目录内 `key-<id>.bin`；`key_id` 记录用于换钥后旧 DEK 仍可解——
      验收 ⑤）；HTTP KMS：trait + 明确 NotImplemented 桩（钉子）
- [x] DEK：每对象 32B（OsRng）；wrap 算法 AES-256-GCM（wrap 层独立 nonce 全随机，非派生）
- [x] 单测：wrap/unwrap 往返、错 key_id 拒绝、换主密钥后旧 wrap 仍解（模拟 ⑤）、主密钥文件权限与
      生成幂等
- **定稿（2026-10-16）**：七闸全绿于终树（xl-store 159+1ignored（kms 16 枚）/ kill9 2 /
  oss-server 67 全二进制 / xl-s3 52 / xl-client+xl-proto 5 / clippy --workspace --all-targets
  -D warnings / fmt）。**双哈希锚**：终闸树 `git diff | md5sum` = `5f193ef4`（kms.rs 经 `add -N`
  纳入哈希——第 2 轮 `0de33398` 仅覆盖 tracked 文件，本轮修正）+ kms.rs sha256 `c3320e62`；
  相对第 2 轮复验树（`0de33398` / kms.rs `fd59a7b6`）的增量 = N1-N4 收尾。oss-server 闸首跑遇
  rustc ICE（机器重启致 incremental query-cache 损坏，日志明示 corrupt artifact、零代码诊断）→
  清损坏目录 + `CARGO_INCREMENTAL=0` 复跑绿；ICE 原文留档 /tmp/m5s1c-ossserver-ice.log。
  verifier **三轮对抗**：第 1 轮 不可收口——高 1（unwrap 读路径会**创建**主密钥文件→幽灵钥被 max
  扫描扶正，且 `k4294967295` 可溢出 rotate）/ 高 2（掉盘首启静默生成新钥→**已写死为切片 2 前置
  约束**）/ 中 3（已存在文件权限不校验）/ 中 4（生成后无目录 fsync）/ 中 5（wrong-length 无钉，
  <12B 落 panic 面）/ 低 6（解析宽容）——全修；第 2 轮复验 **可收口**（读路径只读重建 + 规范形态
  校验 + 权限拒启 + 目录 fsync + 14 枚单测突变核对）；第 3 轮 scoped 复验 **可收口**（N1 非空无
  合规钥目录→拒启、静默生成收窄至真·空目录；N2 规范判据补**可判别**双钉——k1 存在时 "k01" 必拒
  + `parse_key_file` 直测，删 filter / 删规范比较均必红；N3 `write_new`→`Result<()>`；N4 目录
  fsync 的 open 失败同口径 warn；回归面六项全绿）。证据：/tmp/m5s1-verifier-report.md、
  /tmp/m5s1-verifier-recheck.md、/tmp/m5s1-verifier-nfix.md——前两份因机器重启清空 /tmp 后自
  子代理 transcript **逐字节恢复**，树以哈希锚钉死未漂移（恢复账本
  `.superpowers/sdd/2026-10-16-m5-security-lock/progress.md`）。篡改/错钥/长度/权限四类失败全
  `Error::Crypto` fail-closed；键料不泄（手工 Debug 有测钉、错误文案无钥无路径、S3 面 500 泛化
  映射）。**记档**：①无 AAD 可接受，key_id 建议随切片 2 的 meta 格式纳入 AAD；②wrap 随机 nonce
  按钥 2^32 上限（本片每对象 1 wrap；切片 2 若每 part 一 wrap 需换钥口径）；③trait 无「请求 CMK」
  形参——切片 3 定夺；④双 provider key_id 路由（"file"/"k<n>"）→ 切片 3 遗留条；⑤`OSS_KMS_DIR`
  为模块新造 env 命名、`default_path`/`default_dir` 生产零调用（env 接线随切片 2/3）；
  ⑥chacha20poly1305 未引（未用不引，需第二算法再引）；⑦HTTP KMS 只留 trait（钉子「只留 trait」
  取字面，未落 NotImplemented 实体）；⑧半写钥文件=永久启动失败须人工删（错误文案已带处方，
  README 补）；⑨unwrap 每次 `std::fs::read`（2 syscall）——切片 2/3 定缓存或 tokio::fs 口径；
  ⑩权限校验引入启动硬失败面（0644/无 POSIX mode 的文件系统拒启）→ 切片 7 README 主密钥运维章写
  「install -m 600 分发 / 权限报错处置 / 数据盘须支持 POSIX 权限」；⑪KMS 目录 0755（共享主机建议
  0700；目录写权 = 钥控制权）；⑫并发面已核（首启 create_new + 有界重试覆半写窗；双进程 rotate
  收敛无损坏、内存 current 可落后——切片 7 e2e 可补）；⑬无 zeroize（核心转储面，可选）；⑭权限
  过宽属永久错却按 transient 重试（重试集=全部 Crypto；无害 fail-closed，可细分）；⑮KMS 目录须为
  **专用空子目录**——挂载点根自带 lost+found 会首启拒启（fail-closed、文案可循；README 与 ⑩
  同章）；⑯open_or_init 的 max 命中分支扫到半写钥时无重试（µs 窗、显式拒启可重启自愈；切片 2/3
  定夺是否纳入）。提交 `997c47f`。

## 切片 2：加密数据面（**执行前先出子设计交 verifier 评审**）

**前提（切片 1 verifier 高 2，接线前必须钉死）**：KeyProvider 初始化必须在 `Store::open`
**成功之后**、且以**在册盘**为根——掉盘首启（挂载点在父 fs 残留）时生成新主密钥会让既有
加密对象静默报废（不可逆），且 provider 先跑会在 store 的盘在册扫描里插进「无 drive-id 的
.oss.sys」污染降级判定。生成门闩（要求 store 已初始化/drive-id 在）与顺序在切片 2 子设计定。

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
- [ ] **切片 1 遗留接线**：按 key_id 路由 provider（`"file"` → FileKeyProvider；`"k<n>"` →
      KmsFileProvider——同部署 SSE-S3 与 SSE-KMS 混用）；trait 是否加请求 CMK 形参
      （`x-amz-server-side-encryption-aws-kms-key-id` 语义）在接线时定夺并记档；
      key_id 建议纳入 meta AAD（切片 2 定格式时一并）
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
      STS 临时凭据调用（验收 ③④）；**并补切片 0 遗留的接线断言**（建/删桶后各节点解析立即一致，
      免重启——广播四条调用点 + 回填接线的 e2e 覆盖）
- [ ] README：加密/锁/STS 章节 + 主密钥运维（分发/轮换口径）+ 已知边界
- [ ] 计划定稿（逐片勾选 + 证据链）；发布待用户口令

## 执行模型

- 与 M4 同款：主会话唯一写者；verifier 每片对抗核查；e2e-runner 关键节点真进程；docs-fixer 只碰 docs。
- 七闸 = xl-store lib / kill9 / oss-server / xl-s3 / xl-client+xl-proto / clippy -D warnings / fmt
  （+ web 三项当触及 web 时）。
