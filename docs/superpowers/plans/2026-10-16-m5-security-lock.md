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

**子设计 v2（2026-10-16，评审第 1 轮 B1-B6 已修、S1-S8 已采，交复评）——块对齐信封加密（DARE 式）**

**D1 核心形态（块对齐）**：明文流按 `PLAIN_UNIT = 65536 − 16 = 65520B` 分单元；单元 u 独立
AES-256-GCM（tag 16B，AAD 见 D4）→ **密文单元恒 65536B（= `ec::CHUNK_W`）**，末单元短（明文尾+16）。
于是「EC 单元 ↔（片 s, 块 t）」成**双射**：`u = s·blocks_ec + t`（∵ `ss = blocks_ec·W`）。
收益：写侧 gather 逐单元取 P 段 → 加密切片 → `encode_block` 原样喂（单元即整块）；读侧每个解码块
恰为一个整单元，无跨块窗；tag 永不跨片；每单元加解密恰一次。**否**弃「密文流=明文+逐块 tag 顺拼」
方案：tag 周期与 W-window 错位，跨窗单元要重加密（虽确定性可行但复杂）。依据：MinIO DARE 同构
（加密块与 erasure 条带对齐）+ 设计 §7.6「先加密再 EC」+ 工作令「块 = 现有 64KiB 粒度」。

**D2 长度换算（单源函数，写读共用，置 `crypto.rs`）**：`nb = ceil(P/65520)`；**P=0 ⇒ nb=0、
c_len=0、无单元**（几何函数对 nb==0 显式短路，防 `nb−1` 下溢——B1）；`unit_clen(u) = u < nb−1 ?
65536 : (P − 65520·(nb−1)) + 16`；`c_len = (nb−1)·65536 + unit_clen(nb−1)`（P>0，等价
`c_len = P + 16·nb`）；`blocks_ec = ceil(c_len / (data·65536))`；`ss = blocks_ec·65536`。单元 u 的
明文区间 `[65520·u, min(65520·(u+1), P))`。恒等式 `data·blocks_ec ≥ nb` 由 `c_len > (nb−1)·W`
保证（末单元至少 +17B）。**上限单点校验（写读共用，B3）**：`P ≤ 65520·2³²`（=
281,406,257,233,920 B ≈ **281.4 TB**），超限 → `Error::Crypto` fail-closed——nonce 取 BE32(u)
的不变量在读侧同样强制。`u ≥ nb` 的条带盘上恒全零，**读侧必须显式跳过**（D7）。

**D3 nonce**：`nonce = seed(8B 随机) ‖ BE32(u)`。**每对象一把 DEK（wrap 一次）+ 每存储流一个
seed**（本片 MPU 合并为单流，见 D8——「每 part 一 wrap」不适用）。同 DEK 下单元号唯一 ⇒ nonce
唯一；跨对象 DEK 不同 ⇒ 无跨对象面。seed 随机源写死 OsRng（kms.rs `new_dek` 同口径）。
**记档（S5 回填）**：本片每对象仅 1 wrap ⇒ 切片 1 复验「wrap 随机 nonce 2³²/钥」的触限 =
2³² 对象/钥，远超现实——口径闭合。

**D4 AAD**：单元加密带 `AAD = b"oss-m5|" ‖ key_id`——闭合切片 1 复验记档①「key_id 随切片 2 的
meta 格式纳入 AAD」。**wrap 层（kms.rs）保持无 AAD 不动**（recheck 判定可接受；交叉替换只 tag
失配 fail-closed）。AAD 无版本前缀风险不计：算法判别靠 D5 algorithm 白名单（白名单外读侧
`CorruptMeta` fail-closed），单元级换位被「位置相关 nonce」天然拦截。

**D5 meta**：`Version.encryption: Option<EncryptionRecord>`（Option + `serde(default)` 前向兼容
契约）：
```rust
pub struct EncryptionRecord {
    pub algorithm: String,       // 本片唯一值 "AES256-GCM-DARE-65520-v1"（S1：自描述几何
                                 // PLAIN_UNIT=65520 / W=65536 / DARE 布局——格式一回写即锁死，
                                 // 方案演进靠新标识，读侧白名单 fail-closed）
    pub key_id: String,          // "file" / "k<n>"
    pub wrapped_dek_hex: String, // 60B hex；SSE-C（切片 3）允许空
    pub nonce_seed_hex: String,  // 8B hex（16 字符）
}
```
decode 信任边界校验：algorithm 白名单（唯一值）；key_id 可打印 ≤64；hex 合法 + 长度（wrapped 0
或 60B、seed 16 hex 恰 8B）；违者 `CorruptMeta`。「wrapped 空 + 算法白名单内」为 SSE-C 预留的
**合法解码形态**，读侧 unwrap 空必拒 → 测试双钉（decode 允 / read `Crypto`，S2）。golden 摘要
**第六次有意识更新**（注释注明 M5 切片 2 增 encryption 字段）。**期望长度口径 = `c_len(size)`，
改面三处（B4）**：read.rs:79（get）、read.rs:162（range）、**version.rs:282（read_version_data）**。
inline 上限联动：decode 放宽 `c_len(MAX_INLINE)=131120`；阈值 ≤ MAX_INLINE 校验不变。

**D6 写入管线**：`put_object_stream_with_meta` / `complete_multipart_upload_versioned` 增
`encryption: Option<EncSpec>` 参数（其余调用点传 None；切片 3 由 S3 头映射）。起点（对象锁内）：
`dek=new_dek(); (wrapped,key_id)=按 spec 选 provider.wrap_dek(&dek)?; seed=OsRng 8B` →
`EncCtx{cipher,seed,key_id}`。`stage_payload`/`stage_payload_multi` 增 `enc: Option<&EncCtx>`
（None = 零行为变化）：gather 按单元取 P 段（短读按实长补零）→ 加密切片 → 喂 `encode_block`；
`blocks/ss` 以 **c_len** 算；**分片 CRC 覆盖密文（落盘态）**；ETag/MD5/SHA256/checksum 均对明文
（harvest 不变，读端解密后终验）。inline 分支：P 读回后**按 D2 单元切分加密**（P>65520 时 nb 可为 2/3，非「整段单 tag」，N1）——
`InlineData.bytes = C = c_len(P)`；**P=0 ⇒ C 空**（与 D2 短路一致）。
`PartRef.size = P`（明文），sha256 = 明文 sha。**ec.enabled=false + EncSpec → 两条写路径起点
（stage 前）检查 → `Error::Encode("encryption requires ec.enabled")`**（该模式下连 inline 分支
都不存在——put.rs 分支序如此；记档：切片 3 在 S3 面提前映射 4xx）。版本链：encryption 随 Version
落 meta——旧版本各留各的记录 ⇒ 换主密钥后旧版本仍可读（验收⑤）；delete marker 无 encryption。
**CopyObject 遗漏面（记档 S3）**：store.rs:1212-1231 `copy_object` = get（解密成 P）→ put(None)
⇒ 密文源会复制成**明文目标**；本片 S3 面无头映射不可达；切片 3 决定语义（随源/桶默认/显式记档）。
**aether 摄取** = `crates/oss-server/src/aether.rs:131-142`（内部摄取按明文落 None）✓。**MPU×AWS
偏离**（本实现 complete 时单流加密 vs AWS 分片级）→ 切片 3/7 文档落纸（S4）。

**D7 读管线**：`read_ec_range` 对 `v.encryption` 存在者——
- 整读：按 **c_len** 截断重建 C（现循环 `obj_start = s·ss + t·W ≡ u·W` 恒等式使重建代码**原样
  工作**）；逐单元 trim 到 `unit_clen(u)` → GCM 解密（AAD 同 D4）→ 拼 P → 截 P → **SHA256 对 P
  终验**（`v.part.sha256` 是明文 sha，信任锚不变）；读时修 `repair_shards_chunked` 改用重建出的
  **C** 重编（非 P）。
- Range：**u 驱动精确读**——`u0=a/65520`；`u1=(b−1)/65520`；**空区间 [x,x) 先行短路返回空**
  （read.rs:153 约定 start==end 合法，勿让 b−1 下溢——照 read.rs:462 `saturating_sub` 先例钳
  下界，B2）；按 t 去重解码块（一块 12 片读一次）；**u ≥ nb 的单元显式跳过**（零条带不可解密，
  防伪 Crypto）；每单元取片 `s=u/blocks_ec` 的块前 `unit_clen(u)` 字节解密 → 与请求区间求交
  拷贝。比草稿「(s,t) 窗猜」更精确；**对偶校验/坏片定位仍在密文（落盘态）进行**（优于草稿
  「解密后」：检错不需解密、在 GCM 之前完成定位）。`real_len=(ss−t·W).min(W)` 为密文口径、
  单元 trim 走 `unit_clen`，两层勿混。tag 校验失败 → `Error::Crypto` fail-closed。
- legacy parts 分支（无 data_layout/inline）遇 encryption → `CorruptMeta`；**拒落点在
  version.rs:299-314**（非 read.rs）。
- unwrap 一次/读调用（N6 口径：不引全局缓存；记档）；provider 缺失 → `Error::Crypto` fail-closed。

**D8 MPU**：保持「complete 时 concat → 单流」既有形态——加密只在 `stage_payload*`（complete 内）
发生 ⇒ **对象级单 DEK + 单 seed**；upload_part 暂存保持明文（与单段 PUT 的 `tmp/part.1` 明文暂存
同口径——提交前不可见；记档：暂存明文为已知边界，切片 7 README 写明）。`concat_stream_verified`
的并发重传快照核验对明文做，不变。

**D9 接线与门闩**：`Store` 增 `encryption: OnceLock<EncState>`；`EncState { file:
Option<Arc<FileKeyProvider>>, kms: Option<Arc<KmsFileProvider>> }` + **wrap 按 spec 选路
（SseS3→file、SseKms→kms）、unwrap 按 key_id 路由（"file"→file、"k<n>"→kms）**——切片 1 遗留的
双 provider 路由条本片落定。`Store::init_encryption(file_path: Option<PathBuf>, kms_dir:
Option<PathBuf>) -> Result<()>`：**须在 `Store::open` 成功后调用**（前提块）；门闩 = root 盘
`<root>/.oss.sys/drive-id` 在册才允许（掉盘/未在册 → Err 拒启、**不生成**）。oss-server
`state.rs` 在 open 后读 env（`OSS_MASTER_KEY_FILE` / `OSS_KMS_DIR`）调之——env 接线本片落地；
两 env 均未设 = 不启用加密（Store 照旧，零行为变化）。门闩先于**任何** provider 构造（含
open_or_init 的创建路径）；被拒路径**磁盘不得出现新钥文件**（D12⑥ 断言，S6）。首启全盘 down 为
open 既有盲点（store.rs:249-273 warn 容忍），非本片引入——记档。

**D10 API 面**：`pub enum EncSpec { SseS3, SseKms }`（切片 3 扩展 SseC 与请求参数）；
`ObjectInfo` 增 `encryption: Option<EncryptionRecord>`（切片 3 回 head 头用）——**定义 1 处 +
构造 5 处**（N3）：store.rs:92（定义）；put.rs:700、mpu.rs:414、version.rs:433、list.rs:255-264、
**read.rs:24（load_object_info）**。

**D11 改动面清单（B6 补全）**：新 `crates/xl-store/src/crypto.rs`（几何 + 单元 AEAD +
`EncSpec`/`EncState`/`EncCtx`）｜`meta.rs`（EncryptionRecord + decode 校验 + golden）｜`lib.rs`
（pub mod）｜`store.rs`（OnceLock + init_encryption + 选路）｜`store/put.rs`、`store/read.rs`、
`store/mpu.rs`｜**`store/version.rs`（:282 期望长度、:299-314 legacy 拒、:433 ObjectInfo）**｜
**`store/list.rs`（:255-264 ObjectInfo）**｜**`store/fsck.rs`（:482 scrub inline 校验按
encryption 分支取 `c_len(size)`——否则加密 inline 对象误报 failed，B5）**｜`store/tests.rs`
（Version 字面量）｜需补字段的 `meta::Version` 字面量 **7 处**（裸 grep「Version {」23 处含定义/impl/DeleteOutcome
等——复评 N2 勘误；含 meta.rs 自身测）｜
`crates/oss-server/src/state.rs`（env 接线）｜`crates/xl-s3/src/service.rs` **三处**调用点
（:494 put、:617 complete、:1236 copy→None；切片 3 映射头）｜`crates/oss-server/src/aether.rs`
（:131-142，None）｜新测试放 `store/tests_crypto.rs`（tests.rs 已 4.3k 行）。

**D12 测试计划（评审第 1 轮补 7 项）**：① 往返/几何——size ∈ {**0**、1、65519/65520/65521
（单元边界±1）、131039/131040/131041、2·W·data±1（块边界）}：写读等 + 随机 Range（含跨单元/跨
ss 边界、**空区间 [x,x) 返回空**）等 + **盘上布局契约**（按 D2 独立构造 C，与 part.<shard> 逐段
比对）；② 篡改：密文翻 1B→Crypto、wrapped_dek 翻 1B→Crypto、另钥 provider→Crypto、**wrapped 空
+ 算法在白名单 → decode 允而 read `Crypto`（双钉）**；③ 泄露：≥200KB 全 marker 明文 → 全部片 +
parity + inline-C 内 grep 不到任何 ≥16B 明文窗（验收②单测级锚）；两单元全零明文 → 两段密文不等；
④ MPU（≥2 part，首段 ≥5MiB）整读/Range 往返；版本链 v1 加密 + v2 覆盖后 v1 仍可读（⑤）；
delete marker 不受影响；**version.rs 点名读（get/range/head）回归**；⑤ 旧对象零回归；
ec.enabled=false + EncSpec → Err；inline 加密（C 长 = c_len）与内联 Range；**repair-from-C 回归
（丢片→整读→盘上分片与写侧契约逐段一致→再读通过；突变「从 P 重编码」必红）**；**多盘/集群
stage_payload_multi 加密往返**；⑥ 门闩：drives=[未在册, 在册] → init Err **且磁盘无新钥文件**；
正常 Ok；重复 init → Err；**decode 拒测（非法 algorithm/hex/越界长度）**；**MPU 暂存明文断言
（上传后暂存区 part.N 为明文、complete 后版本目录全密文）**。e2e/头/STS 归切片 3/7。
- 评审记录（第 1 轮，verifier-m5s2-design，报告 /tmp/m5s2-design-verifier-report.md）：架构成立、
无结构性隐患；阻断 B1-B6 已修（B1 P=0 短路 / B2 空 range 钳下界 / B3 上限读写共用 / B4 期望长度
三处 / B5 fsck:482 / B6 面补全），建议 S1-S8 已采（S5 为口径回填行）。
- **定稿（2026-10-16）**：七闸全绿于终树（xl-store **179+1ignored**（kms 16 / crypto 5 /
  tests_crypto 15 枚）/ kill9 2 / oss-server 67 / xl-s3 52 / xl-client+xl-proto 5 / clippy
  --workspace --all-targets -D warnings / fmt）。**锚（复验口径）**：全树 `git diff|md5sum` =
  `b1e64732`（两个新文件均在 `-N` 索引内）+ `crypto.rs` sha256 `545384b7` + `tests_crypto.rs`
  sha256 `e3533dff`；勘误：更早锚 `e3f65030` 系 crypto.rs 尚未 `add -N`（未进 diff 文本）时刻的
  口径——复验已逐字节复现，内容零漂移。verifier **两段四轮**：子设计段——第 1 轮 需修订（B1-B6：
  P=0 短路 / 空 range 钳下界 / 上限读写共用 / 期望长度三处 / fsck:482 / 改动面补全；S1-S8 采）→
  v2 复评 **可实施** + N1-N4 折入点检通过；实现段——对抗核查 **可收口**（九点全成立：几何单源读写
  共用、写读管线、三处 inline、legacy 三拒、门闩先于任何 provider 构造、调用面全 None、明文路径
  逐字节零回归；突变判别 3/4 可红），M1-M3（SseKms 零覆盖 / MPU 暂存断言 / 空区间无判别测）+ L1
  （unit_plain_len 断言）+ L2（fsck 对 encryption+parts 口径）修入并 scoped 复验 **可收口**（五项
  全 ADDRESSED、零新问题、七闸 B 树全绿计数自洽）。**核心形态**：DARE 式块对齐信封加密——明文单元
  65520B → 密文单元恒 65536B（=CHUNK_W）；(片 s, 块 t)↔单元 u=s·blocks_ec+t 双射（tag 不跨块、
  每单元加解密恰一次）；nonce = seed(8B OsRng) ‖ BE32(u)；AAD = `b"oss-m5|"` ‖ key_id；每对象 1
  DEK（wrap 一次）+ 1 seed；ETag/MD5/SHA256 对明文、分片 CRC 对密文、对偶校验在密文域；meta
  `Version.encryption`（algorithm="AES256-GCM-DARE-65520-v1" 自描述几何；golden 第六次有意识
  更新）；读时修从 **C** 重编码。e2e/头/STS 归切片 3/7。**记档**：①L4——init_encryption 部分失败
  （file 成、kms 败）留下已生成 file 钥文件：失败即 Err、重试幂等（open_or_init 复用），无静默报废；
  ②L5——MPU 加密起点先于 upload 存在性检查（不存在的 upload 也耗一次 wrap，错误语义不变）；③L6——
  三处 inline 文案统一「≠ {expected}」为有意识变化；④D6 四项（CopyObject 语义 / MPU×AWS 偏离 /
  暂存区明文边界 / provider env 默认路径）挂切片 3/7；⑤fsck/scrub 只做密文域 CRC/对偶、GCM 完整性
  靠读时 SHA（D7 设计）；⑥恶意 meta 大 size 巨分配属既有面（本片 c_len≈size 未加剧）；⑦M1 负例①
  与 kms.rs canonical 既有拒重叠、L1 断言无直接单测（复验记档级观察）。提交 `f501bc2`。

## 切片 3：S3 面 SSE 接线

- [x] PutObject/CreateMPU/UploadPart/CompleteMPU/CopyObject/GET/HEAD 的 SSE 头全链
      （s3s DTO 已备 `server_side_encryption`/`sse_customer_*` 字段）
- [x] **切片 1 遗留接线**：按 key_id 路由 provider（`"file"` → FileKeyProvider；`"k<n>"` →
      KmsFileProvider——同部署 SSE-S3 与 SSE-KMS 混用）；trait 是否加请求 CMK 形参
      （`x-amz-server-side-encryption-aws-kms-key-id` 语义）在接线时定夺并记档；
      key_id 建议纳入 meta AAD（切片 2 定格式时一并）
- [x] **切片 2 遗留（复评 S3/S4）**：CopyObject 语义定夺（密文源 → 目标：随源 / 桶默认 / 显式
      记档——store.rs:1212-1231 现为解密成 P 再 None 写）；MPU×AWS 偏离（complete 单流加密 vs
      AWS 分片级）落切片 7 README
- [x] `head-object`/`get-object` 回 `x-amz-server-side-encryption*`（SSE-C 必回算法；S3/KMS 回 S3/KMS+key_id）
- [x] SSE-C 错 key → 拒绝（HMAC 校验，`sse_c_hmac` 落 meta；密钥**绝不落盘**）
- [x] 桶级默认加密（PutBucketEncryption/GetBucketEncryption + buckets 列）——评：是否本片（AWS 常用）
**接线裁决（2026-10-16，实施前定，交 verifier 随实现核查）**：
- **R1 SSE-C 形态**：客户端钥（32B）**直接作单元 AEK**（无信封）；校验值 = **base64(MD5(key))（AWS
  同款 key-MD5）**而非 HMAC——与 aws-cli 行为逐字节对齐；落 meta 新字段
  `EncryptionRecord.sse_c_key_md5`，`key_id="sse-c"`、`wrapped_dek` 恒空（decode 强制三态一致）。
  密钥绝不落盘；错钥/缺钥 → `Error::InvalidArgument`（→400）。读/HEAD 均校验 MD5；非 SSE-C 对象
  带 SSE-C 头 → 400（AWS 语义）。
- **R2 trait CMK 形参**：显式 `x-amz-server-side-encryption-aws-kms-key-id` → **400 fail-closed**
  （文件型 KMS 单主密钥；「KMS 多主密钥轮换编排」恰在钉子不做清单）；HTTP KMS 落地时再开。
  ——闭合切片 1 遗留条（定夺 = 拒而记档）。
- **R3 桶级默认加密**：**不本片**——需 buckets 列 + 三组 handler + 覆写/复制面联动，属独立小片；
  记档进切片 7 遗留（「M5 后独立小片」）。
- **R4 CopyObject**：源读按 `copy-source-sse-c*` 头（源为 SSE-C 必带，否则 400）；目标按
  `sse_customer_*`/`server_side_encryption` 显式头决定，未给 = 明文目标（AWS 无桶默认时同义）
  ——履行切片 2 复评 S3 承诺。
- **R5 MPU 一致性**：Create 时 SSE-C → `mpu.meta` 记 md5；UploadPart/Complete 必带同钥（md5 不符/
  缺失 → 400）；Create 无而 Complete 带 SSE-C → 400。分片暂存仍明文（D8 口径），加密在 complete
  单流。**（复验升级 P2-b）**：SSE-S3/KMS 意图由 Create 记入 `mpu.meta.sse_kind` 并在 Complete
  **落地**（AWS：该头只出现在 create）——不落地=「create 回 AES256、对象实为明文」静默降级；
  跨调用模式不一致 / 单侧携带 → 400。原「SseS3/KMS 不强制」措辞作废。
- **R6 读面**：`get/head/version` 增 `_ext` 变体带 `Option<&SseCKey>`（旧签名委托 None，零 churn）；
  GET/HEAD 响应：S3/KMS 回 `server_side_encryption` + `ssekms_key_id`（= meta key_id）；SSE-C 回
  `sse_customer_algorithm` + key-MD5（DTO 缺字段处用 `S3Response::with_headers` 补）。
- **R7 错误面**：客户端可纠正类一律 `InvalidArgument` → 400（convert.rs 既有映射）；服务端类维持
  Crypto → 500。
- **R8 测试面**：xl-s3 新模块 `sse.rs`（纯解析/响应助手 + 单测）；store 级 SSE-C 往返/Range/内联/
  MPU 一致性/错钥进 `tests_crypto.rs`。
- **定稿（2026-10-16）**：七闸全绿于终树（xl-store **183+1ignored**（SSE-C 新测 4 枚）/ kill9 2 /
  oss-server 67 / xl-s3 **56**（sse.rs 3 + 服务层线级 2）/ xl-client+xl-proto 5 / clippy
  --workspace --all-targets -D warnings / fmt）。**锚**：闸树 `git diff|md5sum` = `4d6a0f13`
  （代码自闸后零漂移；计划 R5 段系闸后按其复验观察改写）；sse.rs `597ee959` / service.rs
  `dce5ba87` / object_ops.rs `af960700` / mpu.rs `56ceb218` / tests_crypto.rs `c81a81a8`。
  verifier **两轮**：第 1 轮 对抗核查 可收口（R1-R8 全条成立、钥不落盘全链证明、读面 6 落点枚举、
  无 provider 纯 SSE-C 可用），定夺两枚 P2 + P3-a 修复；scoped 复验 **可收口**（P2-a/P2-b/P3-a/
  P3-c①② 全 ADDRESSED、零新引入）。**落地形态**：SSE-C = 客户端钥直作单元 AEK（无信封）+
  base64(MD5(key)) 为唯一落盘校验值（key_id="sse-c"、wrapped 恒空、decode 三态一致，切片 2 的
  「空 wrapped 预留」形态被收编）；SSE-S3/KMS 响应回 server_side_encryption+ssekms_key_id；SSE-C
  回 algorithm + key-MD5（GET/HEAD 的 MD5 头与 Complete 的 dual 头经 `S3Response::with_headers`
  补）；MPU 三向一致性（create 记 md5 与 sse_kind → upload/complete 校验 → **kind 在 complete
  落地加密**）；CMK 显式指定 → 400（R2 钉子）；CopyObject 源/目标双向接线（R4）。
  **复验修复记**：P2-a（PUT/UploadPart 响应补 key-MD5，服务层线级测试直钉）、P2-b（create 的
  SSE-S3/KMS 意图经 mpu.meta 持久化并在 complete 落地——修前属「create 回 AES256、对象实为明文」
  静默降级）、P3-a（SSE-C×CMK 互斥 400）。**记档**：①P3-b——`store.copy_object`（store.rs）仍为
  读明文再写 None 的遗留实现（S3 面已不走；console/内部启用前须带 spec）；②P3-d——头形态错误用
  InvalidRequest、store 面客户端可纠正类用 InvalidArgument，皆 400（AWS 对齐）；③P3-c 残余——
  (None,Some 非法 kind)/(Some(SseS3),None) 两分支与 GET 线测 body 未直钉（复验判 P3 微缺口，
  语义已逐分支推演）；④fsck repair 对无钥 SSE-C 对象读时修静默跳过（`let _ =`；分片级 CRC 计数
  不受影响——修复需钥，属正确行为）；⑤桶级默认加密按 R3 未本片（切片 7 遗留「M5 后独立小片」）；
  ⑥仅直调 store API 可构造的 complete 单侧 SSE-S3/KMS → 400（R5 扩展 fail-closed）。提交
  `40ecc9a`。

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
