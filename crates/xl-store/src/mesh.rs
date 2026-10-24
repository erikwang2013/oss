//! 节点间传输面（M3）：**xl-store 只认这个 trait，不依赖任何 RPC 框架**。
//!
//! 分层裁决：trait 与 DTO 定义在这里（纯 async trait + 数据），tonic 实现放 xl-client
//! （`impl MeshTransport for TonicMesh`），oss-server 装配注入。测试用 in-process 实现
//! （对端 `Store::mesh_*` 直连）——多节点语义无需真网络即可覆盖。
//!
//! 语义学分工（服务端 handler 侧）：一个节点收到的请求只操作**它自己的本地槽**；
//! 扇出永远由「协调者」（收到客户端请求的节点）做。

use bytes::Bytes;

use crate::crypto::{EncSpec, SseCKey};
use crate::error::{Error, Result};
use crate::meta::ObjectLockRecord;
use crate::store::{ByteStream, ObjectInfo, VersioningMode};

/// meta 内容 hash（sha256）——提交协议只计「翻转内容 == 自己写的」的槽位（防伪达成）。
pub fn content_hash(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest as _;
    sha2::Sha256::digest(bytes).into()
}

/// 幂等分片写：同 (uuid, part, shard) 覆盖写；服务端**先验 CRC32C 再落盘**。
#[derive(Clone, Debug)]
pub struct ShardWrite {
    pub bucket: String,
    pub key: String,
    pub uuid: String,
    pub part: u32,
    pub shard: u32,
    pub crc32c: u32,
    pub data: Bytes,
    /// M4 切片 2 分块写：true 时忽略 `crc32c`、校验 `chunk_crc32c` 并把 `data` 写到分片
    /// 文件 `offset` 处（`offset=0` 先截断——重试从头幂等）；`is_final` 时 fsync（含目录）。
    /// false = 旧整片语义（整片 CRC 校验 + 整写，行为不变）。
    pub chunked: bool,
    pub offset: u64,
    pub chunk_crc32c: u32,
    pub is_final: bool,
    /// M4 切片 5：请求方携带的槽位（写路径 = 放置结果；服务端只校验归属，不重算公式）。
    pub slot: u32,
    /// M4 切片 8：桶所属 set（槽为**组内**槽；服务端按 sets[set] 校验 + flat_of 映射物理盘）。
    pub set: u32,
}

#[derive(Clone, Debug)]
pub struct ShardRead {
    pub bucket: String,
    pub key: String,
    pub uuid: String,
    pub part: u32,
    pub shard: u32,
    /// M4 切片 2：`length=0` = 整片（旧语义）；否则读 `[offset, offset+length)`，
    /// 短读视同缺失（崩溃半截件由 EC 重建兜底）。
    pub offset: u64,
    pub length: u64,
    /// M4 切片 5：请求方携带的槽位（= meta 的 ShardRef.drive；老槽在 flip 后不再有记录）。
    pub slot: u32,
    /// M4 切片 8：桶所属 set（组内槽域）。
    pub set: u32,
}

/// 单片删除（M4 切片 5：rebalance 翻转后的老片清理；槽位显式携带）。
#[derive(Clone, Debug)]
pub struct ShardDelete {
    pub bucket: String,
    pub key: String,
    pub uuid: String,
    pub part: u32,
    pub shard: u32,
    pub slot: u32,
    /// M4 切片 8：桶所属 set（组内槽域）。
    pub set: u32,
}

/// 版本目录引用（删除 / 回收两用；UUID 是版本目录名，天然幂等）。
#[derive(Clone, Debug)]
pub struct VersionDirRef {
    pub bucket: String,
    pub key: String,
    pub uuid: String,
}

#[derive(Clone, Debug)]
pub struct MetaWrite {
    pub bucket: String,
    pub key: String,
    pub meta: Bytes,
}

#[derive(Clone, Debug)]
pub struct MetaRead {
    pub bucket: String,
    pub key: String,
}

/// 单槽位 ack：`hash` = 该槽位**实际写入/翻转的内容**的 [`content_hash`]。
#[derive(Clone, Debug)]
pub struct SlotAck {
    pub slot: u16,
    pub ok: bool,
    pub hash: [u8; 32],
}

/// prepare / commit 的结果：node 侧报告它**全部本地槽**的逐槽结果。
#[derive(Clone, Debug)]
pub struct MetaAck {
    pub ok: bool,
    pub msg: String,
    pub slots: Vec<SlotAck>,
    /// 成功读到的本地盘数（覆盖计数用；prepare/commit 里 = 操作过的槽位数）。
    pub slots_ok: u32,
}

/// 读 meta 的一次快照：node 侧返回它本地最优副本 + 成功读到的盘数。
/// `meta: None` = 该节点没有此键（**仍携带 slots_ok**——覆盖率快路径靠它对「缺席」
/// 计数，Option 化的返回会把覆盖数丢掉，故此结构恒存在）。
#[derive(Clone, Debug)]
pub struct MetaSnapshot {
    pub meta: Option<Bytes>,
    pub mod_time_nanos: i64,
    pub slots_ok: u32,
}

/// LIST 并集条目：`max_mod_time_nanos` = 该节点本地各槽副本的每键最大 mod_time。
/// **不带 meta 字节**（内联 meta 可达 128KiB，整页带字节 = 百 MiB 级线放大）。
#[derive(Clone, Debug)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub max_mod_time_nanos: i64,
}

/// 分页与 S3 同参；`limit = max_keys + 1`（页合并完备性的关键，见计划 D 节证明）。
#[derive(Clone, Debug)]
pub struct ListLocal {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    /// true = V1 排他 marker；false = V2 包含式 token（默认，兼容旧客户端）。
    pub marker_exclusive: bool,
    pub limit: usize,
    /// 版本列表：链尾为 delete marker 的键也要返回（对象列表 = false）。
    pub include_deleted: bool,
}

#[derive(Clone, Debug)]
pub struct ListLocalResp {
    pub entries: Vec<ListEntry>,
    pub prefixes: Vec<String>,
    /// 成功读到的本地盘数（排查用；不参与判定）。
    pub slots_ok: u32,
}

/// ListMultipartUploads 并集条目：按 (key, upload_id) 全局有序；`initiated_nanos`
/// 与单机响应同源（mpu.meta.created_nanos）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated_nanos: i64,
    pub storage_class: String,
}

/// LMU 单节点页请求。分页锚是 (key_marker, upload_id_marker)：
/// - key_marker 排他（严格大于）；upload_id_marker 仅在同一 key 内生效（id > marker）。
///   `limit = max_uploads + 1`（集群并集完备性证明与 ListLocal 同款）。
#[derive(Clone, Debug)]
pub struct ListUploadsLocal {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct ListUploadsLocalResp {
    pub uploads: Vec<UploadEntry>,
    pub prefixes: Vec<String>,
    /// 成功读到的本地盘数（排查用；不参与判定）。
    pub slots_ok: u32,
}

#[derive(Clone, Debug)]
pub struct NodeHealth {
    pub node_id: String,
    pub version: String,
    /// (全局槽位, 该盘在册)
    pub slots: Vec<(u16, bool)>,
    pub buckets: u64,
    /// DP-0 数据面线格式（原样上报；旧节点缺省 0）。判定一律用
    /// [`NodeHealth::peer_data_wire_version`]（见 `mesh_gate`）。
    pub data_wire_version: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockOp {
    Acquire,
    Renew,
    Release,
}

/// 租约锁请求。`epoch = (unix_nanos, node_id)`：仅用于「过期替换」的定序与日志——
/// **不得用于抢占活租约**（时钟快的节点会抢走活锁，评审修正）。
#[derive(Clone, Debug)]
pub struct LockReq {
    pub bucket: String,
    pub key: String,
    pub epoch_nanos: i64,
    pub epoch_node: String,
    pub holder: String,
    pub ttl_ms: u64,
    pub op: LockOp,
}

#[derive(Clone, Debug)]
pub struct LockResp {
    pub granted: bool,
}

/// 控制面读扇出白名单（服务端 match 字面量 SQL；安全边界）。
#[derive(Clone, Debug)]
pub enum ControlQuery {
    /// JOIN users.status 与 SigV4Auth 同口径
    AccessKey(String),
    Permission {
        uid: i64,
        bucket: String,
        action: u8,
    },
    OwnedBuckets(i64),
    VisibleBuckets(i64),
    BucketOwner(String),
    /// M4 切片 8：桶 → set（无行 = None）
    BucketSet(String),
}

#[derive(Clone, Debug, Default)]
pub struct ControlRow {
    pub uid: Option<i64>,
    pub is_admin: Option<bool>,
    pub active: Option<bool>,
    pub secret: Option<String>,
    pub flag: Option<bool>,
    pub names: Option<Vec<String>>,
    /// M4 切片 8：BucketSet 应答
    pub set: Option<u32>,
    /// M5 切片 5（S9）：AccessKey 应答携带的会话策略原文（`kind='session'` 且非空时；
    /// 远端节点据此照常评价会话交集）。
    pub session_policy: Option<String>,
}

/// 控制面后端（Task 9）：服务端**本地**查询执行器。oss-server 注入 sqlx 实现
/// （复用 xl-s3 的 Acl，判权 SQL 唯一事实源）；in-process / xl-client 的服务端
/// handler 统一经 `Store::mesh_control_query` 调它。**只查本节点 DB，不扇出**。
#[async_trait::async_trait]
pub trait ControlBackend: Send + Sync + std::fmt::Debug {
    /// 白名单查询（安全边界在实现侧）；None = 本节点查无。
    async fn control_query(&self, q: ControlQuery) -> Result<Option<ControlRow>>;
    /// M5 切片 0：白名单**写**（接收端只写本地 DB；调用方 best-effort 广播）。
    /// 默认 NotImplemented 桩——旧实现不被迫改动，写面未接者显式失败。
    async fn control_write(&self, w: ControlWrite) -> Result<()> {
        Err(crate::error::Error::Encode(format!(
            "控制面写未实现: {w:?}"
        )))
    }
}

/// 控制面写白名单（M5 切片 0 / P4-1）：桶行变更广播的载荷。
#[derive(Clone, Debug)]
pub enum ControlWrite {
    /// 设桶行 set（upsert；owner/versioning 不动）；`None` = 删行（随删桶清理）。
    BucketRowSet { bucket: String, set: Option<u16> },
}

/// 桶 → set 解析（M4 切片 8）。**独立于 MeshTransport 的扇出面**（verifier M2）：
/// 「对端不可达」必须与「对端答无行」可区分——不复用吞错的 ControlMesh::ask 语义。
#[async_trait::async_trait]
pub trait SetResolver: Send + Sync + std::fmt::Debug {
    /// `Ok(Some(set))` = 有行（含一致性合并）；`Ok(None)` = **全部可达且都答无**；
    /// `Err` = 任一传输错 / 行分叉（多节点答案不一致）/ 无任何可达节点。
    /// 数据面热路径语义：本地先行（本地命中不扇出）。
    async fn resolve_bucket_set(&self, bucket: &str) -> Result<Option<u32>>;
    /// M5 切片 0：**权威**解析（启动对账用）——跳过本地短路，以对端共识为准，收敛
    /// 本地陈旧行。默认回落数据面语义（旧实现不被迫改动）。
    async fn resolve_bucket_set_authoritative(&self, bucket: &str) -> Result<Option<u32>> {
        self.resolve_bucket_set(bucket).await
    }
}

// ---------- M3 MPU 会话粘性：转发 DTO ----------
//
// 设计：`create_multipart_upload` 仍留在接收节点（暂存区在其 root 盘），返回的
// upload_id 内嵌属主节点（见 `store::mpu_route`）。其余 MPU 操作在**协调者**（收到
// 客户端请求的节点）解析属主并转发；服务端 handler 只执行本地暂存区操作。
//
// 转发语义：
// - UploadPart 流式转发（`body` 为 ByteStream，不整段进内存）；传输层必须保证
//   「流结束前未声明末块」被接收端视为截断（见 proto 的 is_final）。
// - 传输失败一律 `Error::Unavailable`（S3 面 503），不得伪装成功/NoSuchUpload。
// - 鉴权在协调者完成（SigV4/ACL/策略），属主只执行已授权动作；集群令牌是节点面
//   鉴权。SSE-C 的 complete 密钥随请求转发，**跨节点时必须 mesh TLS**（L-3）；
//   `TonicMesh` 在明文通道上对 SSE-C Complete/UploadPart fail-closed。

/// 转发 UploadPart：`body` 为前端节点的原始分片流（流式，不缓冲整段）。
pub struct MpuPartUpload {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: u16,
    pub sse_c_md5: Option<String>,
    pub body: ByteStream,
}

impl std::fmt::Debug for MpuPartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MpuPartUpload")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &self.upload_id)
            .field("part_number", &self.part_number)
            .field("sse_c_md5", &self.sse_c_md5)
            .finish_non_exhaustive()
    }
}

/// MPU 引用（Abort / ListParts）。
#[derive(Clone, Debug)]
pub struct MpuRef {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

/// 转发 CompleteMultipartUpload：版本形态由协调者解析、属主落地。
#[derive(Clone, Debug)]
pub struct MpuComplete {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<(u16, String)>,
    pub mode: VersioningMode,
    pub encryption: Option<MpuEnc>,
    pub lock: Option<ObjectLockRecord>,
}

/// 转发用加密意图。**SSE-C 变体携带原始客户钥**——Debug 已脱敏。
/// L-3：跨节点转发 SSE-C MPU 必须启用 mesh TLS；生产传输 `TonicMesh` 在明文通道上
/// 对 SSE-C 的 Complete/UploadPart fail-closed。本地属主（upload_id 无远端前缀）不经
/// 传输层，SSE-C MPU 不受影响。
#[derive(Clone, PartialEq, Eq)]
pub enum MpuEnc {
    SseS3,
    SseKms,
    SseKmsCmk(String),
    SseC([u8; 32]),
}

impl std::fmt::Debug for MpuEnc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SseS3 => f.write_str("SseS3"),
            Self::SseKms => f.write_str("SseKms"),
            Self::SseKmsCmk(n) => f.debug_tuple("SseKmsCmk").field(n).finish(),
            Self::SseC(_) => f.write_str("SseC(<redacted>)"),
        }
    }
}

impl MpuEnc {
    /// 请求侧 spec → 线上形态（密钥字节仅在 xl-store 内可取）。
    pub fn from_spec(spec: &EncSpec) -> Self {
        match spec {
            EncSpec::SseS3 => Self::SseS3,
            EncSpec::SseKms => Self::SseKms,
            EncSpec::SseKmsCmk(n) => Self::SseKmsCmk(n.clone()),
            EncSpec::SseC(k) => Self::SseC(*k.key()),
        }
    }

    /// 线上形态 → 属主侧 spec（SSE-C 密钥重建为 `SseCKey`，md5 由密钥重算）。
    pub fn to_spec(&self) -> EncSpec {
        match self {
            Self::SseS3 => EncSpec::SseS3,
            Self::SseKms => EncSpec::SseKms,
            Self::SseKmsCmk(n) => EncSpec::SseKmsCmk(n.clone()),
            Self::SseC(k) => EncSpec::SseC(SseCKey::from_key(*k)),
        }
    }
}

/// ListParts 转发结果行（升序）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MpuPartRow {
    pub part_number: u16,
    pub size: u64,
    pub etag: String,
}

/// ObjectInfo 线上编码（Complete 应答）：msgpack named，字段面由 xl-store 的 serde
/// 派生钉住（跨版本转发在旧节点无此 RPC，本就 fail-closed）。
pub fn encode_object_info(info: &ObjectInfo) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(info).map_err(|e| Error::Encode(format!("ObjectInfo 编码失败: {e}")))
}

/// 见 [`encode_object_info`]。
pub fn decode_object_info(bytes: &[u8]) -> Result<ObjectInfo> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::Encode(format!("ObjectInfo 解码失败: {e}")))
}

/// 节点间传输。实现必须：幂等（write_shard/delete_version）、超时即错（调用方按
/// 「缺失/quorum 计数」消化）、不得把对端错误吞成成功。
#[async_trait::async_trait]
pub trait MeshTransport: Send + Sync + std::fmt::Debug {
    async fn write_shard(&self, node: &str, w: ShardWrite) -> Result<()>;
    async fn read_shard(&self, node: &str, r: ShardRead) -> Result<Option<Bytes>>;
    async fn delete_version(&self, node: &str, d: VersionDirRef) -> Result<()>;
    /// 旧版本 → 该盘 `.oss.sys/trash/`（先决链：提交 quorum 成立后回收才安全）
    async fn recycle_version(&self, node: &str, d: VersionDirRef) -> Result<()>;
    /// M5 切片 0：控制面白名单写转发（桶行广播；best-effort 由调用方 warn）。
    /// 默认 NotImplemented——测试替身免被迫实现；生产实现（TonicMesh/InProcessMesh）必须覆盖。
    async fn control_write(&self, node: &str, w: ControlWrite) -> Result<()> {
        let _ = (node, w);
        Err(crate::error::Error::Encode("control_write 未实现".into()))
    }
    /// 单片删除（幂等）——rebalance 老片清理用。
    async fn delete_shard(&self, node: &str, d: ShardDelete) -> Result<()>;
    async fn prepare_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck>;
    async fn commit_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck>;
    async fn abort_meta(&self, node: &str, m: MetaWrite) -> Result<()>;
    async fn read_meta(&self, node: &str, r: MetaRead) -> Result<MetaSnapshot>;
    /// 删除本机 meta（可见性提交点的远端分派；失败必须上抛，不得吞）
    async fn delete_meta(&self, node: &str, r: MetaRead) -> Result<()>;
    async fn ensure_bucket(&self, node: &str, bucket: &str) -> Result<()>;
    /// 删除本机桶目录（服务端空检查 + 幂等；非空 → Err）
    async fn delete_bucket_dir(&self, node: &str, bucket: &str) -> Result<()>;
    async fn list_bucket_dirs(&self, node: &str) -> Result<Vec<String>>;
    async fn list_local(&self, node: &str, l: ListLocal) -> Result<ListLocalResp>;
    /// LMU 的远端页。默认 NotImplemented——测试替身免被迫实现；生产实现
    /// （TonicMesh/InProcessMesh）必须覆盖（缺失时调用方按「少一路并集」warn 消化）。
    async fn list_uploads_local(
        &self,
        node: &str,
        l: ListUploadsLocal,
    ) -> Result<ListUploadsLocalResp> {
        let _ = (node, l);
        Err(crate::error::Error::Encode(
            "list_uploads_local 未实现".into(),
        ))
    }
    async fn health(&self, node: &str) -> Result<NodeHealth>;
    /// DP-0：对端宣告的数据面线格式（缺失/0 → 1）。默认一次 Health 探测；生产实现
    /// （[`crate::mesh_gate::GatedMesh`]）覆盖为带 TTL 的缓存版本。
    async fn data_wire_version(&self, node: &str) -> Result<u32> {
        Ok(self.health(node).await?.peer_data_wire_version())
    }
    async fn lock(&self, node: &str, req: LockReq) -> Result<LockResp>;
    async fn control_query(&self, node: &str, q: ControlQuery) -> Result<Option<ControlRow>>;

    // ---------- M3 MPU 会话粘性转发 ----------
    // 默认实现 = 显式 Unavailable（fail-closed）：旧替身不必被迫实现，且绝不会把
    // 「传输不支持」伪装成成功/NoSuchUpload。生产实现（TonicMesh / 测试 InProcessMesh）
    // 必须覆盖。

    /// 转发 UploadPart（流式；对端只落本地暂存区）。
    async fn mpu_upload_part(&self, node: &str, req: MpuPartUpload) -> Result<String> {
        let _ = (node, req);
        Err(Error::Unavailable("MPU UploadPart 转发未实现".into()))
    }
    /// 转发 CompleteMultipartUpload（对端只做本地提交）。
    async fn mpu_complete(&self, node: &str, req: MpuComplete) -> Result<ObjectInfo> {
        let _ = (node, req);
        Err(Error::Unavailable("MPU Complete 转发未实现".into()))
    }
    /// 转发 AbortMultipartUpload。
    async fn mpu_abort(&self, node: &str, req: MpuRef) -> Result<()> {
        let _ = (node, req);
        Err(Error::Unavailable("MPU Abort 转发未实现".into()))
    }
    /// 转发 ListParts。
    async fn mpu_list_parts(&self, node: &str, req: MpuRef) -> Result<Vec<MpuPartRow>> {
        let _ = (node, req);
        Err(Error::Unavailable("MPU ListParts 转发未实现".into()))
    }
}
