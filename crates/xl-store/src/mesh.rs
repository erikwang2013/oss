//! 节点间传输面（M3）：**xl-store 只认这个 trait，不依赖任何 RPC 框架**。
//!
//! 分层裁决：trait 与 DTO 定义在这里（纯 async trait + 数据），tonic 实现放 xl-client
//! （`impl MeshTransport for TonicMesh`），oss-server 装配注入。测试用 in-process 实现
//! （对端 `Store::mesh_*` 直连）——多节点语义无需真网络即可覆盖。
//!
//! 语义学分工（服务端 handler 侧）：一个节点收到的请求只操作**它自己的本地槽**；
//! 扇出永远由「协调者」（收到客户端请求的节点）做。

use bytes::Bytes;

use crate::error::Result;

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
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct ListLocalResp {
    pub entries: Vec<ListEntry>,
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
}

/// 控制面后端（Task 9）：服务端**本地**查询执行器。oss-server 注入 sqlx 实现
/// （复用 xl-s3 的 Acl，判权 SQL 唯一事实源）；in-process / xl-client 的服务端
/// handler 统一经 `Store::mesh_control_query` 调它。**只查本节点 DB，不扇出**。
#[async_trait::async_trait]
pub trait ControlBackend: Send + Sync + std::fmt::Debug {
    /// 白名单查询（安全边界在实现侧）；None = 本节点查无。
    async fn control_query(&self, q: ControlQuery) -> Result<Option<ControlRow>>;
}

/// 桶 → set 解析（M4 切片 8）。**独立于 MeshTransport 的扇出面**（verifier M2）：
/// 「对端不可达」必须与「对端答无行」可区分——不复用吞错的 ControlMesh::ask 语义。
#[async_trait::async_trait]
pub trait SetResolver: Send + Sync + std::fmt::Debug {
    /// `Ok(Some(set))` = 有行（含一致性合并）；`Ok(None)` = **全部可达且都答无**；
    /// `Err` = 任一传输错 / 行分叉（多节点答案不一致）/ 无任何可达节点。
    async fn resolve_bucket_set(&self, bucket: &str) -> Result<Option<u32>>;
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
    async fn health(&self, node: &str) -> Result<NodeHealth>;
    async fn lock(&self, node: &str, req: LockReq) -> Result<LockResp>;
    async fn control_query(&self, node: &str, q: ControlQuery) -> Result<Option<ControlRow>>;
}
