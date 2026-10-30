//! xl-client（M3 Task 3）：节点间 mesh 的 **tonic 实现**——客户端封装（TonicMesh）
//! 与服务端 handler（serve，经 `MeshSvc` 调 `Store::mesh_*`）。
//!
//! 分层约定（与 xl-store/src/mesh.rs 头注一致）：
//! - 客户端：lazy connect（`connect_lazy` 不预连）、连接超时 1s / 请求超时 5s；
//!   每个 RPC 带 metadata `x-oss-cluster-token`（集群内共享令牌，非公网面）。
//! - 服务端：拦截器校验令牌；handler **只操作本节点本地槽**，不做任何扇出。
//!
//! 落地节奏：本 Task 只实现 Health（切片一自验收）；其余 RPC 随所属 Task 填充，
//! 在此之前显式 `unimplemented`（不静默成功）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::watch;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use xl_store::error::{Error, Result};
use xl_store::mesh::{self, MeshTransport};
use xl_store::meta::ObjectLockRecord;
use xl_store::store::{ByteStream, ObjectInfo, Store, VersioningMode, ec};

/// 集群内共享令牌的 metadata 键。
const TOKEN_KEY: &str = "x-oss-cluster-token";
/// W3C traceparent（M3 轻量传播：无 OTLP 接入前，客户端注入、服务端记入日志——
/// 双节点可按同一 trace id 抽查，设计 D:362 的最小兑现）。
const TRACEPARENT_KEY: &str = "traceparent";
/// MPU 转发单条 RPC 的超时：普通 mesh RPC 是 5s 快路径口径，但 UploadPart 要走完
/// 整段数据（最大 5 GiB），必须单独放宽。超时 = 属主侧流被截断 → 半截 part 被清理。
const MPU_FORWARD_TIMEOUT: Duration = Duration::from_secs(3600);
/// MPU 转发分块大小（tonic 默认 4 MiB 解码上限内留余量；零拷贝 `Bytes::slice`）。
const MPU_FORWARD_CHUNK: usize = 1024 * 1024;

fn rpc_err(node: &str, e: Status) -> Error {
    Error::Encode(format!("mesh RPC 失败（node {node}）：{e}"))
}

/// MPU 转发的传输失败 → `Unavailable`（S3 503；调用方据此确定性失败，绝不降级）。
fn rpc_unavailable(node: &str, e: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("MPU 属主 {node} 不可达：{e}"))
}

/// L-3：SSE-C 的客户密钥（Complete）与分片（UploadPart）不得跨明文 mesh 传输。
/// 属主是本机时不经此处（本地 SSE-C MPU 不受影响）；跨节点转发要求开启 mesh TLS
/// （oss-server 配置 `[tls.server]`，即 `peer_tls`），否则 fail-closed。
fn mesh_tls_required() -> Error {
    Error::Unavailable(
        "SSE-C MPU 跨节点转发要求启用 mesh TLS（[tls.server]）：拒绝在明文节点间传输客户密钥/分片"
            .into(),
    )
}

// ---------- MPU 错误线上编码 ----------
//
// 语义错误经响应信封（code+payload）回传，避免把 NoSuchUpload/EntityTooSmall 等
// 客户端可纠正错误压成 gRPC 状态码丢失精度；只有传输层失败走 Unavailable。

fn err_wire(e: &Error) -> (&'static str, String) {
    match e {
        Error::NoSuchBucket(s) => ("NoSuchBucket", s.clone()),
        Error::BucketAlreadyExists(s) => ("BucketAlreadyExists", s.clone()),
        Error::BucketNotEmpty(s) => ("BucketNotEmpty", s.clone()),
        Error::NoSuchKey(s) => ("NoSuchKey", s.clone()),
        Error::InvalidKey(s) => ("InvalidKey", s.clone()),
        Error::InvalidBucketName(s) => ("InvalidBucketName", s.clone()),
        Error::CorruptMeta(s) => ("CorruptMeta", s.clone()),
        Error::NoSuchUpload(s) => ("NoSuchUpload", s.clone()),
        Error::InvalidPart(s) => ("InvalidPart", s.clone()),
        Error::InvalidPartRange(s) => ("InvalidPartRange", s.clone()),
        Error::InvalidPartOrder(s) => ("InvalidPartOrder", s.clone()),
        Error::EntityTooSmall(s) => ("EntityTooSmall", s.clone()),
        Error::BadDigest(s) => ("BadDigest", s.clone()),
        Error::QuorumUnmet(s) => ("QuorumUnmet", s.clone()),
        Error::Io(e) => ("Io", e.to_string()),
        Error::Encode(s) => ("Encode", s.clone()),
        Error::DeleteMarker(s) => ("DeleteMarker", s.clone()),
        Error::InvalidArgument(s) => ("InvalidArgument", s.clone()),
        Error::InvalidObjectState(s) => ("InvalidObjectState", s.clone()),
        Error::RestoreAlreadyInProgress(s) => ("RestoreAlreadyInProgress", s.clone()),
        Error::Crypto(s) => ("Crypto", s.clone()),
        Error::ObjectLocked(s) => ("ObjectLocked", s.clone()),
        Error::Unavailable(s) => ("Unavailable", s.clone()),
        Error::Unsupported(s) => ("Unsupported", s.clone()),
        Error::Conflict(s) => ("Conflict", s.clone()),
        Error::BadGateway(s) => ("BadGateway", s.clone()),
        // DP-0：混版数据面 fail-closed 必须原样过线（S3 503 peer_incompatible）。
        Error::PeerIncompatible(s) => ("PeerIncompatible", s.clone()),
    }
}

fn err_from_wire(code: &str, msg: String) -> Error {
    match code {
        "NoSuchBucket" => Error::NoSuchBucket(msg),
        "BucketAlreadyExists" => Error::BucketAlreadyExists(msg),
        "BucketNotEmpty" => Error::BucketNotEmpty(msg),
        "NoSuchKey" => Error::NoSuchKey(msg),
        "InvalidKey" => Error::InvalidKey(msg),
        "InvalidBucketName" => Error::InvalidBucketName(msg),
        "CorruptMeta" => Error::CorruptMeta(msg),
        "NoSuchUpload" => Error::NoSuchUpload(msg),
        "InvalidPart" => Error::InvalidPart(msg),
        "InvalidPartRange" => Error::InvalidPartRange(msg),
        "InvalidPartOrder" => Error::InvalidPartOrder(msg),
        "EntityTooSmall" => Error::EntityTooSmall(msg),
        "BadDigest" => Error::BadDigest(msg),
        "QuorumUnmet" => Error::QuorumUnmet(msg),
        "Io" => Error::Io(std::io::Error::other(msg)),
        "Encode" => Error::Encode(msg),
        "DeleteMarker" => Error::DeleteMarker(msg),
        "InvalidArgument" => Error::InvalidArgument(msg),
        "InvalidObjectState" => Error::InvalidObjectState(msg),
        "RestoreAlreadyInProgress" => Error::RestoreAlreadyInProgress(msg),
        "Crypto" => Error::Crypto(msg),
        "ObjectLocked" => Error::ObjectLocked(msg),
        "Unavailable" => Error::Unavailable(msg),
        "Unsupported" => Error::Unsupported(msg),
        "Conflict" => Error::Conflict(msg),
        "BadGateway" => Error::BadGateway(msg),
        "PeerIncompatible" => Error::PeerIncompatible(msg),
        // 未知 code：不猜语义，按内部错误上抛（fail-closed）
        other => Error::Encode(format!("对端错误码 {other}：{msg}")),
    }
}

fn mpu_ref_to_proto(r: mesh::MpuRef) -> xl_proto::MpuRef {
    xl_proto::MpuRef {
        bucket: r.bucket,
        key: r.key,
        upload_id: r.upload_id,
    }
}

fn mpu_ref_from_proto(r: xl_proto::MpuRef) -> mesh::MpuRef {
    mesh::MpuRef {
        bucket: r.bucket,
        key: r.key,
        upload_id: r.upload_id,
    }
}

/// 对端 `WriteResp{ok:false}` → 显式错误（不得吞成成功）。
fn check_write(node: &str, r: xl_proto::WriteResp) -> Result<()> {
    if r.ok {
        Ok(())
    } else {
        Err(Error::Encode(format!("对端 {node} 拒收：{}", r.msg)))
    }
}

/// 服务端错误 → `internal`（5xx 语义透传消息）。
fn internal(e: Error) -> Status {
    Status::internal(e.to_string())
}

fn write_ok() -> xl_proto::WriteResp {
    xl_proto::WriteResp {
        ok: true,
        msg: String::new(),
    }
}

fn meta_write_to_proto(m: mesh::MetaWrite) -> xl_proto::MetaWrite {
    xl_proto::MetaWrite {
        bucket: m.bucket,
        key: m.key,
        meta: m.meta.to_vec(),
    }
}

fn meta_read_to_proto(r: mesh::MetaRead) -> xl_proto::MetaRead {
    xl_proto::MetaRead {
        bucket: r.bucket,
        key: r.key,
    }
}

fn dir_ref_to_proto(d: mesh::VersionDirRef) -> xl_proto::VersionDirRef {
    xl_proto::VersionDirRef {
        bucket: d.bucket,
        key: d.key,
        uuid: d.uuid,
    }
}

fn dir_ref_from_proto(d: xl_proto::VersionDirRef) -> mesh::VersionDirRef {
    mesh::VersionDirRef {
        bucket: d.bucket,
        key: d.key,
        uuid: d.uuid,
    }
}

fn meta_write_from_proto(m: xl_proto::MetaWrite) -> mesh::MetaWrite {
    mesh::MetaWrite {
        bucket: m.bucket,
        key: m.key,
        meta: m.meta.into(),
    }
}

fn meta_read_from_proto(r: xl_proto::MetaRead) -> mesh::MetaRead {
    mesh::MetaRead {
        bucket: r.bucket,
        key: r.key,
    }
}

fn meta_ack_to_proto(a: mesh::MetaAck) -> xl_proto::MetaAck {
    xl_proto::MetaAck {
        ok: a.ok,
        msg: a.msg,
        slots: a
            .slots
            .into_iter()
            .map(|s| xl_proto::SlotAck {
                slot: s.slot as u32,
                ok: s.ok,
                hash: s.hash.to_vec(),
            })
            .collect(),
        slots_ok: a.slots_ok,
    }
}

fn meta_ack_from_proto(a: xl_proto::MetaAck) -> mesh::MetaAck {
    mesh::MetaAck {
        ok: a.ok,
        msg: a.msg,
        slots: a
            .slots
            .into_iter()
            .map(|s| {
                // 防御式：hash 不足 32 字节补零、超长截断（proto bytes 不限长）。
                let mut hash = [0u8; 32];
                let n = hash.len().min(s.hash.len());
                hash[..n].copy_from_slice(&s.hash[..n]);
                mesh::SlotAck {
                    slot: s.slot as u16,
                    ok: s.ok,
                    hash,
                }
            })
            .collect(),
        slots_ok: a.slots_ok,
    }
}

// ---------- 拦截器 ----------

/// 客户端：注入集群令牌。
#[derive(Clone)]
struct TokenInterceptor {
    token: MetadataValue<tonic::metadata::Ascii>,
}

impl TokenInterceptor {
    fn new(secret: &str) -> Result<Self> {
        let token = MetadataValue::try_from(secret)
            .map_err(|e| Error::Encode(format!("集群令牌非法（metadata）：{e}")))?;
        Ok(Self { token })
    }
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        req.metadata_mut().insert(TOKEN_KEY, self.token.clone());
        // traceparent 注入：`00-<32hex trace id>-<16hex span id>-01`（uuid 派生，零新依赖）
        let tp = format!(
            "00-{}-{}-01",
            uuid::Uuid::new_v4().simple(),
            &uuid::Uuid::new_v4().simple().to_string()[..16]
        );
        if let Ok(v) = MetadataValue::try_from(tp) {
            req.metadata_mut().insert(TRACEPARENT_KEY, v);
        }
        Ok(req)
    }
}

/// 常数时间比较集群令牌（长度不等直接 false；不因前缀匹配提前返回——避免计时侧信道
/// 逐字节猜出共享令牌）。
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// 服务端：校验集群令牌。
#[derive(Clone)]
struct SecretInterceptor {
    secret: String,
}

impl Interceptor for SecretInterceptor {
    fn call(&mut self, req: Request<()>) -> std::result::Result<Request<()>, Status> {
        let ok = req
            .metadata()
            .get(TOKEN_KEY)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| ct_eq(v, &self.secret));
        if ok {
            // 提取 traceparent（debug 级：默认日志级别下不刷屏；抽查用 RUST_LOG=debug）
            if let Some(tp) = req
                .metadata()
                .get(TRACEPARENT_KEY)
                .and_then(|v| v.to_str().ok())
            {
                tracing::debug!(traceparent = tp, "mesh 入站（trace 传播）");
            }
            Ok(req)
        } else {
            Err(Status::unauthenticated("集群令牌校验失败"))
        }
    }
}

// ---------- 客户端 ----------

type MeshChannel = tonic::codegen::InterceptedService<Channel, TokenInterceptor>;
type MeshClientMap = HashMap<String, xl_proto::mesh_client::MeshClient<MeshChannel>>;

/// mesh TLS 热加载信号：值为当前 tonic 客户端配置（`None` = 明文）。
pub type TlsWatch = watch::Receiver<Option<tonic::transport::ClientTlsConfig>>;

/// 节点间传输的 tonic 实现（lazy connect：首次 RPC 才建连，不阻塞启动）。
pub struct TonicMesh {
    interceptor: TokenInterceptor,
    nodes: Vec<(String, String)>,
    connect: Duration,
    request: Duration,
    /// `Some` = 订阅 TLS 物料热加载；成功 reload 后下一次 RPC 前重建全部通道。
    /// 旧通道上已发出的请求持有自己的 client clone，继续完成（不中断在途 RPC）。
    tls_watch: Option<Mutex<TlsWatch>>,
    clients: RwLock<HashMap<String, xl_proto::mesh_client::MeshClient<MeshChannel>>>,
    /// MPU 转发专用通道：同一节点/令牌，但请求超时 = [`MPU_FORWARD_TIMEOUT`]
    /// （普通通道的快路径 5s 会把大 part 传输截断）。
    /// 第二项 = 这些通道是否走 TLS，与通道同锁替换：SSE-C 转发据此 fail-closed
    /// （L-3：明文 mesh 不得承载客户密钥/分片）。热加载切换瞬间不会读到错配组合。
    mpu_clients: RwLock<(MeshClientMap, bool)>,
}

impl std::fmt::Debug for TonicMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let clients = self.clients.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("TonicMesh")
            .field("nodes", &clients.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl TonicMesh {
    /// `nodes` = (node_id, "ip:port") 全表（含自身——集群锁要对全节点广播，含本机的表操作）。
    pub fn new(_self_id: &str, secret: &str, nodes: &[(String, String)]) -> Result<Self> {
        Self::new_with_timeouts(
            _self_id,
            secret,
            nodes,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
    }

    /// 同 [`Self::new`]，mesh 全链路 TLS（`Some` = 所有节点走 https，证书主机名校验）。
    pub fn new_with_tls(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Result<Self> {
        Self::new_with_timeouts_tls(
            _self_id,
            secret,
            nodes,
            Duration::from_secs(1),
            Duration::from_secs(5),
            tls,
        )
    }

    /// M9 S4：同 [`Self::new`]，连接/请求超时可配（stretch `[stretch].mesh_timeout_ms`）。
    pub fn new_with_timeouts(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        connect: Duration,
        request: Duration,
    ) -> Result<Self> {
        Self::new_with_timeouts_tls(_self_id, secret, nodes, connect, request, None)
    }

    /// 超时可配 + 可选 mesh TLS；`tls = None` 保持旧明文行为，但 SSE-C MPU 跨节点
    /// 转发会 fail-closed（见 `mesh_tls_required`）。
    pub fn new_with_timeouts_tls(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        connect: Duration,
        request: Duration,
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Result<Self> {
        let interceptor = TokenInterceptor::new(secret)?;
        let secure = tls.is_some();
        let (clients, mpu_clients) =
            Self::build_clients(&interceptor, nodes, connect, request, tls.as_ref())?;
        Ok(Self {
            interceptor,
            nodes: nodes.to_vec(),
            connect,
            request,
            tls_watch: None,
            clients: RwLock::new(clients),
            mpu_clients: RwLock::new((mpu_clients, secure)),
        })
    }

    /// mesh TLS 热加载版本：`tls` 为 `None` 时保持明文；`Some` 时以当前值为初始配置，
    /// 之后每次成功 reload 发布的配置都会在下次 RPC 前重建通道（构建失败保留旧通道）。
    pub fn new_with_tls_watch(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        tls: Option<TlsWatch>,
    ) -> Result<Self> {
        Self::new_with_timeouts_tls_watch(
            _self_id,
            secret,
            nodes,
            Duration::from_secs(1),
            Duration::from_secs(5),
            tls,
        )
    }

    /// [`Self::new_with_tls_watch`] 的连接/请求超时可配版本。
    pub fn new_with_timeouts_tls_watch(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        connect: Duration,
        request: Duration,
        tls: Option<TlsWatch>,
    ) -> Result<Self> {
        let interceptor = TokenInterceptor::new(secret)?;
        let initial = tls.as_ref().map(|rx| rx.borrow().clone()).unwrap_or(None);
        let secure = initial.is_some();
        let (clients, mpu_clients) =
            Self::build_clients(&interceptor, nodes, connect, request, initial.as_ref())?;
        Ok(Self {
            interceptor,
            nodes: nodes.to_vec(),
            connect,
            request,
            tls_watch: tls.map(Mutex::new),
            clients: RwLock::new(clients),
            mpu_clients: RwLock::new((mpu_clients, secure)),
        })
    }

    fn build_clients(
        interceptor: &TokenInterceptor,
        nodes: &[(String, String)],
        connect: Duration,
        request: Duration,
        tls: Option<&tonic::transport::ClientTlsConfig>,
    ) -> Result<(MeshClientMap, MeshClientMap)> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let mut clients = HashMap::with_capacity(nodes.len());
        let mut mpu_clients = HashMap::with_capacity(nodes.len());
        for (id, addr) in nodes {
            let host_port = addr
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/');
            let endpoint = |timeout: Duration| -> Result<Endpoint> {
                let mut ep = Endpoint::from_shared(format!("{scheme}://{host_port}"))
                    .map_err(|e| Error::Encode(format!("节点 {id} 地址非法（{addr}）：{e}")))?
                    .connect_timeout(connect)
                    .timeout(timeout);
                if let Some(t) = tls {
                    ep = ep
                        .tls_config((*t).clone())
                        .map_err(|e| Error::Encode(format!("节点 {id} TLS 配置失败：{e}")))?;
                }
                Ok(ep)
            };
            let ch = endpoint(request)?.connect_lazy();
            clients.insert(
                id.clone(),
                xl_proto::mesh_client::MeshClient::with_interceptor(ch, interceptor.clone()),
            );
            let mch = endpoint(MPU_FORWARD_TIMEOUT)?.connect_lazy();
            mpu_clients.insert(
                id.clone(),
                xl_proto::mesh_client::MeshClient::with_interceptor(mch, interceptor.clone()),
            );
        }
        Ok((clients, mpu_clients))
    }

    /// TLS 物料变化时重建全部通道并整体替换；构建失败保留旧通道（fail closed）。
    /// 每个 mesh RPC 都会调用；用 try_lock 避免和正在重建的线程串行排队，抢不到就
    /// 继续用旧通道（下一次 RPC 再检查），重建本身仍然串行。
    fn refresh_tls(&self) {
        let Some(rx) = &self.tls_watch else { return };
        let mut rx = match rx.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return,
        };
        if !rx.has_changed().unwrap_or(false) {
            return;
        }
        let tls = rx.borrow_and_update().clone();
        match Self::build_clients(
            &self.interceptor,
            &self.nodes,
            self.connect,
            self.request,
            tls.as_ref(),
        ) {
            Ok((fresh, fresh_mpu)) => {
                let nodes = fresh.len();
                let secure = tls.is_some();
                *self.clients.write().unwrap_or_else(|e| e.into_inner()) = fresh;
                *self.mpu_clients.write().unwrap_or_else(|e| e.into_inner()) = (fresh_mpu, secure);
                tracing::info!(
                    nodes,
                    tls = tls.is_some(),
                    "mesh gRPC TLS 物料已热加载，通道已重建"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "mesh gRPC TLS 通道重建失败，继续使用旧通道")
            }
        }
    }

    fn client(&self, node: &str) -> Result<xl_proto::mesh_client::MeshClient<MeshChannel>> {
        self.refresh_tls();
        self.clients
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
            .cloned()
            .ok_or_else(|| Error::Encode(format!("mesh：未知节点 {node}（不在拓扑表内）")))
    }

    /// 返回 (MPU 通道, 该通道是否 TLS)；两项同锁读取，热加载时不会读到错配组合。
    fn mpu_client(
        &self,
        node: &str,
    ) -> Result<(xl_proto::mesh_client::MeshClient<MeshChannel>, bool)> {
        self.refresh_tls();
        let guard = self.mpu_clients.read().unwrap_or_else(|e| e.into_inner());
        let client = guard.0.get(node).cloned().ok_or_else(|| {
            Error::Unavailable(format!("MPU 属主 {node} 不在拓扑表内（无法转发）"))
        })?;
        Ok((client, guard.1))
    }
}

#[async_trait::async_trait]
impl MeshTransport for TonicMesh {
    async fn health(&self, node: &str) -> Result<mesh::NodeHealth> {
        let resp = self
            .client(node)?
            .health(xl_proto::Empty {})
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(mesh::NodeHealth {
            node_id: resp.node_id,
            version: resp.version,
            slots: resp
                .slots
                .into_iter()
                .map(|s| (s.slot as u16, s.up))
                .collect(),
            buckets: resp.buckets,
            // DP-0：旧节点无此字段 → proto 缺省 0，xl-store 侧统一按 1 解读。
            data_wire_version: resp.data_wire_version,
            // 拓扑指纹：旧节点缺省 0/空串 = 未宣告（消费方不得当匹配）。
            topo_epoch: resp.topo_epoch,
            topo_hash: resp.topo_hash,
            drives: resp
                .drives
                .into_iter()
                .map(|d| mesh::DriveSpace {
                    set: d.set as u16,
                    drive: d.drive as u16,
                    free_bytes: d.free_bytes,
                })
                .collect(),
            maintenance: resp.maintenance,
        })
    }
    // 分片 / meta / 版本目录 / 回收（切片二）；其余 RPC 随所属 Task 落地（6/8/9）。
    async fn write_shard(&self, node: &str, w: mesh::ShardWrite) -> Result<()> {
        let resp = self
            .client(node)?
            .write_shard(xl_proto::ShardWrite {
                bucket: w.bucket,
                key: w.key,
                uuid: w.uuid,
                part: w.part,
                shard: w.shard,
                crc32c: w.crc32c,
                data: w.data.to_vec(),
                chunked: w.chunked,
                offset: w.offset,
                chunk_crc32c: w.chunk_crc32c,
                is_final: w.is_final,
                slot: w.slot,
                set: w.set,
            })
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn read_shard(&self, node: &str, r: mesh::ShardRead) -> Result<Option<bytes::Bytes>> {
        let resp = self
            .client(node)?
            .read_shard(xl_proto::ShardRead {
                bucket: r.bucket,
                key: r.key,
                uuid: r.uuid,
                part: r.part,
                shard: r.shard,
                offset: r.offset,
                length: r.length,
                slot: r.slot,
                set: r.set,
            })
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(resp.found.then(|| bytes::Bytes::from(resp.data)))
    }
    async fn delete_version(&self, node: &str, d: mesh::VersionDirRef) -> Result<()> {
        let resp = self
            .client(node)?
            .delete_version(dir_ref_to_proto(d))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn delete_shard(&self, node: &str, d: mesh::ShardDelete) -> Result<()> {
        let resp = self
            .client(node)?
            .delete_shard(xl_proto::ShardDelete {
                bucket: d.bucket,
                key: d.key,
                uuid: d.uuid,
                part: d.part,
                shard: d.shard,
                slot: d.slot,
                set: d.set,
            })
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn recycle_version(&self, node: &str, d: mesh::VersionDirRef) -> Result<()> {
        let resp = self
            .client(node)?
            .recycle_version(dir_ref_to_proto(d))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn prepare_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        let resp = self
            .client(node)?
            .prepare_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(meta_ack_from_proto(resp))
    }
    async fn commit_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        let resp = self
            .client(node)?
            .commit_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(meta_ack_from_proto(resp))
    }
    async fn abort_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<()> {
        let resp = self
            .client(node)?
            .abort_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn read_meta(&self, node: &str, r: mesh::MetaRead) -> Result<mesh::MetaSnapshot> {
        let resp = self
            .client(node)?
            .read_meta(meta_read_to_proto(r))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        // 未命中恒返回快照（meta=None，覆盖率快路径需要「缺席也计数」）。
        Ok(mesh::MetaSnapshot {
            meta: resp.found.then(|| bytes::Bytes::from(resp.meta)),
            mod_time_nanos: resp.mod_time_nanos,
            slots_ok: resp.slots_ok,
        })
    }
    async fn delete_meta(&self, node: &str, r: mesh::MetaRead) -> Result<()> {
        let resp = self
            .client(node)?
            .delete_meta(meta_read_to_proto(r))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn delete_bucket_dir(&self, node: &str, bucket: &str) -> Result<()> {
        let resp = self
            .client(node)?
            .delete_bucket(xl_proto::BucketRef {
                bucket: bucket.to_string(),
            })
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn ensure_bucket(&self, n: &str, b: &str) -> Result<()> {
        let resp = self
            .client(n)?
            .ensure_bucket(xl_proto::BucketRef {
                bucket: b.to_string(),
            })
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        check_write(n, resp)
    }
    async fn list_bucket_dirs(&self, n: &str) -> Result<Vec<String>> {
        let resp = self
            .client(n)?
            .list_buckets(xl_proto::Empty {})
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        Ok(resp.names)
    }
    async fn list_local(&self, n: &str, l: mesh::ListLocal) -> Result<mesh::ListLocalResp> {
        let req = xl_proto::ListReq {
            bucket: l.bucket,
            prefix: l.prefix,
            delimiter: l.delimiter.unwrap_or_default(),
            marker: l.marker.unwrap_or_default(),
            limit: l.limit as u32,
            include_deleted: l.include_deleted,
            marker_exclusive: l.marker_exclusive,
        };
        let resp = self
            .client(n)?
            .list_local(req)
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        Ok(mesh::ListLocalResp {
            entries: resp
                .entries
                .into_iter()
                .map(|e| mesh::ListEntry {
                    key: e.key,
                    size: e.size,
                    etag: e.etag,
                    max_mod_time_nanos: e.max_mod_time_nanos,
                })
                .collect(),
            prefixes: resp.prefixes,
            slots_ok: resp.slots_ok,
        })
    }
    async fn list_uploads_local(
        &self,
        n: &str,
        l: mesh::ListUploadsLocal,
    ) -> Result<mesh::ListUploadsLocalResp> {
        let req = xl_proto::UploadListReq {
            bucket: l.bucket,
            prefix: l.prefix,
            delimiter: l.delimiter.unwrap_or_default(),
            key_marker: l.key_marker.unwrap_or_default(),
            upload_id_marker: l.upload_id_marker.unwrap_or_default(),
            limit: l.limit as u32,
        };
        let resp = self
            .client(n)?
            .list_uploads_local(req)
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        Ok(mesh::ListUploadsLocalResp {
            uploads: resp
                .uploads
                .into_iter()
                .map(|u| mesh::UploadEntry {
                    key: u.key,
                    upload_id: u.upload_id,
                    initiated_nanos: u.initiated_nanos,
                    storage_class: u.storage_class,
                })
                .collect(),
            prefixes: resp.prefixes,
            slots_ok: resp.slots_ok,
        })
    }
    async fn lock(&self, n: &str, req: mesh::LockReq) -> Result<mesh::LockResp> {
        let op = match req.op {
            mesh::LockOp::Acquire => xl_proto::LockOp::Acquire,
            mesh::LockOp::Renew => xl_proto::LockOp::Renew,
            mesh::LockOp::Release => xl_proto::LockOp::Release,
        };
        let resp = self
            .client(n)?
            .lock(xl_proto::LockReq {
                bucket: req.bucket,
                key: req.key,
                epoch_nanos: req.epoch_nanos,
                epoch_node: req.epoch_node,
                holder: req.holder,
                ttl_ms: req.ttl_ms,
                op: op as i32,
            })
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        Ok(mesh::LockResp {
            granted: resp.granted,
        })
    }
    async fn control_write(&self, node: &str, w: mesh::ControlWrite) -> Result<()> {
        let mesh::ControlWrite::BucketRowSet { bucket, set } = w;
        let resp = self
            .client(node)?
            .control_write(xl_proto::ControlWriteReq {
                kind: 0, // BUCKET_ROW_SET
                a: bucket,
                has_set: set.is_some(),
                set: set.unwrap_or(0) as u32,
            })
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }

    async fn control_query(
        &self,
        n: &str,
        q: mesh::ControlQuery,
    ) -> Result<Option<mesh::ControlRow>> {
        // kind 编码（两侧一致）：0=AccessKey(ak→a) 1=Permission(uid,a=bucket,c=action)
        // 2=OwnedBuckets(uid) 3=VisibleBuckets(uid) 4=BucketOwner(a=bucket)
        let req = match q {
            mesh::ControlQuery::AccessKey(ak) => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::AccessKey as i32,
                a: ak,
                b: String::new(),
                c: 0,
                uid: 0,
            },
            mesh::ControlQuery::Permission {
                uid,
                bucket,
                action,
            } => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::Permission as i32,
                a: bucket,
                b: String::new(),
                c: action as u32,
                uid,
            },
            mesh::ControlQuery::OwnedBuckets(uid) => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::OwnedBuckets as i32,
                a: String::new(),
                b: String::new(),
                c: 0,
                uid,
            },
            mesh::ControlQuery::VisibleBuckets(uid) => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::VisibleBuckets as i32,
                a: String::new(),
                b: String::new(),
                c: 0,
                uid,
            },
            mesh::ControlQuery::BucketOwner(bucket) => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::BucketOwner as i32,
                a: bucket,
                b: String::new(),
                c: 0,
                uid: 0,
            },
            mesh::ControlQuery::BucketSet(bucket) => xl_proto::ControlReq {
                kind: xl_proto::ControlKind::BucketSet as i32,
                a: bucket,
                b: String::new(),
                c: 0,
                uid: 0,
            },
        };
        let resp = self
            .client(n)?
            .control_query(req)
            .await
            .map_err(|e| rpc_err(n, e))?
            .into_inner();
        Ok(resp.found.then(|| mesh::ControlRow {
            uid: Some(resp.uid),
            is_admin: Some(resp.is_admin),
            active: Some(resp.active),
            secret: (!resp.secret.is_empty()).then_some(resp.secret),
            flag: Some(resp.flag),
            names: Some(resp.names),
            set: Some(resp.set),
            // M5 切片 5（S9）：空串 = 无策略（与 row.session_policy=None 同义）
            session_policy: (!resp.session_policy.is_empty()).then_some(resp.session_policy),
        }))
    }

    // ---------- M3 MPU 会话粘性：流式转发 ----------

    async fn mpu_upload_part(&self, node: &str, req: mesh::MpuPartUpload) -> Result<String> {
        // 先取 client 再移动 body（借用在 await 前收口）
        let (mut client, secure) = self.mpu_client(node)?;
        // L-3：带 SSE-C 校验头的 part 属于 SSE-C MPU，禁止走明文 mesh。
        if req.sse_c_md5.is_some() && !secure {
            return Err(mesh_tls_required());
        }
        let resp = client
            .upload_part(mpu_chunk_stream(req))
            .await
            .map_err(|e| rpc_unavailable(node, e))?
            .into_inner();
        if resp.ok {
            Ok(resp.etag)
        } else {
            Err(err_from_wire(&resp.code, resp.msg))
        }
    }

    async fn mpu_complete(&self, node: &str, req: mesh::MpuComplete) -> Result<ObjectInfo> {
        // L-3：SSE-C 原始客户钥只允许走 mesh TLS；明文通道 fail-closed（本地属主不经此处）。
        let is_ssec = matches!(&req.encryption, Some(mesh::MpuEnc::SseC(_)));
        let (mut client, secure) = self.mpu_client(node)?;
        if is_ssec && !secure {
            return Err(mesh_tls_required());
        }
        let (enc_kind, enc_cmk, sse_c_key) = match req.encryption {
            None => (0, String::new(), Vec::new()),
            Some(mesh::MpuEnc::SseS3) => (1, String::new(), Vec::new()),
            Some(mesh::MpuEnc::SseKms) => (2, String::new(), Vec::new()),
            Some(mesh::MpuEnc::SseKmsCmk(name)) => (3, name, Vec::new()),
            // L-3：原始客户钥随 Complete 转发；只有 mesh TLS 能防止其明文过网。
            Some(mesh::MpuEnc::SseC(key)) => (4, String::new(), key.to_vec()),
        };
        let (has_lock, lock_mode, lock_until, lock_legal_hold) = match &req.lock {
            None => (false, String::new(), 0, false),
            Some(l) => (
                true,
                l.mode.clone().unwrap_or_default(),
                l.retain_until_nanos.unwrap_or(0),
                l.legal_hold,
            ),
        };
        let versioning_mode = match req.mode {
            VersioningMode::Off => 0,
            VersioningMode::Enabled => 1,
            VersioningMode::Suspended => 2,
        };
        let resp = client
            .complete_upload(xl_proto::MpuCompleteReq {
                target: Some(mpu_ref_to_proto(mesh::MpuRef {
                    bucket: req.bucket,
                    key: req.key,
                    upload_id: req.upload_id,
                })),
                parts: req
                    .parts
                    .iter()
                    .map(|(n, etag)| xl_proto::MpuPart {
                        number: *n as u32,
                        etag: etag.clone(),
                    })
                    .collect(),
                versioning_mode,
                enc_kind,
                enc_cmk,
                sse_c_key,
                has_lock,
                lock_mode,
                lock_until_nanos: lock_until,
                lock_legal_hold,
            })
            .await
            .map_err(|e| rpc_unavailable(node, e))?
            .into_inner();
        if !resp.ok {
            return Err(err_from_wire(&resp.code, resp.msg));
        }
        mesh::decode_object_info(&resp.info)
    }

    async fn mpu_abort(&self, node: &str, req: mesh::MpuRef) -> Result<()> {
        let (mut client, _) = self.mpu_client(node)?;
        let resp = client
            .abort_upload(mpu_ref_to_proto(req))
            .await
            .map_err(|e| rpc_unavailable(node, e))?
            .into_inner();
        if resp.ok {
            Ok(())
        } else {
            Err(err_from_wire(&resp.code, resp.msg))
        }
    }

    async fn mpu_list_parts(&self, node: &str, req: mesh::MpuRef) -> Result<Vec<mesh::MpuPartRow>> {
        let (mut client, _) = self.mpu_client(node)?;
        let resp = client
            .list_parts(mpu_ref_to_proto(req))
            .await
            .map_err(|e| rpc_unavailable(node, e))?
            .into_inner();
        if !resp.ok {
            return Err(err_from_wire(&resp.code, resp.msg));
        }
        Ok(resp
            .parts
            .into_iter()
            .map(|p| mesh::MpuPartRow {
                part_number: u16::try_from(p.number).unwrap_or(0),
                size: p.size,
                etag: p.etag,
            })
            .collect())
    }
}

/// UploadPart 转发发送流：首块元数据 → 若干 ≤[`MPU_FORWARD_CHUNK`] 数据块 → 末块。
/// body 出错时发 `abort=true` 并终止（属主据此拒收并清理，绝不会把半截数据当成功）。
/// 正常情况下最后必发 `is_final=true`；连接中断时属主见不到末块 → 同样拒收。
fn mpu_chunk_stream(
    req: mesh::MpuPartUpload,
) -> impl futures::Stream<Item = xl_proto::MpuPartChunk> + Send + 'static {
    struct St {
        meta: Option<(String, String, String, u32, String)>,
        body: mesh::MpuPartUpload, // 持有原始流（未单独拆字段，省一次装箱）
        queue: std::collections::VecDeque<bytes::Bytes>,
        done: bool,
    }
    impl St {
        fn take_meta(&mut self) -> Option<xl_proto::MpuPartChunk> {
            let (bucket, key, upload_id, part_number, sse) = self.meta.take()?;
            Some(xl_proto::MpuPartChunk {
                bucket,
                key,
                upload_id,
                part_number,
                sse_c_md5: sse,
                ..Default::default()
            })
        }
    }
    let init = St {
        meta: Some((
            req.bucket.clone(),
            req.key.clone(),
            req.upload_id.clone(),
            req.part_number as u32,
            req.sse_c_md5.clone().unwrap_or_default(),
        )),
        body: req,
        queue: std::collections::VecDeque::new(),
        done: false,
    };
    futures::stream::unfold(init, |mut st| async move {
        use futures::StreamExt as _;
        loop {
            if let Some(m) = st.take_meta() {
                return Some((m, st));
            }
            if let Some(piece) = st.queue.pop_front() {
                return Some((
                    xl_proto::MpuPartChunk {
                        data: piece.to_vec(),
                        ..Default::default()
                    },
                    st,
                ));
            }
            if st.done {
                return None;
            }
            match st.body.body.next().await {
                Some(Ok(b)) => {
                    let mut rest = b;
                    while !rest.is_empty() {
                        let n = MPU_FORWARD_CHUNK.min(rest.len());
                        st.queue.push_back(rest.split_to(n));
                    }
                }
                Some(Err(e)) => {
                    st.done = true;
                    return Some((
                        xl_proto::MpuPartChunk {
                            abort: true,
                            error: e.to_string(),
                            ..Default::default()
                        },
                        st,
                    ));
                }
                None => {
                    st.done = true;
                    return Some((
                        xl_proto::MpuPartChunk {
                            is_final: true,
                            ..Default::default()
                        },
                        st,
                    ));
                }
            }
        }
    })
}

fn mpu_part_reject(code: &str, msg: String) -> xl_proto::MpuUploadPartResp {
    xl_proto::MpuUploadPartResp {
        ok: false,
        code: code.into(),
        msg,
        ..Default::default()
    }
}

fn mpu_op_reject(e: &Error) -> xl_proto::MpuOpResp {
    let (code, msg) = err_wire(e);
    xl_proto::MpuOpResp {
        ok: false,
        code: code.into(),
        msg,
        ..Default::default()
    }
}

/// 属主侧：把 gRPC 数据块流适配成 store 的 [`ByteStream`]。
///
/// **截断防护**：只有见到 `is_final=true` 后的流结束才算正常收尾；连接断开/发送端
/// 崩溃导致的提前 EOF 返回 `Err`，`upload_part_local` 据此删除 `part.N.new`，
/// 绝不会把半截数据改名为正式 part。`abort=true` 同样转成 `Err`。
fn forwarded_body_stream(
    stream: tonic::Streaming<xl_proto::MpuPartChunk>,
    pending: Option<bytes::Bytes>,
    final_seen: bool,
) -> ByteStream {
    struct St {
        stream: tonic::Streaming<xl_proto::MpuPartChunk>,
        pending: Option<bytes::Bytes>,
        final_seen: bool,
        done: bool,
    }
    let init = St {
        stream,
        pending,
        final_seen,
        done: false,
    };
    Box::pin(futures::stream::unfold(init, |mut st| async move {
        loop {
            if let Some(b) = st.pending.take()
                && !b.is_empty()
            {
                return Some((Ok(b), st));
            }
            if st.done {
                return None;
            }
            match st.stream.message().await {
                Ok(Some(m)) => {
                    if m.abort {
                        st.done = true;
                        return Some((
                            Err(Error::Encode(format!(
                                "对端中止 UploadPart 转发流：{}",
                                m.error
                            ))),
                            st,
                        ));
                    }
                    st.final_seen |= m.is_final;
                    if m.data.is_empty() {
                        continue;
                    }
                    st.pending = Some(bytes::Bytes::from(m.data));
                }
                Ok(None) => {
                    st.done = true;
                    if st.final_seen {
                        return None;
                    }
                    return Some((
                        Err(Error::Encode(
                            "MPU 转发流截断（缺 is_final）：拒收半截 part".into(),
                        )),
                        st,
                    ));
                }
                Err(e) => {
                    st.done = true;
                    return Some((Err(Error::Unavailable(format!("MPU 转发流中断：{e}"))), st));
                }
            }
        }
    }))
}

// ---------- 服务端 ----------

/// gRPC service 骨架：**只操作本节点本地槽**（扇出由协调者做；此处再扇出必循环）。
struct MeshSvc {
    store: Arc<Store>,
}

#[tonic::async_trait]
impl xl_proto::mesh_server::Mesh for MeshSvc {
    async fn health(
        &self,
        _req: Request<xl_proto::Empty>,
    ) -> std::result::Result<tonic::Response<xl_proto::NodeHealth>, Status> {
        let h = self.store.mesh_health().await;
        Ok(tonic::Response::new(xl_proto::NodeHealth {
            node_id: h.node_id,
            version: h.version,
            slots: h
                .slots
                .into_iter()
                .map(|(slot, up)| xl_proto::SlotState {
                    slot: slot as u32,
                    up,
                })
                .collect(),
            buckets: h.buckets,
            data_wire_version: h.data_wire_version,
            topo_epoch: h.topo_epoch,
            topo_hash: h.topo_hash,
            drives: h
                .drives
                .into_iter()
                .map(|d| xl_proto::DriveSpace {
                    set: d.set as u32,
                    drive: d.drive as u32,
                    free_bytes: d.free_bytes,
                })
                .collect(),
            maintenance: h.maintenance,
        }))
    }

    // 分片 / meta / 版本目录 / 回收（切片二）；其余 RPC 随所属 Task 落地（6/8/9）
    async fn write_shard(
        &self,
        r: Request<xl_proto::ShardWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        let w = r.into_inner();
        self.store
            .mesh_write_shard(mesh::ShardWrite {
                bucket: w.bucket,
                key: w.key,
                uuid: w.uuid,
                part: w.part,
                shard: w.shard,
                crc32c: w.crc32c,
                data: w.data.into(),
                chunked: w.chunked,
                offset: w.offset,
                chunk_crc32c: w.chunk_crc32c,
                is_final: w.is_final,
                slot: w.slot,
                set: w.set,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn read_shard(
        &self,
        r: Request<xl_proto::ShardRead>,
    ) -> std::result::Result<tonic::Response<xl_proto::ShardResp>, Status> {
        let s = r.into_inner();
        let data = self
            .store
            .mesh_read_shard(mesh::ShardRead {
                bucket: s.bucket,
                key: s.key,
                uuid: s.uuid,
                part: s.part,
                shard: s.shard,
                offset: s.offset,
                length: s.length,
                slot: s.slot,
                set: s.set,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(match data {
            Some(d) => xl_proto::ShardResp {
                found: true,
                crc32c: ec::crc32c_of(&d),
                data: d.to_vec(),
            },
            None => xl_proto::ShardResp::default(),
        }))
    }
    async fn delete_shard(
        &self,
        r: Request<xl_proto::ShardDelete>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        let d = r.into_inner();
        self.store
            .mesh_delete_shard(mesh::ShardDelete {
                bucket: d.bucket,
                key: d.key,
                uuid: d.uuid,
                part: d.part,
                shard: d.shard,
                slot: d.slot,
                set: d.set,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn delete_version(
        &self,
        r: Request<xl_proto::VersionDirRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_delete_version(dir_ref_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn recycle_version(
        &self,
        r: Request<xl_proto::VersionDirRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_recycle_version(dir_ref_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn prepare_meta(
        &self,
        r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaAck>, Status> {
        let ack = self
            .store
            .mesh_prepare_meta(meta_write_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(meta_ack_to_proto(ack)))
    }
    async fn commit_meta(
        &self,
        r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaAck>, Status> {
        let ack = self
            .store
            .mesh_commit_meta(meta_write_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(meta_ack_to_proto(ack)))
    }
    async fn abort_meta(
        &self,
        r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_abort_meta(meta_write_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn read_meta(
        &self,
        r: Request<xl_proto::MetaRead>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaResp>, Status> {
        let snap = self
            .store
            .mesh_read_meta(meta_read_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(xl_proto::MetaResp {
            found: snap.meta.is_some(),
            meta: snap.meta.map(|b| b.to_vec()).unwrap_or_default(),
            mod_time_nanos: snap.mod_time_nanos,
            slots_ok: snap.slots_ok,
        }))
    }
    async fn delete_meta(
        &self,
        r: Request<xl_proto::MetaRead>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_delete_meta(meta_read_from_proto(r.into_inner()))
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn delete_bucket(
        &self,
        r: Request<xl_proto::BucketRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_delete_bucket(&r.into_inner().bucket)
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn ensure_bucket(
        &self,
        r: Request<xl_proto::BucketRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        self.store
            .mesh_ensure_bucket(&r.into_inner().bucket)
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }
    async fn list_buckets(
        &self,
        _r: Request<xl_proto::Empty>,
    ) -> std::result::Result<tonic::Response<xl_proto::BucketsResp>, Status> {
        let names = self.store.mesh_list_buckets().await.map_err(internal)?;
        Ok(tonic::Response::new(xl_proto::BucketsResp { names }))
    }
    async fn list_local(
        &self,
        r: Request<xl_proto::ListReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::ListLocalResp>, Status> {
        let l = r.into_inner();
        let resp = self
            .store
            .mesh_list_local(mesh::ListLocal {
                bucket: l.bucket,
                prefix: l.prefix,
                // 线上空串 ↔ None（客户端同款映射）
                delimiter: (!l.delimiter.is_empty()).then_some(l.delimiter),
                marker: (!l.marker.is_empty()).then_some(l.marker),
                marker_exclusive: l.marker_exclusive,
                limit: l.limit as usize,
                include_deleted: l.include_deleted,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(xl_proto::ListLocalResp {
            entries: resp
                .entries
                .into_iter()
                .map(|e| xl_proto::ListEntry {
                    key: e.key,
                    size: e.size,
                    etag: e.etag,
                    max_mod_time_nanos: e.max_mod_time_nanos,
                })
                .collect(),
            prefixes: resp.prefixes,
            slots_ok: resp.slots_ok,
        }))
    }
    async fn list_uploads_local(
        &self,
        r: Request<xl_proto::UploadListReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::UploadListLocalResp>, Status> {
        let l = r.into_inner();
        let resp = self
            .store
            .mesh_list_uploads_local(mesh::ListUploadsLocal {
                bucket: l.bucket,
                prefix: l.prefix,
                // 线上空串 ↔ None（客户端同款映射）
                delimiter: (!l.delimiter.is_empty()).then_some(l.delimiter),
                key_marker: (!l.key_marker.is_empty()).then_some(l.key_marker),
                upload_id_marker: (!l.upload_id_marker.is_empty()).then_some(l.upload_id_marker),
                limit: l.limit as usize,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(xl_proto::UploadListLocalResp {
            uploads: resp
                .uploads
                .into_iter()
                .map(|u| xl_proto::UploadEntry {
                    key: u.key,
                    upload_id: u.upload_id,
                    initiated_nanos: u.initiated_nanos,
                    storage_class: u.storage_class,
                })
                .collect(),
            prefixes: resp.prefixes,
            slots_ok: resp.slots_ok,
        }))
    }

    // ---------- M3 MPU 会话粘性：属主侧本地执行 ----------
    //
    // 这些 handler 调 `mesh_*`（= `*_local`，不再转发）；若在此再走 Store 的
    // 转发入口会造成属主→属主回环。首块携带目标元数据，其余块为纯数据；
    // 数据流必须以 `is_final` 收尾，否则适配器报错 → 属主拒收半截 part。

    async fn upload_part(
        &self,
        r: Request<tonic::Streaming<xl_proto::MpuPartChunk>>,
    ) -> std::result::Result<tonic::Response<xl_proto::MpuUploadPartResp>, Status> {
        let mut stream = r.into_inner();
        let first = match stream.message().await {
            Ok(Some(m)) => m,
            Ok(None) => {
                return Ok(tonic::Response::new(mpu_part_reject(
                    "InvalidArgument",
                    "MPU 转发流为空".into(),
                )));
            }
            Err(e) => return Err(Status::unavailable(format!("MPU 转发流读取失败：{e}"))),
        };
        if first.abort {
            return Ok(tonic::Response::new(mpu_part_reject(
                "Encode",
                format!("对端中止 UploadPart：{}", first.error),
            )));
        }
        if first.bucket.is_empty() || first.upload_id.is_empty() {
            return Ok(tonic::Response::new(mpu_part_reject(
                "InvalidArgument",
                "MPU 转发首块缺少 bucket/upload_id".into(),
            )));
        }
        let Ok(part_number) = u16::try_from(first.part_number) else {
            return Ok(tonic::Response::new(mpu_part_reject(
                "InvalidPartRange",
                format!("part_number 越界：{}", first.part_number),
            )));
        };
        if part_number == 0 || part_number > 10000 {
            return Ok(tonic::Response::new(mpu_part_reject(
                "InvalidPartRange",
                format!("part_number 越界：{part_number}"),
            )));
        }
        let body = forwarded_body_stream(
            stream,
            (!first.data.is_empty()).then(|| bytes::Bytes::from(first.data)),
            first.is_final,
        );
        let req = mesh::MpuPartUpload {
            bucket: first.bucket,
            key: first.key,
            upload_id: first.upload_id,
            part_number,
            sse_c_md5: (!first.sse_c_md5.is_empty()).then_some(first.sse_c_md5),
            body,
        };
        match self.store.mesh_mpu_upload_part(req).await {
            Ok(etag) => Ok(tonic::Response::new(xl_proto::MpuUploadPartResp {
                ok: true,
                etag,
                ..Default::default()
            })),
            Err(e) => {
                let (code, msg) = err_wire(&e);
                Ok(tonic::Response::new(mpu_part_reject(code, msg)))
            }
        }
    }

    async fn complete_upload(
        &self,
        r: Request<xl_proto::MpuCompleteReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::MpuOpResp>, Status> {
        let c = r.into_inner();
        let Some(target) = c.target else {
            return Ok(tonic::Response::new(mpu_op_reject(
                &Error::InvalidArgument("缺少 target".into()),
            )));
        };
        if target.bucket.is_empty() || target.upload_id.is_empty() {
            return Ok(tonic::Response::new(mpu_op_reject(
                &Error::InvalidArgument("target 缺少 bucket/upload_id".into()),
            )));
        }
        let mode = match c.versioning_mode {
            0 => VersioningMode::Off,
            1 => VersioningMode::Enabled,
            2 => VersioningMode::Suspended,
            other => {
                return Ok(tonic::Response::new(mpu_op_reject(
                    &Error::InvalidArgument(format!("未知 versioning_mode：{other}")),
                )));
            }
        };
        let encryption = match c.enc_kind {
            0 => None,
            1 => Some(mesh::MpuEnc::SseS3),
            2 => Some(mesh::MpuEnc::SseKms),
            3 if !c.enc_cmk.is_empty() => Some(mesh::MpuEnc::SseKmsCmk(c.enc_cmk)),
            3 => {
                return Ok(tonic::Response::new(mpu_op_reject(
                    &Error::InvalidArgument("SSE-KMS 缺 cmk".into()),
                )));
            }
            4 => match <[u8; 32]>::try_from(c.sse_c_key.as_slice()) {
                Ok(key) => Some(mesh::MpuEnc::SseC(key)),
                Err(_) => {
                    return Ok(tonic::Response::new(mpu_op_reject(
                        &Error::InvalidArgument("SSE-C 密钥长度必须为 32 字节".into()),
                    )));
                }
            },
            other => {
                return Ok(tonic::Response::new(mpu_op_reject(
                    &Error::InvalidArgument(format!("未知 enc_kind：{other}")),
                )));
            }
        };
        let lock = c.has_lock.then(|| ObjectLockRecord {
            mode: (!c.lock_mode.is_empty()).then_some(c.lock_mode.clone()),
            retain_until_nanos: (c.lock_until_nanos != 0).then_some(c.lock_until_nanos),
            legal_hold: c.lock_legal_hold,
        });
        let mut parts = Vec::with_capacity(c.parts.len());
        for p in c.parts {
            let Ok(n) = u16::try_from(p.number) else {
                return Ok(tonic::Response::new(mpu_op_reject(
                    &Error::InvalidPartRange(format!("part number 越界：{}", p.number)),
                )));
            };
            if n == 0 || n > 10000 {
                return Ok(tonic::Response::new(mpu_op_reject(
                    &Error::InvalidPartRange(format!("part number 越界：{n}")),
                )));
            }
            if p.etag.is_empty() {
                return Ok(tonic::Response::new(mpu_op_reject(&Error::InvalidPart(
                    format!("part {n} 缺 etag"),
                ))));
            }
            parts.push((n, p.etag));
        }
        if parts.is_empty() {
            return Ok(tonic::Response::new(mpu_op_reject(
                &Error::InvalidArgument("parts 为空".into()),
            )));
        }
        let req = mesh::MpuComplete {
            bucket: target.bucket,
            key: target.key,
            upload_id: target.upload_id,
            parts,
            mode,
            encryption,
            lock,
        };
        match self.store.mesh_mpu_complete(req).await {
            Ok(info) => match mesh::encode_object_info(&info) {
                Ok(info) => Ok(tonic::Response::new(xl_proto::MpuOpResp {
                    ok: true,
                    info,
                    ..Default::default()
                })),
                Err(e) => Ok(tonic::Response::new(mpu_op_reject(&e))),
            },
            Err(e) => Ok(tonic::Response::new(mpu_op_reject(&e))),
        }
    }

    async fn abort_upload(
        &self,
        r: Request<xl_proto::MpuRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::MpuOpResp>, Status> {
        let a = r.into_inner();
        if a.bucket.is_empty() || a.upload_id.is_empty() {
            return Ok(tonic::Response::new(mpu_op_reject(
                &Error::InvalidArgument("缺少 bucket/upload_id".into()),
            )));
        }
        match self.store.mesh_mpu_abort(mpu_ref_from_proto(a)).await {
            Ok(()) => Ok(tonic::Response::new(xl_proto::MpuOpResp {
                ok: true,
                ..Default::default()
            })),
            Err(e) => Ok(tonic::Response::new(mpu_op_reject(&e))),
        }
    }

    async fn list_parts(
        &self,
        r: Request<xl_proto::MpuRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::MpuOpResp>, Status> {
        let a = r.into_inner();
        if a.bucket.is_empty() || a.upload_id.is_empty() {
            return Ok(tonic::Response::new(mpu_op_reject(
                &Error::InvalidArgument("缺少 bucket/upload_id".into()),
            )));
        }
        match self.store.mesh_mpu_list_parts(mpu_ref_from_proto(a)).await {
            Ok(rows) => Ok(tonic::Response::new(xl_proto::MpuOpResp {
                ok: true,
                parts: rows
                    .into_iter()
                    .map(|p| xl_proto::MpuPartRow {
                        number: p.part_number as u32,
                        size: p.size,
                        etag: p.etag,
                    })
                    .collect(),
                ..Default::default()
            })),
            Err(e) => Ok(tonic::Response::new(mpu_op_reject(&e))),
        }
    }

    async fn lock(
        &self,
        r: Request<xl_proto::LockReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::LockResp>, Status> {
        let l = r.into_inner();
        let op = match xl_proto::LockOp::try_from(l.op) {
            Ok(xl_proto::LockOp::Acquire) => mesh::LockOp::Acquire,
            Ok(xl_proto::LockOp::Renew) => mesh::LockOp::Renew,
            Ok(xl_proto::LockOp::Release) => mesh::LockOp::Release,
            _ => return Err(Status::invalid_argument("未知锁操作")),
        };
        let resp = self
            .store
            .mesh_lock(mesh::LockReq {
                bucket: l.bucket,
                key: l.key,
                epoch_nanos: l.epoch_nanos,
                epoch_node: l.epoch_node,
                holder: l.holder,
                ttl_ms: l.ttl_ms,
                op,
            })
            .await
            .map_err(internal)?;
        Ok(tonic::Response::new(xl_proto::LockResp {
            granted: resp.granted,
        }))
    }
    async fn control_write(
        &self,
        r: Request<xl_proto::ControlWriteReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        let l = r.into_inner();
        if l.kind != 0 {
            return Err(Status::invalid_argument("未知控制面写类型"));
        }
        // fail-closed：u32 越界不得静默截断成合法 set（与存储侧 try_from 口径一致）
        let set = if l.has_set {
            Some(
                u16::try_from(l.set)
                    .map_err(|_| Status::invalid_argument("set 越界（>= 65536）"))?,
            )
        } else {
            None
        };
        let w = mesh::ControlWrite::BucketRowSet { bucket: l.a, set };
        self.store.mesh_control_write(w).await.map_err(internal)?;
        Ok(tonic::Response::new(write_ok()))
    }

    async fn control_query(
        &self,
        r: Request<xl_proto::ControlReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::ControlResp>, Status> {
        let l = r.into_inner();
        let kind = xl_proto::ControlKind::try_from(l.kind)
            .map_err(|_| Status::invalid_argument("未知控制面查询类型"))?;
        let q = match kind {
            xl_proto::ControlKind::AccessKey => mesh::ControlQuery::AccessKey(l.a),
            xl_proto::ControlKind::Permission => mesh::ControlQuery::Permission {
                uid: l.uid,
                bucket: l.a,
                action: l.c as u8,
            },
            xl_proto::ControlKind::OwnedBuckets => mesh::ControlQuery::OwnedBuckets(l.uid),
            xl_proto::ControlKind::VisibleBuckets => mesh::ControlQuery::VisibleBuckets(l.uid),
            xl_proto::ControlKind::BucketOwner => mesh::ControlQuery::BucketOwner(l.a),
            xl_proto::ControlKind::BucketSet => mesh::ControlQuery::BucketSet(l.a),
        };
        let row = self.store.mesh_control_query(q).await.map_err(internal)?;
        Ok(tonic::Response::new(match row {
            Some(r) => xl_proto::ControlResp {
                found: true,
                uid: r.uid.unwrap_or(0),
                is_admin: r.is_admin.unwrap_or(false),
                active: r.active.unwrap_or(false),
                secret: r.secret.unwrap_or_default(),
                flag: r.flag.unwrap_or(false),
                names: r.names.unwrap_or_default(),
                set: r.set.unwrap_or(0),
                session_policy: r.session_policy.unwrap_or_default(),
            },
            None => xl_proto::ControlResp {
                found: false,
                ..Default::default()
            },
        }))
    }
}

/// 起节点间 gRPC 服务（oss-server 在 cluster 配置时调用；HTTP 端口与 gRPC 端口分离）。
pub async fn serve(store: Arc<Store>, addr: SocketAddr, secret: String) -> Result<()> {
    serve_tls(store, addr, secret, None).await
}

/// 同 [`serve`]；`tls = Some` 时 gRPC 服务端启用 TLS（可要求 mTLS 客户端证书）。
pub async fn serve_tls(
    store: Arc<Store>,
    addr: SocketAddr,
    secret: String,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> Result<()> {
    let svc = MeshSvc { store };
    let server =
        xl_proto::mesh_server::MeshServer::with_interceptor(svc, SecretInterceptor { secret });
    let mut builder = tonic::transport::Server::builder();
    if let Some(t) = tls {
        builder = builder
            .tls_config(t)
            .map_err(|e| Error::Encode(format!("mesh gRPC 服务端 TLS 配置失败：{e}")))?;
    }
    builder
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| Error::Encode(format!("mesh gRPC server 退出（{addr}）：{e}")))
}

/// mesh 入站 TLS 热加载信号：值为当前 rustls 服务端配置（`None` = 未启用 peer TLS）。
pub type ServerTlsWatch = watch::Receiver<Option<Arc<rustls::ServerConfig>>>;

/// mesh gRPC 握手超时（与主 HTTP 监听器同口径）。
const MESH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// mesh gRPC 并发握手上限（与主 HTTP 监听器同口径）：耗尽时断开新连接（fail closed）。
const MESH_MAX_CONCURRENT_HANDSHAKES: usize = 1024;

/// 同 [`serve`]，但每条新连接从 `tls` 取当前快照做 rustls 握手（ALPN h2）后交给
/// tonic；成功 reload 后新连接即用新证书/客户端 CA，旧连接保持旧物料。
/// `tls = None` 与 [`serve`] 完全一致（明文）。
pub async fn serve_tls_watch(
    store: Arc<Store>,
    addr: SocketAddr,
    secret: String,
    tls: Option<ServerTlsWatch>,
) -> Result<()> {
    let Some(cfg) = tls else {
        return serve(store, addr, secret).await;
    };
    let svc = MeshSvc { store };
    let server =
        xl_proto::mesh_server::MeshServer::with_interceptor(svc, SecretInterceptor { secret });
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Encode(format!("mesh gRPC 监听 {addr} 失败：{e}")))?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let accept =
        tokio::spawn(async move {
            // 每监听器独立限额；许可随握手任务结束（成功/失败/超时）释放。
            let semaphore = Arc::new(tokio::sync::Semaphore::new(MESH_MAX_CONCURRENT_HANDSHAKES));
            let mut dropped: u64 = 0;
            loop {
                let (tcp, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error = %e, "mesh TCP accept 失败");
                        continue;
                    }
                };
                // 无可用许可直接断开：不排队、不堆积握手任务（fail closed）。
                let Some(permit) = semaphore.clone().try_acquire_owned().ok() else {
                    dropped += 1;
                    tracing::debug!(dropped, "mesh TLS 握手并发已达上限，断开新连接");
                    continue;
                };
                // 每连接取一次快照：reload 失败时 Tls 保留旧快照；取不到（未配置
                // [tls.server]）时 fail closed，绝不回退明文。
                let Some(config) = cfg.borrow().clone() else {
                    tracing::error!("mesh TLS 配置不可用，断开新连接");
                    continue;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let acceptor = tokio_rustls::TlsAcceptor::from(config);
                    let result =
                        match tokio::time::timeout(MESH_HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
                            .await
                        {
                            Ok(result) => result,
                            Err(_) => Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "mesh TLS handshake timed out",
                            )),
                        };
                    let _ = tx.send(result).await;
                });
            }
        });
    // 只把握手成功的流交给 tonic：tonic 对部分 io::ErrorKind 会终止整个 accept
    // 循环（含 TimedOut），坏握手不得拖垮 mesh 服务端。
    let incoming = futures::stream::poll_fn(move |cx| {
        loop {
            match rx.poll_recv(cx) {
                std::task::Poll::Ready(Some(Ok(io))) => {
                    return std::task::Poll::Ready(Some(Ok::<_, std::io::Error>(io)));
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    tracing::debug!(error = %e, "mesh TLS 握手失败");
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    });
    let res = tonic::transport::Server::builder()
        .add_service(server)
        .serve_with_incoming(incoming)
        .await;
    accept.abort();
    res.map_err(|e| Error::Encode(format!("mesh gRPC server 退出（{addr}）：{e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xl_store::store::ec::EcConfig;
    use xl_store::store::{ClusterConfig, ClusterSets, NodeSpec, StoreConfig};

    async fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    }

    /// Health 往返 + 令牌校验（错误令牌 → Unauthenticated）。
    #[tokio::test]
    async fn health_roundtrip_and_secret_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        let addr = free_addr().await;
        let s2 = store.clone();
        tokio::spawn(async move {
            let _ = serve(s2, addr, "s3cret".into()).await;
        });

        let nodes = vec![("n1".to_string(), addr.to_string())];
        let mesh = TonicMesh::new("n1", "s3cret", &nodes).unwrap();

        // server 启动是异步的：重试若干次直到通
        let mut last: Option<Error> = None;
        let mut h = None;
        for _ in 0..40 {
            match mesh.health("n1").await {
                Ok(v) => {
                    h = Some(v);
                    break;
                }
                Err(e) => {
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        let h = h.unwrap_or_else(|| panic!("server 未就绪：{last:?}"));
        assert_eq!(h.node_id, "single"); // 单节点部署的退化 id
        assert_eq!(h.slots, vec![(0, true)]);
        assert_eq!(h.version, env!("CARGO_PKG_VERSION"));
        // DP-0 握手字段随 Health 往返，且与本版常量一致。
        assert_eq!(h.data_wire_version, xl_store::mesh_gate::DATA_WIRE_VERSION);
        // 拓扑指纹：单节点部署 = epoch 1 + 空 hash（字段 6/7 往返）。
        assert_eq!(h.topo_epoch, 1);
        assert!(h.topo_hash.is_empty());

        // 错误令牌 → Unauthenticated
        let bad = TonicMesh::new("n1", "wrong", &nodes).unwrap();
        let e = bad.health("n1").await.unwrap_err().to_string();
        assert!(e.contains("Unauthenticated"), "实得：{e}");
    }

    /// M9 S4：stretch 2×6——A 经真实 TonicMesh 写 B 的 6 个远端槽（B 是规范序第二节点），
    /// 6/6 成功且每盘恰 1 片；服务端槽归属校验不得误拒。
    #[tokio::test]
    async fn stretch_peer_six_remote_slots() {
        let a_tmp: Vec<tempfile::TempDir> =
            (0..6).map(|_| tempfile::TempDir::new().unwrap()).collect();
        let b_tmp: Vec<tempfile::TempDir> =
            (0..6).map(|_| tempfile::TempDir::new().unwrap()).collect();
        let a_dirs: Vec<std::path::PathBuf> =
            a_tmp.iter().map(|d| d.path().to_path_buf()).collect();
        let b_dirs: Vec<std::path::PathBuf> =
            b_tmp.iter().map(|d| d.path().to_path_buf()).collect();
        let (a_addr, b_addr) = (free_addr().await, free_addr().await);
        let ec = EcConfig {
            data: 8,
            parity: 4,
            enabled: true,
        };
        // 故意反序：两端 `new_stretch` 都必须规范为 [A,B]
        let nodes = vec![
            NodeSpec {
                id: "B".into(),
                addr: b_addr.to_string(),
                drive_count: 6,
            },
            NodeSpec {
                id: "A".into(),
                addr: a_addr.to_string(),
                drive_count: 6,
            },
        ];
        let secret = "mesh-secret".to_string();

        // B：服务端（规范序 self_idx=1）
        let b_sets = ClusterSets::new_stretch("B", nodes.clone(), &ec).unwrap();
        let b_pairs: Vec<(String, String)> = b_sets
            .flat
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.addr.clone()))
            .collect();
        let b_mesh = Arc::new(TonicMesh::new("B", &secret, &b_pairs).unwrap());
        let mut b_cfg = StoreConfig::new(&b_dirs[0]);
        b_cfg.drives = b_dirs.clone();
        b_cfg.ec = ec;
        b_cfg.inline_threshold = 0;
        b_cfg.cluster = Some(Arc::new(ClusterConfig {
            sets: b_sets,
            transport: b_mesh,
            placement_v2: true,
            resolver: None,
            stretch: true,
        }));
        let b_store = Arc::new(Store::open(b_cfg).await.unwrap());
        let b2 = Arc::clone(&b_store);
        let b_secret = secret.clone();
        tokio::spawn(async move {
            let _ = serve(b2, b_addr, b_secret).await;
        });

        // A：客户端
        let a_sets = ClusterSets::new_stretch("A", nodes, &ec).unwrap();
        let a_pairs: Vec<(String, String)> = a_sets
            .flat
            .nodes
            .iter()
            .map(|n| (n.id.clone(), n.addr.clone()))
            .collect();
        let mesh = Arc::new(TonicMesh::new("A", &secret, &a_pairs).unwrap());
        let mut a_cfg = StoreConfig::new(&a_dirs[0]);
        a_cfg.drives = a_dirs.clone();
        a_cfg.ec = ec;
        a_cfg.inline_threshold = 0;
        a_cfg.cluster = Some(Arc::new(ClusterConfig {
            sets: a_sets,
            transport: mesh.clone(),
            placement_v2: true,
            resolver: None,
            stretch: true,
        }));
        let a_store = Arc::new(Store::open(a_cfg).await.unwrap());

        let mut ready = false;
        for _ in 0..40 {
            if mesh.health("B").await.is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(ready, "B mesh server 未就绪");

        let topo = a_store.cluster().unwrap().sets.set_topo(0).unwrap().clone();
        let remote: Vec<u16> = (0..topo.slot_count())
            .filter(|s| topo.local_drive_of_slot(*s).is_none())
            .collect();
        assert_eq!(
            remote,
            (6..12).collect::<Vec<u16>>(),
            "B 的槽须为规范序第二段"
        );

        let data = bytes::Bytes::from_static(b"stretch-shard-payload");
        let crc = xl_store::store::ec::crc32c_of(&data);
        for slot in &remote {
            mesh.write_shard(
                "B",
                xl_store::mesh::ShardWrite {
                    bucket: "bkt".into(),
                    key: "k".into(),
                    uuid: "u1".into(),
                    part: 1,
                    shard: *slot as u32,
                    crc32c: crc,
                    data: data.clone(),
                    chunked: false,
                    offset: 0,
                    chunk_crc32c: 0,
                    is_final: false,
                    slot: *slot as u32,
                    set: 0,
                },
            )
            .await
            .unwrap();
        }
        // 6/6 落盘：B 的每个盘恰 1 片；A 本地 0 片（全写远端）
        for slot in &remote {
            let p = xl_store::layout::object_dir(&b_dirs[(*slot - 6) as usize], "bkt", "k")
                .unwrap()
                .join("u1")
                .join(format!("part.1.{slot}"));
            assert_eq!(
                std::fs::read(&p).unwrap(),
                &data[..],
                "B 盘 {slot} 内容不符"
            );
        }
        let a_files = a_dirs
            .iter()
            .filter_map(|d| std::fs::read_dir(d.join("bkt")).ok())
            .count();
        assert_eq!(a_files, 0, "A 不应有本地分片目录");
    }

    /// 未知节点名显式报错（不 panic、不静默）。
    #[tokio::test]
    async fn unknown_node_is_explicit_error() {
        let nodes = vec![("n1".to_string(), "127.0.0.1:1".to_string())];
        let mesh = TonicMesh::new("n1", "s", &nodes).unwrap();
        let e = mesh.health("n9").await.unwrap_err().to_string();
        assert!(e.contains("未知节点 n9"), "实得：{e}");
    }

    /// 切片二 RPC 往返：proto ↔ DTO 映射逐字段实证（分片 / meta / 版本目录 / 删桶）。
    #[tokio::test]
    async fn data_and_meta_rpcs_roundtrip() {
        // mesh 面要求 cluster 配置（非集群模式无入口）；1 节点 × 2 盘、ec 1+1 →
        // q_s = data+1 = 2 全由本机覆盖，写/读都不触发远端扇出（本测无 peer）。
        let tmp: Vec<tempfile::TempDir> =
            (0..2).map(|_| tempfile::TempDir::new().unwrap()).collect();
        let dirs: Vec<std::path::PathBuf> = tmp.iter().map(|d| d.path().to_path_buf()).collect();
        let addr = free_addr().await;
        let nodes = vec![("n1".to_string(), addr.to_string())];
        // 同一个 TonicMesh 兼作 cluster transport 与测试客户端（1 节点拓扑下不会真连）。
        let mesh = Arc::new(TonicMesh::new("n1", "tok", &nodes).unwrap());

        let ec = ec::EcConfig {
            data: 1,
            parity: 1,
            enabled: true,
        };
        let topo_nodes = vec![NodeSpec {
            id: "n1".into(),
            addr: addr.to_string(),
            drive_count: 2,
        }];
        let sets = ClusterSets::single("n1", topo_nodes, &ec).unwrap();
        let mut cfg = StoreConfig::new(&dirs[0]);
        cfg.drives = dirs;
        cfg.ec = ec;
        cfg.fsync = false; // 测试提速
        cfg.cluster = Some(Arc::new(ClusterConfig {
            placement_v2: false,
            sets,
            transport: mesh.clone(),
            resolver: None,
            stretch: false,
        }));
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let s2 = store.clone();
        tokio::spawn(async move {
            let _ = serve(s2, addr, "tok".into()).await;
        });

        // server 启动异步：重试直到 health 通
        let mut up = false;
        for _ in 0..40 {
            if mesh.health("n1").await.is_ok() {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(up, "server 未就绪");

        // 真对象（meta 读回要求真实编码的 XlMeta——集群读路径会解码）
        store.create_bucket("buck").await.unwrap();
        store
            .put_object("buck", "k", bytes::Bytes::from_static(b"hello"), None)
            .await
            .unwrap();
        let mr = mesh::MetaRead {
            bucket: "buck".into(),
            key: "k".into(),
        };
        let snap = mesh.read_meta("n1", mr.clone()).await.unwrap();
        let meta = snap.meta.expect("已提交对象必须读回 meta");
        assert_eq!(snap.slots_ok, 2); // 覆盖率 = 在册本地盘数（1 节点 × 2 盘）

        // prepare（逐槽 hash）→ commit（内容不变，幂等重演）
        let mw = mesh::MetaWrite {
            bucket: "buck".into(),
            key: "k".into(),
            meta: meta.clone(),
        };
        let ack = mesh.prepare_meta("n1", mw.clone()).await.unwrap();
        assert!(ack.ok && ack.slots_ok == 2, "{ack:?}");
        assert_eq!(ack.slots[0].slot, 0);
        assert_eq!(ack.slots[0].hash, mesh::content_hash(&meta));
        assert!(mesh.commit_meta("n1", mw.clone()).await.unwrap().ok);

        // 未命中恒返回快照（meta=None），不报错
        let miss = mesh
            .read_meta(
                "n1",
                mesh::MetaRead {
                    bucket: "buck".into(),
                    key: "ghost".into(),
                },
            )
            .await
            .unwrap();
        assert!(miss.meta.is_none());

        // 分片：写 → 读回 → 换 part 即缺失（part/shard 映射真的落在查找路径上）
        let data = bytes::Bytes::from_static(b"hello shard");
        let sw = |part| mesh::ShardWrite {
            set: 0,
            bucket: "buck".into(),
            key: "k2".into(),
            uuid: "u1".into(),
            part,
            shard: 0,
            crc32c: ec::crc32c_of(&data),
            data: data.clone(),
            chunked: false,
            offset: 0,
            chunk_crc32c: 0,
            is_final: false,
            slot: 0,
        };
        mesh.write_shard("n1", sw(3)).await.unwrap();
        let sr = |part| mesh::ShardRead {
            set: 0,
            bucket: "buck".into(),
            key: "k2".into(),
            uuid: "u1".into(),
            part,
            shard: 0,
            offset: 0,
            length: 0,
            slot: 0,
        };
        assert_eq!(
            mesh.read_shard("n1", sr(3)).await.unwrap().as_deref(),
            Some(&data[..])
        );
        assert!(mesh.read_shard("n1", sr(4)).await.unwrap().is_none());

        // CRC 不符 → 对端拒收（5xx 上抛，不静默成功）
        let mut bad = sw(3);
        bad.crc32c ^= 1;
        assert!(mesh.write_shard("n1", bad).await.is_err());

        // 版本目录：回收 → 删除（均幂等：不存在也成功）
        let dirref = mesh::VersionDirRef {
            bucket: "buck".into(),
            key: "k".into(),
            uuid: "ghost".into(),
        };
        mesh.recycle_version("n1", dirref.clone()).await.unwrap();
        mesh.delete_version("n1", dirref).await.unwrap();

        // 删 meta（可见性提交点）→ 再读即缺席
        mesh.abort_meta("n1", mw).await.unwrap();
        mesh.delete_meta("n1", mr.clone()).await.unwrap();
        assert!(mesh.read_meta("n1", mr).await.unwrap().meta.is_none());

        // 删桶：对象目录仍在 → BucketNotEmpty 经 5xx 上抛（对端消息可达，不静默成功）
        let e = mesh
            .delete_bucket_dir("n1", "buck")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("bucket not empty"), "实得：{e}");
    }

    /// Task 10：拦截器注入令牌 + traceparent；服务端拦截器拒无令牌请求。
    #[test]
    fn interceptors_inject_token_and_traceparent() {
        use tonic::service::Interceptor as _;
        let mut ti = TokenInterceptor::new("s").unwrap();
        let req = ti.call(Request::new(())).unwrap();
        assert!(req.metadata().get(TOKEN_KEY).is_some());
        let tp = req
            .metadata()
            .get(TRACEPARENT_KEY)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(tp.starts_with("00-") && tp.ends_with("-01"), "{tp}");
        assert_eq!(tp.len(), 3 + 32 + 1 + 16 + 3, "W3C 形态：{tp}");
        // 服务端：无令牌 → 拒
        let mut si = SecretInterceptor {
            secret: "secret".into(),
        };
        assert!(si.call(Request::new(())).is_err());
        // L-2：正确令牌放行；错误/前缀/同长度异值一律拒（常量时间比较）。
        let with_token = |v: &'static str| {
            let mut r = Request::new(());
            r.metadata_mut().insert(TOKEN_KEY, v.parse().unwrap());
            r
        };
        assert!(si.call(with_token("secret")).is_ok());
        assert!(si.call(with_token("secre")).is_err(), "前缀不得放行");
        assert!(si.call(with_token("secreu")).is_err(), "同长度异值不得放行");
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "ab"));
    }

    /// LMU 远端页的真实 gRPC 往返：None ↔ 空串、prefix/delimiter/key_marker、
    /// 字段（upload_id/initiated/storage_class）不丢。
    #[tokio::test]
    async fn list_uploads_local_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        store.create_bucket("alpha").await.unwrap();
        let id_a = store
            .create_multipart_upload("alpha", "a/1", None, Default::default())
            .await
            .unwrap();
        let id_b = store
            .create_multipart_upload("alpha", "b", None, Default::default())
            .await
            .unwrap();

        let addr = free_addr().await;
        let s2 = store.clone();
        tokio::spawn(async move {
            let _ = serve(s2, addr, "s3cret".into()).await;
        });
        let nodes = vec![("n1".to_string(), addr.to_string())];
        let mesh = TonicMesh::new("n1", "s3cret", &nodes).unwrap();

        // server 启动是异步的：重试若干次直到通
        let mut ready = false;
        for _ in 0..40 {
            if mesh.health("n1").await.is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(ready, "server 未就绪");

        let req = |prefix: &str, delimiter: Option<&str>, key_marker: Option<&str>| {
            mesh::ListUploadsLocal {
                bucket: "alpha".into(),
                prefix: prefix.into(),
                delimiter: delimiter.map(str::to_string),
                key_marker: key_marker.map(str::to_string),
                upload_id_marker: None,
                limit: 11,
            }
        };
        let r = mesh
            .list_uploads_local("n1", req("", None, None))
            .await
            .unwrap();
        assert_eq!(
            r.uploads
                .iter()
                .map(|u| (u.key.as_str(), u.upload_id.as_str()))
                .collect::<Vec<_>>(),
            [("a/1", id_a.as_str()), ("b", id_b.as_str())]
        );
        assert!(r.uploads.iter().all(|u| u.initiated_nanos > 0));
        // 线上空串 ↔ None：未声明存储类
        assert!(r.uploads.iter().all(|u| u.storage_class.is_empty()));
        assert!(r.prefixes.is_empty());

        // delimiter 折叠 + prefix
        let r = mesh
            .list_uploads_local("n1", req("", Some("/"), None))
            .await
            .unwrap();
        assert_eq!(r.prefixes, ["a/"]);
        assert_eq!(r.uploads.len(), 1);
        assert_eq!(r.uploads[0].key, "b");

        let r = mesh
            .list_uploads_local("n1", req("a/", None, None))
            .await
            .unwrap();
        assert_eq!(r.uploads.len(), 1);
        assert_eq!(r.uploads[0].key, "a/1");

        // key_marker 排他
        let r = mesh
            .list_uploads_local("n1", req("", None, Some("b")))
            .await
            .unwrap();
        assert!(r.uploads.is_empty() && r.prefixes.is_empty());
    }

    /// M3 MPU：真实 tonic client-streaming 往返。覆盖 >1 MiB 自动分块、Complete
    /// 的 msgpack ObjectInfo 回传、缺 `is_final` 的截断流拒收（不留半截 part）、
    /// 语义错误信封（NoSuchUpload）与属主不可达 → Unavailable。
    #[tokio::test]
    async fn mpu_forward_rpcs_streaming_roundtrip() {
        let tmp: Vec<tempfile::TempDir> =
            (0..2).map(|_| tempfile::TempDir::new().unwrap()).collect();
        let dirs: Vec<std::path::PathBuf> = tmp.iter().map(|d| d.path().to_path_buf()).collect();
        let addr = free_addr().await;
        let ec = EcConfig {
            data: 1,
            parity: 1,
            enabled: true,
        };
        let node = NodeSpec {
            id: "n1".into(),
            addr: addr.to_string(),
            drive_count: 2,
        };
        let pairs = vec![("n1".to_string(), addr.to_string())];
        let mesh = Arc::new(TonicMesh::new("n1", "tok", &pairs).unwrap());
        let sets = ClusterSets::single("n1", vec![node], &ec).unwrap();
        let mut cfg = StoreConfig::new(&dirs[0]);
        cfg.drives = dirs;
        cfg.ec = ec;
        cfg.fsync = false;
        cfg.cluster = Some(Arc::new(ClusterConfig {
            sets,
            transport: mesh.clone(),
            placement_v2: false,
            resolver: None,
            stretch: false,
        }));
        let store = Arc::new(Store::open(cfg).await.unwrap());
        store.create_bucket("buck").await.unwrap();
        let s2 = store.clone();
        tokio::spawn(async move {
            let _ = serve(s2, addr, "tok".into()).await;
        });
        let mut up = false;
        for _ in 0..40 {
            if mesh.health("n1").await.is_ok() {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(up, "server 未就绪");

        let upload_id = store
            .create_multipart_upload("buck", "k", None, Default::default())
            .await
            .unwrap();
        assert!(upload_id.contains('.'), "集群 upload_id 应带属主前缀");
        let target = || mesh::MpuRef {
            bucket: "buck".into(),
            key: "k".into(),
            upload_id: upload_id.clone(),
        };

        // 1) 截断流（无 is_final）→ 信封 Encode；不得提交 part
        let (mut raw, _) = mesh.mpu_client("n1").unwrap();
        let trunc = vec![xl_proto::MpuPartChunk {
            bucket: "buck".into(),
            key: "k".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            data: b"half".to_vec(),
            ..Default::default()
        }];
        let resp = raw
            .upload_part(futures::stream::iter(trunc))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert_eq!(resp.code, "Encode");
        assert!(
            mesh.mpu_list_parts("n1", target())
                .await
                .unwrap()
                .is_empty()
        );

        // 2) 正常流式上传（>2 MiB → 客户端按 1 MiB 分块；属主流式落盘）
        let payload: Vec<u8> = (0..(2 * 1024 * 1024 + 123_457))
            .map(|i| (i % 251) as u8)
            .collect();
        let body: ByteStream = Box::pin(futures::stream::once({
            let p = payload.clone();
            async move { Ok(bytes::Bytes::from(p)) }
        }));
        let etag = mesh
            .mpu_upload_part(
                "n1",
                mesh::MpuPartUpload {
                    bucket: "buck".into(),
                    key: "k".into(),
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    sse_c_md5: None,
                    body,
                },
            )
            .await
            .unwrap();
        let parts = mesh.mpu_list_parts("n1", target()).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].size, payload.len() as u64);
        assert_eq!(parts[0].etag, etag);

        // 3) Complete 往返：属主提交 + ObjectInfo msgpack 解码
        let info = mesh
            .mpu_complete(
                "n1",
                mesh::MpuComplete {
                    bucket: "buck".into(),
                    key: "k".into(),
                    upload_id: upload_id.clone(),
                    parts: vec![(1, etag)],
                    mode: VersioningMode::Off,
                    encryption: None,
                    lock: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(info.size, payload.len() as u64);
        let (_, got) = store.get_object("buck", "k").await.unwrap();
        assert_eq!(got.len(), payload.len());
        assert_eq!(&got[..], &payload[..]);

        // 4) Abort 往返；随后 ListParts 收到语义错误 NoSuchUpload 信封（非笼统传输错）
        let gone = store
            .create_multipart_upload("buck", "gone", None, Default::default())
            .await
            .unwrap();
        mesh.mpu_abort(
            "n1",
            mesh::MpuRef {
                bucket: "buck".into(),
                key: "gone".into(),
                upload_id: gone.clone(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            mesh.mpu_list_parts(
                "n1",
                mesh::MpuRef {
                    bucket: "buck".into(),
                    key: "gone".into(),
                    upload_id: gone,
                }
            )
            .await,
            Err(Error::NoSuchUpload(_))
        ));

        // 5) 属主不可达（连接被拒）→ Unavailable（S3 503），绝不伪造成功
        let closed = free_addr().await;
        let down = TonicMesh::new("n1", "tok", &[("n1".to_string(), closed.to_string())]).unwrap();
        let body: ByteStream = Box::pin(futures::stream::once(async {
            Ok(bytes::Bytes::from_static(b"x"))
        }));
        let e = down
            .mpu_upload_part(
                "n1",
                mesh::MpuPartUpload {
                    bucket: "buck".into(),
                    key: "k".into(),
                    upload_id,
                    part_number: 1,
                    sse_c_md5: None,
                    body,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Unavailable(_)), "实得 {e:?}");
    }
}
