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
use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use xl_store::error::{Error, Result};
use xl_store::mesh::{self, MeshTransport};
use xl_store::store::{Store, ec};

/// 集群内共享令牌的 metadata 键。
const TOKEN_KEY: &str = "x-oss-cluster-token";
/// W3C traceparent（M3 轻量传播：无 OTLP 接入前，客户端注入、服务端记入日志——
/// 双节点可按同一 trace id 抽查，设计 D:362 的最小兑现）。
const TRACEPARENT_KEY: &str = "traceparent";

fn rpc_err(node: &str, e: Status) -> Error {
    Error::Encode(format!("mesh RPC 失败（node {node}）：{e}"))
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
            .is_some_and(|v| v == self.secret);
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

/// 节点间传输的 tonic 实现（lazy connect：首次 RPC 才建连，不阻塞启动）。
pub struct TonicMesh {
    clients: HashMap<String, xl_proto::mesh_client::MeshClient<MeshChannel>>,
}

impl std::fmt::Debug for TonicMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TonicMesh")
            .field("nodes", &self.clients.keys().collect::<Vec<_>>())
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

    /// 超时可配 + 可选 mesh TLS；`tls = None` 保持旧明文行为。
    pub fn new_with_timeouts_tls(
        _self_id: &str,
        secret: &str,
        nodes: &[(String, String)],
        connect: Duration,
        request: Duration,
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Result<Self> {
        let interceptor = TokenInterceptor::new(secret)?;
        let scheme = if tls.is_some() { "https" } else { "http" };
        let mut clients = HashMap::with_capacity(nodes.len());
        for (id, addr) in nodes {
            let host_port = addr
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/');
            let mut ep = Endpoint::from_shared(format!("{scheme}://{host_port}"))
                .map_err(|e| Error::Encode(format!("节点 {id} 地址非法（{addr}）：{e}")))?
                .connect_timeout(connect)
                .timeout(request);
            if let Some(t) = &tls {
                ep = ep
                    .tls_config(t.clone())
                    .map_err(|e| Error::Encode(format!("节点 {id} TLS 配置失败：{e}")))?;
            }
            let ch = ep.connect_lazy();
            clients.insert(
                id.clone(),
                xl_proto::mesh_client::MeshClient::with_interceptor(ch, interceptor.clone()),
            );
        }
        Ok(Self { clients })
    }

    fn client(&self, node: &str) -> Result<&xl_proto::mesh_client::MeshClient<MeshChannel>> {
        self.clients
            .get(node)
            .ok_or_else(|| Error::Encode(format!("mesh：未知节点 {node}（不在拓扑表内）")))
    }
}

#[async_trait::async_trait]
impl MeshTransport for TonicMesh {
    async fn health(&self, node: &str) -> Result<mesh::NodeHealth> {
        let resp = self
            .client(node)?
            .clone()
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
        })
    }
    // 分片 / meta / 版本目录 / 回收（切片二）；其余 RPC 随所属 Task 落地（6/8/9）。
    async fn write_shard(&self, node: &str, w: mesh::ShardWrite) -> Result<()> {
        let resp = self
            .client(node)?
            .clone()
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
            .clone()
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
            .clone()
            .delete_version(dir_ref_to_proto(d))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn delete_shard(&self, node: &str, d: mesh::ShardDelete) -> Result<()> {
        let resp = self
            .client(node)?
            .clone()
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
            .clone()
            .recycle_version(dir_ref_to_proto(d))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn prepare_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        let resp = self
            .client(node)?
            .clone()
            .prepare_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(meta_ack_from_proto(resp))
    }
    async fn commit_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        let resp = self
            .client(node)?
            .clone()
            .commit_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        Ok(meta_ack_from_proto(resp))
    }
    async fn abort_meta(&self, node: &str, m: mesh::MetaWrite) -> Result<()> {
        let resp = self
            .client(node)?
            .clone()
            .abort_meta(meta_write_to_proto(m))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn read_meta(&self, node: &str, r: mesh::MetaRead) -> Result<mesh::MetaSnapshot> {
        let resp = self
            .client(node)?
            .clone()
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
            .clone()
            .delete_meta(meta_read_to_proto(r))
            .await
            .map_err(|e| rpc_err(node, e))?
            .into_inner();
        check_write(node, resp)
    }
    async fn delete_bucket_dir(&self, node: &str, bucket: &str) -> Result<()> {
        let resp = self
            .client(node)?
            .clone()
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
            .clone()
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
            .clone()
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
        };
        let resp = self
            .client(n)?
            .clone()
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
    async fn lock(&self, n: &str, req: mesh::LockReq) -> Result<mesh::LockResp> {
        let op = match req.op {
            mesh::LockOp::Acquire => xl_proto::LockOp::Acquire,
            mesh::LockOp::Renew => xl_proto::LockOp::Renew,
            mesh::LockOp::Release => xl_proto::LockOp::Release,
        };
        let resp = self
            .client(n)?
            .clone()
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
            .clone()
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
            .clone()
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
                limit: l.limit as usize,
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
        let mut si = SecretInterceptor { secret: "s".into() };
        assert!(si.call(Request::new(())).is_err());
    }
}
