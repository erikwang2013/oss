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
use xl_store::store::Store;

/// 集群内共享令牌的 metadata 键。
const TOKEN_KEY: &str = "x-oss-cluster-token";

fn rpc_err(node: &str, e: Status) -> Error {
    Error::Encode(format!("mesh RPC 失败（node {node}）：{e}"))
}

fn unimpl(op: &str) -> Error {
    Error::Encode(format!("tonic mesh 未实现：{op}（随所属 Task 落地）"))
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
        let interceptor = TokenInterceptor::new(secret)?;
        let mut clients = HashMap::with_capacity(nodes.len());
        for (id, addr) in nodes {
            let ep = Endpoint::from_shared(format!("http://{addr}"))
                .map_err(|e| Error::Encode(format!("节点 {id} 地址非法（{addr}）：{e}")))?
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(5));
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
    // 以下 RPC 随所属 Task 落地（5a/5b/6/8/9）；在此之前显式报未实现，不静默成功。
    async fn write_shard(&self, _n: &str, _w: mesh::ShardWrite) -> Result<()> {
        Err(unimpl("write_shard"))
    }
    async fn read_shard(&self, _n: &str, _r: mesh::ShardRead) -> Result<Option<bytes::Bytes>> {
        Err(unimpl("read_shard"))
    }
    async fn delete_version(&self, _n: &str, _d: mesh::VersionDirRef) -> Result<()> {
        Err(unimpl("delete_version"))
    }
    async fn recycle_version(&self, _n: &str, _d: mesh::VersionDirRef) -> Result<()> {
        Err(unimpl("recycle_version"))
    }
    async fn prepare_meta(&self, _n: &str, _m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        Err(unimpl("prepare_meta"))
    }
    async fn commit_meta(&self, _n: &str, _m: mesh::MetaWrite) -> Result<mesh::MetaAck> {
        Err(unimpl("commit_meta"))
    }
    async fn abort_meta(&self, _n: &str, _m: mesh::MetaWrite) -> Result<()> {
        Err(unimpl("abort_meta"))
    }
    async fn read_meta(&self, _n: &str, _r: mesh::MetaRead) -> Result<Option<mesh::MetaSnapshot>> {
        Err(unimpl("read_meta"))
    }
    async fn ensure_bucket(&self, _n: &str, _b: &str) -> Result<()> {
        Err(unimpl("ensure_bucket"))
    }
    async fn list_bucket_dirs(&self, _n: &str) -> Result<Vec<String>> {
        Err(unimpl("list_bucket_dirs"))
    }
    async fn list_local(&self, _n: &str, _l: mesh::ListLocal) -> Result<mesh::ListLocalResp> {
        Err(unimpl("list_local"))
    }
    async fn lock(&self, _n: &str, _req: mesh::LockReq) -> Result<mesh::LockResp> {
        Err(unimpl("lock"))
    }
    async fn control_query(
        &self,
        _n: &str,
        _q: mesh::ControlQuery,
    ) -> Result<Option<mesh::ControlRow>> {
        Err(unimpl("control_query"))
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

    // 其余 RPC 随所属 Task 落地（5a/5b/6/8/9）
    async fn write_shard(
        &self,
        _r: Request<xl_proto::ShardWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn read_shard(
        &self,
        _r: Request<xl_proto::ShardRead>,
    ) -> std::result::Result<tonic::Response<xl_proto::ShardResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn delete_version(
        &self,
        _r: Request<xl_proto::VersionDirRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn recycle_version(
        &self,
        _r: Request<xl_proto::VersionDirRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn prepare_meta(
        &self,
        _r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaAck>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn commit_meta(
        &self,
        _r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaAck>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn abort_meta(
        &self,
        _r: Request<xl_proto::MetaWrite>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn read_meta(
        &self,
        _r: Request<xl_proto::MetaRead>,
    ) -> std::result::Result<tonic::Response<xl_proto::MetaResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn ensure_bucket(
        &self,
        _r: Request<xl_proto::BucketRef>,
    ) -> std::result::Result<tonic::Response<xl_proto::WriteResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn list_buckets(
        &self,
        _r: Request<xl_proto::Empty>,
    ) -> std::result::Result<tonic::Response<xl_proto::BucketsResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn list_local(
        &self,
        _r: Request<xl_proto::ListReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::ListLocalResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn lock(
        &self,
        _r: Request<xl_proto::LockReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::LockResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
    async fn control_query(
        &self,
        _r: Request<xl_proto::ControlReq>,
    ) -> std::result::Result<tonic::Response<xl_proto::ControlResp>, Status> {
        Err(Status::unimplemented("随所属 Task 落地"))
    }
}

/// 起节点间 gRPC 服务（oss-server 在 cluster 配置时调用；HTTP 端口与 gRPC 端口分离）。
pub async fn serve(store: Arc<Store>, addr: SocketAddr, secret: String) -> Result<()> {
    let svc = MeshSvc { store };
    let server =
        xl_proto::mesh_server::MeshServer::with_interceptor(svc, SecretInterceptor { secret });
    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| Error::Encode(format!("mesh gRPC server 退出（{addr}）：{e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xl_store::store::StoreConfig;

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

    /// 未知节点名显式报错（不 panic、不静默）。
    #[tokio::test]
    async fn unknown_node_is_explicit_error() {
        let nodes = vec![("n1".to_string(), "127.0.0.1:1".to_string())];
        let mesh = TonicMesh::new("n1", "s", &nodes).unwrap();
        let e = mesh.health("n9").await.unwrap_err().to_string();
        assert!(e.contains("未知节点 n9"), "实得：{e}");
    }
}
