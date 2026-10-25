//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::error_handling::HandleError;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use jwt_rust::integrations::axum::JwtLayer;
use s3s::service::S3ServiceBuilder;

use crate::state::AppState;
use xl_s3::auth::SigV4Auth;
use xl_s3::service::OssS3;

/// 组装全部路由。`build_runtime` 失败（暂存盘不可写等）向上传播：
/// serve 与运维命令统一 exit 1 + stderr，不从启动路径 panic。
pub fn build_router(state: AppState) -> Result<Router, Box<dyn std::error::Error + Send + Sync>> {
    // s3s 0.17 的 builder 是 `&mut self` 风格，不能链式调用
    // 授权门：Acl 同时给 service（ListBuckets 过滤/建桶记账）与 access 层（set_access）
    // Task 9：集群时 Acl/SigV4Auth 接控制面读扇出（本地 miss → peers；写仍本地，M9 收口）
    let acl = state.acl();
    let oss_s3 = OssS3::new(
        Arc::clone(&state.store),
        acl.clone(),
        state.cfg.max_put_bytes,
    )
    .with_events(Some(
        Arc::clone(&state.events) as Arc<dyn xl_s3::events::EventSink>
    ))
    .with_replication(
        Some(Arc::clone(&state.repl_sink) as Arc<dyn xl_s3::replication::ReplicationSink>),
        Some(Arc::clone(&state.repl_store) as Arc<dyn xl_s3::replication::ReplicationStore>),
    )
    .with_bucket_cleanup(Some(Arc::clone(&state.bucket_cleanup)))
    .with_lifecycle(Some(Arc::clone(&state.lifecycle)))
    .with_restore(Some(Arc::clone(&state.restore_sink)))
    .with_tier_gc(Some(Arc::clone(&state.tier_gc)))
    .with_inventory(Some(Arc::clone(&state.inventory)))
    // M9 S4：stretch 禁止 sync 桶（对象内容不得依赖单站点异步副本）；
    // M9 S3（I3）：S3 面的 sync 建桶要求 SYNC_READY + RAFT_READY + [raft]（与 console 同口径）。
    // “恰好 1 个数据 peer”是运行期 DB 条件（sites 行会变），在建桶入口按请求校验：
    // service.rs 的 site_peer_count / console 的 list_sites。
    .with_sync_ready(
        crate::raft::SYNC_READY
            && crate::raft::RAFT_READY
            && state.cfg.stretch.is_none()
            && state.cfg.raft.is_some(),
    )
    // M9：sync 摄入 token 校验（无 [site] = None → 一律 403）
    .with_site_token(state.site_enabled.then(|| state.site.token.clone()))
    // M9 S3：S3/lifecycle/batch/console 删除统一走远端优先服务。
    .with_sync_delete(Some(
        Arc::clone(&state.sync_delete) as Arc<dyn xl_s3::sync_delete::SyncDeleteSink>
    ))
    // M9 S6c：仅 Raft 模式注入桶控制面 sink（None = M7 直写路径逐字节不变）。
    .with_bucket_control(state.control.bucket_sink());
    // 归档对象 Range 直读：默认关（保持 403 InvalidObjectState）；显式开时只走
    // 有分块哈希且后端支持 Range 的对象，不可用自动回退。
    let oss_s3 = if std::env::var("OSS_TIER_DIRECT_READ").is_ok_and(|v| v == "1") {
        oss_s3.with_tier_direct(Some(Arc::new(crate::tier_direct::TierDirectReader::new(
            Arc::clone(&state.store),
            Arc::clone(&state.tiers),
        ))))
    } else {
        oss_s3
    };
    // M8 S1（N1）：内层服务无 auth/access/route，仅供 AP 重写后调用。
    let inner = Arc::new(S3ServiceBuilder::new(oss_s3.clone()).build());
    let mut s3_builder = S3ServiceBuilder::new(oss_s3);
    // AP/STS 自定义路由会承载 PutObject/UploadPart 等大请求；s3s 默认 1 MiB
    // custom_route_max_body_size 会误拒正常大对象。放宽到 AWS 单 PUT 上限；
    // 底层操作仍受 put_object_max_size 与 oss `max_put_bytes` 约束，AP 批量删
    // 解析器自带 1 MiB/1000 键上限。
    let mut s3_config = s3s::config::S3Config::default();
    s3_config.custom_route_max_body_size = Some(5 * 1024 * 1024 * 1024);
    s3_builder.set_config(Arc::new(s3s::config::StaticConfigProvider::new(Arc::new(
        s3_config,
    ))));
    s3_builder.set_auth(
        SigV4Auth::with_mesh(state.db.pool().clone(), state.control_mesh())
            .with_vault(state.vault.clone()) // M5 切片 6：SK 解密/透明重写
            .with_raft_mode(state.control.is_raft()), // M9 S6b H2：Raft 下禁止 apply 外重封
    );
    s3_builder.set_access(acl.clone());
    // STS：[sts.web_identity] 缺省关闭；启用时 AppConfig::load 已校验，这里构建验证器
    // （只读本地 JWKS，无网络）。配置错误 fail-closed：拒绝启动。
    let web_identity = state
        .cfg
        .sts
        .web_identity()
        .map_err(|e| format!("[sts.web_identity] 配置非法：{e}"))?;
    // M8 S1（N2）：AP 与 STS 共用一个 route 槽：先 AP，未命中委托 STS。
    let sts: Arc<dyn s3s::route::S3Route> = Arc::new(
        xl_s3::sts::StsRoute::new(state.db.pool().clone())
            .with_vault(state.vault.clone())
            .with_web_identity(web_identity),
    );
    let ap = xl_s3::ap::ApRoute::new(
        Arc::clone(&state.ap_cache),
        Arc::new(crate::access_points_api::DbApSource {
            db: Arc::clone(&state.db),
            region: state.ap_region.clone(),
        }),
        acl,
        Arc::clone(&state.olap_cache),
        inner,
    )
    .with_fallback(sts);
    s3_builder.set_route(ap);
    let s3 = s3_builder.build();
    // 读路径补全 Task 5：s3s 0.17 的 Range 解析对多区间（`bytes=0-1,3-4`）直接 400；
    // 冻结裁决不实现 multipart/byteranges —— 仅 S3 面剥离 Range 头，GET/HEAD 降级整对象 200。
    // 包成 mini Router 挂 fallback，控制面路由不受影响。
    let s3: Router = Router::new()
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .layer(middleware::from_fn(strip_multirange));

    // 控制面：`/api/v1` 整体走 jwt-rust 的 JwtLayer，`/login` 在 except 名单里
    // （名单与 12h 过期都在 AppState::new；nest 会先剥掉 `/api/v1` 前缀，层看到的是 `/login`）。
    let console = JwtLayer::new(Arc::clone(&state.jwt)).attach(crate::console::routes());

    // aetherupload：四条路由（预处理 / 分块 / 展示 / 下载）。
    // 不用 crate 自带的 `integrations::ecat::routes`——它的 handler 读不到身份，
    // 无法按子账户权限执行「分组 → 桶」的门；改用 `aether_gate`（同一内核，形态逐字对齐）。
    //
    // 鉴权层（规格 §0.1「会话 Cookie/JWT 覆盖层」）：cookie 垫片在最外层把 `oss_token`
    // 写成 Authorization，再交 JwtLayer 校验（与控制面同一个 JwtAuth 实例 → token 两面通用）。
    // 层序：后挂的 `.layer` 在更外层，先跑；JwtLayer 把 payload 放进类型化 extension，
    // aether_gate 的 handler 用 `Extension<JwtPayload>` 取。
    let aether = crate::aether_gate::routes(crate::aether::build_runtime(&state)?, state.clone())
        .layer(JwtLayer::new(Arc::clone(&state.jwt)))
        .layer(middleware::from_fn(cookie_to_bearer));

    let app = Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .route("/health/cluster", get(health_cluster))
        .route("/health/ready", get(health_ready))
        // Prometheus 文本面（M4 观测面）。公开口径与 /health/* 相同：只暴露节点 id 与
        // 计数（/health/cluster 本就公开同量级信息）；要收严时在外层套鉴权即可。
        .route("/metrics", get(metrics))
        // M9 S0 骨架：S5 补 leader/term/applied；B 经 advertise_endpoint 访问。
        .route("/api/v1/raft/status", get(raft_status))
        .nest("/api/v1", console)
        .merge(aether.with_state(()))
        .fallback_service(s3)
        .with_state(state.clone());
    // S7.1：Raft RPC 路由只在 `[raft]` 下挂载；走集群令牌，不经 console JWT 层。
    let app = match state.raft.clone() {
        Some(handle) => {
            let token = state
                .cfg
                .raft
                .as_ref()
                .expect("raft handle implies [raft]")
                .cluster_token
                .clone();
            app.merge(crate::raft::routes::rpc_router(
                handle,
                token,
                state.cfg.site.is_some(),
                state.cfg.stretch.is_some(),
            ))
        }
        None => app,
    };
    // 层在路由之后挂：同时包住 fallback（S3 面）。
    let app = app.layer(middleware::from_fn(restore_accepted));
    // M8：SourceIp 条件求值的连接对端（最外层，S3 fallback 也覆盖）。ecat 的 serve 不带
    // connect_info，生产用可信反代注入 XFF；只有显式 `OSS_AP_TRUSTED_PROXY=1` 才信。
    // 转发头仅在直连 TCP 对端落在 `OSS_AP_TRUSTED_PEER_CIDRS` 内时采信；未配置该网段
    // （或传输层不提供 ConnectInfo，如 ecat 明文 HttpServer）→ 一律不采信（fail-closed）。
    // 默认单层反代（取最右一跳，向后兼容）；多层用 HOPS 或可信 CIDR 配置，解析失败启动失败。
    let trust_proxy = std::env::var("OSS_AP_TRUSTED_PROXY")
        .is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "yes"));
    let trust = if trust_proxy {
        let trust = ProxyTrust::from_env()?;
        if trust.peers.is_empty() {
            tracing::warn!(
                "OSS_AP_TRUSTED_PROXY=1 但未配置 OSS_AP_TRUSTED_PEER_CIDRS：\
                 X-Forwarded-For / x-oss-source-vpc 一律不采信（fail-closed）"
            );
        }
        Some(Arc::new(trust))
    } else {
        None
    };
    // `aws:SourceVpc`/`aws:SourceVpce` 的来源证明：静态部署 ID（优先）或可信反代的
    // 单行头；非法配置启动失败。两者独立配置，绝不互相推断。
    let vpc = Arc::new(Attest::vpc(trust_proxy)?);
    let vpce = Arc::new(Attest::vpce(trust_proxy)?);
    let app = app.layer(middleware::from_fn(move |req: Request, next: Next| {
        let trust = trust.clone();
        let vpc = vpc.clone();
        let vpce = vpce.clone();
        async move { ap_peer(req, next, trust, vpc, vpce).await }
    }));
    Ok(app)
}

/// 可信反代配置（仅 `OSS_AP_TRUSTED_PROXY` 开启时解析）：
/// - `OSS_AP_TRUSTED_PEER_CIDRS`：允许直连 S3 监听器的反代网段。**只有实际 TCP 对端
///   落在这些网段内**才采信转发头；未配置 → 转发头一律不采信（安全默认）。仅凭
///   `OSS_AP_TRUSTED_PROXY=1` 不再足以信任头，避免直连客户端伪造。
/// - `OSS_AP_TRUSTED_PROXY_HOPS`（默认 1）：前置代理层数，取右起第 HOPS 项为客户端；
///   单层反代 = 最右一跳（旧行为）。
/// - `OSS_AP_TRUSTED_PROXY_CIDRS`：逗号分隔可信代理网段；配置后从右向左跳过可信代理，
///   首个非可信项为客户端（此模式忽略 HOPS）。它过滤的是**头内链路**，不是直连对端。
///
/// 非法网段/0 跳 → 启动失败（配置错误不静默降级）。
#[derive(Clone)]
struct ProxyTrust {
    hops: usize,
    cidrs: Vec<xl_s3::ap::Cidr>,
    peers: Vec<xl_s3::ap::Cidr>,
}

impl ProxyTrust {
    fn from_env() -> Result<Self, String> {
        Self::parse(
            std::env::var("OSS_AP_TRUSTED_PROXY_HOPS").ok().as_deref(),
            std::env::var("OSS_AP_TRUSTED_PROXY_CIDRS").ok().as_deref(),
            std::env::var("OSS_AP_TRUSTED_PEER_CIDRS").ok().as_deref(),
        )
    }

    /// 纯解析入口（测试用）：None = 未设置。
    fn parse(hops: Option<&str>, cidrs: Option<&str>, peers: Option<&str>) -> Result<Self, String> {
        let hops = match hops.map(str::trim).filter(|v| !v.is_empty()) {
            Some(v) => v
                .parse::<usize>()
                .map_err(|_| format!("OSS_AP_TRUSTED_PROXY_HOPS 非法: {}", v.escape_debug()))?,
            None => 1,
        };
        if hops == 0 {
            return Err("OSS_AP_TRUSTED_PROXY_HOPS 必须 >= 1".into());
        }
        Ok(Self {
            hops,
            cidrs: parse_cidr_list(cidrs, "OSS_AP_TRUSTED_PROXY_CIDRS")?,
            peers: parse_cidr_list(peers, "OSS_AP_TRUSTED_PEER_CIDRS")?,
        })
    }

    /// 直连对端是否在显式反代网段内。无对端信息（传输层不提供 ConnectInfo）→ false。
    fn peer_trusted(&self, direct: Option<IpAddr>) -> bool {
        direct.is_some_and(|ip| self.peers.iter().any(|c| c.contains(ip)))
    }

    /// AP 条件使用的客户端地址。只有直连对端可信时才解析转发头；否则退回真实对端。
    /// 转发头畸形/跳数不足 → None（fail-closed，不回退到反代自身 IP）。
    fn resolve(&self, headers: &axum::http::HeaderMap, direct: Option<IpAddr>) -> Option<IpAddr> {
        if !self.peer_trusted(direct) {
            return direct;
        }
        self.client_ip(headers)
    }

    fn client_ip(&self, headers: &axum::http::HeaderMap) -> Option<IpAddr> {
        forwarded_ip(headers, self.hops, &self.cidrs)
    }
}

fn parse_cidr_list(raw: Option<&str>, name: &str) -> Result<Vec<xl_s3::ap::Cidr>, String> {
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(Vec::new());
    };
    v.split(',')
        .map(|raw| {
            let raw = raw.trim();
            xl_s3::ap::parse_cidr(raw).ok_or_else(|| format!("{name} 非法 CIDR: {raw}"))
        })
        .collect()
}

/// `aws:SourceVpc` / `aws:SourceVpce` 的**来源证明**（不是 PrivateLink/ENI 端点身份）：
/// - `OSS_AP_VPC_ID` / `OSS_AP_VPCE_ID` 非空：进程级静态证明（单 VPC/端点部署），
///   对应头一律不采信；
/// - 否则仅当 `OSS_AP_TRUSTED_PROXY=1` 且直连对端在 `OSS_AP_TRUSTED_PEER_CIDRS`
///   内时采信恰好一条同名头（`x-oss-source-vpc` / `x-oss-source-vpce`）；
///   重复/畸形/未信任/对端不可信 → 无证明（fail-closed）。
///
/// 两个证明**独立**：VPC 证明绝不推导端点证明。反代必须**覆盖**客户端自带的同名头；
/// 未显式开启信任时该头完全无效（部署不保证覆盖时，请用静态 ID）。
#[derive(Clone)]
struct Attest {
    static_id: Option<String>,
    trust_proxy: bool,
    header: &'static str,
    valid: fn(&str) -> bool,
}

impl Attest {
    /// `aws:SourceVpc`（静态 `OSS_AP_VPC_ID` / 头 `x-oss-source-vpc`）。
    fn vpc(trust_proxy: bool) -> Result<Self, String> {
        Self::from_env(
            "OSS_AP_VPC_ID",
            "x-oss-source-vpc",
            xl_s3::ap::valid_vpc_id,
            trust_proxy,
        )
    }

    /// `aws:SourceVpce`（静态 `OSS_AP_VPCE_ID` / 头 `x-oss-source-vpce`）。
    fn vpce(trust_proxy: bool) -> Result<Self, String> {
        Self::from_env(
            "OSS_AP_VPCE_ID",
            "x-oss-source-vpce",
            xl_s3::ap::valid_vpce_id,
            trust_proxy,
        )
    }

    fn from_env(
        env: &'static str,
        header: &'static str,
        valid: fn(&str) -> bool,
        trust_proxy: bool,
    ) -> Result<Self, String> {
        let static_id = match std::env::var(env) {
            Ok(v) if !v.trim().is_empty() => {
                let v = v.trim().to_string();
                if !valid(&v) {
                    return Err(format!("{env} 非法: {}", v.escape_debug()));
                }
                Some(v)
            }
            _ => None,
        };
        Ok(Self {
            static_id,
            trust_proxy,
            header,
            valid,
        })
    }

    /// `peer_trusted` = 直连对端是否在 `OSS_AP_TRUSTED_PEER_CIDRS` 内。静态 ID 与对端
    /// 无关；头形态仅在显式信任反代且对端可信时成立，否则无证明（fail-closed）。
    fn value(&self, headers: &axum::http::HeaderMap, peer_trusted: bool) -> Option<String> {
        if let Some(id) = &self.static_id {
            return Some(id.clone());
        }
        if !self.trust_proxy || !peer_trusted {
            return None;
        }
        let mut vals = headers.get_all(self.header).iter();
        let raw = vals.next()?.to_str().ok()?.trim();
        if vals.next().is_some() || !(self.valid)(raw) {
            return None;
        }
        Some(raw.to_string())
    }
}

/// 连接对端/来源 VPC/VPCE → `ApPeer`/`ApVpc`/`ApVpce` extension
/// （缺失则由 AP 条件 fail-closed）。
async fn ap_peer(
    mut req: Request,
    next: Next,
    trust: Option<Arc<ProxyTrust>>,
    vpc: Arc<Attest>,
    vpce: Arc<Attest>,
) -> Response {
    // ConnectInfo 是唯一可信的直连对端来源；缺失（如 ecat 明文 listener）时转发头不采信。
    let direct = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_canonical());
    let (ip, peer_trusted) = match trust.as_ref() {
        Some(t) => (t.resolve(req.headers(), direct), t.peer_trusted(direct)),
        None => (direct, false),
    };
    if let Some(ip) = ip {
        req.extensions_mut().insert(xl_s3::ap::ApPeer(ip));
    }
    if let Some(id) = vpc.value(req.headers(), peer_trusted) {
        req.extensions_mut().insert(xl_s3::ap::ApVpc(id));
    }
    if let Some(id) = vpce.value(req.headers(), peer_trusted) {
        req.extensions_mut().insert(xl_s3::ap::ApVpce(id));
    }
    next.run(req).await
}

/// 从 `X-Forwarded-For` 解析客户端地址。左侧值客户端可伪造；只有右侧由可信代理
/// 追加/校验的部分可信。任意一项畸形或重复、跳数不足 → `None`（fail-closed）。
fn forwarded_ip(
    headers: &axum::http::HeaderMap,
    hops: usize,
    trusted: &[xl_s3::ap::Cidr],
) -> Option<IpAddr> {
    let mut vals = headers.get_all("x-forwarded-for").iter();
    let raw = vals.next()?.to_str().ok()?;
    if vals.next().is_some() {
        return None;
    }
    let mut ips: Vec<IpAddr> = Vec::new();
    for part in raw.split(',') {
        let ip = part.trim().parse::<IpAddr>().ok()?.to_canonical();
        // 重复项通常意味着链路被注入/重放，无法可靠归属 → 拒绝
        if ips.contains(&ip) {
            return None;
        }
        ips.push(ip);
    }
    if trusted.is_empty() {
        return ips.get(ips.len().checked_sub(hops)?).copied();
    }
    // CIDR 模式：从右向左跳过可信代理，首个非可信项即客户端；全为可信代理 → 拒绝。
    ips.iter()
        .rev()
        .find(|ip| !trusted.iter().any(|c| c.contains(**ip)))
        .copied()
}

async fn raft_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut v = state.control.status_json(state.cfg.raft.is_some());
    // 与 /api/v1/site 同口径：stretch 能力要求实际配置了 [stretch]，而非仅有 [raft]。
    if let Some(caps) = v.get_mut("capabilities") {
        caps["stretch"] =
            serde_json::json!(crate::raft::STRETCH_READY && state.cfg.stretch.is_some());
    }
    Json(v)
}

/// s3s 0.17 的 RestoreObject 生成代码固定按 `200 OK` 序列化并丢弃 `S3Response.status`，
/// handler 里设的 202 到不了线上；这里在 HTTP 层补回 S3 语义（仅 POST `?restore`，
/// 且只改写 200，错误状态原样透传）。`x-amz-restore` 服务已设置，entry 避免重复。
async fn restore_accepted(req: Request, next: Next) -> Response {
    let is_restore = req.method() == axum::http::Method::POST
        && req.uri().query().is_some_and(|q| {
            q.split('&')
                .any(|kv| kv.split('=').next() == Some("restore"))
        });
    let mut res = next.run(req).await;
    if is_restore && res.status() == StatusCode::OK {
        *res.status_mut() = StatusCode::ACCEPTED;
        res.headers_mut()
            .entry(header::HeaderName::from_static("x-amz-restore"))
            .or_insert(HeaderValue::from_static("ongoing-request=\"true\""));
    }
    res
}

/// 多区间 Range（`bytes=0-1,3-4`）在 s3s 0.17 解析层直接 400；冻结裁决不做
/// multipart/byteranges —— 未签名的 S3 面剥离该头，GET/HEAD 降级整对象 200。
/// **Range 已计入 SigV4 SignedHeaders 时不得剥离**：剥离会让验签失败（403
/// SignatureDoesNotMatch）；保留则验签通过、反序列化按 s3s 语义返回确定性
/// 400 InvalidArgument。单区间原样透传。
async fn strip_multirange(mut req: Request, next: Next) -> Response {
    let multi = matches!(*req.method(), Method::GET | Method::HEAD)
        && req
            .headers()
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains(','));
    if multi && !range_is_signed(req.headers(), req.uri()) {
        req.headers_mut().remove(header::RANGE);
    }
    next.run(req).await
}

/// Range 是否出现在 SigV4 SignedHeaders（Authorization 头或预签名查询）。
fn range_is_signed(headers: &axum::http::HeaderMap, uri: &axum::http::Uri) -> bool {
    let has_range = |list: &str| {
        list.split(';')
            .any(|h| h.trim().eq_ignore_ascii_case("range"))
    };
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && let Some((_, rest)) = auth.split_once("SignedHeaders=")
        && has_range(rest.split(',').next().unwrap_or(""))
    {
        return true;
    }
    uri.query().is_some_and(|q| {
        q.split('&').any(|kv| {
            let Some((k, v)) = kv.split_once('=') else {
                return false;
            };
            k.eq_ignore_ascii_case("X-Amz-SignedHeaders")
                && has_range(&v.replace("%3B", ";").replace("%3b", ";"))
        })
    })
}

/// peer 探测结果。`up=false` 时版本/指纹字段无意义（恒 0/空）。
pub(crate) struct PeerProbe {
    pub id: String,
    pub up: bool,
    /// 在册槽数
    pub slots: usize,
    /// 对端数据面版本（旧节点 health 缺字段 → 按 1 展示）。
    pub data_wire_version: u32,
    /// 对端拓扑指纹（未宣告 = 0/空）。
    pub topo_epoch: u64,
    pub topo_hash: String,
}

/// DP-1 数据面状态（`/health/cluster`、`/api/v1/cluster/status`、`/metrics` 共用）。
/// `compatible` = 无不可兼容 peer（降级 peer 不算不兼容：混版窗口可服务）。
pub(crate) struct DataPlaneStatus {
    pub compatible: bool,
    /// 可达但只能走降级语义的旧版 peer（`id(v1)`）。
    pub degraded_peers: Vec<String>,
    /// 可达且能力不可兼容（未知 wire / 拓扑不一致）。
    pub incompatible_peers: Vec<String>,
}

/// DP-1 判定：down 节点不下结论（交给既有 quorum/降级语义）；可达 peer 套用唯一策略
/// [`xl_store::mesh_gate::check_peer`]——Legacy 计入降级，不兼容计入 fail-closed 列表。
pub(crate) fn data_plane_status(
    peers: &[PeerProbe],
    local: &xl_store::mesh_gate::LocalCaps,
) -> DataPlaneStatus {
    let mut degraded = Vec::new();
    let mut incompatible = Vec::new();
    for p in peers.iter().filter(|p| p.up) {
        let caps = xl_store::mesh_gate::PeerCaps {
            wire_version: p.data_wire_version,
            topo_epoch: p.topo_epoch,
            topo_hash: p.topo_hash.clone(),
        };
        match xl_store::mesh_gate::check_peer(&p.id, &caps, local) {
            Ok(xl_store::mesh_gate::PeerClass::Legacy) => {
                degraded.push(format!("{}(v{})", p.id, p.data_wire_version))
            }
            Ok(xl_store::mesh_gate::PeerClass::Current) => {}
            Err(_) => incompatible.push(format!("{}(v{})", p.id, p.data_wire_version)),
        }
    }
    DataPlaneStatus {
        compatible: incompatible.is_empty(),
        degraded_peers: degraded,
        incompatible_peers: incompatible,
    }
}

/// peer 探测（单点 800ms 超时；错接线按 down + 显性日志）。
/// `/health/cluster`、`/metrics`、`/api/v1/cluster/status` 三处共用一份探测逻辑。
/// 顺序 = 拓扑顺序（不含本节点；单机为空表）。
pub(crate) async fn probe_peers(state: &AppState) -> Vec<PeerProbe> {
    let mut out = Vec::new();
    let Some(c) = state.store.cluster() else {
        return out;
    };
    let self_node_id = c.sets.flat.self_node().id.clone();
    for n in &c.sets.flat.nodes {
        if n.id == self_node_id {
            continue;
        }
        // 单点探测 800ms：就绪探针会被 LB 轮询，要短平快；掉队节点标 down 即可
        match tokio::time::timeout(Duration::from_millis(800), c.transport.health(&n.id)).await {
            Ok(Ok(h)) if h.node_id == n.id => out.push(PeerProbe {
                id: n.id.clone(),
                up: true,
                slots: h.slots.iter().filter(|(_, up)| *up).count(),
                // 缺字段（旧节点）= 1（保守），与本机判定口径一致。
                data_wire_version: h.peer_data_wire_version(),
                topo_epoch: h.topo_epoch,
                topo_hash: h.topo_hash,
            }),
            Ok(Ok(h)) => {
                // 错接线：地址通了但不是声明的节点——按 down 处理并留下显性日志
                tracing::error!(node = %n.id, reported = %h.node_id, "mesh 节点身份不符（错接线？）");
                out.push(PeerProbe {
                    id: n.id.clone(),
                    up: false,
                    slots: 0,
                    data_wire_version: 0,
                    topo_epoch: 0,
                    topo_hash: String::new(),
                });
            }
            _ => out.push(PeerProbe {
                id: n.id.clone(),
                up: false,
                slots: 0,
                data_wire_version: 0,
                topo_epoch: 0,
                topo_hash: String::new(),
            }),
        }
    }
    out
}

/// Prometheus 文本面（手写 exposition——十来个指标不值一个依赖树）。
/// 指标：quorum 未达计数（写/锁）、各节点可达性、本机盘数、全局槽位数。
async fn metrics(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    let local = state.store.mesh_health().await;
    let (unmet_write, unmet_lock) = state.store.quorum_unmet_counts();
    let drive_count = local.slots.len();
    let slot_count = state
        .store
        .cluster()
        .map(|c| c.sets.flat.slot_count())
        .unwrap_or(drive_count as u16);

    let mut out = String::with_capacity(512);
    out.push_str("# HELP oss_quorum_unmet_total quorum 未达成次数（写 quorum / 租约锁分列）\n");
    out.push_str("# TYPE oss_quorum_unmet_total counter\n");
    out.push_str(&format!(
        "oss_quorum_unmet_total{{op=\"write\"}} {unmet_write}\n"
    ));
    out.push_str(&format!(
        "oss_quorum_unmet_total{{op=\"lock\"}} {unmet_lock}\n"
    ));
    out.push_str("# HELP oss_node_up 本节点视角各节点可达性（自身恒 1）\n");
    out.push_str("# TYPE oss_node_up gauge\n");
    out.push_str(&format!(
        "oss_node_up{{node=\"{}\"}} 1\n",
        label_escape(&local.node_id)
    ));
    let peers = probe_peers(&state).await;
    for p in &peers {
        out.push_str(&format!(
            "oss_node_up{{node=\"{}\"}} {}\n",
            label_escape(&p.id),
            u8::from(p.up)
        ));
    }
    // DP-1：混版窗口的线上信号——不兼容（fail-closed）与降级（可服务但语义受限）分列。
    let status = data_plane_status(&peers, &state.store.local_caps());
    out.push_str(
        "# HELP oss_data_plane_compatible 数据面能力兼容（1 = 无可达的不兼容节点）\n\
         # TYPE oss_data_plane_compatible gauge\n",
    );
    out.push_str(&format!(
        "oss_data_plane_compatible {}\n",
        u8::from(status.compatible)
    ));
    out.push_str(
        "# HELP oss_data_plane_degraded_peers 可达但能力降级的旧版 peer 数（混版窗口）\n\
         # TYPE oss_data_plane_degraded_peers gauge\n",
    );
    out.push_str(&format!(
        "oss_data_plane_degraded_peers {}\n",
        status.degraded_peers.len()
    ));
    out.push_str("# HELP oss_drive_count 本机盘数\n# TYPE oss_drive_count gauge\n");
    out.push_str(&format!("oss_drive_count {drive_count}\n"));
    out.push_str(
        "# HELP oss_slot_count 全局槽位数（单机 = 本机盘数）\n# TYPE oss_slot_count gauge\n",
    );
    out.push_str(&format!("oss_slot_count {slot_count}\n"));
    // stretch 数据面信号：未配置 [stretch] 时为 0（本机可达槽数此时无 stretch 语义）。
    let stretch_slots_ok = if state.cfg.stretch.is_some() {
        local.slots.iter().filter(|(_, up)| *up).count()
    } else {
        0
    };
    out.push_str(
        "# HELP oss_stretch_slots_ok 本机可达盘/槽数（stretch 数据面信号；未配置 [stretch] = 0）\n\
         # TYPE oss_stretch_slots_ok gauge\n",
    );
    out.push_str(&format!("oss_stretch_slots_ok {stretch_slots_ok}\n"));
    out.push_str(
        "# HELP oss_sync_delete_partial_total DeleteObjects 远端失败整体 503 次数（部分键已提交）\n\
         # TYPE oss_sync_delete_partial_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_delete_partial_total {}\n",
        state.sync_delete.partial_total()
    ));
    out.push_str(
        "# HELP oss_sync_delete_recovered_total 周期 worker 收敛的 sync delete intent 行数\n\
         # TYPE oss_sync_delete_recovered_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_delete_recovered_total {}\n",
        state.sync_delete.recovered_total()
    ));
    out.push_str(
        "# HELP oss_sync_ambiguous_total 远端删除超时且 HEAD 探测无法定位（执行与否不可判定）次数\n\
         # TYPE oss_sync_ambiguous_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_ambiguous_total {}\n",
        state.sync_delete.ambiguous_total()
    ));
    out.push_str(
        "# HELP oss_sync_acked_total sync 写远端确认成功次数\n\
         # TYPE oss_sync_acked_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_acked_total {}\n",
        state.sync_metrics.acked_total()
    ));
    out.push_str(
        "# HELP oss_sync_failed_total sync 写 before_commit 阶段失败次数（不含 is_sync 门控拒绝）\n\
         # TYPE oss_sync_failed_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_failed_total {}\n",
        state.sync_metrics.failed_total()
    ));
    out.push_str(
        "# HELP oss_sync_write_intent_recovered_total 恢复 worker 清理的写 intent 行数\n\
         # TYPE oss_sync_write_intent_recovered_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_write_intent_recovered_total {}\n",
        state.sync_metrics.write_intent_recovered_total()
    ));
    if let Some(handle) = &state.raft {
        let (term, applied, voters, role) = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            let role = if m.current_leader == Some(handle.node_id()) {
                "leader"
            } else {
                "follower"
            };
            (
                m.current_term,
                m.last_applied.map(|l| l.index).unwrap_or(0),
                m.membership_config.voter_ids().count(),
                role,
            )
        };
        out.push_str("# HELP oss_raft_role 本节点控制面角色（1 = 当前角色）\n");
        out.push_str("# TYPE oss_raft_role gauge\n");
        for r in ["leader", "follower", "witness"] {
            let v =
                u8::from(
                    r == role
                        || (r == "witness"
                            && state.cfg.raft.as_ref().is_some_and(|c| {
                                matches!(c.role, crate::config::RaftRole::Witness)
                            })),
                );
            out.push_str(&format!("oss_raft_role{{role=\"{r}\"}} {v}\n"));
        }
        out.push_str(&format!(
            "oss_raft_term {term}\noss_raft_last_applied {applied}\noss_raft_members {voters}\n"
        ));
        out.push_str(&format!(
            "oss_raft_production_ready {}\n",
            u8::from(voters >= 3)
        ));
        let counters = handle.counters();
        out.push_str(
            "# HELP oss_raft_leader_changes_total 控制面 leader 变更采样次数（含首次当选；可能合并中间跳变）\n\
             # TYPE oss_raft_leader_changes_total counter\n",
        );
        out.push_str(&format!(
            "oss_raft_leader_changes_total {}\n",
            counters.leader_changes()
        ));
        out.push_str(
            "# HELP oss_raft_apply_errors_total 状态机 apply 失败次数（fail-stop 前每批失败 +1）\n\
             # TYPE oss_raft_apply_errors_total counter\n",
        );
        out.push_str(&format!(
            "oss_raft_apply_errors_total {}\n",
            counters.apply_errors()
        ));
        out.push_str(
            "# HELP oss_raft_forward_total follower 发起的控制写转发尝试次数（含失败；不含成员变更）\n\
             # TYPE oss_raft_forward_total counter\n",
        );
        out.push_str(&format!("oss_raft_forward_total {}\n", counters.forwards()));
        out.push_str(
            "# HELP oss_raft_snapshot_bytes 当前已落库快照数据字节数（不含 meta；无快照或读失败 = 0 并告警）\n\
             # TYPE oss_raft_snapshot_bytes gauge\n",
        );
        out.push_str(&format!(
            "oss_raft_snapshot_bytes {}\n",
            handle.state_machine().snapshot_bytes().await
        ));
    }
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}

/// Prometheus 标签值转义（配置里的 node id 属半信任输入；坏配置不该产出坏样本行）。
fn label_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// 集群健康视图：`/health/cluster` 的结构化体与 `/health/ready` 的判定共用一个实现。
/// 返回 `(view, ready, reason)`。单节点（无 cluster）时域 = 自身。
async fn cluster_view(state: &AppState) -> (serde_json::Value, bool, String) {
    let local = state.store.mesh_health().await;
    let self_node_id = local.node_id.clone();
    let (unmet_write, unmet_lock) = state.store.quorum_unmet_counts();
    let up_slots = |h: &xl_store::mesh::NodeHealth| h.slots.iter().filter(|(_, up)| *up).count();
    let mut nodes = vec![serde_json::json!({
        "id": self_node_id,
        "up": true,
        "slots": up_slots(&local),
        "data_wire_version": local.peer_data_wire_version(),
    })];
    let peers = probe_peers(state).await;
    let status = data_plane_status(&peers, &state.store.local_caps());
    // 单机 = 1（自身即多数）；集群 = 1 + peers
    let expected = 1 + peers.len();
    let mut reachable = 1usize;
    for p in &peers {
        if p.up {
            reachable += 1;
        }
        nodes.push(serde_json::json!({
            "id": p.id, "up": p.up, "slots": p.slots,
            // down 节点版本未知 → null（与 /api/v1/cluster/status 口径一致）
            "data_wire_version": p.up.then_some(p.data_wire_version),
        }));
    }
    let qn = expected / 2 + 1;
    let slot0_up = local.slots.first().is_some_and(|(_, up)| *up);
    let (ready, reason) = if !slot0_up {
        (false, "本机 slot0 不在册".to_string())
    } else if reachable < qn {
        (
            false,
            format!("可达节点 {reachable}/{expected} < 节点多数 {qn}"),
        )
    } else {
        (true, "ok".to_string())
    };
    let view = serde_json::json!({
        "node_id": self_node_id,
        "version": local.version,
        "nodes": nodes,
        "quorum_unmet": { "write": unmet_write, "lock": unmet_lock },
        // DP-1：运维可见的数据面版本/混版状态（down 节点不下版本结论）。
        "data_wire_version": local.peer_data_wire_version(),
        "data_plane_compatible": status.compatible,
        "degraded_peers": status.degraded_peers,
        "incompatible_peers": status.incompatible_peers,
        "ready": ready,
    });
    (view, ready, reason)
}

/// 集群健康端点（设计 §19 M3「集群健康端点」）：本节点 + 各 peer 槽位在册状态 +
/// quorum 未达计数。peer 不可达/身份不符标 down；HTTP 恒 200（结构化体自描述）。
async fn health_cluster(State(state): State<AppState>) -> Json<serde_json::Value> {
    let (view, _, _) = cluster_view(&state).await;
    Json(view)
}

/// 就绪探针（设计 §18.4 强语义）：本机 slot0 可用 **且** 可达节点数 ≥ 节点多数 Q_n——
/// 分区/少数派节点返回 503（LB 摘除；弱版「本机一份可用」会把流量继续送进来）。
async fn health_ready(State(state): State<AppState>) -> (StatusCode, String) {
    let (_, ready, reason) = cluster_view(&state).await;
    if ready {
        (StatusCode::OK, "ready".to_string())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}"),
        )
    }
}

/// cookie → Authorization 垫片：aetherupload 自带脚本的分块 XHR 只带 cookie（不开
/// Authorization 头），JwtLayer 只认 Bearer。仅当请求没有 Authorization 头时才注入。
async fn cookie_to_bearer(mut req: Request, next: Next) -> Response {
    if !req.headers().contains_key(header::AUTHORIZATION) {
        let bearer = req
            .headers()
            .get(header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(cookie_token)
            .and_then(|v| HeaderValue::from_str(&format!("Bearer {v}")).ok());
        if let Some(bearer) = bearer {
            req.headers_mut().insert(header::AUTHORIZATION, bearer);
        }
    }
    next.run(req).await
}

/// 手写分号切分：只取 `oss_token` 一个字段，不引 cookie 库。
/// 空值（`oss_token=`）视为没有 cookie —— 不遮蔽 Authorization 回退。
fn cookie_token(cookies: &str) -> Option<&str> {
    cookies
        .split(';')
        .filter_map(|part| part.trim().strip_prefix("oss_token="))
        .find(|v| !v.is_empty())
}

/// 只有 s3s 连错误响应都序列化失败时才到达，近乎不可达的兜底 —— 包括 body 超限、
/// 格式非法在内的 HTTP 层错误都已在 s3s 的 `ops::call` 内 XML 化了。
async fn handle_s3_error(err: s3s::HttpError) -> (StatusCode, String) {
    tracing::error!(error = ?err, "s3s 错误响应失败（兜底 500）");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::label_escape;
    use axum::http::StatusCode;

    fn probe(id: &str, up: bool, v: u32) -> super::PeerProbe {
        super::PeerProbe {
            id: id.into(),
            up,
            slots: 3,
            data_wire_version: v,
            topo_epoch: if up { 1 } else { 0 },
            topo_hash: if up {
                "aaaaaaaaaaaaaaaa".into()
            } else {
                String::new()
            },
        }
    }

    fn local_v1() -> xl_store::mesh_gate::LocalCaps {
        xl_store::mesh_gate::LocalCaps {
            topo_epoch: 1,
            topo_hash: "aaaaaaaaaaaaaaaa".into(),
        }
    }

    /// DP-1 状态口径：可达 legacy = 降级（可服务）；未知 wire / 拓扑不一致 = 不兼容；
    /// down 节点不下结论。
    #[test]
    fn data_plane_status_reports_legacy_peers_as_degraded() {
        let st = super::data_plane_status(&[], &local_v1());
        assert!(st.compatible);
        assert!(st.degraded_peers.is_empty());
        let st =
            super::data_plane_status(&[probe("n2", true, 2), probe("n3", true, 2)], &local_v1());
        assert!(st.compatible);
        assert!(st.degraded_peers.is_empty());
        // down 节点：版本未知，既不算降级也不算不兼容
        let st =
            super::data_plane_status(&[probe("n2", false, 0), probe("n3", false, 1)], &local_v1());
        assert!(st.compatible);
        assert!(st.degraded_peers.is_empty());
        assert!(st.incompatible_peers.is_empty());
        // 可达旧版 = 降级；未来版 = 不兼容（fail-closed 列表带节点与版本）
        let st = super::data_plane_status(
            &[
                probe("n2", true, 1),
                probe("n3", true, 2),
                probe("n4", true, 3),
            ],
            &local_v1(),
        );
        assert!(!st.compatible);
        assert_eq!(st.degraded_peers, vec!["n2(v1)".to_string()]);
        assert_eq!(st.incompatible_peers, vec!["n4(v3)".to_string()]);
        // 拓扑 fingerprint 不一致 → 不兼容（同 wire 2）
        let mut drifted = probe("n5", true, 2);
        drifted.topo_hash = "bbbbbbbbbbbbbbbb".into();
        let st = super::data_plane_status(&[drifted], &local_v1());
        assert!(!st.compatible);
        assert_eq!(st.incompatible_peers, vec!["n5(v2)".to_string()]);
    }

    #[test]
    fn forwarded_ip_hops_and_trusted_cidrs_fail_closed() {
        let mut h = axum::http::HeaderMap::new();
        assert!(super::forwarded_ip(&h, 1, &[]).is_none());
        // 默认单跳：左侧可伪造，只有最右一跳（可信代理追加）参与判定
        h.insert("x-forwarded-for", "203.0.113.5 , 10.0.0.1".parse().unwrap());
        assert_eq!(
            super::forwarded_ip(&h, 1, &[]),
            Some("10.0.0.1".parse().unwrap())
        );
        // 只取最后一个逗号段，且 IPv6/v4-mapped 归一
        h.insert(
            "x-forwarded-for",
            "192.0.2.9, ::ffff:10.2.3.4".parse().unwrap(),
        );
        assert_eq!(
            super::forwarded_ip(&h, 1, &[]),
            Some("10.2.3.4".parse().unwrap())
        );
        h.insert("x-forwarded-for", "2001:db8::1".parse().unwrap());
        assert_eq!(
            super::forwarded_ip(&h, 1, &[]),
            Some("2001:db8::1".parse().unwrap())
        );
        // 任意段畸形 → 无法定对端（不再只看最右一跳）
        for bad in ["10.0.0.1,", "garbage", "10.0.0.1, garbage"] {
            h.insert("x-forwarded-for", bad.parse().unwrap());
            assert!(super::forwarded_ip(&h, 1, &[]).is_none(), "{bad}");
        }
        // 重复项/重复头行：来源不可信 → 拒绝（不得任选其一）
        h.insert("x-forwarded-for", "10.0.0.1, 10.0.0.1".parse().unwrap());
        assert!(super::forwarded_ip(&h, 1, &[]).is_none());
        let mut h = axum::http::HeaderMap::new();
        h.append("x-forwarded-for", "10.0.0.1".parse().unwrap());
        h.append("x-forwarded-for", "10.0.0.2".parse().unwrap());
        assert!(super::forwarded_ip(&h, 1, &[]).is_none());

        // 多层（H=2）：右起第 2 项 = 客户端；跳数不足 → 拒绝
        h.insert(
            "x-forwarded-for",
            "192.0.2.9, garbage, 10.0.0.1".parse().unwrap(),
        );
        assert!(super::forwarded_ip(&h, 2, &[]).is_none(), "畸形段仍拒绝");
        h.insert(
            "x-forwarded-for",
            "192.0.2.9, 203.0.113.5, 10.0.0.1".parse().unwrap(),
        );
        assert_eq!(
            super::forwarded_ip(&h, 2, &[]),
            Some("203.0.113.5".parse().unwrap())
        );
        assert!(super::forwarded_ip(&h, 4, &[]).is_none(), "跳数不足");

        // CIDR 模式：从右向左跳过可信代理，首个非可信 = 客户端
        let cidrs = [
            xl_s3::ap::parse_cidr("10.0.0.0/8").unwrap(),
            xl_s3::ap::parse_cidr("2001:db8::/32").unwrap(),
        ];
        h.insert(
            "x-forwarded-for",
            "203.0.113.5, 10.0.0.1, 2001:db8::1".parse().unwrap(),
        );
        assert_eq!(
            super::forwarded_ip(&h, 1, &cidrs),
            Some("203.0.113.5".parse().unwrap())
        );
        // 全为可信代理 → 无客户端可归属，拒绝
        h.insert("x-forwarded-for", "10.0.0.1, 10.0.0.2".parse().unwrap());
        assert!(super::forwarded_ip(&h, 1, &cidrs).is_none());
        // 客户端不在可信网段：单条即客户端
        h.insert("x-forwarded-for", "203.0.113.5".parse().unwrap());
        assert_eq!(
            super::forwarded_ip(&h, 1, &cidrs),
            Some("203.0.113.5".parse().unwrap())
        );
    }

    /// SourceVpc/SourceVpce 来源证明：静态 ID 优先；未信任代理的头完全无效；
    /// 重复/畸形头 fail-closed；两个证明互不推断（VPC 头不能当 VPCE 证明）。
    #[test]
    fn source_vpc_attestation_static_and_proxy_modes() {
        use super::Attest;
        let mk = |static_id: Option<&str>, trust_proxy: bool| Attest {
            static_id: static_id.map(str::to_string),
            trust_proxy,
            header: "x-oss-source-vpc",
            valid: xl_s3::ap::valid_vpc_id,
        };
        let mut h = axum::http::HeaderMap::new();
        // 未开启信任：头无效（即使形态合法）
        let untrusted = mk(None, false);
        h.insert("x-oss-source-vpc", "vpc-abc123".parse().unwrap());
        assert!(untrusted.value(&h, true).is_none());
        // 可信反代 + 可信直连对端：恰好一条合法头才成立
        let trusted = mk(None, true);
        assert_eq!(trusted.value(&h, true).as_deref(), Some("vpc-abc123"));
        assert!(
            trusted.value(&h, false).is_none(),
            "直连对端不在 PEER_CIDRS → 头不采信"
        );
        assert_eq!(
            trusted.value(&axum::http::HeaderMap::new(), true),
            None,
            "缺头 → 无证明"
        );
        // 重复头行 → 无证明
        let mut dup = axum::http::HeaderMap::new();
        dup.append("x-oss-source-vpc", "vpc-a".parse().unwrap());
        dup.append("x-oss-source-vpc", "vpc-b".parse().unwrap());
        assert!(trusted.value(&dup, true).is_none());
        // 畸形（逗号拼接/大写/前缀错误/超长）→ 无证明
        for bad in [
            "vpc-a,vpc-b",
            "VPC-ABC",
            "abc",
            "vpc-",
            &format!("vpc-{}", "a".repeat(65)),
        ] {
            let mut h = axum::http::HeaderMap::new();
            h.insert("x-oss-source-vpc", bad.parse().unwrap());
            assert!(trusted.value(&h, true).is_none(), "{bad}");
        }
        // 静态 ID 优先，且头（含伪造值）一律不采信
        let fixed = mk(Some("vpc-static"), false);
        assert_eq!(fixed.value(&h, false).as_deref(), Some("vpc-static"));
        h.insert("x-oss-source-vpc", "vpc-attacker".parse().unwrap());
        assert_eq!(fixed.value(&h, true).as_deref(), Some("vpc-static"));
        // 配置值校验
        assert!(xl_s3::ap::valid_vpc_id("vpc-0123456789abcdef0"));
        assert!(xl_s3::ap::valid_vpc_id("vpce-abc"));
        assert!(!xl_s3::ap::valid_vpc_id("vpc-"));
        assert!(!xl_s3::ap::valid_vpc_id("vpc-ABC"));
        assert!(!xl_s3::ap::valid_vpc_id(""));

        // SourceVpce：独立证明、独立头；只接受 vpce- 形态
        let mk_vpce = |static_id: Option<&str>, trust_proxy: bool| Attest {
            static_id: static_id.map(str::to_string),
            trust_proxy,
            header: "x-oss-source-vpce",
            valid: xl_s3::ap::valid_vpce_id,
        };
        let mut hv = axum::http::HeaderMap::new();
        hv.insert("x-oss-source-vpce", "vpce-0123abc".parse().unwrap());
        let trusted_vpce = mk_vpce(None, true);
        assert_eq!(
            trusted_vpce.value(&hv, true).as_deref(),
            Some("vpce-0123abc")
        );
        assert!(trusted_vpce.value(&hv, false).is_none(), "对端不可信");
        assert!(
            trusted_vpce.value(&h, true).is_none(),
            "x-oss-source-vpc 头不得充当 VPCE 证明"
        );
        assert_eq!(
            mk_vpce(Some("vpce-static"), false)
                .value(&hv, false)
                .as_deref(),
            Some("vpce-static")
        );
        // SourceVpce 只收 vpce- 形态（vpc- 前缀/畸形拒）
        for bad in ["vpc-abc", "vpce-", "VPCE-ABC", "vpce-abc,extra"] {
            let mut hb = axum::http::HeaderMap::new();
            hb.insert("x-oss-source-vpce", bad.parse().unwrap());
            assert!(trusted_vpce.value(&hb, true).is_none(), "{bad}");
        }
        assert!(xl_s3::ap::valid_vpce_id("vpce-0123456789abcdef0"));
        assert!(!xl_s3::ap::valid_vpce_id("vpc-abc"));
    }

    /// 转发头仅在直连对端属于 `OSS_AP_TRUSTED_PEER_CIDRS` 时采信：
    /// 直连伪造者退回真实对端；无对端信息/无允许网段/链路畸形 → fail-closed。
    #[test]
    fn forwarded_headers_require_trusted_direct_peer() {
        use super::ProxyTrust;
        let t = ProxyTrust::parse(None, None, Some("10.0.0.0/8, 127.0.0.1")).unwrap();
        let mut h = axum::http::HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.9".parse().unwrap());

        // 允许的反代网段 / 回环反代 → 采信链尾客户端
        assert_eq!(
            t.resolve(&h, Some("10.1.2.3".parse().unwrap())),
            Some("203.0.113.9".parse().unwrap())
        );
        assert_eq!(
            t.resolve(&h, Some("127.0.0.1".parse().unwrap())),
            Some("203.0.113.9".parse().unwrap())
        );
        // 直连对端不在允许网段：只认真实对端，伪造头无效
        assert_eq!(
            t.resolve(&h, Some("198.51.100.7".parse().unwrap())),
            Some("198.51.100.7".parse().unwrap())
        );
        assert!(!t.peer_trusted(Some("198.51.100.7".parse().unwrap())));
        // 无 ConnectInfo（传输层不给对端）：不解析头
        assert_eq!(t.resolve(&h, None), None);
        assert!(!t.peer_trusted(None));
        // 未配置 PEER_CIDRS：安全默认 = 不信任任何转发头
        let no_peers = ProxyTrust::parse(None, None, None).unwrap();
        assert_eq!(
            no_peers.resolve(&h, Some("10.1.2.3".parse().unwrap())),
            Some("10.1.2.3".parse().unwrap())
        );
        assert!(!no_peers.peer_trusted(Some("10.1.2.3".parse().unwrap())));
        // 重复头行（可信对端）：链不可归属 → None，不回退到反代 IP
        let mut dup = axum::http::HeaderMap::new();
        dup.append("x-forwarded-for", "203.0.113.9".parse().unwrap());
        dup.append("x-forwarded-for", "203.0.113.10".parse().unwrap());
        assert_eq!(t.resolve(&dup, Some("10.1.2.3".parse().unwrap())), None);
        // 非法 CIDR 配置 → 启动失败
        assert!(ProxyTrust::parse(None, None, Some("not-a-cidr")).is_err());
        assert!(ProxyTrust::parse(None, Some("garbage"), None).is_err());
        assert!(ProxyTrust::parse(Some("0"), None, None).is_err());
    }

    /// RestoreObject 线上状态修复：仅 POST `?restore` 的 200 → 202 + `x-amz-restore`；
    /// 普通 POST/GET 与形似查询（`x-restore`）原样通过。
    #[tokio::test]
    async fn restore_post_is_rewritten_to_202() {
        async fn ok() -> StatusCode {
            StatusCode::OK
        }
        let app = axum::Router::new()
            .fallback(ok)
            .layer(axum::middleware::from_fn(super::restore_accepted));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();

        let res = http
            .post(format!("http://{addr}/b/k?restore"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202);
        assert_eq!(res.headers()["x-amz-restore"], "ongoing-request=\"true\"");
        assert_eq!(res.headers().get_all("x-amz-restore").iter().count(), 1);

        let res = http
            .post(format!("http://{addr}/b/k?restore=1"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202, "?restore=<v> 也是 RestoreObject");

        for (method, path) in [
            ("POST", "/b/k"),
            ("POST", "/b/k?x-restore=1"),
            ("GET", "/b/k?restore"),
        ] {
            let res = http
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    format!("http://{addr}{path}"),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "{method} {path} 不应被改写");
            assert!(
                res.headers().get("x-amz-restore").is_none(),
                "{method} {path}"
            );
        }
    }

    /// 多区间 Range 在 S3 fallback 前被剥离（降级整对象）；单区间原样透传。
    #[tokio::test]
    async fn multi_range_header_stripped_before_s3_fallback() {
        async fn echo_range(req: axum::extract::Request) -> String {
            req.headers()
                .get(axum::http::header::RANGE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string()
        }
        let app = axum::Router::new()
            .fallback(echo_range)
            .layer(axum::middleware::from_fn(super::strip_multirange));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();

        let res = http
            .get(format!("http://{addr}/b/k"))
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(res.text().await.unwrap(), "", "多区间头应被剥离");

        let res = http
            .get(format!("http://{addr}/b/k"))
            .header(axum::http::header::RANGE, "bytes=0-3")
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "bytes=0-3", "单区间不剥离");

        let res = http
            .post(format!("http://{addr}/b/k"))
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.text().await.unwrap(),
            "bytes=0-1,3-4",
            "非 GET/HEAD 不剥离"
        );

        // 已签名的 Range：保留（剥离会 403 签名失配；保留后 s3s 返回确定性 400）
        let auth_range = "AWS4-HMAC-SHA256 Credential=AKIA/20261023/us-east-1/s3/aws4_request, \
                          SignedHeaders=host;range;x-amz-date, Signature=00";
        let res = http
            .get(format!("http://{addr}/b/k"))
            .header(axum::http::header::AUTHORIZATION, auth_range)
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "bytes=0-1,3-4");
        // 签名未含 Range → 仍剥离
        let auth_no_range = "AWS4-HMAC-SHA256 Credential=AKIA/20261023/us-east-1/s3/aws4_request, \
                             SignedHeaders=host;x-amz-date, Signature=00";
        let res = http
            .get(format!("http://{addr}/b/k"))
            .header(axum::http::header::AUTHORIZATION, auth_no_range)
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "");
        // 预签名 URL 的 X-Amz-SignedHeaders（%3B 编码）同样参与判定
        let res = http
            .get(format!(
                "http://{addr}/b/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host%3Brange"
            ))
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "bytes=0-1,3-4");
        let res = http
            .get(format!(
                "http://{addr}/b/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host"
            ))
            .header(axum::http::header::RANGE, "bytes=0-1,3-4")
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "");
    }

    /// 标签值转义：坏配置（node id 带引号/反斜杠/换行）不得产出坏样本行。
    /// 反斜杠必须先转义（先换引号会把 `\"` 里的反斜杠二次转义错）。
    #[test]
    fn label_escape_covers_prometheus_metacharacters() {
        assert_eq!(label_escape("plain"), "plain");
        assert_eq!(label_escape("a\"b"), "a\\\"b");
        assert_eq!(label_escape("a\\b"), "a\\\\b");
        assert_eq!(label_escape("a\nb"), "a\\nb");
        assert_eq!(label_escape("\\\""), "\\\\\\\"");
    }
}
