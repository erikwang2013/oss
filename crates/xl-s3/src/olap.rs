//! M8 S2：Object Lambda——访问点上的 GET 变换器（HTTP / 可选 WASM）。
//!
//! 设计口径（§M8 / §7.1）：
//! - 只变换 GET 对象体；HEAD 直通；带变换器的写操作 405（路由层拦）。
//! - HTTP ABI：`POST <endpoint>`，body = 原始字节；头 `x-oss-bucket`、`x-oss-version-id`、
//!   `x-oss-etag`、`x-oss-size`、`x-oss-key-b64`（base64url 无填充）。
//! - SSRF：仅 http/https、拒 userinfo、每次请求解析并经 `resolve` 钉扎；拒内网/保留段
//!   （含 IPv4-mapped IPv6）；`no_proxy` + 不跟随重定向；连接 2s，总超时 1–30s，并发 16。
//! - 限额：输入上限独立（环境 `OSS_OLAP_MAX_INPUT_BYTES`，默认 64 MiB，路由层执行）；
//!   输出上限 `max_response_bytes`（默认 64 MiB）；gzip 按**解压后**字节计数。
//! - 失败一律上抛字符串详情（调用方只回 500 泛化文案，详情只进日志）。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
#[cfg(feature = "olap-wasm")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use futures::StreamExt as _;

/// WASM 模块上限（保存时校验）。
pub const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
/// 输出默认上限 / WASM 硬上限。
pub const DEFAULT_MAX_RESPONSE_BYTES: i64 = 64 * 1024 * 1024;
/// WASM 输出硬上限（配置更大也按此截）。
pub const MAX_WASM_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
pub const MIN_TIMEOUT_MS: i64 = 1_000;
pub const MAX_TIMEOUT_MS: i64 = 30_000;
/// 输出上限配置的防误配上界（1 GiB）。
const MAX_CONFIGURED_RESPONSE: i64 = 1024 * 1024 * 1024;
/// WASM 燃料（默认 2 亿）。
#[cfg(feature = "olap-wasm")]
const WASM_FUEL: u64 = 200_000_000;
#[cfg(feature = "olap-wasm")]
const WASM_MEMORY_BYTES: usize = 256 * 1024 * 1024;
/// WASM 表元素上限（防止用巨大 table 绕过内存限额）。
#[cfg(feature = "olap-wasm")]
const WASM_TABLE_ELEMENTS: usize = 100_000;

/// 变换器种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OlapKind {
    Http,
    Wasm,
}

impl OlapKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "http" => Some(Self::Http),
            "wasm" => Some(Self::Wasm),
            _ => None,
        }
    }
}

/// 变换器配置（DB `olap_configs` 行 → 编译产物）。
#[derive(Debug, Clone)]
pub struct OlapConfig {
    pub kind: String,
    pub endpoint: Option<String>,
    pub module: Option<Vec<u8>>,
    pub module_sha256: Option<String>,
    pub timeout_ms: i64,
    pub max_response_bytes: i64,
}

impl OlapConfig {
    pub fn prepare(&self) -> Result<OlapTransform, String> {
        OlapTransform::prepare(
            &self.kind,
            self.endpoint.clone(),
            self.module.as_deref(),
            self.timeout_ms,
            self.max_response_bytes,
        )
    }
}

/// 编译后的变换器（HTTP = 配置；WASM = 已编译 `Module`，随缓存替换失效）。
pub struct OlapTransform {
    kind: OlapKind,
    endpoint: Option<String>,
    timeout: Duration,
    max_response_bytes: usize,
    /// 预加载失败原因：配置在但变换器不可用 → GET 500（不得静默直通原文）。
    broken: Option<String>,
    /// 私网 endpoint 放行：`None` = 保存/请求时各读一次环境（撤销即时生效）；测试可固定。
    allow_private: Option<bool>,
    #[cfg(feature = "olap-wasm")]
    module: Option<wasmtime::Module>,
}

impl OlapTransform {
    /// 保存/预加载时调用：校验 URL 形状与限额；WASM 编译（feature 关闭时 `kind=wasm` 拒）。
    pub fn prepare(
        kind: &str,
        endpoint: Option<String>,
        module: Option<&[u8]>,
        timeout_ms: i64,
        max_response_bytes: i64,
    ) -> Result<Self, String> {
        Self::prepare_with_allow_private(
            kind,
            endpoint,
            module,
            timeout_ms,
            max_response_bytes,
            None,
        )
    }

    /// 同 [`Self::prepare`]，但固定私网放行判定：`Some(v)` 测试注入，`None` 走环境。
    pub fn prepare_with_allow_private(
        kind: &str,
        endpoint: Option<String>,
        module: Option<&[u8]>,
        timeout_ms: i64,
        max_response_bytes: i64,
        pinned: Option<bool>,
    ) -> Result<Self, String> {
        let kind = OlapKind::parse(kind).ok_or("kind 必须是 http/wasm")?;
        if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(format!(
                "timeout_ms 必须在 {MIN_TIMEOUT_MS}..={MAX_TIMEOUT_MS}"
            ));
        }
        if !(1..=MAX_CONFIGURED_RESPONSE).contains(&max_response_bytes) {
            return Err(format!(
                "max_response_bytes 必须在 1..={MAX_CONFIGURED_RESPONSE}"
            ));
        }
        let timeout = Duration::from_millis(timeout_ms as u64);
        let max_response_bytes = max_response_bytes as usize;
        match kind {
            OlapKind::Http => {
                let endpoint = endpoint.ok_or("kind=http 必须提供 endpoint")?;
                validate_endpoint_with(&endpoint, pinned.unwrap_or_else(allow_private))?;
                Ok(Self {
                    kind,
                    endpoint: Some(endpoint),
                    timeout,
                    max_response_bytes,
                    broken: None,
                    allow_private: pinned,
                    #[cfg(feature = "olap-wasm")]
                    module: None,
                })
            }
            OlapKind::Wasm => {
                let bytes = module.ok_or("kind=wasm 必须提供 module")?;
                if bytes.is_empty() {
                    return Err("wasm 模块为空".into());
                }
                if bytes.len() > MAX_MODULE_BYTES {
                    return Err(format!("wasm 模块超过 {MAX_MODULE_BYTES} 字节上限"));
                }
                #[cfg(feature = "olap-wasm")]
                {
                    let module = wasmtime::Module::new(wasm_engine(), bytes)
                        .map_err(|e| format!("wasm 编译失败: {e}"))?;
                    Ok(Self {
                        kind,
                        endpoint: None,
                        timeout,
                        max_response_bytes,
                        broken: None,
                        allow_private: pinned,
                        module: Some(module),
                    })
                }
                #[cfg(not(feature = "olap-wasm"))]
                {
                    let _ = bytes;
                    Err("wasm 变换器未启用（本次构建未开启 olap-wasm）".into())
                }
            }
        }
    }
}

/// 变换输入（来自内层 GET 响应）。
pub struct OlapInput<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<&'a str>,
    pub etag: Option<&'a str>,
    pub bytes: Bytes,
}

/// 变换输出。
pub struct OlapOutput {
    pub bytes: Bytes,
    pub content_type: Option<String>,
}

/// 并发闸（每进程 16）。
fn transform_gate() -> &'static tokio::sync::Semaphore {
    static GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);
    &GATE
}

/// 取变换并发槽。调用方必须在读取输入体**之前**取得，并持有到变换输出完成，
/// 使缓冲、变换与输出全部计入并发额度。
pub async fn acquire_transform_slot() -> Result<tokio::sync::SemaphorePermit<'static>, String> {
    transform_gate()
        .acquire()
        .await
        .map_err(|_| "transform gate closed".to_string())
}

/// 当前空闲槽数（诊断/测试用）。
pub fn available_transform_slots() -> usize {
    transform_gate().available_permits()
}

impl OlapTransform {
    /// 预加载失败占位（GET 500，而不是静默返回未变换内容）。
    pub fn broken(reason: String) -> Self {
        Self {
            kind: OlapKind::Http,
            endpoint: None,
            timeout: Duration::from_secs(1),
            max_response_bytes: 0,
            broken: Some(reason),
            allow_private: None,
            #[cfg(feature = "olap-wasm")]
            module: None,
        }
    }

    /// 执行变换。`slot` 由 [`acquire_transform_slot`] 取得，须已覆盖输入体读取。
    /// 错误详情只允许进日志（可能含 endpoint/IP）。
    pub async fn transform(
        &self,
        input: OlapInput<'_>,
        _slot: &tokio::sync::SemaphorePermit<'static>,
    ) -> Result<OlapOutput, String> {
        if let Some(reason) = &self.broken {
            return Err(format!("变换器不可用: {reason}"));
        }
        match self.kind {
            OlapKind::Http => self.http_transform(input).await,
            OlapKind::Wasm => self.wasm_transform(input).await,
        }
    }
}

// ---------------- HTTP 变换器 ----------------

/// 一次请求的解析目标：域名（需钉扎）或已校验的 IP 字面量。
struct EndpointTarget {
    host: String,
    port: u16,
    ip: Option<IpAddr>,
}

fn parse_endpoint(endpoint: &str) -> Result<EndpointTarget, String> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| "endpoint 不是合法 URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("endpoint 只允许 http/https".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("endpoint 不允许 userinfo".into());
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "endpoint 缺端口".to_string())?;
    let raw = url
        .host_str()
        .ok_or_else(|| "endpoint 缺主机".to_string())?;
    // url 对 IPv6 主机串带方括号；剥掉后才能 parse 成 IpAddr / 交给 DNS。
    let host = raw
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(raw);
    let ip = host.parse::<IpAddr>().ok();
    Ok(EndpointTarget {
        host: host.to_string(),
        port,
        ip,
    })
}

/// 保存时校验：形状 + IP 字面量的内网拒（域名留到每次请求解析后判）。
/// `allow_private` 由调用方注入（生产读环境变量，测试显式传入）。
fn validate_endpoint_with(endpoint: &str, allow_private: bool) -> Result<(), String> {
    let t = parse_endpoint(endpoint)?;
    if let Some(ip) = t.ip
        && !allow_private
        && !ip_is_public(ip)
    {
        return Err("endpoint 指向非公网地址".into());
    }
    Ok(())
}

fn allow_private() -> bool {
    std::env::var("OSS_OLAP_ALLOW_PRIVATE").is_ok_and(|v| v == "1")
}

/// 输入上限（环境可调；非法值回落默认）。
pub fn max_input_bytes() -> usize {
    std::env::var("OSS_OLAP_MAX_INPUT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(64 * 1024 * 1024)
}

/// 公网判定：loopback/私网/链路本地/ULA/组播/未指定/CGNAT/文档/保留段一律拒。
pub fn ip_is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4_is_public(v4),
        IpAddr::V6(v6) => {
            // IPv4-mapped（::ffff:127.0.0.1 等）按内嵌 IPv4 判。
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4_is_public(v4);
            }
            let seg = v6.segments();
            let ula = seg[0] & 0xfe00 == 0xfc00; // fc00::/7
            let link_local = seg[0] & 0xffc0 == 0xfe80; // fe80::/10
            let site_local = seg[0] & 0xffc0 == 0xfec0; // fec0::/10（废弃）
            let doc = seg[0] == 0x2001 && seg[1] == 0x0db8; // 2001:db8::/32
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || ula
                || link_local
                || site_local
                || doc)
        }
    }
}

fn v4_is_public(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    let cgnat = o[0] == 100 && (64..=127).contains(&o[1]); // 100.64.0.0/10
    let doc = (o[0] == 192 && o[1] == 0 && o[2] == 2)
        || (o[0] == 198 && (o[1] == 51 && o[2] == 100 || o[1] == 18 || o[1] == 19))
        || (o[0] == 203 && o[1] == 0 && o[2] == 113)
        || (o[0] == 192 && o[1] == 88 && o[2] == 99); // 6to4 relay
    !(v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_multicast()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || o[0] == 0
        || o[0] >= 240
        || cgnat
        || doc)
}

impl OlapTransform {
    async fn http_transform(&self, input: OlapInput<'_>) -> Result<OlapOutput, String> {
        let endpoint = self.endpoint.as_deref().ok_or("http 变换器缺 endpoint")?;
        let target = parse_endpoint(endpoint)?;
        let addrs = resolve_target(&target, self.allow_private).await?;

        let mut builder = reqwest::Client::builder()
            .no_proxy() // 环境代理不得绕过 DNS 钉扎
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(self.timeout)
            .gzip(true);
        if target.ip.is_none() {
            builder = builder.resolve_to_addrs(&target.host, &addrs);
        }
        let client = builder
            .build()
            .map_err(|e| format!("transform client: {e}"))?;

        let mut req = client
            .post(endpoint)
            .header("x-oss-bucket", input.bucket)
            .header("x-oss-size", input.bytes.len().to_string())
            .header(
                "x-oss-key-b64",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(input.key),
            )
            .body(input.bytes);
        if let Some(v) = input.version_id {
            req = req.header("x-oss-version-id", v);
        }
        if let Some(e) = input.etag {
            req = req.header("x-oss-etag", e);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("transform request: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("transform status {status}"));
        }
        // gzip 已由 reqwest 解压并从响应头移除；其余编码无法安全透出（会以原文
        // Content-Type 发出压缩字节），直接 500。
        if let Some(enc) = resp.headers().get(http::header::CONTENT_ENCODING) {
            let enc = enc.to_str().unwrap_or("");
            if !enc.eq_ignore_ascii_case("identity") {
                return Err(format!("transform content-encoding {enc} unsupported"));
            }
        }
        // Content-Length 是解压前值，只作快速拒绝；真正计数在解压后的流上。
        if let Some(len) = resp.content_length()
            && len > self.max_response_bytes as u64
        {
            return Err("transform response too large".into());
        }
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let mut out = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("transform body: {e}"))?;
            if out.len() + chunk.len() > self.max_response_bytes {
                return Err("transform response too large".into());
            }
            out.extend_from_slice(&chunk);
        }
        Ok(OlapOutput {
            bytes: out.into(),
            content_type,
        })
    }

    async fn wasm_transform(&self, input: OlapInput<'_>) -> Result<OlapOutput, String> {
        #[cfg(not(feature = "olap-wasm"))]
        {
            let _ = input;
            Err("wasm 变换器未启用".into())
        }
        #[cfg(feature = "olap-wasm")]
        {
            let module = self.module.clone().ok_or("wasm 模块缺失")?;
            let cap = self.max_response_bytes.min(MAX_WASM_OUTPUT_BYTES);
            let timeout = self.timeout;
            // epoch 心跳 50ms 一拍：deadline 按配置超时换算（配合 fuel 双保险）。
            let deadline = (timeout.as_millis() / 50 + 1) as u64;
            let run = tokio::task::spawn_blocking(move || {
                wasm_execute(&module, &input.bytes, cap, deadline)
            });
            match tokio::time::timeout(timeout, run).await {
                Err(_) => Err("wasm timeout".into()),
                Ok(Err(e)) => Err(format!("wasm task: {e}")),
                Ok(Ok(Ok(bytes))) => Ok(OlapOutput {
                    bytes,
                    content_type: None,
                }),
                Ok(Ok(Err(e))) => Err(e),
            }
        }
    }
}

/// 解析并校验目标地址；域名返回全部钉扎地址，混合记录中出现一个私网即整体拒。
async fn resolve_target(
    t: &EndpointTarget,
    pinned: Option<bool>,
) -> Result<Vec<SocketAddr>, String> {
    // 未固定时按请求时环境判定：撤销 OSS_OLAP_ALLOW_PRIVATE 即刻生效，无需 reload。
    let allow = pinned.unwrap_or_else(allow_private);
    let addrs: Vec<SocketAddr> = match t.ip {
        Some(ip) => vec![SocketAddr::new(ip, t.port)],
        None => tokio::net::lookup_host((t.host.as_str(), t.port))
            .await
            .map_err(|e| format!("transform DNS: {e}"))?
            .collect(),
    };
    if addrs.is_empty() {
        return Err("transform DNS 无结果".into());
    }
    if !allow {
        for a in &addrs {
            if !ip_is_public(a.ip()) {
                return Err("transform endpoint 指向非公网地址".into());
            }
        }
    }
    Ok(addrs)
}

// ---------------- WASM 变换器（可选 feature） ----------------

#[cfg(feature = "olap-wasm")]
fn wasm_engine() -> &'static wasmtime::Engine {
    static ENGINE: OnceLock<wasmtime::Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut cfg = wasmtime::Config::new();
        cfg.consume_fuel(true);
        cfg.epoch_interruption(true);
        cfg.wasm_multi_memory(false); // 只允许单 memory（StoreLimits 再兜底）
        let engine = wasmtime::Engine::new(&cfg).expect("wasm engine 配置恒定合法");
        // 全局 epoch 心跳：超时返回后残留的 spawn_blocking 任务也会在下一拍自行 Trap，
        // 不依赖调用方持有/中止每请求 ticker（否则会绕过并发闸继续占 CPU）。
        let tick = engine.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(50));
                tick.increment_epoch();
            }
        });
        engine
    })
}

#[cfg(feature = "olap-wasm")]
struct WasmLimits {
    limits: wasmtime::StoreLimits,
}

/// guest ABI：`memory` + `alloc(len)->ptr` + `dealloc(ptr,len)` + `transform(ptr,len)->i64`
/// （高 32 位 = 输出指针，低 32 位 = 输出长度）。无 WASI/导入。
#[cfg(feature = "olap-wasm")]
fn wasm_execute(
    module: &wasmtime::Module,
    input: &[u8],
    cap: usize,
    epoch_deadline: u64,
) -> Result<Bytes, String> {
    use wasmtime::{Linker, Store, StoreLimitsBuilder};

    let engine = wasm_engine();
    let limits = StoreLimitsBuilder::new()
        .memory_size(WASM_MEMORY_BYTES)
        .memories(1)
        .instances(1)
        .tables(1)
        .table_elements(WASM_TABLE_ELEMENTS)
        .build();
    let mut store = Store::new(engine, WasmLimits { limits });
    store.limiter(|s| &mut s.limits);
    store
        .set_fuel(WASM_FUEL)
        .map_err(|e| format!("wasm fuel: {e}"))?;
    store.set_epoch_deadline(epoch_deadline);
    // 空 Linker = 无任何导入（WASI/宿主函数不可见）。
    let linker = Linker::new(engine);
    let instance = linker
        .instantiate(&mut store, module)
        .map_err(|e| format!("wasm instantiate: {e}"))?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or("wasm 缺 memory 导出")?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .map_err(|e| format!("wasm 缺 alloc: {e}"))?;
    let dealloc = instance
        .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
        .map_err(|e| format!("wasm 缺 dealloc: {e}"))?;
    let transform = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, "transform")
        .map_err(|e| format!("wasm 缺 transform: {e}"))?;

    let in_len = i32::try_from(input.len()).map_err(|_| "wasm 输入过大")?;
    let in_ptr = alloc
        .call(&mut store, in_len)
        .map_err(|e| format!("wasm alloc: {e}"))?;
    let mem_size = memory.data_size(&store);
    let in_off = check_region(in_ptr, in_len, mem_size)?;
    memory
        .write(&mut store, in_off, input)
        .map_err(|e| format!("wasm 写入输入越界: {e}"))?;

    let packed = transform
        .call(&mut store, (in_ptr, in_len))
        .map_err(|e| format!("wasm transform: {e}"))?;
    let out_ptr = (packed >> 32) as u32 as i32;
    let out_len = packed as u32 as i32;
    if out_len < 0 {
        return Err("wasm 输出长度非法".into());
    }
    if out_len as usize > cap {
        return Err("wasm 输出超限".into());
    }
    let mem_size = memory.data_size(&store);
    let out_off = check_region(out_ptr, out_len, mem_size)?;
    // 输出不得与输入区重叠（alloc/dealloc 语义下输出应是独立分配）。
    if in_len > 0
        && out_len > 0
        && out_off < in_off.saturating_add(in_len as usize)
        && in_off < out_off.saturating_add(out_len as usize)
    {
        return Err("wasm 输出与输入重叠".into());
    }
    let mut out = vec![0u8; out_len as usize];
    memory
        .read(&store, out_off, &mut out)
        .map_err(|e| format!("wasm 读取输出越界: {e}"))?;
    // 释放失败不影响结果（guest 自身内存即将随 store 丢弃）。
    let _ = dealloc.call(&mut store, (out_ptr, out_len));
    Ok(out.into())
}

#[cfg(feature = "olap-wasm")]
fn check_region(ptr: i32, len: i32, mem_size: usize) -> Result<usize, String> {
    if ptr < 0 || len < 0 {
        return Err("wasm 指针/长度为负".into());
    }
    let off = ptr as usize;
    let end = off.checked_add(len as usize).ok_or("wasm 指针/长度溢出")?;
    if end > mem_size {
        return Err("wasm 指针/长度越界".into());
    }
    Ok(off)
}

// ---------------- 缓存 ----------------

/// 变换器缓存：AP 名 → (桶, 编译产物)。控制面写路径更新；路由读路径 miss = 无变换器。
/// 桶删除级联（与 `ApCache::invalidate_bucket` 同步调用）。
///
/// `loaded` 区分“全量配置尚未成功加载/加载失败”与“该 AP 无变换器”：前者 AP 请求必须
/// fail-closed（否则 DB 故障时会静默返回原文），由启动预加载/后续重载置位或清位。
#[derive(Default)]
pub struct OlapCache {
    inner: RwLock<HashMap<String, (String, Arc<OlapTransform>)>>,
    loaded: AtomicBool,
}

impl OlapCache {
    /// 新建缓存处于“未加载”态；加载器成功完成一次全量预加载后须调用 `mark_loaded`。
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded.load(Ordering::Acquire)
    }

    pub fn mark_loaded(&self) {
        self.loaded.store(true, Ordering::Release);
    }

    /// 全量预加载失败：无论此前是否加载过，都退回 fail-closed。
    pub fn mark_failed(&self) {
        self.loaded.store(false, Ordering::Release);
    }

    /// 读缓存。锁中毒视为整体不可用（fail-closed），不得降级成“该 AP 无变换器”。
    pub fn get(&self, ap: &str) -> Result<Option<Arc<OlapTransform>>, String> {
        match self.inner.read() {
            Ok(w) => Ok(w.get(ap).map(|(_, t)| Arc::clone(t))),
            Err(_) => {
                self.mark_failed();
                Err("transform cache poisoned".into())
            }
        }
    }

    pub fn put(&self, ap: &str, bucket: &str, transform: OlapTransform) {
        match self.inner.write() {
            Ok(mut w) => {
                w.insert(ap.to_string(), (bucket.to_string(), Arc::new(transform)));
            }
            Err(_) => self.mark_failed(),
        }
    }

    pub fn remove(&self, ap: &str) {
        match self.inner.write() {
            Ok(mut w) => {
                w.remove(ap);
            }
            Err(_) => self.mark_failed(),
        }
    }

    /// 快照安装后全量失效：先清空并回到未加载态，再由 preload 重新建立（fail-closed）。
    pub fn clear(&self) {
        match self.inner.write() {
            Ok(mut w) => {
                w.clear();
                self.loaded.store(false, Ordering::Release);
            }
            Err(_) => self.mark_failed(),
        }
    }

    pub fn invalidate_bucket(&self, bucket: &str) {
        match self.inner.write() {
            Ok(mut w) => w.retain(|_, (b, _)| b != bucket),
            Err(_) => self.mark_failed(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|w| w.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// SSRF 分类矩阵：内网/保留段全拒；公网样本放行；IPv4-mapped 按内嵌 IPv4 判。
    #[test]
    fn ip_classification_matrix() {
        for bad in [
            "127.0.0.1",
            "127.255.255.254",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.1.1",
            "0.0.0.0",
            "100.64.0.1",
            "100.127.255.255",
            "192.0.2.1",
            "198.51.100.7",
            "203.0.113.9",
            "198.18.0.1",
            "192.88.99.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:192.168.0.1",
        ] {
            assert!(!ip_is_public(ip(bad)), "{bad} 应判非公网");
        }
        for good in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700::1111"] {
            assert!(ip_is_public(ip(good)), "{good} 应判公网");
        }
    }

    #[test]
    fn endpoint_shape_validation() {
        // 形状合法（不解析 DNS）
        assert!(validate_endpoint_with("https://example.com/hook", false).is_ok());
        assert!(validate_endpoint_with("http://example.com:8080/hook?x=1", false).is_ok());
        for bad in [
            "ftp://example.com/x",
            "file:///etc/passwd",
            "https://user:pass@example.com/x",
            "https://user@example.com/x",
            "not-a-url",
        ] {
            assert!(validate_endpoint_with(bad, false).is_err(), "{bad}");
        }
        // IP 字面量保存即拒；显式注入 allow_private=false，不依赖进程环境变量。
        assert!(validate_endpoint_with("http://127.0.0.1:9/", false).is_err());
        assert!(validate_endpoint_with("http://[::ffff:127.0.0.1]:9/", false).is_err());
        assert!(validate_endpoint_with("http://127.0.0.1:9/", true).is_ok());
    }

    #[test]
    fn prepare_limits_and_kind() {
        assert!(OlapTransform::prepare("bogus", None, None, 5000, 1024).is_err());
        assert!(
            OlapTransform::prepare("http", Some("https://example.com/".into()), None, 999, 1024)
                .is_err()
        );
        assert!(
            OlapTransform::prepare("http", Some("https://example.com/".into()), None, 5000, 0)
                .is_err()
        );
        assert!(
            OlapTransform::prepare("wasm", None, Some(b"x"), 5000, 1024).is_err(),
            "wasm 缺模块"
        );
        #[cfg(not(feature = "olap-wasm"))]
        assert!(
            OlapTransform::prepare("wasm", None, Some(b"x"), 5000, 1024).is_err(),
            "无 feature 时 wasm 配置必须拒"
        );
    }

    #[test]
    fn cache_put_remove_bucket_invalidate() {
        let c = OlapCache::new();
        let mk = || {
            OlapTransform::prepare(
                "http",
                Some("https://example.com/".into()),
                None,
                5000,
                1024,
            )
            .unwrap()
        };
        c.put("a1", "b1", mk());
        c.put("a2", "b2", mk());
        assert_eq!(c.len(), 2);
        assert!(c.get("a1").unwrap().is_some());
        c.remove("a2");
        assert!(c.get("a2").unwrap().is_none());
        c.invalidate_bucket("b1");
        assert!(c.is_empty());
    }

    /// 锁中毒必须 fail-closed：get 返回 Err 并清掉 loaded，不能当作“无变换器”。
    #[test]
    fn cache_poison_fails_closed() {
        let c = OlapCache::new();
        c.mark_loaded();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = c.inner.write().unwrap();
            panic!("poison the lock");
        }));
        assert!(poisoned.is_err());
        assert!(c.get("anything").is_err());
        assert!(!c.is_loaded());
    }
}
