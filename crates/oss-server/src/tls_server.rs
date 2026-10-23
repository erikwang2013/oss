//! 可热加载的主 HTTP 监听器（TLS）。
//!
//! ecat 7.0.0 的 `HttpServer` 在 `start()` 时构建一次 `TlsAcceptor` 且没有替换入口
//! （ecat-transport-http 的 `tls_listener` 是私有模块），无法在不改上游的情况下热加载。
//! 这里实现 [`ecat_transport::Server`] 并复用 ecat 的既有语义：后台 accept 循环、
//! 10s 握手超时、1024 并发握手上限、断开日志限频、`/metrics` 合并与优雅停机；
//! 唯一差异是每个新连接握手时从 [`Tls`] 取当前配置快照（旧连接保持旧配置直到结束）。

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::serve::ListenerExt as _;
use ecat_transport::Server as TransportServer;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use crate::tls::Tls;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONCURRENT_HANDSHAKES: usize = 1024;
const DROP_LOG_WINDOW: Duration = Duration::from_secs(10);

pub struct ReloadableHttpServer {
    addr: String,
    router: Router,
    tls: Arc<Tls>,
    shutdown_tx: Mutex<Option<watch::Sender<()>>>,
}

impl ReloadableHttpServer {
    pub fn new(addr: impl Into<String>, router: Router, tls: Arc<Tls>) -> Self {
        Self {
            addr: ecat_transport::normalize_addr(addr.into()),
            router,
            tls,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// 与 ecat `HttpServer` 相同的 `/metrics` 合并语义：用户 router 也定义该路径时
    /// merge 会 panic，此时记录 warn 并降级为用户 router。
    fn merged_router(&self) -> Router {
        let user = self.router.clone();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            user.clone().merge(ecat_metrics::metrics_router())
        })) {
            Ok(merged) => merged,
            Err(_) => {
                tracing::warn!(
                    "user router defines /metrics; serving user route, framework metrics disabled"
                );
                user
            }
        }
    }
}

#[async_trait::async_trait]
impl TransportServer for ReloadableHttpServer {
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 启动即构建当前配置：物料不合法时与 ecat 一样启动失败，不假活。
        self.tls.server_config()?;
        let router = self.merged_router();
        let listener = TcpListener::bind(&self.addr).await?;
        let (tx, mut rx) = watch::channel(());
        *self.shutdown_tx.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
        let shutdown_signal = async move {
            let _ = rx.changed().await;
        };
        let tls_listener = TlsListener::new(listener, Arc::clone(&self.tls));
        // ConnectInfo 注入直连对端地址：AP 转发头信任必须以实际 TCP 对端为准
        // （tap_io 只为让 axum 的 Connected 实现把对端地址带进请求扩展）。
        axum::serve(
            tls_listener.tap_io(|_| {}),
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal)
        .await?;
        Ok(())
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(tx) = self
            .shutdown_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = tx.send(());
        }
        Ok(())
    }
}

type TlsStream = tokio_rustls::server::TlsStream<TcpStream>;

/// axum::serve::Listener：TCP 由后台 accept 循环接收，握手在各连接自己的任务中
/// 异步完成（与 ecat 相同的 DoS 防护）；accept() 只从通道取已握手连接。
struct TlsListener {
    rx: mpsc::Receiver<(std::io::Result<TlsStream>, SocketAddr)>,
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<()>,
}

impl TlsListener {
    fn new(listener: TcpListener, tls: Arc<Tls>) -> Self {
        let local_addr = listener.local_addr().expect("listener has local addr");
        let (tx, rx) = mpsc::channel::<(std::io::Result<TlsStream>, SocketAddr)>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        tokio::spawn(accept_loop(
            listener,
            tls,
            tx,
            shutdown_rx,
            Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_HANDSHAKES)),
            Arc::new(Mutex::new(DropLimiter::new())),
        ));
        Self {
            rx,
            local_addr,
            shutdown_tx,
        }
    }
}

/// 后台 accept 循环：不参与握手，只把每连接派给独立任务。
/// 与 ecat 的差异：acceptor 在每次 accept 时由 [`Tls::server_config`] 现取，
/// 因此热加载对之后的新连接立即生效。
async fn accept_loop(
    listener: TcpListener,
    tls: Arc<Tls>,
    tx: mpsc::Sender<(std::io::Result<TlsStream>, SocketAddr)>,
    mut shutdown_rx: watch::Receiver<()>,
    semaphore: Arc<tokio::sync::Semaphore>,
    limiter: Arc<Mutex<DropLimiter>>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            res = listener.accept() => {
                let (stream, addr) = match res {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error = %e, "tcp accept failed");
                        continue;
                    }
                };
                // 无可用许可直接断开：不排队、不堆积握手任务；日志按窗口限频。
                let Some(permit) = semaphore.clone().try_acquire_owned().ok() else {
                    let dropped = limiter
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .record();
                    if let Some(dropped) = dropped {
                        tracing::warn!(
                            addr = %addr,
                            dropped,
                            "tls handshake concurrency limit reached; dropping connection"
                        );
                    }
                    continue;
                };
                // 快照当前配置；热加载失败时 Tls 保留旧快照，因此正常情况下不会失败。
                // 取不到配置（理论上不可达）时 fail closed：断开连接，不退回明文。
                let acceptor = match tls.server_config() {
                    Ok(config) => tokio_rustls::TlsAcceptor::from(config),
                    Err(e) => {
                        tracing::error!(error = %e, "TLS 服务端配置不可用，断开新连接");
                        continue;
                    }
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let result = match tokio::time::timeout(
                        HANDSHAKE_TIMEOUT,
                        acceptor.accept(stream),
                    )
                    .await
                    {
                        Ok(Ok(tls)) => Ok(tls),
                        Ok(Err(e)) => Err(e),
                        Err(_) => {
                            tracing::warn!(
                                addr = %addr,
                                timeout_secs = HANDSHAKE_TIMEOUT.as_secs(),
                                "tls handshake timed out"
                            );
                            Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "tls handshake timed out",
                            ))
                        }
                    };
                    let _ = tx.send((result, addr)).await;
                });
            }
        }
    }
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (result, addr) = match self.rx.recv().await {
                Some(pair) => pair,
                // accept 循环异常退出：挂起而非 panic，在途连接与优雅停机照常处理。
                None => {
                    tracing::error!("tls accept loop exited unexpectedly; listener is dead");
                    std::future::pending::<(std::io::Result<Self::Io>, Self::Addr)>().await
                }
            };
            match result {
                Ok(tls) => return (tls, addr),
                Err(e) => tracing::warn!(error = %e, "tls handshake failed"),
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

/// 握手断开日志限频器（与 ecat 相同）：窗口内首次立即记，其余静默累计，
/// 窗口结束补记累计数。仅 accept 循环单任务访问。
struct DropLimiter {
    last_log: Instant,
    dropped: u64,
    first: bool,
}

impl DropLimiter {
    fn new() -> Self {
        Self {
            last_log: Instant::now(),
            dropped: 0,
            first: true,
        }
    }

    fn record(&mut self) -> Option<u64> {
        self.dropped += 1;
        if self.last_log.elapsed() >= DROP_LOG_WINDOW {
            let n = self.dropped;
            self.dropped = 0;
            self.last_log = Instant::now();
            self.first = true;
            Some(n)
        } else if self.first {
            self.first = false;
            self.dropped = 0;
            Some(1)
        } else {
            None
        }
    }
}

/// 物料热加载后台任务：`interval_secs` 0 = 关闭轮询，只响应 SIGHUP。
/// 两个来源共用 1s 节流（SIGHUP 洪水合并），内容哈希不变不重建。
pub fn spawn_reload_watchers(tls: Arc<Tls>, interval_secs: u64) {
    if !tls.has_material() {
        return;
    }
    if interval_secs > 0 {
        let tls = Arc::clone(&tls);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // 启动时刚加载过，跳过立即 tick。
            loop {
                tick.tick().await;
                log_reload(&tls, "poll");
            }
        });
    }
    #[cfg(unix)]
    {
        tokio::spawn(async move {
            let mut hup =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "注册 SIGHUP 失败；TLS 物料仅按轮询热加载");
                        return;
                    }
                };
            while hup.recv().await.is_some() {
                log_reload(&tls, "sighup");
            }
        });
    }
}

fn log_reload(tls: &Tls, source: &str) {
    match tls.reload_throttled() {
        // 1s 内的重复触发合并（SIGHUP 洪水/轮询撞车）。
        None => {}
        Some(Ok(false)) => tracing::debug!(source, "TLS 物料未变化"),
        Some(Ok(true)) => {}
        Some(Err(e)) => {
            tracing::error!(source, error = %e, "TLS 热加载失败，继续使用旧物料")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_limiter_aggregates_within_window() {
        let mut limiter = DropLimiter::new();
        assert_eq!(limiter.record(), Some(1), "first drop logs immediately");
        assert_eq!(limiter.record(), None);
        assert_eq!(limiter.record(), None);
        limiter.last_log = Instant::now() - DROP_LOG_WINDOW;
        assert_eq!(
            limiter.record(),
            Some(3),
            "window end logs cumulative count"
        );
    }
}
