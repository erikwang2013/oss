//! 有界异步审计队列：S3 请求路径只 `try_send`，单 worker 批量落 `audit_log`。
//!
//! 审计是 best-effort：队列满/worker 未启动（测试、运维命令）时丢弃并累计
//! `dropped`，绝不阻塞或失败请求。worker 只在 serve 路径挂载。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use xl_s3::access::AuditSink;

/// 有界队列容量（4096 行 × 数百字节，过载时按丢弃处理）。
pub const QUEUE_CAPACITY: usize = 4096;
/// 单批上限（SQLite 单事务逐行插入；256 行约数百 µs）。
const BATCH_MAX: usize = 256;
/// 凑批窗口：首行到达后最多等这么久，低流量时审计延迟不超过该值。
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub struct AuditEntry {
    ts: i64,
    uid: i64,
    op: String,
    bucket: Option<String>,
    object_key: Option<String>,
    decision: String,
    detail: Option<String>,
}

pub struct AuditQueue {
    tx: mpsc::Sender<AuditEntry>,
    /// serve 路径取走并交给 [`spawn_worker`]；未取走时随队列一起丢弃。
    rx: std::sync::Mutex<Option<mpsc::Receiver<AuditEntry>>>,
    dropped: AtomicU64,
}

impl AuditQueue {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        Arc::new(Self {
            tx,
            rx: std::sync::Mutex::new(Some(rx)),
            dropped: AtomicU64::new(0),
        })
    }

    /// serve 路径专用：取走接收端去挂 worker（重复调用返回 None）。
    pub fn take_rx(&self) -> Option<mpsc::Receiver<AuditEntry>> {
        self.rx.lock().ok()?.take()
    }

    /// 已丢弃行数（队列满/已关闭；供测试与指标）。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl AuditSink for AuditQueue {
    fn enqueue(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> bool {
        let entry = AuditEntry {
            ts: now_secs(),
            uid,
            op: op.to_string(),
            bucket: bucket.map(str::to_string),
            object_key: object_key.map(str::to_string),
            decision: decision.to_string(),
            detail: detail.map(str::to_string),
        };
        match self.tx.try_send(entry) {
            Ok(()) => true,
            Err(_) => {
                // 首次 + 每 1000 条告警一次：过载时逐条 warn 反而放大故障。
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 || n.is_multiple_of(1000) {
                    tracing::warn!(dropped = n, "审计队列已满或已关闭，审计行被丢弃");
                }
                false
            }
        }
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 批量写 worker（serve 路径且仅调用一次）。
pub fn spawn_worker(db: Arc<crate::db::Db>, mut rx: mpsc::Receiver<AuditEntry>) {
    tokio::spawn(async move {
        let mut batch = Vec::with_capacity(BATCH_MAX);
        loop {
            let Some(first) = rx.recv().await else {
                return;
            };
            batch.push(first);
            let deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
            let mut closed = false;
            while batch.len() < BATCH_MAX {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(e)) => batch.push(e),
                    Ok(None) => {
                        closed = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            flush(&db, &mut batch).await;
            if closed {
                return;
            }
        }
    });
}

async fn flush(db: &crate::db::Db, batch: &mut Vec<AuditEntry>) {
    if batch.is_empty() {
        return;
    }
    // 对象模式不重试：审计行已写入本地日志，重放会重复。失败则放弃该批
    //（审计不得反压请求）。
    if let Err(e) = insert_batch(db, batch).await {
        tracing::warn!(error = %e, rows = batch.len(), "审计批量写入失败，丢弃该批");
    }
    batch.clear();
}

async fn insert_batch(db: &crate::db::Db, rows: &[AuditEntry]) -> crate::db::Result<()> {
    use xl_store::control_store::audit::{AuditTxn, NewAuditRecord};

    let records: Vec<NewAuditRecord> = rows
        .iter()
        .map(|r| NewAuditRecord {
            ts: r.ts,
            uid: r.uid,
            op: r.op.clone(),
            bucket: r.bucket.clone(),
            object_key: r.object_key.clone(),
            decision: r.decision.clone(),
            detail: r.detail.clone(),
        })
        .collect();
    db.object_local_write("audit", move |txn| {
        Box::pin(async move {
            txn.audit_insert_batch(&records).await?;
            Ok(())
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 满队列立即丢弃并累计计数（不阻塞、不 panic）。
    #[tokio::test]
    async fn full_queue_drops_and_counts() {
        let q = AuditQueue::new(1);
        assert!(q.enqueue(1, "GetObject", Some("b"), Some("k"), "allow", None));
        assert!(!q.enqueue(1, "GetObject", Some("b"), Some("k"), "allow", None));
        assert_eq!(q.dropped(), 1);
    }

    /// worker 成批落对象控制面的本地审计日志。
    #[tokio::test]
    async fn worker_writes_batched_rows() {
        use xl_store::control_state::root::Mode;
        use xl_store::store::{Store, StoreConfig};
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = StoreConfig::new(dir.path());
        cfg.fsync = false;
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let object = crate::control_backend::ObjectControl::open(store, Mode::Standalone, None)
            .await
            .unwrap();
        let db = Arc::new(crate::db::Db::connect_object().await.unwrap());
        db.attach_object_control(object).unwrap();

        let q = AuditQueue::new(QUEUE_CAPACITY);
        let rx = q.take_rx().expect("rx 只能取一次");
        spawn_worker(db.clone(), rx);
        assert!(q.enqueue(7, "PutObject", Some("b"), Some("k"), "allow", Some("d")));
        assert!(q.enqueue(0, "GetObject", Some("b"), Some("k"), "deny", None));

        let query = xl_store::control_store::audit::AuditQuery {
            limit: 10,
            ..Default::default()
        };
        let mut rows = Vec::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            rows = db.control_store().audit_page(&query).await.unwrap();
            if rows.len() == 2 {
                break;
            }
        }
        assert_eq!(rows.len(), 2, "worker 应把两条审计行批量写入");
        let allow = rows
            .iter()
            .find(|r| r.decision == "allow")
            .expect("allow row");
        assert_eq!(allow.uid, 7);
    }
}
