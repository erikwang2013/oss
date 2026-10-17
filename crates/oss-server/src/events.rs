//! M6 切片 0：事件总线——接收方实现（写 `event_outbox`）+ worker（R2 两态租约 /
//! R1 分级重试）+ `Notifier` 投递抽象（文件 sink 为调试/测试/本片生产替身；Kafka 切片 1）。
//!
//! 事件形状 = S3 通知（规格 §11.3）：eventVersion/eventName/eventTime/s3.bucket/
//! s3.object(key,size,eTag,versionId,sequencer)/source(host,port,userAgent)。
//! 发射是 post-commit 尽力而为：**任何失败只 warn，绝不影响 S3 请求结果**。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::Row as _;
use sqlx::SqlitePool;

use xl_s3::events::{EventSink, EventSource, S3EventOp};
use xl_s3::notify::EventTarget;

/// 事件 `awsRegion`（记档：本仓无 region 配置项，AWS 通知字段给固定默认）。
pub const EVENT_REGION: &str = "us-east-1";
/// 每轮 claim 上限。
const CLAIM_BATCH: i64 = 16;
/// 租约（R2/V2：事件 60s——rskafka produce 超时 30s < 60s）。
pub const EVENT_LEASE_SECS: i64 = 60;
/// 永久错死信上限（R1：仅 Permanent 且 attempts > 10）。
pub const MAX_ATTEMPTS: i64 = 10;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 退避：1s·2^attempts 封顶 300s。
pub fn backoff_secs(attempts: i64) -> i64 {
    (1i64 << attempts.clamp(1, 9)).min(300)
}

// ---------- Notifier（投递抽象） ----------

#[derive(Debug, Clone)]
pub struct NotifyFailure {
    /// true = 永久错（计 attempts、超限 dead）；false = 连接类（永不 dead，R1）。
    pub permanent: bool,
    pub message: String,
}

impl NotifyFailure {
    pub fn permanent(msg: impl Into<String>) -> Self {
        Self {
            permanent: true,
            message: msg.into(),
        }
    }
    pub fn retryable(msg: impl Into<String>) -> Self {
        Self {
            permanent: false,
            message: msg.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait Notifier: Send + Sync {
    /// 投递一条事件到 target（target = 桶配置 ARN 里的目标 id）。
    async fn send(&self, target: &str, payload: &str) -> Result<(), NotifyFailure>;
}

/// 文件 sink（调试/测试/本片替身）：每 target 一个追加日志 `<dir>/<target>.log`。
pub struct FileNotifier {
    pub dir: PathBuf,
}

impl FileNotifier {
    pub fn from_spec(spec: &str) -> Option<Self> {
        spec.strip_prefix("file:").map(|d| Self {
            dir: PathBuf::from(d),
        })
    }
}

#[async_trait::async_trait]
impl Notifier for FileNotifier {
    async fn send(&self, target: &str, payload: &str) -> Result<(), NotifyFailure> {
        use tokio::io::AsyncWriteExt as _;
        // P2-3 纵深防御：target 不得含路径分隔符（配置层已白名单，双保险）
        if target.is_empty() || target.contains(['/', '\\']) || target.contains("..") {
            return Err(NotifyFailure::permanent(format!(
                "unsafe target id for file sink: {target:?}"
            )));
        }
        // P3-6：目录缺失 = 配置问题——构造期建目录让真实 I/O 错误可见（不无限 pending）
        if let Err(e) = tokio::fs::create_dir_all(&self.dir).await {
            return Err(NotifyFailure::permanent(format!("file sink dir: {e}")));
        }
        let path = self.dir.join(format!("{target}.log"));
        // 其余 I/O 错归「连接类」（R1：盘满等人工处置期间保留 pending 比丢事件安全）
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| NotifyFailure::retryable(format!("file sink open: {e}")))?;
        f.write_all(payload.as_bytes())
            .await
            .map_err(|e| NotifyFailure::retryable(format!("file sink write: {e}")))?;
        f.write_all(b"\n")
            .await
            .map_err(|e| NotifyFailure::retryable(format!("file sink write: {e}")))
    }
}

/// 未配置任何后端：**永久错**（配置缺失须人工处置——与「未知默认重试」不同，
/// 这是显式的部署缺失；R1 记档口径）。
pub struct NoopNotifier;

#[async_trait::async_trait]
impl Notifier for NoopNotifier {
    async fn send(&self, _target: &str, _payload: &str) -> Result<(), NotifyFailure> {
        Err(NotifyFailure::permanent(
            "no event notifier configured (set OSS_EVENT_SINK=file:<dir> or Kafka targets)",
        ))
    }
}

/// 自 env 构造（切片 1 接 Kafka 后扩展）。
pub fn notifier_from_env() -> Arc<dyn Notifier> {
    if let Ok(spec) = std::env::var("OSS_EVENT_SINK")
        && let Some(f) = FileNotifier::from_spec(&spec)
    {
        return Arc::new(f);
    }
    Arc::new(NoopNotifier)
}

// ---------- 事件 JSON（S3 通知形状） ----------

/// AWS 对象键编码：`application/x-www-form-urlencoded`（space→`+`，其余非 unreserved → %XX）。
pub fn aws_encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn sequencer() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:032x}")
}

fn request_ids() -> (String, String) {
    use std::io::Read as _;
    let mut buf = [0u8; 24];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom")
        && f.read_exact(&mut buf).is_ok()
    {
    } else {
        buf = [7u8; 24];
    }
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    (hex(&buf[..12]), hex(&buf[12..]))
}

fn iso8601(secs: i64) -> String {
    use time::OffsetDateTime;
    let Ok(odt) = OffsetDateTime::from_unix_timestamp(secs) else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        odt.year(),
        u8::from(odt.month()),
        odt.day(),
        odt.hour(),
        odt.minute(),
        odt.second()
    )
}

/// 组装一条事件 JSON（target 的 configurationId 进 `s3.configurationId`）。
#[allow(clippy::too_many_arguments)] // 事件字段面 = 线格式，平铺直译
pub fn build_event_json(
    region: &str,
    event_name: &str,
    config_id: &str,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    size: Option<u64>,
    etag: Option<&str>,
    source: &EventSource,
    owner_uid: Option<i64>,
) -> String {
    let (rid, rid2) = request_ids();
    let principal = owner_uid.map(|u| u.to_string()).unwrap_or_default();
    serde_json::json!({
        "eventVersion": "2.1",
        "eventSource": "oss:s3",
        "awsRegion": region,
        "eventTime": iso8601(now()),
        "eventName": event_name,
        "userIdentity": { "principalId": principal },
        "responseElements": {
            "x-amz-request-id": rid,
            "x-amz-id-2": rid2,
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": config_id,
            "bucket": {
                "name": bucket,
                "ownerIdentity": { "principalId": principal },
                "arn": format!("arn:aws:s3:::{bucket}"),
            },
            "object": {
                "key": aws_encode_key(key),
                "size": size.unwrap_or(0),
                "eTag": etag.unwrap_or(""),
                "versionId": version_id.unwrap_or("null"),
                "sequencer": sequencer(),
            }
        },
        "source": {
            "host": source.host,
            "port": source.port,
            "userAgent": source.user_agent,
        }
    })
    .to_string()
}

// ---------- 接收方（EventSink 实现） ----------

pub struct SqlEventSink {
    pool: SqlitePool,
    region: String,
    notifier: Arc<dyn Notifier>,
}

impl SqlEventSink {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            region: EVENT_REGION.to_string(),
            notifier: notifier_from_env(),
        }
    }

    /// 测试/装配用：指定 notifier。
    pub fn with_notifier(pool: SqlitePool, notifier: Arc<dyn Notifier>) -> Self {
        Self {
            pool,
            region: EVENT_REGION.to_string(),
            notifier,
        }
    }

    pub fn notifier(&self) -> Arc<dyn Notifier> {
        Arc::clone(&self.notifier)
    }

    /// 按事件名发射（内部路径亦用：aether→Post / lifecycle→LifecycleExpiration:*）。
    /// 与桶通知配置匹配（事件名 + 前缀/后缀）→ 每命中目标写一行 outbox。失败只 warn。
    #[allow(clippy::too_many_arguments)] // 同 build_event_json
    pub async fn emit_named(
        &self,
        event_name: &str,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
        source: EventSource,
    ) {
        let targets = match self.load_targets(bucket).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, bucket, "读通知配置失败——事件丢弃（不影响请求）");
                return;
            }
        };
        if targets.is_empty() {
            return;
        }
        let owner_uid: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = ?")
                .bind(bucket)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        for t in targets {
            if !t.events.iter().any(|e| e == event_name) {
                continue;
            }
            if let Some(p) = &t.prefix
                && !key.starts_with(p)
            {
                continue;
            }
            if let Some(s) = &t.suffix
                && !key.ends_with(s)
            {
                continue;
            }
            let payload = build_event_json(
                &self.region,
                event_name,
                &t.id,
                bucket,
                key,
                version_id,
                size,
                etag,
                &source,
                owner_uid,
            );
            if let Err(e) = sqlx::query(
                "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, \
                 next_retry_at, created_at) VALUES (?, ?, ?, 'pending', 0, 0, ?)",
            )
            .bind(bucket)
            .bind(&t.target)
            .bind(&payload)
            .bind(now())
            .execute(&self.pool)
            .await
            {
                tracing::warn!(error = %e, bucket, key, "事件入 outbox 失败（丢弃本条，不影响请求）");
            }
        }
    }

    async fn load_targets(&self, bucket: &str) -> Result<Vec<EventTarget>, sqlx::Error> {
        let row = sqlx::query("SELECT config_json FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else { return Ok(Vec::new()) };
        let json: String = r.get("config_json");
        serde_json::from_str(&json).map_err(|e| sqlx::Error::Decode(Box::new(e)))
    }
}

#[async_trait::async_trait]
impl EventSink for SqlEventSink {
    async fn emit(
        &self,
        op: S3EventOp,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
        source: EventSource,
    ) {
        self.emit_named(op.event_name(), bucket, key, version_id, size, etag, source)
            .await;
    }
}

// ---------- worker（R2 两态租约 + R1 分级重试） ----------

/// P2-2 修复：每行发送前**续租**（claim token 轮换）。返回新 token；`None` = 行已被
/// 他实例回收（丢失 claim）→ 调用方**跳过发送**（防「回收后旧 sender 仍发」的重复）。
async fn renew_claim(pool: &SqlitePool, id: i64, old: Option<i64>) -> Option<i64> {
    let now_s = now();
    match sqlx::query(
        "UPDATE event_outbox SET claimed_at=? WHERE id=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(now_s)
    .bind(id)
    .bind(old)
    .execute(pool)
    .await
    {
        Ok(r) if r.rows_affected() == 1 => Some(now_s),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, id, "事件续租失败（跳过本轮）");
            None
        }
    }
}

/// 成功删行（claim token 守卫；stale sender 不得复活已被重投/删除的行）。
async fn finish_ok(pool: &SqlitePool, id: i64, token: Option<i64>) -> bool {
    match sqlx::query("DELETE FROM event_outbox WHERE id=? AND claimed_at IS ? AND state='sending'")
        .bind(id)
        .bind(token)
        .execute(pool)
        .await
    {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, id, "事件完成删行失败");
            false
        }
    }
}

/// 失败回写（重试 or 死信；同 guarded 口径）。
async fn requeue_or_bury(
    pool: &SqlitePool,
    id: i64,
    token: Option<i64>,
    attempts: i64,
    failure: &NotifyFailure,
    now_s: i64,
) -> bool {
    let bury = failure.permanent && attempts > MAX_ATTEMPTS;
    let res = if bury {
        sqlx::query(
            "UPDATE event_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
             WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(&failure.message)
        .bind(id)
        .bind(token)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE event_outbox SET state='pending', attempts=?, claimed_at=NULL, \
             next_retry_at=?, last_error=? WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(now_s + backoff_secs(attempts))
        .bind(&failure.message)
        .bind(id)
        .bind(token)
        .execute(pool)
        .await
    };
    match res {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, id, "事件失败状态回写失败");
            false
        }
    }
}

/// 一轮处理：回收过期租约 → claim（有序，逐条串行投递）→ 成功删行 / 分级重试。
/// 返回 `(成功, 失败)` 计数（测试与观测用）。
pub async fn process_once(pool: &SqlitePool, notifier: &dyn Notifier) -> (usize, usize) {
    let now_s = now();
    // R2 回收：state='sending' 且租约过期 → 置回 pending（不计 attempts；崩溃自愈）
    if let Err(e) = sqlx::query(
        "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
         WHERE state='sending' AND claimed_at < ?",
    )
    .bind(now_s)
    .bind(now_s - EVENT_LEASE_SECS)
    .execute(pool)
    .await
    {
        tracing::warn!(error = %e, "事件 outbox 租约回收失败");
        return (0, 0);
    }
    // claim（原子 UPDATE…RETURNING；ORDER BY id = seq 序，逐条串行投递）
    let rows = match sqlx::query(
        "UPDATE event_outbox SET state='sending', claimed_at=? \
         WHERE id IN (SELECT id FROM event_outbox WHERE state='pending' AND next_retry_at <= ? \
                      ORDER BY id LIMIT ?) \
         RETURNING id, target_id, event_json, attempts, claimed_at",
    )
    .bind(now_s)
    .bind(now_s)
    .bind(CLAIM_BATCH)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "事件 outbox claim 失败");
            return (0, 0);
        }
    };
    let (mut sent, mut failed) = (0usize, 0usize);
    for r in rows {
        let id: i64 = r.get("id");
        let target: String = r.get("target_id");
        let payload: String = r.get("event_json");
        let attempts: i64 = r.get("attempts");
        let claimed: Option<i64> = r.get("claimed_at");
        // P2-2：发送前续租（claim token 轮换）——丢失 claim 则跳过（他实例已接管）
        let Some(token) = renew_claim(pool, id, claimed).await else {
            continue;
        };
        match notifier.send(&target, &payload).await {
            Ok(()) => {
                if finish_ok(pool, id, Some(token)).await {
                    sent += 1;
                } else {
                    tracing::warn!(id, "事件完成时 claim token 不符（已被回收重投？）");
                }
            }
            Err(f) => {
                failed += 1;
                let a = attempts + 1;
                let wrote = requeue_or_bury(pool, id, Some(token), a, &f, now_s).await;
                if !wrote {
                    tracing::warn!(id, "事件失败回写时 claim token 不符（已被回收重投？）");
                }
                if f.permanent && a > MAX_ATTEMPTS {
                    tracing::error!(id, target, error = %f.message, "事件进入死信（永久错超限）");
                }
            }
        }
    }
    (sent, failed)
}

/// serve 挂载（`OSS_EVENT_INTERVAL_SECS` 默认 1s；显式 0 = 关闭）。
pub fn spawn_worker(sink: Arc<SqlEventSink>, pool: SqlitePool) {
    let secs: u64 = std::env::var("OSS_EVENT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if secs == 0 {
        tracing::info!("事件 worker 已关闭（OSS_EVENT_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let notifier = sink.notifier();
            process_once(&pool, notifier.as_ref()).await;
        }
    });
    tracing::info!(interval_secs = secs, "事件 worker 已启动");
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL);\
             CREATE TABLE event_configs (bucket TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE event_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, bucket TEXT NOT NULL, target_id TEXT NOT NULL, \
              event_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, \
              claimed_at INTEGER, next_retry_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0, last_error TEXT);",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('bkt', 7, 0)")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    async fn set_cfg(pool: &SqlitePool, targets: &str) {
        sqlx::query(
            "INSERT INTO event_configs (bucket, config_json) VALUES ('bkt', ?) \
             ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json",
        )
        .bind(targets)
        .execute(pool)
        .await
        .unwrap();
    }

    fn src() -> EventSource {
        EventSource {
            host: "127.0.0.1".into(),
            port: 9000,
            user_agent: "aws-cli/2.0".into(),
        }
    }

    /// 事件 JSON 形状（规格 §11.3 必需字段逐项）。
    #[test]
    fn event_json_shape() {
        let j = build_event_json(
            EVENT_REGION,
            "s3:ObjectCreated:Put",
            "cfg-0",
            "bkt",
            "dir/a b+c.txt",
            Some("v1"),
            Some(123),
            Some("abc"),
            &src(),
            Some(7),
        );
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["eventVersion"], "2.1");
        assert_eq!(v["eventSource"], "oss:s3");
        assert_eq!(v["eventName"], "s3:ObjectCreated:Put");
        assert!(v["eventTime"].as_str().unwrap().ends_with('Z'));
        assert_eq!(v["s3"]["bucket"]["name"], "bkt");
        assert_eq!(v["s3"]["bucket"]["arn"], "arn:aws:s3:::bkt");
        assert_eq!(v["s3"]["configurationId"], "cfg-0");
        assert_eq!(v["s3"]["object"]["key"], "dir%2Fa+b%2Bc.txt");
        assert_eq!(v["s3"]["object"]["size"], 123);
        assert_eq!(v["s3"]["object"]["eTag"], "abc");
        assert_eq!(v["s3"]["object"]["versionId"], "v1");
        assert!(!v["s3"]["object"]["sequencer"].as_str().unwrap().is_empty());
        assert_eq!(v["source"]["host"], "127.0.0.1");
        assert_eq!(v["source"]["port"], 9000);
        assert_eq!(v["source"]["userAgent"], "aws-cli/2.0");
    }

    /// 匹配矩阵：事件名白名单 + 前缀/后缀过滤；未匹配零入队。
    #[tokio::test]
    async fn emit_matching_matrix() {
        let pool = pool().await;
        set_cfg(
            &pool,
            r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":"logs/","suffix":null},
                {"id":"b","target":"t2","events":["s3:ObjectCreated:Put","s3:ObjectRemoved:Delete"],"prefix":null,"suffix":".csv"}]"#,
        )
        .await;
        let sink = SqlEventSink::with_notifier(pool.clone(), Arc::new(NoopNotifier));
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "logs/x.txt",
            Some("v1"),
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "logs/y.csv",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        sink.emit(
            S3EventOp::RemovedDelete,
            "bkt",
            "z.csv",
            None,
            None,
            None,
            src(),
        )
        .await;
        sink.emit(
            S3EventOp::CreatedCopy,
            "bkt",
            "logs/z.bin",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        let rows = sqlx::query("SELECT target_id FROM event_outbox ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
        let targets: Vec<String> = rows.iter().map(|r| r.get("target_id")).collect();
        // logs/x.txt → t1；logs/y.csv → t1+t2；z.csv → t2（删除事件 t2 白名单）；Copy 不在任何名单
        assert_eq!(targets, vec!["t1", "t1", "t2", "t2"]);
    }

    /// R1：永久错 11 次 → dead；连接类 30 次仍 pending（永不 dead）。
    struct ScriptedNotifier {
        permanent: bool,
    }
    #[async_trait::async_trait]
    impl Notifier for ScriptedNotifier {
        async fn send(&self, _t: &str, _p: &str) -> Result<(), NotifyFailure> {
            if self.permanent {
                Err(NotifyFailure::permanent("auth failed"))
            } else {
                Err(NotifyFailure::retryable("connection refused"))
            }
        }
    }

    #[tokio::test]
    async fn retry_classification() {
        let pool = pool().await;
        set_cfg(&pool, r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#).await;
        let sink = SqlEventSink::with_notifier(pool.clone(), Arc::new(NoopNotifier));
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;

        // 永久错：催熟退避后每轮跑——11 次后 dead
        let pn = ScriptedNotifier { permanent: true };
        for _ in 0..12 {
            sqlx::query("UPDATE event_outbox SET next_retry_at = 0 WHERE state='pending'")
                .execute(&pool)
                .await
                .unwrap();
            process_once(&pool, &pn).await;
        }
        let (state, attempts): (String, i64) =
            sqlx::query_as("SELECT state, attempts FROM event_outbox")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((state.as_str(), attempts), ("dead", 11));

        // 连接类：再来 30 轮仍 pending、attempts 增长但永不 dead
        sqlx::query("DELETE FROM event_outbox")
            .execute(&pool)
            .await
            .unwrap();
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k2",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        let cn = ScriptedNotifier { permanent: false };
        for _ in 0..30 {
            sqlx::query("UPDATE event_outbox SET next_retry_at = 0 WHERE state='pending'")
                .execute(&pool)
                .await
                .unwrap();
            process_once(&pool, &cn).await;
        }
        let state: String = sqlx::query_scalar("SELECT state FROM event_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "pending", "连接类错永不 dead");
    }

    /// R2：claim 后崩溃（state='sending' 租约过期）→ 回收自愈 → 重投成功；回收不计 attempts。
    #[tokio::test]
    async fn lease_recovery_after_crash() {
        let pool = pool().await;
        set_cfg(&pool, r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#).await;
        let sink = SqlEventSink::with_notifier(pool.clone(), Arc::new(NoopNotifier));
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        // 模拟：claim 后进程死掉（sending + claimed_at 陈旧）
        sqlx::query("UPDATE event_outbox SET state='sending', claimed_at = ?")
            .bind(now() - EVENT_LEASE_SECS - 1)
            .execute(&pool)
            .await
            .unwrap();
        struct OkNotifier;
        #[async_trait::async_trait]
        impl Notifier for OkNotifier {
            async fn send(&self, _t: &str, _p: &str) -> Result<(), NotifyFailure> {
                Ok(())
            }
        }
        let (sent, _) = process_once(&pool, &OkNotifier).await;
        assert_eq!(sent, 1, "回收后应重投成功");
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0, "成功应删行");
    }

    /// P2-4：claim token 守卫——token 轮换/错 token 的完成与回写必须 no-op；
    /// 回收不计 attempts（崩溃不是失败）。
    #[tokio::test]
    async fn claim_token_guards_and_recovery_semantics() {
        let pool = pool().await;
        set_cfg(&pool, r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#).await;
        let sink = SqlEventSink::with_notifier(pool.clone(), Arc::new(NoopNotifier));
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        let id: i64 = sqlx::query_scalar("SELECT id FROM event_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        // 置为 sending + 旧 token
        let t0 = now() - 5;
        sqlx::query("UPDATE event_outbox SET state='sending', claimed_at=? WHERE id=?")
            .bind(t0)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        // 续租：正确 token → 新 token；旧 token 再来 → 丢失（None）
        let t1 = renew_claim(&pool, id, Some(t0)).await.expect("续租应成功");
        assert!(t1 > t0);
        assert!(
            renew_claim(&pool, id, Some(t0)).await.is_none(),
            "旧 token 不得再续"
        );
        // 旧 token 的完成/回写必须 no-op（stale sender 不得复活/删行）
        assert!(!finish_ok(&pool, id, Some(t0)).await, "旧 token 不得删行");
        let f = NotifyFailure::permanent("denied");
        assert!(
            !requeue_or_bury(&pool, id, Some(t0), 2, &f, now()).await,
            "旧 token 不得回写"
        );
        let (state, attempts): (String, i64) =
            sqlx::query_as("SELECT state, attempts FROM event_outbox WHERE id=?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((state.as_str(), attempts), ("sending", 0), "no-op 后行不变");
        // 正确 token 完成 → 删行
        assert!(finish_ok(&pool, id, Some(t1)).await);
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    /// P2-4 复查补（第三轮）：租约回收**经 process_once 真路径**不计 attempts——崩溃行
    /// （sending、attempts=3、租约过期）→ 回收 → claim → 投递失败一次（连接类）→
    /// 终态必须是 `pending, attempts=4`（3+1；若回收误计则为 5）。
    #[tokio::test]
    async fn recovery_does_not_count_attempts() {
        let pool = pool().await;
        set_cfg(&pool, r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#).await;
        let sink = SqlEventSink::with_notifier(pool.clone(), Arc::new(NoopNotifier));
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        sqlx::query("UPDATE event_outbox SET state='sending', claimed_at=?, attempts=3")
            .bind(now() - EVENT_LEASE_SECS - 5)
            .execute(&pool)
            .await
            .unwrap();
        struct FailOnce;
        #[async_trait::async_trait]
        impl Notifier for FailOnce {
            async fn send(&self, _t: &str, _p: &str) -> Result<(), NotifyFailure> {
                Err(NotifyFailure::retryable("connection refused"))
            }
        }
        process_once(&pool, &FailOnce).await;
        let (state, attempts): (String, i64) =
            sqlx::query_as("SELECT state, attempts FROM event_outbox")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            (state.as_str(), attempts),
            ("pending", 4),
            "回收不计 attempts；仅投递失败一次 +1（3+1=4）"
        );
    }

    /// P2-3 纵深防御：FileNotifier 拒绝含路径分隔符的 target。
    #[tokio::test]
    async fn file_notifier_rejects_unsafe_target() {
        let td = tempfile::tempdir().unwrap();
        let f = FileNotifier {
            dir: td.path().to_path_buf(),
        };
        for bad in ["../evil", "a/b", "a\\b", ".."] {
            let e = f.send(bad, "{}").await.unwrap_err();
            assert!(e.permanent, "{bad}");
        }
        // 正常 id → 建目录并写入
        let nested = FileNotifier {
            dir: td.path().join("sub/dir"),
        };
        nested.send("t1", "{}").await.unwrap();
        assert!(td.path().join("sub/dir/t1.log").exists());
    }

    /// 文件 sink 端到端：payload 落在 `<dir>/<target>.log`。
    #[tokio::test]
    async fn file_notifier_writes() {
        let pool = pool().await;
        set_cfg(&pool, r#"[{"id":"a","target":"kv1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#).await;
        let td = tempfile::tempdir().unwrap();
        let sink = SqlEventSink::with_notifier(
            pool.clone(),
            Arc::new(FileNotifier {
                dir: td.path().to_path_buf(),
            }),
        );
        sink.emit(
            S3EventOp::CreatedPut,
            "bkt",
            "k",
            None,
            Some(1),
            Some("e"),
            src(),
        )
        .await;
        let (sent, _) = process_once(&pool, sink.notifier().as_ref()).await;
        assert_eq!(sent, 1);
        let body = std::fs::read_to_string(td.path().join("kv1.log")).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
        assert_eq!(v["eventName"], "s3:ObjectCreated:Put");
    }
}
