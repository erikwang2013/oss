//! M6 切片 1：Kafka 真 broker e2e（redpanda docker；`tests/e2e_kafka.sh` 编排三阶段）。
//!
//! 阶段由 env 驱动，缺 env 即跳过（默认测试轮零依赖）：
//! - `OSS_KAFKA_E2E_BROKERS`   broker 地址（脚本动态端口）
//! - `OSS_KAFKA_E2E_TOPIC`     主题
//! - `OSS_KAFKA_E2E_DB`        跨阶段共享的 SQLite 文件（outbox 必须跨进程存活）
//!
//! 阶段由测试函数名选择（phase1/2/3），脚本逐个调起。
//!
//! 验收对映：① 消费者收到 ObjectCreated 且字段正确；② 停机期间上传（事件入 outbox）恢复后
//! 全部补投、不丢不重（稳态无重复——键集合断言）。

use std::sync::Arc;

use oss_server::config::KafkaTarget;
use oss_server::events::{KafkaNotifier, Notifier, SqlEventSink, process_once};
use sqlx::SqlitePool;
use xl_s3::events::{EventSink, EventSource, S3EventOp};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

struct Ctx {
    pool: SqlitePool,
    sink: Arc<SqlEventSink>,
    brokers: Vec<String>,
    topic: String,
}

async fn ctx() -> Option<Ctx> {
    let broker = env("OSS_KAFKA_E2E_BROKERS")?;
    let topic = env("OSS_KAFKA_E2E_TOPIC")?;
    let db = env("OSS_KAFKA_E2E_DB")?;
    let pool = SqlitePool::connect(&format!("sqlite://{db}?mode=rwc"))
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL);\
         CREATE TABLE IF NOT EXISTS event_configs (bucket TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE IF NOT EXISTS event_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, bucket TEXT NOT NULL, target_id TEXT NOT NULL, \
          event_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, \
          claimed_at INTEGER, next_retry_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0, last_error TEXT);",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO buckets (name, owner_id, creation_date) VALUES ('kbk', 1, 0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR REPLACE INTO event_configs (bucket, config_json) VALUES ('kbk', ?)",
    )
    .bind(r#"[{"id":"a","target":"t1","events":["s3:ObjectCreated:Put"],"prefix":null,"suffix":null}]"#)
    .execute(&pool)
    .await
    .unwrap();
    let targets = vec![KafkaTarget {
        id: "t1".into(),
        brokers: vec![broker.clone()],
        topic: topic.clone(),
        tls: false,
        sasl: None,
    }];
    let notifier: Arc<dyn Notifier> = Arc::new(KafkaNotifier::new(&targets));
    let sink = Arc::new(SqlEventSink::new(pool.clone(), notifier));
    Some(Ctx {
        pool,
        sink,
        brokers: vec![broker],
        topic,
    })
}

fn src() -> EventSource {
    EventSource {
        host: "127.0.0.1".into(),
        port: 9000,
        user_agent: "e2e-kafka".into(),
    }
}

async fn emit(ctx: &Ctx, key: &str) {
    ctx.sink
        .emit(
            S3EventOp::CreatedPut,
            "kbk",
            key,
            Some("v1"),
            Some(1),
            Some("etag"),
            src(),
        )
        .await;
}

async fn client(ctx: &Ctx) -> rskafka::client::Client {
    rskafka::client::ClientBuilder::new(ctx.brokers.clone())
        .build()
        .await
        .unwrap()
}

async fn ensure_topic(ctx: &Ctx) {
    let c = client(ctx).await;
    let ctrl = c.controller_client().unwrap();
    let _ = ctrl.create_topic(ctx.topic.clone(), 1, 1, 5_000).await; // TopicAlreadyExists 容忍
}

async fn outbox_counts(pool: &SqlitePool) -> (i64, i64) {
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_outbox WHERE state IN ('pending','sending')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox WHERE state='dead'")
        .fetch_one(pool)
        .await
        .unwrap();
    (pending, dead)
}

/// 消费全量（从 offset 0 起），返回 (键列表, 载荷列表)。
async fn consume_all(ctx: &Ctx) -> (Vec<String>, Vec<String>) {
    use rskafka::client::partition::UnknownTopicHandling;
    let c = client(ctx).await;
    let pc = c
        .partition_client(ctx.topic.clone(), 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let mut keys = Vec::new();
    let mut payloads = Vec::new();
    let mut offset = 0i64;
    loop {
        let (recs, hw) = pc.fetch_records(offset, 0..(1 << 24), 2_000).await.unwrap();
        if recs.is_empty() {
            if offset >= hw {
                break;
            }
            continue;
        }
        for r in &recs {
            let v: serde_json::Value =
                serde_json::from_slice(r.record.value.as_deref().expect("事件载荷")).unwrap();
            keys.push(v["s3"]["object"]["key"].as_str().unwrap().to_string());
            payloads.push(String::from_utf8(r.record.value.clone().unwrap()).unwrap());
            offset = r.offset + 1;
        }
    }
    (keys, payloads)
}

/// 阶段 1（broker 在线）：单事件投递 → 消费者实收 → 字段逐项断言（验收 ①）。
#[tokio::test]
async fn phase1_broker_up_delivers() {
    let Some(ctx) = ctx().await else {
        eprintln!("skip: OSS_KAFKA_E2E_* 未设置");
        return;
    };
    ensure_topic(&ctx).await;
    emit(&ctx, "p1-0.json").await;
    // 有界重取：worker 轮询直到 outbox 清空（≤60s）
    for _ in 0..60 {
        process_once(&ctx.pool, ctx.sink.notifier().as_ref()).await;
        let (pending, _) = outbox_counts(&ctx.pool).await;
        if pending == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let (pending, dead) = outbox_counts(&ctx.pool).await;
    assert_eq!((pending, dead), (0, 0), "阶段 1 应全部投递成功");
    let (keys, payloads) = consume_all(&ctx).await;
    assert!(keys.contains(&"p1-0.json".to_string()), "{keys:?}");
    let v: serde_json::Value =
        serde_json::from_str(payloads.iter().find(|p| p.contains("p1-0.json")).unwrap()).unwrap();
    assert_eq!(v["eventVersion"], "2.1");
    assert_eq!(v["eventSource"], "oss:s3");
    assert_eq!(v["eventName"], "s3:ObjectCreated:Put");
    assert_eq!(v["s3"]["bucket"]["name"], "kbk");
    assert_eq!(v["s3"]["object"]["versionId"], "v1");
    assert_eq!(v["source"]["userAgent"], "e2e-kafka");
    println!("phase1 ✓ 实收 {} 条，字段断言通过", keys.len());
}

/// 阶段 2（broker 停机）：事件入 outbox 保持 pending（连接类错误永不 dead）。
#[tokio::test]
async fn phase2_broker_down_buffers() {
    let Some(ctx) = ctx().await else {
        eprintln!("skip");
        return;
    };
    for i in 0..5 {
        emit(&ctx, &format!("p2-{i}.json")).await;
    }
    // broker 已停：投递必失败；有界执行（连接失败可能慢于一次 RPC 超时）
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        process_once(&ctx.pool, ctx.sink.notifier().as_ref()),
    )
    .await;
    let (pending, dead) = outbox_counts(&ctx.pool).await;
    assert!(
        pending >= 5,
        "停机期 5 条必须积压 pending（实得 {pending}）"
    );
    assert_eq!(dead, 0, "连接类错误不得进死信");
    let attempts: i64 =
        sqlx::query_scalar("SELECT MAX(attempts) FROM event_outbox WHERE state='pending'")
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert!(attempts >= 1, "失败应计 attempts");
    println!("phase2 ✓ 积压 {pending} 条（attempts={attempts}）");
}

/// 阶段 3（broker 恢复）：全部补投；消费端键集合 = 阶段 1+2 全集、无重复（验收 ②）。
#[tokio::test]
async fn phase3_recover_replays_without_dupes() {
    let Some(ctx) = ctx().await else {
        eprintln!("skip");
        return;
    };
    ensure_topic(&ctx).await;
    for _ in 0..120 {
        process_once(&ctx.pool, ctx.sink.notifier().as_ref()).await;
        let (pending, dead) = outbox_counts(&ctx.pool).await;
        assert_eq!(dead, 0, "恢复补投期间不得出现死信");
        if pending == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let (pending, dead) = outbox_counts(&ctx.pool).await;
    assert_eq!((pending, dead), (0, 0), "恢复后必须全部追平");
    let (keys, _) = consume_all(&ctx).await;
    let mut expect: Vec<String> = vec!["p1-0.json".to_string()];
    for i in 0..5 {
        expect.push(format!("p2-{i}.json"));
    }
    expect.sort();
    let mut got: Vec<String> = keys
        .iter()
        .filter(|k| k.starts_with("p1-") || k.starts_with("p2-"))
        .cloned()
        .collect();
    got.sort();
    // 严格集合相等 + 计数相等 = 不丢不重（稳态；崩溃窗至少一次口径见定稿）
    assert_eq!(got, expect, "键集合必须逐项相等（不丢）");
    assert_eq!(
        got.iter().filter(|k| k.starts_with("p2-")).count(),
        5,
        "p2 键不得重复（不重）"
    );
    println!("phase3 ✓ 追平并实收 {:?}", got);
}
