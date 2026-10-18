//! M6 切片 4：桶复制——SQL 规则存储、post-commit 入队（`repl_outbox`）、出站 worker。
//!
//! 语义（计划 R1/R2 + V3）：
//! - 仅版本化提交入队（`version_id` 非 "null"；REPLICA 摄入不再复制，防环）；
//! - 永久错超 10 次 → `state='dead'` 且版本状态 FAILED；连接类错误永不 dead（退避封顶 300s）；
//! - 成功 → 版本状态 COMPLETED 并删行；marker 成功同样 COMPLETED。

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::SqlitePool;
use xl_s3::replication::{ReplRule, ReplicationSink, ReplicationStore, WriteInfo};
use xl_store::store::Store;

use crate::events::{backoff_secs, now};
use crate::repl_client::{ErrClass, ReplClient, ReplError};

const CLAIM_BATCH: i64 = 16;
/// 复制作业租约（计划 R2：长于事件 60s——整读+跨机 PUT 更慢）
const REPL_LEASE_SECS: i64 = 300;
const MAX_ATTEMPTS: i64 = 10;

// ---------- 规则存储（Put/Get/DeleteBucketReplication 背后） ----------

pub struct SqlReplStore {
    pool: SqlitePool,
    remote_ids: Vec<String>,
}

impl SqlReplStore {
    pub fn new(pool: SqlitePool, remote_ids: Vec<String>) -> Self {
        Self { pool, remote_ids }
    }
}

#[async_trait]
impl ReplicationStore for SqlReplStore {
    async fn load_rules(&self, bucket: &str) -> Result<Option<String>, String> {
        sqlx::query_scalar("SELECT rules_json FROM replication_rules WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    async fn save_rules(&self, bucket: &str, rules_json: &str) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json, \
             updated_at=excluded.updated_at",
        )
        .bind(bucket)
        .bind(rules_json)
        .bind(now())
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remove_rules(&self, bucket: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM replication_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remote_ids(&self) -> Vec<String> {
        self.remote_ids.clone()
    }
}

// ---------- 写入钩子：规则匹配 → repl_outbox + PENDING ----------

pub struct SqlReplSink {
    pool: SqlitePool,
    store: Arc<Store>,
}

impl SqlReplSink {
    pub fn new(pool: SqlitePool, store: Arc<Store>) -> Self {
        Self { pool, store }
    }
}

/// 规则是否命中该写事件（enabled + 前缀 + marker 开关）。
fn rule_matches(r: &ReplRule, info: &WriteInfo<'_>) -> bool {
    if !r.enabled {
        return false;
    }
    if info.is_delete_marker && !r.delete_marker {
        return false;
    }
    match &r.prefix {
        Some(p) => info.key.starts_with(p),
        None => true,
    }
}

#[async_trait]
impl ReplicationSink for SqlReplSink {
    async fn on_write(&self, info: WriteInfo<'_>) {
        // REPLICA 不再复制（防环）。数据写仅版本化提交（"null" = 未开/暂停版本）；
        // 删除操作（marker 或硬删）允许无版本号（V3②：写路径全覆盖；顶点状态仅在有版本时维护）。
        if info.is_replica {
            return;
        }
        if !info.is_delete_marker {
            let Some(vid) = info.version_id else { return };
            if vid == "null" {
                return;
            }
        }
        let json = match sqlx::query_scalar::<_, String>(
            "SELECT rules_json FROM replication_rules WHERE bucket = ?",
        )
        .bind(info.bucket)
        .fetch_optional(&self.pool)
        .await
        {
            Ok(Some(j)) => j,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, bucket = info.bucket, "读复制规则失败——本写不复制（不影响请求）");
                return;
            }
        };
        let rules = match xl_s3::replication::rules_from_json(&json) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, bucket = info.bucket, "复制规则记录损坏——跳过");
                return;
            }
        };
        let mut matched = false;
        for r in rules.iter().filter(|r| rule_matches(r, &info)) {
            matched = true;
            if let Err(e) = sqlx::query(
                "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
                 dest_bucket, is_delete_marker, state, attempts, next_retry_at, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?)",
            )
            .bind(info.bucket)
            .bind(info.key)
            .bind(info.version_id.unwrap_or(""))
            .bind(info.etag)
            .bind(info.size as i64)
            .bind(&r.remote_id)
            .bind(&r.dest_bucket)
            .bind(i64::from(info.is_delete_marker))
            .bind(now())
            .execute(&self.pool)
            .await
            {
                tracing::warn!(error = %e, bucket = info.bucket, key = info.key, "复制入队失败");
            }
        }
        // 单调 PENDING：并发 worker 可能已 COMPLETED（P2-2，不降级终态）；无版本号（硬删）跳过
        if matched
            && let Some(vid) = info.version_id
            && let Err(e) = self
                .store
                .mark_replication_pending(info.bucket, info.key, vid)
                .await
        {
            tracing::warn!(error = %e, bucket = info.bucket, key = info.key, "置 PENDING 失败");
        }
    }
}

// ---------- worker（R2 租约 + R1 分级） ----------

struct ReplRow {
    seq: i64,
    bucket: String,
    key: String,
    version_id: String,
    etag: String,
    remote_id: String,
    dest_bucket: String,
    is_delete_marker: bool,
    attempts: i64,
    claimed_at: Option<i64>,
}

async fn renew_claim(pool: &SqlitePool, seq: i64, old: Option<i64>) -> Option<i64> {
    let now_s = now();
    match sqlx::query(
        "UPDATE repl_outbox SET claimed_at=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(now_s)
    .bind(seq)
    .bind(old)
    .execute(pool)
    .await
    {
        Ok(r) if r.rows_affected() == 1 => Some(now_s),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制续租失败（跳过本轮）");
            None
        }
    }
}

async fn finish_ok(pool: &SqlitePool, seq: i64, token: Option<i64>) -> bool {
    match sqlx::query("DELETE FROM repl_outbox WHERE seq=? AND claimed_at IS ? AND state='sending'")
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制完成删行失败");
            false
        }
    }
}

async fn requeue_or_bury(
    pool: &SqlitePool,
    seq: i64,
    token: Option<i64>,
    attempts: i64,
    failure: &ReplError,
    now_s: i64,
) -> bool {
    let bury = failure.class == ErrClass::Permanent && attempts > MAX_ATTEMPTS;
    let res = if bury {
        sqlx::query(
            "UPDATE repl_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
             WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(&failure.message)
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE repl_outbox SET state='pending', attempts=?, claimed_at=NULL, \
             next_retry_at=?, last_error=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(now_s + backoff_secs(attempts))
        .bind(&failure.message)
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    };
    match res {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制失败状态回写失败");
            false
        }
    }
}

/// 一行复制：marker → 目标 DELETE；数据版本 → 读源 + PUT（带 REPLICA）。
async fn send_row(store: &Store, client: &ReplClient, r: &ReplRow) -> Result<(), ReplError> {
    if r.is_delete_marker {
        return client
            .delete_object(&r.remote_id, &r.dest_bucket, &r.key)
            .await;
    }
    // 必须读行内记录的**那一个版本**（对象可能已被覆写；读 latest 会复制错内容）
    let info = store
        .head_object_version(&r.bucket, &r.key, &r.version_id)
        .await
        .map_err(|e| match e {
            xl_store::error::Error::NoSuchKey(_) => {
                ReplError::permanent(format!("源版本已不存在：{}@{}", r.key, r.version_id))
            }
            other => ReplError::retryable(format!("读源元数据失败：{other}")),
        })?;
    // SSE-C 源无持久化钥 → 无法在本机解密复制（计划未承诺；永久错记档）
    if let Some(enc) = &info.encryption
        && enc.sse_c_key_md5.is_some()
    {
        return Err(ReplError::permanent(
            "SSE-C 源对象不支持复制（客户钥不落盘）",
        ));
    }
    let (_info, bytes) = store
        .get_object_version(&r.bucket, &r.key, &r.version_id)
        .await
        .map_err(|e| ReplError::retryable(format!("读源对象失败：{e}")))?;
    client
        .put_object(&r.remote_id, &r.dest_bucket, &r.key, bytes, &r.etag)
        .await
}

/// P2-1：一个版本的复制终态聚合——仍有 pending/sending 行则保持 PENDING；
/// 存在 dead 行则 FAILED；否则 COMPLETED。
async fn settle_version_status(pool: &SqlitePool, store: &Store, r: &ReplRow) {
    async fn count(pool: &SqlitePool, r: &ReplRow, state: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM repl_outbox WHERE bucket=? AND key=? AND version_id=? \
             AND state=?",
        )
        .bind(&r.bucket)
        .bind(&r.key)
        .bind(&r.version_id)
        .bind(state)
        .fetch_one(pool)
        .await
        .unwrap_or(0)
    }
    if count(pool, r, "pending").await > 0 || count(pool, r, "sending").await > 0 {
        return; // 其他规则仍在途：保持 PENDING
    }
    let status = if count(pool, r, "dead").await > 0 {
        "FAILED"
    } else {
        "COMPLETED"
    };
    if let Err(e) = store
        .set_replication_status(&r.bucket, &r.key, &r.version_id, Some(status))
        .await
    {
        tracing::warn!(error = %e, status, "复制终态写入失败（对象可能已被删除）");
    }
}

/// 一轮：回收过期租约 → claim → 逐行续租/发送/完成或重试。返回 (成功, 失败)。
pub async fn process_repl_once(
    pool: &SqlitePool,
    store: &Store,
    client: &ReplClient,
) -> (usize, usize) {
    let now_s = now();
    if let Err(e) = sqlx::query(
        "UPDATE repl_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
         WHERE state='sending' AND claimed_at < ?",
    )
    .bind(now_s)
    .bind(now_s - REPL_LEASE_SECS)
    .execute(pool)
    .await
    {
        tracing::warn!(error = %e, "repl_outbox 租约回收失败");
        return (0, 0);
    }
    let rows = match sqlx::query(
        "UPDATE repl_outbox SET state='sending', claimed_at=? \
         WHERE state='pending' AND seq IN (SELECT seq FROM repl_outbox WHERE state='pending' \
                       AND next_retry_at <= ? ORDER BY seq LIMIT ?) \
         RETURNING seq, bucket, key, version_id, etag, remote_id, dest_bucket, \
         is_delete_marker, attempts, claimed_at",
    )
    .bind(now_s)
    .bind(now_s)
    .bind(CLAIM_BATCH)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "repl_outbox claim 失败");
            return (0, 0);
        }
    };
    let (mut sent, mut failed) = (0usize, 0usize);
    for row in rows {
        use sqlx::Row as _;
        let r = ReplRow {
            seq: row.get("seq"),
            bucket: row.get("bucket"),
            key: row.get("key"),
            version_id: row.get("version_id"),
            etag: row.get("etag"),
            remote_id: row.get("remote_id"),
            dest_bucket: row.get("dest_bucket"),
            is_delete_marker: row.get::<i64, _>("is_delete_marker") != 0,
            attempts: row.get("attempts"),
            claimed_at: row.get("claimed_at"),
        };
        let Some(token) = renew_claim(pool, r.seq, r.claimed_at).await else {
            continue;
        };
        match send_row(store, client, &r).await {
            Ok(()) => {
                let claimed = finish_ok(pool, r.seq, Some(token)).await;
                if claimed {
                    sent += 1;
                } else {
                    tracing::warn!(seq = r.seq, "复制完成时 claim token 不符（跳过状态写）");
                }
                // P2-1：多规则聚合——仍有在途行则保持 PENDING；有死信则 FAILED；否则 COMPLETED。
                // P3-5：token 失配时不得写状态（该行可能已被新 worker 接管）。
                if claimed && !r.version_id.is_empty() {
                    settle_version_status(pool, store, &r).await;
                }
            }
            Err(f) => {
                failed += 1;
                let a = r.attempts + 1;
                let bury = f.class == ErrClass::Permanent && a > MAX_ATTEMPTS;
                let wrote = requeue_or_bury(pool, r.seq, Some(token), a, &f, now_s).await;
                if !wrote {
                    tracing::warn!(
                        seq = r.seq,
                        "复制失败回写时 claim token 不符（已被回收重投？）"
                    );
                }
                if bury && wrote {
                    tracing::error!(seq = r.seq, error = %f.message, "复制作业进入死信（永久错超限）");
                    if !r.version_id.is_empty()
                        && let Err(e) = store
                            .set_replication_status(
                                &r.bucket,
                                &r.key,
                                &r.version_id,
                                Some("FAILED"),
                            )
                            .await
                    {
                        tracing::warn!(error = %e, "置 FAILED 失败（对象可能已被删除）");
                    }
                }
            }
        }
    }
    (sent, failed)
}

/// serve 挂载（`OSS_REPL_INTERVAL_SECS` 默认 1s；显式 0 = 关闭）。
pub fn spawn_repl_worker(pool: SqlitePool, store: Arc<Store>, client: Arc<ReplClient>) {
    let secs: u64 = std::env::var("OSS_REPL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if secs == 0 {
        tracing::info!("复制 worker 已关闭（OSS_REPL_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let _ = process_repl_once(&pool, &store, &client).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(prefix: Option<&str>, dm: bool, enabled: bool) -> ReplRule {
        ReplRule {
            id: "r".into(),
            prefix: prefix.map(str::to_string),
            remote_id: "r1".into(),
            dest_bucket: "dst".into(),
            delete_marker: dm,
            enabled,
        }
    }

    fn info<'a>(key: &'a str, marker: bool, replica: bool) -> WriteInfo<'a> {
        WriteInfo {
            bucket: "b",
            key,
            version_id: Some("v1"),
            etag: "e",
            size: 1,
            is_delete_marker: marker,
            is_replica: replica,
        }
    }

    #[test]
    fn rule_matching_matrix() {
        assert!(rule_matches(
            &rule(None, false, true),
            &info("a", false, false)
        ));
        assert!(rule_matches(
            &rule(Some("logs/"), false, true),
            &info("logs/x", false, false)
        ));
        assert!(!rule_matches(
            &rule(Some("logs/"), false, true),
            &info("data/x", false, false)
        ));
        // marker：仅 DeleteMarkerReplication=Enabled 的规则命中
        assert!(!rule_matches(
            &rule(None, false, true),
            &info("a", true, false)
        ));
        assert!(rule_matches(
            &rule(None, true, true),
            &info("a", true, false)
        ));
        // Disabled 规则永不命中
        assert!(!rule_matches(
            &rule(None, true, false),
            &info("a", false, false)
        ));
    }
}
