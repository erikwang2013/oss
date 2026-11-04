//! `audit_log` 的 SQLite 只读实现（控制台查询）。
//!
//! 过滤片段只拼固定文本、用户值一律绑定，顺序与迁移前 `db.rs::list_audit`
//! 逐字一致。

use sqlx::{AssertSqlSafe, Row};
use xl_store::control_store::ControlError;
use xl_store::control_store::audit::{AuditQuery, AuditRecord, AuditStore};

use super::SqliteControlStore;

#[async_trait::async_trait]
impl AuditStore for SqliteControlStore {
    async fn audit_page(&self, f: &AuditQuery) -> Result<Vec<AuditRecord>, ControlError> {
        let mut sql = String::from(
            "SELECT id, ts, uid, op, bucket, object_key, decision, detail FROM audit_log \
             WHERE 1=1",
        );
        for (present, clause) in [
            (f.decision.is_some(), " AND decision = ?"),
            (f.from.is_some(), " AND ts >= ?"),
            (f.to.is_some(), " AND ts <= ?"),
            (f.uid.is_some(), " AND uid = ?"),
            (f.op.is_some(), " AND op = ?"),
            (f.bucket.is_some(), " AND bucket = ?"),
        ] {
            if present {
                sql.push_str(clause);
            }
        }
        // 前缀按字节字面量匹配：键区分大小写（LIKE 对 ASCII 不区分），无需转义通配符
        if f.key_prefix.is_some() {
            sql.push_str(" AND substr(object_key, 1, length(?)) = ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");

        let mut q = sqlx::query(AssertSqlSafe(sql));
        if let Some(v) = &f.decision {
            q = q.bind(v);
        }
        if let Some(v) = f.from {
            q = q.bind(v);
        }
        if let Some(v) = f.to {
            q = q.bind(v);
        }
        if let Some(v) = f.uid {
            q = q.bind(v);
        }
        if let Some(v) = &f.op {
            q = q.bind(v);
        }
        if let Some(v) = &f.bucket {
            q = q.bind(v);
        }
        if let Some(v) = &f.key_prefix {
            q = q.bind(v).bind(v);
        }
        let rows = q
            .bind(f.limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ControlError::Backend(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| AuditRecord {
                id: r.get("id"),
                ts: r.get("ts"),
                uid: r.get("uid"),
                op: r.get("op"),
                bucket: r.get("bucket"),
                object_key: r.get("object_key"),
                decision: r.get("decision"),
                detail: r.get("detail"),
            })
            .collect())
    }
}
