//! `audit_log` 的调用方事务内写实现（批量插入 / 保留策略删除）。

use xl_store::control_store::ControlError;
use xl_store::control_store::audit::{AuditTxn, NewAuditRecord};

use super::SqliteControlTxn;

#[async_trait::async_trait]
impl AuditTxn for SqliteControlTxn<'_> {
    async fn audit_insert_batch(&mut self, rows: &[NewAuditRecord]) -> Result<(), ControlError> {
        for r in rows {
            sqlx::query(
                "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision, detail) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(r.ts)
            .bind(r.uid)
            .bind(&r.op)
            .bind(&r.bucket)
            .bind(&r.object_key)
            .bind(&r.decision)
            .bind(&r.detail)
            .execute(&mut *self.conn)
            .await
            .map_err(|e| ControlError::Backend(e.to_string()))?;
        }
        Ok(())
    }

    async fn audit_delete_older_than(
        &mut self,
        cutoff: i64,
        limit: i64,
    ) -> Result<u64, ControlError> {
        let n = sqlx::query(
            "DELETE FROM audit_log WHERE id IN \
             (SELECT id FROM audit_log WHERE ts < ? ORDER BY ts LIMIT ?)",
        )
        .bind(cutoff)
        .bind(limit.max(1))
        .execute(&mut *self.conn)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?
        .rows_affected();
        Ok(n)
    }
}
