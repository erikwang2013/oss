//! Lifecycle 扫描游标（`lifecycle_cursors`）的确定性 apply（M8-2 下一片）。
//!
//! 与 inventory 同语义：先只读预检当前行，不合法 → `Conflict` 且零写入；通过后整行
//! upsert，且必须恰好影响 1 行。游标语义：水位非空、`updated_at` 严格前进、fence 不
//! 回退；自然键 `(bucket, rule_id, stage)` 就是主键。桶引用由 leader 在 append 前校验；
//! `rule_id` 不在 apply 复检（配置替换会清游标，游标只是续扫提示）。这里不读副本本地
//! 其他表决定结果。清除是幂等删除，`fence_term` 上限让旧 term 的迟到删除 no-op。

use super::{ControlWrite, invariant};
use crate::raft::command::ControlResponse;
use crate::raft::runstate::{LifecycleCursorRow, lifecycle_cursor_row_from};

/// 当前折叠行 + 日志行：自然键不可变（查询键本身）、fence 不回退、`updated_at`
/// 严格前进；逐字段相同的重复 apply 是 no-op。
pub(crate) fn cursor_row_ok(
    cur: Option<&LifecycleCursorRow>,
    next: &LifecycleCursorRow,
) -> Result<(), String> {
    if next.bucket.is_empty() || next.rule_id.is_empty() || next.stage.is_empty() {
        return Err(format!(
            "lifecycle cursor {}/{}/{}: empty natural key",
            next.bucket, next.rule_id, next.stage
        ));
    }
    if next.cursor.is_empty() {
        return Err(format!(
            "lifecycle cursor {}/{}/{}: empty watermark",
            next.bucket, next.rule_id, next.stage
        ));
    }
    let Some(cur) = cur else {
        return Ok(());
    };
    if next == cur {
        return Ok(());
    }
    if next.fence_term < cur.fence_term {
        return Err(format!(
            "lifecycle cursor {}/{}/{}: fence regressed",
            next.bucket, next.rule_id, next.stage
        ));
    }
    if next.updated_at <= cur.updated_at {
        return Err(format!(
            "lifecycle cursor {}/{}/{}: stale version",
            next.bucket, next.rule_id, next.stage
        ));
    }
    Ok(())
}

async fn current(
    cw: &mut ControlWrite<'_>,
    cursor: &LifecycleCursorRow,
) -> Result<Option<LifecycleCursorRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader \
         FROM lifecycle_cursors WHERE bucket = ? AND rule_id = ? AND stage = ?",
    )
    .bind(&cursor.bucket)
    .bind(&cursor.rule_id)
    .bind(&cursor.stage)
    .fetch_optional(&mut *cw.conn)
    .await?;
    Ok(row.as_ref().map(lifecycle_cursor_row_from))
}

async fn upsert(
    cw: &mut ControlWrite<'_>,
    cursor: &LifecycleCursorRow,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
          fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, rule_id, stage) DO UPDATE SET \
          cursor = excluded.cursor, updated_at = excluded.updated_at, \
          fence_term = excluded.fence_term, fence_leader = excluded.fence_leader",
    )
    .bind(&cursor.bucket)
    .bind(&cursor.rule_id)
    .bind(&cursor.stage)
    .bind(&cursor.cursor)
    .bind(cursor.updated_at)
    .bind(cursor.fence_term)
    .bind(cursor.fence_leader)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

/// 只读预检 → 整行 upsert。预检失败 = 零写入的 `Conflict`。
pub(crate) async fn apply(
    cw: &mut ControlWrite<'_>,
    cursor: &LifecycleCursorRow,
) -> Result<ControlResponse, sqlx::Error> {
    let cur = current(cw, cursor).await?;
    if cursor_row_ok(cur.as_ref(), cursor).is_err() {
        return Ok(ControlResponse::Conflict);
    }
    if upsert(cw, cursor).await? != 1 {
        return Err(invariant(format!(
            "lifecycle cursor {}/{}/{} upsert affected 0 rows",
            cursor.bucket, cursor.rule_id, cursor.stage
        )));
    }
    Ok(ControlResponse::Ok)
}

/// 幂等删除；只删 `fence_term` 不高于命令 token 的行。
pub(crate) async fn clear(
    cw: &mut ControlWrite<'_>,
    bucket: &str,
    rule_id: &str,
    stage: &str,
    fence_term: i64,
) -> Result<ControlResponse, sqlx::Error> {
    sqlx::query(
        "DELETE FROM lifecycle_cursors \
         WHERE bucket = ? AND rule_id = ? AND stage = ? AND fence_term <= ?",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .bind(fence_term)
    .execute(&mut *cw.conn)
    .await?;
    Ok(ControlResponse::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(cursor: &str, updated_at: i64) -> LifecycleCursorRow {
        LifecycleCursorRow {
            bucket: "b".into(),
            rule_id: "r".into(),
            stage: "current".into(),
            cursor: cursor.into(),
            updated_at,
            ..LifecycleCursorRow::default()
        }
    }

    #[test]
    fn cursor_gate_rejects_empty_stale_and_regressed_rows() {
        assert!(cursor_row_ok(None, &row("k", 1)).is_ok());
        assert!(cursor_row_ok(None, &row("", 1)).is_err());
        assert!(cursor_row_ok(None, &LifecycleCursorRow::default()).is_err());
        let base = row("k1", 2);
        // 重复整行幂等。
        assert!(cursor_row_ok(Some(&base), &base).is_ok());
        // updated_at 不回退/不持平。
        assert!(cursor_row_ok(Some(&base), &row("k2", 2)).is_err());
        assert!(cursor_row_ok(Some(&base), &row("k2", 3)).is_ok());
        // fence 不回退；同 term 更高 updated_at 可推进。
        let mut stale = row("k2", 3);
        stale.fence_term = base.fence_term - 1;
        assert!(cursor_row_ok(Some(&base), &stale).is_err());
        let mut refenced = row("k2", 3);
        refenced.fence_term = base.fence_term + 1;
        assert!(cursor_row_ok(Some(&base), &refenced).is_ok());
    }
}
