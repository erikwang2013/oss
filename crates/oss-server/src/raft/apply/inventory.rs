//! Inventory 运行态行（runs/cursors/incrementals）的确定性 apply（M8-2 下一片）。
//!
//! 与 jobs 同语义：整批先只读预检，任一行不合法 → `Conflict` 且零写入；校验通过后
//! 逐行 upsert，且必须恰好影响 1 行。引用完整性（run.job_id）由 leader 在 append 前
//! 校验，这里不读副本本地的 jobs 表决定结果。

use super::{ControlWrite, invariant};
use crate::raft::command::ControlResponse;
use crate::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, inventory_cursor_row_from,
    inventory_incremental_row_from, inventory_run_row_from,
};

fn run_terminal(state: &str) -> bool {
    matches!(state, "done" | "skipped")
}

/// 运行行状态机：done/skipped 终态冻结；error 可重试回 running（失败后重跑）。
fn run_state_ok(from: &str, to: &str) -> bool {
    from == to
        || matches!(
            (from, to),
            ("running", "done")
                | ("running", "error")
                | ("running", "skipped")
                | ("error", "running")
                | ("error", "done")
                | ("error", "skipped")
        )
}

/// 当前折叠行 + 日志行：身份不可变、fence 不回退、started_at/job_id/manifest_key
/// 一旦落定不可改写；终态行只接受逐字段相同的重复 apply。
pub(crate) fn run_row_ok(
    cur: Option<&InventoryRunRow>,
    next: &InventoryRunRow,
) -> Result<(), String> {
    let Some(cur) = cur else {
        return if next.id > 0 && next.state == "running" {
            Ok(())
        } else {
            Err(format!("run {}: new row in state {}", next.id, next.state))
        };
    };
    if cur.bucket != next.bucket
        || cur.config_id != next.config_id
        || cur.run_key != next.run_key
        || cur.created_at != next.created_at
    {
        return Err(format!("run {}: identity changed", next.id));
    }
    if run_terminal(&cur.state) {
        // fence 只是执行权标记：同内容的更高 term 重放可推进 fence，业务字段保持不变。
        return if next.state == cur.state
            && next.started_at == cur.started_at
            && next.job_id == cur.job_id
            && next.manifest_key == cur.manifest_key
            && next.error == cur.error
            && next.fence_term >= cur.fence_term
        {
            Ok(())
        } else {
            Err(format!("run {}: terminal row changed", next.id))
        };
    }
    if next == cur {
        return Ok(());
    }
    if next.fence_term < cur.fence_term {
        return Err(format!("run {}: fence regressed", next.id));
    }
    if !run_state_ok(&cur.state, &next.state) {
        return Err(format!(
            "run {}: illegal state {} -> {}",
            next.id, cur.state, next.state
        ));
    }
    if next.started_at < 0 || (cur.started_at != 0 && next.started_at != cur.started_at) {
        return Err(format!("run {}: started_at changed", next.id));
    }
    if let Some(job_id) = cur.job_id
        && next.job_id != Some(job_id)
    {
        return Err(format!("run {}: job_id changed", next.id));
    }
    if let Some(manifest) = &cur.manifest_key
        && next.manifest_key.as_ref() != Some(manifest)
    {
        return Err(format!("run {}: manifest_key changed", next.id));
    }
    Ok(())
}

/// 游标行：同配置版本的水位只进不退；配置版本可前移（重置水位）；fence 不回退。
pub(crate) fn cursor_row_ok(
    cur: Option<&InventoryCursorRow>,
    next: &InventoryCursorRow,
) -> Result<(), String> {
    if next.cursor.is_empty() {
        return Err(format!(
            "cursor {}/{}: empty watermark",
            next.bucket, next.config_id
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
            "cursor {}/{}: fence regressed",
            next.bucket, next.config_id
        ));
    }
    if next.updated_at <= cur.updated_at {
        return Err(format!(
            "cursor {}/{}: stale version",
            next.bucket, next.config_id
        ));
    }
    if next.config_updated_at < cur.config_updated_at {
        return Err(format!(
            "cursor {}/{}: config version regressed",
            next.bucket, next.config_id
        ));
    }
    if next.config_updated_at == cur.config_updated_at
        && let (Ok(old), Ok(new)) = (cur.cursor.parse::<i64>(), next.cursor.parse::<i64>())
        && new < old
    {
        return Err(format!(
            "cursor {}/{}: watermark regressed {old} -> {new}",
            next.bucket, next.config_id
        ));
    }
    Ok(())
}

/// 增量开关行：只存 enabled=1；`updated_at` 严格前进（重复 apply 幂等）。
pub(crate) fn incremental_row_ok(
    cur: Option<&InventoryIncrementalRow>,
    next: &InventoryIncrementalRow,
) -> Result<(), String> {
    if !next.enabled {
        return Err(format!(
            "incremental {}/{}: disabled rows are deletes",
            next.bucket, next.config_id
        ));
    }
    let Some(cur) = cur else {
        return Ok(());
    };
    if next == cur {
        return Ok(());
    }
    if next.updated_at <= cur.updated_at {
        return Err(format!(
            "incremental {}/{}: stale version",
            next.bucket, next.config_id
        ));
    }
    Ok(())
}

async fn current_run(
    cw: &mut ControlWrite<'_>,
    id: i64,
) -> Result<Option<InventoryRunRow>, sqlx::Error> {
    let row = sqlx::query("SELECT * FROM inventory_runs WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.as_ref().map(inventory_run_row_from))
}

async fn run_key_taken(
    cw: &mut ControlWrite<'_>,
    run: &InventoryRunRow,
) -> Result<bool, sqlx::Error> {
    let id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM inventory_runs WHERE bucket = ? AND config_id = ? AND run_key = ?",
    )
    .bind(&run.bucket)
    .bind(&run.config_id)
    .bind(&run.run_key)
    .fetch_optional(&mut *cw.conn)
    .await?;
    Ok(id.is_some_and(|id| id != run.id))
}

async fn current_cursor(
    cw: &mut ControlWrite<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryCursorRow>, sqlx::Error> {
    let row = sqlx::query("SELECT * FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(config_id)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.as_ref().map(inventory_cursor_row_from))
}

async fn current_incremental(
    cw: &mut ControlWrite<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryIncrementalRow>, sqlx::Error> {
    let row = sqlx::query("SELECT * FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(config_id)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.as_ref().map(inventory_incremental_row_from))
}

async fn apply_run(cw: &mut ControlWrite<'_>, run: &InventoryRunRow) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
          job_id, manifest_key, error, created_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
          bucket = excluded.bucket, config_id = excluded.config_id, \
          run_key = excluded.run_key, started_at = excluded.started_at, \
          state = excluded.state, job_id = excluded.job_id, \
          manifest_key = excluded.manifest_key, error = excluded.error, \
          created_at = excluded.created_at, fence_term = excluded.fence_term, \
          fence_leader = excluded.fence_leader",
    )
    .bind(run.id)
    .bind(&run.bucket)
    .bind(&run.config_id)
    .bind(&run.run_key)
    .bind(run.started_at)
    .bind(&run.state)
    .bind(run.job_id)
    .bind(&run.manifest_key)
    .bind(&run.error)
    .bind(run.created_at)
    .bind(run.fence_term)
    .bind(run.fence_leader)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

async fn apply_cursor(
    cw: &mut ControlWrite<'_>,
    cursor: &InventoryCursorRow,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, \
          updated_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, config_id) DO UPDATE SET \
          cursor = excluded.cursor, config_updated_at = excluded.config_updated_at, \
          updated_at = excluded.updated_at, fence_term = excluded.fence_term, \
          fence_leader = excluded.fence_leader",
    )
    .bind(&cursor.bucket)
    .bind(&cursor.config_id)
    .bind(&cursor.cursor)
    .bind(cursor.config_updated_at)
    .bind(cursor.updated_at)
    .bind(cursor.fence_term)
    .bind(cursor.fence_leader)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

async fn apply_incremental(
    cw: &mut ControlWrite<'_>,
    row: &InventoryIncrementalRow,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES (?, ?, ?, ?) ON CONFLICT(bucket, config_id) DO UPDATE SET \
          enabled = excluded.enabled, updated_at = excluded.updated_at",
    )
    .bind(&row.bucket)
    .bind(&row.config_id)
    .bind(row.enabled)
    .bind(row.updated_at)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

/// 整批只读预检 → 逐行 upsert。预检失败 = 零写入的 `Conflict`。
pub(crate) async fn apply(
    cw: &mut ControlWrite<'_>,
    runs: &[InventoryRunRow],
    cursors: &[InventoryCursorRow],
    incrementals: &[InventoryIncrementalRow],
) -> Result<ControlResponse, sqlx::Error> {
    let mut run_ids = std::collections::HashSet::new();
    for run in runs {
        if !run_ids.insert(run.id) {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_run(cw, run.id).await?;
        if run_row_ok(cur.as_ref(), run).is_err() {
            return Ok(ControlResponse::Conflict);
        }
        if cur.is_none() && run_key_taken(cw, run).await? {
            return Ok(ControlResponse::Conflict);
        }
    }
    let mut cursor_ids = std::collections::HashSet::new();
    for cursor in cursors {
        if !cursor_ids.insert((cursor.bucket.clone(), cursor.config_id.clone())) {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_cursor(cw, &cursor.bucket, &cursor.config_id).await?;
        if cursor_row_ok(cur.as_ref(), cursor).is_err() {
            return Ok(ControlResponse::Conflict);
        }
    }
    let mut incremental_ids = std::collections::HashSet::new();
    for row in incrementals {
        if !incremental_ids.insert((row.bucket.clone(), row.config_id.clone())) {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_incremental(cw, &row.bucket, &row.config_id).await?;
        if incremental_row_ok(cur.as_ref(), row).is_err() {
            return Ok(ControlResponse::Conflict);
        }
    }
    for run in runs {
        if apply_run(cw, run).await? != 1 {
            return Err(invariant(format!(
                "inventory run {} upsert affected 0 rows",
                run.id
            )));
        }
    }
    for cursor in cursors {
        if apply_cursor(cw, cursor).await? != 1 {
            return Err(invariant(format!(
                "inventory cursor {}/{} upsert affected 0 rows",
                cursor.bucket, cursor.config_id
            )));
        }
    }
    for row in incrementals {
        if apply_incremental(cw, row).await? != 1 {
            return Err(invariant(format!(
                "inventory incremental {}/{} upsert affected 0 rows",
                row.bucket, row.config_id
            )));
        }
    }
    Ok(ControlResponse::Ok)
}

/// 关闭增量：删游标与开关。幂等，重复 apply 为 no-op。
pub(crate) async fn clear(
    cw: &mut ControlWrite<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<ControlResponse, sqlx::Error> {
    sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(config_id)
        .execute(&mut *cw.conn)
        .await?;
    sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(config_id)
        .execute(&mut *cw.conn)
        .await?;
    Ok(ControlResponse::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: i64, state: &str) -> InventoryRunRow {
        InventoryRunRow {
            id,
            bucket: "b".into(),
            config_id: "c".into(),
            run_key: "k".into(),
            state: state.into(),
            created_at: 1,
            ..InventoryRunRow::default()
        }
    }

    #[test]
    fn run_gate_freezes_terminal_and_retries_error() {
        assert!(run_row_ok(None, &run(1, "running")).is_ok());
        assert!(run_row_ok(None, &run(1, "done")).is_err());
        let mut started = run(1, "running");
        started.started_at = 5;
        assert!(run_row_ok(Some(&run(1, "running")), &started).is_ok());
        assert!(run_row_ok(Some(&started), &run(1, "running")).is_err());
        let mut failed = started.clone();
        failed.state = "error".into();
        failed.error = Some("boom".into());
        assert!(run_row_ok(Some(&started), &failed).is_ok());
        let mut retried = failed.clone();
        retried.state = "running".into();
        retried.fence_term = 2;
        assert!(run_row_ok(Some(&failed), &retried).is_ok());
        assert!(run_row_ok(Some(&failed), &failed).is_ok());
        let mut revived = failed.clone();
        revived.state = "running".into();
        let done = InventoryRunRow {
            state: "done".into(),
            ..failed.clone()
        };
        assert!(run_row_ok(Some(&done), &revived).is_err());
        let mut stale = done.clone();
        stale.state = "running".into();
        stale.fence_term = done.fence_term - 1;
        assert!(run_row_ok(Some(&done), &stale).is_err());
        let mut refenced = done.clone();
        refenced.fence_term += 1;
        assert!(
            run_row_ok(Some(&done), &refenced).is_ok(),
            "same terminal content may advance the fence after leader change"
        );
        refenced.error = Some("other".into());
        assert!(run_row_ok(Some(&done), &refenced).is_err());
    }

    #[test]
    fn cursor_watermark_only_moves_forward_for_same_config_version() {
        let base = InventoryCursorRow {
            bucket: "b".into(),
            config_id: "c".into(),
            cursor: "100".into(),
            config_updated_at: 1,
            updated_at: 2,
            ..InventoryCursorRow::default()
        };
        let mut next = base.clone();
        next.cursor = "99".into();
        next.updated_at = 3;
        assert!(cursor_row_ok(Some(&base), &next).is_err());
        next.cursor = "100".into();
        assert!(cursor_row_ok(Some(&base), &next).is_ok());
        next.cursor = "abc".into();
        assert!(
            cursor_row_ok(Some(&base), &next).is_ok(),
            "non-numeric watermarks cannot be ordered; last write wins"
        );
        next.config_updated_at = 2;
        next.cursor = "1".into();
        assert!(
            cursor_row_ok(Some(&base), &next).is_ok(),
            "config change may reset the watermark"
        );
        next.cursor = String::new();
        assert!(cursor_row_ok(Some(&base), &next).is_err());
    }

    #[test]
    fn incremental_gate_is_monotonic_and_delete_only() {
        let base = InventoryIncrementalRow {
            bucket: "b".into(),
            config_id: "c".into(),
            enabled: true,
            updated_at: 2,
        };
        assert!(incremental_row_ok(None, &base).is_ok());
        assert!(incremental_row_ok(Some(&base), &base).is_ok());
        let mut stale = base.clone();
        stale.updated_at = 1;
        assert!(incremental_row_ok(Some(&base), &stale).is_err());
        let off = InventoryIncrementalRow {
            enabled: false,
            ..base
        };
        assert!(incremental_row_ok(None, &off).is_err());
    }
}
