//! Inventory 运行态（runs/cursors/incrementals）的复制写入门面（M8-2 下一片）。
//!
//! 与 jobs 相同：Raft 且 advertised feature ≥4 时，leader 在进程内 mutex 下读取已
//! apply 的行、合并出下一整行，经 [`ControlPlane::write`] 提交；follower 不得读旧行
//! 转发。feature<4 或非 Raft 保持节点本地直写。

use super::*;
use crate::raft::apply::inventory::{cursor_row_ok, incremental_row_ok, run_row_ok};
use crate::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, inventory_cursor_row_from,
    inventory_incremental_row_from, inventory_run_row_from,
};

fn run_terminal(state: &str) -> bool {
    matches!(state, "done" | "skipped")
}

impl JobState {
    async fn read_run(db: &Db, id: i64) -> Result<Option<InventoryRunRow>, String> {
        let row = sqlx::query("SELECT * FROM inventory_runs WHERE id = ?")
            .bind(id)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(inventory_run_row_from))
    }

    async fn read_run_by_key(
        db: &Db,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>, String> {
        let row = sqlx::query(
            "SELECT * FROM inventory_runs WHERE bucket = ? AND config_id = ? AND run_key = ?",
        )
        .bind(bucket)
        .bind(config_id)
        .bind(run_key)
        .fetch_optional(db.pool())
        .await
        .map_err(err)?;
        Ok(row.as_ref().map(inventory_run_row_from))
    }

    async fn read_cursor(
        db: &Db,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryCursorRow>, String> {
        let row = sqlx::query("SELECT * FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(config_id)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(inventory_cursor_row_from))
    }

    async fn read_incremental(
        db: &Db,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryIncrementalRow>, String> {
        let row =
            sqlx::query("SELECT * FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
                .bind(bucket)
                .bind(config_id)
                .fetch_optional(db.pool())
                .await
                .map_err(err)?;
        Ok(row.as_ref().map(inventory_incremental_row_from))
    }

    async fn next_run_id(db: &Db) -> Result<i64, String> {
        let meta: Option<i64> =
            sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = 'next_run_id'")
                .fetch_optional(db.pool())
                .await
                .map_err(err)?;
        let max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM inventory_runs")
            .fetch_one(db.pool())
            .await
            .map_err(err)?;
        Ok(meta.unwrap_or(0).max(max + 1).max(1))
    }

    /// 提交前校验身份/版本/运行引用；apply 只按日志事实做确定性 upsert。
    async fn commit_inventory(
        &self,
        db: &Db,
        runs: Vec<InventoryRunRow>,
        cursors: Vec<InventoryCursorRow>,
        incrementals: Vec<InventoryIncrementalRow>,
    ) -> Result<(), String> {
        if runs.is_empty() && cursors.is_empty() && incrementals.is_empty() {
            return Ok(());
        }
        for run in &runs {
            let cur = Self::read_run(db, run.id).await?;
            run_row_ok(cur.as_ref(), run).map_err(err)?;
            if let Some(job_id) = run.job_id
                && Self::read_job(db, job_id).await?.is_none()
            {
                return Err(format!("inventory run {}: job {job_id} missing", run.id));
            }
        }
        for cursor in &cursors {
            let cur = Self::read_cursor(db, &cursor.bucket, &cursor.config_id).await?;
            cursor_row_ok(cur.as_ref(), cursor).map_err(err)?;
        }
        for row in &incrementals {
            let cur = Self::read_incremental(db, &row.bucket, &row.config_id).await?;
            incremental_row_ok(cur.as_ref(), row).map_err(err)?;
        }
        let Some(control) = &self.control else {
            return Err("inventory state commit without control plane".into());
        };
        match control
            .write(ControlCommand::InventoryBatch {
                runs,
                cursors,
                incrementals,
            })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("inventory batch rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn clear_incremental(&self, bucket: &str, config_id: &str) -> Result<(), String> {
        let Some(control) = &self.control else {
            return Err("inventory clear without control plane".into());
        };
        match control
            .write(ControlCommand::InventoryClear {
                bucket: bucket.to_string(),
                config_id: config_id.to_string(),
            })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("inventory clear rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    /// 建运行行（同 `(bucket, config_id, run_key)` 已存在 → None）。
    pub async fn inventory_create_run(
        &self,
        db: &Db,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<Option<i64>, String> {
        if !self.replicated() {
            self.note_local("inventory_create_run");
            return db
                .inventory_create_run(bucket, config_id, run_key, now, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("inventory_create_run").await?;
        if Self::read_run_by_key(db, bucket, config_id, run_key)
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let id = Self::next_run_id(db).await?;
        let run = InventoryRunRow {
            id,
            bucket: bucket.to_string(),
            config_id: config_id.to_string(),
            run_key: run_key.to_string(),
            started_at: 0,
            state: "running".into(),
            job_id: None,
            manifest_key: None,
            error: None,
            created_at: now,
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
        };
        self.commit_inventory(db, vec![run], Vec::new(), Vec::new())
            .await?;
        Ok(Some(id))
    }

    pub async fn inventory_run_set_job(
        &self,
        db: &Db,
        run_id: i64,
        job_id: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("inventory_run_set_job");
            return db
                .inventory_run_set_job(run_id, job_id, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("inventory_run_set_job")?;
        let Some(mut run) = Self::read_run(db, run_id).await? else {
            return Ok(());
        };
        if fence.term_i64() < run.fence_term {
            return Ok(());
        }
        run.job_id = Some(job_id);
        run.fence_term = fence.term_i64();
        run.fence_leader = fence.leader_i64();
        self.commit_inventory(db, vec![run], Vec::new(), Vec::new())
            .await
    }

    /// 首次执行写 started_at（只写一次），并重标 fence。
    pub async fn inventory_run_started(
        &self,
        db: &Db,
        run_id: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("inventory_run_started");
            return db
                .inventory_run_started(run_id, now, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("inventory_run_started")?;
        let Some(mut run) = Self::read_run(db, run_id).await? else {
            return Ok(());
        };
        if run_terminal(&run.state) || fence.term_i64() < run.fence_term {
            return Ok(());
        }
        if run.started_at != 0 && run.fence_term == fence.term_i64() {
            return Ok(());
        }
        if run.started_at == 0 {
            run.started_at = now;
        }
        run.fence_term = fence.term_i64();
        run.fence_leader = fence.leader_i64();
        self.commit_inventory(db, vec![run], Vec::new(), Vec::new())
            .await
    }

    /// 结算（done/error/skipped）；done/skipped 终态冻结，error 可重试。
    pub async fn inventory_run_finish(
        &self,
        db: &Db,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("inventory_run_finish");
            return db
                .inventory_run_finish(run_id, state, manifest_key, error, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("inventory_run_finish")?;
        let Some(mut run) = Self::read_run(db, run_id).await? else {
            return Ok(());
        };
        if run_terminal(&run.state) {
            if run.state != state
                || run.manifest_key.as_deref() != manifest_key
                || run.error.as_deref() != error
            {
                return Err(format!("inventory run {run_id}: terminal row changed"));
            }
            // 同内容重放只推进 fence（换 leader 后任务重试不应因终态冻结失败）。
            if fence.term_i64() < run.fence_term
                || (run.fence_term == fence.term_i64() && run.fence_leader == fence.leader_i64())
            {
                return Ok(());
            }
            run.fence_term = fence.term_i64();
            run.fence_leader = fence.leader_i64();
            return self
                .commit_inventory(db, vec![run], Vec::new(), Vec::new())
                .await;
        }
        if fence.term_i64() < run.fence_term {
            return Ok(());
        }
        run.state = state.to_string();
        run.manifest_key = manifest_key.map(str::to_string);
        run.error = error.map(str::to_string);
        run.fence_term = fence.term_i64();
        run.fence_leader = fence.leader_i64();
        self.commit_inventory(db, vec![run], Vec::new(), Vec::new())
            .await
    }

    /// 写增量水位（`None`/空串 → 删除游标并关闭增量，回到全量起点）。
    #[allow(clippy::too_many_arguments)]
    pub async fn inventory_cursor_set(
        &self,
        db: &Db,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("inventory_cursor_set");
            return db
                .inventory_cursor_set(bucket, config_id, config_updated_at, cursor, now, fence)
                .await
                .map_err(err);
        }
        let Some(cursor) = cursor.filter(|c| !c.is_empty()) else {
            // 删除不读本地行，可从 follower 转发（幂等；重复 apply 为 no-op）。
            return self.clear_incremental(bucket, config_id).await;
        };
        let _guard = self.lock.lock().await;
        self.require_leader("inventory_cursor_set")?;
        let cur = Self::read_cursor(db, bucket, config_id).await?;
        let row = InventoryCursorRow {
            bucket: bucket.to_string(),
            config_id: config_id.to_string(),
            cursor: cursor.to_string(),
            config_updated_at,
            updated_at: now.max(cur.as_ref().map_or(0, |c| c.updated_at.saturating_add(1))),
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
        };
        cursor_row_ok(cur.as_ref(), &row).map_err(err)?;
        self.commit_inventory(db, Vec::new(), vec![row], Vec::new())
            .await
    }

    /// 增量开关：true 写复制行，false 原子删除开关与水位。
    pub async fn inventory_incremental_set(
        &self,
        db: &Db,
        bucket: &str,
        config_id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("inventory_incremental_set");
            return db
                .inventory_incremental_set(bucket, config_id, enabled, now)
                .await
                .map_err(err);
        }
        if !enabled {
            // 同 cursor 删除：无读改写，允许 follower 转发到 leader 落日志。
            return self.clear_incremental(bucket, config_id).await;
        }
        let _guard = self.lock.lock().await;
        self.require_leader("inventory_incremental_set")?;
        let cur = Self::read_incremental(db, bucket, config_id).await?;
        let row = InventoryIncrementalRow {
            bucket: bucket.to_string(),
            config_id: config_id.to_string(),
            enabled: true,
            updated_at: now.max(cur.as_ref().map_or(0, |r| r.updated_at.saturating_add(1))),
        };
        incremental_row_ok(cur.as_ref(), &row).map_err(err)?;
        self.commit_inventory(db, Vec::new(), Vec::new(), vec![row])
            .await
    }
}
