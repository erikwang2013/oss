//! Lifecycle 扫描游标的复制写入门面（M8-2 下一片）。
//!
//! 与 inventory 同 level：Raft 且本机 advertised feature ≥5 时，leader 在进程内
//! mutex 下读取已 apply 的游标行、生成下一整行，经 [`ControlPlane::write`] 提交；
//! follower 不得读旧行转发。feature<5 或非 Raft 保持节点本地直写（本地语义：只有
//! `fence_term <= token` 才覆盖/删除）。删除（阶段扫完）是盲写，可从 follower 转发：
//! fence 随命令携带，apply 只删不高于该 term 的行，旧 term 的迟到删除不会抹掉新
//! leader 已推进的进度。

use super::*;
use crate::raft::apply::lifecycle::cursor_row_ok;
use crate::raft::runstate::{LifecycleCursorRow, lifecycle_cursor_row_from};

impl JobState {
    async fn read_lifecycle_cursor(
        db: &Db,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<LifecycleCursorRow>, String> {
        let row = sqlx::query(
            "SELECT bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader \
             FROM lifecycle_cursors WHERE bucket = ? AND rule_id = ? AND stage = ?",
        )
        .bind(bucket)
        .bind(rule_id)
        .bind(stage)
        .fetch_optional(db.pool())
        .await
        .map_err(err)?;
        Ok(row.as_ref().map(lifecycle_cursor_row_from))
    }

    /// 引用校验前移 append 前：桶已删除时不复活游标（最坏丢本轮进度，重扫安全）。
    async fn lifecycle_bucket_exists(db: &Db, bucket: &str) -> Result<bool, String> {
        let hit: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(hit.is_some())
    }

    async fn commit_lifecycle_cursor(&self, cursor: LifecycleCursorRow) -> Result<(), String> {
        let Some(control) = &self.control else {
            return Err("lifecycle cursor commit without control plane".into());
        };
        match control
            .write(ControlCommand::LifecycleCursorUpsert { cursor })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("lifecycle cursor rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn clear_lifecycle_cursor(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        let Some(control) = &self.control else {
            return Err("lifecycle cursor clear without control plane".into());
        };
        match control
            .write(ControlCommand::LifecycleCursorClear {
                bucket: bucket.to_string(),
                rule_id: rule_id.to_string(),
                stage: stage.to_string(),
                fence_term: fence.term_i64(),
            })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("lifecycle cursor clear rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    /// 持久化/清除扫描游标（`None`/空串 → 删除行，下轮从头扫）。
    #[allow(clippy::too_many_arguments)] // 参数面=调用点字段面；拆结构体只多一层搬运。
    pub async fn lifecycle_cursor_set(
        &self,
        db: &Db,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.lifecycle_replicated() {
            self.note_local("lifecycle_cursor_set");
            return db
                .lifecycle_cursor_set(bucket, rule_id, stage, cursor, now, fence)
                .await
                .map_err(err);
        }
        let Some(cursor) = cursor.filter(|c| !c.is_empty()) else {
            return self
                .clear_lifecycle_cursor(bucket, rule_id, stage, fence)
                .await;
        };
        let _guard = self.lock.lock().await;
        self.leader_ready_for("lifecycle_cursor_set", compat::FEATURE_LEVEL_5)
            .await?;
        let cur = Self::read_lifecycle_cursor(db, bucket, rule_id, stage).await?;
        if cur
            .as_ref()
            .is_some_and(|c| fence.term_i64() < c.fence_term)
        {
            return Ok(()); // 旧 term 迟到写入：no-op（不 append）。
        }
        if let Some(c) = &cur
            && c.cursor == cursor
            && c.fence_term == fence.term_i64()
            && c.fence_leader == fence.leader_i64()
        {
            return Ok(()); // 同内容同 token 重写：无逻辑变化。
        }
        if !Self::lifecycle_bucket_exists(db, bucket).await? {
            return Err(format!(
                "lifecycle cursor {bucket}/{rule_id}/{stage}: bucket missing"
            ));
        }
        let row = LifecycleCursorRow {
            bucket: bucket.to_string(),
            rule_id: rule_id.to_string(),
            stage: stage.to_string(),
            cursor: cursor.to_string(),
            updated_at: now.max(cur.as_ref().map_or(0, |c| c.updated_at.saturating_add(1))),
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
        };
        cursor_row_ok(cur.as_ref(), &row).map_err(err)?;
        self.commit_lifecycle_cursor(row).await
    }
}
