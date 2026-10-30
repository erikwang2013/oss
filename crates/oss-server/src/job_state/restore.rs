//! Tier restore 后续状态的复制写入门面（feature 7）。
//!
//! Raft 且 advertised feature ≥7：leader 在进程内 mutex 下读取已 apply 的整行（含
//! fence），提交 [`ControlCommand::TierRestoreUpsert`]；apply 按自然键整行 upsert。
//! follower 不得读旧行转发。清理删除是盲写，`fence_term` 随命令携带，可从任意节点
//! 转发（apply 只删不高于该 term 的行）。feature<7 或非 Raft 保持本地直写，行为不变。

use super::*;
use crate::raft::apply::restore_row_ok;
use crate::raft::runstate::tier_restore_row_from;

impl JobState {
    async fn read_tier_restore(
        db: &Db,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<TierRestoreRow>, String> {
        let row = sqlx::query(
            "SELECT bucket, key, version_id, expires_at, job_id, state, created_at, \
             fence_term, fence_leader FROM tier_restores \
             WHERE bucket = ? AND key = ? AND version_id = ?",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .fetch_optional(db.pool())
        .await
        .map_err(err)?;
        Ok(row.as_ref().map(tier_restore_row_from))
    }

    async fn commit_tier_restore(&self, restore: TierRestoreRow) -> Result<(), String> {
        let Some(control) = &self.control else {
            return Err("tier restore commit without control plane".into());
        };
        match control
            .write(ControlCommand::TierRestoreUpsert { restore })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("tier restore rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    /// 执行侧状态转换（restored / 回滚 pending）与对账收敛。
    #[allow(clippy::too_many_arguments)] // 参数面=行字段面；拆结构体只多一层搬运。
    pub async fn tier_restore_set_state(
        &self,
        db: &Db,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.restore_replicated() {
            self.note_local("tier_restore_set_state");
            return db
                .set_restore_state_fenced(bucket, key, version_id, state, expires_at, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready_for("tier_restore_set_state", compat::FEATURE_LEVEL_7)
            .await?;
        let Some(cur) = Self::read_tier_restore(db, bucket, key, version_id).await? else {
            return Ok(()); // 行已被清理：不复活。
        };
        if fence.term_i64() < cur.fence_term {
            return Ok(()); // 旧 term 迟到写入：no-op（不 append）。
        }
        let next = TierRestoreRow {
            state: state.to_string(),
            expires_at,
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
            ..cur.clone()
        };
        if next == cur {
            return Ok(()); // 同内容同 token 重写：无逻辑变化。
        }
        restore_row_ok(Some(&cur), &next).map_err(err)?;
        self.commit_tier_restore(next).await
    }

    /// 清理 Restore 行（幂等；`fence_term` 上限让旧 term 迟到删除 no-op）。
    /// 可从 follower 调用：控制面会向 leader 转交一次。
    pub async fn tier_restore_delete(
        &self,
        db: &Db,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.restore_replicated() {
            self.note_local("tier_restore_delete");
            return db
                .delete_restore_fenced(bucket, key, version_id, fence)
                .await
                .map_err(err);
        }
        let Some(control) = &self.control else {
            return Err("tier restore delete without control plane".into());
        };
        match control
            .write(ControlCommand::TierRestoreDelete {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version_id.to_string(),
                fence_term: fence.term_i64(),
            })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("tier restore delete rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }
}
