//! M8-2：jobs/job_tasks 运行态写入门面。
//!
//! 复制模式下（Raft 且本机 advertised feature ≥4）所有作业行/任务行写入都以完整行
//! 经 [`ControlPlane::write`] 提交，apply 按主键 upsert；follower 只 apply，不领取。
//! leader 生效级别 <4（有不可达/未升级成员）时复制写 fail-closed，绝不退回本地直写。
//! 非 Raft 或 feature<4 保持 M8-1 的本地直写（debug 日志记录降级），行为不变。
//!
//! 读-改-写（领取/收尾/抢占/回收）只允许 leader，并在进程内用一把 mutex 串行化：
//! 读本行 → 合并计数 → 提交下一行之间不能被同进程其他 worker 插入。身份/引用/版本
//! 校验在 append 前完成；apply 只按日志事实做确定性的整批 upsert。
//!
//! 已知限制：job/run id 仍由 leader 本地分配（`runstate_meta`/`MAX(id)+1`），新 leader
//! 先等 apply 追平再分配。Restore 受理的 `tier_restores` 行与作业同批进日志（P2-1），
//! 之后的 restored/清理见 [`restore`] 子模块（feature 7，leader 读改写 + 幂等删除）。
//! inventory 运行态（runs/cursors/incrementals）见 [`inventory`] 子模块；lifecycle
//! 扫描游标见 [`lifecycle`] 子模块（同为 feature 5）；调度租约 acquire/renew/takeover
//! 见 [`lease`] 子模块（feature 7，leader 调度前先取得租约）。

mod admin;
mod inventory;
mod lease;
mod lifecycle;
mod restore;
mod worker;

pub use lease::schedule_lease_ttl;

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::db::{Db, JobSpec, NewJobTask};
use crate::raft::command::{ControlCommand, ControlResponse};
use crate::raft::control::ControlPlane;
use crate::raft::runstate::{JobRow, JobTaskRow, TierRestoreRow, job_row_from, job_task_row_from};
use crate::raft::{FencingEpoch, compat};

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn new_job(spec: &JobSpec, id: i64, now: i64, total: i64) -> JobRow {
    JobRow {
        id,
        kind: spec.kind.clone(),
        bucket: spec.bucket.clone(),
        prefix: spec.prefix.clone(),
        state: "queued".into(),
        total,
        priority: spec.priority,
        requested_by: spec.requested_by,
        parent_job_id: spec.parent_job_id,
        report_bucket: spec.report_bucket.clone(),
        report_prefix: spec.report_prefix.clone(),
        spec_json: spec.spec_json.clone(),
        created_at: now,
        updated_at: now,
        ..JobRow::default()
    }
}

fn new_task(job_id: i64, task_no: i64, task: &NewJobTask, now: i64) -> JobTaskRow {
    JobTaskRow {
        job_id,
        task_no,
        action: task.action.clone(),
        payload_json: task.payload.clone(),
        state: "queued".into(),
        next_at: now,
        dedupe_key: task.dedupe_key.clone(),
        ..JobTaskRow::default()
    }
}

/// 作业行的逻辑版本：每次 leader 写都从已提交行 +1，跨节点顺序不依赖墙钟。
fn bump(job: &mut JobRow, now: i64) {
    job.updated_at = now.max(job.updated_at.saturating_add(1));
}

pub struct JobState {
    control: Option<Arc<ControlPlane>>,
    lock: Mutex<()>,
}

impl JobState {
    pub fn new(control: Option<Arc<ControlPlane>>) -> Self {
        Self {
            control,
            lock: Mutex::new(()),
        }
    }

    /// 无控制面（单机/测试）：全部本地直写，与 M8-1 完全一致。
    pub fn local() -> Self {
        Self::new(None)
    }

    /// Raft 且本机配置 advertised feature≥4 → jobs 必须走复制日志（leader 生效级别
    /// 不足时写入 fail-closed，绝不退回本地直写）。Local/feature<4 → 保持节点本地。
    pub fn replicated(&self) -> bool {
        self.replicated_at(compat::FEATURE_LEVEL_4)
    }

    /// inventory 复制是 level 5：v1.23.0 已 advertise 4 但不认识 Inventory*。
    pub fn inventory_replicated(&self) -> bool {
        self.replicated_at(compat::FEATURE_LEVEL_5)
    }

    /// lifecycle 游标与 Inventory* 共用 level 5（同一批 level-5 二进制）。
    pub fn lifecycle_replicated(&self) -> bool {
        self.replicated_at(compat::FEATURE_LEVEL_5)
    }

    /// tier restore 后续状态是 level 7：v1.25.0 已 advertise 6 但不认识这些命令。
    pub fn restore_replicated(&self) -> bool {
        self.replicated_at(compat::FEATURE_LEVEL_7)
    }

    /// 调度租约与 restore 后续状态同属 level 7。
    pub fn lease_replicated(&self) -> bool {
        self.replicated_at(compat::FEATURE_LEVEL_7)
    }

    fn replicated_at(&self, level: u8) -> bool {
        self.control
            .as_ref()
            .and_then(|c| c.raft_handle())
            .is_some_and(|handle| handle.advertised_feature() >= level)
    }

    fn note_local(&self, op: &str) {
        // debug：worker 每 2s 一轮，info 会刷屏；启动时另有一次性 info。
        tracing::debug!(
            op,
            "job 状态写入保持节点本地（raft feature 未达所需 level 或未启用）"
        );
    }

    fn require_leader(&self, op: &str) -> Result<(), String> {
        self.require_leader_for(op, compat::FEATURE_LEVEL_4)
    }

    /// 复制写只允许 leader 生成完整行（follower 不得读旧行转发）；生效级别不足时
    /// 返回明确错误而不是本地写。
    fn require_leader_for(&self, op: &str, required: u8) -> Result<(), String> {
        let Some(control) = &self.control else {
            return Ok(());
        };
        if !control.is_leader() {
            return Err(format!("{op}: not leader; retry on the leader"));
        }
        if let Some(handle) = control.raft_handle() {
            match handle.effective_feature() {
                Some(level) if level >= required => {}
                Some(level) => {
                    return Err(format!(
                        "{op}: raft effective feature {level} < {required}; writes fail closed \
                         (no local fallback while [raft].feature_level>={required}) until \
                         unreachable or older members are removed or upgraded"
                    ));
                }
                None => return Err(format!("{op}: raft effective feature unavailable")),
            }
        }
        Ok(())
    }

    /// 新 leader 先等本地 apply 追平，再读 id/去重键，避免用滞后状态分配 id（P2-3）。
    async fn leader_ready(&self, op: &str) -> Result<(), String> {
        self.leader_ready_for(op, compat::FEATURE_LEVEL_4).await
    }

    async fn leader_ready_for(&self, op: &str, required: u8) -> Result<(), String> {
        self.require_leader_for(op, required)?;
        if let Some(control) = &self.control
            && !control
                .wait_caught_up(std::time::Duration::from_secs(2))
                .await
        {
            return Err(format!("{op}: local raft state not caught up"));
        }
        Ok(())
    }

    /// 提交前校验身份/引用/版本（P1-2）：apply 只按日志事实 upsert，不再读副本表判 Conflict。
    async fn commit(
        &self,
        db: &Db,
        jobs: Vec<JobRow>,
        tasks: Vec<JobTaskRow>,
        restores: Vec<TierRestoreRow>,
    ) -> Result<(), String> {
        if jobs.is_empty() && tasks.is_empty() && restores.is_empty() {
            return Ok(());
        }
        let mut job_ids = HashSet::new();
        for job in &jobs {
            if !job_ids.insert(job.id) {
                return Err(format!("job batch: duplicate job id {}", job.id));
            }
        }
        let mut task_ids = HashSet::new();
        for task in &tasks {
            if !task_ids.insert((task.job_id, task.task_no)) {
                return Err(format!(
                    "job batch: duplicate task {}/{}",
                    task.job_id, task.task_no
                ));
            }
        }
        for job in &jobs {
            if let Some(cur) = Self::read_job(db, job.id).await? {
                if cur.kind != job.kind
                    || cur.created_at != job.created_at
                    || cur.bucket != job.bucket
                    || cur.prefix != job.prefix
                    || cur.spec_json != job.spec_json
                {
                    return Err(format!("job {} identity conflict", job.id));
                }
                if job.updated_at <= cur.updated_at {
                    return Err(format!("job {} stale version", job.id));
                }
            }
        }
        for task in &tasks {
            if !job_ids.contains(&task.job_id) && Self::read_job(db, task.job_id).await?.is_none() {
                return Err(format!(
                    "job {} missing for task {}",
                    task.job_id, task.task_no
                ));
            }
        }
        let mut restore_keys = HashSet::new();
        for restore in &restores {
            let key = (
                restore.bucket.clone(),
                restore.key.clone(),
                restore.version_id.clone(),
            );
            if !restore_keys.insert(key) {
                return Err(format!(
                    "job batch: duplicate restore {}/{}/{}",
                    restore.bucket, restore.key, restore.version_id
                ));
            }
        }
        let Some(control) = &self.control else {
            return Err("job state commit without control plane".into());
        };
        match control
            .write(ControlCommand::JobBatch {
                jobs,
                tasks,
                restores,
            })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("job batch rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn read_job(db: &Db, id: i64) -> Result<Option<JobRow>, String> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(job_row_from))
    }

    async fn read_task(db: &Db, job_id: i64, task_no: i64) -> Result<Option<JobTaskRow>, String> {
        let row = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND task_no = ?")
            .bind(job_id)
            .bind(task_no)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(job_task_row_from))
    }

    async fn next_job_id(db: &Db) -> Result<i64, String> {
        let meta: Option<i64> =
            sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = 'next_job_id'")
                .fetch_optional(db.pool())
                .await
                .map_err(err)?;
        let max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM jobs")
            .fetch_one(db.pool())
            .await
            .map_err(err)?;
        Ok(meta.unwrap_or(0).max(max + 1).max(1))
    }

    pub async fn create_job(&self, db: &Db, spec: &JobSpec, now: i64) -> Result<i64, String> {
        if !self.replicated() {
            self.note_local("create_job");
            return db.create_job(spec, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("create_job").await?;
        let id = Self::next_job_id(db).await?;
        let tasks: Vec<JobTaskRow> = spec
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| new_task(id, i as i64 + 1, t, now))
            .collect();
        self.commit(
            db,
            vec![new_job(spec, id, now, tasks.len() as i64)],
            tasks,
            Vec::new(),
        )
        .await?;
        Ok(id)
    }

    /// Restore 受理：作业行与 `tier_restores` pending 行同一 JobBatch 提交，换 leader
    /// 后新 leader 也能看到该行（不再有“本地补写”窗口）。
    #[allow(clippy::too_many_arguments)]
    pub async fn create_restore(
        &self,
        db: &Db,
        spec: &JobSpec,
        bucket: &str,
        key: &str,
        version_id: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<i64, String> {
        if !self.replicated() {
            self.note_local("create_restore");
            return db
                .create_restore_job(spec, bucket, key, version_id, now, fence)
                .await
                .map(|(id, _)| id)
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("create_restore").await?;
        let id = Self::next_job_id(db).await?;
        let tasks: Vec<JobTaskRow> = spec
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| new_task(id, i as i64 + 1, t, now))
            .collect();
        let restore = TierRestoreRow {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.to_string(),
            expires_at: 0,
            job_id: Some(id),
            state: "pending".into(),
            created_at: now,
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
        };
        self.commit(
            db,
            vec![new_job(spec, id, now, tasks.len() as i64)],
            tasks,
            vec![restore],
        )
        .await?;
        Ok(id)
    }

    /// Inventory 进度（绝对值；调用方用 `scanned-1` 抵消引擎的 +1，因此允许回退值）。
    pub async fn set_progress(
        &self,
        db: &Db,
        id: i64,
        progress: i64,
        now: i64,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("set_progress");
            return db.job_set_progress(id, progress, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("set_progress")?;
        let Some(mut job) = Self::read_job(db, id).await? else {
            return Ok(());
        };
        job.progress = progress;
        bump(&mut job, now);
        self.commit(db, vec![job], Vec::new(), Vec::new()).await
    }

    pub async fn create_job_dedup(
        &self,
        db: &Db,
        spec: &JobSpec,
        now: i64,
    ) -> Result<Option<i64>, String> {
        if !self.replicated() {
            self.note_local("create_job_dedup");
            return db.create_job_dedup(spec, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("create_job_dedup").await?;
        let mut seen: HashSet<String> =
            sqlx::query_scalar("SELECT dedupe_key FROM job_tasks WHERE dedupe_key IS NOT NULL")
                .fetch_all(db.pool())
                .await
                .map_err(err)?
                .into_iter()
                .collect();
        let mut tasks = Vec::new();
        for task in &spec.tasks {
            // 同批重复键也是先到先得（与 INSERT OR IGNORE 一致）。
            if task
                .dedupe_key
                .as_ref()
                .is_none_or(|k| seen.insert(k.clone()))
            {
                tasks.push(new_task(0, tasks.len() as i64 + 1, task, now));
            }
        }
        if tasks.is_empty() {
            return Ok(None);
        }
        let id = Self::next_job_id(db).await?;
        for task in &mut tasks {
            task.job_id = id;
        }
        self.commit(
            db,
            vec![new_job(spec, id, now, tasks.len() as i64)],
            tasks,
            Vec::new(),
        )
        .await?;
        Ok(Some(id))
    }

    /// 失败任务重试：建子作业（新 id）。子作业必须由 leader 分配 id。
    pub async fn job_retry_failed(
        &self,
        db: &Db,
        id: i64,
        now: i64,
    ) -> Result<Option<i64>, String> {
        if !self.replicated() {
            self.note_local("job_retry_failed");
            return db.job_retry_failed(id, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("job_retry_failed").await?;
        let Some(parent) = Self::read_job(db, id).await? else {
            return Ok(None);
        };
        let rows = sqlx::query(
            "SELECT * FROM job_tasks WHERE job_id = ? AND state = 'failed' ORDER BY task_no",
        )
        .bind(id)
        .fetch_all(db.pool())
        .await
        .map_err(err)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let new_id = Self::next_job_id(db).await?;
        let tasks: Vec<JobTaskRow> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut task = job_task_row_from(r);
                task.job_id = new_id;
                task.task_no = i as i64 + 1;
                task.state = "queued".into();
                task.attempt = 0;
                task.next_at = now;
                task.dedupe_key = None;
                task.deadline_at = None;
                task.result_code = None;
                task.result_string = None;
                task.started_at = None;
                task.finished_at = None;
                task.fence_term = 0;
                task.fence_leader = 0;
                task
            })
            .collect();
        let job = JobRow {
            id: new_id,
            kind: parent.kind,
            bucket: parent.bucket,
            prefix: parent.prefix,
            state: "queued".into(),
            total: tasks.len() as i64,
            priority: parent.priority,
            requested_by: parent.requested_by,
            parent_job_id: Some(id),
            report_bucket: parent.report_bucket,
            report_prefix: parent.report_prefix,
            spec_json: parent.spec_json,
            created_at: now,
            updated_at: now,
            ..JobRow::default()
        };
        self.commit(db, vec![job], tasks, Vec::new()).await?;
        Ok(Some(new_id))
    }

    /// level 6 作业历史保留：删除 `cutoff` 之前进入终态的作业历史（每次最多 `batch` 条）。
    ///
    /// 无控制面（单机/测试）：本地事务删除。Raft：必须是 leader 且生效 feature ≥6，
    /// 写入经 [`ControlCommand::JobPurge`] 复制，follower 只 apply；feature <6 返回明确
    /// 错误（worker 跳过），绝不退回本地删除——否则各副本历史分叉。
    pub async fn purge_terminal_before(
        &self,
        db: &Db,
        cutoff: i64,
        batch: i64,
    ) -> Result<usize, String> {
        let limit = batch.clamp(1, crate::raft::command::JOB_PURGE_MAX_IDS as i64);
        let Some(control) = &self.control else {
            return Self::purge_local(db, cutoff, limit).await;
        };
        let Some(handle) = control.raft_handle() else {
            return Self::purge_local(db, cutoff, limit).await;
        };
        if !control.is_leader() {
            return Err("job_purge: not leader; retry on the leader".into());
        }
        match handle.effective_feature() {
            Some(level) if level >= compat::FEATURE_LEVEL_6 => {}
            Some(level) => {
                return Err(format!(
                    "job_purge requires raft feature level 6 (all peers upgraded); \
                     effective level {level}"
                ));
            }
            None => {
                return Err(
                    "job_purge requires raft feature level 6 (all peers upgraded); \
                            effective level unavailable"
                        .into(),
                );
            }
        }
        let ids = db.terminal_jobs_before(cutoff, limit).await.map_err(err)?;
        if ids.is_empty() {
            return Ok(0);
        }
        let n = ids.len();
        match control
            .write(ControlCommand::JobPurge { job_ids: ids })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(n),
            Ok(other) => Err(format!("job_purge rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn purge_local(db: &Db, cutoff: i64, limit: i64) -> Result<usize, String> {
        let ids = db.terminal_jobs_before(cutoff, limit).await.map_err(err)?;
        if ids.is_empty() {
            return Ok(0);
        }
        Ok(db.purge_terminal_jobs_local(&ids).await.map_err(err)? as usize)
    }
}
