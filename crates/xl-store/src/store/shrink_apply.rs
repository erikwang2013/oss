//! M3 shrink T5–T7：离线 apply / verify / commit / rollback。
//!
//! 铁律：`ShardRef.drive` 是槽号，apply 只把「槽号 → 物理盘」的落点从源搬到目标，
//! **不动 xl.meta**。每个节点各自执行：apply/verify/rollback 只处理 `to_node == 本机`
//! 的迁移——commit 前目标槽在旧拓扑里仍属退役节点，跨节点 `write_shard` 会被归属校验
//! 拒绝，故写入只能由目标节点本地完成（源分片走旧拓扑的 read_shard）。
//! commit 逐盘原子写拓扑 v4；旧分片在 commit 前一直可读。内部件见 `shrink_apply_util.rs`。

use std::collections::BTreeMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::layout;

use super::Store;
use super::ec;
use super::shrink::{ShrinkPlanReport, acquire_shrink_lock};
use super::shrink_apply_util::{Receipt, check_receipt, read_receipt, work_dir, write_receipt};

/// apply/commit/rollback 的机器可读回执（CLI `--json`）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShrinkActionReport {
    pub action: String,
    pub node_id: String,
    pub old_epoch: u64,
    pub new_epoch: u64,
    /// 本机为目标的迁移槽数（其余节点执行其余槽）。
    pub local_slots: usize,
    pub files_copied: u64,
    pub files_skipped: u64,
    pub bytes_copied: u64,
    pub reconstructed: u64,
    pub files_removed: u64,
    pub topo_written: usize,
    pub already_committed: bool,
    pub receipt: Option<String>,
    pub elapsed_ms: u64,
}

/// verify 的机器可读回执；`failures` 非空 = 退出码 1。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShrinkVerifyReport {
    pub node_id: String,
    pub old_epoch: u64,
    pub new_epoch: u64,
    pub local_slots: usize,
    pub shards_checked: u64,
    pub objects_read: u64,
    pub failures: Vec<String>,
    pub receipt: Option<String>,
    pub elapsed_ms: u64,
}

impl Store {
    /// 只处理 `to_node == 本机` 的迁移；其余迁移由对应目标节点各自执行。
    pub async fn shrink_apply(&self, report: &ShrinkPlanReport) -> Result<ShrinkActionReport> {
        let plan = &report.plan;
        let started = Instant::now();
        let _lock = acquire_shrink_lock(self.root())?;
        let ctx = self.plan_ctx(plan)?;
        self.validate_live(plan).await?;
        if self.namespace_digest().await? != plan.namespace_digest {
            return Err(Error::Encode(
                "计划后命名空间已变化（维护模式下不应发生）；重新生成 plan".into(),
            ));
        }
        let cl = self.cluster_ctx()?;
        let work = work_dir(self.root(), plan);
        // apply 可能改目标文件：旧 verify 收据作废（commit 必须重新 verify）。
        let _ = std::fs::remove_file(work.join("verify.receipt"));

        let data = self.collect_moved(&ctx.local_moves).await?;
        let mut rep = ShrinkActionReport {
            action: "apply".into(),
            node_id: ctx.self_id.clone(),
            old_epoch: plan.old_epoch,
            new_epoch: plan.new_epoch,
            local_slots: ctx.local_moves.len(),
            ..Default::default()
        };
        for ms in &data.shards {
            let mv = ctx
                .local_moves
                .get(&(ms.set, ms.slot))
                .expect("collect 已按 local_moves 过滤");
            let target_drive = self.target_drive(cl, ms.set, mv.to_drive)?;
            let target =
                super::drives::shard_path(target_drive, &ms.bucket, &ms.key, &ms.uuid, &ms.shard)?;
            if let Ok(b) = tokio::fs::read(&target).await
                && ec::crc32c_of(&b) == ms.shard.crc32c
            {
                rep.files_skipped += 1;
                continue;
            }
            let source = self.slot_target(ms.set, ms.slot).ok_or_else(|| {
                Error::Encode(format!("源槽 set {} slot {} 无法解析", ms.set, ms.slot))
            })?;
            let mut bytes = self
                .read_shard(&ms.bucket, &ms.key, &ms.uuid, &ms.shard, &source, ms.set)
                .await
                .filter(|b| ec::crc32c_of(b) == ms.shard.crc32c);
            if bytes.is_none() {
                bytes = Some(self.reconstruct_shard(ms).await?);
                rep.reconstructed += 1;
            }
            let bytes = bytes.expect("上面已补齐");
            if ec::crc32c_of(&bytes) != ms.shard.crc32c {
                return Err(Error::Encode(format!(
                    "分片 CRC32C 不符：{}/{} uuid {} shard {}（源损坏且重建也失败）",
                    ms.bucket, ms.key, ms.uuid, ms.shard.shard
                )));
            }
            self.write_target(target_drive, ms, &bytes).await?;
            rep.files_copied += 1;
            rep.bytes_copied += bytes.len() as u64;
        }
        let receipt = Receipt::new(
            "apply",
            &ctx.self_id,
            plan,
            rep.files_copied,
            rep.bytes_copied,
            rep.reconstructed,
        );
        let path = work.join("apply.receipt");
        write_receipt(&path, &receipt)?;
        rep.receipt = Some(path.display().to_string());
        rep.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(rep)
    }

    /// 只读校验本机目标副本；任一失败进 `failures`（CLI 退出非零）。
    pub async fn shrink_verify(&self, report: &ShrinkPlanReport) -> Result<ShrinkVerifyReport> {
        let plan = &report.plan;
        let started = Instant::now();
        let _lock = acquire_shrink_lock(self.root())?;
        let ctx = self.plan_ctx(plan)?;
        self.validate_live(plan).await?;
        if self.namespace_digest().await? != plan.namespace_digest {
            return Err(Error::Encode(
                "计划后命名空间已变化（维护模式下不应发生）；重新生成 plan".into(),
            ));
        }
        let cl = self.cluster_ctx()?;
        let data = self.collect_moved(&ctx.local_moves).await?;
        let mut rep = ShrinkVerifyReport {
            node_id: ctx.self_id.clone(),
            old_epoch: plan.old_epoch,
            new_epoch: plan.new_epoch,
            local_slots: ctx.local_moves.len(),
            ..Default::default()
        };
        // 全盘巡检：目标副本在 commit 前会被旧拓扑视为 stray（预期），故不按 stray 判失败。
        match self.fsck(false).await {
            Ok(r) => {
                if r.corrupt_meta > 0 || r.missing_shards > 0 || r.bad_shards > 0 {
                    rep.failures.push(format!(
                        "fsck：meta 损坏 {} / 缺片 {} / 坏片 {}",
                        r.corrupt_meta, r.missing_shards, r.bad_shards
                    ));
                }
                if !r.unreachable_nodes.is_empty() {
                    rep.failures.push(format!(
                        "fsck：不可达节点 {:?}（巡检不完整）",
                        r.unreachable_nodes
                    ));
                }
            }
            Err(e) => rep.failures.push(format!("fsck 失败: {e}")),
        }
        // 逐槽目标副本 CRC + 数量（与 plan 统计对账）。
        let expected: BTreeMap<(u16, u16), u64> = report
            .slots
            .iter()
            .map(|s| ((s.set, s.slot), s.shards))
            .collect();
        let mut found: BTreeMap<(u16, u16), u64> = BTreeMap::new();
        for ms in &data.shards {
            let mv = ctx
                .local_moves
                .get(&(ms.set, ms.slot))
                .expect("collect 已按 local_moves 过滤");
            let drive = self.target_drive(cl, ms.set, mv.to_drive)?;
            let path = super::drives::shard_path(drive, &ms.bucket, &ms.key, &ms.uuid, &ms.shard)?;
            *found.entry((ms.set, ms.slot)).or_default() += 1;
            match tokio::fs::read(&path).await {
                Ok(b) if ec::crc32c_of(&b) == ms.shard.crc32c => rep.shards_checked += 1,
                Ok(_) => rep.failures.push(format!(
                    "目标副本 CRC 不符：{}/{} uuid {} shard {}（set {} slot {}）",
                    ms.bucket, ms.key, ms.uuid, ms.shard.shard, ms.set, ms.slot
                )),
                Err(e) => rep.failures.push(format!(
                    "目标副本缺失/不可读：{}/{} uuid {} shard {}（set {} slot {}）: {e}",
                    ms.bucket, ms.key, ms.uuid, ms.shard.shard, ms.set, ms.slot
                )),
            }
        }
        for &(set, slot) in ctx.local_moves.keys() {
            let want = expected.get(&(set, slot)).copied().ok_or_else(|| {
                Error::Encode(format!(
                    "plan 缺少 set {set} slot {slot} 的统计（报告被改动？）"
                ))
            })?;
            let got = found.get(&(set, slot)).copied().unwrap_or(0);
            if got != want {
                rep.failures.push(format!(
                    "set {set} slot {slot} 目标分片数 {got} ≠ plan 统计 {want}"
                ));
            }
        }
        // 全读受影响版本：验证源侧对象仍可解码/校验 SHA256（目标副本由上面的 CRC 覆盖）。
        for (bucket, key, v) in &data.versions {
            match self.read_version_data(bucket, key, v, None, None).await {
                Ok(Some(_)) => rep.objects_read += 1,
                Ok(None) => rep.failures.push(format!("全读返回缺失：{bucket}/{key}")),
                Err(e) => rep.failures.push(format!("全读失败：{bucket}/{key}: {e}")),
            }
        }
        if rep.failures.is_empty() {
            let receipt = Receipt::new("verify", &ctx.self_id, plan, rep.shards_checked, 0, 0);
            let path = work_dir(self.root(), plan).join("verify.receipt");
            write_receipt(&path, &receipt)?;
            rep.receipt = Some(path.display().to_string());
        }
        rep.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(rep)
    }

    /// 仅 apply + verify 收据齐备时写新拓扑 v4（逐盘 temp+rename）。
    pub async fn shrink_commit(&self, report: &ShrinkPlanReport) -> Result<ShrinkActionReport> {
        let plan = &report.plan;
        let started = Instant::now();
        let _lock = acquire_shrink_lock(self.root())?;
        let ctx = self.plan_ctx(plan)?;
        self.validate_live(plan).await.map_err(|e| {
            Error::Encode(format!(
                "commit 要求全部节点仍运行旧拓扑（先在所有节点 commit，完毕后再逐节点换新配置重启）：{e}"
            ))
        })?;
        let cl = self.cluster_ctx()?;
        let work = work_dir(self.root(), plan);
        for action in ["apply", "verify"] {
            let r = read_receipt(&work.join(format!("{action}.receipt")))?;
            check_receipt(&r, action, plan, &ctx.self_id)?;
        }
        // verify 之后仍可能有写入（维护门之外的收尾/竞态）；写 v4 前再核一次命名空间。
        // 摘要只含 bucket/key：已存在 key 的新版本写入不可见，依赖 maintenance 写门拦截。
        let digest = self.namespace_digest().await?;
        if digest != plan.namespace_digest {
            return Err(Error::Encode(format!(
                "commit 前命名空间已变化（verify 后发生写入）：plan={} now={digest}；请重新 plan/apply/verify",
                plan.namespace_digest
            )));
        }
        let mut rep = ShrinkActionReport {
            action: "commit".into(),
            node_id: ctx.self_id.clone(),
            old_epoch: plan.old_epoch,
            new_epoch: plan.new_epoch,
            local_slots: ctx.local_moves.len(),
            ..Default::default()
        };
        let mut pending = 0usize;
        for (di, set, slot) in super::local_topo_entries(&cl.sets, self.cfg.drives.len())? {
            let path = layout::sys_dir(&self.cfg.drives[di], "topo");
            let have = tokio::fs::read_to_string(&path).await.unwrap_or_default();
            let have = have.trim();
            if let Some(r) = super::parse_topo_record_v4(have) {
                if r.epoch == plan.new_epoch && r.hash == plan.new_hash {
                    continue; // 已写过（重跑 commit 幂等）
                }
                return Err(Error::Encode(format!(
                    "盘 {} 已有其它 v4 拓扑记录 «{have}»（不是本次 commit 的目标 epoch/hash）",
                    self.cfg.drives[di].display()
                )));
            }
            let want = super::topo_record(
                set,
                &ctx.self_id,
                slot,
                Some((plan.new_epoch, plan.new_hash.as_str())),
            );
            super::write_topo_record(&path, &want).await?;
            pending += 1;
        }
        rep.topo_written = pending;
        rep.already_committed = pending == 0;
        rep.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(rep)
    }

    /// 仅 commit 前允许：删除本机目标副本，旧拓扑/旧分片原样保留。
    pub async fn shrink_rollback(&self, report: &ShrinkPlanReport) -> Result<ShrinkActionReport> {
        let plan = &report.plan;
        let started = Instant::now();
        let _lock = acquire_shrink_lock(self.root())?;
        // 已 commit：新配置下 caps 即新 epoch/hash；旧配置下盘上记录已变——都先于 plan 校验拒绝。
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let caps = self.local_caps();
            if caps.topo_epoch == plan.new_epoch && caps.topo_hash == plan.new_hash {
                return Err(Error::Encode(
                    "拓扑已 commit（本机 epoch/hash = 新拓扑）；rollback 仅允许 commit 前".into(),
                ));
            }
            if self.local_any_new_topo(&cl.sets, plan)? {
                return Err(Error::Encode(
                    "已开始/完成 commit（盘上存在新拓扑 v4 记录）；rollback 仅允许 commit 前"
                        .into(),
                ));
            }
        }
        let ctx = self.plan_ctx(plan)?;
        // 对端「已写盘但未重启」没有线上信号，无法跨节点检测；运行手册要求 rollback 只在任何节点 commit 之前。
        self.validate_live(plan).await.map_err(|e| {
            Error::Encode(format!(
                "rollback 仅允许 commit 前且全部节点仍运行旧拓扑（对端已换新配置 = 已有节点 commit）：{e}"
            ))
        })?;
        let cl = self.cluster_ctx()?;
        let data = self.collect_moved(&ctx.local_moves).await?;
        let mut rep = ShrinkActionReport {
            action: "rollback".into(),
            node_id: ctx.self_id.clone(),
            old_epoch: plan.old_epoch,
            new_epoch: plan.new_epoch,
            local_slots: ctx.local_moves.len(),
            ..Default::default()
        };
        for ms in &data.shards {
            let mv = ctx
                .local_moves
                .get(&(ms.set, ms.slot))
                .expect("collect 已按 local_moves 过滤");
            let drive = self.target_drive(cl, ms.set, mv.to_drive)?;
            let path = super::drives::shard_path(drive, &ms.bucket, &ms.key, &ms.uuid, &ms.shard)?;
            match tokio::fs::read(&path).await {
                Ok(b) if ec::crc32c_of(&b) == ms.shard.crc32c => {
                    tokio::fs::remove_file(&path).await?;
                    rep.files_removed += 1;
                    // 只删空目录（对象目录里还有 xl.meta 时 remove_dir 自然失败）。
                    if let Some(d) = path.parent() {
                        let _ = tokio::fs::remove_dir(d).await;
                    }
                    if let Ok(obj) = layout::object_dir(drive, &ms.bucket, &ms.key) {
                        let _ = tokio::fs::remove_dir(obj).await;
                    }
                }
                Ok(_) => {
                    return Err(Error::Encode(format!(
                        "目标副本内容与 meta 记录不符，拒绝删除：{}/{} shard {}（set {} slot {}）",
                        ms.bucket, ms.key, ms.shard.shard, ms.set, ms.slot
                    )));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        let work = work_dir(self.root(), plan);
        let _ = std::fs::remove_file(work.join("apply.receipt"));
        let _ = std::fs::remove_file(work.join("verify.receipt"));
        let _ = std::fs::remove_dir(&work);
        rep.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(rep)
    }
}
