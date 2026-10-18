//! 生命周期（M4 切片 6）：按规则删除到期对象；每次执行落一条 jobs 记录（进度可见）。
//!
//! 验收口径（设计 ③）：规则到期**自动删除**（转换/分层归 M8）；jobs 表可见进度。
//! - 到期判定：`now - last_modified >= expire_days * 86400`（expire_days=0 = 立即到期，
//!   主要给测试与激进清理用；AWS 最小 1 天）。
//! - 版本控制桶：删除 = 打 **delete marker**（S3 语义；noncurrent 版本过期归 M4+，记档）；
//!   非版本桶 = 整键删除。
//! - worker 间隔：`OSS_LIFECYCLE_INTERVAL_SECS`（默认 60；显式 0 = 关闭 worker）。
//! - 逐规则 best-effort：单键失败计 skipped 继续；规则级错误记 jobs.error 并继续下一条规则。

use std::time::Duration;

use xl_store::store::ListRequest;

use crate::state::AppState;

/// M6 切片 0：删除结果 → 生命周期事件（`None` = NotFound 空删，**不发事件**）。
/// 抽出纯函数以便确定性单测（并发窄窗的 NotFound 无法在集成层稳定构造）。
pub(crate) fn lifecycle_event(
    outcome: &xl_store::store::DeleteOutcome,
) -> Option<(&'static str, Option<&str>)> {
    match outcome {
        xl_store::store::DeleteOutcome::Marker { version_id } => Some((
            "s3:LifecycleExpiration:DeleteMarkerCreated",
            Some(version_id.as_str()),
        )),
        xl_store::store::DeleteOutcome::Version { .. }
        | xl_store::store::DeleteOutcome::Object { .. } => {
            Some(("s3:LifecycleExpiration:Delete", None))
        }
        xl_store::store::DeleteOutcome::NotFound => None,
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 单次扫描（可测入口）：对全部启用规则各跑一轮，逐规则一条 jobs。返回处理的规则数。
pub async fn sweep_once(state: &AppState) -> usize {
    let rules = match state.db.enabled_lifecycle_rules().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "lifecycle：规则读取失败，本轮跳过");
            return 0;
        }
    };
    let mut ran = 0usize;
    for rule in rules {
        ran += 1;
        let started = now_secs();
        let job = match state
            .db
            .job_start("lifecycle", &rule.bucket, &rule.prefix, started)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(error = %e, bucket = %rule.bucket, "lifecycle：job 开单失败");
                continue;
            }
        };
        let (mut scanned, mut deleted, mut skipped) = (0i64, 0i64, 0i64);
        let mut err: Option<String> = None;

        let versioned = match state.db.get_bucket_versioning(&rule.bucket).await {
            Ok(s) => s == "Enabled",
            Err(e) => {
                err = Some(format!("版本状态读取失败: {e}"));
                false
            }
        };
        // 负值防呆（verifier S6-2 附属）：console 已拦 0..=36500；直写 DB 绕过时这里
        // fail-closed——`.max(0)` 会静默变成「立即全删」
        if rule.expire_days < 0 {
            err = Some(format!(
                "规则 expire_days 为负（{}），拒绝执行",
                rule.expire_days
            ));
        }
        let cutoff = now_secs() - rule.expire_days.max(0) * 86_400;

        if err.is_none() {
            let mut token: Option<String> = None;
            let mut truncated = false;
            let mut pages = 0usize;
            'pages: loop {
                // 防御上限：单轮 ≤10 万键（100×1000）。**截停记 error**（verifier S6-2）：
                // 生命周期是合规功能，「到期未删」必须可见；token 不跨轮持久化，下轮从头续扫
                // （前缀下 >10 万"未到期"键时尾部到期键存在饥饿边界——README 记档）
                if pages >= 100 {
                    truncated = true;
                    break 'pages;
                }
                pages += 1;
                let page = match state
                    .store
                    .list_objects(
                        &rule.bucket,
                        &ListRequest {
                            prefix: Some(rule.prefix.clone()),
                            continuation_token: token.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(p) => p,
                    Err(xl_store::error::Error::NoSuchBucket(_)) => break 'pages,
                    Err(e) => {
                        err = Some(format!("列举失败: {e}"));
                        break 'pages;
                    }
                };
                for o in &page.objects {
                    scanned += 1;
                    if o.last_modified_nanos / 1_000_000_000 > cutoff {
                        continue; // 未到期
                    }
                    // S6-4（verifier）：删前**复读**当前 lmt——按列举快照直删会把「列举后刚被
                    // 覆盖」的新内容一并删掉（非版本桶=数据丢失面；AWS 亦在执行时刻重判）。
                    // 仅在到期候选上复读，开销小；残窗收窄到 head 与 delete 之间的 µs 级。
                    match state.store.head_object(&rule.bucket, &o.key).await {
                        Ok(fresh) if fresh.last_modified_nanos / 1_000_000_000 <= cutoff => {}
                        Ok(_) => continue, // 已被新内容覆盖：跳过（视为未到期）
                        Err(xl_store::error::Error::NoSuchKey(_)) => continue, // 并发已删
                        Err(e) => {
                            tracing::warn!(bucket = %rule.bucket, key = %o.key, error = %e, "lifecycle：复读失败（计 skipped）");
                            skipped += 1;
                            continue;
                        }
                    }
                    // 删除：版本桶打 marker，非版本桶整键删
                    match state
                        .store
                        .delete_object_versioned(&rule.bucket, &o.key, None, versioned)
                        .await
                    {
                        Ok(outcome) => {
                            // M6 切片 0（R5 + P2-1）：事件映射走纯函数；NotFound（并发窄窗
                            // head 后 delete 前被删）→ None：不发事件、不计 deleted。
                            if let Some((name, vid)) = lifecycle_event(&outcome) {
                                state
                                    .events
                                    .emit_named(
                                        name,
                                        &rule.bucket,
                                        &o.key,
                                        vid,
                                        None,
                                        None,
                                        xl_s3::events::EventSource::internal("oss-lifecycle"),
                                    )
                                    .await;
                                // M6 切片 4（V3②）：生命周期删除入队（marker 有版本号；
                                // 非版本桶整键删无版本号，状态面不维护）
                                use xl_s3::replication::ReplicationSink as _;
                                state
                                    .repl_sink
                                    .on_write(xl_s3::replication::WriteInfo {
                                        bucket: &rule.bucket,
                                        key: &o.key,
                                        version_id: vid,
                                        etag: "",
                                        size: 0,
                                        is_delete_marker: true,
                                        is_replica: false,
                                    })
                                    .await;
                                deleted += 1;
                            }
                        }
                        Err(xl_store::error::Error::NoSuchKey(_)) => {} // 并发已删：不计
                        Err(e) => {
                            tracing::warn!(bucket = %rule.bucket, key = %o.key, error = %e, "lifecycle：删除失败（计 skipped）");
                            skipped += 1;
                        }
                    }
                }
                match page.next_token {
                    Some(t) if page.is_truncated => token = Some(t),
                    _ => break 'pages,
                }
            }
            if truncated && err.is_none() {
                err = Some("单轮页上限截停（前缀下键过多），下轮续扫".into());
            }
        }

        let (st, msg) = match &err {
            None => ("done", None),
            Some(m) => ("error", Some(m.as_str())),
        };
        if let Err(e) = state
            .db
            .job_finish(job, st, scanned, deleted, skipped, msg, now_secs())
            .await
        {
            tracing::warn!(error = %e, "lifecycle：job 收尾失败");
        }
    }
    ran
}

/// 后台 worker（serve 路径挂载；显式 `OSS_LIFECYCLE_INTERVAL_SECS=0` 关闭）。
pub fn spawn_worker(state: AppState) {
    let secs: u64 = std::env::var("OSS_LIFECYCLE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    if secs == 0 {
        tracing::info!("lifecycle worker 已关闭（OSS_LIFECYCLE_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            sweep_once(&state).await;
        }
    });
    tracing::info!(interval_secs = secs, "lifecycle worker 已启动");
}

/// C5：审计保留清理 worker——`OSS_AUDIT_RETENTION_DAYS`（默认 0 = 不清理，保持现状）。
/// 每小时清理一次 `ts < now - days*86400` 的行；清理失败只 warn（保留策略非关键路径）。
pub fn spawn_audit_cleanup(state: AppState) {
    let days: i64 = std::env::var("OSS_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if days <= 0 {
        tracing::info!("审计保留清理未启用（OSS_AUDIT_RETENTION_DAYS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(3600));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let cutoff = now_secs() - days * 86_400;
            match state.db.delete_audit_older_than(cutoff).await {
                Ok(n) if n > 0 => tracing::info!(deleted = n, "审计保留清理"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "审计保留清理失败"),
            }
        }
    });
    tracing::info!(days, "审计保留清理 worker 已启动（每小时一次）");
}

#[cfg(test)]
mod tests {
    use super::lifecycle_event;
    use xl_store::store::DeleteOutcome;

    /// M6 切片 0（P2-1/P2-4 复查补）：删除结果 → 事件映射的确定性矩阵。
    #[test]
    fn lifecycle_event_mapping_matrix() {
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Marker {
                version_id: "v9".into()
            }),
            Some(("s3:LifecycleExpiration:DeleteMarkerCreated", Some("v9")))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Version {
                version_id: "v1".into(),
                tier: None,
            }),
            Some(("s3:LifecycleExpiration:Delete", None))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Object { tiers: vec![] }),
            Some(("s3:LifecycleExpiration:Delete", None))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::NotFound),
            None,
            "空删（并发窄窗）不得发事件"
        );
    }
}
