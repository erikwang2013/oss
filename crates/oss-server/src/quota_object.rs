//! 对象后端的账户硬配额适配器：限制读控制面视图/缝，用量读复制账本
//! `AccountUsageLedger`（根 CAS，每次写入原子预留），采样从对象存储重算账本。
//!
//! 与 SQLite 本地缓存不同，账本是复制数据：并发写由根 CAS 事务串行化，
//! CAS 冲突重跑闭包（每次重读账本），因此不需要 30s 惰性刷新。采样仍是
//! 对账兜底：完整列举直接校准；任一对端不可达（下界）时取账本与下界的较大者。

use std::sync::Arc;

use xl_s3::account_quota::{AccountQuota, Limits, QuotaBackend, QuotaDenied};
use xl_store::control_store::ControlError;
use xl_store::store::{ListRequest, Store};

use crate::db::Db;

const MAX_KEYS: usize = 1000;

pub(crate) struct ObjectQuota {
    db: Arc<Db>,
}

impl ObjectQuota {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// 采样写账本：完整列举直接覆盖；下界不得覆盖已知值。
    async fn write_sample(
        &self,
        account: &str,
        used: i64,
        count: i64,
        complete: bool,
    ) -> Result<(), ControlError> {
        let account = account.to_owned();
        self.db
            .control_write("account-quota-sample", move |txn| {
                let account = account.clone();
                Box::pin(async move {
                    let (used, count) = match txn.account_ledger(&account).await? {
                        Some((p_used, p_count)) if !complete => {
                            (p_used.max(used), p_count.max(count))
                        }
                        _ => (used, count),
                    };
                    txn.account_ledger_upsert(&account, used, count).await
                })
            })
            .await
            .map_err(|e| ControlError::Backend(e.to_string()))
    }
}

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// 控制面缝上的限制读取（无请求视图的后台/离线路径）。
async fn account_for_bucket(db: &Db, bucket: &str) -> Result<Option<AccountQuota>, ControlError> {
    let store = db.control_store();
    let Some(bucket) = store.bucket(bucket).await? else {
        return Ok(None);
    };
    let Some(uid) = bucket.owner_uid else {
        return Ok(None);
    };
    let Some(user) = store.user_by_id(uid).await? else {
        return Ok(None);
    };
    if user.is_admin {
        return Ok(None);
    }
    let account = store
        .account_member_account(uid)
        .await?
        .or_else(|| xl_s3::access::account_id_of(uid));
    let Some(account) = account else {
        return Ok(None);
    };
    let Some((hard, objects)) = store.account_hard_limits(&account).await? else {
        return Ok(None);
    };
    let limits = Limits {
        hard_bytes: hard.filter(|q| *q >= 0),
        objects: objects.filter(|q| *q >= 0),
    };
    if !limits.any() {
        return Ok(None);
    }
    let status = store.account_status(&account).await?;
    Ok(Some(AccountQuota {
        account,
        disabled: status.as_deref() != Some("active"),
        limits,
    }))
}

/// 一个账户全部桶的当前版本对象 `(used_bytes, object_count, complete)`。
async fn scan_usage(store: &Store, buckets: &[String]) -> (u64, u64, bool) {
    let mut used: u64 = 0;
    let mut count: u64 = 0;
    let mut complete = true;
    for bucket in buckets {
        let mut token: Option<String> = None;
        loop {
            let req = ListRequest {
                max_keys: Some(MAX_KEYS),
                continuation_token: token.clone(),
                ..Default::default()
            };
            match store.list_objects(bucket, &req).await {
                Ok(page) => {
                    used = used.saturating_add(page.objects.iter().map(|o| o.size).sum::<u64>());
                    count = count.saturating_add(page.objects.len() as u64);
                    complete &= !page.incomplete;
                    match (page.is_truncated, page.next_token) {
                        (false, _) => break,
                        (true, Some(t)) => token = Some(t),
                        (true, None) => {
                            tracing::warn!(bucket, "账户用量列举截断但无续页 token");
                            complete = false;
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, bucket, "账户用量列举失败（按不完整处理）");
                    complete = false;
                    break;
                }
            }
        }
    }
    (used, count, complete)
}

/// strict 模式播种基线：请求视图账本优先（同请求代数）；否则读根账本。
pub(crate) async fn usage_baseline(db: &Db, account: &str) -> (i64, i64) {
    if let Some(v) = xl_s3::control::request_view() {
        return match v.account_ledger(account).await {
            Ok(Some((used, count))) => (used.max(0), count.max(0)),
            Ok(None) => (0, 0),
            Err(e) => {
                tracing::warn!(error = %e, account, "读取账户用量基线失败（按 0 播种）");
                (0, 0)
            }
        };
    }
    let account_id = account.to_owned();
    let read = db
        .control_write("account-quota-baseline", move |txn| {
            let account = account_id.clone();
            Box::pin(async move { txn.account_ledger(&account).await })
        })
        .await;
    match read {
        Ok(Some((used, count))) => (used.max(0), count.max(0)),
        Ok(None) => (0, 0),
        Err(e) => {
            tracing::warn!(error = %e, account, "读取账户用量基线失败（按 0 播种）");
            (0, 0)
        }
    }
}

#[async_trait::async_trait]
impl QuotaBackend for ObjectQuota {
    async fn account_quota_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Option<AccountQuota>, ControlError> {
        if let Some(v) = xl_s3::control::request_view() {
            return v.account_quota_for_bucket(bucket).await;
        }
        account_for_bucket(&self.db, bucket).await
    }

    async fn admit(
        &self,
        _store: &Store,
        quota: &AccountQuota,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<Result<(), QuotaDenied>, ControlError> {
        if !quota.limits.any() {
            return Ok(Ok(()));
        }
        let account = quota.account.clone();
        let limits = quota.limits;
        let out = self
            .db
            .control_write("account-quota", move |txn| {
                let account = account.clone();
                Box::pin(async move {
                    let (used, count) = txn.account_ledger(&account).await?.unwrap_or((0, 0));
                    // 与 SQLite 条件 UPDATE 同判：已知增量越过硬限制才拒绝。
                    let denied = if limits
                        .hard_bytes
                        .is_some_and(|limit| used.saturating_add(delta_bytes) > limit)
                    {
                        Some(QuotaDenied::Bytes)
                    } else if limits
                        .objects
                        .is_some_and(|limit| count.saturating_add(delta_objects) > limit)
                    {
                        Some(QuotaDenied::Objects)
                    } else {
                        None
                    };
                    if let Some(denied) = denied {
                        return Ok(Err(denied));
                    }
                    txn.account_ledger_upsert(
                        &account,
                        used.saturating_add(delta_bytes).max(0),
                        count.saturating_add(delta_objects).max(0),
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await
            .map_err(backend)?;
        Ok(out)
    }

    async fn adjust(
        &self,
        account: &str,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<(), ControlError> {
        let account = account.to_owned();
        self.db
            .control_write("account-quota", move |txn| {
                let account = account.clone();
                Box::pin(async move {
                    // 与 SQLite 无条件 UPDATE 同口径：缺行 = no-op（不凭空建账本）。
                    let Some((used, count)) = txn.account_ledger(&account).await? else {
                        return Ok(());
                    };
                    txn.account_ledger_upsert(
                        &account,
                        used.saturating_add(delta_bytes).max(0),
                        count.saturating_add(delta_objects).max(0),
                    )
                    .await
                })
            })
            .await
            .map_err(backend)
    }

    async fn refresh_all(&self, store: &Store) -> Result<(), ControlError> {
        let control = self.db.control_store();
        for account in control.accounts(None).await? {
            let has_limits = account.hard_byte_quota.is_some_and(|q| q >= 0)
                || account.object_quota.is_some_and(|q| q >= 0);
            if account.status != "active" || !has_limits {
                continue;
            }
            let buckets = match control.account_bucket_names(&account.id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, account = %account.id, "账户用量采样失败（保留旧账本）");
                    continue;
                }
            };
            let (used, count, complete) = scan_usage(store, &buckets).await;
            let used = i64::try_from(used).unwrap_or(i64::MAX);
            let count = i64::try_from(count).unwrap_or(i64::MAX);
            if let Err(e) = self.write_sample(&account.id, used, count, complete).await {
                tracing::warn!(error = %e, account = %account.id, "账户用量采样失败（保留旧账本）");
            }
        }
        Ok(())
    }

    async fn usage_baseline(&self, account: &str) -> (i64, i64) {
        usage_baseline(&self.db, account).await
    }
}

#[cfg(test)]
mod tests;
