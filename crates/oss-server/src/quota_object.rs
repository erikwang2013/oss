//! 对象后端的账户硬配额适配器：限制读控制面视图/缝，用量读写节点本地缓存
//! （D4 `LocalKv` 命名空间 `account_usage`）；复制账本 `AccountUsageLedger`
//! 只由采样器周期校准，供 strict `ClusterQuota`/Raft 快照语义使用，不在数据面热路径。
//!
//! 热路径不提交控制根：`admit`/`adjust`/`release` 只做本节点文件读写（同一账户
//! 由进程内互斥串行化，文件写入是 temp+fsync+rename）。缓存 30s 惰性重列举；
//! 采样器启动即重建，并在账户消失/取消配额时删除陈旧行。崩溃只丢正在飞行的
//! 增量，采样器从对象列举收敛（见 [`cache`]）。

use std::collections::HashSet;
use std::sync::Arc;

use xl_s3::account_quota::{AccountQuota, Limits, QuotaBackend, QuotaDenied, USAGE_TTL_SECS};
use xl_store::control_store::ControlError;
use xl_store::store::{ListRequest, Store};

use crate::db::Db;

mod cache;

use cache::Usage;
pub(crate) use cache::UsageCache;

const MAX_KEYS: usize = 1000;

pub(crate) struct ObjectQuota {
    db: Arc<Db>,
    usage: UsageCache,
    /// 同一账户的读-改-写串行化；不同账户可并行。
    locks: std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ObjectQuota {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        // 对象后端在启动装配时必然挂载节点本地目录（D4）。
        let local = db
            .object_control()
            .expect("object quota requires the object control backend")
            .control
            .local_store()
            .expect("object control store is missing its local store")
            .clone();
        Self {
            db,
            usage: UsageCache::new(local),
            locks: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn account_lock(&self, account: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(locks.entry(account.to_owned()).or_default())
    }

    /// 用一次列举结果写缓存；下界不得覆盖已知完整/更大的值（同 SQLite）。
    fn write_sample(
        &self,
        account: &str,
        used: i64,
        count: i64,
        complete: bool,
    ) -> Result<(), ControlError> {
        let (used, count) = match self.usage.load(account)? {
            Some(p) if !complete && p.complete => (p.used_bytes, p.object_count),
            Some(p) if !complete => (p.used_bytes.max(used), p.object_count.max(count)),
            _ => (used, count),
        };
        self.usage.store(
            account,
            &Usage {
                used_bytes: used,
                object_count: count,
                complete,
                computed_at: crate::db::now(),
            },
        )
    }

    /// 重新列举该账户全部桶并写本地缓存（调用方持有账户锁）。
    async fn refresh_account(&self, store: &Store, account: &str) -> Result<(), ControlError> {
        let buckets = self
            .db
            .control_store()
            .account_bucket_names(account)
            .await?;
        let (used, count, complete) = scan_usage(store, &buckets).await;
        self.write_sample(
            account,
            i64::try_from(used).unwrap_or(i64::MAX),
            i64::try_from(count).unwrap_or(i64::MAX),
            complete,
        )
    }

    /// 缓存缺失/超过 [`USAGE_TTL_SECS`] 时先重列举（调用方持有账户锁）。
    async fn ensure_fresh(&self, store: &Store, account: &str) -> Result<(), ControlError> {
        let fresh = self
            .usage
            .load(account)?
            .is_some_and(|u| u.is_fresh(crate::db::now(), USAGE_TTL_SECS));
        if !fresh {
            self.refresh_account(store, account).await?;
        }
        Ok(())
    }

    /// 采样器校准复制账本（严格模式的基线/快照语义）；不在请求路径上。
    async fn checkpoint_ledger(
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

/// strict 模式播种基线：本节点本地缓存（与 SQLite 缓存同语义；缺行/读失败 = 0）。
pub(crate) async fn usage_baseline(db: &Db, account: &str) -> (i64, i64) {
    let Some(object) = db.object_control() else {
        return (0, 0);
    };
    let Ok(local) = object.control.local_store() else {
        tracing::warn!(account, "对象控制面缺少本地缓存目录（基线按 0 播种）");
        return (0, 0);
    };
    match UsageCache::new(local.clone()).load(account) {
        Ok(Some(u)) => (u.used_bytes.max(0), u.object_count.max(0)),
        Ok(None) => (0, 0),
        Err(e) => {
            tracing::warn!(error = %e, account, "读取账户用量基线失败（按 0 播种）");
            (0, 0)
        }
    }
}

/// 快照安装/离线迁移后清空本地缓存，由采样器按新根重建（design §6.1）。
pub(crate) fn clear_account_usage(
    control: &crate::control_store_obj::ObjectControlStore,
) -> Result<(), ControlError> {
    UsageCache::new(control.local_store()?.clone()).clear()
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
        store: &Store,
        quota: &AccountQuota,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<Result<(), QuotaDenied>, ControlError> {
        if !quota.limits.any() {
            return Ok(Ok(()));
        }
        let lock = self.account_lock(&quota.account);
        let _guard = lock.lock().await;
        self.ensure_fresh(store, &quota.account).await?;
        let cur = self
            .usage
            .load(&quota.account)?
            .unwrap_or_else(Usage::empty);
        // 与 SQLite 条件 UPDATE 同判：已知增量越过硬限制才拒绝。
        if quota
            .limits
            .hard_bytes
            .is_some_and(|limit| cur.used_bytes.saturating_add(delta_bytes) > limit)
        {
            return Ok(Err(QuotaDenied::Bytes));
        }
        if quota
            .limits
            .objects
            .is_some_and(|limit| cur.object_count.saturating_add(delta_objects) > limit)
        {
            return Ok(Err(QuotaDenied::Objects));
        }
        self.usage.store(
            &quota.account,
            &Usage {
                used_bytes: cur.used_bytes.saturating_add(delta_bytes).max(0),
                object_count: cur.object_count.saturating_add(delta_objects).max(0),
                complete: cur.complete,
                computed_at: cur.computed_at,
            },
        )?;
        Ok(Ok(()))
    }

    async fn adjust(
        &self,
        account: &str,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<(), ControlError> {
        let lock = self.account_lock(account);
        let _guard = lock.lock().await;
        // 与 SQLite 无条件 UPDATE 同口径：缺行 = no-op（不凭空建缓存）。
        let Some(cur) = self.usage.load(account)? else {
            return Ok(());
        };
        self.usage.store(
            account,
            &Usage {
                used_bytes: cur.used_bytes.saturating_add(delta_bytes).max(0),
                object_count: cur.object_count.saturating_add(delta_objects).max(0),
                complete: cur.complete,
                computed_at: cur.computed_at,
            },
        )
    }

    async fn refresh_all(&self, store: &Store) -> Result<(), ControlError> {
        let control = self.db.control_store();
        let mut keep = HashSet::new();
        for account in control.accounts(None).await? {
            let has_limits = account.hard_byte_quota.is_some_and(|q| q >= 0)
                || account.object_quota.is_some_and(|q| q >= 0);
            if account.status != "active" || !has_limits {
                continue;
            }
            keep.insert(account.id.clone());
            let lock = self.account_lock(&account.id);
            let _guard = lock.lock().await;
            let buckets = match control.account_bucket_names(&account.id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, account = %account.id, "账户用量采样失败（保留旧缓存）");
                    continue;
                }
            };
            let (used, count, complete) = scan_usage(store, &buckets).await;
            let used = i64::try_from(used).unwrap_or(i64::MAX);
            let count = i64::try_from(count).unwrap_or(i64::MAX);
            if let Err(e) = self.write_sample(&account.id, used, count, complete) {
                tracing::warn!(error = %e, account = %account.id, "账户用量采样失败（保留旧缓存）");
                continue;
            }
            if let Err(e) = self
                .checkpoint_ledger(&account.id, used, count, complete)
                .await
            {
                tracing::warn!(error = %e, account = %account.id, "账户账本校准失败（保留旧账本）");
            }
        }
        // 账户删除/取消配额后不留下陈旧缓存（设计 §7：account_usage 可重算）。
        for account in self.usage.accounts()? {
            if keep.contains(&account) {
                continue;
            }
            let lock = self.account_lock(&account);
            let _guard = lock.lock().await;
            if let Err(e) = self.usage.remove(&account) {
                tracing::warn!(error = %e, account, "清理账户用量缓存失败");
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
