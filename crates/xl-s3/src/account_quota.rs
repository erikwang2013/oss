//! AC-3/AC-4：账户硬字节/对象数配额（写入预留、删除释放、跨节点有界陈旧）。
//!
//! 用量口径与软配额端点一致：账户全部桶的**当前版本**对象（列举天然排除 delete
//! marker/非当前版本/未完成 MPU）。每节点维护本地 `account_usage` 缓存：
//! - 写入在 SQLite 单条条件 UPDATE 中原子预留，不写 Raft 日志；
//! - 删除立即释放（版本定向删除等交给采样器）；
//! - 跨节点由 `Store::list_objects` 的 mesh 合并列举周期/惰性刷新（无新 RPC）。
//!
//! 陈旧边界：缓存对本地写入精确；其它节点写入最多滞后一个采样间隔/惰性 TTL
//! （[`USAGE_TTL_SECS`]）。对端不可达时列举是下界（`complete=0`）：已知下界超出
//! 配额才拒绝（对未知增量 fail-open），避免脑裂下误拒；DB 错误 fail-closed（500）。
//! 最小夹具/未迁移库缺 accounts 列按「无配额」处理，不改变既有测试路径。
//!
//! shortcut: 定向版本删除与无钥 SSE-C 删除不即时释放，靠采样器收敛；升级触发：
//! 需要逐版本用量索引（或删除路径携带 SSE-C 钥）时。

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{Row, SqlitePool};
use xl_store::store::{ListRequest, Store};

/// 惰性刷新阈值：热路径最多容忍这么旧的跨节点用量。
pub const USAGE_TTL_SECS: i64 = 30;
const MAX_KEYS: usize = 1000;

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    /// 非负；`None` = 不限（NULL/负数同义）。
    pub hard_bytes: Option<i64>,
    pub objects: Option<i64>,
}

impl Limits {
    pub fn any(&self) -> bool {
        self.hard_bytes.is_some() || self.objects.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct AccountQuota {
    pub account: String,
    /// 账户已停用：写入 fail-closed（与桶数配额同判）。
    pub disabled: bool,
    pub limits: Limits,
}

#[derive(Debug, Clone, Copy)]
pub enum QuotaDenied {
    Bytes,
    Objects,
}

impl std::fmt::Display for QuotaDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes => f.write_str("account byte quota exceeded"),
            Self::Objects => f.write_str("account object quota exceeded"),
        }
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 最小夹具/未迁移库：缺账户表或硬配额列视作无配额（生产 connect 必补列）。
fn missing_account_schema(e: &sqlx::Error) -> bool {
    e.as_database_error().is_some_and(|d| {
        let m = d.message();
        m.contains("no such table: accounts")
            || m.contains("no such table: account_members")
            || m.contains("no such column: hard_byte_quota")
            || m.contains("no such column: object_quota")
    })
}

fn missing_usage_table(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(|d| d.message().contains("no such table: account_usage"))
}

/// 桶主的账户与硬配额；桶主是全局管理员/无账户行/未配置配额 → `None`。
/// 账户停用时仍返回 `Some`（带 limits），由写入侧 fail-closed。
pub async fn account_for_bucket(
    pool: &SqlitePool,
    bucket: &str,
) -> Result<Option<AccountQuota>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT COALESCE(am.account_id, printf('%012d', u.id)) AS account_id, \
                a.status AS status, a.hard_byte_quota AS hard_bytes, \
                a.object_quota AS objects \
         FROM buckets b JOIN users u ON u.id = b.owner_id \
         LEFT JOIN account_members am ON am.uid = u.id \
         LEFT JOIN accounts a ON a.id = COALESCE(am.account_id, printf('%012d', u.id)) \
         WHERE b.name = ? AND u.is_admin = 0",
    )
    .bind(bucket)
    .fetch_optional(pool)
    .await;
    let row = match row {
        Ok(r) => r,
        Err(e) if missing_account_schema(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(row) = row else {
        return Ok(None);
    };
    let limits = Limits {
        hard_bytes: row.get::<Option<i64>, _>("hard_bytes").filter(|q| *q >= 0),
        objects: row.get::<Option<i64>, _>("objects").filter(|q| *q >= 0),
    };
    if !limits.any() {
        return Ok(None);
    }
    Ok(Some(AccountQuota {
        account: row.get("account_id"),
        disabled: row.get::<Option<String>, _>("status").as_deref() != Some("active"),
        limits,
    }))
}

type UsageRow = (i64, i64, bool, i64);

async fn load_usage(pool: &SqlitePool, account: &str) -> Result<Option<UsageRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT used_bytes, object_count, complete, computed_at \
         FROM account_usage WHERE account_id = ?",
    )
    .bind(account)
    .fetch_optional(pool)
    .await;
    match row {
        Ok(None) => Ok(None),
        Ok(Some(r)) => Ok(Some((
            r.get("used_bytes"),
            r.get("object_count"),
            r.get::<i64, _>("complete") != 0,
            r.get("computed_at"),
        ))),
        Err(e) if missing_usage_table(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// 账户名下桶名（成员行优先，无行 = uid 派生账户；与软用量端点同口径）。
async fn account_buckets(pool: &SqlitePool, account: &str) -> Result<Vec<String>, sqlx::Error> {
    let sql = "SELECT b.name FROM buckets b JOIN users u ON u.id = b.owner_id \
               LEFT JOIN account_members bm ON bm.uid = u.id \
               WHERE u.is_admin = 0 \
                 AND COALESCE(bm.account_id, printf('%012d', u.id)) = ? \
               ORDER BY b.name";
    let rows = sqlx::query(sql).bind(account).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| r.get("name")).collect())
}

/// 重新列举账户全部桶并写缓存。对端不可达的页是下界：`complete=0` 时保留上次
/// 完整快照（或取两轮下界的较大者），绝不把下界冒充精确值。
pub async fn refresh_account(
    pool: &SqlitePool,
    store: &Store,
    account: &str,
) -> Result<(), sqlx::Error> {
    let buckets = account_buckets(pool, account).await?;
    let mut used: u64 = 0;
    let mut count: u64 = 0;
    let mut complete = true;
    for bucket in &buckets {
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
                    tracing::warn!(error = %e, bucket, account, "账户用量列举失败（按不完整处理）");
                    complete = false;
                    break;
                }
            }
        }
    }
    let used = i64::try_from(used).unwrap_or(i64::MAX);
    let count = i64::try_from(count).unwrap_or(i64::MAX);
    let previous = load_usage(pool, account).await?;
    let (used, count) = match previous {
        Some((p_used, p_count, true, _)) if !complete => (p_used, p_count),
        Some((p_used, p_count, _, _)) if !complete => (p_used.max(used), p_count.max(count)),
        _ => (used, count),
    };
    sqlx::query(
        "INSERT INTO account_usage (account_id, used_bytes, object_count, complete, computed_at) \
         VALUES (?, ?, ?, ?, ?) \
         ON CONFLICT(account_id) DO UPDATE SET used_bytes = excluded.used_bytes, \
           object_count = excluded.object_count, complete = excluded.complete, \
           computed_at = excluded.computed_at",
    )
    .bind(account)
    .bind(used)
    .bind(count)
    .bind(complete as i64)
    .bind(now())
    .execute(pool)
    .await?;
    Ok(())
}

/// 周期采样入口（serve worker）：只刷新配置了硬配额的活跃账户。
pub async fn refresh_all(pool: &SqlitePool, store: &Store) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id FROM accounts WHERE status = 'active' \
         AND ((hard_byte_quota IS NOT NULL AND hard_byte_quota >= 0) \
           OR (object_quota IS NOT NULL AND object_quota >= 0))",
    )
    .fetch_all(pool)
    .await;
    let rows = match rows {
        Ok(r) => r,
        Err(e) if missing_account_schema(&e) => return Ok(()),
        Err(e) => return Err(e),
    };
    for row in rows {
        let account: String = row.get("id");
        if let Err(e) = refresh_account(pool, store, &account).await {
            tracing::warn!(error = %e, account, "账户用量采样失败（保留旧缓存）");
        }
    }
    Ok(())
}

/// 惰性刷新：缓存缺失或超过 [`USAGE_TTL_SECS`] 时先采样（跨节点有界陈旧）。
async fn ensure_fresh(
    pool: &SqlitePool,
    store: &Store,
    quota: &AccountQuota,
) -> Result<(), sqlx::Error> {
    let fresh = match load_usage(pool, &quota.account).await? {
        Some((_, _, _, computed_at)) => now().saturating_sub(computed_at) <= USAGE_TTL_SECS,
        None => false,
    };
    if !fresh {
        refresh_account(pool, store, &quota.account).await?;
    }
    Ok(())
}

/// 写入准入：先保证缓存有界新鲜，再原子预留 `(delta_bytes, delta_objects)`。
/// 任一下降（覆写变小/删旧版本）也走同一 UPDATE；失败时调用方必须归还同样增量。
pub async fn admit(
    pool: &SqlitePool,
    store: &Store,
    quota: &AccountQuota,
    delta_bytes: i64,
    delta_objects: i64,
) -> Result<Result<(), QuotaDenied>, sqlx::Error> {
    ensure_fresh(pool, store, quota).await?;
    reserve(pool, quota, delta_bytes, delta_objects).await
}

/// 原子条件预留（调用方已刷新缓存）。单条 UPDATE：并发写由 SQLite 串行化，
/// 无需 Raft 写；行不存在时先插 0（computed_at=0 保证下一轮重新采样）。
pub async fn reserve(
    pool: &SqlitePool,
    quota: &AccountQuota,
    delta_bytes: i64,
    delta_objects: i64,
) -> Result<Result<(), QuotaDenied>, sqlx::Error> {
    if !quota.limits.any() {
        return Ok(Ok(()));
    }
    sqlx::query(
        "INSERT OR IGNORE INTO account_usage \
         (account_id, used_bytes, object_count, complete, computed_at) \
         VALUES (?, 0, 0, 0, 0)",
    )
    .bind(&quota.account)
    .execute(pool)
    .await?;
    let n = sqlx::query(
        "UPDATE account_usage SET used_bytes = MAX(0, used_bytes + ?), \
           object_count = MAX(0, object_count + ?) \
         WHERE account_id = ? \
           AND used_bytes + ? <= COALESCE(?, 9223372036854775807) \
           AND object_count + ? <= COALESCE(?, 9223372036854775807)",
    )
    .bind(delta_bytes)
    .bind(delta_objects)
    .bind(&quota.account)
    .bind(delta_bytes)
    .bind(quota.limits.hard_bytes)
    .bind(delta_objects)
    .bind(quota.limits.objects)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 1 {
        return Ok(Ok(()));
    }
    let Some((used, count, _, _)) = load_usage(pool, &quota.account).await? else {
        return Ok(Err(QuotaDenied::Bytes));
    };
    if quota
        .limits
        .hard_bytes
        .is_some_and(|limit| used.saturating_add(delta_bytes) > limit)
    {
        Ok(Err(QuotaDenied::Bytes))
    } else if quota
        .limits
        .objects
        .is_some_and(|limit| count.saturating_add(delta_objects) > limit)
    {
        Ok(Err(QuotaDenied::Objects))
    } else {
        // 条件更新失败但按当前行重算未超：并发释放/分类竞争，保守拒绝。
        Ok(Err(QuotaDenied::Objects))
    }
}

/// 无条件增量修正（写成功后的实际大小校准；或写失败时归还预留）。
pub async fn adjust(
    pool: &SqlitePool,
    account: &str,
    delta_bytes: i64,
    delta_objects: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE account_usage SET used_bytes = MAX(0, used_bytes + ?), \
           object_count = MAX(0, object_count + ?) WHERE account_id = ?",
    )
    .bind(delta_bytes)
    .bind(delta_objects)
    .bind(account)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, is_admin INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL);\
             CREATE TABLE accounts (id TEXT PRIMARY KEY, name TEXT, status TEXT NOT NULL DEFAULT 'active', \
               bucket_quota INTEGER, soft_byte_quota INTEGER, hard_byte_quota INTEGER, object_quota INTEGER, \
               created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);\
             CREATE TABLE account_members (account_id TEXT NOT NULL, uid INTEGER UNIQUE, \
               role TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY(account_id, uid));\
             CREATE TABLE account_usage (account_id TEXT PRIMARY KEY, used_bytes INTEGER NOT NULL DEFAULT 0, \
               object_count INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0, \
               computed_at INTEGER NOT NULL DEFAULT 0);\
             INSERT INTO users VALUES (7, 'u7', 0);\
             INSERT INTO buckets VALUES ('bkt', 7, 0);\
             INSERT INTO accounts VALUES ('000000000007', 'u7', 'active', NULL, NULL, 10, 2, 0, 0);\
             INSERT INTO account_members VALUES ('000000000007', 7, 'owner', 0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    /// 字节/对象数都在「恰好等于上限」放行、超过 1 即拒绝；负数修正可清空。
    #[tokio::test]
    async fn reserve_enforces_exact_limits() {
        let pool = pool().await;
        let q = account_for_bucket(&pool, "bkt").await.unwrap().unwrap();
        assert_eq!(q.account, "000000000007");
        assert!(matches!(reserve(&pool, &q, 9, 1).await.unwrap(), Ok(())));
        assert!(matches!(reserve(&pool, &q, 1, 0).await.unwrap(), Ok(())));
        assert!(matches!(
            reserve(&pool, &q, 1, 0).await.unwrap(),
            Err(QuotaDenied::Bytes)
        ));
        assert!(matches!(reserve(&pool, &q, 0, 1).await.unwrap(), Ok(())));
        assert!(matches!(
            reserve(&pool, &q, 0, 1).await.unwrap(),
            Err(QuotaDenied::Objects)
        ));
        adjust(&pool, &q.account, -10, -2).await.unwrap();
        assert!(matches!(reserve(&pool, &q, 0, 2).await.unwrap(), Ok(())));
    }

    /// 对端不可达的缓存是下界（complete=0）：下界已满即拒绝，未满的小增量仍放行
    /// （未知增量 fail-open，与模块文档一致）。
    #[tokio::test]
    async fn incomplete_cache_enforces_known_lower_bound() {
        let pool = pool().await;
        sqlx::query(
            "INSERT INTO account_usage (account_id, used_bytes, object_count, complete, computed_at) \
             VALUES ('000000000007', 6, 0, 0, ?)",
        )
        .bind(now())
        .execute(&pool)
        .await
        .unwrap();
        let q = account_for_bucket(&pool, "bkt").await.unwrap().unwrap();
        assert!(matches!(reserve(&pool, &q, 4, 0).await.unwrap(), Ok(())));
        assert!(matches!(
            reserve(&pool, &q, 1, 0).await.unwrap(),
            Err(QuotaDenied::Bytes)
        ));
    }

    /// 采样口径 = 当前版本对象：两个键 3+5 字节、计数 2、complete=1。
    #[tokio::test]
    async fn refresh_account_counts_current_versions() {
        let pool = pool().await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(xl_store::store::StoreConfig::new(dir.path()))
            .await
            .unwrap();
        store.create_bucket("bkt").await.unwrap();
        store
            .put_object("bkt", "a", bytes::Bytes::from_static(b"abc"), None)
            .await
            .unwrap();
        store
            .put_object("bkt", "b", bytes::Bytes::from_static(b"12345"), None)
            .await
            .unwrap();
        refresh_account(&pool, &store, "000000000007")
            .await
            .unwrap();
        let (used, count, complete, _) = load_usage(&pool, "000000000007").await.unwrap().unwrap();
        assert_eq!((used, count, complete), (8, 2, true));

        // 对端不可达（列举失败）不得用下界覆盖上次完整快照的值。
        sqlx::query("INSERT INTO buckets VALUES ('ghost', 7, 0)")
            .execute(&pool)
            .await
            .unwrap();
        refresh_account(&pool, &store, "000000000007")
            .await
            .unwrap();
        let (used, count, complete, _) = load_usage(&pool, "000000000007").await.unwrap().unwrap();
        assert_eq!((used, count, complete), (8, 2, false));
    }
}
