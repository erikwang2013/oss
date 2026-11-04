//! 桶 / 桶配置的 SQLite 写实现（调用方事务内；不 begin/commit）。
//!
//! SQL 形状逐字复刻旧 `Db::create_bucket_tx` 与各 `*_raw` helper：`buckets`
//! INSERT 固定 `set_id=0`，UPDATE 不写 `set_id/creation_date`；策略/事件/复制规则
//! 用 `INSERT ... ON CONFLICT DO UPDATE`（保留 site 触发器语义）。

use sqlx::SqliteConnection;
use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry};
use xl_store::control_store::{ControlError, OwnerRef};

use super::SqliteControlTxn;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// `Admin` → NULL；`User` 存在 → 本地 id；缺失且存在管理员 → Invalid；
/// 缺失且无管理员（witness）→ NULL（与 `apply::bucket_owner_id` 逐字等价）。
async fn resolve_owner(
    conn: &mut SqliteConnection,
    owner: &OwnerRef,
) -> Result<Option<i64>, ControlError> {
    let admin: Option<i64> =
        sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(backend)?;
    match owner {
        OwnerRef::Admin => Ok(None),
        OwnerRef::User(username) => {
            let id: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&mut *conn)
                .await
                .map_err(backend)?;
            match id {
                Some(id) => Ok(Some(id)),
                None if admin.is_some() => Err(ControlError::Invalid(format!(
                    "bucket owner user {username} not found"
                ))),
                None => Ok(None),
            }
        }
    }
}

pub(super) async fn bucket_create(
    txn: &mut SqliteControlTxn<'_>,
    row: &BucketEntry,
) -> Result<BucketCreateOutcome, ControlError> {
    let owner_id = resolve_owner(&mut *txn.conn, &row.owner).await?;
    let existing: Option<String> =
        sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name = ?")
            .bind(&row.name)
            .fetch_optional(&mut *txn.conn)
            .await
            .map_err(backend)?;
    if let Some(mode) = existing {
        if mode != row.replication_mode {
            return Ok(BucketCreateOutcome::ModeConflict);
        }
        sqlx::query(
            "UPDATE buckets SET owner_id = ?, versioning = ?, object_lock = ?, \
             object_lock_default_mode = ?, object_lock_default_days = ?, \
             default_encryption = ?, replication_mode = ?, deleting = 0 WHERE name = ?",
        )
        .bind(owner_id)
        .bind(&row.versioning)
        .bind(i64::from(row.object_lock))
        .bind(&row.object_lock_default_mode)
        .bind(row.object_lock_default_days)
        .bind(&row.default_encryption)
        .bind(&row.replication_mode)
        .bind(&row.name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
        return Ok(BucketCreateOutcome::Updated);
    }
    sqlx::query(
        "INSERT INTO buckets \
         (name, owner_id, creation_date, versioning, set_id, object_lock, \
          object_lock_default_mode, object_lock_default_days, default_encryption, \
          replication_mode, deleting) \
         VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0)",
    )
    .bind(&row.name)
    .bind(owner_id)
    .bind(row.creation_date)
    .bind(&row.versioning)
    .bind(i64::from(row.object_lock))
    .bind(&row.object_lock_default_mode)
    .bind(row.object_lock_default_days)
    .bind(&row.default_encryption)
    .bind(&row.replication_mode)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(BucketCreateOutcome::Created)
}

pub(super) async fn bucket_delete(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
) -> Result<bool, ControlError> {
    let res = sqlx::query("DELETE FROM buckets WHERE name = ?")
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_set_deleting(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
    deleting: bool,
) -> Result<bool, ControlError> {
    let res = sqlx::query("UPDATE buckets SET deleting = ? WHERE name = ?")
        .bind(i64::from(deleting))
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_set_versioning(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
    versioning: &str,
) -> Result<bool, ControlError> {
    let res = sqlx::query("UPDATE buckets SET versioning = ? WHERE name = ?")
        .bind(versioning)
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_set_object_lock(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
    object_lock: bool,
    default_mode: Option<&str>,
    default_days: Option<i64>,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "UPDATE buckets SET object_lock = ?, object_lock_default_mode = ?, \
         object_lock_default_days = ? WHERE name = ?",
    )
    .bind(i64::from(object_lock))
    .bind(default_mode)
    .bind(default_days)
    .bind(name)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_set_default_encryption(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
    algorithm: &str,
) -> Result<bool, ControlError> {
    let res = sqlx::query("UPDATE buckets SET default_encryption = ? WHERE name = ?")
        .bind(algorithm)
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_clear_default_encryption(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
) -> Result<bool, ControlError> {
    let res = sqlx::query("UPDATE buckets SET default_encryption = NULL WHERE name = ?")
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn bucket_policy_set(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    policy_json: &str,
    updated_at: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?) \
         ON CONFLICT(bucket) DO UPDATE SET policy_json = excluded.policy_json, \
         updated_at = excluded.updated_at",
    )
    .bind(bucket)
    .bind(policy_json)
    .bind(updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_policy_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_notification_set(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_json: &str,
    updated_at: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?) \
         ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json, \
         updated_at = excluded.updated_at",
    )
    .bind(bucket)
    .bind(config_json)
    .bind(updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_notification_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM event_configs WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_replication_rules_set(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    rules_json: &str,
    updated_at: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?) \
         ON CONFLICT(bucket) DO UPDATE SET rules_json = excluded.rules_json, \
         updated_at = excluded.updated_at",
    )
    .bind(bucket)
    .bind(rules_json)
    .bind(updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_replication_rules_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM replication_rules WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_exists(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
) -> Result<bool, ControlError> {
    let row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
        .bind(name)
        .fetch_optional(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(row.is_some())
}

pub(super) async fn delete_bucket_configs_for_bucket(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    for sql in [
        "DELETE FROM bucket_policies WHERE bucket = ?",
        "DELETE FROM event_configs WHERE bucket = ?",
        "DELETE FROM replication_rules WHERE bucket = ?",
    ] {
        sqlx::query(sql)
            .bind(bucket)
            .execute(&mut *txn.conn)
            .await
            .map_err(backend)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_store::SqliteControlTxn;
    use crate::db::Db;

    fn entry(mode: &str, created: i64) -> BucketEntry {
        BucketEntry {
            name: "b".into(),
            owner: OwnerRef::Admin,
            creation_date: created,
            versioning: "Off".into(),
            object_lock: false,
            default_encryption: None,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: mode.into(),
            deleting: false,
        }
    }

    #[tokio::test]
    async fn bucket_create_mode_conflict_is_zero_write() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
             replication_mode, deleting, set_id) VALUES ('b', NULL, 7, 'Enabled', 1, 'async', 0, 3)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let before: (i64, String, i64, i64) = sqlx::query_as(
            "SELECT creation_date, versioning, object_lock, set_id FROM buckets WHERE name = 'b'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        let mut tx = db.pool().begin().await.unwrap();
        let out = {
            let mut txn = SqliteControlTxn::new(&mut tx);
            bucket_create(&mut txn, &entry("sync", 999)).await.unwrap()
        };
        tx.rollback().await.unwrap();
        assert_eq!(out, BucketCreateOutcome::ModeConflict);
        let after: (i64, String, i64, i64) = sqlx::query_as(
            "SELECT creation_date, versioning, object_lock, set_id FROM buckets WHERE name = 'b'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(before, after, "冲突路径不得写任何列");
    }

    #[tokio::test]
    async fn bucket_create_missing_user_with_admin_is_invalid() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.create_user("root", "pw", true).await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let mut txn = SqliteControlTxn::new(&mut tx);
        let row = BucketEntry {
            owner: OwnerRef::User("ghost".into()),
            ..entry("async", 1)
        };
        let err = bucket_create(&mut txn, &row).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("bucket owner user ghost not found"),
            "{err}"
        );
        tx.rollback().await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn bucket_create_without_admin_maps_user_to_null() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let mut txn = SqliteControlTxn::new(&mut tx);
        let row = BucketEntry {
            owner: OwnerRef::User("ghost".into()),
            ..entry("async", 1)
        };
        assert_eq!(
            bucket_create(&mut txn, &row).await.unwrap(),
            BucketCreateOutcome::Created
        );
        tx.commit().await.unwrap();
        let owner: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = 'b'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, None, "witness 口径");
    }

    #[tokio::test]
    async fn point_writes_missing_bucket_return_false() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let mut txn = SqliteControlTxn::new(&mut tx);
        assert!(
            !bucket_set_deleting(&mut txn, "missing", true)
                .await
                .unwrap()
        );
        assert!(
            !bucket_set_versioning(&mut txn, "missing", "Enabled")
                .await
                .unwrap()
        );
        assert!(
            !bucket_set_object_lock(&mut txn, "missing", true, None, None)
                .await
                .unwrap()
        );
        assert!(
            !bucket_set_default_encryption(&mut txn, "missing", "AES256")
                .await
                .unwrap()
        );
        assert!(
            !bucket_clear_default_encryption(&mut txn, "missing")
                .await
                .unwrap()
        );
        assert!(!bucket_delete(&mut txn, "missing").await.unwrap());
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn policy_notification_replication_upsert_and_delete() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let mut txn = SqliteControlTxn::new(&mut tx);
        bucket_policy_set(&mut txn, "b", "{\"v\":1}", 5)
            .await
            .unwrap();
        bucket_policy_set(&mut txn, "b", "{\"v\":2}", 6)
            .await
            .unwrap();
        bucket_notification_set(&mut txn, "b", "[]", 7)
            .await
            .unwrap();
        bucket_replication_rules_set(&mut txn, "b", "[]", 8)
            .await
            .unwrap();
        bucket_policy_delete(&mut txn, "b").await.unwrap();
        bucket_notification_delete(&mut txn, "b").await.unwrap();
        bucket_replication_rules_delete(&mut txn, "b")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let p: Option<String> = sqlx::query_scalar("SELECT policy_json FROM bucket_policies")
            .fetch_optional(db.pool())
            .await
            .unwrap();
        assert_eq!(p, None);
        let e: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_configs")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(e, 0);
        let r: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM replication_rules")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(r, 0);
    }

    #[tokio::test]
    async fn delete_bucket_configs_is_scoped() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let mut txn = SqliteControlTxn::new(&mut tx);
        for b in ["b1", "b2"] {
            bucket_policy_set(&mut txn, b, "{}", 1).await.unwrap();
            bucket_notification_set(&mut txn, b, "[]", 1).await.unwrap();
            bucket_replication_rules_set(&mut txn, b, "[]", 1)
                .await
                .unwrap();
        }
        delete_bucket_configs_for_bucket(&mut txn, "b1")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        async fn count(db: &Db, t: &str) -> i64 {
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {t} WHERE bucket = 'b1'"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap()
        }
        assert_eq!(count(&db, "bucket_policies").await, 0);
        assert_eq!(count(&db, "event_configs").await, 0);
        assert_eq!(count(&db, "replication_rules").await, 0);
        let b2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM bucket_policies WHERE bucket = 'b2'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(b2, 1);
    }
}
