//! 桶 / 桶配置的快照导出与安装（调用方事务内；不 begin/commit）。
//!
//! 严格导出口径：`owner_id` NULL/0 或 `is_admin=1` → `Admin`；现存用户 → `User`；
//! 悬空非 0 id → `MissingOwner(id)`（文本与旧 `DataError::MissingUser` 一致）。
//! 安装保留目标 `set_id`（UPDATE 列不含它）。

use sqlx::Row;
use xl_store::control_store::bucket::{
    BucketEntry, BucketPolicyEntry, EventConfigEntry, ReplicationRuleEntry,
};
use xl_store::control_store::iam::IamTxn;
use xl_store::control_store::{ControlError, OwnerRef};

use super::SqliteControlTxn;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

const BUCKET_EXPORT_SELECT: &str = "SELECT b.name, b.owner_id, b.creation_date, b.versioning, b.object_lock, \
    b.default_encryption, b.object_lock_default_mode, b.object_lock_default_days, \
    b.replication_mode, b.deleting, u.username AS owner_username, \
    u.is_admin AS owner_is_admin \
    FROM buckets b LEFT JOIN users u ON u.id = b.owner_id";

fn bucket_entry(r: &sqlx::sqlite::SqliteRow) -> Result<BucketEntry, ControlError> {
    let owner_id: Option<i64> = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let is_admin: Option<i64> = r.get("owner_is_admin");
    let owner = match owner_id {
        None | Some(0) => OwnerRef::Admin,
        Some(id) => match (username, is_admin) {
            (_, Some(1)) => OwnerRef::Admin,
            (Some(name), _) => OwnerRef::User(name),
            (None, _) => return Err(ControlError::MissingOwner(id)),
        },
    };
    Ok(BucketEntry {
        name: r.get("name"),
        owner,
        creation_date: r.get("creation_date"),
        versioning: r.get("versioning"),
        object_lock: r.get::<i64, _>("object_lock") != 0,
        default_encryption: r.get("default_encryption"),
        object_lock_default_mode: r.get("object_lock_default_mode"),
        object_lock_default_days: r.get("object_lock_default_days"),
        replication_mode: r.get("replication_mode"),
        deleting: r.get::<i64, _>("deleting") != 0,
    })
}

pub(super) async fn buckets(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<BucketEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{BUCKET_EXPORT_SELECT} ORDER BY b.name"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    rows.iter().map(bucket_entry).collect()
}

pub(super) async fn bucket_policies(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<BucketPolicyEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, policy_json, updated_at FROM bucket_policies ORDER BY bucket")
            .fetch_all(&mut *txn.conn)
            .await
            .map_err(backend)?;
    Ok(rows
        .iter()
        .map(|r| BucketPolicyEntry {
            bucket: r.get("bucket"),
            policy_json: r.get("policy_json"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub(super) async fn replace_bucket_policies(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[BucketPolicyEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM bucket_policies")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.policy_json)
        .bind(row.updated_at)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn event_configs(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<EventConfigEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, config_json, updated_at FROM event_configs ORDER BY bucket")
            .fetch_all(&mut *txn.conn)
            .await
            .map_err(backend)?;
    Ok(rows
        .iter()
        .map(|r| EventConfigEntry {
            bucket: r.get("bucket"),
            config_json: r.get("config_json"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub(super) async fn replace_event_configs(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[EventConfigEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM event_configs")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query("INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?)")
            .bind(&row.bucket)
            .bind(&row.config_json)
            .bind(row.updated_at)
            .execute(&mut *txn.conn)
            .await
            .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn replication_rules(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<ReplicationRuleEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, rules_json, updated_at FROM replication_rules ORDER BY bucket")
            .fetch_all(&mut *txn.conn)
            .await
            .map_err(backend)?;
    Ok(rows
        .iter()
        .map(|r| ReplicationRuleEntry {
            bucket: r.get("bucket"),
            rules_json: r.get("rules_json"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub(super) async fn replace_replication_rules(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[ReplicationRuleEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM replication_rules")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.rules_json)
        .bind(row.updated_at)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn bucket_install_prepare(
    txn: &mut SqliteControlTxn<'_>,
    wanted: &[String],
) -> Result<(), ControlError> {
    let existing: Vec<String> = sqlx::query_scalar("SELECT name FROM buckets")
        .fetch_all(&mut *txn.conn)
        .await
        .map_err(backend)?;
    let wanted: std::collections::HashSet<&str> = wanted.iter().map(|n| n.as_str()).collect();
    for name in existing.iter().filter(|n| !wanted.contains(n.as_str())) {
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&mut *txn.conn)
            .await
            .map_err(backend)?;
    }
    sqlx::query("UPDATE buckets SET owner_id = NULL")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn bucket_upsert_install(
    txn: &mut SqliteControlTxn<'_>,
    row: &BucketEntry,
) -> Result<(), ControlError> {
    let owner_id = match &row.owner {
        OwnerRef::Admin => None,
        OwnerRef::User(name) => {
            let id = txn.user_uid_non_admin(name).await?;
            Some(id.ok_or_else(|| {
                ControlError::Invalid(format!("bucket owner {name} not in snapshot"))
            })?)
        }
    };
    sqlx::query(
        "INSERT INTO buckets \
         (name, owner_id, creation_date, versioning, object_lock, default_encryption, \
          object_lock_default_mode, object_lock_default_days, replication_mode, deleting) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(name) DO UPDATE SET \
          owner_id = excluded.owner_id, creation_date = excluded.creation_date, \
          versioning = excluded.versioning, object_lock = excluded.object_lock, \
          default_encryption = excluded.default_encryption, \
          object_lock_default_mode = excluded.object_lock_default_mode, \
          object_lock_default_days = excluded.object_lock_default_days, \
          replication_mode = excluded.replication_mode, deleting = excluded.deleting",
    )
    .bind(&row.name)
    .bind(owner_id)
    .bind(row.creation_date)
    .bind(&row.versioning)
    .bind(i64::from(row.object_lock))
    .bind(&row.default_encryption)
    .bind(&row.object_lock_default_mode)
    .bind(row.object_lock_default_days)
    .bind(&row.replication_mode)
    .bind(i64::from(row.deleting))
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_store::SqliteControlTxn;
    use crate::db::Db;

    #[tokio::test]
    async fn strict_export_maps_owners_and_rejects_dangling() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
             default_encryption, object_lock_default_mode, object_lock_default_days, \
             replication_mode, deleting) VALUES \
             ('admin-null', NULL, 1, 'Off', 0, NULL, NULL, NULL, 'async', 0), \
             ('admin-user', ?, 2, 'Enabled', 1, 'AES256', 'COMPLIANCE', 7, 'sync', 1), \
             ('alice', ?, 3, 'Off', 0, NULL, NULL, NULL, 'async', 0)",
        )
        .bind(admin)
        .bind(alice)
        .execute(db.pool())
        .await
        .unwrap();

        let mut tx = db.pool().begin().await.unwrap();
        let rows = {
            let mut txn = SqliteControlTxn::new(&mut tx);
            buckets(&mut txn).await.unwrap()
        };
        tx.rollback().await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            ["admin-null", "admin-user", "alice"]
        );
        assert_eq!(rows[0].owner, OwnerRef::Admin);
        assert_eq!(rows[1].owner, OwnerRef::Admin, "is_admin=1 → Admin");
        assert_eq!(rows[2].owner, OwnerRef::User("alice".into()));

        let mut conn = db.pool().acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('dangling', 999, 5)",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        let mut tx = db.pool().begin().await.unwrap();
        let err = {
            let mut txn = SqliteControlTxn::new(&mut tx);
            buckets(&mut txn).await.unwrap_err()
        };
        tx.rollback().await.unwrap();
        assert_eq!(err.to_string(), "owner user id 999 missing");
    }

    #[tokio::test]
    async fn install_roundtrip_preserves_set_id_and_config_bytes() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES ('b', NULL, 1, 7)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let row = BucketEntry {
            name: "b".into(),
            owner: OwnerRef::User("alice".into()),
            creation_date: 9,
            versioning: "Enabled".into(),
            object_lock: true,
            default_encryption: Some("AES256".into()),
            object_lock_default_mode: Some("GOVERNANCE".into()),
            object_lock_default_days: Some(3),
            replication_mode: "sync".into(),
            deleting: true,
        };
        let policies = vec![BucketPolicyEntry {
            bucket: "b".into(),
            policy_json: "{\"p\":1}".into(),
            updated_at: 2,
        }];
        let events = vec![EventConfigEntry {
            bucket: "b".into(),
            config_json: "[]".into(),
            updated_at: 3,
        }];
        let rules = vec![ReplicationRuleEntry {
            bucket: "b".into(),
            rules_json: "[]".into(),
            updated_at: 4,
        }];

        let mut tx = db.pool().begin().await.unwrap();
        {
            let mut txn = SqliteControlTxn::new(&mut tx);
            bucket_install_prepare(&mut txn, &["b".to_string()])
                .await
                .unwrap();
            bucket_upsert_install(&mut txn, &row).await.unwrap();
            replace_bucket_policies(&mut txn, &policies).await.unwrap();
            replace_event_configs(&mut txn, &events).await.unwrap();
            replace_replication_rules(&mut txn, &rules).await.unwrap();
        }
        tx.commit().await.unwrap();

        let (set_id, owner): (i64, Option<i64>) =
            sqlx::query_as("SELECT set_id, owner_id FROM buckets WHERE name = 'b'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(set_id, 7, "目标 set_id 保留");
        assert_eq!(owner, Some(alice));
        let (v, lock, mode, days, enc, repl, deleting): (
            String,
            i64,
            Option<String>,
            Option<i64>,
            Option<String>,
            String,
            i64,
        ) = sqlx::query_as(
            "SELECT versioning, object_lock, object_lock_default_mode, \
             object_lock_default_days, default_encryption, replication_mode, deleting \
             FROM buckets WHERE name = 'b'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            (
                v.as_str(),
                lock,
                mode.as_deref(),
                days,
                enc.as_deref(),
                repl.as_str(),
                deleting
            ),
            (
                "Enabled",
                1,
                Some("GOVERNANCE"),
                Some(3),
                Some("AES256"),
                "sync",
                1
            )
        );
        let p: String = sqlx::query_scalar("SELECT policy_json FROM bucket_policies")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(p, "{\"p\":1}");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT config_json FROM event_configs")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "[]"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT rules_json FROM replication_rules")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "[]"
        );
    }

    #[tokio::test]
    async fn install_missing_bucket_owner_is_rejected() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.create_user("root", "pw", true).await.unwrap();
        let row = BucketEntry {
            name: "b".into(),
            owner: OwnerRef::User("ghost".into()),
            creation_date: 1,
            versioning: "Off".into(),
            object_lock: false,
            default_encryption: None,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: "async".into(),
            deleting: false,
        };
        let mut tx = db.pool().begin().await.unwrap();
        let err = {
            let mut txn = SqliteControlTxn::new(&mut tx);
            bucket_upsert_install(&mut txn, &row).await.unwrap_err()
        };
        tx.rollback().await.unwrap();
        assert_eq!(
            err.to_string(),
            "invalid control data: bucket owner ghost not in snapshot"
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
}
