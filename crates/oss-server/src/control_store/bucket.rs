//! 桶 / 桶策略 / 事件通知 / 复制规则的 SQLite 读写实现。
//!
//! 运行期属主投影：`owner_id` NULL/0 或用户行缺失 → `Admin`；现存用户（含管理员）
//! → `User(username)`；`owner_uid` 原样保留本地 id。快照严格口径见 Task 6。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::bucket::{
    BucketPolicyEntry, BucketView, EventConfigEntry, ReplicationRuleEntry,
};
use xl_store::control_store::{ControlError, OwnerRef};

use super::SqliteControlStore;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

const BUCKET_SELECT: &str = "SELECT b.name, b.owner_id, b.creation_date, b.versioning, \
    b.object_lock, b.default_encryption, b.object_lock_default_mode, \
    b.object_lock_default_days, b.replication_mode, b.deleting, \
    u.username AS owner_username, COALESCE(u.is_admin, 0) AS owner_is_admin \
    FROM buckets b LEFT JOIN users u ON u.id = b.owner_id";

fn bucket_view(r: &SqliteRow) -> BucketView {
    let owner_uid: Option<i64> = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let owner = match (owner_uid, username) {
        (None, _) | (Some(0), _) | (_, None) => Some(OwnerRef::Admin),
        (_, Some(name)) => Some(OwnerRef::User(name)),
    };
    BucketView {
        name: r.get("name"),
        owner,
        owner_uid,
        owner_is_admin: r.get::<i64, _>("owner_is_admin") != 0,
        creation_date: r.get("creation_date"),
        versioning: r.get("versioning"),
        object_lock: r.get::<i64, _>("object_lock") != 0,
        default_encryption: r.get("default_encryption"),
        object_lock_default_mode: r.get("object_lock_default_mode"),
        object_lock_default_days: r.get("object_lock_default_days"),
        replication_mode: r.get("replication_mode"),
        deleting: r.get::<i64, _>("deleting") != 0,
    }
}

pub(super) async fn bucket(
    store: &SqliteControlStore,
    name: &str,
) -> Result<Option<BucketView>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{BUCKET_SELECT} WHERE b.name = ?"
    )))
    .bind(name)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(bucket_view))
}

pub(super) async fn buckets(store: &SqliteControlStore) -> Result<Vec<BucketView>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{BUCKET_SELECT} ORDER BY b.name"
    )))
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(bucket_view).collect())
}

pub(super) async fn bucket_policy(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<Option<BucketPolicyEntry>, ControlError> {
    let row =
        sqlx::query("SELECT bucket, policy_json, updated_at FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&store.pool)
            .await
            .map_err(backend)?;
    Ok(row.as_ref().map(|r| BucketPolicyEntry {
        bucket: r.get("bucket"),
        policy_json: r.get("policy_json"),
        updated_at: r.get("updated_at"),
    }))
}

pub(super) async fn bucket_policies(
    store: &SqliteControlStore,
) -> Result<Vec<BucketPolicyEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, policy_json, updated_at FROM bucket_policies ORDER BY bucket")
            .fetch_all(&store.pool)
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

pub(super) async fn bucket_notification(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<Option<EventConfigEntry>, ControlError> {
    let row =
        sqlx::query("SELECT bucket, config_json, updated_at FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&store.pool)
            .await
            .map_err(backend)?;
    Ok(row.as_ref().map(|r| EventConfigEntry {
        bucket: r.get("bucket"),
        config_json: r.get("config_json"),
        updated_at: r.get("updated_at"),
    }))
}

pub(super) async fn event_configs(
    store: &SqliteControlStore,
) -> Result<Vec<EventConfigEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, config_json, updated_at FROM event_configs ORDER BY bucket")
            .fetch_all(&store.pool)
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

pub(super) async fn bucket_replication_rules(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<Option<ReplicationRuleEntry>, ControlError> {
    let row = sqlx::query(
        "SELECT bucket, rules_json, updated_at FROM replication_rules WHERE bucket = ?",
    )
    .bind(bucket)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(|r| ReplicationRuleEntry {
        bucket: r.get("bucket"),
        rules_json: r.get("rules_json"),
        updated_at: r.get("updated_at"),
    }))
}

pub(super) async fn replication_rules(
    store: &SqliteControlStore,
) -> Result<Vec<ReplicationRuleEntry>, ControlError> {
    let rows =
        sqlx::query("SELECT bucket, rules_json, updated_at FROM replication_rules ORDER BY bucket")
            .fetch_all(&store.pool)
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

// ---------- 写路径（Task 4 追加） ----------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[tokio::test]
    async fn runtime_bucket_owner_projection_matches_legacy() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        // 悬空属主只能来自 FK 关闭时期的数据；本连接临时关闭 FK 造现场。
        let mut conn = db.pool().acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
             replication_mode, deleting) VALUES \
             ('null-owner', NULL, 1, 'Off', 0, 'async', 0), \
             ('alice-owner', ?, 2, 'Enabled', 1, 'sync', 0), \
             ('dangling', 999, 3, 'Off', 0, 'async', 1), \
             ('admin-owner', ?, 4, 'Off', 0, 'async', 0)",
        )
        .bind(alice)
        .bind(admin)
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);

        let store = db.control_store();
        let v = store.bucket("null-owner").await.unwrap().unwrap();
        assert_eq!(v.owner, Some(OwnerRef::Admin));
        assert_eq!(v.owner_uid, None);
        assert!(!v.owner_is_admin);

        let v = store.bucket("alice-owner").await.unwrap().unwrap();
        assert_eq!(v.owner, Some(OwnerRef::User("alice".into())));
        assert_eq!(v.owner_uid, Some(alice));

        let v = store.bucket("dangling").await.unwrap().unwrap();
        assert_eq!(v.owner, Some(OwnerRef::Admin));
        assert_eq!(v.owner_uid, Some(999));
        assert!(v.deleting);

        let v = store.bucket("admin-owner").await.unwrap().unwrap();
        assert_eq!(v.owner, Some(OwnerRef::User("root".into())));
        assert!(v.owner_is_admin);
    }

    #[tokio::test]
    async fn buckets_are_name_ordered() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        for name in ["b2", "b1"] {
            sqlx::query(
                "INSERT INTO buckets (name, creation_date, versioning) VALUES (?, 1, 'Off')",
            )
            .bind(name)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let names: Vec<String> = db
            .control_store()
            .buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, ["b1", "b2"]);
    }

    #[tokio::test]
    async fn config_reads_are_bucket_ordered_and_exact() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        for (bucket, json, ts) in [("b2", "{\"b\":2}", 2), ("b1", "{\"a\":1}", 1)] {
            sqlx::query(
                "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?)",
            )
            .bind(bucket)
            .bind(json)
            .bind(ts)
            .execute(db.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?)",
            )
            .bind(bucket)
            .bind(json)
            .bind(ts)
            .execute(db.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?)",
            )
            .bind(bucket)
            .bind(json)
            .bind(ts)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let store = db.control_store();
        let p = store.bucket_policies().await.unwrap();
        assert_eq!(
            p.iter().map(|r| r.bucket.as_str()).collect::<Vec<_>>(),
            ["b1", "b2"]
        );
        assert_eq!(p[0].policy_json, "{\"a\":1}");
        let e = store.event_configs().await.unwrap();
        assert_eq!(
            e.iter().map(|r| r.bucket.as_str()).collect::<Vec<_>>(),
            ["b1", "b2"]
        );
        let r = store.replication_rules().await.unwrap();
        assert_eq!(
            r.iter().map(|r| r.bucket.as_str()).collect::<Vec<_>>(),
            ["b1", "b2"]
        );
        assert_eq!(
            store
                .bucket_policy("b2")
                .await
                .unwrap()
                .unwrap()
                .policy_json,
            "{\"b\":2}"
        );
        assert_eq!(
            store
                .bucket_notification("b1")
                .await
                .unwrap()
                .unwrap()
                .config_json,
            "{\"a\":1}"
        );
        assert_eq!(
            store
                .bucket_replication_rules("b2")
                .await
                .unwrap()
                .unwrap()
                .rules_json,
            "{\"b\":2}"
        );
    }

    #[tokio::test]
    async fn missing_rows_are_none() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let store = db.control_store();
        assert!(store.bucket("missing").await.unwrap().is_none());
        assert!(store.bucket_policy("missing").await.unwrap().is_none());
        assert!(
            store
                .bucket_notification("missing")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .bucket_replication_rules("missing")
                .await
                .unwrap()
                .is_none()
        );
    }
}
