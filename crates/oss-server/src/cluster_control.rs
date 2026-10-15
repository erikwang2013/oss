//! 控制面读扇出的服务端执行器（M3 Task 9）：把 mesh 的 `ControlQuery` 落成**本节点**的
//! SQL 查询。判权语义复用 xl-s3 的 `Acl`（SQL 唯一事实源，防两套实现漂移）；
//! `AccessKey` 的 SK 查询与 `SigV4Auth::get_secret_key` 同口径（同一条 JOIN、同 status/
//! expires 条件——认证面与扇出面查的是同一行，撤销在两面同时生效）。

use sqlx::{Row, SqlitePool};

use xl_s3::access::{Acl, Action, AuthedUser};
use xl_store::error::{Error, Result};
use xl_store::mesh::{ControlBackend, ControlQuery, ControlRow};

#[derive(Debug)]
pub struct SqliteControlBackend {
    pool: SqlitePool,
}

impl SqliteControlBackend {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn enc(e: impl std::fmt::Display) -> Error {
    Error::Encode(format!("控制面查询失败：{e}"))
}

#[async_trait::async_trait]
impl ControlBackend for SqliteControlBackend {
    async fn control_query(&self, q: ControlQuery) -> Result<Option<ControlRow>> {
        match q {
            ControlQuery::AccessKey(ak) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                // 与 SigV4Auth::get_secret_key 同口径（条件逐字一致）
                let row = sqlx::query(
                    "SELECT ak.secret_key AS sk, ak.user_id AS uid, u.is_admin AS is_admin \
                     FROM access_keys ak JOIN users u ON u.id = ak.user_id \
                     WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
                       AND (ak.expires_at IS NULL OR ak.expires_at > ?)",
                )
                .bind(&ak)
                .bind(now)
                .fetch_optional(&self.pool)
                .await
                .map_err(enc)?;
                Ok(row.map(|r| ControlRow {
                    uid: Some(r.get("uid")),
                    is_admin: Some(r.get::<i64, _>("is_admin") != 0),
                    active: Some(true),
                    secret: Some(r.get("sk")),
                    flag: None,
                    names: None,
                }))
            }
            ControlQuery::Permission {
                uid,
                bucket,
                action,
            } => {
                let act = match action {
                    0 => Action::Read,
                    1 => Action::Write,
                    2 => Action::Delete,
                    _ => return Err(Error::Encode(format!("未知权限动作 {action}"))),
                };
                let acl = Acl::new(self.pool.clone());
                // 发起端已完成 admin 判定；对端本地语义 = owner ∨ permission 位
                //（与 Acl::can 的非 admin 分支同构）
                let granted = acl.is_owner(&bucket, uid).await.map_err(enc)?
                    || acl
                        .can(
                            AuthedUser {
                                uid,
                                is_admin: false,
                                ak: String::new(),
                            },
                            act,
                            &bucket,
                        )
                        .await
                        .map_err(enc)?;
                Ok(Some(ControlRow {
                    uid: Some(uid),
                    flag: Some(granted),
                    ..Default::default()
                }))
            }
            ControlQuery::OwnedBuckets(uid) => {
                let rows = sqlx::query("SELECT name FROM buckets WHERE owner_id = ?")
                    .bind(uid)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(enc)?;
                Ok(Some(ControlRow {
                    names: Some(rows.into_iter().map(|r| r.get("name")).collect()),
                    ..Default::default()
                }))
            }
            ControlQuery::VisibleBuckets(uid) => {
                let acl = Acl::new(self.pool.clone());
                let names = acl.visible(uid).await.map_err(enc)?;
                Ok(Some(ControlRow {
                    names: Some(names),
                    ..Default::default()
                }))
            }
            ControlQuery::BucketOwner(bucket) => {
                let row = sqlx::query("SELECT owner_id FROM buckets WHERE name = ?")
                    .bind(&bucket)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(enc)?;
                Ok(row.map(|r| ControlRow {
                    uid: r.get::<Option<i64>, _>("owner_id"),
                    ..Default::default()
                }))
            }
        }
    }
}
