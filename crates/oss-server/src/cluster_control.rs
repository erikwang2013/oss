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
                    set: None,
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
            ControlQuery::BucketSet(bucket) => {
                // M4 切片 8：桶 → set（无行 = None；found=false 与「有行 set=0」由 set 字段区分）
                let row = sqlx::query("SELECT set_id FROM buckets WHERE name = ?")
                    .bind(&bucket)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(enc)?;
                Ok(row.map(|r| ControlRow {
                    set: Some(r.get::<i64, _>("set_id") as u32),
                    ..Default::default()
                }))
            }
        }
    }
}

/// 桶 → set 解析（M4 切片 8 / 子设计 S-C）：本地 SELECT → miss 扇出（**独立扇出面，不复用
/// 吞错的 ControlMesh::ask**——verifier M2）。语义：任一传输错 → Err；收集全部 Some 要求
/// set 一致（行分叉 = Err）；只有「全部可达且都答无」才 None。单 set 由 Store 侧短路，不调用。
#[derive(Debug)]
pub struct DbSetResolver {
    pool: sqlx::SqlitePool,
    peers: Vec<String>,
    transport: std::sync::Arc<dyn xl_store::mesh::MeshTransport>,
}

impl DbSetResolver {
    pub fn new(
        pool: sqlx::SqlitePool,
        peers: Vec<String>,
        transport: std::sync::Arc<dyn xl_store::mesh::MeshTransport>,
    ) -> Self {
        Self {
            pool,
            peers,
            transport,
        }
    }
}

#[async_trait::async_trait]
impl xl_store::mesh::SetResolver for DbSetResolver {
    async fn resolve_bucket_set(
        &self,
        bucket: &str,
    ) -> std::result::Result<Option<u32>, xl_store::error::Error> {
        use xl_store::error::Error;
        let enc = |e: sqlx::Error| Error::Encode(format!("set 解析查询失败: {e}"));
        // 本地先行（本节点恒可达）
        let local: Option<u32> = sqlx::query("SELECT set_id FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await
            .map_err(enc)?
            .map(|r| r.get::<i64, _>("set_id") as u32);
        let mut found: Option<u32> = local;
        if found.is_none() && !self.peers.is_empty() {
            // 并发扇出：任一 Err → 整体 Err（不可达 ≠ 无行）；Some 一致性要求
            let mut futs = futures::stream::FuturesUnordered::new();
            for p in &self.peers {
                let t = self.transport.clone();
                let b = bucket.to_string();
                futs.push(async move {
                    let row = t
                        .control_query(p, xl_store::mesh::ControlQuery::BucketSet(b))
                        .await;
                    (p.clone(), row)
                });
            }
            use futures::StreamExt as _;
            while let Some((node, row)) = futs.next().await {
                match row {
                    Ok(Some(r)) => {
                        if let Some(s) = r.set {
                            match found {
                                Some(prev) if prev != s => {
                                    return Err(Error::Encode(format!(
                                        "桶 {bucket} 的 set 行分叉：{prev} vs {s}（节点 {node}）——拒用，人工介入"
                                    )));
                                }
                                _ => found = Some(s),
                            }
                        }
                    }
                    // 「对端答无行」= 可达观察（收集完毕才判 None：全部可达且全无 → None）
                    Ok(None) => {}
                    Err(e) => {
                        return Err(Error::Encode(format!(
                            "set 解析扇出到 {node} 失败（不可达 ≠ 无行）：{e}"
                        )));
                    }
                }
            }
        }
        Ok(found)
    }
}
