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
                // 与 SigV4Auth::get_secret_key 同口径（条件逐字一致；M5 切片 5 起含
                // 会话父链条件——父须 long-term 且 active 未过期，父属主用户亦 active（P3-1））
                let row = sqlx::query(
                    "SELECT ak.secret_key AS sk, ak.user_id AS uid, u.is_admin AS is_admin, \
                            ak.session_policy AS sp \
                     FROM access_keys ak JOIN users u ON u.id = ak.user_id \
                     LEFT JOIN access_keys p ON p.access_key = ak.parent_ak \
                     LEFT JOIN users pu ON pu.id = p.user_id \
                     WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
                       AND (ak.expires_at IS NULL OR ak.expires_at > ?) \
                       AND (ak.parent_ak IS NULL OR (p.kind = 'long-term' \
                            AND p.status = 'active' AND (p.expires_at IS NULL OR p.expires_at > ?) \
                            AND pu.status = 'active'))",
                )
                .bind(&ak)
                .bind(now)
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
                    session_policy: r.get::<Option<String>, _>("sp"),
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

    /// M5 切片 0（P4-1）：桶行白名单写。接收端只写本地 DB（广播由发起端 best-effort 做）。
    async fn control_write(
        &self,
        w: xl_store::mesh::ControlWrite,
    ) -> std::result::Result<(), xl_store::error::Error> {
        let enc = |e: sqlx::Error| xl_store::error::Error::Encode(format!("控制面写失败: {e}"));
        match w {
            xl_store::mesh::ControlWrite::BucketRowSet { bucket, set } => match set {
                Some(s) => {
                    // 与 Db::upsert_bucket_set（启动回填对账）同文：只动 set_id，owner 不碰
                    sqlx::query(
                        "INSERT INTO buckets (name, owner_id, creation_date, set_id) \
                         VALUES (?, NULL, ?, ?) \
                         ON CONFLICT(name) DO UPDATE SET set_id = excluded.set_id",
                    )
                    .bind(&bucket)
                    .bind(crate::db::now())
                    .bind(s as i64)
                    .execute(&self.pool)
                    .await
                    .map_err(enc)?;
                }
                None => {
                    sqlx::query("DELETE FROM buckets WHERE name = ?")
                        .bind(&bucket)
                        .execute(&self.pool)
                        .await
                        .map_err(enc)?;
                }
            },
        }
        Ok(())
    }
}

/// 桶 → set 解析（M4 切片 8 / 子设计 S-C）：本地 SELECT → miss 扇出（**独立扇出面，不复用
/// 吞错的 ControlMesh::ask**——verifier M2）。语义：任一传输错 → Err；收集全部 Some 要求
/// set 一致（行分叉 = Err）；只有「全部可达且都答无」才 None。单 set 由 Store 侧短路，不调用。
/// M5 切片 0：数据面 `resolve_bucket_set`（本地先行）与对账 `resolve_bucket_set_authoritative`
/// （跳过本地短路，对端共识为准）两形态。
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

    /// 本地行读（DbSetResolver 语义的「本地先行」半边；本节点恒可达）。
    async fn local(
        &self,
        bucket: &str,
    ) -> std::result::Result<Option<u32>, xl_store::error::Error> {
        use xl_store::error::Error;
        let enc = |e: sqlx::Error| Error::Encode(format!("set 解析查询失败: {e}"));
        Ok(sqlx::query("SELECT set_id FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await
            .map_err(enc)?
            .map(|r| r.get::<i64, _>("set_id") as u32))
    }

    /// 对端扇出（**不含本地**）：任一 Err → Err（不可达 ≠ 无行）；Some 一致性要求。
    async fn fan_out(
        &self,
        bucket: &str,
    ) -> std::result::Result<Option<u32>, xl_store::error::Error> {
        use xl_store::error::Error;
        let mut found: Option<u32> = None;
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
        Ok(found)
    }
}

#[async_trait::async_trait]
impl xl_store::mesh::SetResolver for DbSetResolver {
    /// 数据面：本地先行（热路径）；本地 miss 才扇出。
    async fn resolve_bucket_set(
        &self,
        bucket: &str,
    ) -> std::result::Result<Option<u32>, xl_store::error::Error> {
        if let Some(l) = self.local(bucket).await? {
            return Ok(Some(l));
        }
        if self.peers.is_empty() {
            return Ok(None);
        }
        self.fan_out(bucket).await
    }

    /// M5 切片 0 对账：跳过本地短路，以对端共识为准（本地陈旧行被覆盖收敛）；对端全无行
    /// 时回落本地（可能是本节点建的桶而对端漏收广播——此时本地是唯一副本，不得判无行）。
    async fn resolve_bucket_set_authoritative(
        &self,
        bucket: &str,
    ) -> std::result::Result<Option<u32>, xl_store::error::Error> {
        if self.peers.is_empty() {
            return self.local(bucket).await;
        }
        match self.fan_out(bucket).await? {
            Some(s) => Ok(Some(s)),
            None => self.local(bucket).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xl_store::mesh::{ControlWrite, SetResolver as _};

    /// M5 切片 0（P4-1）：接收端写语义——B 的 DbSetResolver 本地先行查的就是这张表，
    /// 陈旧行落库覆盖后即「免重启」收敛；owner 不丢、缺行按回填语义补插、删行生效。
    #[tokio::test]
    async fn control_write_bucket_row_reconverges_stale_row() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        let uid = db.create_user("u1", "pw-123456", false).await.unwrap();
        let back = SqliteControlBackend::new(db.pool().clone());

        // 陈旧行（bkt 属 set0 带 owner）→ 广播说 set1：set 覆盖、owner 不碰
        db.insert_bucket_set("bkt", Some(uid), 0).await.unwrap();
        back.control_write(ControlWrite::BucketRowSet {
            bucket: "bkt".into(),
            set: Some(1),
        })
        .await
        .unwrap();
        let row = sqlx::query("SELECT owner_id, set_id FROM buckets WHERE name = 'bkt'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("set_id"), 1, "本地解析应立即见 set1");
        assert_eq!(
            row.get::<Option<i64>, _>("owner_id"),
            Some(uid),
            "对账不得清 owner"
        );

        // 缺行：补插（owner NULL，同启动回填语义）
        back.control_write(ControlWrite::BucketRowSet {
            bucket: "bkt2".into(),
            set: Some(0),
        })
        .await
        .unwrap();
        let row = sqlx::query("SELECT owner_id FROM buckets WHERE name = 'bkt2'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<Option<i64>, _>("owner_id"), None);

        // 删行（随删桶清理）
        back.control_write(ControlWrite::BucketRowSet {
            bucket: "bkt".into(),
            set: None,
        })
        .await
        .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'bkt'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "删行生效");
    }

    /// 程序化应答的 MeshTransport 桩：只实现 control_query（按预置队列弹出应答），
    /// 其余方法测试不应触达（触达即 unreachable）。
    #[derive(Debug, Default)]
    struct ProgMesh {
        replies:
            std::sync::Mutex<std::collections::VecDeque<std::result::Result<Option<u32>, String>>>,
    }

    impl ProgMesh {
        fn push(&self, r: std::result::Result<Option<u32>, &str>) {
            self.replies
                .lock()
                .unwrap()
                .push_back(r.map_err(str::to_string));
        }
    }

    #[async_trait::async_trait]
    impl xl_store::mesh::MeshTransport for ProgMesh {
        async fn write_shard(
            &self,
            _: &str,
            _: xl_store::mesh::ShardWrite,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn read_shard(
            &self,
            _: &str,
            _: xl_store::mesh::ShardRead,
        ) -> xl_store::error::Result<Option<bytes::Bytes>> {
            unreachable!("测试桩未预期调用")
        }
        async fn delete_version(
            &self,
            _: &str,
            _: xl_store::mesh::VersionDirRef,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn recycle_version(
            &self,
            _: &str,
            _: xl_store::mesh::VersionDirRef,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn delete_shard(
            &self,
            _: &str,
            _: xl_store::mesh::ShardDelete,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn prepare_meta(
            &self,
            _: &str,
            _: xl_store::mesh::MetaWrite,
        ) -> xl_store::error::Result<xl_store::mesh::MetaAck> {
            unreachable!("测试桩未预期调用")
        }
        async fn commit_meta(
            &self,
            _: &str,
            _: xl_store::mesh::MetaWrite,
        ) -> xl_store::error::Result<xl_store::mesh::MetaAck> {
            unreachable!("测试桩未预期调用")
        }
        async fn abort_meta(
            &self,
            _: &str,
            _: xl_store::mesh::MetaWrite,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn read_meta(
            &self,
            _: &str,
            _: xl_store::mesh::MetaRead,
        ) -> xl_store::error::Result<xl_store::mesh::MetaSnapshot> {
            unreachable!("测试桩未预期调用")
        }
        async fn delete_meta(
            &self,
            _: &str,
            _: xl_store::mesh::MetaRead,
        ) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn ensure_bucket(&self, _: &str, _: &str) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn delete_bucket_dir(&self, _: &str, _: &str) -> xl_store::error::Result<()> {
            unreachable!("测试桩未预期调用")
        }
        async fn list_bucket_dirs(&self, _: &str) -> xl_store::error::Result<Vec<String>> {
            unreachable!("测试桩未预期调用")
        }
        async fn list_local(
            &self,
            _: &str,
            _: xl_store::mesh::ListLocal,
        ) -> xl_store::error::Result<xl_store::mesh::ListLocalResp> {
            unreachable!("测试桩未预期调用")
        }
        async fn health(&self, _: &str) -> xl_store::error::Result<xl_store::mesh::NodeHealth> {
            unreachable!("测试桩未预期调用")
        }
        async fn lock(
            &self,
            _: &str,
            _: xl_store::mesh::LockReq,
        ) -> xl_store::error::Result<xl_store::mesh::LockResp> {
            unreachable!("测试桩未预期调用")
        }
        async fn control_query(
            &self,
            _: &str,
            _: xl_store::mesh::ControlQuery,
        ) -> xl_store::error::Result<Option<xl_store::mesh::ControlRow>> {
            let r = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("桩应答未预置");
            r.map_err(xl_store::error::Error::Encode).map(|set| {
                set.map(|s| xl_store::mesh::ControlRow {
                    set: Some(s),
                    ..Default::default()
                })
            })
        }
    }

    /// M5 切片 0（verifier 高 2 修复）：权威解析——本地陈旧行不得短路，对端共识为准；
    /// 对端全无行回落本地（唯一副本）；对端不可达 = Err；数据面本地先行语义保持不动。
    #[tokio::test]
    async fn authoritative_resolve_overrides_stale_local_row() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        db.insert_bucket_set("bkt", None, 0).await.unwrap(); // 本地陈旧 set0
        let mesh = std::sync::Arc::new(ProgMesh::default());
        let r = DbSetResolver::new(db.pool().clone(), vec!["n2".into()], mesh.clone());

        // 对端说 set1 → 权威 = set1（本地陈旧被覆盖）
        mesh.push(Ok(Some(1)));
        assert_eq!(
            r.resolve_bucket_set_authoritative("bkt").await.unwrap(),
            Some(1)
        );

        // 本地无行 + 对端全无行 → None（全可达且确无）
        mesh.push(Ok(None));
        assert_eq!(
            r.resolve_bucket_set_authoritative("nope").await.unwrap(),
            None
        );

        // 本地有行 + 对端全无行 → 回落本地（唯一副本，不得判无行）
        mesh.push(Ok(None));
        assert_eq!(
            r.resolve_bucket_set_authoritative("bkt").await.unwrap(),
            Some(0)
        );

        // 对端不可达 → Err（不可达 ≠ 无行）
        mesh.push(Err("boom"));
        assert!(r.resolve_bucket_set_authoritative("bkt").await.is_err());

        // 数据面同态：本地命中 → set0 且不扇出（预置应答不得被消费）
        mesh.push(Ok(Some(9)));
        assert_eq!(r.resolve_bucket_set("bkt").await.unwrap(), Some(0));
        assert_eq!(mesh.replies.lock().unwrap().len(), 1, "本地命中不得扇出");
    }
}
