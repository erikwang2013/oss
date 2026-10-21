//! M7 切片 1a：控制面触发器捕获/门控 + changelog apply（LWW/毒丸/桶删除分类/对账）。

mod common;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::db::SitePeer;
use oss_server::site::{Entry, apply_changelog};
use oss_server::state::AppState;
use serde_json::json;
use sqlx::Row;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn site(name: &str, ch: char) -> SiteToml {
    SiteToml {
        id: None,
        name: Some(name.into()),
        region: "us-east-1".into(),
        priority: 0,
        token: Some(ch.to_string().repeat(32)),
    }
}

fn peer(peer_id: &str, remote_site_id: &str) -> SitePeer {
    SitePeer {
        peer_id: peer_id.into(),
        remote_site_id: remote_site_id.into(),
        name: peer_id.into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "AKIATEST000000001".into(),
        s3_secret_key: "SKTEST000000000001".into(),
        peer_site_token: "b".repeat(32),
        created_at: now(),
        last_seen: None,
        last_error: None,
        raft_node_id: None,
    }
}

async fn cl_count(st: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog")
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

async fn cl_rows(st: &AppState) -> Vec<(String, String, String, String)> {
    let rows = sqlx::query(
        "SELECT entity, entity_id, op, origin_site_id FROM site_changelog ORDER BY seq",
    )
    .fetch_all(st.db.pool())
    .await
    .unwrap();
    rows.iter()
        .map(|r| {
            (
                r.get("entity"),
                r.get("entity_id"),
                r.get("op"),
                r.get("origin_site_id"),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn ent(
    seq: i64,
    origin: &str,
    entity: &str,
    entity_id: &str,
    op: &str,
    payload: serde_json::Value,
    ts: i64,
    priority: i64,
) -> Entry {
    Entry {
        seq,
        origin_site_id: origin.into(),
        entity: entity.into(),
        entity_id: entity_id.into(),
        op: op.into(),
        payload,
        priority,
        ts,
    }
}

fn user_payload(username: &str, hash: &str) -> serde_json::Value {
    json!({
        "username": username,
        "password_hash": hash,
        "status": "active",
        "created_at": 1,
    })
}

async fn hash_of(st: &AppState, username: &str) -> Option<String> {
    sqlx::query_scalar("SELECT password_hash FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(st.db.pool())
        .await
        .unwrap()
}

async fn clock(st: &AppState, entity: &str, id: &str) -> Option<(i64, i64, String, i64)> {
    let r = sqlx::query(
        "SELECT ts, priority, origin_site_id, seq FROM site_entity_clock \
         WHERE entity = ? AND entity_id = ?",
    )
    .bind(entity)
    .bind(id)
    .fetch_optional(st.db.pool())
    .await
    .unwrap()?;
    Some((
        r.get("ts"),
        r.get("priority"),
        r.get("origin_site_id"),
        r.get("seq"),
    ))
}

async fn dead_count(st: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM site_apply_dead")
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

// ---------------- 触发器：捕获 / 门控 / admin 过滤 / apply 抑制 ----------------

#[tokio::test]
async fn capture_gate_admin_and_suppression() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site("a", 'a'))).await;

    // 未配对（sites 空）：控制面写不捕获、不落钟。
    st.db.create_user("root", "pw", true).await.unwrap();
    st.db.create_user("early", "pw", false).await.unwrap();
    assert_eq!(cl_count(&st).await, 0);
    assert!(clock(&st, "user", "early").await.is_none());

    st.db.insert_site(&peer("b", "remote-b")).await.unwrap();

    // 管理员行不复制（插入与时钟都不产生）。
    st.db.create_user("root2", "pw", true).await.unwrap();
    assert_eq!(cl_count(&st).await, 0);
    assert!(clock(&st, "user", "root2").await.is_none());

    // 普通用户：changelog + 时钟（origin=本站）。
    st.db.create_user("alice", "pw", false).await.unwrap();
    let rows = cl_rows(&st).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].0, "user");
    assert_eq!(rows[0].1, "alice");
    assert_eq!(rows[0].2, "upsert");
    assert_eq!(rows[0].3, st.site.id);
    let c = clock(&st, "user", "alice").await.expect("clock");
    assert_eq!(c.2, st.site.id);
    assert!(c.3 > 0, "clock seq 必须来自 changelog 行");

    // 维护性 no-op（delete_user 的 `UPDATE users SET id=id` 守卫）不得捕获；
    // 实值变更（status）必须捕获。
    let noop = cl_count(&st).await;
    sqlx::query("UPDATE users SET id = id WHERE username = 'alice'")
        .execute(st.db.pool())
        .await
        .unwrap();
    assert_eq!(cl_count(&st).await, noop, "no-op 用户更新不得捕获");
    sqlx::query("UPDATE users SET status = 'disabled' WHERE username = 'alice'")
        .execute(st.db.pool())
        .await
        .unwrap();
    assert_eq!(cl_count(&st).await, noop + 1, "status 实值变更必须捕获");

    // BEFORE 门控：远端未来钟 → 本地写 ABORT。
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('bucket', 'gated', ?, 0, 'remote-z', 99)",
    )
    .bind(now() + 120)
    .execute(st.db.pool())
    .await
    .unwrap();
    let err = st.db.insert_bucket("gated", None).await.unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );

    // 旧钟（远端更早）→ 本地写放行，钟翻转到本站。
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('bucket', 'open', ?, 0, 'remote-z', 1)",
    )
    .bind(now() - 10)
    .execute(st.db.pool())
    .await
    .unwrap();
    st.db.insert_bucket("open", None).await.unwrap();
    assert_eq!(clock(&st, "bucket", "open").await.unwrap().2, st.site.id);

    // 列级捕获：set_id 维护写不产生条目；versioning 实值变更产生。
    let before = cl_count(&st).await;
    st.db.upsert_bucket_set("open", 5).await.unwrap();
    assert_eq!(cl_count(&st).await, before, "set_id-only 更新不得捕获");
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'open'")
        .execute(st.db.pool())
        .await
        .unwrap();
    assert_eq!(cl_count(&st).await, before + 1);
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM site_changelog WHERE entity='bucket' AND entity_id='open' \
         ORDER BY seq DESC LIMIT 1",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert!(payload.contains("\"Enabled\""), "{payload}");

    // apply_ctx=1：既抑制捕获，也跳过 BEFORE 门控。
    st.db.set_apply_ctx(true).await.unwrap();
    st.db.create_user("quiet", "pw", false).await.unwrap();
    st.db.insert_bucket("gated", None).await.unwrap();
    st.db.set_apply_ctx(false).await.unwrap();
    assert!(
        !cl_rows(&st).await.iter().any(|r| r.1 == "quiet"),
        "apply 期不得捕获"
    );
    let active: i64 = sqlx::query_scalar("SELECT active FROM apply_ctx WHERE id=1")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(active, 0);
}

// ---------------- apply：LWW / 毒丸 / admin 目标 / 桶删除分类 / 对账 ----------------

#[tokio::test]
async fn apply_lww_poison_admin_and_bucket_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site("a", 'a'))).await;
    st.db.create_user("root", "pw", true).await.unwrap();
    let t = now();
    let origin = "remote-a";

    // 首次 upsert → 应用；更旧 → 跳过。
    let s = apply_changelog(
        &st,
        vec![ent(
            1,
            origin,
            "user",
            "alice",
            "upsert",
            user_payload("alice", "h1"),
            t - 10,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!((s.applied, s.skipped, s.dead), (1, 0, 0));
    assert_eq!(hash_of(&st, "alice").await.as_deref(), Some("h1"));

    let s = apply_changelog(
        &st,
        vec![ent(
            2,
            origin,
            "user",
            "alice",
            "upsert",
            user_payload("alice", "h0"),
            t - 20,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!((s.applied, s.skipped), (0, 1));
    assert_eq!(hash_of(&st, "alice").await.as_deref(), Some("h1"));

    // 同秒更高 priority 胜出。
    let s = apply_changelog(
        &st,
        vec![ent(
            3,
            origin,
            "user",
            "alice",
            "upsert",
            user_payload("alice", "h2"),
            t - 10,
            5,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.applied, 1);
    assert_eq!(hash_of(&st, "alice").await.as_deref(), Some("h2"));

    // 同秒同 priority、origin 字典序较小 → 跳过。
    let s = apply_changelog(
        &st,
        vec![ent(
            4,
            "remote-0",
            "user",
            "alice",
            "upsert",
            user_payload("alice", "h3"),
            t - 10,
            5,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.skipped, 1);
    assert_eq!(hash_of(&st, "alice").await.as_deref(), Some("h2"));

    // 目标为本地管理员 → 死信，口令不被覆盖。
    let s = apply_changelog(
        &st,
        vec![ent(
            10,
            origin,
            "user",
            "root",
            "upsert",
            user_payload("root", "evil"),
            t,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.dead, 1);
    assert_ne!(hash_of(&st, "root").await.as_deref(), Some("evil"));

    // AK 属主为本地管理员 → 死信（R11）。
    let s = apply_changelog(
        &st,
        vec![ent(
            11,
            origin,
            "access_key",
            "AKROOT000000000000",
            "upsert",
            json!({"access_key":"AKROOT000000000000","secret_key":"SK","username":"root",
                   "status":"active","expires_at":null,"created_at":1}),
            t,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.dead, 1);

    // 未来 ts / 未知 entity / 未知 op → 死信。
    for (seq, e) in [
        (
            12,
            ent(
                12,
                origin,
                "user",
                "future",
                "upsert",
                user_payload("future", "h"),
                t + 600,
                0,
            ),
        ),
        (13, ent(13, origin, "bogus", "x", "upsert", json!({}), t, 0)),
        (
            14,
            ent(14, origin, "user", "x", "frobnicate", json!({}), t, 0),
        ),
    ] {
        let s = apply_changelog(&st, vec![e]).await.unwrap();
        assert_eq!(s.dead, 1, "seq {seq}");
    }
    assert!(dead_count(&st).await >= 5);

    // 多字节载荷截断不得 panic（M4）：未知 entity → dead，payload 按字符边界截至 4KB。
    let big = ent(
        15,
        origin,
        "bogus",
        "cjk",
        "upsert",
        json!({"x": "中".repeat(2000)}),
        t,
        0,
    );
    let s = apply_changelog(&st, vec![big]).await.unwrap();
    assert_eq!(s.dead, 1);
    let dead_payload: String =
        sqlx::query_scalar("SELECT payload FROM site_apply_dead WHERE seq = 15")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(dead_payload.len() <= 4096);
    assert!(std::str::from_utf8(dead_payload.as_bytes()).is_ok());

    // 非法桶名是永久毒丸（M5）：dead，且不落行、不阻塞后续对账。
    let s = apply_changelog(
        &st,
        vec![ent(
            16,
            origin,
            "bucket",
            "Bad/Name",
            "upsert",
            json!({"name":"Bad/Name","versioning":"Off"}),
            t,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.dead, 1);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='Bad/Name'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);

    // 同 (peer,seq) 重投不冲突（R10）。
    let dup = ent(
        10,
        origin,
        "user",
        "root",
        "upsert",
        user_payload("root", "evil"),
        t,
        0,
    );
    let s = apply_changelog(&st, vec![dup]).await.unwrap();
    assert_eq!(s.dead, 1);

    // R9：已有物理桶的元数据 upsert 不得 500；对账幂等。
    st.store.create_bucket("keep").await.unwrap();
    st.db.insert_bucket("keep", None).await.unwrap();
    let up = |ts: i64, versioning: &str| {
        ent(
            20,
            origin,
            "bucket",
            "keep",
            "upsert",
            json!({"name":"keep","owner_username":null,"creation_date":t,
                   "versioning":versioning,"object_lock":0}),
            ts,
            0,
        )
    };
    let s = apply_changelog(&st, vec![up(t - 5, "Enabled")])
        .await
        .unwrap();
    assert_eq!(s.applied, 1);
    let v: String = sqlx::query_scalar("SELECT versioning FROM buckets WHERE name='keep'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(v, "Enabled");
    // 重投同一批（如 2xx 丢失）：跳过后仍按最终行对账，不报错。
    let s = apply_changelog(&st, vec![up(t - 5, "Enabled")])
        .await
        .unwrap();
    assert_eq!(s.skipped, 1);

    // 缺失物理桶的 upsert → 提交后对账建桶。
    let s = apply_changelog(
        &st,
        vec![ent(
            21,
            origin,
            "bucket",
            "newb",
            "upsert",
            json!({"name":"newb","versioning":"Off","creation_date":t,"object_lock":0}),
            t - 4,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.applied, 1);
    assert!(st.store.bucket_exists("newb").await.unwrap());

    // 空桶删除：物理 + 行都删除。
    st.store.create_bucket("gone").await.unwrap();
    st.db.insert_bucket("gone", None).await.unwrap();
    let s = apply_changelog(
        &st,
        vec![ent(
            22,
            origin,
            "bucket",
            "gone",
            "delete",
            json!({"name":"gone"}),
            t - 3,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.applied, 1);
    assert!(!st.store.bucket_exists("gone").await.unwrap());
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='gone'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);

    // 非空桶删除：死信 + 行/数据保留（R3b）。
    st.store.create_bucket("full").await.unwrap();
    st.db.insert_bucket("full", None).await.unwrap();
    st.store
        .put_object("full", "k", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let s = apply_changelog(
        &st,
        vec![ent(
            23,
            origin,
            "bucket",
            "full",
            "delete",
            json!({"name":"full"}),
            t - 2,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.dead, 1);
    assert!(st.store.bucket_exists("full").await.unwrap());
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='full'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);

    // 目录已不在（上次物理删后回滚）：NoSuchBucket 视为已删，行清掉（R12）。
    st.store.create_bucket("lost").await.unwrap();
    st.db.insert_bucket("lost", None).await.unwrap();
    st.store.delete_bucket("lost").await.unwrap();
    let s = apply_changelog(
        &st,
        vec![ent(
            24,
            origin,
            "bucket",
            "lost",
            "delete",
            json!({"name":"lost"}),
            t - 1,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.applied, 1);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='lost'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);

    // 自身 origin 回环 → 跳过。
    let s = apply_changelog(
        &st,
        vec![ent(
            30,
            &st.site.id,
            "user",
            "loop",
            "upsert",
            user_payload("loop", "h"),
            t,
            0,
        )],
    )
    .await
    .unwrap();
    assert_eq!(s.skipped, 1);

    // 事务结束后 apply_ctx 复位。
    let active: i64 = sqlx::query_scalar("SELECT active FROM apply_ctx WHERE id=1")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(active, 0);
}
