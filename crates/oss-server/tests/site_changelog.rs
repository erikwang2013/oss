//! M7 切片 1a：控制面 apply 的 LWW/毒丸/桶删除分类/对账（对象后端）。
//!
//! SQLite 触发器时代的 no-op / 列级捕获 / apply_ctx 行断言随 SQLite 后端一并移除；
//! 等价的对象 hook 语义由 `control_store_obj` 单元测试覆盖。

mod common;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::site::{Entry, apply_changelog};
use oss_server::state::AppState;
use serde_json::json;

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

async fn hash_of(st: &AppState, username: &str) -> Option<String> {
    st.db
        .control_store()
        .login_user(username)
        .await
        .unwrap()
        .map(|u| u.password_hash)
}

fn local(st: &AppState) -> &oss_server::local_store::LocalStore {
    st.db
        .object_control()
        .unwrap()
        .control
        .local_store()
        .unwrap()
}

async fn dead_count(st: &AppState, origin: &str) -> usize {
    local(st).dead(origin).unwrap().count().unwrap()
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
    assert!(dead_count(&st, origin).await >= 5);

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
    let dead_payload: String = local(&st)
        .dead(origin)
        .unwrap()
        .scan()
        .unwrap()
        .into_iter()
        .find(|r| r.seq == 15)
        .expect("dead row 15")
        .payload;
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
    assert!(!st.db.bucket_exists("Bad/Name").await.unwrap());

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
    assert_eq!(
        st.db.get_bucket_versioning("keep").await.unwrap(),
        "Enabled"
    );
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
    assert!(!st.db.bucket_exists("gone").await.unwrap());

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
    assert!(st.db.bucket_exists("full").await.unwrap());

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
    assert!(!st.db.bucket_exists("lost").await.unwrap());

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
}

/// M4（S6d review）：M7 下 GET changelog 仍 405，但显式声明 `Allow: POST`。
#[tokio::test]
async fn m7_get_changelog_is_405_with_allow_post() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = oss_server::router::build_router(st).unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = reqwest::get(format!("http://{addr}/api/v1/site/changelog"))
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(resp.headers().get(reqwest::header::ALLOW).unwrap(), "POST");
}
