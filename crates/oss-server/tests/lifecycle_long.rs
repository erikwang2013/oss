//! 发布后验证：生命周期破坏性长场景（真实 Store 落盘 + 真实 DB + 真实 sweep 入口）。
//!
//! 为什么不用 shell + HTTP：Date / Noncurrent 的到期判定依赖对象 `mod_time` 与服务端时钟，
//! 发布二进制没有时钟注入入口；实时等待 ≥1 天不可行。这里用 `sweep_once_at` 注入判定时刻，
//! 其余全部走生产路径（lifecycle::run_rule / sync_delete / xl-store 删除守卫）。
//! 当前版本 Days=0 的真实 worker + HTTP 路径已由 `tests/e2e_m8.sh` 步骤 5 覆盖。

mod common;

use common::state;
use oss_server::lifecycle::sweep_once_at;
use oss_server::state::AppState;
use xl_s3::lifecycle::{
    ExpirationAction, LifecycleFilter, LifecycleRuleSpec, NoncurrentExpiration,
};
use xl_store::meta::ObjectLockRecord;
use xl_store::store::VersioningMode;

fn spec() -> LifecycleRuleSpec {
    LifecycleRuleSpec {
        rule_id: None,
        filter: LifecycleFilter::default(),
        expiration: None,
        transitions: vec![],
        noncurrent_expiration: None,
        noncurrent_transitions: vec![],
        enabled: true,
    }
}

async fn versioned_bucket(st: &AppState, bucket: &str) {
    st.store.create_bucket(bucket).await.unwrap();
    st.db.insert_bucket(bucket, None).await.unwrap();
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = ?")
        .bind(bucket)
        .execute(st.db.pool())
        .await
        .unwrap();
}

fn stream(b: &'static [u8]) -> xl_store::store::ByteStream {
    Box::pin(futures::stream::once(async move {
        Ok(bytes::Bytes::from_static(b))
    }))
}

async fn put_version(st: &AppState, bucket: &str, key: &str, body: &'static [u8]) -> String {
    st.store
        .put_object_stream_with_meta(
            bucket,
            key,
            stream(body),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .version_id
        .unwrap()
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

type GetObject = Result<(xl_store::store::ObjectInfo, bytes::Bytes), xl_store::error::Error>;

fn no_such_key(r: GetObject) -> bool {
    matches!(r, Err(xl_store::error::Error::NoSuchKey(_)))
}

/// 当前版本 Days、Date 边界、版本化 marker、EODM、noncurrent 与保留期守卫，一次破坏性演练。
#[tokio::test]
async fn lifecycle_destructive_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("plain").await.unwrap();
    st.db.insert_bucket("plain", None).await.unwrap();
    versioned_bucket(&st, "ver").await;
    versioned_bucket(&st, "lock").await;
    assert_eq!(st.db.get_bucket_versioning("ver").await.unwrap(), "Enabled");
    assert_eq!(
        st.db.get_bucket_versioning("lock").await.unwrap(),
        "Enabled"
    );

    let now = unix_now();
    let body = bytes::Bytes::from_static(b"life");

    // ① 当前版本 Days=0：非版本化桶直接删除。
    st.store
        .put_object("plain", "a.txt", body.clone(), None)
        .await
        .unwrap();
    let mut s = spec();
    s.expiration = Some(ExpirationAction::Days { days: 0 });
    st.lifecycle.put_rules("plain", &[s]).await.unwrap();
    // now 早于对象 lmt 时 Days=0 尚未到期（秒级边界）；注入 now+2 模拟到期轮次。
    sweep_once_at(&st, unix_now() + 2).await;
    assert!(
        no_such_key(st.store.get_object("plain", "a.txt").await),
        "Days=0 应删除当前版本"
    );

    // ② Date 过滤：未到执行日不动；到点后删「date 之前创建」的对象。
    //    （lmt > date 的边界无法用外部配置造：API 拒绝过去的 date，且无时钟注入。）
    st.store
        .put_object("plain", "dated.txt", body.clone(), None)
        .await
        .unwrap();
    let next_midnight = (now / 86_400 + 1) * 86_400;
    let mut s = spec();
    s.expiration = Some(ExpirationAction::Date {
        date: next_midnight,
    });
    st.lifecycle.put_rules("plain", &[s]).await.unwrap();
    sweep_once_at(&st, now).await;
    assert!(
        st.store.get_object("plain", "dated.txt").await.is_ok(),
        "未到执行日不得删除"
    );
    sweep_once_at(&st, next_midnight + 3_600).await;
    assert!(
        no_such_key(st.store.get_object("plain", "dated.txt").await),
        "执行日后 lmt < date 的对象应删除"
    );

    // ③ 版本化桶当前版本过期 = 打 delete marker（数据版本仍在）。
    put_version(&st, "ver", "m.txt", b"v1").await;
    let mut s = spec();
    s.expiration = Some(ExpirationAction::Days { days: 0 });
    st.lifecycle.put_rules("ver", &[s]).await.unwrap();
    sweep_once_at(&st, unix_now() + 2).await;
    let vl = st
        .store
        .list_object_versions("ver", "m.txt", 10, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 2, "应为数据版本 + delete marker：{vl:?}");
    assert!(vl.entries[0].is_marker, "链首应是 marker：{vl:?}");
    assert!(
        no_such_key(st.store.get_object("ver", "m.txt").await),
        "marker 之下普通读应 404"
    );

    // ④ EODM：只删「链上唯一且为 marker」的墓碑；带数据版本的 marker 不动。
    st.store
        .delete_object_versioned("ver", "e.txt", None, VersioningMode::Enabled)
        .await
        .unwrap();
    let mut s = spec();
    s.expiration = Some(ExpirationAction::ExpiredObjectDeleteMarker);
    st.lifecycle.put_rules("ver", &[s]).await.unwrap();
    sweep_once_at(&st, now).await;
    let probe = st
        .store
        .list_object_versions("ver", "e.txt", 10, None, None)
        .await
        .unwrap();
    assert!(
        probe.entries.is_empty(),
        "唯一 marker 应被 EODM 清除：{probe:?}"
    );
    let keep = st
        .store
        .list_object_versions("ver", "m.txt", 10, None, None)
        .await
        .unwrap();
    assert_eq!(
        keep.entries.len(),
        2,
        "EODM 不得删带数据版本的 marker：{keep:?}"
    );

    // ⑤ Noncurrent + 保留期守卫：GOVERNANCE 锁定的 noncurrent 版本删不掉（计 skipped）；
    //    解除保留后同一规则才删 → 证明拦截只来自保留期，不是规则失效。
    let locked = st
        .store
        .put_object_stream_with_meta(
            "lock",
            "k.txt",
            stream(b"old"),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            Some(ObjectLockRecord {
                mode: Some("GOVERNANCE".into()),
                retain_until_nanos: Some(now_nanos() + 3_600 * 1_000_000_000),
                legal_hold: false,
            }),
            None,
            None,
        )
        .await
        .unwrap()
        .version_id
        .unwrap();
    put_version(&st, "lock", "k.txt", b"new").await;
    let mut s = spec();
    s.noncurrent_expiration = Some(NoncurrentExpiration {
        days: 1,
        newer: None,
    });
    st.lifecycle.put_rules("lock", &[s]).await.unwrap();
    sweep_once_at(&st, now + 2 * 86_400).await;

    let vl = st
        .store
        .list_object_versions("lock", "k.txt", 10, None, None)
        .await
        .unwrap();
    assert!(
        vl.entries.iter().any(|e| e.version_id == locked),
        "保留期内 noncurrent 版本不得被删：{vl:?}"
    );
    let job = st
        .db
        .list_jobs(20)
        .await
        .unwrap()
        .into_iter()
        .find(|j| j["bucket"] == "lock")
        .expect("应有 lock 桶的 lifecycle job");
    assert_eq!(job["deleted"], 0, "{job:?}");
    assert_eq!(job["skipped"], 1, "{job:?}");

    st.store
        .set_object_retention("lock", "k.txt", &locked, None, true)
        .await
        .unwrap();
    sweep_once_at(&st, now + 2 * 86_400).await;
    let vl = st
        .store
        .list_object_versions("lock", "k.txt", 10, None, None)
        .await
        .unwrap();
    assert!(
        !vl.entries.iter().any(|e| e.version_id == locked),
        "解除保留后 noncurrent 过期应生效：{vl:?}"
    );
    assert_eq!(vl.entries.len(), 1, "只剩当前版本：{vl:?}");
}
