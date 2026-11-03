//! Task 4：集群 legacy 控制对象补证书——fenced 证书写入、幂等与补齐。

use super::control_migrate::{certified_meta, legacy_meta, snapshot};
use super::*;
use crate::layout;
use crate::store::RecertifyOutcome;
use crate::store::control_migrate::canonical_content;

/// 盘上带证书的槽（全局槽号, token）。
async fn certificate_slots(c: &Cluster, key: &str) -> Vec<(u16, u64)> {
    let mut out = Vec::new();
    for (i, drive) in c.dirs.iter().flatten().enumerate() {
        if let Some(m) = disk_meta_opt(drive, layout::SYS_DIR, key).await
            && m.control_committed == Some(true)
        {
            out.push((i as u16, m.fence_token.unwrap_or(0)));
        }
    }
    out
}

/// 12 槽 9 份 legacy → Certified；证书 ≥9、token 严格递增；读得到数据；
/// 第二次调用 AlreadyCommitted 且全盘字节不变。
#[tokio::test]
async fn legacy_recertify_certifies_quorum_and_is_idempotent() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-basic";
    let m = legacy_meta(k, 1, 5, b"v1");
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let out = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    let RecertifyOutcome::Certified {
        version,
        certified,
        token,
    } = out
    else {
        panic!("实得：{out:?}");
    };
    assert_eq!(version, 1);
    assert!(certified >= 9, "证书 {certified} < Q_s");
    assert!(token > 5, "新证书 token {token} 未超过旧高水位 5");
    let certs = certificate_slots(&c, k).await;
    assert!(certs.len() >= 9, "证书槽 {} < Q_s", certs.len());
    assert!(certs.iter().all(|&(_, t)| t == token), "{certs:?}");
    let obj = c.stores[2].get_system_object(k).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));

    let before = snapshot(&c, k).await;
    let again = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    let RecertifyOutcome::AlreadyCommitted { version, certified } = again else {
        panic!("幂等重跑实得：{again:?}");
    };
    assert_eq!(version, 1);
    assert!(certified >= 9);
    assert_eq!(snapshot(&c, k).await, before, "幂等重跑必须零写入");
}

/// 绑定负例：8 份 < Q_s → CorruptMeta，全盘 `xl.meta`/`.new` 字节不变；
/// 无证书但存在副本的读仍 fail-closed（CorruptMeta，绝不伪装成缺席）。
#[tokio::test]
async fn legacy_recertify_below_quorum_writes_nothing() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-below";
    let m = legacy_meta(k, 1, 5, b"v1");
    for slot in 0..8 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let before = snapshot(&c, k).await;
    let err = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    assert_eq!(snapshot(&c, k).await, before, "失败路径必须零写入");
    assert!(
        matches!(
            c.stores[0].get_system_object(k).await,
            Err(crate::error::Error::CorruptMeta(_))
        ),
        "8 份无证书副本必须 fail-closed"
    );
}

/// 1 份证书 + 8 份同内容 legacy（总 9）→ 补齐为 Certified；版本/内容不变。
#[tokio::test]
async fn legacy_recertify_completes_partial_certificate() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-complete";
    let base = legacy_meta(k, 1, 4, b"v1");
    let mut cert = base.clone();
    cert.fence_token = Some(5);
    cert.control_committed = Some(true);
    write_control_slot(&c, 0, k, &cert).await;
    for slot in 1..9 {
        write_control_slot(&c, slot, k, &base).await;
    }
    let out = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    let RecertifyOutcome::Certified {
        version,
        certified,
        token,
    } = out
    else {
        panic!("实得：{out:?}");
    };
    assert_eq!(version, 1);
    assert!(certified >= 9);
    assert!(token > 5);
    let obj = c.stores[1].get_system_object(k).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));
    // 证书副本的规范内容必须仍是原组（补齐不得改数据/版本）。
    let want = canonical_content(&base).unwrap();
    for (slot, drive) in c.dirs.iter().flatten().enumerate() {
        let Some(m) = disk_meta_opt(drive, layout::SYS_DIR, k).await else {
            continue;
        };
        if m.control_committed == Some(true) {
            assert_eq!(canonical_content(&m).unwrap(), want, "槽 {slot} 内容被改写");
        }
    }
}

/// 已提交 v2（证书 4 份 = Q_s）绝不被 legacy v1 覆盖：AlreadyCommitted，字节原样。
#[tokio::test]
async fn legacy_recertify_never_overwrites_newer_committed() {
    let c = Cluster::new(7, 1).await; // Q_s = 4
    let k = "control/recertify-newer";
    let v2 = certified_meta(k, 2, 10, b"v2");
    for slot in 0..4 {
        write_control_slot(&c, slot, k, &v2).await;
    }
    let v1 = legacy_meta(k, 1, 7, b"v1");
    for slot in 4..7 {
        write_control_slot(&c, slot, k, &v1).await;
    }
    let before = snapshot(&c, k).await;
    let out = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    let RecertifyOutcome::AlreadyCommitted { version, certified } = out else {
        panic!("实得：{out:?}");
    };
    assert_eq!((version, certified), (2, 4));
    assert_eq!(snapshot(&c, k).await, before, "已提交状态不得被改写");
    assert_eq!(
        c.stores[0]
            .get_system_object(k)
            .await
            .unwrap()
            .unwrap()
            .data,
        b"v2"
    );
}

/// 同版本两份不同内容（各 < Q_s）→ CorruptMeta，不写。
#[tokio::test]
async fn legacy_recertify_refuses_same_version_fork() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-fork";
    let a = legacy_meta(k, 1, 5, b"v1a");
    let mut b = a.clone();
    b.versions[0].inline.as_mut().unwrap().bytes = b"v1b".to_vec();
    for slot in 0..5 {
        write_control_slot(&c, slot, k, &a).await;
    }
    for slot in 5..9 {
        write_control_slot(&c, slot, k, &b).await;
    }
    let before = snapshot(&c, k).await;
    let err = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    assert_eq!(snapshot(&c, k).await, before);
}

/// 锁 token 必须严格大于**所有已观察** token（含未提交残骸的高水位）。
#[tokio::test]
async fn legacy_recertify_token_is_strictly_monotonic() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-token";
    let base = legacy_meta(k, 1, 5, b"v1");
    let mut high = base.clone();
    high.fence_token = Some(100);
    write_control_slot(&c, 0, k, &high).await;
    for slot in 1..9 {
        write_control_slot(&c, slot, k, &base).await;
    }
    let out = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    let RecertifyOutcome::Certified { token, .. } = out else {
        panic!("实得：{out:?}");
    };
    assert!(token > 100, "新证书 token {token} 未超过全部旧 token 100");
    let certs = certificate_slots(&c, k).await;
    assert!(certs.len() >= 9);
    assert!(
        certs.iter().all(|&(_, t)| t > 100 && t == token),
        "{certs:?}"
    );
}

/// tombstone legacy 也补证书；证书落盘但读仍返回 None。
#[tokio::test]
async fn legacy_recertify_tombstone_certifies() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-tombstone";
    let mut m = crate::store::control::control_meta(k, 1, 5, None, true);
    m.control_committed = None;
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let out = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap();
    assert!(
        matches!(out, RecertifyOutcome::Certified { version: 1, .. }),
        "实得：{out:?}"
    );
    assert!(certificate_slots(&c, k).await.len() >= 9);
    assert!(c.stores[0].get_system_object(k).await.unwrap().is_none());
}

/// 混版（peer 缺 control_raw）→ PeerIncompatible，且槽 meta 一个字节都不改。
#[tokio::test]
async fn legacy_recertify_mixed_version_no_writes() {
    let c = Cluster::new(4, 3).await;
    let k = "control/recertify-mixed";
    let m = legacy_meta(k, 1, 5, b"v1");
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    c.mesh.set_control_raw("n2", Some(false));
    let before = snapshot(&c, k).await;
    let err = c.stores[0]
        .recertify_system_object_cluster(k)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::PeerIncompatible(_)),
        "实得：{err:?}"
    );
    assert_eq!(snapshot(&c, k).await, before);
}
