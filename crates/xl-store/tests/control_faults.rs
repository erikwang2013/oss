//! Task 6：控制对象（`.oss.sys/control/...`）崩溃/分区故障注入。
//!
//! 崩溃注入沿用 `kill9.rs` 的子进程重入模式（真实进程、真实盘、SIGABRT 注入）；
//! 分区用例用单节点集群拓扑（12 槽全本机，无 peer 传输）走真实 quorum 读/写路径：
//! 摘掉 4 盘（`drive_up` 依赖 `<drive>/.oss.sys`）后覆盖 8 < Q_s=9 → fail-closed。
//!
//! 场景矩阵（brief Task 6）：
//! 1. token 1 prepare 后崩溃 → token 2 提交 → token 1 重试必须 Conflict，token 2 内容胜。
//! 2. 第一阶段翻 1 槽后崩溃 → 数据无提交证书，重启**不可见**（不是半截内容）；
//!    token 2 两阶段提交后胜出。提交点 = 第二阶段证书，不是第一次数据翻转。
//! 3. 过期写者在新 token 之后提交/恢复 → 每槽 `Stale`，新 token 内容原样保留。
//! 4. 分区 < Q_s → 读 `CorruptMeta`、写 fail-closed，无陈旧成功；恢复后旧值不变。
//! 5. `inline_threshold = 0`（stretch 强制配置）→ system object 仍内联且可读写。

mod common;

use bytes::Bytes;
use common::*;
use xl_store::error::Error;
use xl_store::layout;
use xl_store::mesh::MetaWrite;

/// 场景 1：token 1 prepare 完成即崩（未翻转任何槽）；token 2 提交后，token 1 的重试
/// 必须 Conflict，且 token 2 的内容/版本/token 原样保留。
#[tokio::test]
async fn control_crash_before_commit_keeps_newer_token() {
    const KEY: &str = "control/crash-before-commit";
    const TEST: &str = "control_crash_before_commit_keeps_newer_token";

    if std::env::var(CHILD).is_ok() {
        let dirs = dirs_from_env();
        let s = open_store(&dirs, None, false).await;
        match std::env::var(PHASE).unwrap().as_str() {
            "prepare" => {
                let _ = s.put_system_object(KEY, b"token1", None, 1).await;
                unreachable!("abort 注入（prepare）未生效：put 返回了");
            }
            "stale" => match s.put_system_object(KEY, b"token1-retry", Some(1), 1).await {
                Err(Error::Conflict(msg)) if msg.contains("fence token") => {}
                other => panic!("过期 token 的提交必须 Conflict(fence)：{other:?}"),
            },
            other => unreachable!("未知 phase {other}"),
        }
        return;
    }

    let (_tmp, dirs) = temp_dirs();
    let exe = std::env::current_exe().unwrap();

    // ---- 段 1：token 1 准备后、提交前崩 ----
    assert_aborted(spawn(&exe, TEST, "prepare", &dirs, &[(ABORT_PREPARE, "1")]));
    let (committed, staged) = slot_counts(&dirs, KEY);
    assert_eq!(
        (committed, staged),
        (0, N_DRIVES),
        "注入点应在 prepare 之后、commit 之前"
    );

    // 崩溃后无任何已提交版本
    let s = open_store(&dirs, None, false).await;
    assert!(s.get_system_object(KEY).await.unwrap().is_none());

    // ---- 段 2：token 2 提交（覆盖 token 1 的 `.new` 暂存）----
    assert_eq!(
        s.put_system_object(KEY, b"token2", None, 2).await.unwrap(),
        1
    );
    assert_eq!(
        slot_counts(&dirs, KEY),
        (N_DRIVES, 0),
        "提交后不应残留 `.new`"
    );

    // ---- 段 3：重启 token 1 的写者 → 必须 Conflict，token 2 胜 ----
    assert!(
        spawn(&exe, TEST, "stale", &dirs, &[]).success(),
        "token 1 重试子进程应正常退出（观察到 Conflict）"
    );
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"token2");
    assert_eq!((obj.version, obj.fence_token), (1, 2));
    // 盘上每个槽都必须是 token 2 版本
    for d in &dirs {
        let m = disk_meta(d, KEY);
        assert_eq!(m.control_version, Some(1), "{}", d.display());
        assert_eq!(m.fence_token, Some(2), "{}", d.display());
    }
}

/// 场景 2：单机多盘（非集群）第一阶段翻 1 槽后崩——数据副本完整但**没有提交证书**，
/// 重启后读必须是缺席（未提交不可见），不是半截内容；token 2 两阶段提交后胜出。
/// 提交点 = 第二阶段证书（`control_committed = Some(true)`），不是第一次数据翻转。
#[tokio::test]
async fn single_node_multi_drive_phase1_crash_stays_uncommitted() {
    const KEY: &str = "control/crash-mid-commit";
    const TEST: &str = "single_node_multi_drive_phase1_crash_stays_uncommitted";

    if std::env::var(CHILD).is_ok() {
        let dirs = dirs_from_env();
        let s = open_store(&dirs, None, false).await;
        let phase = std::env::var(PHASE).unwrap();
        assert_eq!(phase, "flip1", "未知 phase");
        let _ = s.put_system_object(KEY, b"token1", None, 1).await;
        unreachable!("abort 注入（control commit K=1）未生效：put 返回了");
    }

    let (_tmp, dirs) = temp_dirs();
    let exe = std::env::current_exe().unwrap();

    // ---- 段 1：token 1 翻转第 1 槽即崩 ----
    assert_aborted(spawn(
        &exe,
        TEST,
        "flip1",
        &dirs,
        &[(ABORT_CONTROL_COMMIT_K, "1")],
    ));
    // 崩溃现场：恰 1 槽已提交，其余 11 槽仍是 `.new`
    assert_eq!(slot_counts(&dirs, KEY), (1, N_DRIVES - 1));

    // ---- 段 2：第一阶段数据（未提交）不可见：无证书但有副本 → CorruptMeta（不是缺席）----
    let s = open_store(&dirs, None, false).await;
    match s.get_system_object(KEY).await {
        Err(Error::CorruptMeta(_)) => {}
        other => panic!("无证书的第一阶段副本必须 fail-closed，不得读成缺席：{other:?}"),
    }
    // 盘上那一份是完整 meta（可被下一次写覆盖），只是没有证书
    assert_eq!(slot_counts(&dirs, KEY), (1, N_DRIVES - 1));

    // ---- 段 3：token 2 提交（版本越过已观察的未提交 v1 → 2；token 须高于已观察最大值 1）----
    assert_eq!(
        s.put_system_object(KEY, b"token2", None, 2).await.unwrap(),
        2
    );
    assert_eq!(slot_counts(&dirs, KEY), (N_DRIVES, 0));

    // ---- 段 4：完整重启 → token 2 胜，内容逐字节精确 ----
    drop(s);
    let s = open_store(&dirs, None, false).await;
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"token2");
    assert_eq!((obj.version, obj.fence_token), (2, 2));
    for d in &dirs {
        let m = disk_meta(d, KEY);
        assert_eq!(m.fence_token, Some(2), "{}", d.display());
        assert_eq!(m.versions.len(), 1, "{}: 不得残留半写版本", d.display());
        assert_eq!(
            &m.latest().unwrap().inline.as_ref().unwrap().bytes[..],
            b"token2",
            "{}",
            d.display()
        );
    }
}

/// 场景 3：新 token 已提交后，过期写者的 commit / 补偿 restore 每槽 `Stale`，
/// 新 token 的内容、版本、token 全部原样保留。
#[tokio::test]
async fn stale_commit_and_restore_after_newer_token_are_stale() {
    const KEY: &str = "control/stale-commit";
    let (_tmp, dirs) = temp_dirs();
    let s = open_store(&dirs, None, false).await;

    // token 1 提交 → 取它的盘上 meta 作为「过期写者持有的旧内容」
    assert_eq!(s.put_system_object(KEY, b"old", None, 1).await.unwrap(), 1);
    let old_bytes = Bytes::from(std::fs::read(control_dir(&dirs[0], KEY).join("xl.meta")).unwrap());
    // 新 token 2 提交
    assert_eq!(
        s.put_system_object(KEY, b"new", Some(1), 2).await.unwrap(),
        2
    );

    // 过期写者 prepare 自己的 `.new`（token 1）
    let ack = s
        .mesh_prepare_meta(MetaWrite {
            bucket: layout::SYS_DIR.into(),
            key: KEY.into(),
            meta: old_bytes.clone(),
            fence_token: Some(1),
            restore: false,
            slots: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(ack.slots.iter().filter(|x| x.ok).count(), N_DRIVES);

    // commit：对 token 2 的槽必须 Stale；槽 0 是保留的 v1/token1 旧证书副本，
    // 同 token 同内容 = 自己的幂等重试（不改盘），按已提交应答。
    let old_hash = xl_store::mesh::content_hash(&old_bytes);
    let ack = s
        .mesh_commit_meta(MetaWrite {
            bucket: layout::SYS_DIR.into(),
            key: KEY.into(),
            meta: old_bytes.clone(),
            fence_token: Some(1),
            restore: false,
            slots: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(ack.slots.len(), N_DRIVES);
    assert!(ack.slots[0].ok && ack.slots[0].hash == old_hash, "{ack:?}");
    assert!(
        ack.slots[1..].iter().all(|x| x.stale && !x.ok),
        "token 2 的槽必须 stale：{ack:?}"
    );

    // 补偿恢复（restore=token 1）：token 2 的槽 stale；槽 0 自恢复（同字节，无变化）
    let ack = s
        .mesh_commit_meta(MetaWrite {
            bucket: layout::SYS_DIR.into(),
            key: KEY.into(),
            meta: old_bytes,
            fence_token: Some(1),
            restore: true,
            slots: Vec::new(),
        })
        .await
        .unwrap();
    assert!(ack.slots[0].ok, "{ack:?}");
    assert!(ack.slots[1..].iter().all(|x| x.stale && !x.ok), "{ack:?}");

    // 新 token 内容原样保留；槽 0 是第一阶段保留的旧证书副本（Critical A）
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"new");
    assert_eq!((obj.version, obj.fence_token), (2, 2));
    let kept = disk_meta(&dirs[0], KEY);
    assert_eq!(
        (kept.control_version, kept.fence_token),
        (Some(1), Some(1)),
        "{}：保留槽必须是旧证书副本",
        dirs[0].display()
    );
    assert_eq!(
        &kept.latest().unwrap().inline.as_ref().unwrap().bytes[..],
        b"old",
        "{}",
        dirs[0].display()
    );
    for d in &dirs[1..] {
        let m = disk_meta(d, KEY);
        assert_eq!(m.control_version, Some(2), "{}", d.display());
        assert_eq!(m.fence_token, Some(2), "{}", d.display());
        assert_eq!(
            &m.latest().unwrap().inline.as_ref().unwrap().bytes[..],
            b"new",
            "{}",
            d.display()
        );
    }

    // 调用方 CAS：过期 token 的 put 直接 Conflict，且不留下 `.new`
    match s.put_system_object(KEY, b"stale", Some(2), 1).await {
        Err(Error::Conflict(_)) => {}
        other => panic!("过期 token 的 put 必须 Conflict：{other:?}"),
    }
    // token 2 槽上的 `.new` 仍在（stale 不消费）；槽 0 的暂存被自恢复消费
    assert_eq!(slot_counts(&dirs, KEY), (N_DRIVES, N_DRIVES - 1));
    assert_eq!(
        &s.get_system_object(KEY).await.unwrap().unwrap().data[..],
        b"new"
    );

    // 更新写者（token 2）暂存自己的 `.new`：abort(token 1) 不得清掉它。
    // 注意取 v2 槽（槽 0 是保留的旧证书副本）
    let new_bytes = Bytes::from(std::fs::read(control_dir(&dirs[1], KEY).join("xl.meta")).unwrap());
    let ack = s
        .mesh_prepare_meta(MetaWrite {
            bucket: layout::SYS_DIR.into(),
            key: KEY.into(),
            meta: new_bytes.clone(),
            fence_token: Some(2),
            restore: false,
            slots: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(ack.slots.iter().filter(|x| x.ok).count(), N_DRIVES);
    s.mesh_abort_meta(MetaWrite {
        bucket: layout::SYS_DIR.into(),
        key: KEY.into(),
        meta: Bytes::new(),
        fence_token: Some(1),
        restore: false,
        slots: Vec::new(),
    })
    .await
    .unwrap();
    assert_eq!(
        slot_counts(&dirs, KEY),
        (N_DRIVES, N_DRIVES),
        "abort(token 1) 必须保留更新写者的 `.new`"
    );
    for d in &dirs {
        let staged = xl_store::meta::XlMeta::decode(
            &std::fs::read(control_dir(d, KEY).join("xl.meta.new")).unwrap(),
        )
        .unwrap();
        assert_eq!(staged.fence_token, Some(2), "{}", d.display());
    }
    // 更新写者自己的 abort 才清理暂存
    s.mesh_abort_meta(MetaWrite {
        bucket: layout::SYS_DIR.into(),
        key: KEY.into(),
        meta: Bytes::new(),
        fence_token: Some(2),
        restore: false,
        slots: Vec::new(),
    })
    .await
    .unwrap();
    assert_eq!(slot_counts(&dirs, KEY), (N_DRIVES, 0));
    // 已提交版本不受 abort 影响
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"new");
    assert_eq!((obj.version, obj.fence_token), (2, 2));
}

/// 场景 4：分区 < Q_s。摘掉 4 盘后：quorum 读 `CorruptMeta`、写 fail-closed；
/// 恢复后旧值不变，随后写入照常成功（无陈旧成功、无半提交残留）。
#[tokio::test]
async fn partition_below_quorum_fails_closed_read_and_write() {
    const KEY: &str = "control/partition";
    let (_tmp, dirs) = temp_dirs();
    let s = open_store(&dirs, None, true).await; // 单节点集群：真实 quorum 语义

    assert_eq!(s.put_system_object(KEY, b"v1", None, 1).await.unwrap(), 1);

    // 摘掉 4 盘（drive_up 以 `<drive>/.oss.sys` 存在为准）→ 覆盖 8 < Q_s=9
    let down: Vec<std::path::PathBuf> = dirs[N_DRIVES - 4..].to_vec();
    for d in &down {
        std::fs::rename(d.join(layout::SYS_DIR), d.join(".oss.sys.down")).unwrap();
    }

    // 读：必须 CorruptMeta，不得返回旧值/None 伪装成功
    match s.get_system_object(KEY).await {
        Err(Error::CorruptMeta(_)) => {}
        other => panic!(
            "覆盖 {}/{} < Q_s={Q_S} 的读必须 CorruptMeta：{other:?}",
            N_DRIVES - 4,
            N_DRIVES
        ),
    }
    // 写：先在 `control_token_floor` 的 quorum 读处失败，只可能是 CorruptMeta
    match s.put_system_object(KEY, b"v2", Some(1), 2).await {
        Err(Error::CorruptMeta(_)) => {}
        other => panic!("分区写必须 fail-closed 于 token 下限读：{other:?}"),
    }
    // 分区期间不得留下任何 `.new` 提交意图
    for d in &dirs[..N_DRIVES - 4] {
        assert!(
            !control_dir(d, KEY).join("xl.meta.new").exists(),
            "{}：fail-closed 的写不得留下暂存",
            d.display()
        );
    }

    // 分区恢复：旧值原样，随后正常推进
    for d in &down {
        std::fs::rename(d.join(".oss.sys.down"), d.join(layout::SYS_DIR)).unwrap();
    }
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"v1");
    assert_eq!((obj.version, obj.fence_token), (1, 1));
    assert_eq!(
        s.put_system_object(KEY, b"v2", Some(1), 2).await.unwrap(),
        2
    );
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"v2");
    assert_eq!((obj.version, obj.fence_token), (2, 2));
}

/// 场景 5：`inline_threshold = 0`（stretch 启动强制值）下，system object 仍内联在
/// `xl.meta` 里、可读写；普通数据对象则确实走分片（证明阈值生效）。
///
/// shortcut: 用 forced `inline_threshold=0` 代理真实 2-site stretch 拓扑，
/// Phase 5 必须补真实 stretch e2e（跨站点读写 + system object 内联可见性）。
#[tokio::test]
async fn control_objects_stay_inline_with_zero_inline_threshold() {
    const KEY: &str = "control/inline";
    let (_tmp, dirs) = temp_dirs();
    let s = open_store(&dirs, Some(0), true).await;

    let data = b"control-plane-state-inline";
    assert_eq!(s.put_system_object(KEY, data, None, 1).await.unwrap(), 1);
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], data);
    assert_eq!((obj.version, obj.fence_token), (1, 1));

    // 控制写恰翻 Q_s 槽；每个已提交槽的 meta 都内联携带完整内容
    let mut present = 0;
    for d in &dirs {
        let Ok(bytes) = std::fs::read(control_dir(d, KEY).join("xl.meta")) else {
            continue;
        };
        present += 1;
        let m = xl_store::meta::XlMeta::decode(&bytes).unwrap();
        let inline = m
            .latest()
            .and_then(|v| v.inline.as_ref())
            .unwrap_or_else(|| panic!("{}：system object 必须内联", d.display()));
        assert_eq!(&inline.bytes[..], data, "{}", d.display());
        assert_eq!(m.control_version, Some(1), "{}", d.display());
        assert_eq!(m.fence_token, Some(1), "{}", d.display());
    }
    assert_eq!(present, Q_S, "集群控制写恰翻 Q_s 槽");

    // 阈值 0 确实生效：同一 Store 的普通小对象不内联
    s.create_bucket("bkt").await.unwrap();
    s.put_object("bkt", "small", Bytes::from_static(b"hello"), None)
        .await
        .unwrap();
    let path = layout::object_dir(&dirs[0], "bkt", "small").unwrap();
    let small =
        xl_store::meta::XlMeta::decode(&std::fs::read(path.join("xl.meta")).unwrap()).unwrap();
    assert!(
        small.latest().unwrap().inline.is_none(),
        "inline_threshold=0 下普通对象不得内联"
    );

    // 重启后仍可读
    drop(s);
    let s = open_store(&dirs, Some(0), true).await;
    assert_eq!(
        &s.get_system_object(KEY).await.unwrap().unwrap().data[..],
        data
    );
}
