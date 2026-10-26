//! M3 shrink T5–T7：apply / verify / commit / rollback（in-process 4→3 集群）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::drives::{ClusterSets, NodeSpec, SlotMap, SlotTarget};
use super::ec::EcConfig;
use super::shrink::ShrinkPlanReport;
use super::tests::{Cluster, InProcessMesh};
use super::{Store, StoreConfig};

fn n(id: &str, dc: u16) -> NodeSpec {
    NodeSpec {
        id: id.into(),
        addr: format!("127.0.0.1:1910{}", &id[1..]),
        drive_count: dc,
    }
}

fn ec84() -> EcConfig {
    EcConfig::default()
}
/// 新拓扑：退役 n4，槽 9/10/11 归 n1/n2/n3 的 drive 0（槽数与保留节点盘序不变）。
fn target(epoch: u64) -> ClusterSets {
    let mut slots = vec![
        SlotTarget {
            node: String::new(),
            drive: 0
        };
        12
    ];
    for (i, s) in slots.iter_mut().enumerate() {
        *s = SlotTarget {
            node: format!("n{}", i / 3 + 1),
            drive: (i % 3) as u16,
        };
    }
    for (i, node) in [(9, "n1"), (10, "n2"), (11, "n3")] {
        slots[i] = SlotTarget {
            node: node.into(),
            drive: 0,
        };
    }
    ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n1", 3), n("n2", 3), n("n3", 3)]],
        vec![Some(SlotMap { epoch, slots })],
        &ec84(),
    )
    .unwrap()
}

/// 单节点 12 槽全在 0 号盘：只用于拓扑记录格式/启动校验（与集群 fixture 解耦）。
fn one_node(epoch: u64) -> ClusterSets {
    let slots = std::iter::repeat_n(
        SlotTarget {
            node: "n1".into(),
            drive: 0,
        },
        12,
    )
    .collect();
    ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n1", 1)]],
        vec![Some(SlotMap { epoch, slots })],
        &ec84(),
    )
    .unwrap()
}

/// 一个 EC 对象（12 片各落一槽）+ 一个 inline 对象。
async fn setup() -> (Cluster, ShrinkPlanReport) {
    let c = Cluster::new_in_maintenance(4, 3).await;
    let s = &c.stores[0];
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "ec", Bytes::from(vec![7u8; 300 * 1024]), None)
        .await
        .unwrap();
    s.put_object("alpha", "small", Bytes::from_static(b"inline"), None)
        .await
        .unwrap();
    let report = s.shrink_plan(&target(2)).await.unwrap();
    assert_eq!(report.plan.moves.len(), 3);
    (c, report)
}

/// 本机目标槽的全部分片（路径, CRC）——复用生产侧 plan_ctx/collect_moved 解析。
async fn target_files(store: &Store, report: &ShrinkPlanReport) -> Vec<(PathBuf, u32)> {
    let ctx = store.plan_ctx(&report.plan).unwrap();
    let data = store.collect_moved(&ctx.local_moves).await.unwrap();
    let cl = store.cluster().unwrap();
    let mut out = Vec::new();
    for ms in &data.shards {
        let mv = ctx.local_moves.get(&(ms.set, ms.slot)).unwrap();
        let d = store.target_drive(cl, ms.set, mv.to_drive).unwrap();
        let p = super::drives::shard_path(d, &ms.bucket, &ms.key, &ms.uuid, &ms.shard).unwrap();
        out.push((p, ms.shard.crc32c));
    }
    out
}

async fn apply_all(c: &Cluster, report: &ShrinkPlanReport) {
    for i in 0..3 {
        c.stores[i].shrink_apply(report).await.unwrap();
    }
}

async fn verify_all(c: &Cluster, report: &ShrinkPlanReport) {
    for i in 0..3 {
        let r = c.stores[i].shrink_verify(report).await.unwrap();
        assert!(r.failures.is_empty(), "{:?}", r.failures);
        assert!(r.receipt.is_some());
    }
}

async fn err_of(
    f: impl std::future::Future<Output = crate::error::Result<super::shrink_apply::ShrinkActionReport>>,
) -> String {
    f.await.unwrap_err().to_string()
}

/// `Store` 没有 Debug，`Result<Store>::unwrap_err` 不可用。
fn store_err(r: crate::error::Result<Store>) -> String {
    match r {
        Ok(_) => panic!("预期打开失败"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn apply_copies_targets_without_touching_meta_and_resumes() {
    let (c, report) = setup().await;
    let meta_before = c.stores[0].read_meta("alpha", "ec").await.unwrap().unwrap();
    for i in 0..3 {
        let r = c.stores[i].shrink_apply(&report).await.unwrap();
        assert_eq!(r.local_slots, 1, "每保留节点各接 1 槽");
        assert_eq!(r.files_copied, 1, "{r:?}");
        assert!(r.receipt.is_some());
    }
    for i in 0..3 {
        let files = target_files(&c.stores[i], &report).await;
        assert!(!files.is_empty());
        for (path, crc) in &files {
            let b = tokio::fs::read(path).await.unwrap();
            assert_eq!(super::ec::crc32c_of(&b), *crc, "{}", path.display());
        }
    }
    let meta_after = c.stores[0].read_meta("alpha", "ec").await.unwrap().unwrap();
    assert_eq!(meta_before, meta_after, "apply 不得改 xl.meta（槽号稳定）");
    // 重跑：全部命中已验收文件（幂等）。
    for i in 0..3 {
        let r = c.stores[i].shrink_apply(&report).await.unwrap();
        assert_eq!(r.files_copied, 0, "{r:?}");
        assert_eq!(
            r.files_skipped as usize,
            target_files(&c.stores[i], &report).await.len()
        );
    }
    // 拷贝中断形态：删一片后重跑只补该片（resume）。
    let victim = target_files(&c.stores[0], &report).await[0].0.clone();
    tokio::fs::remove_file(&victim).await.unwrap();
    let r = c.stores[0].shrink_apply(&report).await.unwrap();
    assert_eq!(r.files_copied, 1, "{r:?}");
    assert!(tokio::fs::try_exists(&victim).await.unwrap());
    // 源片缺失：删 n4 槽 9 源片 + 本机目标副本 → apply 用同版本其它 11 片 EC 重建（shard 9 落 slot 9）。
    let meta = c.stores[3].read_meta("alpha", "ec").await.unwrap().unwrap();
    let v = &meta.versions[0];
    let s9 = v.data_layout.as_ref().unwrap().shards[9].clone();
    let src = super::drives::shard_path(&c.dirs[3][0], "alpha", "ec", &v.part.uuid, &s9).unwrap();
    let cl = c.stores[3].cluster().unwrap();
    assert_eq!(cl.sets.sets[0].slot_target(9).unwrap().node, "n4");
    tokio::fs::remove_file(&src).await.unwrap();
    let (p, _) = target_files(&c.stores[0], &report).await.remove(0);
    tokio::fs::remove_file(&p).await.unwrap();
    let r = c.stores[0].shrink_apply(&report).await.unwrap();
    assert_eq!(r.reconstructed, 1, "{r:?}");
    let (p, crc) = target_files(&c.stores[0], &report).await.remove(0);
    let b = tokio::fs::read(p).await.unwrap();
    assert_eq!(super::ec::crc32c_of(&b), crc);
}

#[tokio::test]
async fn verify_detects_corruption_and_apply_repairs_target() {
    let (c, report) = setup().await;
    apply_all(&c, &report).await;
    verify_all(&c, &report).await;
    let victim = target_files(&c.stores[0], &report).await[0].0.clone();
    let mut b = tokio::fs::read(&victim).await.unwrap();
    b[0] ^= 0xff;
    tokio::fs::write(&victim, &b).await.unwrap();
    let bad = c.stores[0].shrink_verify(&report).await.unwrap();
    assert!(
        bad.failures.iter().any(|f| f.contains("CRC")),
        "{:?}",
        bad.failures
    );
    assert!(bad.receipt.is_none(), "失败不得留下 verify 收据");
    // apply 从源片重抄（CRC 不符 → 走源读，源仍健康）。
    let r = c.stores[0].shrink_apply(&report).await.unwrap();
    assert!(r.files_copied >= 1, "{r:?}");
    verify_all(&c, &report).await;
}

#[tokio::test]
async fn rollback_before_commit_removes_targets_only() {
    let (c, report) = setup().await;
    apply_all(&c, &report).await;
    let mut targets = Vec::new();
    for i in 0..3 {
        targets.push(target_files(&c.stores[i], &report).await);
    }
    for (i, target) in targets.iter().enumerate() {
        let r = c.stores[i].shrink_rollback(&report).await.unwrap();
        assert_eq!(r.files_removed as usize, target.len(), "{r:?}");
    }
    for files in &targets {
        for (p, _) in files {
            assert!(!tokio::fs::try_exists(p).await.unwrap(), "{}", p.display());
        }
    }
    // 旧分片/旧拓扑原样：对象仍可读，topo 仍是 v2。
    let (_, body) = c.stores[0].get_object("alpha", "ec").await.unwrap();
    assert_eq!(body.len(), 300 * 1024);
    let topo = std::fs::read_to_string(crate::layout::sys_dir(&c.dirs[0][0], "topo")).unwrap();
    assert!(topo.starts_with("v2 "), "{topo}");
    // rollback 后重跑 apply 可恢复；commit 无 verify 收据仍拒绝。
    apply_all(&c, &report).await;
    let e = err_of(c.stores[0].shrink_commit(&report)).await;
    assert!(e.contains("verify"), "{e}");
}

#[tokio::test]
async fn commit_requires_apply_and_verify_then_writes_v4() {
    let (c, report) = setup().await;
    let e = err_of(c.stores[0].shrink_commit(&report)).await;
    assert!(e.contains("apply") && e.contains("verify"), "{e}");
    apply_all(&c, &report).await;
    let e = err_of(c.stores[0].shrink_commit(&report)).await;
    assert!(e.contains("verify"), "{e}");
    verify_all(&c, &report).await;
    for i in 0..3 {
        let r = c.stores[i].shrink_commit(&report).await.unwrap();
        assert_eq!(r.topo_written, 3, "3 盘逐盘写");
        assert!(!r.already_committed);
        for d in 0..3 {
            let path = crate::layout::sys_dir(&c.dirs[i][d], "topo");
            let s = std::fs::read_to_string(path).unwrap();
            assert_eq!(
                s.trim(),
                format!(
                    "v4 set=0 node=n{} slot={} epoch=2 hash={}",
                    i + 1,
                    d,
                    report.plan.new_hash
                )
            );
        }
    }
    // 旧分片保留（commit 前一直可读 → 删 n4 前仍有副本）。
    assert!(
        tokio::fs::try_exists(&c.dirs[3][0].join("alpha/ec"))
            .await
            .unwrap()
    );
    // 幂等重跑。
    let r = c.stores[0].shrink_commit(&report).await.unwrap();
    assert!(r.already_committed && r.topo_written == 0);
    // commit 后 rollback 拒绝（盘上已是新 v4；进程内旧配置 caps 仍是旧值）。
    let e = err_of(c.stores[0].shrink_rollback(&report)).await;
    assert!(e.contains("commit"), "{e}");
}

#[tokio::test]
async fn apply_rejects_tampered_plan_and_unhealthy_peers() {
    let (c, report) = setup().await;
    type Mutate = Box<dyn Fn(&mut ShrinkPlanReport)>;
    let cases: Vec<(&str, Mutate)> = vec![
        ("schema", Box::new(|r| r.plan.schema = 99)),
        ("epoch", Box::new(|r| r.plan.new_epoch += 1)),
        ("槽数", Box::new(|r| r.plan.slot_counts = vec![11])),
        (
            "指纹",
            Box::new(|r| r.plan.old_hash = "0000000000000000".into()),
        ),
        ("源", Box::new(|r| r.plan.moves[0].from_node = "n1".into())),
        (
            "重建的新拓扑",
            Box::new(|r| r.plan.new_hash = "0000000000000000".into()),
        ),
        ("越界", Box::new(|r| r.plan.moves[0].to_drive = 9)),
    ];
    for (want, mutate) in cases {
        let mut bad = report.clone();
        mutate(&mut bad);
        let e = err_of(c.stores[0].shrink_apply(&bad)).await;
        assert!(e.contains(want), "want {want}: {e}");
    }
    // 对端不在 maintenance / 不可达 → 拒绝（本节点自身是 maintenance）。
    c.mesh.down("n2");
    let e = err_of(c.stores[0].shrink_apply(&report)).await;
    assert!(e.contains("n2") && e.contains("不可达"), "{e}");
    c.mesh.up("n2");
    // 非 maintenance 集群：plan_ctx 第一道门即拒。
    let c2 = Cluster::new(4, 3).await;
    let mut dummy = report.clone();
    dummy.plan.old_hash = c2.stores[0].cluster().unwrap().sets.topology_hash();
    dummy.plan.old_wire_hash = c2.stores[0].local_caps().topo_hash;
    let e = err_of(c2.stores[0].shrink_apply(&dummy)).await;
    assert!(e.contains("maintenance=true"), "{e}");
    // 退役节点不得执行任何 shrink 命令。
    let e = err_of(c.stores[3].shrink_apply(&report)).await;
    assert!(e.contains("退役") && e.contains("n4"), "{e}");
}

/// 打开同一批盘但换拓扑配置（commit 后重启的进程内等价）。
async fn open_with(
    dirs: Vec<PathBuf>,
    id: &str,
    sets: ClusterSets,
    ec: EcConfig,
) -> crate::error::Result<Store> {
    open_on(InProcessMesh::new(), dirs, id, sets, ec).await
}

/// 注册到既有 mesh：模拟「某节点已换新配置重启，其余节点还跑旧配置」。
async fn open_on(
    mesh: Arc<InProcessMesh>,
    dirs: Vec<PathBuf>,
    id: &str,
    sets: ClusterSets,
    ec: EcConfig,
) -> crate::error::Result<Store> {
    let local = crate::mesh_gate::local_caps(&sets, &ec, 1);
    let transport: Arc<dyn crate::mesh::MeshTransport> = Arc::new(
        crate::mesh_gate::GatedMesh::with_local(mesh.clone(), local, Duration::ZERO),
    );
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs;
    cfg.ec = ec;
    cfg.fsync = false;
    cfg.maintenance = true;
    cfg.cluster = Some(Arc::new(super::ClusterConfig {
        sets,
        transport,
        placement_v2: false,
        resolver: None,
        stretch: false,
    }));
    let s = Arc::new(Store::open(cfg).await?);
    mesh.register(id, &s);
    Arc::try_unwrap(s).map_err(|_| crate::error::Error::Encode("store 被传输持有".into()))
}

/// 规则 (a)：全部节点必须在旧拓扑上 commit 完，才允许逐节点换新配置重启。
#[tokio::test]
async fn restart_after_commit_requires_new_config() {
    let (c, report) = setup().await;
    apply_all(&c, &report).await;
    verify_all(&c, &report).await;
    c.stores[0].shrink_commit(&report).await.unwrap();
    let dirs = c.dirs[0].clone();
    // 旧配置（positional）启动：v4 记录 = 明确拒绝，并提示 commit 后不能再 rollback。
    let old = ClusterSets::single(
        "n1",
        vec![n("n1", 3), n("n2", 3), n("n3", 3), n("n4", 3)],
        &ec84(),
    )
    .unwrap();
    let e = store_err(open_with(dirs.clone(), "n1", old, ec84()).await);
    assert!(
        e.contains("v4") && (e.contains("配置回退") || e.contains("显式槽表")),
        "{e}"
    );
    // 新配置：v4 exact-match（epoch/hash/set/node/slot）→ 成功；注册进原 mesh = 该节点已重启。
    let n1 = Arc::new(
        open_on(c.mesh.clone(), dirs, "n1", target(2), ec84())
            .await
            .unwrap(),
    );
    // open_on 的 weak 指向 try_unwrap 后释放的 Arc；重注册让 mesh 能拿到重启用新配置的 n1。
    c.mesh.register("n1", &n1);
    assert_eq!(n1.local_caps().topo_epoch, 2);
    assert_eq!(n1.local_caps().topo_hash, report.plan.new_hash);
    // 其余节点仍旧配置：commit/rollback 都必须拒绝（不能「一半新配置一半撤销」）。
    let e = err_of(c.stores[1].shrink_commit(&report)).await;
    assert!(e.contains("先在所有节点 commit") && e.contains("n1"), "{e}");
    let e = err_of(c.stores[1].shrink_rollback(&report)).await;
    assert!(e.contains("commit 前") && e.contains("n1"), "{e}");
}

/// 1 盘目录 + 预置 topo 记录（记录格式测试用）。
async fn record_dir(rec: &str) -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(crate::layout::sys_dir(d.path(), ""))
        .await
        .unwrap();
    tokio::fs::write(crate::layout::sys_dir(d.path(), "topo"), rec)
        .await
        .unwrap();
    d
}

async fn open_record(rec: &str) -> String {
    let d = record_dir(rec).await;
    store_err(open_with(vec![d.path().to_path_buf()], "n1", one_node(2), ec84()).await)
}

/// v4 启动校验：epoch/hash/node/slot 任一不符 → 拒启，错误要能指向 commit/配置。
#[tokio::test]
async fn v4_startup_exact_match_rejects_each_field_mismatch() {
    let good_hash = one_node(2).topology_hash();
    let cases = [
        format!("v4 set=0 node=n1 slot=0 epoch=3 hash={good_hash}"),
        "v4 set=0 node=n1 slot=0 epoch=2 hash=0123456789abcdef".to_string(),
        format!("v4 set=0 node=n2 slot=0 epoch=2 hash={good_hash}"),
        format!("v4 set=0 node=n1 slot=1 epoch=2 hash={good_hash}"),
    ];
    for (i, rec) in cases.iter().enumerate() {
        let e = open_record(rec).await;
        assert!(
            e.contains("v4 与配置不符") || e.contains("无法识别"),
            "case {i}: {e}"
        );
    }
    // 未知更高版本 v5 → fail-closed。
    let e = open_record("v5 set=0 node=n1 slot=0 epoch=2 hash=0123456789abcdef").await;
    assert!(e.contains("无法识别"), "{e}");
    // 本机 0 号盘 = slot 0；上面 4 个 case 已覆盖 epoch/hash/node/slot。
}

#[tokio::test]
async fn legacy_v2_v3_records_upgrade_to_v4_on_explicit_start() {
    // v2：显式单 set 的 0 号盘，旧记录三元组一致 → 首启升级 v4。
    let dir = record_dir("v2 node=n1 slot=0").await;
    let s = open_with(vec![dir.path().to_path_buf()], "n1", one_node(2), ec84())
        .await
        .unwrap();
    let rec = std::fs::read_to_string(crate::layout::sys_dir(dir.path(), "topo")).unwrap();
    let h = s.cluster().unwrap().sets.topology_hash();
    assert_eq!(
        rec.trim(),
        format!("v4 set=0 node=n1 slot=0 epoch=2 hash={h}")
    );
    drop(s);

    // v3（set>0）：本机只在 set 1，0 号盘写 `v3 set=1 node=n1 slot=0` → 升级 v4 set=1。
    let ec = EcConfig {
        data: 2,
        parity: 1,
        enabled: true,
    };
    let t0 = tempfile::tempdir().unwrap();
    let t1 = tempfile::tempdir().unwrap();
    let t2 = tempfile::tempdir().unwrap();
    let dirs: Vec<PathBuf> = vec![
        t0.path().to_path_buf(),
        t1.path().to_path_buf(),
        t2.path().to_path_buf(),
    ];
    tokio::fs::create_dir_all(crate::layout::sys_dir(&dirs[0], ""))
        .await
        .unwrap();
    tokio::fs::write(
        crate::layout::sys_dir(&dirs[0], "topo"),
        "v3 set=1 node=n1 slot=0",
    )
    .await
    .unwrap();
    let m = |node: &str, epoch: u64| SlotMap {
        epoch,
        slots: (0..3)
            .map(|_| SlotTarget {
                node: node.into(),
                drive: 0,
            })
            .collect(),
    };
    let sets = ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n2", 3)], vec![n("n1", 3)]],
        vec![Some(m("n2", 2)), Some(m("n1", 2))],
        &ec,
    )
    .unwrap();
    let s = open_with(dirs.clone(), "n1", sets, ec).await.unwrap();
    let rec = std::fs::read_to_string(crate::layout::sys_dir(&dirs[0], "topo")).unwrap();
    assert!(
        rec.starts_with("v4 set=1 node=n1 slot=0 epoch=2 hash="),
        "{rec}"
    );
    assert_eq!(
        s.cluster().unwrap().sets.topology_hash(),
        rec.trim().rsplit("hash=").next().unwrap()
    );
}
