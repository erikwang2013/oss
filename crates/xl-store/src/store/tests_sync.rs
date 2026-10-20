//! M9 S1：sync 写提交门（staging → gate → 本地 meta 翻转）的 store 侧契约。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::*;
use crate::gate::{GateError, GateRequest, GateTicket, PutCommitGate};
use crate::meta;

#[derive(Debug)]
struct Seen {
    key: String,
    size: u64,
    etag: String,
    tee_exists: bool,
    tee_len: u64,
    meta_visible: bool,
}

#[derive(Debug)]
struct MockGate {
    root: PathBuf,
    sync: bool,
    fail: AtomicBool,
    calls: AtomicUsize,
    aborts: AtomicUsize,
    seen: Mutex<Vec<Seen>>,
}

impl MockGate {
    fn new(root: &Path, sync: bool) -> Arc<Self> {
        Arc::new(Self {
            root: root.to_path_buf(),
            sync,
            fail: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl PutCommitGate for MockGate {
    async fn is_sync(&self, _bucket: &str) -> std::result::Result<bool, GateError> {
        Ok(self.sync)
    }

    async fn before_commit(
        &self,
        req: GateRequest<'_>,
    ) -> std::result::Result<GateTicket, GateError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let tee = tokio::fs::metadata(req.plaintext).await.ok();
        // gate 时刻本地 meta 必须还不存在（gate 在提交点之前）
        let meta_visible = layout::object_dir(&self.root, req.bucket, req.key)
            .map(|d| d.join(meta::META_FILE).exists())
            .unwrap_or(true);
        self.seen.lock().unwrap().push(Seen {
            key: req.key.to_string(),
            size: req.size,
            etag: req.etag.to_string(),
            tee_exists: tee.is_some(),
            tee_len: tee.map(|m| m.len()).unwrap_or(0),
            meta_visible,
        });
        if self.fail.load(Ordering::SeqCst) {
            return Err(GateError::Unavailable("remote down".into()));
        }
        Ok(GateTicket {
            remote_version_id: Some(req.local_version_id.to_string()),
            bucket: req.bucket.to_string(),
            key: req.key.to_string(),
            local_version_id: req.local_version_id.to_string(),
        })
    }

    async fn abort(&self, _ticket: &GateTicket) {
        self.aborts.fetch_add(1, Ordering::SeqCst);
    }
}

async fn put(s: &Store, key: &str, body: Vec<u8>, ext: PutExt) -> Result<ObjectInfo> {
    s.put_object_stream_with_meta_ext(
        "alpha",
        key,
        Box::pin(futures::stream::once(async move { Ok(Bytes::from(body)) })),
        Some("text/plain".into()),
        [("k".to_string(), "v".to_string())].into_iter().collect(),
        None,
        true, // sync 桶要求版本控制
        None,
        None,
        None,
        None,
        ext,
    )
    .await
}

fn tee_entries(root: &Path) -> usize {
    match std::fs::read_dir(layout::sys_dir(root, "tee")) {
        Ok(it) => it.count(),
        Err(_) => 0,
    }
}

#[tokio::test]
async fn gate_runs_after_staging_before_meta() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let info = put(&s, "k.txt", b"hello".to_vec(), PutExt::default())
        .await
        .unwrap();

    {
        let seen = gate.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].key, "k.txt");
        assert_eq!(seen[0].size, 5);
        assert_eq!(seen[0].etag, info.etag);
        assert!(seen[0].tee_exists, "gate 时必须已 tee 出明文");
        assert_eq!(seen[0].tee_len, 5);
        assert!(!seen[0].meta_visible, "gate 必须先于本地 meta 翻转");
    }

    assert_eq!(info.replication_status.as_deref(), Some("COMPLETED"));
    assert_eq!(info.remote_version_id, info.version_id);
    let vid = info.version_id.clone().unwrap();
    assert!(
        !layout::sys_dir(dir.path(), "tee")
            .join(format!("{vid}.plain"))
            .exists(),
        "成功后 tee 必须清理"
    );
    let (_, bytes) = s.get_object("alpha", "k.txt").await.unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[tokio::test]
async fn gate_failure_leaves_no_readable_object() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    gate.fail.store(true, Ordering::SeqCst);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let err = put(&s, "k.txt", b"hello".to_vec(), PutExt::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unavailable(_)), "{err:?}");
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        s.head_object("alpha", "k.txt").await,
        Err(Error::NoSuchKey(_))
    ));
    let obj_dir = layout::object_dir(dir.path(), "alpha", "k.txt").unwrap();
    assert!(!obj_dir.join(meta::META_FILE).exists());
    assert!(!obj_dir.join("xl.meta.new").exists());
    assert_eq!(tee_entries(dir.path()), 0, "失败后 tee 必须清理");
}

#[tokio::test]
async fn async_path_unchanged_without_gate() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let info = put(&s, "k.txt", b"hello".to_vec(), PutExt::default())
        .await
        .unwrap();
    assert_eq!(info.replication_status, None);
    assert_eq!(info.remote_version_id, None);
    assert_eq!(tee_entries(dir.path()), 0, "非 sync 写不得产生 tee");
}

#[tokio::test]
async fn replica_skips_gate_and_adopts_origin_id() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let origin = uuid::Uuid::new_v4();
    let info = put(
        &s,
        "k.txt",
        b"hello".to_vec(),
        PutExt {
            replica: true,
            forced_version_id: Some(origin.to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        gate.calls.load(Ordering::SeqCst),
        0,
        "REPLICA 摄入不得再 gate"
    );
    assert_eq!(
        info.version_id.as_deref(),
        Some(origin.to_string().as_str())
    );
    assert_eq!(
        info.remote_version_id.as_deref(),
        Some(origin.to_string().as_str())
    );
    assert_eq!(info.replication_status, None);
    let (_, bytes) = s.get_object("alpha", "k.txt").await.unwrap();
    assert_eq!(&bytes[..], b"hello");
    assert_eq!(tee_entries(dir.path()), 0, "replica 不 tee");
}

#[tokio::test]
async fn forced_id_idempotent_and_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let origin = uuid::Uuid::new_v4();
    let ext = PutExt {
        replica: true,
        forced_version_id: Some(origin.to_string()),
    };

    let first = put(&s, "k.txt", b"hello".to_vec(), ext.clone())
        .await
        .unwrap();
    let again = put(&s, "k.txt", b"hello".to_vec(), ext.clone())
        .await
        .unwrap();
    assert_eq!(again.version_id, first.version_id);
    assert_eq!(again.etag, first.etag);
    assert!(matches!(
        put(&s, "k.txt", b"other".to_vec(), ext).await,
        Err(Error::InvalidArgument(_))
    ));
    // 冲突不毁已有对象
    let (_, bytes) = s.get_object("alpha", "k.txt").await.unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[tokio::test]
async fn sync_max_bytes_rejected_before_gate() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    cfg.sync_max_bytes = 4;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let err = put(&s, "k.txt", b"hello".to_vec(), PutExt::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0, "超限在 gate 前拒绝");
    assert!(matches!(
        s.head_object("alpha", "k.txt").await,
        Err(Error::NoSuchKey(_))
    ));
    assert_eq!(tee_entries(dir.path()), 0);
}

/// M3：重放返回被重放的那个版本（不是链上 latest），否则发起端 id 校验会误判。
#[tokio::test]
async fn replay_returns_matched_version_not_latest() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let v1 = put(&s, "k.txt", b"one".to_vec(), PutExt::default())
        .await
        .unwrap();
    let v2 = put(&s, "k.txt", b"two".to_vec(), PutExt::default())
        .await
        .unwrap();
    assert_ne!(v1.version_id, v2.version_id);

    let replay = put(
        &s,
        "k.txt",
        b"one".to_vec(),
        PutExt {
            replica: true,
            forced_version_id: v1.version_id.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        replay.version_id, v1.version_id,
        "重放必须返回被重放版本，而不是 latest"
    );
    assert_eq!(replay.etag, v1.etag);
    let head = s.head_object("alpha", "k.txt").await.unwrap();
    assert_eq!(head.version_id, v2.version_id, "latest 不得被重放改写");
}

/// M5：远端确认后本地提交失败 → abort 补偿（计划要求的失败路径）。
#[tokio::test]
async fn local_commit_failure_aborts_remote_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let gate = MockGate::new(dir.path(), true);
    let mut cfg = StoreConfig::new(dir.path());
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    // 提交点必然失败：xl.meta.new 位置先占成目录（File::create → EISDIR）。
    let obj_dir = layout::object_dir(dir.path(), "alpha", "k.txt").unwrap();
    std::fs::create_dir_all(obj_dir.join(meta::META_NEW_FILE)).unwrap();

    let err = put(&s, "k.txt", b"hello".to_vec(), PutExt::default())
        .await
        .unwrap_err();
    assert!(
        !matches!(err, Error::Unavailable(_)),
        "本地提交失败: {err:?}"
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        gate.aborts.load(Ordering::SeqCst),
        1,
        "本地提交失败必须补偿远端"
    );
    assert!(matches!(
        s.head_object("alpha", "k.txt").await,
        Err(Error::NoSuchKey(_))
    ));
    assert_eq!(tee_entries(dir.path()), 0, "失败后 tee 必须清理");
}

/// M5：多盘/EC 直写路径也必须 tee 明文并先过门（覆盖 stage_payload_multi_tee）。
#[tokio::test]
async fn multi_drive_sync_tees_plaintext_before_gate() {
    let dirs: Vec<_> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    let gate = MockGate::new(dirs[0].path(), true);
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let data = vec![7u8; 300_000];
    let info = put(&s, "ec.bin", data.clone(), PutExt::default())
        .await
        .unwrap();
    {
        let seen = gate.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].size, data.len() as u64);
        assert!(seen[0].tee_exists, "EC 路径 gate 时必须已 tee 出明文");
        assert_eq!(seen[0].tee_len, data.len() as u64);
        assert!(!seen[0].meta_visible, "gate 必须先于本地 meta 翻转");
    }
    assert_eq!(info.replication_status.as_deref(), Some("COMPLETED"));
    let (_, bytes) = s.get_object("alpha", "ec.bin").await.unwrap();
    assert_eq!(bytes, data);
    assert_eq!(tee_entries(dirs[0].path()), 0);
}

/// 多盘路径的数据直写版本目录：重投不得覆盖/回滚已提交版本。
#[tokio::test]
async fn multi_drive_replay_keeps_committed_version() {
    let dirs: Vec<_> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    let gate = MockGate::new(dirs[0].path(), true);
    cfg.gate = Some(gate.clone());
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let origin = uuid::Uuid::new_v4().to_string();
    let ext = || PutExt {
        replica: true,
        forced_version_id: Some(origin.clone()),
    };
    let data = vec![9u8; 300_000];
    let first = put(&s, "k.txt", data.clone(), ext()).await.unwrap();
    assert_eq!(first.version_id.as_deref(), Some(origin.as_str()));

    // 同体同 id 重放：幂等、不再远端、已提交版本仍可完整读回。
    let again = put(&s, "k.txt", data.clone(), ext()).await.unwrap();
    assert_eq!(again.version_id, first.version_id);
    let (_, bytes) = s.get_object("alpha", "k.txt").await.unwrap();
    assert_eq!(bytes, data);

    // 异体同 id：400 且不毁旧数据。
    assert!(matches!(
        put(&s, "k.txt", b"different".to_vec(), ext()).await,
        Err(Error::InvalidArgument(_))
    ));
    let (_, bytes) = s.get_object("alpha", "k.txt").await.unwrap();
    assert_eq!(bytes, data);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0, "REPLICA 摄入不 gate");
}
