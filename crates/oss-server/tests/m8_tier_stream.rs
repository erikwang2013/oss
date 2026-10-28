//! M8 3.11：tier 流式搬运——大对象往返、增量摘要、恢复校验失败/空间不足回滚。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt as _;
use md5::Digest as _;
use xl_store::meta::TierPhase;
use xl_store::store::ec::EcConfig;
use xl_store::store::{Store, StoreConfig};

use oss_server::config::{TierKind, TierTarget};
use oss_server::jobs::TaskExecutor as _;
use oss_server::state::AppState;
use oss_server::tier::{
    S3TierGc, TierBackend, TierBackends, TierByteStream, TierPutResult, tier_key,
};
use oss_server::tier_jobs::M8TaskExecutor;

/// 略大于 100 MiB，尾块非整对齐。
const LARGE: u64 = 100 * 1024 * 1024 + 3;
const GEN_CHUNK: u64 = 1 << 20;

#[derive(Default)]
struct MockTier {
    objs: Mutex<HashMap<String, Bytes>>,
}

#[async_trait::async_trait]
impl TierBackend for MockTier {
    fn id(&self) -> &str {
        "cold"
    }

    async fn put(
        &self,
        key: &str,
        mut body: TierByteStream,
        size: u64,
    ) -> Result<TierPutResult, String> {
        let mut buf = Vec::with_capacity(size as usize);
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        if buf.len() as u64 != size {
            return Err(format!("mock 长度 {} ≠ {size}", buf.len()));
        }
        let md5 = hex::encode(md5::Md5::digest(&buf));
        let sha256 = hex::encode(sha2::Sha256::digest(&buf));
        self.objs
            .lock()
            .unwrap()
            .insert(key.to_string(), Bytes::from(buf));
        Ok(TierPutResult {
            etag: format!("mock-{md5}"),
            md5,
            sha256,
        })
    }

    async fn get(&self, key: &str) -> Result<TierByteStream, String> {
        let b = self
            .objs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))?;
        Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        Ok(self.objs.lock().unwrap().get(key).map(|b| b.len() as u64))
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.objs.lock().unwrap().remove(key);
        Ok(())
    }
}

fn tier_target(id: &str, kind: TierKind, path: Option<std::path::PathBuf>, ep: &str) -> TierTarget {
    TierTarget {
        id: id.into(),
        kind,
        path,
        endpoint: ep.into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: None,
    }
}

async fn setup(targets: Vec<TierTarget>) -> (AppState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // 100MiB 用例在高负载下可跑 >600s（sqlx idle_timeout 默认 600s）：内存库连接被
    // 回收后库随之销毁，后续访问报 no such table。改用文件库（test.db）规避。
    let st = common::state_with_tiers_file_db(dir.path(), targets).await;
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    (st, dir)
}

fn executor(st: &AppState) -> M8TaskExecutor {
    M8TaskExecutor::new(
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.tiers),
        Arc::clone(&st.events),
    )
}

fn payload(key: &str, vid: &str, class: &str, tier: Option<&str>) -> String {
    serde_json::json!({
        "bucket": "bkt", "key": key, "version_id": vid, "class": class, "tier": tier
    })
    .to_string()
}

fn restore_payload(key: &str, vid: &str) -> String {
    serde_json::json!({
        "bucket": "bkt", "key": key, "version_id": vid, "days": 1
    })
    .to_string()
}

async fn archive(st: &AppState, key: &str, vid: &str) -> String {
    executor(st)
        .execute("Transition", &payload(key, vid, "GLACIER", Some("cold")))
        .await
        .unwrap()
}

async fn phase(st: &AppState, key: &str, vid: &str) -> TierPhase {
    st.store
        .tier_version("bkt", key, vid)
        .await
        .unwrap()
        .unwrap()
        .tier
        .unwrap()
        .phase
}

fn mock_setup(st: &mut AppState) -> Arc<MockTier> {
    let mock = Arc::new(MockTier::default());
    let backends = Arc::new(TierBackends::new(vec![
        Arc::clone(&mock) as Arc<dyn TierBackend>
    ]));
    st.tiers = Arc::clone(&backends);
    st.tier_gc = Arc::new(S3TierGc::new(backends));
    mock
}

/// 测试数据生成流：1 MiB 块、`(pos % 251)` 模式，不整份驻留内存；
/// 同时把独立计算的 MD5/SHA-256 写入 `digest`（源对象归档后不可再读，不能事后取 oracle）。
fn large_stream(digest: Arc<Mutex<Option<(String, String)>>>) -> xl_store::store::ByteStream {
    Box::pin(futures::stream::unfold(
        (0u64, md5::Md5::new(), sha2::Sha256::new()),
        move |(off, mut m, mut s)| {
            let digest = Arc::clone(&digest);
            async move {
                if off >= LARGE {
                    digest
                        .lock()
                        .unwrap()
                        .replace((hex::encode(m.finalize()), hex::encode(s.finalize())));
                    return None;
                }
                let len = GEN_CHUNK.min(LARGE - off) as usize;
                let v: Vec<u8> = (0..len).map(|i| ((off as usize + i) % 251) as u8).collect();
                m.update(&v);
                s.update(&v);
                Some((Ok(Bytes::from(v)), (off + len as u64, m, s)))
            }
        },
    ))
}

/// >100 MiB：Transition 按 8 MiB 窗口读取、后端流式增量摘要；Restore 流式落盘 +
/// SHA-256 校验。全程不出现整对象驻留（MockTier 自身是内存后端，属测试替身）。
#[tokio::test]
async fn large_transition_restore_roundtrip_over_100mib() {
    let (mut st, _d) = setup(vec![tier_target(
        "cold",
        TierKind::S3,
        None,
        "http://127.0.0.1:9",
    )])
    .await;
    let mock = mock_setup(&mut st);

    let digest = Arc::new(Mutex::new(None));
    let info = st
        .store
        .put_object_stream("bkt", "huge.bin", large_stream(Arc::clone(&digest)), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    assert_eq!(archive(&st, "huge.bin", &vid).await, "archived");
    let (md5, sha) = digest
        .lock()
        .unwrap()
        .clone()
        .expect("源流已完整消费，摘要应已就绪");

    let v = st
        .store
        .tier_version("bkt", "huge.bin", &vid)
        .await
        .unwrap()
        .unwrap();
    let t = v.tier.unwrap();
    assert_eq!(t.phase, TierPhase::Archived);
    assert_eq!(t.size, LARGE);

    // 生成流独立算出的摘要 vs 后端在搬运流上增量算出的摘要。
    assert_eq!(t.sha256, sha, "增量 SHA-256 必须覆盖全部窗口");
    assert_eq!(t.etag, format!("mock-{md5}"), "增量 MD5 必须覆盖全部窗口");
    assert_eq!(
        mock.objs.lock().unwrap()[&tier_key("cold", "bkt", &vid, "huge.bin")].len() as u64,
        LARGE
    );

    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore_payload("huge.bin", &vid))
            .await
            .unwrap(),
        "restored"
    );
    let (_, got) = st.store.get_object("bkt", "huge.bin").await.unwrap();
    assert_eq!(got.len() as u64, LARGE);
    assert_eq!(hex::encode(sha2::Sha256::digest(&got)), sha);
    assert_eq!(got[0], 0);
    assert_eq!(got[LARGE as usize - 1], ((LARGE - 1) % 251) as u8);
}

/// 恢复前空间检查失败 → fail closed，回 Archived，且不创建暂存文件。
#[tokio::test]
async fn restore_free_space_failure_leaves_archived_and_clean() {
    let (mut st, _d) = setup(vec![tier_target(
        "cold",
        TierKind::S3,
        None,
        "http://127.0.0.1:9",
    )])
    .await;
    let _mock = mock_setup(&mut st);
    let info = st
        .store
        .put_object("bkt", "nospace.bin", Bytes::from_static(b"payload"), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    assert_eq!(archive(&st, "nospace.bin", &vid).await, "archived");
    let part = st
        .store
        .tier_version("bkt", "nospace.bin", &vid)
        .await
        .unwrap()
        .unwrap()
        .part;
    let stage = st
        .store
        .restore_stage_path("bkt", "nospace.bin", &part.uuid)
        .unwrap();

    let err = executor(&st)
        .with_restore_free_reserve(u64::MAX)
        .execute("RestoreObject", &restore_payload("nospace.bin", &vid))
        .await
        .unwrap_err();
    assert!(err.contains("磁盘剩余空间不足"), "{err}");
    assert_eq!(phase(&st, "nospace.bin", &vid).await, TierPhase::Archived);
    assert!(!stage.exists(), "空间检查失败不得留下暂存文件");

    // 余量恢复正常后，同一任务可重试成功。
    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore_payload("nospace.bin", &vid))
            .await
            .unwrap(),
        "restored"
    );
}

/// 远端字节被篡改（长度合法、SHA-256 不符）→ 拒绝提交、回 Archived、清理暂存；
/// 修好远端后重试成功（重试从干净状态开始）。
#[tokio::test]
async fn restore_sha_mismatch_leaves_archived_clean_then_retry_succeeds() {
    let (mut st, _d) = setup(vec![tier_target(
        "cold",
        TierKind::S3,
        None,
        "http://127.0.0.1:9",
    )])
    .await;
    let mock = mock_setup(&mut st);
    let info = st
        .store
        .put_object(
            "bkt",
            "corrupt.bin",
            Bytes::from_static(b"trusted-bytes"),
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    assert_eq!(archive(&st, "corrupt.bin", &vid).await, "archived");
    let remote = tier_key("cold", "bkt", &vid, "corrupt.bin");
    let part = st
        .store
        .tier_version("bkt", "corrupt.bin", &vid)
        .await
        .unwrap()
        .unwrap()
        .part;
    let stage = st
        .store
        .restore_stage_path("bkt", "corrupt.bin", &part.uuid)
        .unwrap();
    mock.objs
        .lock()
        .unwrap()
        .insert(remote.clone(), Bytes::from_static(b"corrupt-bytes"));
    assert_eq!(b"corrupt-bytes".len(), b"trusted-bytes".len());

    let err = executor(&st)
        .execute("RestoreObject", &restore_payload("corrupt.bin", &vid))
        .await
        .unwrap_err();
    assert!(err.contains("SHA-256 不符"), "{err}");
    assert_eq!(phase(&st, "corrupt.bin", &vid).await, TierPhase::Archived);
    assert!(!stage.exists(), "校验失败必须删除暂存文件");

    mock.objs
        .lock()
        .unwrap()
        .insert(remote, Bytes::from_static(b"trusted-bytes"));
    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore_payload("corrupt.bin", &vid))
            .await
            .unwrap(),
        "restored"
    );
    let (_, got) = st.store.get_object("bkt", "corrupt.bin").await.unwrap();
    assert_eq!(&got[..], b"trusted-bytes");
}

fn walk_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for ent in rd.flatten() {
        let p = ent.path();
        if p.is_dir() {
            walk_files(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// 本地 tier 同样走流式 put/get：最终目录只有对象文件（无 `.tmp-` 残留），
/// 恢复后内容一致。
#[tokio::test]
async fn local_tier_transition_restore_streams() {
    let dir = tempfile::tempdir().unwrap();
    let tier_root = tempfile::tempdir().unwrap();
    let st = common::state_with_tiers(
        dir.path(),
        vec![tier_target(
            "local1",
            TierKind::Local,
            Some(tier_root.path().to_path_buf()),
            "http://127.0.0.1:9",
        )],
    )
    .await;
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();

    // 9 MiB+：跨多个 8 MiB 源窗口。
    let body = Bytes::from(vec![7u8; 9 * 1024 * 1024 + 5]);
    let info = st
        .store
        .put_object("bkt", "local.bin", body.clone(), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    assert_eq!(
        executor(&st)
            .execute(
                "Transition",
                &payload("local.bin", &vid, "GLACIER", Some("local1")),
            )
            .await
            .unwrap(),
        "archived"
    );
    let mut files = Vec::new();
    walk_files(tier_root.path(), &mut files);
    assert_eq!(files.len(), 1, "tier 根下应只有一个对象文件：{files:?}");
    assert!(
        !files[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains(".tmp-"),
        "不得残留临时文件：{:?}",
        files[0]
    );
    assert_eq!(
        std::fs::metadata(&files[0]).unwrap().len(),
        body.len() as u64
    );

    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore_payload("local.bin", &vid))
            .await
            .unwrap(),
        "restored"
    );
    let (_, got) = st.store.get_object("bkt", "local.bin").await.unwrap();
    assert_eq!(got, body);
}

/// 换成 `ec.enabled=false` 的 Store：对象以 `part.1` 原样落盘（M1 旧形态），
/// Range 读不做整对象摘要校验——正是 C2 要封的静默源损坏面。
async fn swap_in_ec_off_store(st: &mut AppState, dir: &std::path::Path) {
    let cfg = StoreConfig {
        ec: EcConfig {
            enabled: false,
            ..Default::default()
        },
        ..StoreConfig::new(dir.join("ecoff"))
    };
    let store = Store::open(cfg).await.unwrap();
    store.create_bucket("bkt").await.unwrap();
    st.store = Arc::new(store);
}

/// C2：非 EC 源 `part.1` 同长度静默损坏 → Transition 拒绝归档（修复前会把坏副本归档，
/// 之后直读/Restore 都自洽通过，坏摘要还会被 restore_commit「转正」）。
#[tokio::test]
async fn transition_rejects_corrupted_source_part_without_ec() {
    let (mut st, d) = setup(vec![tier_target(
        "cold",
        TierKind::S3,
        None,
        "http://127.0.0.1:9",
    )])
    .await;
    let _mock = mock_setup(&mut st);
    swap_in_ec_off_store(&mut st, d.path()).await;

    let data = vec![0x5Au8; 1024];
    let info = st
        .store
        .put_object("bkt", "bad.bin", Bytes::from(data.clone()), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    let v = st
        .store
        .tier_version("bkt", "bad.bin", &vid)
        .await
        .unwrap()
        .unwrap();
    assert!(v.parts.is_none(), "非 MPU 源应为单段形态");
    assert_eq!(v.part.sha256, hex::encode(sha2::Sha256::digest(&data)));

    let part = st
        .store
        .restore_stage_path("bkt", "bad.bin", &v.part.uuid)
        .unwrap()
        .parent()
        .unwrap()
        .join("part.1");
    let mut raw = std::fs::read(&part).unwrap();
    raw[10] ^= 0xff;
    std::fs::write(&part, &raw).unwrap();

    let err = executor(&st)
        .execute(
            "Transition",
            &payload("bad.bin", &vid, "GLACIER", Some("cold")),
        )
        .await
        .unwrap_err();
    assert!(err.contains("SHA-256"), "{err}");
    assert!(
        st.store
            .tier_version("bkt", "bad.bin", &vid)
            .await
            .unwrap()
            .unwrap()
            .tier
            .is_none(),
        "坏副本不得归档"
    );
}

/// C2 legacy 例外：`parts=Some` 或空 `part.sha256` 时跳过摘要绑定，不误伤旧对象。
#[tokio::test]
async fn transition_skips_digest_binding_for_legacy_source() {
    let (mut st, d) = setup(vec![tier_target(
        "cold",
        TierKind::S3,
        None,
        "http://127.0.0.1:9",
    )])
    .await;
    let _mock = mock_setup(&mut st);

    // 旧 M2a 形态：parts=Some（part.sha256 与归档流不符也不得拒绝）。
    let info = st
        .store
        .put_object(
            "bkt",
            "legacy-parts.bin",
            Bytes::from_static(b"legacy"),
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    common::rewrite_meta(&d.path().join("data"), "bkt", "legacy-parts.bin", |m| {
        let v = m.versions.last_mut().unwrap();
        let first = v.part.clone();
        v.parts = Some(vec![first]);
        v.part.sha256 = "00".repeat(32);
    });
    assert_eq!(archive(&st, "legacy-parts.bin", &vid).await, "archived");

    // 旧对象无摘要：part.sha256 为空 → 同样跳过。
    let info = st
        .store
        .put_object("bkt", "legacy-sha.bin", Bytes::from_static(b"legacy"), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    common::rewrite_meta(&d.path().join("data"), "bkt", "legacy-sha.bin", |m| {
        m.versions.last_mut().unwrap().part.sha256.clear();
    });
    assert_eq!(archive(&st, "legacy-sha.bin", &vid).await, "archived");
}
