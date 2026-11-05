//! Root CAS and content-addressed object IO over the node-local control API
//! (`Store::{get_local_control, put_local_control}`, A5).
//!
//! Nodes and blob chunks are create-once: an existing key with identical bytes
//! is success and a different payload is `CorruptMeta` (design §1.1). The root
//! is the only CAS object; its read-back is verified before a commit is
//! reported, and an `Indeterminate` CAS is resolved by re-reading the generation.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use xl_store::control_state::blob::{self, BlobRef};
use xl_store::control_state::hash;
use xl_store::control_state::io::NodeIo;
use xl_store::control_state::root::{Mode, Root, decode_root, encode_root};
use xl_store::error::{Error, Result};
use xl_store::store::{Store, SystemObject};

/// A pinned committed root generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedRoot {
    /// Local-control version of `control/state/root`; also the generation number.
    pub version: u64,
    pub fence_token: u64,
    /// Exact framed bytes, used to recognize our own CAS after `Indeterminate`.
    pub bytes: Vec<u8>,
    pub root: Root,
}

#[async_trait]
pub(crate) trait LocalControlIo: Send + Sync {
    async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>>;
    async fn put_local_control(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<u64>,
        fence: u64,
    ) -> Result<u64>;
}

#[async_trait]
impl LocalControlIo for Store {
    async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>> {
        Store::get_local_control(self, key).await
    }

    async fn put_local_control(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<u64>,
        fence: u64,
    ) -> Result<u64> {
        Store::put_local_control(self, key, data, expected, fence).await
    }
}

pub(crate) enum CasOutcome {
    Committed(u64),
    /// `Indeterminate`, but the re-read proved the old generation is still current.
    NotCommitted,
}

/// Test-only crash point: fail the root put without writing it.
#[cfg(test)]
pub(crate) const CRASH_BEFORE_ROOT_CAS: u8 = 1;
/// Test-only crash point: write the root, then report a failure.
#[cfg(test)]
pub(crate) const CRASH_AFTER_ROOT_CAS: u8 = 2;

/// Delegating backend that injects a failure at the root CAS boundary (F3).
#[cfg(test)]
pub(crate) struct CrashIo {
    inner: std::sync::Arc<dyn LocalControlIo>,
    point: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

#[cfg(test)]
impl CrashIo {
    pub(crate) fn root_io(
        store: std::sync::Arc<Store>,
    ) -> (RootIo, std::sync::Arc<std::sync::atomic::AtomicU8>) {
        let point = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
        let io = Self {
            inner: store,
            point: std::sync::Arc::clone(&point),
        };
        (RootIo::with_backend(std::sync::Arc::new(io)), point)
    }
}

#[cfg(test)]
#[async_trait]
impl LocalControlIo for CrashIo {
    async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>> {
        self.inner.get_local_control(key).await
    }

    async fn put_local_control(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<u64>,
        fence: u64,
    ) -> Result<u64> {
        if key == hash::ROOT_KEY {
            match self.point.load(Ordering::SeqCst) {
                CRASH_BEFORE_ROOT_CAS => {
                    return Err(Error::CorruptMeta("injected crash before root CAS".into()));
                }
                CRASH_AFTER_ROOT_CAS => {
                    self.inner
                        .put_local_control(key, data, expected, fence)
                        .await?;
                    return Err(Error::CorruptMeta("injected crash after root CAS".into()));
                }
                _ => {}
            }
        }
        self.inner
            .put_local_control(key, data, expected, fence)
            .await
    }
}

/// Root CAS plus content-addressed node/blob IO.
pub struct RootIo {
    backend: std::sync::Arc<dyn LocalControlIo>,
    /// Monotonic fence allocator, raised by observed root tokens on load.
    fence: AtomicU64,
}

/// Create-once retries before a contended content object fails closed.
const CREATE_ATTEMPTS: usize = 8;

impl RootIo {
    pub fn new(store: std::sync::Arc<Store>) -> Self {
        Self {
            backend: store,
            fence: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_backend(backend: std::sync::Arc<dyn LocalControlIo>) -> Self {
        Self {
            backend,
            fence: AtomicU64::new(0),
        }
    }

    /// Load and validate the committed root, if any. Fails closed when the root
    /// (or any table it references) needs a newer reader minor.
    pub async fn load(&self) -> Result<Option<PinnedRoot>> {
        let Some(obj) = self.backend.get_local_control(hash::ROOT_KEY).await? else {
            return Ok(None);
        };
        let root = decode_root(&obj.data)?;
        root.check_required_minor()?;
        self.fence.fetch_max(obj.fence_token, Ordering::SeqCst);
        Ok(Some(PinnedRoot {
            version: obj.version,
            fence_token: obj.fence_token,
            bytes: obj.data,
            root,
        }))
    }

    /// Create the empty root if the store has none.
    pub async fn bootstrap(&self, mode: Mode, created_at: i64) -> Result<PinnedRoot> {
        if let Some(pinned) = self.load().await? {
            return Ok(pinned);
        }
        let bytes = encode_root(&Root::empty(mode, created_at))?;
        match self.cas_root(0, self.alloc_fence(1), &bytes).await {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(e) => return Err(e),
        }
        self.load()
            .await?
            .ok_or_else(|| Error::CorruptMeta("control root missing after bootstrap".into()))
    }

    /// Strictly increasing fence token, never below `floor`.
    pub(crate) fn alloc_fence(&self, floor: u64) -> u64 {
        self.fence
            .fetch_max(floor.saturating_sub(1), Ordering::SeqCst);
        self.fence.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// CAS the root at `expected_version`. A successful write is verified by
    /// re-reading; an `Indeterminate` write is resolved by comparing the re-read
    /// generation with the bytes we tried to commit.
    pub(crate) async fn cas_root(
        &self,
        expected_version: u64,
        fence: u64,
        bytes: &[u8],
    ) -> Result<CasOutcome> {
        match self
            .backend
            .put_local_control(hash::ROOT_KEY, bytes, Some(expected_version), fence)
            .await
        {
            Ok(version) => match self.load().await? {
                Some(pinned) if pinned.version == version && pinned.bytes == bytes => {
                    Ok(CasOutcome::Committed(version))
                }
                other => Err(Error::CorruptMeta(format!(
                    "control root read-back mismatch after CAS at version {version}: {:?}",
                    other.map(|p| p.version)
                ))),
            },
            Err(Error::Indeterminate(_)) => match self.load().await {
                Ok(Some(pinned)) if pinned.bytes == bytes && pinned.version > expected_version => {
                    Ok(CasOutcome::Committed(pinned.version))
                }
                Ok(Some(pinned)) if pinned.version == expected_version => {
                    Ok(CasOutcome::NotCommitted)
                }
                Ok(Some(pinned)) => Err(Error::Conflict(format!(
                    "control root moved to version {} during indeterminate CAS",
                    pinned.version
                ))),
                Ok(None) => Err(Error::Indeterminate(
                    "control root missing after indeterminate CAS".into(),
                )),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// Write a large value as content-addressed chunks; returns its reference.
    pub(crate) async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef> {
        let (blob, chunks) = blob::split(bytes)?;
        for (n, chunk) in chunks.iter().enumerate() {
            let key = hash::blob_key(&blob.sha256, n as u32);
            self.create_once(&key, &blob::encode_chunk(chunk)?).await?;
        }
        Ok(blob)
    }

    /// Reassemble and verify a blob; a missing or corrupt chunk fails closed.
    pub(crate) async fn get_blob(&self, blob: &BlobRef) -> Result<Vec<u8>> {
        let mut chunks = Vec::with_capacity(blob.chunks as usize);
        for n in 0..blob.chunks {
            let key = hash::blob_key(&blob.sha256, n);
            let obj = self
                .backend
                .get_local_control(&key)
                .await?
                .ok_or_else(|| Error::CorruptMeta(format!("missing blob chunk {key}")))?;
            chunks.push(blob::decode_chunk(&obj.data)?);
        }
        blob::join(blob, &chunks)
    }

    /// Create a content-addressed object. `expected: None` means version 0, so
    /// a successful blind put proves the key was absent and needs no read-back.
    /// Only a conflict (already present) or an indeterminate write reads it to
    /// check that the existing bytes match.
    async fn create_once(&self, key: &str, bytes: &[u8]) -> Result<()> {
        for _ in 0..CREATE_ATTEMPTS {
            match self
                .backend
                .put_local_control(key, bytes, None, self.alloc_fence(1))
                .await
            {
                Ok(_) => return Ok(()),
                Err(Error::Conflict(_) | Error::Indeterminate(_)) => {
                    if let Some(existing) = self.backend.get_local_control(key).await? {
                        return if existing.data == bytes {
                            Ok(())
                        } else {
                            Err(Error::CorruptMeta(format!(
                                "duplicate create of {key} with different bytes"
                            )))
                        };
                    }
                    // `Indeterminate` with no visible object: retry the create.
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Conflict(format!(
            "create-once {key} contended after {CREATE_ATTEMPTS} attempts"
        )))
    }
}

#[async_trait]
impl NodeIo for RootIo {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .backend
            .get_local_control(key)
            .await?
            .map(|obj| obj.data))
    }

    async fn put_create(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.create_once(key, bytes).await
    }
}
