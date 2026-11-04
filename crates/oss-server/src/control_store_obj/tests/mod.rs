//! B1 tests: basic transaction/commit, crash matrix, conflict retry.

mod basic;
mod conflict;
mod crash;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tempfile::TempDir;
use xl_store::control_state::fault_io::{FaultIo, FaultRootCas};
use xl_store::control_state::hash::ROOT_KEY;
use xl_store::control_state::io::NodeIo;
use xl_store::control_state::root::Mode;
use xl_store::error::{Error, Result};
use xl_store::store::{Store, StoreConfig, SystemObject};

use super::root_io::LocalControlIo;
use super::{ObjectControlStore, RootIo};

pub(super) struct Real {
    pub(super) _dir: TempDir,
    pub(super) store: Arc<Store>,
    pub(super) control: Arc<ObjectControlStore>,
}

/// Single-drive, non-fsync store with a bootstrapped root.
pub(super) async fn real_store() -> Real {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(
        &store,
    )))));
    control.bootstrap(Mode::Standalone, 100).await.unwrap();
    Real {
        _dir: dir,
        store,
        control,
    }
}

/// In-memory A4 harness: nodes in [`FaultIo`], the root in [`FaultRootCas`].
pub(super) struct FaultBackend {
    pub(super) nodes: Arc<FaultIo>,
    pub(super) roots: Arc<FaultRootCas>,
    pub(super) indeterminate_without_store: AtomicBool,
    corrupt_next_root_put: AtomicBool,
    root_fence: Mutex<u64>,
    root_override: Mutex<Option<Vec<u8>>>,
    versions: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl FaultBackend {
    fn new() -> Self {
        Self {
            nodes: Arc::new(FaultIo::new()),
            roots: Arc::new(FaultRootCas::new()),
            indeterminate_without_store: AtomicBool::new(false),
            corrupt_next_root_put: AtomicBool::new(false),
            root_fence: Mutex::new(0),
            root_override: Mutex::new(None),
            versions: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn corrupt_next_root(&self) {
        self.corrupt_next_root_put.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl LocalControlIo for FaultBackend {
    async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>> {
        if key == ROOT_KEY {
            let version = self.roots.load().map(|slot| slot.version).unwrap_or(0);
            let fence = *self.root_fence.lock().unwrap();
            if let Some(bytes) = self.root_override.lock().unwrap().clone() {
                return Ok(Some(SystemObject {
                    key: key.to_owned(),
                    data: bytes,
                    version,
                    fence_token: fence,
                    deleted: false,
                }));
            }
            return Ok(self.roots.load().map(|slot| SystemObject {
                key: key.to_owned(),
                data: slot.bytes,
                version: slot.version,
                fence_token: fence,
                deleted: false,
            }));
        }
        let Some(data) = NodeIo::get(&*self.nodes, key).await? else {
            return Ok(None);
        };
        let (version, fence) = self
            .versions
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or((0, 0));
        Ok(Some(SystemObject {
            key: key.to_owned(),
            data,
            version,
            fence_token: fence,
            deleted: false,
        }))
    }

    async fn put_local_control(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<u64>,
        fence: u64,
    ) -> Result<u64> {
        if key == ROOT_KEY {
            if self.indeterminate_without_store.load(Ordering::SeqCst) {
                return Err(Error::Indeterminate("injected root CAS".into()));
            }
            if fence <= *self.root_fence.lock().unwrap() {
                return Err(Error::Conflict("stale root fence".into()));
            }
            let result = self.roots.cas(expected.unwrap_or(0), data);
            if result.is_ok() || matches!(&result, Err(Error::Indeterminate(_))) {
                *self.root_fence.lock().unwrap() = fence;
            }
            if self.corrupt_next_root_put.swap(false, Ordering::SeqCst) {
                *self.root_override.lock().unwrap() = Some(b"corrupt-root".to_vec());
            }
            return result;
        }
        {
            let versions = self.versions.lock().unwrap();
            let (version, cur_fence) = versions.get(key).copied().unwrap_or((0, 0));
            if expected.unwrap_or(0) != version {
                return Err(Error::Conflict("stale object version".into()));
            }
            if fence <= cur_fence {
                return Err(Error::Conflict("stale object fence".into()));
            }
        }
        NodeIo::put_create(&*self.nodes, key, data).await?;
        let mut versions = self.versions.lock().unwrap();
        let version = versions.get(key).map(|(v, _)| *v).unwrap_or(0) + 1;
        versions.insert(key.to_owned(), (version, fence));
        Ok(version)
    }
}

pub(super) async fn fault_store() -> (Arc<FaultBackend>, Arc<ObjectControlStore>) {
    let backend = Arc::new(FaultBackend::new());
    let control = reopen_fault(&backend);
    control.bootstrap(Mode::Standalone, 100).await.unwrap();
    (backend, control)
}

/// Unsized handle so independent root handles can share one fault backend.
pub(super) fn backend_io(backend: &Arc<FaultBackend>) -> Arc<dyn LocalControlIo> {
    backend.clone()
}

pub(super) fn reopen_fault(backend: &Arc<FaultBackend>) -> Arc<ObjectControlStore> {
    Arc::new(ObjectControlStore::new(Arc::new(RootIo::with_backend(
        backend_io(backend),
    ))))
}

pub(super) fn local() -> super::WriteOrigin {
    super::WriteOrigin::local("test")
}
