//! B4：控制面后端选择与对象后端启动装配。
//!
//! `[server].control_backend = "sqlite" | "objects"`（默认 sqlite）。对象后端在
//! 启动时：
//!
//! 1. 对节点状态目录取独占 `flock`（第二个进程必须启动失败），守卫随本结构
//!    存活到进程结束；
//! 2. 从 `<data root>/.oss.sys` 的控制根对象加载/引导 `RootIo`，并把节点本地
//!    运行态（D4：outbox/audit/uploads/…）挂在 `<data root>/.oss.sys/local`；
//! 3. 断言控制根 `mode` 与配置一致（standalone/site 都是 `Mode::Standalone`，
//!    `[raft]` 是 `Mode::Raft`）；不一致 fail-closed，绝不把 SQLite 库当成另一种
//!    形态继续服务；
//! 4. 校验根格式（未知表/需要更高 reader minor 时拒绝），并种子 `site_config`
//!    身份行（与 SQLite `site_bootstrap` 同语义；已有行不覆盖）。
//!
//! 对象后端的事务写入统一走 [`ObjectControlStore::transaction`]：冲突时整段闭包
//! 重跑。D3 的本地域（local_store）写入是立即生效的，调用方不得把它们放进会重跑
//! 的闭包里（见 `control_store_obj::local_ops` 的模块注释）。

use std::sync::Arc;

use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::store::{LocalControlRootGuard, Store};

use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::control_view::{CacheDelta, ViewCache};

/// 控制根锁 + 对象控制面句柄。`_root_lock` 必须在进程生命周期内持有。
pub struct ObjectControl {
    pub control: Arc<ObjectControlStore>,
    /// E1：请求热视图缓存；提交经 publisher 增长，快照安装走 [`Self::rebuild_view`]。
    pub view: Arc<ViewCache>,
    _root_lock: LocalControlRootGuard,
}

/// 配置形态 → 根 `mode`。站点复制是 standalone 之上的功能，不改变 `Mode`。
pub fn expected_mode(raft: bool) -> Mode {
    if raft { Mode::Raft } else { Mode::Standalone }
}

/// 节点本地运行态目录（D4）：`<data root>/.oss.sys/local`。
pub fn local_state_dir(root: &std::path::Path) -> std::path::PathBuf {
    xl_store::layout::sys_dir(root, "local")
}

impl ObjectControl {
    /// 打开/引导对象控制根并持有启动锁。`store` 必须是本节点的存储内核句柄
    /// （控制对象走 `Store::{get,put}_local_control`）。
    pub async fn open(
        store: Arc<Store>,
        expected: Mode,
        site_id: Option<&str>,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        // 第二个服务进程必须在这里失败；守卫 drop 才释放。
        let root_lock = store
            .lock_local_control_root()
            .map_err(|e| format!("对象控制根启动锁获取失败：{e}"))?;
        let local_dir = local_state_dir(store.root());
        let control = Arc::new(
            ObjectControlStore::new(Arc::new(RootIo::new(store)))
                .with_local_dir(&local_dir)
                .map_err(|e| format!("节点本地运行态目录 {} 打开失败：{e}", local_dir.display()))?,
        );

        match control
            .root
            .load()
            .await
            .map_err(|e| format!("读取对象控制根失败：{e}"))?
        {
            Some(pinned) => {
                if pinned.root.meta.mode != expected {
                    return Err(format!(
                        "对象控制根 mode={:?} 与配置模式 {expected:?} 不一致：拒绝启动（fail-closed）",
                        pinned.root.meta.mode
                    )
                    .into());
                }
            }
            None => {
                // 无根即首次启动（标识 marker 也在根内，不存在“根缺失但 marker 在”
                // 的可达状态）；引导后立即回读校验格式。
                control
                    .bootstrap(expected, crate::db::now())
                    .await
                    .map_err(|e| format!("对象控制根引导失败：{e}"))?;
            }
        }
        control
            .check_compatible()
            .await
            .map_err(|e| format!("对象控制根格式不受支持：{e}"))?;

        // 与 SQLite 首启一样始终恰好一条 site_config 身份行；配置 id 一经生成不可变更。
        let existing = xl_store::control_store::sites::SiteStore::site_identity(&*control)
            .await
            .map_err(|e| format!("读取对象 site_config 失败：{e}"))?;
        if let (Some(seed), Some(row)) = (site_id, existing.as_ref())
            && row.id != seed
        {
            return Err(format!(
                "[site].id={seed} 与对象控制根内站点 id={} 不一致：拒绝启动（id 一经生成不可变更）",
                row.id
            )
            .into());
        }
        if existing.is_none() {
            let id = site_id
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let token = uuid::Uuid::new_v4().simple().to_string();
            let now = crate::db::now();
            control
                .transaction(WriteOrigin::local("startup"), move |txn| {
                    let id = id.clone();
                    let token = token.clone();
                    Box::pin(async move {
                        xl_store::control_store::sites::SiteTxn::site_config_seed(
                            txn, &id, &token, now,
                        )
                        .await
                        .map_err(store_error)
                    })
                })
                .await
                .map_err(|e| format!("写入对象 site_config 身份失败：{e}"))?;
        }

        // E1：视图在种子之后加载（包含身份行）；publisher 在加载后安装，二者之间
        // 若有提交，下一次发布按 generation 缺口整体重建，不会把旧表标成新代。
        let view = ViewCache::load(Arc::clone(&control))
            .await
            .map_err(|e| format!("控制热视图加载失败：{e}"))?;
        {
            // Weak：store 持有 publisher，若 publisher 再强持 view 会形成
            // store→publisher→view→store 的引用环；view 本身由本结构持有。
            let view = Arc::downgrade(&view);
            control.set_publisher(move |stamp| {
                let view = view.clone();
                Box::pin(async move {
                    if let Some(view) = view.upgrade()
                        && let Err(e) = view.publish(CacheDelta::from_stamp(&stamp)).await
                    {
                        tracing::error!(error = %e, "控制提交后热视图发布失败");
                    }
                })
            });
        }

        Ok(Arc::new(Self {
            control,
            view,
            _root_lock: root_lock,
        }))
    }

    /// 快照安装/批量替换后，从新根整体重建热视图（§4；下一个请求见新代）。
    pub async fn rebuild_view(&self) -> Result<(), ControlError> {
        self.view.rebuild().await
    }
}

/// 领域错误 → 对象事务的 `xl_store::error::Error` 通道（保留原始文案；
/// `ObjectControlStore::transaction` 的闭包返回 StoreError，领域 trait 返回
/// ControlError，二者只在启动引导/写派发边界相遇）。
pub(crate) fn store_error(e: ControlError) -> xl_store::error::Error {
    xl_store::error::Error::CorruptMeta(e.to_string())
}

/// 对象模式下尚未迁移的 SQLite 门面统一错误（E/F 收口；绝不静默写空）。
pub fn unsupported(facade: &str) -> ControlError {
    ControlError::Backend(format!(
        "control_backend=objects：{facade} 尚未双路由（B4 保留给 E/F）"
    ))
}

#[cfg(test)]
mod tests;
