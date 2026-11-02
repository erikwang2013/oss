//! 跨节点命名空间锁（M3 Task 8 / 设计 §4.4 路线的 quorum 租约锁，**节点域**）。
//!
//! 两半：
//! - **节点侧锁表**（[`LockTable`]）：每节点内存表 `key → {epoch, holder, expires}`，
//!   授予规则（评审三洞修正的落地）：
//!     * 仅当**键空闲或已过期**才授予新持有者；
//!     * 过期替换按 `epoch = (unix_nanos, node_id)` 定序——**迟到的旧 epoch 不得抢回**；
//!     * **活租约绝不按 epoch 抢占**（初稿字面「epoch 更大者胜」= 时钟快的节点抢走活锁）；
//!     * Renew/Release 仅持有者有效（非持有者的 Release 幂等忽略）。
//! - **协调者流程**（[`Store::acquire_cluster_lock`]）：广播 Acquire，
//!   **授予数（含本机表）≥ Q_n = ⌈n/2⌉+1** 才持有；否则对已授者 Release + 退避重试，
//!   耗尽 → `QuorumUnmet`（**分区拒写**，验收⑤）。持有时后台续租（TTL/3 广播，
//!   **Renew 同口径 Q_n**——初稿「≥半数」与 Acquire 不相交会双持有者）；租约丢失 →
//!   `lease_lost` 标记，写入口在提交点前检查。
//!
//! **Phase 0 Task 4：fenced 控制锁（opt-in）**。`fence_token = 0` 的数据面请求走上面
//! 完全不变的旧路径；`> 0` 的请求按**持久化高水位**校验：授予条件 = 提议 token 严格大于
//! 本键已存高水位（高水位在授予时落盘 `<root>/.oss.sys/locks/<sha256(bucket/key)>.fence`，
//! tmp + fsync + rename + 目录 fsync；重启/释放都不回落）。协调者
//! [`Store::acquire_fenced_cluster_lock`] 读高水位 → 提议 max+1 → 收满 Q_n 后持久化，
//! 过期提议被拒时用响应的 `last_fence_token` 重试。跨进程读-改-写用 flock 串行化
//! （与数据面 `lock_object` 同款）。解析失败 fail-closed（`CorruptMeta`），绝不回退 0。
//!
//! 写入侧 fencing 的最终守卫仍是对象 meta 的已提交 `fence_token`（Task 3）；锁服务的
//! token 只保证发放单调、不重号。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::{LockOp, LockReq, MeshTransport};

use super::{Store, now_nanos};

/// 单键租约条目。
#[derive(Debug, Clone)]
struct LockEntry {
    epoch: (i64, String),
    holder: String,
    expires: Instant,
    /// Phase 0：0 = 数据面锁（不参与 fence 校验）。
    fence_token: u64,
}

#[derive(Default)]
struct LockState {
    entries: HashMap<String, LockEntry>,
    /// 每键 fence 高水位：授予即抬高；Release/Drop 不清——旧 token 永不复用。
    fence_high: HashMap<String, u64>,
}

/// 节点侧锁表（每节点内存；重启即清——租约本质，重启后旧租约自然作废；
/// fence 高水位例外：从磁盘 `.fence` 文件恢复）。
#[derive(Default)]
pub struct LockTable {
    inner: std::sync::Mutex<LockState>,
}

impl LockTable {
    /// 授予规则（唯一入口）：返回是否授予/续成。
    pub fn apply(&self, key: &str, req: &LockReq, ttl: Duration) -> bool {
        let now = Instant::now();
        let fenced = req.fence_token != 0;
        let mut st = self.inner.lock().unwrap();
        let LockState {
            entries,
            fence_high,
        } = &mut *st;
        let stored = fence_high.get(key).copied().unwrap_or(0);
        match entries.get(key).cloned() {
            None => match req.op {
                LockOp::Release => true, // 无锁可放：幂等成功
                LockOp::Renew => {
                    // fenced 续租不抬高水位：重启后同 token 可重建条目，旧 token 不得
                    if fenced && req.fence_token != stored {
                        return false;
                    }
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            epoch: (req.epoch_nanos, req.epoch_node.clone()),
                            holder: req.holder.clone(),
                            expires: now + ttl,
                            fence_token: req.fence_token,
                        },
                    );
                    true
                }
                LockOp::Acquire => {
                    if fenced && req.fence_token <= stored {
                        return false;
                    }
                    if fenced {
                        fence_high.insert(key.to_string(), req.fence_token);
                    }
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            epoch: (req.epoch_nanos, req.epoch_node.clone()),
                            holder: req.holder.clone(),
                            expires: now + ttl,
                            fence_token: req.fence_token,
                        },
                    );
                    true
                }
            },
            Some(e) if e.holder == req.holder => match req.op {
                LockOp::Release => {
                    // fenced：旧/异 token 的迟到 Release 不得解掉新锁
                    if fenced && req.fence_token != e.fence_token {
                        return false;
                    }
                    entries.remove(key);
                    true
                }
                // Renew /（同持有者重入的）Acquire：旧数据面语义 = 续期
                LockOp::Renew => {
                    if fenced && req.fence_token != e.fence_token {
                        return false;
                    }
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            expires: now + ttl,
                            ..e
                        },
                    );
                    true
                }
                LockOp::Acquire => {
                    // fenced：活租约不得重入/抢占；新一次获取须等释放或过期
                    if fenced {
                        return false;
                    }
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            expires: now + ttl,
                            ..e
                        },
                    );
                    true
                }
            },
            Some(e) if e.expires <= now => {
                if req.op == LockOp::Release {
                    return false; // 非持有者的 Release：忽略
                }
                if fenced {
                    if req.fence_token <= stored {
                        return false;
                    }
                    fence_high.insert(key.to_string(), req.fence_token);
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            epoch: (req.epoch_nanos, req.epoch_node.clone()),
                            holder: req.holder.clone(),
                            expires: now + ttl,
                            fence_token: req.fence_token,
                        },
                    );
                    return true;
                }
                // 已过期：epoch 更大的请求替换；迟到的旧 epoch 不抢回
                let newer = req.epoch_nanos > e.epoch.0
                    || (req.epoch_nanos == e.epoch.0 && req.epoch_node > e.epoch.1);
                if newer {
                    entries.insert(
                        key.to_string(),
                        LockEntry {
                            epoch: (req.epoch_nanos, req.epoch_node.clone()),
                            holder: req.holder.clone(),
                            expires: now + ttl,
                            fence_token: 0,
                        },
                    );
                    true
                } else {
                    false
                }
            }
            // 活租约：绝不按 epoch 抢占
            Some(_) => false,
        }
    }

    /// 从持久化文件恢复的高水位与内存高水位取 max（绝不回落）。
    pub fn seed_fence_high(&self, key: &str, token: u64) {
        let mut st = self.inner.lock().unwrap();
        let cur = st.fence_high.entry(key.to_string()).or_insert(0);
        *cur = (*cur).max(token);
    }

    /// 该键 fence 高水位（应答 `last_fence_token` / 协调者重试锚点）。
    pub fn last_fence_token(&self, key: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .fence_high
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    /// 观测用：当前持有者（测试断言）。
    #[cfg(test)]
    pub fn holder(&self, key: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .entries
            .get(key)
            .map(|e| e.holder.clone())
    }
}

/// 高水位文件路径：`<root>/.oss.sys/locks/<sha256(bucket/key)>.fence`（完整 64 hex）。
pub(in crate::store) fn fence_path(root: &Path, lock_key: &str) -> PathBuf {
    fence_file(root, lock_key, "fence")
}

fn fence_file(root: &Path, lock_key: &str, ext: &str) -> PathBuf {
    use sha2::Digest as _;
    let id = hex::encode(sha2::Sha256::digest(lock_key.as_bytes()));
    layout::sys_dir(root, "locks").join(format!("{id}.{ext}"))
}

/// 读持久化高水位：文件不存在 = 0（合法初始态）；读取/解析失败 fail-closed。
pub(in crate::store) async fn read_fence_high(root: &Path, lock_key: &str) -> Result<u64> {
    match tokio::fs::read_to_string(fence_path(root, lock_key)).await {
        Ok(s) => s.trim().parse::<u64>().map_err(|_| {
            let head: String = s.chars().take(32).collect();
            Error::CorruptMeta(format!("fence 高水位文件损坏（{lock_key:?}）：{head:?}"))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// tmp + fsync + rename + 目录 fsync。调用方须已持 [`lock_fence_file`] 或确认 token 更高。
async fn write_fence_high(root: &Path, lock_key: &str, token: u64) -> Result<()> {
    let path = fence_path(root, lock_key);
    let dir = path
        .parent()
        .ok_or_else(|| Error::Encode("fence 路径无父目录".into()))?;
    tokio::fs::create_dir_all(dir).await?;
    let tmp = fence_file(root, lock_key, "fence.tmp");
    let mut f = tokio::fs::File::create(&tmp).await?;
    f.write_all(token.to_string().as_bytes()).await?;
    f.sync_all().await?;
    drop(f);
    tokio::fs::rename(&tmp, &path).await?;
    tokio::fs::File::open(dir).await?.sync_all().await?;
    Ok(())
}

/// 协调者收满 quorum 后的持久化：持 flock 取 max 再写——绝不下调高水位。
#[allow(dead_code)] // Phase 0 Task 5 接入控制面写路径；当前仅测试调用
async fn advance_fence_high(root: &Path, lock_key: &str, token: u64) -> Result<u64> {
    let _flock = lock_fence_file(root, lock_key).await?;
    let cur = read_fence_high(root, lock_key).await?;
    let next = cur.max(token);
    if next != cur {
        write_fence_high(root, lock_key, next).await?;
    }
    Ok(next)
}

/// 跨进程串行化 fenced 高水位读-改-写（与 `lock_object` 同款 flock；File drop 即解锁）。
async fn lock_fence_file(root: &Path, lock_key: &str) -> Result<std::fs::File> {
    let path = fence_file(root, lock_key, "fence.lock");
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let file = tokio::fs::File::create(&path).await?.into_std().await;
    tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
        file.lock()?;
        Ok(file)
    })
    .await
    .map_err(|e| Error::Encode(format!("fence 锁等待任务异常: {e}")))?
    .map_err(Error::from)
}

/// 集群锁守卫：持有时后台续租；Drop 时中止续租、释放本机表条目、best-effort 广播 Release。
pub(in crate::store) struct ClusterLockGuard {
    bucket: String,
    key: String,
    epoch: (i64, String),
    holder: String,
    /// Phase 0：0 = 数据面；>0 = fenced（Release/Renew 必须携带同 token）。
    fence_token: u64,
    lease_lost: Arc<AtomicBool>,
    renew_task: tokio::task::JoinHandle<()>,
    transport: Arc<dyn MeshTransport>,
    others: Vec<String>, // 除本机外的节点 id（Release 广播目标）
    table: Arc<LockTable>,
}

impl ClusterLockGuard {
    /// 租约是否已丢失（续租未达 Q_n）——写入口在提交点前检查。
    pub(in crate::store) fn lease_lost(&self) -> bool {
        self.lease_lost.load(Ordering::Relaxed)
    }

    fn release_req(&self) -> LockReq {
        LockReq {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            epoch_nanos: self.epoch.0,
            epoch_node: self.epoch.1.clone(),
            holder: self.holder.clone(),
            ttl_ms: 0,
            op: LockOp::Release,
            fence_token: self.fence_token,
        }
    }
}

impl Drop for ClusterLockGuard {
    fn drop(&mut self) {
        self.renew_task.abort();
        // 本机表条目同步释放（holder 匹配 → 移除）
        let req = self.release_req();
        self.table.apply(
            &format!("{}/{}", self.bucket, self.key),
            &req,
            Duration::ZERO,
        );
        // 远端 best-effort 广播（不在 tokio 运行时内则跳过——租约 TTL 兜底）
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("Guard Drop 不在 tokio 运行时内——远程 Release 广播跳过（TTL 兜底）");
            return;
        };
        let transport = self.transport.clone();
        let others = self.others.clone();
        handle.spawn(async move {
            for n in &others {
                let _ = transport.lock(n, req.clone()).await;
            }
        });
    }
}

/// fenced 锁守卫（Phase 0 Task 4）：`token` = 本次授予的单调 fence token；
/// 集群下复用 [`ClusterLockGuard`] 的续租/释放，单机下 Drop 释放本机条目。
#[allow(dead_code)] // Phase 0 Task 5 接入控制面写路径；当前仅测试调用
pub(in crate::store) struct FencedLockGuard {
    pub token: u64,
    guard: Option<ClusterLockGuard>,
    /// 单机：Drop 时释放本机表条目（无租约/续租/远端广播）。
    local: Option<(Arc<LockTable>, LockReq)>,
}

impl FencedLockGuard {
    /// 租约是否已丢失（单机恒 false）——写入口在提交点前检查。
    #[allow(dead_code)] // Phase 0 Task 5 接入写入口
    pub(in crate::store) fn lease_lost(&self) -> bool {
        self.guard.as_ref().is_some_and(|g| g.lease_lost())
    }
}

impl std::fmt::Debug for FencedLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedLockGuard")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl Drop for FencedLockGuard {
    fn drop(&mut self) {
        if let Some((table, req)) = &self.local {
            table.apply(&format!("{}/{}", req.bucket, req.key), req, Duration::ZERO);
        }
    }
}

impl Store {
    /// 集群锁表（服务端 `mesh_lock` 与测试共用）。
    pub(in crate::store) fn lock_table(&self) -> &LockTable {
        &self.cluster_locks
    }

    /// 节点域多数：Q_n = ⌈n/2⌉ + 1（单机退化 1）。
    pub(in crate::store) fn q_n(&self) -> usize {
        let n = self
            .cfg
            .cluster
            .as_deref()
            .map_or(1, |c| c.sets.flat.nodes.len());
        n / 2 + 1
    }

    /// 本地应用一次锁请求（服务端 handler 与协调者共用）。
    ///
    /// fenced Acquire：持 flock 读持久化高水位 → seed 内存 → 校验/授予；
    /// `persist = true` 时先落盘再回 granted，落盘失败撤回授予（fail-closed）。
    /// 协调者本地授予用 `persist = false`（规则：收满 quorum 后才持久化）。
    pub(in crate::store) async fn apply_lock(&self, req: &LockReq, persist: bool) -> Result<bool> {
        let key = format!("{}/{}", req.bucket, req.key);
        let ttl = Duration::from_millis(req.ttl_ms.max(1));
        let fenced = req.fence_token != 0;
        // 跨进程/重启同步：Acquire 恒读盘；Renew 仅在本进程没见过该键时读盘
        // （重启后重建租约；正常续租不付磁盘 I/O）。
        let sync = fenced
            && (req.op == LockOp::Acquire
                || (req.op == LockOp::Renew && self.lock_table().last_fence_token(&key) == 0));
        let _flock = if sync {
            Some(lock_fence_file(self.cfg.root(), &key).await?)
        } else {
            None
        };
        if sync {
            let disk = read_fence_high(self.cfg.root(), &key).await?;
            self.lock_table().seed_fence_high(&key, disk);
        }
        let granted = self.lock_table().apply(&key, req, ttl);
        if granted && persist && fenced && req.op == LockOp::Acquire {
            // 在 flock 内：apply 已保证 token > 已存高水位，写值即新高水位。
            if let Err(e) = write_fence_high(self.cfg.root(), &key, req.fence_token).await {
                let rel = LockReq {
                    op: LockOp::Release,
                    ..req.clone()
                };
                self.lock_table().apply(&key, &rel, Duration::ZERO);
                return Err(e);
            }
        }
        Ok(granted)
    }

    /// 协调者：获取集群租约锁（进程内 / flock 之后的第三层；顺序恒定防死锁）。
    pub(in crate::store) async fn acquire_cluster_lock(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ClusterLockGuard> {
        let cl = self.cfg.cluster.as_deref().expect("集群分支调用");
        let self_id = cl.sets.flat.self_node().id.clone();
        let lock_key = format!("{bucket}/{key}");
        let q_n = self.q_n();
        let ttl_ms = self.cfg.cluster_lock_ttl_ms;
        let ttl = Duration::from_millis(ttl_ms);
        let others: Vec<String> = cl
            .sets
            .flat
            .nodes
            .iter()
            .filter(|n| n.id != self_id)
            .map(|n| n.id.clone())
            .collect();

        for attempt in 0..self.cfg.cluster_lock_retries {
            let epoch = (now_nanos(), self_id.clone());
            let req = LockReq {
                bucket: bucket.to_string(),
                key: key.to_string(),
                epoch_nanos: epoch.0,
                epoch_node: epoch.1.clone(),
                holder: self_id.clone(),
                ttl_ms,
                op: LockOp::Acquire,
                fence_token: 0,
            };
            // 广播（含本机表）；收满 Q_n 即停（未收的响应若稍后授予也无妨：Release 广播覆盖）
            let mut granted: Vec<String> = Vec::new();
            if self.lock_table().apply(&lock_key, &req, ttl) {
                granted.push(self_id.clone());
            }
            let mut futs = futures::stream::FuturesUnordered::new();
            for n in &others {
                let r = req.clone();
                let node = n.clone();
                futs.push(async move {
                    let res = cl.transport.lock(&node, r).await;
                    (node, res)
                });
            }
            while granted.len() < q_n {
                let Some((node, res)) = futs.next().await else {
                    break;
                };
                if matches!(res, Ok(r) if r.granted) {
                    granted.push(node);
                }
            }
            if granted.len() >= q_n {
                let lease_lost = Arc::new(AtomicBool::new(false));
                let renew_task = self.spawn_renew_task(req.clone(), lease_lost.clone(), ttl);
                return Ok(ClusterLockGuard {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    epoch,
                    holder: self_id,
                    fence_token: 0,
                    lease_lost,
                    renew_task,
                    transport: cl.transport.clone(),
                    others,
                    table: self.cluster_locks.clone(),
                });
            }
            // 不足：Release 已授予者（含本机）→ 退避重试（抖动）
            let rel = LockReq {
                op: LockOp::Release,
                ..req
            };
            self.lock_table().apply(&lock_key, &rel, Duration::ZERO);
            for node in granted.iter().filter(|n| n.as_str() != self_id) {
                let _ = cl.transport.lock(node, rel.clone()).await;
            }
            let (lo, hi) = self.cfg.cluster_lock_backoff_ms;
            let backoff = if hi > lo {
                lo + (now_nanos().unsigned_abs() % (hi - lo))
            } else {
                lo
            };
            tracing::debug!(
                attempt,
                granted = granted.len(),
                "集群锁未达 quorum，退避重试"
            );
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
        self.bump_quorum_unmet_lock();
        Err(Error::QuorumUnmet(format!(
            "集群锁 quorum 未达（{lock_key}，重试耗尽）——分区/节点不可达时拒绝写"
        )))
    }

    /// Phase 0 Task 4：获取带持久化单调 fence token 的集群锁。
    ///
    /// 流程：读本地持久化高水位 → `proposed = max+1` → 广播 Acquire → 收满 Q_n
    /// （且授予者回报 `last_fence_token >= proposed`，旧节点不计）后持久化 proposed。
    /// 节点拒绝并回报更高 `last_fence_token` 时以 `max(last)+1` 重试。
    /// 单机（无 cluster 配置）：从同一持久化文件读 +1，本地授予即持久化。
    #[allow(dead_code)] // Phase 0 Task 5 消费；当前仅测试调用
    pub(in crate::store) async fn acquire_fenced_cluster_lock(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<FencedLockGuard> {
        let lock_key = format!("{bucket}/{key}");
        let ttl_ms = self.cfg.cluster_lock_ttl_ms;
        let ttl = Duration::from_millis(ttl_ms);
        let mut proposed = read_fence_high(self.cfg.root(), &lock_key)
            .await?
            .saturating_add(1);

        let Some(cl) = self.cfg.cluster.as_deref() else {
            // 单机：本地授予即 quorum；持久化成功后才返回（apply_lock persist=true）。
            for _ in 0..self.cfg.cluster_lock_retries.max(1) {
                let req = LockReq {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    epoch_nanos: now_nanos(),
                    epoch_node: "local".into(),
                    holder: "local".into(),
                    ttl_ms,
                    op: LockOp::Acquire,
                    fence_token: proposed,
                };
                if self.apply_lock(&req, true).await? {
                    return Ok(FencedLockGuard {
                        token: proposed,
                        guard: None,
                        local: Some((
                            self.cluster_locks.clone(),
                            LockReq {
                                op: LockOp::Release,
                                ..req
                            },
                        )),
                    });
                }
                // 并发进程抬高了落盘高水位 → 用 last+1 重试；否则是活租约冲突。
                let last = self.lock_table().last_fence_token(&lock_key);
                if last < proposed {
                    return Err(Error::Conflict(format!("fenced 锁已被持有：{lock_key}")));
                }
                proposed = last.saturating_add(1);
            }
            return Err(Error::Conflict(format!(
                "fenced 锁竞争重试耗尽：{lock_key}"
            )));
        };

        let self_id = cl.sets.flat.self_node().id.clone();
        let q_n = self.q_n();
        let others: Vec<String> = cl
            .sets
            .flat
            .nodes
            .iter()
            .filter(|n| n.id != self_id)
            .map(|n| n.id.clone())
            .collect();

        for attempt in 0..self.cfg.cluster_lock_retries {
            let epoch = (now_nanos(), self_id.clone());
            let req = LockReq {
                bucket: bucket.to_string(),
                key: key.to_string(),
                epoch_nanos: epoch.0,
                epoch_node: epoch.1.clone(),
                holder: self_id.clone(),
                ttl_ms,
                op: LockOp::Acquire,
                fence_token: proposed,
            };
            // 本机表也算一票（持久化留到收满 quorum 之后——规则 4）。
            let mut granted: Vec<String> = Vec::new();
            let mut remote_granted: Vec<String> = Vec::new();
            let mut max_last = self.lock_table().last_fence_token(&lock_key);
            if self.apply_lock(&req, false).await? {
                granted.push(self_id.clone());
            }
            let mut futs = futures::stream::FuturesUnordered::new();
            for n in &others {
                let r = req.clone();
                let node = n.clone();
                futs.push(async move {
                    let res = cl.transport.lock(&node, r).await;
                    (node, res)
                });
            }
            while granted.len() < q_n {
                let Some((node, res)) = futs.next().await else {
                    break;
                };
                let Ok(r) = res else { continue };
                max_last = max_last.max(r.last_fence_token);
                if r.granted {
                    // 旧节点忽略 fence 字段（回声 0/低于提议）→ 不得计为 fenced 授予
                    if r.last_fence_token >= proposed {
                        granted.push(node.clone());
                    }
                    remote_granted.push(node);
                }
            }
            if granted.len() >= q_n {
                let persisted = match advance_fence_high(self.cfg.root(), &lock_key, proposed).await
                {
                    Ok(v) => v,
                    Err(e) => {
                        // 持久化失败 = 不得持有：释放已授者（fail-closed）。
                        let rel = LockReq {
                            op: LockOp::Release,
                            ..req.clone()
                        };
                        self.lock_table().apply(&lock_key, &rel, Duration::ZERO);
                        for node in &remote_granted {
                            let _ = cl.transport.lock(node, rel.clone()).await;
                        }
                        return Err(e);
                    }
                };
                if persisted > proposed {
                    // 同节点另一进程已发过更高 token：本次授予无效，释放后重试。
                    let rel = LockReq {
                        op: LockOp::Release,
                        ..req.clone()
                    };
                    self.lock_table().apply(&lock_key, &rel, Duration::ZERO);
                    for node in &remote_granted {
                        let _ = cl.transport.lock(node, rel.clone()).await;
                    }
                    proposed = persisted.saturating_add(1);
                    continue;
                }
                let lease_lost = Arc::new(AtomicBool::new(false));
                let renew_task = self.spawn_renew_task(req.clone(), lease_lost.clone(), ttl);
                return Ok(FencedLockGuard {
                    token: proposed,
                    guard: Some(ClusterLockGuard {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        epoch,
                        holder: self_id,
                        fence_token: proposed,
                        lease_lost,
                        renew_task,
                        transport: cl.transport.clone(),
                        others,
                        table: self.cluster_locks.clone(),
                    }),
                    local: None,
                });
            }
            // 不足：释放所有实际授予者（含旧节点的不计数授予）→ 退避重试。
            let rel = LockReq {
                op: LockOp::Release,
                ..req.clone()
            };
            self.lock_table().apply(&lock_key, &rel, Duration::ZERO);
            for node in &remote_granted {
                let _ = cl.transport.lock(node, rel.clone()).await;
            }
            // 已授予过的 token 在各节点已落盘，不能复用：下一轮至少 max_last+1。
            proposed = proposed.max(max_last).saturating_add(1);
            let (lo, hi) = self.cfg.cluster_lock_backoff_ms;
            let backoff = if hi > lo {
                lo + (now_nanos().unsigned_abs() % (hi - lo))
            } else {
                lo
            };
            tracing::debug!(
                attempt,
                granted = granted.len(),
                proposed,
                "fenced 集群锁未达 quorum，退避重试"
            );
            tokio::time::sleep(Duration::from_millis(backoff)).await;
        }
        self.bump_quorum_unmet_lock();
        Err(Error::QuorumUnmet(format!(
            "fenced 集群锁 quorum 未达（{lock_key}，重试耗尽）——分区/节点不可达时拒绝写"
        )))
    }

    /// 后台续租：每 TTL/3 广播 Renew（全节点），授予数 ≥ Q_n 续成；失败 → lease_lost。
    fn spawn_renew_task(
        &self,
        req: LockReq,
        lease_lost: Arc<AtomicBool>,
        ttl: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let cl = self.cfg.cluster.as_deref().expect("集群分支");
        let transport = cl.transport.clone();
        let self_id = cl.sets.flat.self_node().id.clone();
        let others: Vec<String> = cl
            .sets
            .flat
            .nodes
            .iter()
            .filter(|n| n.id != self_id)
            .map(|n| n.id.clone())
            .collect();
        let q_n = self.q_n();
        let table = self.cluster_locks.clone();
        let lock_key = format!("{}/{}", req.bucket, req.key);
        let period = (ttl / 3).max(Duration::from_millis(5));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await; // 首个 tick 立即返回——跳过
            loop {
                ticker.tick().await;
                let renew = LockReq {
                    op: LockOp::Renew,
                    ..req.clone()
                };
                let mut count = 0usize;
                if table.apply(&lock_key, &renew, ttl) {
                    count += 1;
                }
                let mut futs = futures::stream::FuturesUnordered::new();
                for n in &others {
                    let r = renew.clone();
                    let node = n.clone();
                    let t = transport.clone();
                    futs.push(async move {
                        let res = t.lock(&node, r).await;
                        matches!(res, Ok(r) if r.granted)
                    });
                }
                while count < q_n {
                    let Some(ok) = futs.next().await else { break };
                    if ok {
                        count += 1;
                    }
                }
                if count < q_n {
                    tracing::warn!("集群锁续租未达 quorum——标记租约丢失");
                    lease_lost.store(true, Ordering::Relaxed);
                    break;
                }
            }
        })
    }
}
