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
//! **不做写入侧 fencing（裁决 + 边界）**：兜底 = 版本 uuid 隔离——交错最坏 = 双目录 +
//! meta 最新者胜，败者成孤儿（各节点 clean/fsck 回收），已提交数据不损坏，代价是回收延迟。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt as _;

use crate::error::{Error, Result};
use crate::mesh::{LockOp, LockReq, MeshTransport};

use super::{Store, now_nanos};

/// 单键租约条目。
#[derive(Debug, Clone)]
struct LockEntry {
    epoch: (i64, String),
    holder: String,
    expires: Instant,
}

/// 节点侧锁表（每节点内存；重启即清——租约本质，重启后旧租约自然作废）。
#[derive(Default)]
pub struct LockTable {
    inner: std::sync::Mutex<HashMap<String, LockEntry>>,
}

impl LockTable {
    /// 授予规则（唯一入口）：返回是否授予/续成。
    pub fn apply(&self, key: &str, req: &LockReq, ttl: Duration) -> bool {
        let now = Instant::now();
        let mut m = self.inner.lock().unwrap();
        match m.get_mut(key) {
            None => {
                if req.op == LockOp::Release {
                    return true; // 无锁可放：幂等成功
                }
                m.insert(
                    key.to_string(),
                    LockEntry {
                        epoch: (req.epoch_nanos, req.epoch_node.clone()),
                        holder: req.holder.clone(),
                        expires: now + ttl,
                    },
                );
                true
            }
            Some(e) if e.holder == req.holder => {
                match req.op {
                    LockOp::Release => {
                        m.remove(key);
                    }
                    // Renew /（同持有者重入的）Acquire：续期
                    _ => e.expires = now + ttl,
                }
                true
            }
            Some(e) if e.expires <= now => {
                if req.op == LockOp::Release {
                    return false; // 非持有者的 Release：忽略
                }
                // 已过期：epoch 更大的请求替换；迟到的旧 epoch 不抢回
                let newer = req.epoch_nanos > e.epoch.0
                    || (req.epoch_nanos == e.epoch.0 && req.epoch_node > e.epoch.1);
                if newer {
                    *e = LockEntry {
                        epoch: (req.epoch_nanos, req.epoch_node.clone()),
                        holder: req.holder.clone(),
                        expires: now + ttl,
                    };
                    true
                } else {
                    false
                }
            }
            // 活租约：绝不按 epoch 抢占
            Some(_) => false,
        }
    }

    /// 观测用：当前持有者（测试断言）。
    #[cfg(test)]
    pub fn holder(&self, key: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .map(|e| e.holder.clone())
    }
}

/// 集群锁守卫：持有时后台续租；Drop 时中止续租、释放本机表条目、best-effort 广播 Release。
pub(in crate::store) struct ClusterLockGuard {
    bucket: String,
    key: String,
    epoch: (i64, String),
    holder: String,
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
