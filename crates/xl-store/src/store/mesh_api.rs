//! 服务端 mesh 面（M3 Task 3 骨架）：**pub 入口**——in-process 实现与 xl-client 的
//! tonic handler 共用这一层。可见性约定：外部（xl-client）只能经 `Store::mesh_*`；
//! 其余内部件（stage_payload / write_meta_on / object_dir）保持 `pub(in crate::store)`。
//!
//! 语义学分工（与 mesh.rs 头注一致）：**一个节点收到的请求只操作它自己的本地槽**；
//! 扇出永远由协调者（收到客户端请求的节点）做——in-process 实现若在对端再扇出会循环 await 死锁。

use crate::layout;
use crate::mesh::NodeHealth;

use super::Store;
use super::drives;

impl Store {
    /// 本节点健康：拓扑视角的槽位在册状态 + 桶计数（/health/cluster 与 fixture 共用）。
    /// 单节点形态（未配 cluster）node_id = "single"、槽位从 0 起 = 本机盘。
    pub async fn mesh_health(&self) -> NodeHealth {
        let (node_id, base) = match self.cfg.cluster.as_deref() {
            None => ("single".to_string(), 0u16),
            Some(cl) => (cl.topo.self_node().id.clone(), cl.topo.local_slot_base()),
        };
        let mut slots = Vec::with_capacity(self.cfg.drives.len());
        for (i, drive) in self.cfg.drives.iter().enumerate() {
            slots.push((base + i as u16, drives::drive_up(drive).await));
        }
        // 桶计数 = 本机各盘桶目录并集（与 list_buckets 同口径：磁盘权威）
        let mut buckets = std::collections::BTreeSet::new();
        for drive in &self.cfg.drives {
            let Ok(mut rd) = tokio::fs::read_dir(drive).await else {
                continue;
            };
            while let Ok(Some(e)) = rd.next_entry().await {
                let name = e.file_name().to_string_lossy().into_owned();
                if name != layout::SYS_DIR
                    && e.file_type().await.map(|t| t.is_dir()).unwrap_or(false)
                {
                    buckets.insert(name);
                }
            }
        }
        NodeHealth {
            node_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
            slots,
            buckets: buckets.len() as u64,
        }
    }
}
