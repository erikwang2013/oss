//! fsck：上盘巡检（M2b-2 Task 10 起步版；设计 §4.1 预留的 `.oss.sys/fsck/` 报告目录在此启用）。
//!
//! 巡检三类对象（只读报告；`--repair` 才写）：
//!   a) tmp/trash 条目计数（回收交给 `clean`，fsck 只报数）；
//!   b) 孤儿版本目录：对象目录下未被任何版本引用的子目录（崩溃残骸 / 未完成 MPU）；
//!   c) EC 分片缺/坏计数：逐对象按 `DataLayout.shards` 校验（本机直读 / 远端 read_shard）。
//! `--repair`：删孤儿目录 + 对缺/坏片的对象走一次 `get_object`（触发读时修回写）。
//!
//! **M3 集群扩展（Task 7）**：
//!   - peer 先探活：down → `unreachable_nodes`（其数据不巡检、不计缺）；
//!   - **缺键检测 = 差集**（「本机键集 vs 各 peer ListLocal 键并集」）——本机扫描
//!     发现不了自己缺的键（掉线期写入的对象在本机毫无痕迹），必须借对端键表；
//!   - `--repair` 对缺键做**补建闭环**：quorum 读最新 meta → 写回本机各槽（本机读侧
//!     从此有本；分片补建靠读时修兜底、批量归 M4 heal）；
//!   - 桶目录比对：对端有而本机无的桶 → `missing_bucket_dirs`（报告项，补建归 M4）。
//!
//! **裁决**：独立的 `scrub` 全量深度巡检仍是 M4（设计验收顺延）；本命令是「找出问题 + 最小修复」，
//! 报告 JSON 由调用方（oss-server cli）序列化落盘——xl-store 不引 serde_json。

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use serde::Serialize;

use crate::error::Result;
use crate::layout;
use crate::store::{Store, drives, ec};

/// 巡检报告（JSON 序列化形态由调用方决定）。
#[derive(Debug, Default, Serialize)]
pub struct FsckReport {
    pub drives: usize,
    pub objects_scanned: usize,
    /// xl.meta 全部副本都读不出（损坏）的对象数
    pub corrupt_meta: usize,
    /// tmp 目录条目数（每盘合计；清理由 clean/启动自检负责）
    pub tmp_entries: usize,
    /// trash 目录条目数（同上）
    pub trash_entries: usize,
    /// 孤儿版本目录（不可见残骸）：`<盘下标>#<bucket>/<key>/<uuid>`
    pub orphan_dirs: Vec<String>,
    /// 缺失的 EC 分片数（文件不存在 / 槽位越界 / 远端不可达视为未巡检）
    pub missing_shards: usize,
    /// CRC32C 不符的 EC 分片数
    pub bad_shards: usize,
    /// 是否执行了修复（删孤儿 + 读时修 + 缺键补建）
    pub repaired: bool,
    /// 集群：不可达的节点 id（其数据未巡检——「加回节点后补齐」的巡检面）
    pub unreachable_nodes: Vec<String>,
    /// 集群：对端存在而本机缺 meta 的键（差集口径）
    pub missing_meta_keys: Vec<String>,
    /// 集群：对端存在而本机无桶目录的桶（报告项；补建归 M4 heal）
    pub missing_bucket_dirs: Vec<String>,
    /// 集群 repair：补建 meta 的键数
    pub repaired_meta_keys: usize,
}

impl Store {
    /// 全盘巡检。`repair=true` 时执行最小修复（删孤儿 + 触发读时修 + 缺键补 meta）。
    pub async fn fsck(&self, repair: bool) -> Result<FsckReport> {
        let mut r = FsckReport {
            drives: self.cfg.drives.len(),
            repaired: repair,
            ..Default::default()
        };

        // a) tmp/trash 计数（每盘各自的 sys 目录）
        for drive in &self.cfg.drives {
            for sub in ["tmp", "trash"] {
                let n = count_entries(&layout::sys_dir(drive, sub)).await?;
                match sub {
                    "tmp" => r.tmp_entries += n,
                    _ => r.trash_entries += n,
                }
            }
        }

        // 集群：peer 探活（down → 报告项；其数据不巡检不计缺）
        let mut peers_up: Vec<String> = Vec::new();
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.topo.self_node().id.clone();
            for n in &cl.topo.nodes {
                if n.id == self_id {
                    continue;
                }
                match cl.transport.health(&n.id).await {
                    Ok(h) if h.node_id == n.id => peers_up.push(n.id.clone()),
                    _ => {
                        tracing::warn!(node = %n.id, "fsck：节点不可达（其数据未巡检）");
                        r.unreachable_nodes.push(n.id.clone());
                    }
                }
            }
        }

        // b)+c) 命名空间：从所有盘收集桶与对象键（容错——掉线盘跳过）
        let mut keys: BTreeSet<(String, String)> = BTreeSet::new();
        let mut local_buckets: BTreeSet<String> = BTreeSet::new();
        for drive in &self.cfg.drives {
            let mut buckets = match tokio::fs::read_dir(drive).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(bucket) = buckets.next_entry().await? {
                if !bucket.file_type().await?.is_dir() {
                    continue;
                }
                let bucket_name = bucket.file_name().to_string_lossy().into_owned();
                if bucket_name == layout::SYS_DIR {
                    continue;
                }
                local_buckets.insert(bucket_name.clone());
                let mut objects = tokio::fs::read_dir(bucket.path()).await?;
                while let Some(object) = objects.next_entry().await? {
                    if !object.file_type().await?.is_dir() {
                        continue;
                    }
                    let dir_name = object.file_name().to_string_lossy().into_owned();
                    if let Ok(key) = layout::key_from_dir_name(&dir_name) {
                        keys.insert((bucket_name.clone(), key));
                    }
                }
            }
        }

        // 集群：差集检测（本机扫描发现不了缺键——必须借对端键表）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let mut peer_keys: BTreeSet<(String, String)> = BTreeSet::new();
            let mut peer_buckets: BTreeSet<String> = BTreeSet::new();
            for peer in &peers_up {
                match cl.transport.list_bucket_dirs(peer).await {
                    Ok(names) => {
                        for b in names {
                            peer_buckets.insert(b.clone());
                            match collect_peer_keys(cl, peer, &b).await {
                                Ok(ks) => {
                                    for k in ks {
                                        peer_keys.insert((b.clone(), k));
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(node = %peer, bucket = %b, error = %e, "fsck：对端键表拉取失败")
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(node = %peer, error = %e, "fsck：对端桶表拉取失败")
                    }
                }
            }
            let mut missing_pairs: Vec<(String, String)> = Vec::new();
            for (b, k) in peer_keys.difference(&keys) {
                missing_pairs.push((b.clone(), k.clone()));
                r.missing_meta_keys.push(format!("{b}/{k}"));
            }
            for b in peer_buckets.difference(&local_buckets) {
                r.missing_bucket_dirs.push(b.clone());
            }

            // repair：缺键补建闭环——quorum 读最新 meta → 写回本机各在册槽
            if repair {
                for (b, k) in missing_pairs {
                    match self.read_meta(&b, &k).await {
                        Ok(Some(m)) => {
                            let bytes = m.encode()?;
                            for drive in &self.cfg.drives {
                                if !drives::drive_up(drive).await {
                                    continue;
                                }
                                self.write_meta_new_on(drive, &b, &k, &bytes).await?;
                                let _ = self.commit_meta_on(drive, &b, &k).await?;
                            }
                            r.repaired_meta_keys += 1;
                        }
                        Ok(None) => {
                            tracing::warn!(bucket = %b, key = %k, "fsck：对端有的键 quorum 读却缺席（并发删除？）")
                        }
                        Err(e) => {
                            tracing::warn!(bucket = %b, key = %k, error = %e, "fsck：缺键补建失败")
                        }
                    }
                }
            }
        }

        for (bucket, key) in keys {
            r.objects_scanned += 1;
            // 引用集：所有版本（含旧 parts 兼容位）的 uuid
            let referenced: HashSet<String> = match self.read_meta(&bucket, &key).await {
                Ok(Some(m)) => m
                    .versions
                    .iter()
                    .flat_map(|v| v.effective_parts())
                    .map(|p| p.uuid)
                    .collect(),
                Ok(None) => HashSet::new(), // 无 meta = 崩在提交点前：全部子目录都是孤儿
                Err(e) => {
                    r.corrupt_meta += 1;
                    tracing::warn!(bucket = %bucket, key = %key, error = %e, "fsck：meta 读取失败");
                    continue; // 保守：损坏 meta 的对象整体跳过（与 reap_stale 同口径）
                }
            };

            // 孤儿扫描（逐盘）+ 分片巡检（按 meta 里的 DataLayout）
            for (d, drive) in self.cfg.drives.iter().enumerate() {
                let Ok(obj_dir) = layout::object_dir(drive, &bucket, &key) else {
                    continue;
                };
                let mut subs = match tokio::fs::read_dir(&obj_dir).await {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                while let Some(sub) = subs.next_entry().await? {
                    if !sub.file_type().await?.is_dir() {
                        continue;
                    }
                    let name = sub.file_name().to_string_lossy().into_owned();
                    if !referenced.contains(&name) {
                        r.orphan_dirs.push(format!("{d}#{bucket}/{key}/{name}"));
                        if repair {
                            let _ = tokio::fs::remove_dir_all(sub.path()).await;
                        }
                    }
                }
            }

            // c) 分片巡检：取任一盘的 meta 里最新版本（uuid + DataLayout）
            let layout_of = self
                .read_meta(&bucket, &key)
                .await
                .ok()
                .flatten()
                .and_then(|m| {
                    m.latest()
                        .and_then(|v| v.data_layout.clone().map(|l| (v.part.uuid.clone(), l)))
                });
            if let Some((uuid, l)) = layout_of {
                let mut miss = 0usize;
                let mut bad = 0usize;
                for s in &l.shards {
                    match self.shard_target(s) {
                        Some(drives::ShardTarget::Local { drive, .. }) => {
                            let Ok(path) = drives::shard_path(drive, &bucket, &key, &uuid, s)
                            else {
                                miss += 1;
                                continue;
                            };
                            match tokio::fs::read(&path).await {
                                Ok(bytes) if ec::crc32c_of(&bytes) == s.crc32c => {}
                                Ok(_) => bad += 1,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => miss += 1,
                                Err(e) => return Err(e.into()),
                            }
                        }
                        Some(drives::ShardTarget::Remote { node, .. }) => {
                            if !peers_up.iter().any(|p| p == node) {
                                continue; // 不可达节点：已计 unreachable，不重复计缺
                            }
                            let Some(t) = self.mesh_transport() else {
                                miss += 1;
                                continue;
                            };
                            let req = crate::mesh::ShardRead {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                uuid: uuid.clone(),
                                part: s.part as u32,
                                shard: s.shard as u32,
                            };
                            match t.read_shard(node, req).await {
                                Ok(Some(bytes)) => {
                                    if ec::crc32c_of(&bytes) != s.crc32c {
                                        bad += 1;
                                    }
                                }
                                Ok(None) | Err(_) => miss += 1,
                            }
                        }
                        None => miss += 1,
                    }
                }
                r.missing_shards += miss;
                r.bad_shards += bad;
                if repair && (miss > 0 || bad > 0) {
                    // 读时修：get_object 重建成功后 best-effort 回写缺/坏片（含远端 write_shard）
                    let _ = self.get_object(&bucket, &key).await;
                }
            }
        }
        Ok(r)
    }
}

/// 对端某桶的完整键表（分页拉取；`list_page_local` 的 token 语义「含该键本身」→ 用集合去重）。
async fn collect_peer_keys(
    cl: &drives::ClusterConfig,
    peer: &str,
    bucket: &str,
) -> Result<Vec<String>> {
    let mut all: BTreeSet<String> = BTreeSet::new();
    let mut marker: Option<String> = None;
    for _page in 0..100_000 {
        let resp = cl
            .transport
            .list_local(
                peer,
                crate::mesh::ListLocal {
                    bucket: bucket.to_string(),
                    prefix: String::new(),
                    delimiter: None,
                    marker: marker.clone(),
                    limit: 1000,
                },
            )
            .await?;
        let n = resp.entries.len();
        for e in resp.entries {
            marker = Some(e.key.clone());
            all.insert(e.key);
        }
        if n < 1000 {
            break;
        }
    }
    Ok(all.into_iter().collect())
}

/// 目录条目计数（不存在视为 0）。
async fn count_entries(dir: &Path) -> Result<usize> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let mut n = 0;
    while entries.next_entry().await?.is_some() {
        n += 1;
    }
    Ok(n)
}
