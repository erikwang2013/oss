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
//!     从此有本；分片补建靠读时修兜底，**批量 = `heal` 命令**（M4 切片 4 已落地））；
//!   - 桶目录比对：对端有而本机无的桶 → `missing_bucket_dirs`（fsck 报告项；`heal` 补建）。
//!
//! **裁决已兑现（M4 切片 4）**：独立 `scrub`（逐块对偶校验的深度巡检）与 `heal`（批量补建）
//! 已落地；本命令保持「整片 CRC 快检 + 最小修复」定位，
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
    /// 集群：对端存在而本机无桶目录的桶（fsck 报告项；`heal` 负责补建）
    pub missing_bucket_dirs: Vec<String>,
    /// 集群 repair：补建 meta 的键数
    pub repaired_meta_keys: usize,
    /// heal：补建（本地）的桶目录数
    pub repaired_bucket_dirs: usize,
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

        // b)+c) 命名空间（fsck 与 scrub 共用收集）
        let (keys, local_buckets) = self.collect_namespace().await?;

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

            // c) 分片巡检（M4 切片 4：**全链覆盖**——历史版本与最新版一视同仁，marker 跳过；
            //    旧 parts 版本校验文件存在与长度）
            let Some(chain) = self.read_meta(&bucket, &key).await.ok().flatten() else {
                continue;
            };
            let last_idx = chain.versions.len().saturating_sub(1);
            for (idx, v) in chain.versions.iter().enumerate() {
                if v.delete_marker() {
                    continue; // 无数据
                }
                let mut miss = 0usize;
                let mut bad = 0usize;
                if let Some(l) = &v.data_layout {
                    let uuid = v.part.uuid.clone();
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
                                    offset: 0,
                                    length: 0, // 整片（fsck 全量校验语义）
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
                } else if v.inline.is_none() {
                    // 旧 parts（ec.enabled=false 遗留形态）：文件齐 + 长度对。
                    // 判定按 part 独立（verifier 观察项 a：bad 跨 part 累积会让后续 part
                    // 的缺片被抑制、报表少计）
                    for part in v.effective_parts() {
                        let mut found = false;
                        let mut part_bad = false;
                        for drive in &self.cfg.drives {
                            let Ok(dir) = layout::object_dir(drive, &bucket, &key) else {
                                continue;
                            };
                            let path = dir.join(&part.uuid).join(format!("part.{}", part.part));
                            match tokio::fs::metadata(&path).await {
                                Ok(md) if md.len() == part.size => {
                                    found = true;
                                    break;
                                }
                                Ok(_) => part_bad = true,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => return Err(e.into()),
                            }
                        }
                        if part_bad {
                            bad += 1;
                        } else if !found {
                            miss += 1;
                        }
                    }
                }
                r.missing_shards += miss;
                r.bad_shards += bad;
                if repair && (miss > 0 || bad > 0) {
                    // 逐版本修复：最新版走 get_object（含整读 SHA/读时修）；历史版本点名读同样
                    // 触发 read_ec_range 的读时修补写（版本目录不可变，缺失片只可能是掉线期欠账）
                    if idx == last_idx {
                        let _ = self.get_object(&bucket, &key).await;
                    } else {
                        let _ = self.read_version_data(&bucket, &key, v, None).await;
                    }
                }
            }
        }
        Ok(r)
    }

    /// heal（M4 切片 4）：fsck(repair=true) 的强化——在删孤儿/读时修/缺键补 meta 之外，
    /// 把集群差集报出的**缺失桶目录**在本机各盘补建（`missing_bucket_dirs` 闭环）。
    pub async fn heal(&self) -> Result<FsckReport> {
        let mut r = self.fsck(true).await?;
        // 掉线盘（挂载点被遮）上 create_dir_all 会落父 fs 且返回 Ok——假成功（verifier 观察项 b：
        // 不探活会把它移出 missing 列表）。任一配置盘不在册 → 本轮不补，保留在「仍缺」清单。
        let mut any_down = false;
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                any_down = true;
                break;
            }
        }
        let mut still_missing: Vec<String> = Vec::new();
        for b in std::mem::take(&mut r.missing_bucket_dirs) {
            if any_down {
                tracing::warn!(bucket = %b, "heal：有盘不在册，跳过桶目录补建（保留在仍缺清单）");
                still_missing.push(b);
                continue;
            }
            match self.mesh_ensure_bucket(&b).await {
                Ok(()) => r.repaired_bucket_dirs += 1,
                Err(e) => {
                    tracing::warn!(bucket = %b, error = %e, "heal：桶目录补建失败");
                    still_missing.push(b);
                }
            }
        }
        // 列表语义 = 「补建后仍缺」（补建成功的移出，免误读为还缺）
        r.missing_bucket_dirs = still_missing;
        Ok(r)
    }

    /// scrub（M4 切片 4）：全量深度巡检（**只读**，不修）。对每个 EC 版本逐块读取 +
    /// 逐块 12×W 对偶校验（沿用读路径的坏片定位），**不组装整份 payload**（常量内存）。
    /// marker 跳过；inline 仅自洽校验；旧 parts 校验文件存在与长度。
    /// 与 fsck 的分工：fsck = 整片 CRC 计数（快）；scrub = 块级深度验证（慢、能定位坏片）。
    pub async fn scrub(&self) -> Result<ScrubReport> {
        let mut rep = ScrubReport {
            drives: self.cfg.drives.len(),
            ..Default::default()
        };
        let mut peers_up: Vec<String> = Vec::new();
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.topo.self_node().id.clone();
            for n in &cl.topo.nodes {
                if n.id == self_id {
                    continue;
                }
                match cl.transport.health(&n.id).await {
                    Ok(h) if h.node_id == n.id => peers_up.push(n.id.clone()),
                    _ => rep.unreachable_nodes.push(n.id.clone()),
                }
            }
        }
        let (keys, _buckets) = self.collect_namespace().await?;
        for (bucket, key) in keys {
            let m = match self.read_meta(&bucket, &key).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(e) => {
                    rep.corrupt_meta += 1;
                    tracing::warn!(bucket = %bucket, key = %key, error = %e, "scrub：meta 读取失败");
                    continue;
                }
            };
            rep.keys_scanned += 1;
            for v in &m.versions {
                if v.delete_marker() {
                    continue;
                }
                rep.versions_scanned += 1;
                let vid = v.version_id_str();
                let Some(l) = &v.data_layout else {
                    // inline：解码边界已验长度上限，这里再对 size 一致性
                    if let Some(inline) = &v.inline {
                        if inline.bytes.len() as u64 != v.size {
                            rep.failed_versions
                                .push(format!("{bucket}/{key}@{vid}: 内联长度不符"));
                        } else {
                            rep.ok_versions += 1;
                        }
                        continue;
                    }
                    // 旧 parts（ec.enabled=false 遗留形态）：文件存在 + 长度校（注释口径兑现）
                    let mut parts_ok = true;
                    for part in v.effective_parts() {
                        let mut found = false;
                        for drive in &self.cfg.drives {
                            let Ok(dir) = layout::object_dir(drive, &bucket, &key) else {
                                continue;
                            };
                            let path = dir.join(&part.uuid).join(format!("part.{}", part.part));
                            match tokio::fs::metadata(&path).await {
                                Ok(md) if md.len() == part.size => {
                                    found = true;
                                    break;
                                }
                                Ok(_) => {}
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => return Err(e.into()),
                            }
                        }
                        if !found {
                            rep.missing_shards += 1;
                            parts_ok = false;
                        }
                    }
                    if parts_ok {
                        rep.ok_versions += 1;
                    } else {
                        rep.failed_versions
                            .push(format!("{bucket}/{key}@{vid}: 旧 parts 文件缺失或长度不符"));
                    }
                    continue;
                };
                let cfg = ec::EcConfig {
                    data: l.data,
                    parity: l.parity,
                    ..Default::default()
                };
                let ss = l.shard_size;
                let blocks = ss.div_ceil(ec::CHUNK_W as u64);
                let mut reader = match ec::ChunkReader::new(cfg) {
                    Ok(x) => x,
                    Err(e) => {
                        rep.failed_versions
                            .push(format!("{bucket}/{key}@{vid}: {e}"));
                        continue;
                    }
                };
                let mut v_missing = 0usize;
                let mut v_bad = 0usize;
                let mut fail_reason: Option<String> = None;
                'blocks: for t in 0..blocks {
                    let mut chunks: Vec<Option<Vec<u8>>> = Vec::with_capacity(l.shards.len());
                    for (i, s) in l.shards.iter().enumerate() {
                        match self.read_shard_block(&bucket, &key, v, s, i, t, ss).await? {
                            Some(b) => chunks.push(Some(b)),
                            None => {
                                v_missing += 1;
                                chunks.push(None);
                            }
                        }
                    }
                    let present = chunks.iter().filter(|c| c.is_some()).count();
                    if present < cfg.data as usize {
                        fail_reason = Some(format!("块 {t} 存活 {present} < data"));
                        break 'blocks;
                    }
                    match reader.decode_block(&mut chunks) {
                        Ok(Some(_)) => v_bad += 1, // 定位到坏片（块级）——计数并继续
                        Ok(None) => {}
                        Err(e) => {
                            fail_reason = Some(format!("块 {t}: {e}"));
                            break 'blocks;
                        }
                    }
                }
                rep.missing_shards += v_missing;
                rep.bad_shards += v_bad;
                match fail_reason {
                    Some(why) => rep
                        .failed_versions
                        .push(format!("{bucket}/{key}@{vid}: {why}")),
                    None => rep.ok_versions += 1,
                }
            }
        }
        Ok(rep)
    }

    /// 命名空间收集（fsck 与 scrub 共用）：全盘收集桶与 (bucket,key) 键集（掉线盘跳过）。
    pub(in crate::store) async fn collect_namespace(
        &self,
    ) -> Result<(BTreeSet<(String, String)>, BTreeSet<String>)> {
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
        Ok((keys, local_buckets))
    }
}

/// scrub 报告（M4 切片 4；JSON 落盘口径同 fsck）。
#[derive(Debug, Default, Serialize)]
pub struct ScrubReport {
    pub drives: usize,
    pub keys_scanned: usize,
    pub versions_scanned: usize,
    pub ok_versions: usize,
    /// 深度校验失败的版本（键@版本: 原因）
    pub failed_versions: Vec<String>,
    /// 缺失分块数（文件不存在/短读/对端不可达视同缺失）
    pub missing_shards: usize,
    /// 定位到坏片的块数
    pub bad_shards: usize,
    pub corrupt_meta: usize,
    pub unreachable_nodes: Vec<String>,
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
