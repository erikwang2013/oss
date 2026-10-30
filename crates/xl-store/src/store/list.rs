//! 列举机制：桶目录扫描 + ListObjectsV2 的过滤/分页（`Store` 的第二个 impl 块，
//! 从 store.rs 拆出；pub 面不变）。
//!
//! M3（Task 6 / D4）：集群列举 = 「本机页 ∪ 各节点 ListLocal 页」的**键并集**，
//! 每源页取 `limit = max_keys + 1` 个输出。**完备性证明**（写进注释防误改）：设合并页
//! 截断于 K（合并后第 max 个输出之后那个输出的排序键）。任一源已返回其前 max+1 个输出；
//! 若该源有未返回输出其键 < K，则它的前 max+1 个输出全 < K，而合并中 < K 的输出至多
//! max−1 个——矛盾。故所有未返回输出 > K，`next_token = K` 续页不丢键。
//! 同键条目取 `max_mod_time_nanos` 最大者（单调 mod_time ⇒ 其持有者的 meta 即已提交
//! 版本，尺寸/ETag 直接可用——**不做逐键 meta RPC**，避免扇出风暴）。

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::layout;

use super::{ListRequest, ListResult, ObjectInfo, Store};

const DEFAULT_MAX_KEYS: usize = 1000;

impl Store {
    /// 列举：直接走盘。M1 每个对象读一次 xl.meta（O(n) 次系统调用），
    /// M4 用 scrub 的命名空间索引优化（ponytail: 先对，再快）。
    pub async fn list_objects(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        if self.cfg.cluster.is_some() {
            return self.list_objects_cluster(bucket, req).await;
        }
        self.ensure_bucket(bucket).await?;
        let max = req.max_keys.unwrap_or(DEFAULT_MAX_KEYS).min(1000);
        // max-keys=0：空页且不截断（报截断会让客户端拿同一个 token 空转）
        if max == 0 {
            return Ok(ListResult::default());
        }
        // 空 delimiter 视同未指定：find("") 命中每个键的首字节，会把整份键都当 common prefix
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
        let prefix = req.prefix.clone().unwrap_or_default();
        // V1 marker 排他（严格大于）；V2 token 包含起点。两者互斥，marker 优先。
        let (start_after, exclusive) = match req.marker.as_deref() {
            Some(m) => (Some(m), true),
            None => (req.continuation_token.as_deref(), false),
        };
        let (objs, cps) = self
            .list_page_local(
                bucket,
                &prefix,
                start_after,
                exclusive,
                delimiter,
                false,
                max + 1,
            )
            .await?;
        Ok(merge_page(
            max,
            objs.into_iter().map(|(e, info)| (e, Some(info))),
            cps,
        ))
    }

    /// 单源页（单机路径与集群的 ListLocal 共用）：按 key 序遍历本机键集，
    /// **最多收 `limit` 个输出**（键与 cp 合计；cp 已发不占位也不触发截断——与
    /// M1 单机循环同规则）。条目自带每键本地 max_mod_time（并集判定用）。
    /// `exclusive`：start_after 为 V1 marker（跳过该键本身）而非 V2 token（包含）。
    #[allow(clippy::too_many_arguments)] // 列举四选项 + 分页/版本显式传参，内部函数
    pub(in crate::store) async fn list_page_local(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        exclusive: bool,
        delimiter: Option<&str>,
        include_deleted: bool,
        limit: usize,
    ) -> Result<(Vec<(crate::mesh::ListEntry, ObjectInfo)>, Vec<String>)> {
        // 先按 prefix/marker 过滤再收集：大桶里按前缀翻页时不再把整桶键集读进内存。
        let keys = self
            .collect_keys_filtered(bucket, prefix, start_after, exclusive, true)
            .await?;

        let mut objects: Vec<(crate::mesh::ListEntry, ObjectInfo)> = Vec::new();
        let mut cps: Vec<String> = Vec::new();
        for k in keys {
            if !k.starts_with(prefix) {
                continue;
            }
            if let Some(after) = start_after {
                // V2 token = 本页「未返回的第一个 key」，恢复时包含该 key 本身；
                // V1 marker = 严格大于（AWS：start listing after this key）。
                if k.as_str() < after || (exclusive && k.as_str() == after) {
                    continue;
                }
            }
            if let Some(d) = delimiter {
                let rest = &k[prefix.len()..];
                if let Some(pos) = rest.find(d) {
                    let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
                    // V1：CommonPrefixes 不返回字典序 <= marker 的项（AWS 语义）；这些键
                    // 已折叠进被过滤的 cp，不能再作为对象返回。
                    if exclusive && start_after.is_some_and(|m| cp.as_str() <= m) {
                        continue;
                    }
                    // 去重检查在容量判断之前：已发过的 cp 不再占位、也不触发截断
                    if !cps.contains(&cp) {
                        if objects.len() + cps.len() >= limit {
                            break;
                        }
                        cps.push(cp);
                    }
                    continue;
                }
            }
            if objects.len() + cps.len() >= limit {
                break;
            }
            if let Some(m) = self.read_meta(bucket, &k).await? {
                // 版本控制：链尾为 delete marker 的键在对象列表不可见（版本列表仍可见）
                if !include_deleted && m.latest_is_marker() {
                    continue;
                }
                let t = m.latest().map_or(i64::MIN, |v| v.mod_time_nanos);
                let info = self.load_object_info(bucket, &k, &m)?;
                objects.push((
                    crate::mesh::ListEntry {
                        key: k.clone(),
                        size: info.size,
                        etag: info.etag.clone(),
                        max_mod_time_nanos: t,
                    },
                    info,
                ));
            }
        }
        Ok((objects, cps))
    }

    /// 集群列举（D4）：本机页 ∪ 各节点 ListLocal 页；远端不可达 warn 跳过（少一路并集）。
    ///
    /// DP-1 混版：legacy peer 忽略 `marker_exclusive`（V1 排他 marker）——这类请求改发
    /// 包含式 marker，并在合并前丢弃 <= marker 的输出（不重复边界、marker 必前进），
    /// 同时置 `degraded_marker` 供 S3 回降级头。未知 wire / 拓扑不一致仍 fail-closed
    /// （不得静默给出不完整并集）；能力探测失败不得把 PeerIncompatible 吞成「不可达」。
    async fn list_objects_cluster(&self, bucket: &str, req: &ListRequest) -> Result<ListResult> {
        let cl = self.cfg.cluster.as_deref().expect("cluster 分支");
        let self_id = cl.sets.flat.self_node().id.clone();

        // 桶存在性走扇出口径（本机可能曾掉线错过桶目录——懒自愈前先别 404）
        if !self.bucket_exists(bucket).await? {
            return Err(Error::NoSuchBucket(bucket.to_string()));
        }
        let max = req.max_keys.unwrap_or(DEFAULT_MAX_KEYS).min(1000);
        if max == 0 {
            return Ok(ListResult::default());
        }
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
        let prefix = req.prefix.clone().unwrap_or_default();
        // 与单机同一套互斥语义：V1 marker 排他、V2 token 包含
        let (start_after, exclusive) = match req.marker.clone() {
            Some(m) => (Some(m), true),
            None => (req.continuation_token.clone(), false),
        };
        let limit = max + 1; // 完备性的关键（见头注证明）

        // 本机页（本机无桶目录 → 空页）
        let (local_objs, local_cps) = match self
            .list_page_local(
                bucket,
                &prefix,
                start_after.as_deref(),
                exclusive,
                delimiter,
                false,
                limit,
            )
            .await
        {
            Ok(x) => x,
            Err(Error::NoSuchBucket(_)) => (Vec::new(), Vec::new()),
            Err(e) => return Err(e),
        };
        // key → (entry, 本地全量信息 Option)；同键取 max_mod_time 最大的条目
        let mut objs: BTreeMap<String, (crate::mesh::ListEntry, Option<ObjectInfo>)> =
            BTreeMap::new();
        for (e, info) in local_objs {
            objs.insert(e.key.clone(), (e, Some(info)));
        }
        let mut cps: Vec<String> = local_cps;
        let local_caps = self.local_caps();
        let mut degraded_marker = false;
        let mut incomplete = false;
        // V1 排他页：远端可能按包含式返回 marker 本身（旧端忽略 marker_exclusive，或
        // 探测失败被当成现版）。多取一条再过滤 <= marker 的输出，既让 marker 必前进，
        // 又保住「每源先返回 max+1 个 > marker 输出」的完备性前提（见头注证明）。
        let remote_limit = limit + usize::from(exclusive);
        let after = if exclusive {
            start_after.as_deref()
        } else {
            None
        };

        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            // 探测失败 = 不可达：V1 marker 页可能被旧端按包含式处理 → 显式降级；
            // 请求仍按现版语义发出（未知不得当旧版），实际 RPC 错误另行处理。
            let legacy = match cl.transport.peer_caps(&n.id).await {
                Ok(caps) => matches!(
                    crate::mesh_gate::check_peer(&n.id, &caps, &local_caps)?,
                    crate::mesh_gate::PeerClass::Legacy
                ),
                Err(_) => {
                    if exclusive {
                        degraded_marker = true;
                    }
                    false
                }
            };
            if legacy && exclusive {
                degraded_marker = true;
            }
            let l = crate::mesh::ListLocal {
                bucket: bucket.into(),
                prefix: prefix.clone(),
                delimiter: delimiter.map(|d| d.to_string()),
                marker: start_after.clone(),
                marker_exclusive: exclusive && !legacy,
                limit: remote_limit,
                include_deleted: false,
            };
            match cl.transport.list_local(&n.id, l).await {
                Ok(resp) => {
                    for e in resp.entries {
                        // 排他 marker 页的并集不得含 <= marker 的输出（重复会让客户端原地打转）。
                        if after.is_some_and(|m| e.key.as_str() <= m) {
                            continue;
                        }
                        match objs.get(&e.key) {
                            Some((cur, _)) if cur.max_mod_time_nanos >= e.max_mod_time_nanos => {}
                            _ => {
                                objs.insert(e.key.clone(), (e, None));
                            }
                        }
                    }
                    for cp in resp.prefixes {
                        if after.is_some_and(|m| cp.as_str() <= m) {
                            continue;
                        }
                        if !cps.contains(&cp) {
                            cps.push(cp);
                        }
                    }
                }
                // 能力闸门在第二次探测才发现旧版：不得当成「不可达」吞掉整路（静默缺项）。
                Err(e @ Error::PeerIncompatible(_)) => return Err(e),
                Err(e) => {
                    // 丢一路并集：V1 marker 页必须让客户端看到降级（头由 S3 层回）；
                    // 只读聚合还需知道结果是下界（incomplete）。
                    incomplete = true;
                    if exclusive {
                        degraded_marker = true;
                    }
                    tracing::warn!(node = %n.id, error = %e, "远端列举不可达（少一路并集）")
                }
            }
        }

        let mut result = merge_page(max, objs.into_values(), cps);
        result.degraded_marker = degraded_marker;
        result.incomplete = incomplete;
        Ok(result)
    }

    // 可见性等价于原先在 crate::store 里的私有：store 子树（含 store::tests）可用，子树外不可见。
    /// 评审 R3：各盘取并集（drive0 掉线不得让列举报 NoSuchBucket/空）。任一盘读到桶目录即算
    /// 「见过」；全盘 NotFound = 桶被并发删除的窗口 → NoSuchBucket（而不是把裸 IO 错误抛给上层）。
    pub(in crate::store) async fn collect_keys(&self, bucket: &str) -> Result<Vec<String>> {
        self.collect_keys_filtered(bucket, "", None, false, false)
            .await
    }

    /// 带 prefix/marker 预过滤的键集。prefix 先展平成目录名前缀再比较，未命中的目录
    /// 不进入 BTreeSet——大桶按前缀翻页时内存从 O(整桶) 降到 O(匹配前缀)。
    /// 根前缀（`""`）仍是 O(整桶)；彻底解决需要命名空间索引（独立架构项）。
    /// 另有硬上限（`OSS_LIST_MAX_KEYS`，默认 100 万，0=不限）防止单请求 OOM。
    pub(in crate::store) async fn collect_keys_filtered(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        exclusive: bool,
        apply_cap: bool,
    ) -> Result<Vec<String>> {
        static MAX_KEYS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let max_keys = *MAX_KEYS.get_or_init(|| {
            std::env::var("OSS_LIST_MAX_KEYS")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(1_000_000)
        });
        let dir_prefix = prefix.replace('/', layout::SEP);
        let mut keys = std::collections::BTreeSet::new();
        let mut seen = false;
        for drive in &self.cfg.drives {
            let mut entries = match tokio::fs::read_dir(layout::bucket_dir(drive, bucket)).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            seen = true;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with(&dir_prefix) {
                    continue;
                }
                // 杂物目录（手工塞入、崩溃残留）解出的键非法：跳过并告警，不能让一个目录名
                // 把整桶列举打挂；真实对象的目录名由 object_dir_name 生成，必过同一套校验。
                match layout::key_from_dir_name(&name) {
                    Ok(k) => {
                        if let Some(after) = start_after
                            && (k.as_str() < after || (exclusive && k.as_str() == after))
                        {
                            continue;
                        }
                        keys.insert(k);
                        if apply_cap && max_keys != 0 && keys.len() > max_keys {
                            return Err(Error::InvalidArgument(format!(
                                "桶 {bucket} 的列举键数超过上限 {max_keys}；请使用 prefix 缩小范围，\
                                 或调大 OSS_LIST_MAX_KEYS（会占用更多内存）"
                            )));
                        }
                    }
                    Err(e) => tracing::warn!(dir = ?name, error = %e, "跳过非法对象目录"),
                }
            }
        }
        if !seen {
            return Err(Error::NoSuchBucket(bucket.to_string()));
        }
        Ok(keys.into_iter().collect())
    }
}

/// 归并两路有序流（对象按 key、cp 按 cp 串——排序等价于键流序，见头注）并按单机
/// 截断规则取前 `max` 个输出；截断时 `next_token` = 被拒输出的排序键（续页含该键）。
fn merge_page(
    max: usize,
    objects: impl Iterator<Item = (crate::mesh::ListEntry, Option<ObjectInfo>)>,
    cps: Vec<String>,
) -> ListResult {
    enum Out<'a> {
        Obj(&'a crate::mesh::ListEntry, &'a Option<ObjectInfo>),
        Cp(&'a str),
    }
    let objects: Vec<_> = objects.collect();
    let mut seq: Vec<(String, Out<'_>)> = objects
        .iter()
        .map(|(e, info)| (e.key.clone(), Out::Obj(e, info)))
        .collect();
    seq.extend(cps.iter().map(|cp| (cp.clone(), Out::Cp(cp.as_str()))));
    seq.sort_by(|a, b| a.0.cmp(&b.0));

    let mut result = ListResult::default();
    for (sort_key, out) in seq {
        if result.objects.len() + result.common_prefixes.len() >= max {
            result.is_truncated = true;
            result.next_token = Some(sort_key);
            break;
        }
        match out {
            Out::Obj(e, info) => {
                let info = match info {
                    Some(i) => i.clone(),
                    // 远端独有条目：无全量信息，按并集条目的字段构造（content_type/user_metadata
                    // 缺失——该键本机曾错过落位，控制台展示走同样字段）
                    None => ObjectInfo {
                        key: e.key.clone(),
                        size: e.size,
                        etag: e.etag.clone(),
                        content_type: None,
                        last_modified_nanos: e.max_mod_time_nanos,
                        user_metadata: Default::default(),
                        checksum: None,
                        version_id: None,
                        // 并集降级条目：无全量信息（切片 3 的 SSE 头仅由 head/get 面提供）
                        encryption: None,
                        lock: None,
                        replication_status: None,
                        remote_version_id: None,
                        // 并集降级条目无全量信息（标签只在 Get/Head/GetTagging 面）
                        tags: Vec::new(),
                        storage_class: None,
                        tier_phase: None,
                        restore_expires_at: None,
                        part_sizes: None,
                    },
                };
                result.objects.push(info);
            }
            Out::Cp(cp) => result.common_prefixes.push(cp.to_string()),
        }
    }
    result.common_prefixes.sort();
    result
}
