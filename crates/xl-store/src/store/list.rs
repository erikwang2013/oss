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
        let (objs, cps) = self
            .list_page_local(
                bucket,
                &prefix,
                req.continuation_token.as_deref(),
                delimiter,
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
    pub(in crate::store) async fn list_page_local(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        delimiter: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<(crate::mesh::ListEntry, ObjectInfo)>, Vec<String>)> {
        let mut keys = self.collect_keys(bucket).await?;
        keys.sort();

        let mut objects: Vec<(crate::mesh::ListEntry, ObjectInfo)> = Vec::new();
        let mut cps: Vec<String> = Vec::new();
        for k in keys {
            if !k.starts_with(prefix) {
                continue;
            }
            if let Some(after) = start_after {
                // token = 本页「未返回的第一个 key」，恢复时包含该 key 本身
                if k.as_str() < after {
                    continue;
                }
            }
            if let Some(d) = delimiter {
                let rest = &k[prefix.len()..];
                if let Some(pos) = rest.find(d) {
                    let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
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
                if m.latest_is_marker() {
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
        let start_after = req.continuation_token.clone();
        let limit = max + 1; // 完备性的关键（见头注证明）

        // 本机页（本机无桶目录 → 空页）
        let (local_objs, local_cps) = match self
            .list_page_local(bucket, &prefix, start_after.as_deref(), delimiter, limit)
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

        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            let l = crate::mesh::ListLocal {
                bucket: bucket.into(),
                prefix: prefix.clone(),
                delimiter: delimiter.map(|d| d.to_string()),
                marker: start_after.clone(),
                limit,
            };
            match cl.transport.list_local(&n.id, l).await {
                Ok(resp) => {
                    for e in resp.entries {
                        match objs.get(&e.key) {
                            Some((cur, _)) if cur.max_mod_time_nanos >= e.max_mod_time_nanos => {}
                            _ => {
                                objs.insert(e.key.clone(), (e, None));
                            }
                        }
                    }
                    for cp in resp.prefixes {
                        if !cps.contains(&cp) {
                            cps.push(cp);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(node = %n.id, error = %e, "远端列举不可达（少一路并集）")
                }
            }
        }

        Ok(merge_page(max, objs.into_values(), cps))
    }

    // 可见性等价于原先在 crate::store 里的私有：store 子树（含 store::tests）可用，子树外不可见。
    /// 评审 R3：各盘取并集（drive0 掉线不得让列举报 NoSuchBucket/空）。任一盘读到桶目录即算
    /// 「见过」；全盘 NotFound = 桶被并发删除的窗口 → NoSuchBucket（而不是把裸 IO 错误抛给上层）。
    pub(in crate::store) async fn collect_keys(&self, bucket: &str) -> Result<Vec<String>> {
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
                // 杂物目录（手工塞入、崩溃残留）解出的键非法：跳过并告警，不能让一个目录名
                // 把整桶列举打挂；真实对象的目录名由 object_dir_name 生成，必过同一套校验。
                match layout::key_from_dir_name(&name) {
                    Ok(k) => {
                        keys.insert(k);
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
