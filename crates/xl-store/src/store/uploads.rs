//! ListMultipartUploads 内核：扫描**活着的 MPU 暂存区**（对象目录下含 `mpu.meta`
//! 的子目录），按 (key, upload_id) 全局有序输出 + delimiter 折叠 + 双 marker 分页。
//!
//! M3 同款集群并集：本机页 ∪ 各节点 ListUploadsLocal 页，每源取 `limit = max+1`
//! 个输出；完备性证明与 `list.rs` 头注同构——合并页若截断，任一源若有未返回输出
//! 排序小于截断点，则其前 max+1 个输出都小于截断点，与合并中至多 max−1 个小于
//! 截断点的输出矛盾。故续页 marker 不丢项。
//!
//! 已知代价（recon 记录）：每页 O(对象目录数 × 子目录数) 次 readdir + 每个候选
//! 暂存区一次 mpu.meta 读；与对象数而非 upload 数相关。先对再快（ponytail）。

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result};
use crate::layout;

use super::{Store, UploadInfo, UploadListRequest, UploadListResult};

const DEFAULT_MAX_UPLOADS: usize = 1000;

impl Store {
    /// 列举进行中的 MPU（集群自动扇出）。
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        req: &UploadListRequest,
    ) -> Result<UploadListResult> {
        if self.cfg.cluster.is_some() {
            return self.list_uploads_cluster(bucket, req).await;
        }
        self.ensure_bucket(bucket).await?;
        let max = req
            .max_uploads
            .unwrap_or(DEFAULT_MAX_UPLOADS)
            .min(DEFAULT_MAX_UPLOADS);
        // max-uploads=0：空页且不截断（报截断会让客户端拿同一 marker 空转）
        if max == 0 {
            return Ok(UploadListResult::default());
        }
        let prefix = req.prefix.clone().unwrap_or_default();
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
        let (uploads, cps) = self
            .list_uploads_page_local(
                bucket,
                &prefix,
                delimiter,
                req.key_marker.as_deref(),
                req.upload_id_marker.as_deref(),
                max + 1,
            )
            .await?;
        Ok(merge_uploads_page(max, uploads.into_iter(), cps))
    }

    /// 单源页（单机路径与集群的 ListUploadsLocal 共用）。输出按**全局字典序**
    /// （(key, upload_id)，cp 视作 upload_id 为空）有序，**最多收 `limit` 个输出**
    /// （upload 与 cp 合计；cp 已按集合去重）。
    ///
    /// 分页语义（AWS LMU）：
    /// - key_marker 排他；仅当同时给出 upload_id_marker 时，同键 upload 从 id 严格大于
    ///   marker 处开始（无 key_marker 时 upload_id_marker 被忽略）。
    /// - 带 delimiter 时，`cp <= key_marker` 的 cp 与其折叠的键整体跳过（上页已发）。
    ///
    /// cp 必须在**生成它的键之前**就位：键 "a0" 字典序在 "a/" 之后、生成键 "a/b"
    /// 之前；若边扫描边发 cp，小页会先返回 "a0"，后续页又因 `cp <= key_marker` 跳过
    /// "a/"，前缀永久丢失。故先整表收 cp，再与未折叠键归并扫描——单源页输出是全局
    /// 有序前缀，集群并集完备性证明（见文件头注）依赖这一点。
    pub(in crate::store) async fn list_uploads_page_local(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<UploadInfo>, Vec<String>)> {
        let mut keys = self.collect_keys(bucket).await?;
        keys.sort();

        let mut cp_set: BTreeSet<String> = BTreeSet::new();
        if let Some(d) = delimiter {
            for k in &keys {
                if !k.starts_with(prefix) {
                    continue;
                }
                let rest = &k[prefix.len()..];
                let Some(pos) = rest.find(d) else { continue };
                let cp = format!("{prefix}{}", &rest[..pos + d.len()]);
                if key_marker.is_none_or(|m| cp.as_str() > m) {
                    cp_set.insert(cp);
                }
            }
        }
        let mut cp_iter = cp_set.into_iter().peekable();

        let mut uploads: Vec<UploadInfo> = Vec::new();
        let mut cps: Vec<String> = Vec::new();
        for k in keys {
            if !k.starts_with(prefix) {
                continue;
            }
            // 排在本键之前的 cp 先输出（它们 < k，且不会漏掉生成键更靠后的前缀）。
            while cp_iter.peek().is_some_and(|cp| cp.as_str() < k.as_str()) {
                if uploads.len() + cps.len() >= limit {
                    return Ok((uploads, cps));
                }
                cps.push(cp_iter.next().expect("peek 已判 Some"));
            }
            // V1 marker：严格小于 marker 的键跳过；同键仅当给 upload_id_marker 时续传。
            if key_marker.is_some_and(|m| k.as_str() < m) {
                continue;
            }
            let at_marker_key = key_marker.is_some_and(|m| k.as_str() == m);
            if at_marker_key && upload_id_marker.is_none() {
                continue; // 只给 key_marker：同键 upload 全部跳过（排他）
            }
            if let Some(d) = delimiter
                && k[prefix.len()..].contains(d)
            {
                continue; // 已折叠进 cp（上面按序发出）
            }
            let id_after = if at_marker_key {
                upload_id_marker
            } else {
                None
            };

            let mut by_id: BTreeMap<String, (i64, Option<String>)> = BTreeMap::new();
            for drive in &self.cfg.drives {
                let Ok(obj_dir) = layout::object_dir(drive, bucket, &k) else {
                    continue;
                };
                let mut rd = match tokio::fs::read_dir(&obj_dir).await {
                    Ok(r) => r,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };
                while let Some(ent) = rd.next_entry().await? {
                    if !ent.file_type().await?.is_dir() {
                        continue;
                    }
                    let name = ent.file_name().to_string_lossy().into_owned();
                    // 暂存区目录名 = 规范 UUID；版本目录（xl.meta）在这一步被排除
                    let Ok(id) = uuid::Uuid::parse_str(&name) else {
                        continue;
                    };
                    match super::mpu::mpu_summary(&ent.path()).await {
                        Ok(Some(sum)) => {
                            // M3 MPU 会话粘性：集群创建的暂存区在 mpu.meta 记属主，
                            // LMU 必须重建**客户端可见**的带前缀 upload_id（目录名仍是 UUID）。
                            // 旧格式（owner=None）= 裸 UUID（本地属主）。
                            let id_str = id.to_string();
                            let upload_id = match sum.owner.as_deref() {
                                Some(owner) => super::mpu_route::format_upload_id(owner, &id_str),
                                None => id_str,
                            };
                            by_id
                                .entry(upload_id)
                                .or_insert((sum.created_nanos, sum.storage_class));
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(dir = ?name, error = %e, "跳过损坏的 MPU 暂存区")
                        }
                    }
                }
            }
            for (upload_id, (initiated_nanos, storage_class)) in by_id {
                if id_after.is_some_and(|m| upload_id.as_str() <= m) {
                    continue;
                }
                if uploads.len() + cps.len() >= limit {
                    return Ok((uploads, cps));
                }
                uploads.push(UploadInfo {
                    key: k.clone(),
                    upload_id,
                    initiated_nanos,
                    storage_class,
                });
            }
        }
        // 尾部 cp：排在所有键之后。
        for cp in cp_iter {
            if uploads.len() + cps.len() >= limit {
                break;
            }
            cps.push(cp);
        }
        Ok((uploads, cps))
    }

    /// 集群列举：本机页 ∪ 各节点页；远端不可达 warn 跳过（少一路并集）。
    async fn list_uploads_cluster(
        &self,
        bucket: &str,
        req: &UploadListRequest,
    ) -> Result<UploadListResult> {
        // DP-0：远端页传输错会被「少一路并集」吞掉 → 混版下必须先 fail-closed。
        self.ensure_data_plane_compatible().await?;
        let cl = self.cfg.cluster.as_deref().expect("cluster 分支");
        let self_id = cl.sets.flat.self_node().id.clone();

        // 桶存在性走扇出口径（本机可能曾掉线错过桶目录——懒自愈前先别 404）
        if !self.bucket_exists(bucket).await? {
            return Err(Error::NoSuchBucket(bucket.to_string()));
        }
        let max = req
            .max_uploads
            .unwrap_or(DEFAULT_MAX_UPLOADS)
            .min(DEFAULT_MAX_UPLOADS);
        if max == 0 {
            return Ok(UploadListResult::default());
        }
        let prefix = req.prefix.clone().unwrap_or_default();
        let delimiter = req.delimiter.as_deref().filter(|d| !d.is_empty());
        let limit = max + 1; // 完备性的关键（见头注证明）

        let (local_uploads, local_cps) = match self
            .list_uploads_page_local(
                bucket,
                &prefix,
                delimiter,
                req.key_marker.as_deref(),
                req.upload_id_marker.as_deref(),
                limit,
            )
            .await
        {
            Ok(x) => x,
            Err(Error::NoSuchBucket(_)) => (Vec::new(), Vec::new()),
            Err(e) => return Err(e),
        };
        let mut uploads: BTreeMap<(String, String), UploadInfo> = BTreeMap::new();
        for u in local_uploads {
            uploads.insert((u.key.clone(), u.upload_id.clone()), u);
        }
        let mut cps = local_cps;

        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            let l = crate::mesh::ListUploadsLocal {
                bucket: bucket.into(),
                prefix: prefix.clone(),
                delimiter: delimiter.map(|d| d.to_string()),
                key_marker: req.key_marker.clone(),
                upload_id_marker: req.upload_id_marker.clone(),
                limit,
            };
            match cl.transport.list_uploads_local(&n.id, l).await {
                Ok(resp) => {
                    for e in resp.uploads {
                        let sc = if e.storage_class.is_empty() {
                            None
                        } else {
                            Some(e.storage_class)
                        };
                        uploads
                            .entry((e.key.clone(), e.upload_id.clone()))
                            .or_insert(UploadInfo {
                                key: e.key,
                                upload_id: e.upload_id,
                                initiated_nanos: e.initiated_nanos,
                                storage_class: sc,
                            });
                    }
                    for cp in resp.prefixes {
                        if !cps.contains(&cp) {
                            cps.push(cp);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(node = %n.id, error = %e, "远端 MPU 列举不可达（少一路并集）")
                }
            }
        }

        Ok(merge_uploads_page(max, uploads.into_values(), cps))
    }
}

/// 归并两路有序流（upload 按 (key, upload_id)、cp 按 cp 串），取前 `max` 个输出；
/// 截断时 NextKeyMarker/NextUploadIdMarker = **最后一个返回项**（cp 只带 key marker）。
fn merge_uploads_page(
    max: usize,
    uploads: impl Iterator<Item = UploadInfo>,
    cps: Vec<String>,
) -> UploadListResult {
    enum Out<'a> {
        Up(&'a UploadInfo),
        Cp(&'a str),
    }
    let uploads: Vec<_> = uploads.collect();
    let mut seq: Vec<(String, String, Out<'_>)> = uploads
        .iter()
        .map(|u| (u.key.clone(), u.upload_id.clone(), Out::Up(u)))
        .collect();
    seq.extend(
        cps.iter()
            .map(|cp| (cp.clone(), String::new(), Out::Cp(cp))),
    );
    seq.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));

    let mut result = UploadListResult::default();
    let mut last: Option<(String, Option<String>)> = None;
    for (key, upload_id, out) in seq {
        if result.uploads.len() + result.common_prefixes.len() >= max {
            result.is_truncated = true;
            if let Some((k, id)) = last {
                result.next_key_marker = Some(k);
                result.next_upload_id_marker = id;
            }
            break;
        }
        match out {
            Out::Up(u) => {
                last = Some((key, Some(upload_id)));
                result.uploads.push(u.clone());
            }
            Out::Cp(cp) => {
                last = Some((key, None));
                result.common_prefixes.push(cp.to_string());
            }
        }
    }
    result.common_prefixes.sort();
    result
}
