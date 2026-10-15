//! 版本控制（M4 切片 3）：delete marker、指定版本读、版本枚举。
//!
//! 桶状态（Off/Enabled）由 S3 层从 `buckets.versioning` 读出后**显式传入**——store 不查库
//! （与 MeshTransport 注入同一条信任边界原则）。链约定：`XlMeta.versions` 追加序
//! （链尾 = 最新）；版本控制桶的历史版本目录不可变（写路径不回收、删路径进 trash），
//! 因此历史版本读没有并发覆写窗口。
//!
//! 明确边界（v1）：
//! - PutBucketVersioning 只接受 Off→Enabled（单向）；Suspended 由 S3 层拒绝（记档）。
//! - `ListObjectVersions` 集群路径 = 本机键集 ∪ 各 peer 首页（limit=max+1 完备性证明
//!   同对象列表）；每键的**完整链**由 cluster 感知的 `read_meta` 取（逐键一次，低频运维面，
//!   与对象列表的"零逐键 RPC"口径在此有意放宽——量级上来再做分页 RPC）。

use bytes::Bytes;
use std::ops::Range as StdRange;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{PartRef, Version, XlMeta};

use super::{ObjectInfo, Store};

/// [`Store::delete_object_versioned`] 的结果（S3 DeleteObject 响应字段来源）。
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteOutcome {
    /// 版本控制桶无版本号删除：追加了 delete marker
    Marker { version_id: String },
    /// 点名删除了某版本（含 marker）
    Version { version_id: String },
    /// 非版本桶：整键删除（M2b 语义）
    Object,
}

/// 版本枚举条目（ListObjectVersions 行）。
#[derive(Debug, Clone, PartialEq)]
pub struct VersionEntry {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub is_marker: bool,
    pub size: u64,
    pub etag: String,
    pub mod_time_nanos: i64,
}

/// [`Store::list_object_versions`] 的单页结果。
#[derive(Debug, Default)]
pub struct VersionList {
    pub entries: Vec<VersionEntry>,
    pub is_truncated: bool,
    /// 下一页续点 = 本页最后一条的 (key, version_id)（S3 的 NextKeyMarker/NextVersionIdMarker 语义）
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

impl Store {
    /// 版本感知删除（M4 切片 3）。
    /// - `version_id=None` 且 `versioned`：追加 delete marker——**键不存在也创建**（S3 语义）。
    /// - `version_id=None` 且非版本桶：走 M2b 整键删除。
    /// - `version_id=Some`：从链中移除该版本（重写 meta = 提交点；该版本目录 → trash）。
    ///   版本/键不存在 = 幂等成功（S3 语义）；链清空 → 整键清理。
    pub async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioned: bool,
    ) -> Result<DeleteOutcome> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;

        let Some(vid) = version_id else {
            if !versioned {
                self.purge_key(bucket, key).await?;
                return Ok(DeleteOutcome::Object);
            }
            // 追加 delete marker（无数据目录；链尾哨兵）
            let old = self.read_meta(bucket, key).await?; // 损坏 fail-closed（同写路径口径）
            let now = match old.as_ref().and_then(|m| m.latest()) {
                Some(v) => super::now_nanos().max(v.mod_time_nanos + 1),
                None => super::now_nanos(),
            };
            let uuid = uuid::Uuid::new_v4().to_string();
            let marker = Version {
                version_id: uuid::Uuid::parse_str(&uuid).expect("自造 uuid"),
                mod_time_nanos: now,
                size: 0,
                etag: String::new(),
                content_type: None,
                user_metadata: Default::default(),
                part: PartRef {
                    uuid,
                    part: 1,
                    size: 0,
                    sha256: String::new(),
                },
                parts: None,
                checksum: None,
                data_layout: None,
                inline: None,
                is_delete_marker: Some(true),
                is_latest: None,
                // NTP 回拨收口：marker 也是新写版本，epoch 同样单调 +1
                epoch: Some(old.as_ref().map_or(0, |m| m.max_epoch()) + 1),
            };
            let marker_vid = marker.version_id_str();
            let mut m = match old.as_ref() {
                Some(old) => old.clone(),
                None => XlMeta {
                    object_key: key.to_string(),
                    versions: Vec::new(),
                },
            };
            m.versions.push(marker);
            m.refresh_latest_flags();
            self.write_meta(bucket, key, &m, old.as_ref()).await?;
            return Ok(DeleteOutcome::Marker {
                version_id: marker_vid,
            });
        };

        // 点名删版本
        let vid = vid.to_string();
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Ok(DeleteOutcome::Version { version_id: vid }); // 幂等：键不存在
        };
        let Some(pos) = old.versions.iter().position(|v| v.version_id_str() == vid) else {
            return Ok(DeleteOutcome::Version { version_id: vid }); // 幂等：版本不存在
        };
        let removed = old.versions[pos].clone();
        let mut m = old.clone();
        m.versions.remove(pos);
        if m.versions.is_empty() {
            self.purge_key(bucket, key).await?;
            return Ok(DeleteOutcome::Version { version_id: vid });
        }
        m.refresh_latest_flags();
        // rank 防倒退（verifier #2）：链重写不得让 max_epoch 下降——否则掉线盘/半翻盘的旧链
        // 会凭更高或平秩在 read_meta 选举中胜出（已 204 的删除在掉线盘回来时复活）。
        // 把链尾 revision epoch 抬到旧链 max+1：epoch 语义 = 该 meta 的修订序号，
        // 追加与删除统一单调（链非空：空链已走 purge_key 分支）。
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        // 数据目录 → trash（提交点已过；best-effort——残骸由 clean 按龄回收）
        if !removed.delete_marker() {
            self.recycle_version_dir(bucket, key, &removed.part.uuid)
                .await;
        }
        Ok(DeleteOutcome::Version { version_id: vid })
    }

    /// 指定版本目录 → 各盘 trash（本地 rename + 远端 RPC；best-effort——meta 已不引用）。
    pub(in crate::store) async fn recycle_version_dir(&self, bucket: &str, key: &str, uuid: &str) {
        for drive in &self.cfg.drives {
            let Ok(obj_dir) = layout::object_dir(drive, bucket, key) else {
                continue;
            };
            let old_dir = obj_dir.join(uuid);
            let trash = layout::sys_dir(drive, "trash").join(uuid);
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let d = crate::mesh::VersionDirRef {
                    bucket: bucket.into(),
                    key: key.into(),
                    uuid: uuid.into(),
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    cl.transport.recycle_version(&n.id, d),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(node = %n.id, error = %e, "远程版本回收失败（残骸由该节点 clean 回收）")
                    }
                    Err(_) => {
                        tracing::warn!(node = %n.id, "远程版本回收超时（残骸由该节点 clean 回收）")
                    }
                }
            }
        }
    }

    /// 指定版本元数据（HEAD ?versionId）。marker 版本 → `DeleteMarker`（S3 405）。
    pub async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        let v = m
            .find_version(version_id)
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        if v.delete_marker() {
            return Err(Error::DeleteMarker(format!("{bucket}/{key}@{version_id}")));
        }
        Ok(version_info(key, v))
    }

    /// 指定版本整读。marker 版本 → `DeleteMarker`（S3 405）。
    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectInfo, Bytes)> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        let v = m
            .find_version(version_id)
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        if v.delete_marker() {
            return Err(Error::DeleteMarker(format!("{bucket}/{key}@{version_id}")));
        }
        let bytes = self
            .read_version_data(bucket, key, v, None)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        Ok((version_info(key, v), bytes))
    }

    /// 指定版本区间读。
    pub async fn get_object_range_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        range: StdRange<u64>,
    ) -> Result<Bytes> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        let v = m
            .find_version(version_id)
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        if v.delete_marker() {
            return Err(Error::DeleteMarker(format!("{bucket}/{key}@{version_id}")));
        }
        if range.start > range.end || range.end > v.size {
            return Err(Error::InvalidKey(format!(
                "range {range:?} 越界（版本 {} 字节）",
                v.size
            )));
        }
        self.read_version_data(bucket, key, v, Some(range))
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))
    }

    /// 单版本数据读取（整读 / 区间）：inline → EC 分块 → 旧 parts。**单遍**。
    /// `Ok(None)` = 数据文件不在（历史版本目录被外力清掉/非版本桶点读遇覆写）。
    pub(in crate::store) async fn read_version_data(
        &self,
        bucket: &str,
        key: &str,
        v: &Version,
        range: Option<StdRange<u64>>,
    ) -> Result<Option<Bytes>> {
        if let Some(inline) = &v.inline {
            if inline.bytes.len() as u64 != v.size {
                return Err(Error::CorruptMeta(format!(
                    "{bucket}/{key}: 内联长度 {} ≠ 对象长度 {}",
                    inline.bytes.len(),
                    v.size
                )));
            }
            let b = match range {
                None => Bytes::from(inline.bytes.clone()),
                Some(r) => Bytes::copy_from_slice(&inline.bytes[r.start as usize..r.end as usize]),
            };
            return Ok(Some(b));
        }
        if let Some(l) = &v.data_layout {
            // strict=true：版本点名读按 fail-closed 直报损坏（无整读重试语义）
            return self.read_ec_range(bucket, key, v, l, range, true).await;
        }
        // 旧 parts（M2a / ec.enabled=false 遗留形态）：整份读后切片
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?;
        let mut buf = Vec::new();
        for p in v.effective_parts() {
            let path = dir.join(&p.uuid).join(format!("part.{}", p.part));
            match tokio::fs::read(&path).await {
                Ok(b) => buf.extend_from_slice(&b),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        let b = match range {
            None => Bytes::from(buf),
            Some(r) => Bytes::from(buf).slice(r.start as usize..r.end as usize),
        };
        Ok(Some(b))
    }

    /// 版本枚举（ListObjectVersions 单页）：键升序、键内**新→旧**；
    /// `max_keys` 上限 1000；截断时返回 (key, version_id) 续点（= 本页最后一条）。
    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<VersionList> {
        self.ensure_bucket(bucket).await?;
        let max = max_keys.min(1000);
        if max == 0 {
            return Ok(VersionList::default());
        }
        // 续点参数成对出现（S3 语义）：只给 version-id-marker 无从定位
        if version_id_marker.is_some() && key_marker.is_none() {
            return Err(Error::InvalidArgument(
                "version-id-marker 需要同时提供 key-marker".into(),
            ));
        }
        // 键集（完备性同对象列表：各源首页 limit=max+1 的并集覆盖全局前 max 键）
        let mut keys: Vec<String> = {
            let mut local: Vec<String> = self
                .collect_keys(bucket)
                .await?
                .into_iter()
                .filter(|k| k.starts_with(prefix))
                .collect();
            local.sort();
            local.truncate(max + 1);
            local
        };
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let req = crate::mesh::ListLocal {
                    bucket: bucket.into(),
                    prefix: prefix.into(),
                    delimiter: None,
                    marker: None,
                    limit: max + 1,
                };
                match cl.transport.list_local(&n.id, req).await {
                    Ok(page) => keys.extend(page.entries.into_iter().map(|e| e.key)),
                    Err(e) => {
                        tracing::warn!(node = %n.id, error = %e, "peer 键集拉取失败（少一路并集）")
                    }
                }
            }
        }
        keys.sort();
        keys.dedup();

        let mut out = VersionList::default();
        'keys: for k in keys {
            if let Some(km) = key_marker
                && k.as_str() < km
            {
                continue;
            }
            let Some(m) = self.read_meta(bucket, &k).await? else {
                continue; // 并发删除的残影键
            };
            // 键内新→旧；key_marker 命中时按 version_id_marker 断点续（S3：从该版本**之后**继续）。
            // 续点版本不在链内（并发删/坏值）→ fail-loud（静默跳键会让分页客户端以为已删完）
            let mut skip_until: Option<&str> = if Some(k.as_str()) == key_marker {
                let su = version_id_marker;
                if let Some(su) = su
                    && !m.versions.iter().any(|v| v.version_id_str() == su)
                {
                    return Err(Error::InvalidArgument(format!(
                        "version-id-marker {su} 不在键 {k} 的版本链内"
                    )));
                }
                su
            } else {
                None
            };
            for (idx, v) in m.versions.iter().enumerate().rev() {
                if let Some(su) = skip_until {
                    if v.version_id_str() != su {
                        continue;
                    }
                    skip_until = None;
                    continue; // 续点版本本身不返回（S3 语义：从该版本**之后**继续）
                }
                if out.entries.len() >= max {
                    out.is_truncated = true;
                    break 'keys;
                }
                out.entries.push(VersionEntry {
                    key: k.clone(),
                    version_id: v.version_id_str(),
                    is_latest: idx + 1 == m.versions.len(),
                    is_marker: v.delete_marker(),
                    size: v.size,
                    etag: v.etag.clone(),
                    mod_time_nanos: v.mod_time_nanos,
                });
            }
        }
        if out.is_truncated
            && let Some(last) = out.entries.last()
        {
            out.next_key_marker = Some(last.key.clone());
            out.next_version_id_marker = Some(last.version_id.clone());
        }
        Ok(out)
    }
}

/// 从版本条目构造 ObjectInfo（`load_object_info` 的历史版本对偶）。
fn version_info(key: &str, v: &Version) -> ObjectInfo {
    ObjectInfo {
        key: key.to_string(),
        size: v.size,
        etag: v.etag.clone(),
        content_type: v.content_type.clone(),
        last_modified_nanos: v.mod_time_nanos,
        user_metadata: v.user_metadata.clone(),
        checksum: v.checksum.clone(),
        version_id: Some(v.version_id_str()),
    }
}
