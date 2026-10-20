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

use crate::crypto;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, Version, XlMeta};

use super::read::{check_read_sse_c, check_tier_readable};
use super::{ObjectInfo, Store};

/// M5 切片 4（B1/B2 复验补）：该版本的「不可删原因」（**不含 bypass 语义**——整链场景
/// purge/覆盖写用；点名删版本的 bypass 语义在 [`check_lock_delete`]）。
pub(in crate::store) fn active_lock_reason(v: &Version) -> Option<String> {
    let l = v.lock.as_ref()?;
    if l.legal_hold {
        return Some(format!("legal hold（版本 {}）", v.version_id_str()));
    }
    if let (Some(mode), Some(until)) = (l.mode.as_deref(), l.retain_until_nanos)
        && until > super::now_nanos()
    {
        return Some(format!(
            "{mode} 保留至 {until}（版本 {}）",
            v.version_id_str()
        ));
    }
    None
}

/// 链上任一「不可删」版本 → Some(原因)。purge 与「非版本化覆盖写」守卫共用（B2：集群
/// 陈旧行/控制台硬删等绕过 S3 点名删的路径，在此 fail-closed 兜底）。
pub(in crate::store) fn chain_active_lock(m: &XlMeta) -> Option<String> {
    m.versions.iter().find_map(active_lock_reason)
}

/// M5 切片 4（L4）：删除守卫——锁定版本拒绝删除，返回 `Error::ObjectLocked`（S3 面 403）。
/// COMPLIANCE 与 legal hold **一律拒（含 admin）**；GOVERNANCE 仅 `bypass_governance` 放行；
/// `until <= now`（到期）放行。时钟依赖 NTP（设计口径；测试直接构造过期值，无需时钟注入）。
fn check_lock_delete(v: &Version, bypass_governance: bool) -> Result<()> {
    let Some(l) = &v.lock else { return Ok(()) };
    if l.legal_hold {
        return Err(Error::ObjectLocked(format!(
            "legal hold 生效中（版本 {}）",
            v.version_id_str()
        )));
    }
    if let (Some(mode), Some(until)) = (l.mode.as_deref(), l.retain_until_nanos)
        && until > super::now_nanos()
    {
        match mode {
            "COMPLIANCE" => {
                return Err(Error::ObjectLocked(format!(
                    "Compliance 保留期至 {until}（含 admin 不可删）"
                )));
            }
            "GOVERNANCE" if !bypass_governance => {
                return Err(Error::ObjectLocked(format!(
                    "Governance 保留期至 {until}（需 x-amz-bypass-governance-retention）"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// [`Store::delete_object_versioned`] 的结果（S3 DeleteObject 响应字段来源）。
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteOutcome {
    /// M6 切片 0（P2-1）：幂等空删（键/版本不存在，未发生删除）——服务端 204 但**不发事件**。
    NotFound,
    /// 版本控制桶无版本号删除：追加了 delete marker
    Marker { version_id: String },
    /// 点名删除了某版本（含 marker）；归档版本带回远端引用（S4a/S4b 清 tier 用）
    Version {
        version_id: String,
        tier: Option<TierRef>,
    },
    /// 非版本桶：整键删除（M2b 语义）；归档版本带回远端引用
    Object { tiers: Vec<TierRef> },
}

/// M8 S4a：被删归档版本的远端引用（`backend`/`key` 原样转交 TierGcSink）。
#[derive(Debug, Clone, PartialEq)]
pub struct TierRef {
    pub backend: String,
    pub key: String,
}

fn tier_ref(v: &meta::Version) -> Option<TierRef> {
    v.tier.as_ref().map(|t| TierRef {
        backend: t.backend.clone(),
        key: t.key.clone(),
    })
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
    /// M8 S4a：存储类（None = STANDARD）。
    pub storage_class: Option<String>,
    /// M8 S4b：分层阶段（None = 无 tier 记录；对账扫描用）。
    pub tier_phase: Option<crate::meta::TierPhase>,
    /// M6：复制状态（Transition sweep 跳过 PENDING/FAILED）。
    pub replication_status: Option<String>,
    /// M8 S4b：SSE 对象不参与 Transition（sweep 跳过）。
    pub encrypted: bool,
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
        self.delete_object_versioned_ext(bucket, key, version_id, versioned, false)
            .await
    }

    /// 版本感知删除（可带 `x-amz-bypass-governance-retention`；M5 切片 4）。
    /// `bypass_governance=true` 仅对 GOVERNANCE 保留期生效；COMPLIANCE 与 legal hold 不受其影响。
    pub async fn delete_object_versioned_ext(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioned: bool,
        bypass_governance: bool,
    ) -> Result<DeleteOutcome> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;

        let Some(vid) = version_id else {
            if !versioned {
                // M5 切片 4（B2 兜底）：非版本化整键删先查链——陈旧节点（行未同步）也不得
                // 借 purge 清掉锁定版本
                let old = self.read_meta(bucket, key).await?;
                let Some(old) = old else {
                    // M6 切片 0（P2-1）：空删幂等——未发生删除，事件不发
                    return Ok(DeleteOutcome::NotFound);
                };
                if let Some(why) = chain_active_lock(&old) {
                    return Err(Error::ObjectLocked(format!("键存在锁定版本：{why}")));
                }
                let tiers = old.versions.iter().filter_map(tier_ref).collect();
                self.purge_key(bucket, key).await?;
                return Ok(DeleteOutcome::Object { tiers });
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
                replication_status: None,
                remote_version_id: None,
                tags: None,
                storage_class: None,
                tier: None,
                // NTP 回拨收口：marker 也是新写版本，epoch 同样单调 +1
                epoch: Some(old.as_ref().map_or(0, |m| m.max_epoch()) + 1),
                encryption: None, // marker 无数据，恒无加密记录
                lock: None,       // marker 无数据，恒无锁
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
            return Ok(DeleteOutcome::NotFound); // 幂等空删：键不存在（不发事件）
        };
        let Some(pos) = old.versions.iter().position(|v| v.version_id_str() == vid) else {
            return Ok(DeleteOutcome::NotFound); // 幂等空删：版本不存在（不发事件）
        };
        let removed = old.versions[pos].clone();
        let removed_tier = tier_ref(&removed);
        // M5 切片 4（L4）：对象锁守卫——legal hold / COMPLIANCE 未到期一律拒（含 admin）；
        // GOVERNANCE 未到期仅 bypass 放行；到期（until <= now）放行。delete-marker 路径不受锁约束。
        check_lock_delete(&removed, bypass_governance)?;
        let mut m = old.clone();
        m.versions.remove(pos);
        if m.versions.is_empty() {
            self.purge_key(bucket, key).await?;
            return Ok(DeleteOutcome::Version {
                version_id: vid,
                tier: removed_tier,
            });
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
        Ok(DeleteOutcome::Version {
            version_id: vid,
            tier: removed_tier,
        })
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

    /// M5 切片 4（N2 复验补）：翻转某版本 legal hold（开关 ON/OFF）。
    /// marker 版本 → InvalidArgument；键/版本不存在 → NoSuchKey。重写 meta（rank 防倒退：
    /// 链尾 epoch 抬到旧链 max+1，同点名删口径）。
    pub async fn set_object_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        on: bool,
    ) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let mut m = old.clone();
        let target = m
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
            .ok_or_else(|| Error::NoSuchKey(format!("{key}@{version_id}")))?;
        if m.versions[target].delete_marker() {
            return Err(Error::InvalidArgument(
                "delete marker 不支持 legal hold".into(),
            ));
        }
        {
            let v = &mut m.versions[target];
            let l = v.lock.get_or_insert(meta::ObjectLockRecord {
                mode: None,
                retain_until_nanos: None,
                legal_hold: false,
            });
            l.legal_hold = on;
            if l.is_empty() {
                v.lock = None; // 关闭后无 retention/无 hold → 不残留空记录
            }
        }
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        Ok(())
    }

    /// M5 切片 4（P3-b 复验补）：读某版本 legal hold（**元数据面**——不做 SSE-C 客户钥
    /// 校验，锁状态非秘密；与 AWS「SSE-C 对象须带数据钥」的差异记档）。`None` = 无版本号
    /// （按 GET 口径取 latest；latest 为 marker → NoSuchKey）。marker 版本 → InvalidArgument
    /// （与 set_object_legal_hold 对称）。无锁记录 → false。
    pub async fn get_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<bool> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        let v = match version_id {
            Some(vid) => {
                let v = m
                    .find_version(vid)
                    .ok_or_else(|| Error::NoSuchKey(format!("{key}@{vid}")))?;
                if v.delete_marker() {
                    return Err(Error::InvalidArgument(
                        "delete marker 不支持 legal hold".into(),
                    ));
                }
                v
            }
            None => {
                let v = m
                    .latest()
                    .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
                if v.delete_marker() {
                    return Err(Error::NoSuchKey(key.to_string())); // 链尾 marker = 无版本号解析视同不存在
                }
                v
            }
        };
        Ok(v.lock.as_ref().is_some_and(|l| l.legal_hold))
    }

    /// M6 切片 3：设置/清除版本级复制状态（meta 重写 + 链尾 epoch 抬升，同 legal-hold 口径）。
    /// `status = None` 清除；白名单 PENDING/COMPLETED/FAILED/REPLICA。
    pub async fn set_replication_status(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        status: Option<&str>,
    ) -> Result<()> {
        if let Some(s) = status
            && !matches!(s, "PENDING" | "COMPLETED" | "FAILED" | "REPLICA")
        {
            return Err(Error::InvalidArgument(format!("非法复制状态：{s}")));
        }
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let mut m = old.clone();
        let target = m
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
            .ok_or_else(|| Error::NoSuchKey(format!("{key}@{version_id}")))?;
        // V3①：marker 版本允许携带复制状态（切片 4 DeleteMarkerReplication 成功态依赖；
        // 与 legal-hold 的 marker 拒绝口径有意不同——仅拒「版本不存在」）。
        m.versions[target].replication_status = status.map(str::to_string);
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        Ok(())
    }

    /// M8 前置：写某版本标签（空 vec = 清除）。返回解析到的版本号（"null" 或 uuid）。
    /// 无 versionId = 链尾（链尾 marker → NoSuchKey）；点名 marker → DeleteMarker（405）；
    /// 未知版本 → NoSuchKey。标签写只重写 meta，不新增版本。
    pub async fn set_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<meta::ObjectTag>,
    ) -> Result<String> {
        meta::validate_tags(&tags).map_err(Error::InvalidArgument)?;
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let mut m = old.clone();
        let target = match version_id {
            Some(vid) => {
                let idx = m
                    .versions
                    .iter()
                    .position(|v| v.version_id_str() == vid)
                    .ok_or_else(|| Error::NoSuchKey(format!("{key}@{vid}")))?;
                if m.versions[idx].delete_marker() {
                    return Err(Error::DeleteMarker(format!("{bucket}/{key}@{vid}")));
                }
                idx
            }
            None => {
                let idx = m
                    .versions
                    .len()
                    .checked_sub(1)
                    .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
                if m.versions[idx].delete_marker() {
                    return Err(Error::NoSuchKey(key.to_string()));
                }
                idx
            }
        };
        let vid = m.versions[target].version_id_str();
        m.versions[target].tags = (!tags.is_empty()).then_some(tags);
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        Ok(vid)
    }

    /// M8 前置：读某版本标签。None = 链尾；链尾 marker → NoSuchKey；点名 marker → DeleteMarker。
    pub async fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(String, Vec<meta::ObjectTag>)> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        let v = match version_id {
            Some(vid) => {
                let v = m
                    .find_version(vid)
                    .ok_or_else(|| Error::NoSuchKey(format!("{key}@{vid}")))?;
                if v.delete_marker() {
                    return Err(Error::DeleteMarker(format!("{bucket}/{key}@{vid}")));
                }
                v
            }
            None => {
                let v = m
                    .latest()
                    .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
                if v.delete_marker() {
                    return Err(Error::NoSuchKey(key.to_string()));
                }
                v
            }
        };
        Ok((v.version_id_str(), v.tags.clone().unwrap_or_default()))
    }

    /// M6 切片 4（P2-2）：置 PENDING 但**不降级终态**（COMPLETED/FAILED/REPLICA 保持）。
    /// 用于入队侧——并发 worker 可能已在 PENDING 写入前完成复制。
    pub async fn mark_replication_pending(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool> {
        self.ensure_bucket(bucket).await?;
        let _guard = self.lock_object(bucket, key).await?;
        let Some(old) = self.read_meta(bucket, key).await? else {
            return Err(Error::NoSuchKey(key.to_string()));
        };
        let mut m = old.clone();
        let target = m
            .versions
            .iter()
            .position(|v| v.version_id_str() == version_id)
            .ok_or_else(|| Error::NoSuchKey(format!("{key}@{version_id}")))?;
        if matches!(
            m.versions[target].replication_status.as_deref(),
            Some("COMPLETED" | "FAILED" | "REPLICA")
        ) {
            return Ok(false);
        }
        m.versions[target].replication_status = Some("PENDING".to_string());
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        self.write_meta(bucket, key, &m, Some(&old)).await?;
        Ok(true)
    }

    /// 指定版本元数据（HEAD ?versionId）。marker 版本 → `DeleteMarker`（S3 405）。
    pub async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectInfo> {
        self.head_object_version_ext(bucket, key, version_id, None)
            .await
    }

    /// 指定版本 HEAD（可带 SSE-C 客户钥；M5 切片 3——校验同 `check_read_sse_c`）。
    pub async fn head_object_version_ext(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        sse_c: Option<&crypto::SseCKey>,
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
        check_read_sse_c(v, sse_c)?;
        Ok(version_info(key, v))
    }

    /// 指定版本整读。marker 版本 → `DeleteMarker`（S3 405）。
    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectInfo, Bytes)> {
        self.get_object_version_ext(bucket, key, version_id, None)
            .await
    }

    /// 指定版本整读（可带 SSE-C 客户钥；M5 切片 3）。
    pub async fn get_object_version_ext(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        sse_c: Option<&crypto::SseCKey>,
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
        check_read_sse_c(v, sse_c)?;
        let bytes = self
            .read_version_data(bucket, key, v, None, sse_c)
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
        self.get_object_range_version_ext(bucket, key, version_id, range, None)
            .await
    }

    /// 指定版本区间读（可带 SSE-C 客户钥；M5 切片 3）。
    pub async fn get_object_range_version_ext(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        range: StdRange<u64>,
        sse_c: Option<&crypto::SseCKey>,
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
        check_read_sse_c(v, sse_c)?;
        if range.start > range.end || range.end > v.size {
            return Err(Error::InvalidKey(format!(
                "range {range:?} 越界（版本 {} 字节）",
                v.size
            )));
        }
        self.read_version_data(bucket, key, v, Some(range), sse_c)
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
        sse_c: Option<&crypto::SseCKey>,
    ) -> Result<Option<Bytes>> {
        check_tier_readable(v, bucket, key)?;
        if let Some(inline) = &v.inline {
            // M5 切片 2：加密 inline 落盘态 = 密文 C = c_len(P)（B4 期望长度落点之一）
            let expected = match &v.encryption {
                Some(_) => crypto::Geometry::new(v.size)?.cipher_len,
                None => v.size,
            };
            if inline.bytes.len() as u64 != expected {
                return Err(Error::CorruptMeta(format!(
                    "{bucket}/{key}: 内联长度 {} ≠ {expected}",
                    inline.bytes.len()
                )));
            }
            let plain = match &v.encryption {
                None => inline.bytes.clone(),
                Some(rec) => {
                    let ctx = crypto::UnitCtx::for_read(rec, self.maybe_enc_state(), sse_c).await?;
                    crypto::open_all(&ctx, v.size, &inline.bytes)?
                }
            };
            let b = match range {
                None => Bytes::from(plain),
                Some(r) => Bytes::copy_from_slice(&plain[r.start as usize..r.end as usize]),
            };
            return Ok(Some(b));
        }
        if let Some(l) = &v.data_layout {
            // strict=true：版本点名读按 fail-closed 直报损坏（无整读重试语义）
            return self
                .read_ec_range(bucket, key, v, l, range, true, sse_c)
                .await;
        }
        // M5 切片 2：加密对象不会以旧 parts 形态落盘（写前 fail-closed）——见到即损坏/伪造（B4）
        if v.encryption.is_some() {
            return Err(Error::CorruptMeta(format!(
                "{bucket}/{key}: 加密版本出现在旧 parts 形态（写路径不可能产出）"
            )));
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
                    storage_class: v.storage_class.clone(),
                    tier_phase: v.tier.as_ref().map(|t| t.phase),
                    replication_status: v.replication_status.clone(),
                    encrypted: v.encryption.is_some(),
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
        encryption: v.encryption.clone(),
        lock: v.lock.clone(),
        replication_status: v.replication_status.clone(),
        remote_version_id: v.remote_version_id.clone(),
        tags: v.tags.clone().unwrap_or_default(),
        storage_class: v.storage_class.clone(),
        tier_phase: v.tier.as_ref().map(|t| t.phase),
        restore_expires_at: v.tier.as_ref().and_then(|t| t.restore_expires_at),
    }
}
