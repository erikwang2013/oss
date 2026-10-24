//! Multipart Upload：暂存区在**对象目录下的** `<upload_id>/`（规格布局）。
//! complete 时按声明顺序拼装零件 → 走与单段 PUT 相同的「内联 / EC」落盘为新版本目录，
//! 暂存区整目录清理（M2b-1 Task 6 裁决：旧 parts 形态只留给 M2a 旧对象读取，新写入不再使用）。
//!
//! 提交语义与单段 PUT 完全一致：part 上传只把数据放进对象目录（**无 meta 引用 → 不可见**），
//! complete 写 `xl.meta` 才是唯一可见性翻转。

use std::collections::BTreeMap;
use std::path::PathBuf;

use bytes::Bytes;
use futures::StreamExt as _;
use md5::{Digest as _, Md5};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::crypto;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, Version, XlMeta};
use crate::store::mpu_route;
use crate::store::put::TeeGuard;
use crate::store::{ByteStream, ObjectInfo, Store, VersioningMode, now_nanos};

/// 暂存区标记文件；存在即「这是一个活着的 MPU 暂存区」。
/// complete 成功后改名为 [`MPU_DONE_FILE`]——abort 以本标记为生死门，防误删已转正的版本目录。
const MPU_META_FILE: &str = "mpu.meta";
const MPU_DONE_FILE: &str = "mpu.meta.done";
/// 除末段外的最小 part 大小（S3 规格）。
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub(crate) const MAX_PARTS: u16 = 10_000;

#[derive(Debug, Serialize, Deserialize)]
struct MpuMeta {
    content_type: Option<String>,
    #[serde(default)]
    user_metadata: BTreeMap<String, String>,
    created_nanos: i64,
    /// M5 切片 3（R5）：SSE-C 上传的 base64(MD5(key)) 校验值（create 时记；**钥本身绝不落盘**）。
    /// create/UploadPart/Complete 三处必须携带同一把钥（md5 一致），否则 400。
    #[serde(default)]
    sse_c_md5: Option<String>,
    /// M5 切片 3（R5 扩展，P2-b）：create 携带的 SSE-S3/KMS 意图（"sse-s3"/"sse-kms"）。
    /// AWS 语义下该头只出现在 create——complete 必须据此**落地加密**，不得静默降级为明文。
    #[serde(default)]
    sse_kind: Option<String>,
    /// M5 切片 4（L3）：create 携带的对象锁意图（同 AWS：锁头只出现在 create，complete 落地）。
    #[serde(default)]
    lock_mode: Option<String>,
    #[serde(default)]
    lock_until_nanos: Option<i64>,
    #[serde(default)]
    lock_legal_hold: bool,
    /// M8 S4a：Create 时声明的存储类（Complete 落地到版本；归档类在 Create 被拒）。
    #[serde(default)]
    storage_class: Option<String>,
    /// M8 S6 缺口 A：Create 的 `x-amz-tagging`（Complete 落地到版本；Complete 无标签输入）。
    /// 旧 msgpack 无此字段 → None（在途 MPU 跨版本升级仍可 complete）。
    #[serde(default)]
    pub tags: Option<Vec<meta::ObjectTag>>,
    /// M3 MPU 会话粘性：属主节点 id（None = 单节点/旧格式 = 本地属主）。
    /// 目录名仍是规范 UUID；本字段只用于 LMU 重建客户端可见的 upload_id（含前缀）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
}

/// LMU 扫描摘要：活暂存区的 `mpu.meta` 投影。
pub(super) struct MpuSummary {
    pub created_nanos: i64,
    pub storage_class: Option<String>,
    /// `None` = 旧格式（属主 = 本地）
    pub owner: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartMeta {
    etag: String,
    sha256: String,
    size: u64,
}

/// 3.16：`mpu.meta.sse_kind` → `EncSpec`。旧形态 "sse-s3"/"sse-kms" 不变；显式 CMK 编码为
/// `sse-kms:<name>`（名白名单与对象 key_id 同规）。非法值 → CorruptMeta（complete 前拒绝）。
fn sse_spec_from_kind(kind: &str) -> Result<crypto::EncSpec> {
    match kind {
        "sse-s3" => Ok(crypto::EncSpec::SseS3),
        "sse-kms" => Ok(crypto::EncSpec::SseKms),
        k => match k.strip_prefix("sse-kms:") {
            Some(name) if crate::kms::validate_cmk_name(name).is_ok() => {
                Ok(crypto::EncSpec::SseKmsCmk(name.to_string()))
            }
            _ => Err(Error::CorruptMeta(format!(
                "mpu.meta sse_kind 非法：{kind:?}"
            ))),
        },
    }
}

/// `ListParts` 的单项（内核形态；s3s 翻译在 xl-s3 层）。
#[derive(Debug, Clone, PartialEq)]
pub struct MpuPartInfo {
    pub part_number: u16,
    pub size: u64,
    pub etag: String,
}

impl Store {
    /// 上传 id → 暂存区目录。`upload_id` 是客户端可控字符串：先拆分属主前缀再
    /// `Uuid::parse_str` 取**规范化字符串**拼路径（不用原始输入），`../`、大小写变体
    /// 一律收口到同一目录。
    ///
    /// 本地入口只接受「无前缀（旧格式）」或「前缀 = 本节点」；其他前缀 → NoSuchUpload
    /// （mesh handler 误投/直调也在此收口，绝不去碰别的节点的暂存区）。
    fn mpu_dir(&self, bucket: &str, key: &str, upload_id: &str) -> Result<PathBuf> {
        let (owner, uuid) = mpu_route::split_upload_id(upload_id)?;
        if let Some(owner) = owner {
            let self_id = self
                .cfg
                .cluster
                .as_deref()
                .map(|c| c.sets.flat.self_node().id.as_str());
            if self_id != Some(owner.as_str()) {
                return Err(Error::NoSuchUpload(upload_id.to_string()));
            }
        }
        Ok(layout::object_dir(self.cfg.root(), bucket, key)?.join(uuid))
    }

    /// 属主解析：`None` = 本地执行（旧格式或无前缀）；`Some(node)` = 需转发。
    ///
    /// 未知属主（不在拓扑内）→ `Unavailable`（S3 503）：upload_id 指向的节点已不存在，
    /// 既不能本地找到也不能假装 NoSuchUpload（会诱导客户端放弃可恢复的暂存区）。
    pub(in crate::store) fn mpu_remote_owner(&self, upload_id: &str) -> Result<Option<String>> {
        let (owner, _) = mpu_route::split_upload_id(upload_id)?;
        let Some(owner) = owner else {
            return Ok(None); // 旧格式：本地属主（滚动升级前的在途 MPU）
        };
        let Some(cl) = self.cfg.cluster.as_deref() else {
            return Err(Error::NoSuchUpload(upload_id.to_string())); // 单节点不接受前缀
        };
        if owner == cl.sets.flat.self_node().id {
            return Ok(None);
        }
        if !cl.sets.flat.nodes.iter().any(|n| n.id == owner) {
            return Err(Error::Unavailable(format!(
                "MPU 属主节点 {owner} 不在拓扑内（upload_id {upload_id}）"
            )));
        }
        Ok(Some(owner))
    }

    /// 创建 MPU：对象目录下建 `<upload_id>/` 暂存区并写 `mpu.meta`。
    /// 对象已有旧版本不受影响（complete 才提交）。
    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<String> {
        self.create_multipart_upload_ext(
            bucket,
            key,
            content_type,
            user_metadata,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// 创建 MPU（可带 SSE-C 记录 + SSE-S3/KMS 意图；M5 切片 3/R5——只记 md5 与 kind，
    /// 密钥绝不落盘；kind 在 complete 落地）。`tags` = Create 的标签，complete 落地。
    #[allow(clippy::too_many_arguments)] // 参数面 = 线头映射，无需聚合体（同 rebalance/read 先例）
    pub async fn create_multipart_upload_ext(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
        sse_c_md5: Option<&str>,
        sse_kind: Option<&str>,
        lock: Option<&meta::ObjectLockRecord>,
        storage_class: Option<String>,
        tags: Option<Vec<meta::ObjectTag>>,
    ) -> Result<String> {
        layout::validate_key(key)?;
        // 信任边界：同单段 PUT（put.rs），非法标签在 Create 即拒；空集合归一为 None
        if let Some(t) = &tags {
            meta::validate_tags(t).map_err(Error::InvalidArgument)?;
        }
        let tags = tags.filter(|t| !t.is_empty());
        // 3.16：服务端加密意图（sse_kind）落盘前校验形态；非法 → 400（含 sse-kms:<bad>）
        if let Some(kind) = sse_kind {
            sse_spec_from_kind(kind)
                .map_err(|_| Error::InvalidArgument(format!("非法 SSE 模式：{kind:?}")))?;
        }
        // M8 S4a：存储类归一/白名单；归档类在服务层已拒（store 兜底）
        let storage_class = meta::normalize_storage_class(storage_class.as_deref());
        if let Some(sc) = &storage_class
            && (!meta::is_valid_storage_class(sc) || meta::is_archive_class(sc))
        {
            return Err(Error::InvalidArgument(format!("非法存储类：{sc}")));
        }
        // 桶窗口（M2b-2 Task 10，同单段 PUT）：ensure + 对象目录先落
        let obj_dir = {
            let _bguard = self.lock_bucket_owned(bucket).await;
            self.ensure_bucket(bucket).await?;
            let dir = layout::object_dir(self.cfg.root(), bucket, key)?;
            tokio::fs::create_dir_all(&dir).await?;
            dir
        };
        let _guard = self.lock_object(bucket, key).await?;

        // M3 MPU 会话粘性：目录名恒为规范 UUID；集群模式额外把属主编码进客户端可见的
        // upload_id（单节点保持裸 UUID，v1.4.0 行为逐字节一致）。
        let uuid = uuid::Uuid::new_v4().to_string();
        let (upload_id, owner) = match self.cfg.cluster.as_deref() {
            Some(cl) => {
                let node_id = cl.sets.flat.self_node().id.clone();
                if node_id.is_empty() {
                    return Err(Error::Encode(
                        "集群 self_id 为空：无法生成可路由的 MPU upload_id".into(),
                    ));
                }
                (mpu_route::format_upload_id(&node_id, &uuid), Some(node_id))
            }
            None => (uuid.clone(), None),
        };
        let dir = obj_dir.join(&uuid);
        let meta = MpuMeta {
            content_type,
            user_metadata,
            created_nanos: now_nanos(),
            sse_c_md5: sse_c_md5.map(str::to_string),
            sse_kind: sse_kind.map(str::to_string),
            lock_mode: lock.and_then(|l| l.mode.clone()),
            lock_until_nanos: lock.and_then(|l| l.retain_until_nanos),
            lock_legal_hold: lock.is_some_and(|l| l.legal_hold),
            storage_class,
            tags,
            owner,
        };
        let bytes = rmp_serde::to_vec_named(&meta).map_err(|e| Error::Encode(e.to_string()))?;
        tokio::fs::create_dir_all(&dir).await?;
        if let Err(e) = tokio::fs::write(dir.join(MPU_META_FILE), bytes).await {
            // 失败清理：暂存区 + 可能新建的空对象目录（空目录会卡住删桶）
            let _ = tokio::fs::remove_dir_all(&dir).await;
            let _ = tokio::fs::remove_dir(&obj_dir).await;
            return Err(e.into());
        }
        Ok(upload_id)
    }

    /// 上传一个 part（重传同号允许覆盖）。不取对象锁：文件名按 part 号互斥、sidecar 独立
    /// （complete/abort 才需要与 PUT/DELETE 串行）。返回 part 的 ETag（hex MD5）。
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        body: ByteStream,
    ) -> Result<String> {
        self.upload_part_ext(bucket, key, upload_id, part_number, body, None)
            .await
    }

    /// 上传一个 part（可带 SSE-C 校验；M5 切片 3/R5）：create 记了 md5 则本 part 必带同钥
    /// （md5 一致）；未记则不得携带。分片暂存仍明文（D8），实际加密在 complete 单流。
    ///
    /// M3 MPU 会话粘性：非属主节点收到请求时**流式转发**给属主（分段走 mesh 流，
    /// 不在内存里整段缓冲）；属主执行 [`Self::upload_part_local`]。
    pub async fn upload_part_ext(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        body: ByteStream,
        sse_c_md5: Option<&str>,
    ) -> Result<String> {
        if let Some(node) = self.mpu_remote_owner(upload_id)? {
            let t = self.mesh_transport().ok_or_else(|| {
                Error::Unavailable("集群传输未注入，无法转发 MPU UploadPart".into())
            })?;
            return t
                .mpu_upload_part(
                    &node,
                    crate::mesh::MpuPartUpload {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.to_string(),
                        part_number,
                        sse_c_md5: sse_c_md5.map(str::to_string),
                        body,
                    },
                )
                .await;
        }
        self.upload_part_local(bucket, key, upload_id, part_number, body, sse_c_md5)
            .await
    }

    /// UploadPart 的本地执行（**不再转发**；mesh 服务端 handler 与属主路径共用）。
    pub(in crate::store) async fn upload_part_local(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        mut body: ByteStream,
        sse_c_md5: Option<&str>,
    ) -> Result<String> {
        if !(1..=MAX_PARTS).contains(&part_number) {
            return Err(Error::InvalidPart(format!(
                "part number {part_number} 超出 1..={MAX_PARTS}"
            )));
        }
        self.ensure_bucket(bucket).await?;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        // R5：读暂存 meta（存在性 + SSE-C 一致性一次完成）
        let mpu: MpuMeta = match tokio::fs::read(dir.join(MPU_META_FILE)).await {
            Ok(b) => rmp_serde::from_slice(&b)
                .map_err(|e| Error::CorruptMeta(format!("mpu.meta 解码失败: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NoSuchUpload(upload_id.to_string()));
            }
            Err(e) => return Err(e.into()),
        };
        match (mpu.sse_c_md5.as_deref(), sse_c_md5) {
            (Some(exp), Some(got)) if exp == got => {}
            (Some(_), _) => {
                return Err(Error::InvalidArgument(
                    "SSE-C 上传需携带创建 MPU 时的同一密钥（md5 不符或缺失）".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(Error::InvalidArgument(
                    "该 MPU 未以 SSE-C 创建，不应携带客户端密钥".into(),
                ));
            }
            (None, None) => {}
        }

        // 半截文件防护：先写 part.N.new，流完整落盘后 rename 覆盖 part.N
        let new_path = dir.join(format!("part.{part_number}.new"));
        let part_path = dir.join(format!("part.{part_number}"));
        let staged: Result<(u64, String, String)> = async {
            let mut file = tokio::fs::File::create(&new_path).await?;
            let mut md5 = Md5::new();
            let mut sha = sha2::Sha256::new();
            let mut size: u64 = 0;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                md5.update(&chunk);
                sha.update(&chunk);
                size += chunk.len() as u64;
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            if self.cfg.fsync {
                file.sync_all().await?;
            }
            drop(file);
            Ok((
                size,
                hex::encode(md5.finalize()),
                hex::encode(sha.finalize()),
            ))
        }
        .await;
        let (size, etag, sha256) = match staged {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_file(&new_path).await;
                return Err(e);
            }
        };

        let part_meta = PartMeta {
            etag: etag.clone(),
            sha256,
            size,
        };
        let bytes =
            rmp_serde::to_vec_named(&part_meta).map_err(|e| Error::Encode(e.to_string()))?;
        tokio::fs::write(dir.join(format!("part.{part_number}.meta")), bytes).await?;
        tokio::fs::rename(&new_path, &part_path).await?;
        Ok(etag)
    }

    /// 完成 MPU：校验（对齐规格）→ 按声明顺序拼装零件为**内联/EC 形态**的新版本目录
    /// → 写 `xl.meta`（唯一提交点）→ 暂存区清理 → 旧版本进 trash。
    ///
    /// `parts` 为客户端声明的 `(part_number, etag)` 列表，须严格升序。校验链：
    /// 空/超量 → InvalidPart；非升序 → InvalidPartOrder；part 缺失/ETag 不一致 → InvalidPart；
    /// 非末段 < 5 MiB → EntityTooSmall。
    /// 兼容壳（无版本控制）：S3 层显式带桶状态走 [`Self::complete_multipart_upload_versioned`]。
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u16, String)],
    ) -> Result<ObjectInfo> {
        self.complete_multipart_upload_versioned(
            bucket,
            key,
            upload_id,
            parts,
            VersioningMode::Off,
            None,
            None,
        )
        .await
    }

    /// 版本控制感知的 complete（M4 切片 3 / S3 Suspended）：Enabled 追加版本（保留旧
    /// 目录）；Suspended 替换链中 null 版本；Off 维持单版本 + 回收的 M2b 语义。
    ///
    /// M3 MPU 会话粘性：非属主节点转发给属主（版本形态/加密/锁意图随请求透传；
    /// 提交点只在属主发生，前端节点不重复提交）。
    #[allow(clippy::too_many_arguments)] // 同 create_multipart_upload_ext：线头直通
    pub async fn complete_multipart_upload_versioned(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u16, String)],
        mode: VersioningMode,
        encryption: Option<crypto::EncSpec>,
        lock: Option<meta::ObjectLockRecord>,
    ) -> Result<ObjectInfo> {
        if let Some(node) = self.mpu_remote_owner(upload_id)? {
            let t = self.mesh_transport().ok_or_else(|| {
                Error::Unavailable("集群传输未注入，无法转发 MPU Complete".into())
            })?;
            return t
                .mpu_complete(
                    &node,
                    crate::mesh::MpuComplete {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.to_string(),
                        parts: parts.to_vec(),
                        mode,
                        encryption: encryption.as_ref().map(crate::mesh::MpuEnc::from_spec),
                        lock: lock.clone(),
                    },
                )
                .await;
        }
        self.complete_multipart_upload_versioned_local(
            bucket, key, upload_id, parts, mode, encryption, lock,
        )
        .await
    }

    /// Complete 的本地执行（**不再转发**；mesh 服务端 handler 与属主路径共用）。
    #[allow(clippy::too_many_arguments)] // 同上
    pub(in crate::store) async fn complete_multipart_upload_versioned_local(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u16, String)],
        mode: VersioningMode,
        encryption: Option<crypto::EncSpec>,
        lock: Option<meta::ObjectLockRecord>,
    ) -> Result<ObjectInfo> {
        layout::validate_key(key)?;
        // M9 S3：删桶 quiesce 屏障（complete 也是提交点）。
        let _writer = self.begin_write(bucket)?;
        // 桶窗口（同单段 PUT）
        let obj_dir = {
            let _bguard = self.lock_bucket_owned(bucket).await;
            self.ensure_bucket(bucket).await?;
            let dir = layout::object_dir(self.cfg.root(), bucket, key)?;
            tokio::fs::create_dir_all(&dir).await?;
            dir
        };
        let _guard = self.lock_object(bucket, key).await?;

        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let mpu: MpuMeta = rmp_serde::from_slice(&tokio::fs::read(dir.join(MPU_META_FILE)).await?)
            .map_err(|e| Error::CorruptMeta(format!("mpu.meta 解码失败: {e}")))?;

        // R5：SSE-C 一致性（create ↔ complete 双向）——create 记了 md5 则 complete 必带同钥；
        // create 未记则不得携带客户端密钥（AWS 语义：SSE-C 参数须全程一致）。
        match (mpu.sse_c_md5.as_deref(), &encryption) {
            (Some(exp), Some(crypto::EncSpec::SseC(k))) if k.md5_b64() == exp => {}
            (Some(_), _) => {
                return Err(Error::InvalidArgument(
                    "complete 需携带创建 MPU 时的 SSE-C 密钥".into(),
                ));
            }
            (None, Some(crypto::EncSpec::SseC(_))) => {
                return Err(Error::InvalidArgument(
                    "该 MPU 未以 SSE-C 创建，complete 不应携带客户端密钥".into(),
                ));
            }
            (None, _) => {}
        }

        // M5 切片 4（L3）：create 记录的锁意图在 complete **落地**（AWS：锁头只出现在 create）。
        // complete 携带（仅直调 store API 可构造）须与 create 记录一致；否则 400。
        let recorded_lock = match (
            mpu.lock_mode.as_deref(),
            mpu.lock_until_nanos,
            mpu.lock_legal_hold,
        ) {
            (None, None, false) => None,
            (m, u, h) => Some(meta::ObjectLockRecord {
                mode: m.map(str::to_string),
                retain_until_nanos: u,
                legal_hold: h,
            }),
        };
        let effective_lock: Option<meta::ObjectLockRecord> = match (&lock, &recorded_lock) {
            (None, None) => None,
            (None, Some(rec)) => Some(rec.clone()),
            (Some(l), None) => {
                // 与 sse_kind 同口径：create 未声明而 complete 携带 → fail-closed
                if l.is_empty() {
                    None
                } else {
                    return Err(Error::InvalidArgument(
                        "create 未声明对象锁，complete 不应携带锁参数".into(),
                    ));
                }
            }
            (Some(l), Some(rec)) if l == rec => Some(l.clone()),
            (Some(_), Some(_)) => {
                return Err(Error::InvalidArgument(
                    "create 与 complete 的对象锁参数不一致".into(),
                ));
            }
        };

        // R5 扩展（P2-b 复验补）：create 记录的 SSE-S3/KMS 意图在 complete **落地**——AWS 语义下
        // 该头只出现在 create；不落地则「create 回 AES256、对象实为明文」= 静默降级。
        let effective_enc: Option<crypto::EncSpec> =
            match (encryption.as_ref(), mpu.sse_kind.as_deref()) {
                (None, None) => None,
                // create 记录的服务端模式即 complete 的落地规格；旧 kind 语义不变
                (None, Some(kind)) => Some(sse_spec_from_kind(kind)?),
                (Some(crypto::EncSpec::SseC(_)), None) => encryption.clone(),
                (Some(crypto::EncSpec::SseC(_)), Some(_)) => {
                    return Err(Error::InvalidArgument(
                        "create 已带 SSE-S3/KMS 头，complete 不得携带 SSE-C".into(),
                    ));
                }
                (Some(_), None) => {
                    return Err(Error::InvalidArgument(
                        "create 未声明 SSE-S3/KMS，complete 不应携带该模式".into(),
                    ));
                }
                // 3.16：显式 CMK 参与一致性核对（名/ARN 归一后必须同名，防张冠李戴）
                (Some(spec), Some(kind)) => {
                    if sse_spec_from_kind(kind)? == *spec {
                        encryption.clone()
                    } else {
                        return Err(Error::InvalidArgument(
                            "create 与 complete 的 SSE 模式不一致".into(),
                        ));
                    }
                }
            };

        // M5 切片 2/3：加密上下文（wrap 一次/对象）。ec.enabled=false → 写前 fail-closed。
        let enc_ctx = match &effective_enc {
            Some(spec) => {
                if !self.cfg.ec.enabled {
                    return Err(Error::Encode(
                        "encryption requires ec.enabled（加密数据面需要 EC 管线）".into(),
                    ));
                }
                Some(crypto::UnitCtx::for_write(spec.clone(), self.maybe_enc_state()).await?)
            }
            None => None,
        };
        let enc_record = enc_ctx.as_ref().map(|(_, r)| r.clone());

        if parts.is_empty() {
            return Err(Error::InvalidPart("parts 为空".into()));
        }
        if parts.len() > MAX_PARTS as usize {
            return Err(Error::InvalidPart(format!(
                "parts 数量 {} 超过 {MAX_PARTS}",
                parts.len()
            )));
        }
        for w in parts.windows(2) {
            if w[0].0 >= w[1].0 {
                return Err(Error::InvalidPartOrder(format!(
                    "part 号非严格升序: {} -> {}",
                    w[0].0, w[1].0
                )));
            }
        }

        let mut raw_md5s: Vec<Vec<u8>> = Vec::with_capacity(parts.len());
        // 拼装快照（part 号、数据文件、sidecar 记录）：concat 时对每段做字节级核验
        let mut snapshots: Vec<(u16, PathBuf, PartMeta)> = Vec::with_capacity(parts.len());
        for (i, (pn, etag)) in parts.iter().enumerate() {
            let sidecar = dir.join(format!("part.{pn}.meta"));
            let pm: PartMeta = match tokio::fs::read(&sidecar).await {
                Ok(b) => rmp_serde::from_slice(&b)
                    .map_err(|e| Error::CorruptMeta(format!("part.{pn}.meta 解码失败: {e}")))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::InvalidPart(format!("part {pn} 未上传")));
                }
                Err(e) => return Err(e.into()),
            };
            if !tokio::fs::try_exists(dir.join(format!("part.{pn}"))).await? {
                return Err(Error::InvalidPart(format!("part {pn} 数据文件缺失")));
            }
            if &pm.etag != etag {
                return Err(Error::InvalidPart(format!(
                    "part {pn} ETag 不一致（声明 {etag}，实际 {}）",
                    pm.etag
                )));
            }
            let is_last = i + 1 == parts.len();
            if !is_last && pm.size < MIN_PART_SIZE {
                return Err(Error::EntityTooSmall(format!(
                    "part {pn} 只有 {} 字节（非末段须 ≥ 5 MiB）",
                    pm.size
                )));
            }
            raw_md5s.push(
                hex::decode(&pm.etag)
                    .map_err(|e| Error::CorruptMeta(format!("part.{pn} etag 非法: {e}")))?,
            );
            snapshots.push((*pn, dir.join(format!("part.{pn}")), pm));
        }

        // multipart ETag = hex(md5(逐段原始 md5 字节拼接)) + "-N"（S3 定义）
        let mut h = Md5::new();
        for raw in &raw_md5s {
            h.update(raw);
        }
        let etag = format!("{}-{}", hex::encode(h.finalize()), parts.len());
        // partNumber 读路径的偏移表：暂存 part 文件在 complete 后可回收，边界只此一份
        let part_sizes: Vec<u64> = snapshots.iter().map(|(_, _, pm)| pm.size).collect();

        // M-6 同口径：损坏旧 meta 让 complete fail-closed
        let old_meta = self.read_meta(bucket, key).await?;
        // 覆盖写会回收的旧版本：Off = 整链；Suspended = 链中 null 版本（其余保留）。
        // 链上带锁 → fail-closed（同单段 PUT 口径）。
        let replaced: Vec<Version> = match (mode, old_meta.as_ref()) {
            (VersioningMode::Off, Some(old)) => old.versions.clone(),
            (VersioningMode::Suspended, Some(old)) => old
                .versions
                .iter()
                .filter(|v| v.version_id.is_nil())
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        if let Some(why) = replaced.iter().find_map(super::version::active_lock_reason) {
            return Err(Error::ObjectLocked(format!("覆盖写将回收锁定版本：{why}")));
        }
        // per-key 单调 mod_time（C4）：与单段 PUT 同口径
        let now = match old_meta.as_ref().and_then(|m| m.latest()) {
            Some(v) => now_nanos().max(v.mod_time_nanos + 1),
            None => now_nanos(),
        };

        // 装进新版本目录（内联 / EC 形态）——与单段 PUT 同一形态核心
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(self.cfg.root(), "tmp").join(&uuid);
        // M9：sync 桶 complete 同样先 staging（不可见）→ 远端确认 → 本地 meta 翻转。
        // MPU 的 tee 在拼接 part 流时一次写出（无法从已存 part 安全重建 ETag/元数据）。
        let sync = self.sync_mode(bucket).await?;
        let max_bytes = sync.then_some(self.cfg.sync_max_bytes);
        let tee = sync.then(|| TeeGuard::new(self.tee_dir().join(format!("{uuid}.plain"))));
        let tee_path = tee.as_ref().and_then(TeeGuard::path);
        // 集群 + 单盘节点也必须走槽位路径（与 put.rs 同口径）
        let multi = self.cfg.cluster.is_some() || self.cfg.multi_drive();
        // 1) staging：数据落位但对读路径不可见
        let staged: Result<(
            u64,
            String,
            Option<meta::DataLayout>,
            Option<meta::InlineData>,
        )> = async {
            // concat_stream_verified 逐段核验 bytes == sidecar 快照，通过则 size 恒等于各段总和。
            // upload_part 不持对象锁（允许并发传不同 part）：complete 期间零件被并发重传
            // = 快照与实际字节不一致 → InvalidPart（fail-closed，让客户端重试 complete）
            let (size, _md5, sha256, data_layout, inline) = if multi {
                let (s, e, sh, l, i, _touched) = self
                    .stage_payload_multi_tee(
                        bucket,
                        key,
                        &uuid,
                        &tmp,
                        concat_stream_verified(snapshots),
                        enc_ctx.as_ref().map(|(c, _)| c),
                        tee_path,
                        max_bytes,
                    )
                    .await?;
                (s, e, sh, l, i)
            } else {
                self.stage_payload_tee(
                    &tmp,
                    concat_stream_verified(snapshots),
                    enc_ctx.as_ref().map(|(c, _)| c),
                    tee_path,
                    max_bytes,
                )
                .await?
            };
            Ok((size, sha256, data_layout, inline))
        }
        .await;
        let (size, sha256, data_layout, inline) = match staged {
            Ok(v) => v,
            Err(e) => {
                if multi {
                    self.rollback_versions(bucket, key, &uuid).await;
                }
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

        // 2) gate：staging 后、本地 meta 翻转前（sync 桶必须远端先确认）。
        let ticket = if sync {
            let plaintext = match tee_path {
                Some(p) => p,
                None => {
                    if multi {
                        self.rollback_versions(bucket, key, &uuid).await;
                    }
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    return Err(Error::Encode("sync tee 缺失（明文副本未生成）".into()));
                }
            };
            let req = crate::gate::GateRequest {
                bucket,
                key,
                local_version_id: &uuid,
                plaintext,
                size,
                etag: &etag,
                content_type: mpu.content_type.as_deref(),
                user_metadata: &mpu.user_metadata,
                tags: mpu.tags.as_deref(),
                storage_class: mpu.storage_class.as_deref(),
                checksum: None, // multipart 组合校验和留 M2b
                part_sizes: Some(part_sizes.as_slice()),
            };
            match self.gate_commit(req).await {
                Ok(t) => Some(t),
                Err(e) => {
                    if multi {
                        self.rollback_versions(bucket, key, &uuid).await;
                    }
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    return Err(e);
                }
            }
        } else {
            None
        };
        let remote_version_id = ticket.as_ref().and_then(|t| t.remote_version_id.clone());

        // 3) Version 与候选 meta（gate 成功之后才构造——本地提交不得先于远端）
        let version = Version {
            version_id: if mode == VersioningMode::Enabled {
                uuid::Uuid::parse_str(&uuid)
                    .map_err(|e| Error::Encode(format!("版本目录 uuid 解析失败（{uuid}）: {e}")))?
            } else {
                uuid::Uuid::nil()
            },
            mod_time_nanos: now,
            size,
            etag: etag.clone(),
            content_type: mpu.content_type.clone(),
            user_metadata: mpu.user_metadata.clone(),
            // 兼容位：整对象引用（sha256 = 整份 SHA256，读路径 EC 终验用）
            part: PartRef {
                uuid: uuid.clone(),
                part: 1,
                size,
                sha256,
            },
            parts: None, // 裁决：新写入不再使用 parts（降为 M2a 旧对象只读兼容位）
            part_sizes: Some(part_sizes.clone()),
            checksum: None, // multipart 组合校验和留 M2b
            data_layout,
            inline,
            is_delete_marker: None,
            is_latest: None,
            // NTP 回拨收口：epoch = 旧链 max+1（与单段 PUT 同口径）
            epoch: Some(old_meta.as_ref().map_or(0, |m| m.max_epoch()) + 1),
            // M5 切片 2：加密记录随版本落 meta（单流形态：MPU 于 complete 合并后加密）
            encryption: enc_record.clone(),
            // M5 切片 4：对象锁（create 记录的意图在此落地）
            lock: effective_lock.clone(),
            // sync 写远端已确认 → COMPLETED；非 sync 保持 M6 口径（None）。
            replication_status: ticket.as_ref().map(|_| "COMPLETED".to_string()),
            remote_version_id: remote_version_id.clone(),
            // Create 声明的标签在 complete 落地（complete 无标签输入）
            tags: mpu.tags.clone(),
            storage_class: mpu.storage_class.clone(),
            tier: None,
        };
        let mut meta = match (mode, old_meta.as_ref()) {
            (VersioningMode::Enabled, Some(old)) => {
                let mut m = old.clone();
                m.versions.push(version);
                m
            }
            (VersioningMode::Suspended, Some(old)) => {
                let mut m = old.clone();
                m.versions.retain(|v| !v.version_id.is_nil());
                m.versions.push(version);
                m
            }
            _ => XlMeta {
                object_key: key.to_string(),
                versions: vec![version],
            },
        };
        meta.refresh_latest_flags();
        let candidate: Result<()> = if multi {
            Ok(())
        } else {
            match meta.encode() {
                Ok(bytes) => tokio::fs::write(tmp.join(crate::meta::META_FILE), bytes)
                    .await
                    .map_err(Error::from),
                Err(e) => Err(e),
            }
        };
        if let Err(e) = candidate {
            if let Some(t) = &ticket {
                self.abort_gate(t).await;
            }
            if multi {
                self.rollback_versions(bucket, key, &uuid).await;
            }
            let _ = tokio::fs::remove_dir_all(&tmp).await;
            return Err(e);
        }

        // 4) 本地提交（提交点 = xl.meta 翻转）。失败 → 补偿远端（best-effort）。
        let committed: Result<()> = async {
            if multi {
                // 多盘：两阶段 meta 提交（数据已在各盘版本目录就位；见 put.rs 同款流程）
                // 提交点前复查租约（Task 8）
                if _guard.lease_lost() {
                    self.rollback_versions(bucket, key, &uuid).await;
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    return Err(Error::QuorumUnmet("集群锁租约丢失，MPU 提交中止".into()));
                }
                let quorum = self.cfg.ec.data as usize + 1;
                let flipped = match self.write_meta(bucket, key, &meta, old_meta.as_ref()).await {
                    Ok(n) => n,
                    Err(e) => {
                        self.rollback_versions(bucket, key, &uuid).await;
                        let _ = tokio::fs::remove_dir_all(&tmp).await;
                        return Err(e);
                    }
                };
                if flipped == 0 {
                    self.rollback_versions(bucket, key, &uuid).await;
                    self.clear_meta_new(bucket, key).await; // R7：清各盘遗留的 .new 暂存 meta
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    return Err(Error::QuorumUnmet("meta 翻转全失败，未提交".into()));
                }
                if flipped < quorum {
                    tracing::warn!(
                        flipped,
                        quorum,
                        "meta 翻转不足 quorum（已提交，M4 heal 补副本）"
                    );
                }
                let _ = tokio::fs::remove_dir_all(&tmp).await;
            } else {
                // 数据就位（同盘 rename）→ 唯一提交点
                tokio::fs::create_dir_all(&obj_dir).await?;
                tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;
                self.write_meta(bucket, key, &meta, old_meta.as_ref())
                    .await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = committed {
            if let Some(t) = &ticket {
                self.abort_gate(t).await;
            }
            return Err(e);
        }
        // 本地提交点已过：通知门清理写 intent。
        self.commit_gate(&ticket).await;

        // 暂存区收尾（best-effort：提交点已过）：mpu.meta 生死门先改名，再整目录删。
        // 若删到一半崩溃，残骸无 meta 引用，由 `reap_stale` 按 mtime 回收
        let _ = tokio::fs::rename(dir.join(MPU_META_FILE), dir.join(MPU_DONE_FILE)).await;
        let _ = tokio::fs::remove_dir_all(&dir).await;

        // 老版本进回收站（跨盘遍历，与单段 PUT 相同；版本控制桶跳过——旧版本仍被链引用）
        if mode == VersioningMode::Off {
            self.recycle_old(bucket, key, old_meta.as_ref()).await;
        } else if mode == VersioningMode::Suspended {
            self.recycle_versions(bucket, key, &replaced).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type: mpu.content_type,
            last_modified_nanos: now,
            user_metadata: mpu.user_metadata,
            checksum: None,
            // 与 version_id_str() 同式：Enabled 用目录 uuid，否则 S3 的 "null"
            version_id: Some(if mode == VersioningMode::Enabled {
                uuid.clone()
            } else {
                "null".to_string()
            }),
            encryption: enc_record,
            lock: effective_lock,
            replication_status: ticket.as_ref().map(|_| "COMPLETED".to_string()),
            remote_version_id,
            tags: mpu.tags.unwrap_or_default(),
            storage_class: mpu.storage_class.clone(),
            tier_phase: None,
            restore_expires_at: None,
            part_sizes: Some(part_sizes),
        })
    }

    /// 中止 MPU：删暂存区。`mpu.meta` 为生死门——complete 成功后它已改名，不得再删。
    ///
    /// M3 MPU 会话粘性：非属主节点转发给属主（abort 与 complete 在属主侧按对象锁
    /// 串行，二者竞争只会有先到者生效，另一个得到 NoSuchUpload）。
    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        if let Some(node) = self.mpu_remote_owner(upload_id)? {
            let t = self
                .mesh_transport()
                .ok_or_else(|| Error::Unavailable("集群传输未注入，无法转发 MPU Abort".into()))?;
            return t
                .mpu_abort(
                    &node,
                    crate::mesh::MpuRef {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.to_string(),
                    },
                )
                .await;
        }
        self.abort_multipart_upload_local(bucket, key, upload_id)
            .await
    }

    /// Abort 的本地执行（**不再转发**）。
    pub(in crate::store) async fn abort_multipart_upload_local(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;
        let _guard = self.lock_object(bucket, key).await?;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        tokio::fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    /// 列出暂存区里已上传的 parts（升序）。分页不做：≤10000 全量、`is_truncated=false`。
    ///
    /// M3 MPU 会话粘性：非属主节点转发给属主。
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MpuPartInfo>> {
        if let Some(node) = self.mpu_remote_owner(upload_id)? {
            let t = self.mesh_transport().ok_or_else(|| {
                Error::Unavailable("集群传输未注入，无法转发 MPU ListParts".into())
            })?;
            let rows = t
                .mpu_list_parts(
                    &node,
                    crate::mesh::MpuRef {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.to_string(),
                    },
                )
                .await?;
            return Ok(rows
                .into_iter()
                .map(|r| MpuPartInfo {
                    part_number: r.part_number,
                    size: r.size,
                    etag: r.etag,
                })
                .collect());
        }
        self.list_parts_local(bucket, key, upload_id).await
    }

    /// ListParts 的本地执行（**不再转发**）。
    pub(in crate::store) async fn list_parts_local(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<MpuPartInfo>> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let mut out = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(e) = entries.next_entry().await? {
            let name = e.file_name().to_string_lossy().into_owned();
            // sidecar 形如 part.<n>.meta
            let Some(n) = name
                .strip_prefix("part.")
                .and_then(|r| r.strip_suffix(".meta"))
            else {
                continue;
            };
            let Ok(pn) = n.parse::<u16>() else { continue };
            let Ok(bytes) = tokio::fs::read(e.path()).await else {
                continue;
            };
            let Ok(pm) = rmp_serde::from_slice::<PartMeta>(&bytes) else {
                continue;
            };
            out.push(MpuPartInfo {
                part_number: pn,
                size: pm.size,
                etag: pm.etag,
            });
        }
        out.sort_by_key(|p| p.part_number);
        Ok(out)
    }
}

/// LMU 用摘要：读 `<upload_dir>/mpu.meta` → (created_nanos, storage_class, owner)。
/// 无 mpu.meta（非暂存区 / 已 complete / 已 abort）→ None。解码失败按损坏上抛，
/// 调用方跳过单个坏目录并告警——一个坏 mpu.meta 不得打挂整桶列举。
pub(super) async fn mpu_summary(dir: &std::path::Path) -> Result<Option<MpuSummary>> {
    let bytes = match tokio::fs::read(dir.join(MPU_META_FILE)).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let m: MpuMeta = rmp_serde::from_slice(&bytes)
        .map_err(|e| Error::CorruptMeta(format!("mpu.meta 解码失败: {e}")))?;
    Ok(Some(MpuSummary {
        created_nanos: m.created_nanos,
        storage_class: m.storage_class,
        owner: m.owner,
    }))
}

/// 按声明顺序把暂存的 `part.N` 拼成 64 KiB 块的 `ByteStream`（complete 装对象用），
/// 并对每段做**字节级核验**：段内容与 sidecar 快照（etag/sha256/长度）不符 →
/// `InvalidPart`（complete 期间被并发重传；核验对象正是即将提交的字节，竞态在核验点收口）。
///
/// 逐块读、不整份进内存；`stage_payload` 的 encode 一步自 M4 切片 2 起也是逐块流式——
/// complete 全链路常量内存（块缓冲 ≈1.3 MiB，与对象大小无关）。
fn concat_stream_verified(snapshots: Vec<(u16, PathBuf, PartMeta)>) -> ByteStream {
    type Cur = (tokio::fs::File, Md5, sha2::Sha256, u64);
    type St = (
        std::vec::IntoIter<(u16, PathBuf, PartMeta)>,
        Option<Cur>,
        Option<(u16, PartMeta)>,
    );
    let state: St = (snapshots.into_iter(), None, None);
    Box::pin(futures::stream::try_unfold(
        state,
        |(mut files, mut cur, mut cur_meta): St| async move {
            loop {
                // 不用 `match &mut cur`（Polonius 未落地：在另一分支里给 cur 赋值会被借用检查拒绝）
                if cur.is_none() {
                    match files.next() {
                        Some((pn, p, pm)) => {
                            cur = Some((
                                tokio::fs::File::open(&p).await?,
                                Md5::new(),
                                sha2::Sha256::new(),
                                0,
                            ));
                            cur_meta = Some((pn, pm));
                        }
                        None => return Ok(None),
                    }
                }
                let (f, md5, sha, n) = cur.as_mut().expect("刚填过");
                let mut buf = vec![0u8; 64 * 1024];
                let nread = f.read(&mut buf).await?;
                md5.update(&buf[..nread]); // nread=0 时 no-op
                sha.update(&buf[..nread]);
                *n += nread as u64;
                if nread == 0 {
                    // 段末：核验快照（md5/sha256/长度三者全等才算同一份字节）
                    let (_, md5, sha, n) = cur.take().expect("刚填过");
                    let (pn, pm) = cur_meta.take().expect("刚填过");
                    if n != pm.size
                        || hex::encode(md5.finalize()) != pm.etag
                        || hex::encode(sha.finalize()) != pm.sha256
                    {
                        return Err(Error::InvalidPart(format!(
                            "part {pn} 数据与上传记录不符（complete 期间被并发修改，请重试 complete）"
                        )));
                    }
                    continue; // 切下一段
                }
                buf.truncate(nread);
                return Ok(Some((Bytes::from(buf), (files, cur, cur_meta))));
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 滚动升级：旧 `mpu.meta`（无 tags 字段）必须解码为 None——否则在途 MPU 全部报废。
    #[test]
    fn old_mpu_meta_without_tags_decodes() {
        #[derive(Serialize)]
        struct OldMpuMeta {
            content_type: Option<String>,
            user_metadata: BTreeMap<String, String>,
            created_nanos: i64,
            sse_c_md5: Option<String>,
            sse_kind: Option<String>,
            lock_mode: Option<String>,
            lock_until_nanos: Option<i64>,
            lock_legal_hold: bool,
            storage_class: Option<String>,
        }
        let bytes = rmp_serde::to_vec_named(&OldMpuMeta {
            content_type: Some("text/plain".into()),
            user_metadata: BTreeMap::new(),
            created_nanos: 42,
            sse_c_md5: None,
            sse_kind: None,
            lock_mode: None,
            lock_until_nanos: None,
            lock_legal_hold: false,
            storage_class: None,
        })
        .unwrap();
        let m: MpuMeta = rmp_serde::from_slice(&bytes).unwrap();
        assert!(m.tags.is_none());
        assert_eq!(m.created_nanos, 42);
        assert_eq!(m.content_type.as_deref(), Some("text/plain"));
    }
}
