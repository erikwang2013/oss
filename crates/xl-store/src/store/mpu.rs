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

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{PartRef, Version, XlMeta};
use crate::store::{ByteStream, ObjectInfo, Store, now_nanos};

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
}

#[derive(Debug, Serialize, Deserialize)]
struct PartMeta {
    etag: String,
    sha256: String,
    size: u64,
}

/// `ListParts` 的单项（内核形态；s3s 翻译在 xl-s3 层）。
#[derive(Debug, Clone, PartialEq)]
pub struct MpuPartInfo {
    pub part_number: u16,
    pub size: u64,
    pub etag: String,
}

impl Store {
    /// 上传 id → 暂存区目录。`upload_id` 是客户端可控字符串：先 `Uuid::parse_str` 再取
    /// **规范化字符串**拼路径（不用原始输入），`../`、大小写变体一律收口到同一目录。
    fn mpu_dir(&self, bucket: &str, key: &str, upload_id: &str) -> Result<PathBuf> {
        let id = uuid::Uuid::parse_str(upload_id)
            .map_err(|_| Error::NoSuchUpload(upload_id.to_string()))?;
        Ok(layout::object_dir(&self.cfg.root, bucket, key)?.join(id.to_string()))
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
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;
        let _guard = self.lock_for(bucket, key).lock_owned().await;

        let upload_id = uuid::Uuid::new_v4().to_string();
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?.join(&upload_id);
        tokio::fs::create_dir_all(&dir).await?;
        let meta = MpuMeta {
            content_type,
            user_metadata,
            created_nanos: now_nanos(),
        };
        let bytes = rmp_serde::to_vec_named(&meta).map_err(|e| Error::Encode(e.to_string()))?;
        tokio::fs::write(dir.join(MPU_META_FILE), bytes).await?;
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
        mut body: ByteStream,
    ) -> Result<String> {
        if !(1..=MAX_PARTS).contains(&part_number) {
            return Err(Error::InvalidPart(format!(
                "part number {part_number} 超出 1..={MAX_PARTS}"
            )));
        }
        self.ensure_bucket(bucket).await?;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
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
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u16, String)],
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;
        let _guard = self.lock_for(bucket, key).lock_owned().await;

        let obj_dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        let mpu: MpuMeta = rmp_serde::from_slice(&tokio::fs::read(dir.join(MPU_META_FILE)).await?)
            .map_err(|e| Error::CorruptMeta(format!("mpu.meta 解码失败: {e}")))?;

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
        let mut total: u64 = 0;
        let mut paths: Vec<PathBuf> = Vec::with_capacity(parts.len());
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
            total += pm.size;
            paths.push(dir.join(format!("part.{pn}")));
        }

        // multipart ETag = hex(md5(逐段原始 md5 字节拼接)) + "-N"（S3 定义）
        let mut h = Md5::new();
        for raw in &raw_md5s {
            h.update(raw);
        }
        let etag = format!("{}-{}", hex::encode(h.finalize()), parts.len());

        // M-6 同口径：损坏旧 meta 让 complete fail-closed
        let old_meta = self.read_meta(bucket, key).await?;
        let now = now_nanos();

        // 装进新版本目录（内联 / EC 形态）——与单段 PUT 同一形态核心
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(&self.cfg.root, "tmp").join(&uuid);
        let staged: Result<XlMeta> = async {
            let (size, _md5, sha256, data_layout, inline) =
                self.stage_payload(&tmp, concat_stream(paths)).await?;
            if size != total {
                // upload_part 不持对象锁（允许并发传不同 part）：complete 期间零件被并发重传
                // = 声明 etag/长度与盘上字节不一致。fail-closed，让客户端重试 complete
                return Err(Error::InvalidPart(format!(
                    "part 数据在 complete 期间被并发修改（声明 {total} 字节，盘上 {size}）"
                )));
            }
            let version = Version {
                version_id: uuid::Uuid::nil(),
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
                parts: None,    // 裁决：新写入不再使用 parts（降为 M2a 旧对象只读兼容位）
                checksum: None, // multipart 组合校验和留 M2b
                data_layout,
                inline,
            };
            let meta = XlMeta {
                object_key: key.to_string(),
                versions: vec![version],
            };
            tokio::fs::write(tmp.join(crate::meta::META_FILE), meta.encode()?).await?;
            Ok(meta)
        }
        .await;
        let meta = match staged {
            Ok(m) => m,
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

        // 数据就位（同盘 rename）→ 唯一提交点
        tokio::fs::create_dir_all(&obj_dir).await?;
        tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;
        self.write_meta(bucket, key, &meta).await?;

        // 暂存区收尾（best-effort：提交点已过）：mpu.meta 生死门先改名，再整目录删。
        // 若删到一半崩溃，残骸无 meta 引用，由 `reap_stale` 按 mtime 回收
        let _ = tokio::fs::rename(dir.join(MPU_META_FILE), dir.join(MPU_DONE_FILE)).await;
        let _ = tokio::fs::remove_dir_all(&dir).await;

        // 老版本进回收站（与单段 PUT 相同）
        if let Some(old) = old_meta.as_ref().and_then(|m| m.latest()) {
            let old_dir = obj_dir.join(&old.part.uuid);
            let trash = layout::sys_dir(&self.cfg.root, "trash").join(&old.part.uuid);
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size: total,
            etag,
            content_type: mpu.content_type,
            last_modified_nanos: now,
            user_metadata: mpu.user_metadata,
            checksum: None,
        })
    }

    /// 中止 MPU：删暂存区。`mpu.meta` 为生死门——complete 成功后它已改名，不得再删。
    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;
        let _guard = self.lock_for(bucket, key).lock_owned().await;
        let dir = self.mpu_dir(bucket, key, upload_id)?;
        if !tokio::fs::try_exists(dir.join(MPU_META_FILE)).await? {
            return Err(Error::NoSuchUpload(upload_id.to_string()));
        }
        tokio::fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    /// 列出暂存区里已上传的 parts（升序）。分页不做：≤10000 全量、`is_truncated=false`。
    pub async fn list_parts(
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

/// 按声明顺序把暂存的 `part.N` 文件拼成一个 64 KiB 块的 `ByteStream`（complete 装对象用）。
/// 逐块读、不整份进内存——内存峰值留给 `stage_payload` 的 encode 一步（与单段 PUT 同口径）。
fn concat_stream(paths: Vec<PathBuf>) -> ByteStream {
    type St = (std::vec::IntoIter<PathBuf>, Option<tokio::fs::File>);
    let state: St = (paths.into_iter(), None);
    Box::pin(futures::stream::try_unfold(
        state,
        |(mut files, mut cur): St| async move {
            loop {
                // 不用 `match &mut cur`（Polonius 未落地：在另一分支里给 cur 赋值会被借用检查拒绝）
                if cur.is_none() {
                    match files.next() {
                        Some(p) => cur = Some(tokio::fs::File::open(&p).await?),
                        None => return Ok(None),
                    }
                }
                let f = cur.as_mut().expect("刚填过");
                let mut buf = vec![0u8; 64 * 1024];
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    cur = None; // 本段读完，切下一段
                    continue;
                }
                buf.truncate(n);
                return Ok(Some((Bytes::from(buf), (files, cur))));
            }
        },
    ))
}
