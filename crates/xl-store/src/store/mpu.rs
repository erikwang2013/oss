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
        Ok(layout::object_dir(self.cfg.root(), bucket, key)?.join(id.to_string()))
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
        let dir = layout::object_dir(self.cfg.root(), bucket, key)?.join(&upload_id);
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

        let obj_dir = layout::object_dir(self.cfg.root(), bucket, key)?;
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

        // M-6 同口径：损坏旧 meta 让 complete fail-closed
        let old_meta = self.read_meta(bucket, key).await?;
        let now = now_nanos();

        // 装进新版本目录（内联 / EC 形态）——与单段 PUT 同一形态核心
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(self.cfg.root(), "tmp").join(&uuid);
        let multi = self.cfg.multi_drive();
        let staged: Result<(u64, XlMeta)> = async {
            // concat_stream_verified 逐段核验 bytes == sidecar 快照，通过则 size 恒等于各段总和。
            // upload_part 不持对象锁（允许并发传不同 part）：complete 期间零件被并发重传
            // = 快照与实际字节不一致 → InvalidPart（fail-closed，让客户端重试 complete）
            let (size, _md5, sha256, data_layout, inline) = if multi {
                let (s, e, sh, l, i, _touched) = self
                    .stage_payload_multi(
                        bucket,
                        key,
                        &uuid,
                        &tmp,
                        concat_stream_verified(snapshots),
                    )
                    .await?;
                (s, e, sh, l, i)
            } else {
                self.stage_payload(&tmp, concat_stream_verified(snapshots))
                    .await?
            };
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
            if !multi {
                tokio::fs::write(tmp.join(crate::meta::META_FILE), meta.encode()?).await?;
            }
            Ok((size, meta))
        }
        .await;
        let (size, meta) = match staged {
            Ok(v) => v,
            Err(e) => {
                if multi {
                    self.rollback_versions(bucket, key, &uuid).await;
                }
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

        if multi {
            // 多盘：两阶段 meta 提交（数据已在各盘版本目录就位；见 put.rs 同款流程）
            let quorum = self.cfg.ec.data as usize + 1;
            let flipped = match self.write_meta(bucket, key, &meta).await {
                Ok(n) => n,
                Err(e) => {
                    self.rollback_versions(bucket, key, &uuid).await;
                    let _ = tokio::fs::remove_dir_all(&tmp).await;
                    return Err(e);
                }
            };
            if flipped == 0 {
                self.rollback_versions(bucket, key, &uuid).await;
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(Error::QuorumUnmet("meta 翻转全失败，未提交".into()));
            }
            if flipped < quorum {
                tracing::warn!(
                    flipped,
                    quorum,
                    "meta 翻转不足 quorum（已提交，M3 heal 补副本）"
                );
            }
            let _ = tokio::fs::remove_dir_all(&tmp).await;
        } else {
            // 数据就位（同盘 rename）→ 唯一提交点
            tokio::fs::create_dir_all(&obj_dir).await?;
            tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;
            self.write_meta(bucket, key, &meta).await?;
        }

        // 暂存区收尾（best-effort：提交点已过）：mpu.meta 生死门先改名，再整目录删。
        // 若删到一半崩溃，残骸无 meta 引用，由 `reap_stale` 按 mtime 回收
        let _ = tokio::fs::rename(dir.join(MPU_META_FILE), dir.join(MPU_DONE_FILE)).await;
        let _ = tokio::fs::remove_dir_all(&dir).await;

        // 老版本进回收站（跨盘遍历，与单段 PUT 相同）
        self.recycle_old(bucket, key, old_meta.as_ref()).await;

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
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

/// 按声明顺序把暂存的 `part.N` 拼成 64 KiB 块的 `ByteStream`（complete 装对象用），
/// 并对每段做**字节级核验**：段内容与 sidecar 快照（etag/sha256/长度）不符 →
/// `InvalidPart`（complete 期间被并发重传；核验对象正是即将提交的字节，竞态在核验点收口）。
///
/// 逐块读、不整份进内存——内存峰值留给 `stage_payload` 的 encode 一步（与单段 PUT 同口径；
/// ponytail: complete 的整体峰值 ≈ 2.5× 对象，P 级大对象受此限制，ShardByShard 流式编码是
/// M2b-3 之后的升级路径）。
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
