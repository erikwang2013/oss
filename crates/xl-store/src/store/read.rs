//! 读路径：HEAD / 整读 / Range 有界读（含并发覆写时的 meta 重读重试）。

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::XlMeta;

use super::{ObjectInfo, Store};

impl Store {
    pub(in crate::store) fn load_object_info(
        &self,
        bucket: &str,
        key: &str,
        m: &XlMeta,
    ) -> Result<ObjectInfo> {
        let v = m
            .latest()
            .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: v.size,
            etag: v.etag.clone(),
            content_type: v.content_type.clone(),
            last_modified_nanos: v.mod_time_nanos,
            user_metadata: v.user_metadata.clone(),
            checksum: v.checksum.clone(),
        })
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        self.load_object_info(bucket, key, &m)
    }

    /// 读对象：一次 meta 读取定 info 与数据文件位置。
    ///
    /// 读路径不加锁，meta 与数据文件之间隔着一次 open：覆写者在提交点之后才把老版本目录移进
    /// trash，所以先读到的 meta 可能指向刚被移走的版本目录（数据文件 ENOENT）。此时重读一次
    /// meta 拿新版本再试；两轮都读不到 = 对象确实不在（或被并发删除）→ NoSuchKey。
    /// ponytail: 固定重试一次；持续覆写风暴下不保证收敛（M2 快照读解决）。
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;

        for _ in 0..2 {
            let m = self
                .read_meta(bucket, key)
                .await?
                .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
            let info = self.load_object_info(bucket, key, &m)?;
            let parts = v.effective_parts();
            // ponytail: M2a 整份读进内存（跨 part 拼接，与既有限制同口径）；M2b 换流式。
            let mut buf = Vec::with_capacity(v.size as usize);
            let mut missing = false;
            for p in &parts {
                let path = dir.join(&p.uuid).join(format!("part.{}", p.part));
                match tokio::fs::read(&path).await {
                    Ok(bytes) => buf.extend_from_slice(&bytes),
                    // 旧版本目录已被并发覆写者移走：下一轮重读 meta 拿新版本
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        missing = true;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            if missing {
                continue;
            }
            return Ok((info, Bytes::from(buf)));
        }
        // 两轮 meta → 数据文件都 ENOENT：按 S3 语义报 NoSuchKey，而不是把裸 IO 错误抛给上层
        Err(Error::NoSuchKey(key.to_string()))
    }

    /// 有界读：`range` 为半开区间 `[start, end)`，须满足 `start <= end <= size`
    ///（越界由调用方——s3s 的 `Range::check`——先拦；内核仍防呆，防呆错误是内部错误）。
    /// 重试语义与 `get_object` 相同（并发覆写时重读 meta 一轮）。
    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;

        for _ in 0..2 {
            let m = self
                .read_meta(bucket, key)
                .await?
                .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;

            if range.start > range.end || range.end > v.size {
                return Err(Error::InvalidKey(format!(
                    "range {range:?} 越界（对象 {} 字节）",
                    v.size
                )));
            }

            // 跨 part 映射：按 parts 序列累计偏移，只读命中段（首尾 part 各读一段）。
            // ponytail: M2a 单盘无分片；M2b 起 part 内部再映射到分片。
            let parts = v.effective_parts();
            let mut buf = Vec::with_capacity((range.end - range.start) as usize);
            let mut offset: u64 = 0;
            let mut missing = false;
            for p in &parts {
                let part_end = offset + p.size;
                // 本 part 与请求区间的交集（半开）；空交集跳过
                let s = range.start.max(offset);
                let e = range.end.min(part_end);
                if s < e {
                    let path = dir.join(&p.uuid).join(format!("part.{}", p.part));
                    let mut file = match tokio::fs::File::open(&path).await {
                        Ok(f) => f,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            missing = true;
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    };
                    file.seek(std::io::SeekFrom::Start(s - offset)).await?;
                    let mut chunk = vec![0u8; (e - s) as usize];
                    match file.read_exact(&mut chunk).await {
                        // tokio 的 read_exact 返回 io::Result<usize>（与 std 的 Result<()> 不同）
                        Ok(_) => buf.extend_from_slice(&chunk),
                        // 版本目录被并发覆写者移走/截断：下一轮重读 meta 拿新版本
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            missing = true;
                            break;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                offset = part_end;
                if offset >= range.end {
                    break;
                }
            }
            if missing {
                continue;
            }
            return Ok(Bytes::from(buf));
        }
        Err(Error::NoSuchKey(key.to_string()))
    }
}
