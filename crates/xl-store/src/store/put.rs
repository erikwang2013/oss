//! 写路径：整份/流式 PUT（提交协议）与候选 meta 落盘。
//! 提交协议（顺序不可变，见 `put_object_stream_with_meta` 的文档注释）。

use std::collections::BTreeMap;

use bytes::Bytes;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use crate::error::Result;
use crate::layout;
use crate::meta::{self, ChecksumRecord, PartRef, Version, XlMeta};

use super::{ByteStream, ObjectInfo, Store, now_nanos};

impl Store {
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_with_meta(bucket, key, body, content_type, Default::default())
            .await
    }

    /// 带用户元数据的整份 PUT。metadata 是**全量替换**（S3 语义，不是合并）。
    pub async fn put_object_with_meta(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(
            bucket,
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
            content_type,
            user_metadata,
            None,
        )
        .await
    }

    /// 提交协议（顺序不可变）：
    ///   1. 分片/数据写 `.oss.sys/tmp/<uuid>/part.1`，边写边算 MD5+SHA256+size
    ///   2. 候选 `xl.meta` 写进同一个 tmp 目录
    ///   3. **同盘 rename** tmp/<uuid> → <bucket>/<obj>/<uuid>     ← 数据就位
    ///   4. 写 `<obj>/xl.meta.new` 并 fsync，再 rename 覆盖 `xl.meta` ← **提交点**
    ///   5. 老版本目录 → `.oss.sys/trash/`（移入 trash，由 GC 回收——M1 未实现回收器）
    ///
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，M1 无回收器，残留只占空间）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，同上）。读路径只信 xl.meta。
    ///
    /// M1 持久化假设：`fsync=true` 时数据文件与 `xl.meta.new` 都先做 file fsync；
    /// ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可。**没有目录 fsync**，
    /// 换文件系统（或要求断电零丢失）需复核这两条假设。
    pub async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(bucket, key, body, content_type, Default::default(), None)
            .await
    }

    /// 流式 PUT 的完整形态（含用户元数据）；`put_object_stream` 是空元数据的薄壳。
    pub async fn put_object_stream_with_meta(
        &self,
        bucket: &str,
        key: &str,
        mut body: ByteStream,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
        checksum: Option<ChecksumRecord>,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;

        let _guard = self.lock_for(bucket, key).lock_owned().await;

        // M-6 裁决：损坏的 meta 让 PUT fail-closed（报错），不静默当作空对象覆盖——盘上可能还有旧数据，宁可失败也不毁
        let old_meta = self.read_meta(bucket, key).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(&self.cfg.root, "tmp").join(&uuid);
        let now = now_nanos();

        // 1)+2) 数据与候选 meta 落 tmp；任何写失败/流错误都 best-effort 清掉本轮 tmp
        //（未提交前 tmp 不被任何 meta 引用，失败即弃；残留只占空间，由 `oss-server clean` 按 mtime 回收）
        let staged: Result<(u64, String, XlMeta)> = async {
            tokio::fs::create_dir_all(&tmp).await?;

            // 1) 流式写数据 + 增量哈希
            let part_path = tmp.join("part.1");
            let mut file = tokio::fs::File::create(&part_path).await?;
            let mut md5 = Md5::new();
            let mut sha = Sha256::new();
            let mut size: u64 = 0;
            while let Some(chunk) = futures::StreamExt::next(&mut body).await {
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

            let etag = hex::encode(md5.finalize());
            let version = Version {
                version_id: uuid::Uuid::nil(), // M1 无版本控制：nil = S3 的 "null" 版本
                mod_time_nanos: now,
                size,
                etag: etag.clone(),
                content_type: content_type.clone(),
                user_metadata: user_metadata.clone(),
                part: PartRef {
                    uuid: uuid.clone(),
                    part: 1,
                    size,
                    sha256: hex::encode(sha.finalize()),
                },
                // 单段 PUT 保持与旧文件同形的 None（MPU complete 才写 Some）
                parts: None,
                checksum: checksum.clone(),
                // M2b 起写路径分叉（Task 4/6 落地）；本任务先保持旧形态
                data_layout: None,
                inline: None,
            };
            let meta = XlMeta {
                object_key: key.to_string(),
                versions: vec![version],
            };

            // 2) 候选 meta 落 tmp
            tokio::fs::write(tmp.join(meta::META_FILE), meta.encode()?).await?;
            Ok((size, etag, meta))
        }
        .await;
        let (size, etag, meta) = match staged {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

        // 3) 同盘 rename：数据就位（此时对读路径仍不可见）
        let obj_dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        tokio::fs::create_dir_all(&obj_dir).await?;
        tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;

        // 4) 提交点：写 xl.meta.new 再原子覆盖
        self.write_meta(bucket, key, &meta).await?;

        // 5) 老版本进回收站
        if let Some(old) = old_meta.as_ref().and_then(|m| m.latest()) {
            let old_dir = obj_dir.join(&old.part.uuid);
            let trash = layout::sys_dir(&self.cfg.root, "trash").join(&old.part.uuid);
            // best-effort：提交点已过，回收站的任何错误都不该让已提交的 PUT 报失败
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now,
            user_metadata,
            checksum,
        })
    }
}
