//! s3s 的 `S3` trait 实现。M1 覆盖：桶 CRUD、对象 CRUD、ListObjectsV2、CopyObject、批量删除。
//! Multipart / Range / 版本 / 加密 / 锁 / 复制分别是 M2/M5/M6 的事。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http::StatusCode;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result};
use xl_store::error::Error as StoreError;
use xl_store::store::{ListRequest, ObjectInfo, Store};

use crate::convert::to_s3_error;

pub struct OssS3 {
    store: Arc<Store>,
    /// 单次 PUT 的整份读入上限，见 `put_object` 的 ponytail 取舍。
    max_put_bytes: usize,
}

impl OssS3 {
    pub fn new(store: Arc<Store>, max_put_bytes: usize) -> Self {
        Self {
            store,
            max_put_bytes,
        }
    }
}

/// nanos → S3 时间戳：`LastModified` 与 `CreationDate` 同为 `Timestamp` 别名，
/// 对象用 `mod_time_nanos`、桶用 `creation_nanos`。不要用 now() 冒充。
fn last_modified(nanos: i64) -> LastModified {
    LastModified::from(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos.max(0) as u64))
}

/// 流上限包装：累计超限置 `exceeded`、读取出错置 `body_err`——随后由 [`stream_err_to_s3`]
/// 换成稳定错误码（不把 413 / IncompleteBody 的语义沉进内核错误类型）。
type LimitedStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, xl_store::error::Error>> + Send>>;

fn limit_stream(
    body: StreamingBlob,
    max: usize,
) -> (LimitedStream, Arc<AtomicBool>, Arc<AtomicBool>) {
    let exceeded = Arc::new(AtomicBool::new(false));
    let exceeded_in_stream = Arc::clone(&exceeded);
    let body_err = Arc::new(AtomicBool::new(false));
    let body_err_in_stream = Arc::clone(&body_err);
    let mut seen = 0usize;
    let stream = futures::StreamExt::map(body, move |chunk| {
        let chunk = chunk.map_err(|e| {
            tracing::warn!(error = ?e, "读取请求体失败");
            body_err_in_stream.store(true, Ordering::Relaxed);
            xl_store::error::Error::Io(std::io::Error::other(format!("request body: {e}")))
        })?;
        seen += chunk.len();
        if seen > max {
            exceeded_in_stream.store(true, Ordering::Relaxed);
            return Err(xl_store::error::Error::Io(std::io::Error::other(
                "request body exceeds max_put_bytes",
            )));
        }
        Ok(chunk)
    });
    (Box::pin(stream), exceeded, body_err)
}

/// [`limit_stream`] 的标志位 → 稳定错误码；无标志时走通用映射。
fn stream_err_to_s3(
    e: xl_store::error::Error,
    exceeded: &AtomicBool,
    body_err: &AtomicBool,
) -> s3s::S3Error {
    if exceeded.load(Ordering::Relaxed) {
        return s3s::s3_error!(EntityTooLarge, "object exceeds max_put_bytes");
    }
    if body_err.load(Ordering::Relaxed) {
        // 与 M1 口径一致：请求体读取失败 = 客户端侧不完整（而不是 500）
        return s3s::s3_error!(IncompleteBody, "failed to read request body");
    }
    to_s3_error(e)
}

/// 条件请求求值（RFC 9110 §13.2.2 顺序）：If-Match → If-Unmodified-Since →
/// If-None-Match → If-Modified-Since，任一有决定即短路。
/// 返回 `Err(NOT_MODIFIED)`（304，调用方回空 body）或 `Err(PRECONDITION_FAILED)`（412）。
///
/// ponytail: 求值基于 head 快照——与随后读盘之间是 TOCTOU 窗（并发覆写时条件可能对着
/// 旧版本判、数据读到新版本）；无版本控制语义下接受，M4 版本控制落地时改为对版本求值。
fn eval_preconditions(
    info: &ObjectInfo,
    if_match: Option<&ETagCondition>,
    if_unmodified_since: Option<&Timestamp>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
) -> Result<(), StatusCode> {
    let etag_matches = |c: &ETagCondition| match c {
        // 能走到求值处，实体必存在 → `*` 恒真
        ETagCondition::Any => true,
        // 值比较、不区分强弱：我们的 ETag 恒为 Strong 无引号 hex，客户端回传任意强弱按值比。
        // ponytail: 弱校验语义（W/ 差异）不细分；有真实互操作诉求时按 RFC 9110 §8.8.3 收严
        ETagCondition::ETag(e) => e.value() == info.etag,
    };
    let last = last_modified(info.last_modified_nanos);
    if let Some(c) = if_match {
        if !etag_matches(c) {
            return Err(StatusCode::PRECONDITION_FAILED);
        }
    } else if let Some(t) = if_unmodified_since
        && last > *t
    {
        return Err(StatusCode::PRECONDITION_FAILED);
    }
    if let Some(c) = if_none_match {
        if etag_matches(c) {
            return Err(StatusCode::NOT_MODIFIED);
        }
    } else if let Some(t) = if_modified_since
        && last <= *t
    {
        return Err(StatusCode::NOT_MODIFIED);
    }
    Ok(())
}

#[async_trait::async_trait]
impl S3 for OssS3 {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        self.store
            .create_bucket(&bucket)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = req.input.bucket;
        match self.store.bucket_exists(&bucket).await {
            Ok(true) => Ok(S3Response::new(HeadBucketOutput::default())),
            Ok(false) => Err(s3s::s3_error!(NoSuchBucket)),
            Err(e) => Err(to_s3_error(e)),
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = req.input.bucket;
        self.store
            .delete_bucket(&bucket)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.store.list_buckets().await.map_err(to_s3_error)?;
        let list = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(last_modified(b.creation_nanos)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(list),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput {
            bucket,
            key,
            body,
            content_type,
            content_length,
            metadata,
            ..
        } = req.input;
        let body = body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;

        // 声明长度已知且超限：不读体直接 413
        if let Some(cl) = content_length {
            if cl < 0 {
                return Err(s3s::s3_error!(InvalidRequest, "negative content-length"));
            }
            if cl as usize > self.max_put_bytes {
                return Err(s3s::s3_error!(
                    EntityTooLarge,
                    "object exceeds max_put_bytes"
                ));
            }
        }

        // 流式直通内核（不再整份进内存）；content-length 可缺省/不可信，流上计数兜底。
        let (stream, exceeded, body_err) = limit_stream(body, self.max_put_bytes);

        let result = self
            .store
            .put_object_stream_with_meta(
                &bucket,
                &key,
                stream,
                content_type,
                metadata.unwrap_or_default().into_iter().collect(),
            )
            .await;
        let info = match result {
            Ok(info) => info,
            Err(e) => return Err(stream_err_to_s3(e, &exceeded, &body_err)),
        };

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput {
            bucket,
            key,
            content_type,
            metadata,
            ..
        } = req.input;
        let upload_id = self
            .store
            .create_multipart_upload(
                &bucket,
                &key,
                content_type,
                metadata.unwrap_or_default().into_iter().collect(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            bucket,
            key,
            upload_id,
            part_number,
            body,
            content_length,
            ..
        } = req.input;
        if !(1..=10_000).contains(&part_number) {
            return Err(s3s::s3_error!(InvalidArgument, "part number out of range"));
        }
        let body = body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;
        // 单 part 上限沿用 max_put_bytes（multipart 大对象整体不受它约束、按 part 计）
        if let Some(cl) = content_length {
            if cl < 0 {
                return Err(s3s::s3_error!(InvalidRequest, "negative content-length"));
            }
            if cl as usize > self.max_put_bytes {
                return Err(s3s::s3_error!(EntityTooLarge, "part exceeds max_put_bytes"));
            }
        }
        let (stream, exceeded, body_err) = limit_stream(body, self.max_put_bytes);
        let etag = match self
            .store
            .upload_part(&bucket, &key, &upload_id, part_number as u16, stream)
            .await
        {
            Ok(etag) => etag,
            Err(e) => return Err(stream_err_to_s3(e, &exceeded, &body_err)),
        };
        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            bucket,
            key,
            upload_id,
            multipart_upload,
            ..
        } = req.input;
        let Some(mu) = multipart_upload else {
            return Err(s3s::s3_error!(InvalidRequest, "missing multipart_upload"));
        };
        let Some(parts) = mu.parts else {
            return Err(s3s::s3_error!(InvalidRequest, "missing parts"));
        };
        let mut pairs = Vec::with_capacity(parts.len());
        for p in &parts {
            // 缺字段一并按 InvalidPart（规格只写了 ETag 不一致的情形，缺字段同属客户端报错）
            let (Some(pn), Some(etag)) = (p.part_number, p.e_tag.as_ref()) else {
                return Err(s3s::s3_error!(InvalidPart, "part missing number or etag"));
            };
            if !(1..=10_000).contains(&pn) {
                return Err(s3s::s3_error!(InvalidPart, "part number out of range"));
            }
            pairs.push((pn as u16, etag.value().to_string()));
        }
        let info = self
            .store
            .complete_multipart_upload(&bucket, &key, &upload_id, &pairs)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.store
            .abort_multipart_upload(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;
        let upload_id = req.input.upload_id;
        let rows = self
            .store
            .list_parts(&bucket, &key, &upload_id)
            .await
            .map_err(to_s3_error)?;
        let parts = rows
            .into_iter()
            .map(|p| Part {
                part_number: Some(p.part_number as i32),
                size: Some(p.size as i64),
                e_tag: Some(ETag::Strong(p.etag)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts),
            // 分页不做：≤10000 全量
            is_truncated: Some(false),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;

        // 条件求值需要 info：统一先取一次 head（Range 分支本来也要 size）；条件失败不读盘上数据。
        let info = self
            .store
            .head_object(&bucket, &key)
            .await
            .map_err(to_s3_error)?;
        if let Err(status) = eval_preconditions(
            &info,
            req.input.if_match.as_ref(),
            req.input.if_unmodified_since.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
        ) {
            if status == StatusCode::NOT_MODIFIED {
                // 304 无 body（空 output 即可）
                return Ok(S3Response::with_status(
                    GetObjectOutput::default(),
                    StatusCode::NOT_MODIFIED,
                ));
            }
            return Err(s3s::s3_error!(PreconditionFailed, "precondition failed"));
        }

        // Range → 有界读 + 206/416；无 Range → 整读 + 200。两条路共用同一份响应组装。
        let (bytes, content_range, status) = match req.input.range {
            Some(range) => {
                // 越界 → RangeNotSatisfiable → InvalidRange(416)
                let rr = range.check(info.size).map_err(s3s::S3Error::from)?;
                let (start, end) = (rr.start, rr.end);
                let bytes = if start == 0 && end == info.size {
                    // 显式全段 Range：仍回 206（S3 语义），数据走整读
                    self.store
                        .get_object(&bucket, &key)
                        .await
                        .map_err(to_s3_error)?
                        .1
                } else {
                    self.store
                        .get_object_range(&bucket, &key, start..end)
                        .await
                        .map_err(to_s3_error)?
                };
                // check 保证 end >= 1（start <= last < full_length 且区间非空）
                let cr = format!("bytes {}-{}/{}", start, end - 1, info.size);
                (bytes, Some(cr), StatusCode::PARTIAL_CONTENT)
            }
            None => {
                let bytes = self
                    .store
                    .get_object(&bucket, &key)
                    .await
                    .map_err(to_s3_error)?
                    .1;
                (bytes, None, StatusCode::OK)
            }
        };
        let len = bytes.len() as i64;

        let mut out = GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(bytes)),
            content_length: Some(len),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            accept_ranges: Some("bytes".to_string()),
            content_range,
            // 空集也回 Some(空)：AWS 语义下与 None 无差别，客户端按「无自定义元数据」处理
            metadata: Some(info.user_metadata.into_iter().collect()),
            ..Default::default()
        };
        // response-content-* 是响应头覆盖：客户端给什么值就原样下发（含 &filename= 的 RFC 5987
        // 编码由客户端负责——服务端无编码逻辑，见规格 §3.2）。
        if let Some(v) = req.input.response_content_type {
            out.content_type = Some(v);
        }
        if let Some(v) = req.input.response_content_disposition {
            out.content_disposition = Some(v);
        }
        if let Some(v) = req.input.response_cache_control {
            out.cache_control = Some(v);
        }
        if let Some(v) = req.input.response_content_language {
            out.content_language = Some(v);
        }
        if let Some(v) = req.input.response_content_encoding {
            out.content_encoding = Some(v);
        }
        if let Some(ts) = req.input.response_expires {
            // Timestamp::format 吃 io::Write；String 只有 fmt::Write——用 Vec<u8> 缓冲再转
            let mut buf = Vec::new();
            ts.format(TimestampFormat::HttpDate, &mut buf)
                .expect("HttpDate 格式化到 Vec 不会失败");
            out.expires = Some(String::from_utf8(buf).expect("HttpDate 只含 ASCII"));
        }

        Ok(S3Response::with_status(out, status))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let info = self
            .store
            .head_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        // 条件与 GET 同口径（§3.3 的「错误一律 404」只罩读错误；304/412 是条件结果，照常返回）
        if let Err(status) = eval_preconditions(
            &info,
            req.input.if_match.as_ref(),
            req.input.if_unmodified_since.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
        ) {
            if status == StatusCode::NOT_MODIFIED {
                return Ok(S3Response::with_status(
                    HeadObjectOutput::default(),
                    StatusCode::NOT_MODIFIED,
                ));
            }
            return Err(s3s::s3_error!(PreconditionFailed, "precondition failed"));
        }
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(info.size as i64),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            metadata: Some(info.user_metadata.into_iter().collect()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.store
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket;
        // §3.4：一次最多 1000 个
        if req.input.delete.objects.len() > 1000 {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "too many objects in a single delete request"
            ));
        }

        // §3.4 部分失败语义：逐键删，成功的进 deleted、键级错误进 errors 后继续；
        // 桶级错误（桶不存在/桶名不合法）是请求级错误，整体返回 Err。
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in req.input.delete.objects {
            match self.store.delete_object(&bucket, &obj.key).await {
                Ok(()) => deleted.push(DeletedObject {
                    key: Some(obj.key),
                    ..Default::default()
                }),
                Err(e @ (StoreError::NoSuchBucket(_) | StoreError::InvalidBucketName(_))) => {
                    return Err(to_s3_error(e));
                }
                Err(e) => {
                    // 经 to_s3_error：4xx 保留原文，5xx 细节只进日志
                    let s3err = to_s3_error(e);
                    errors.push(Error {
                        key: Some(obj.key),
                        code: Some(s3err.code().as_str().to_string()),
                        message: Some(s3err.message().unwrap_or_default().to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            prefix,
            delimiter,
            max_keys,
            continuation_token,
            ..
        } = req.input;

        // max-keys < 0：i32 → usize 的 `as` 会回绕成巨值、再被 store 的 .min(1000) 钳成 1000，
        // 非法参数被静默当默认值。报 InvalidRequest，不猜客户端意图。
        let max_keys = match max_keys {
            Some(m) if m < 0 => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "max-keys must not be negative"
                ));
            }
            other => other.map(|m| m as usize),
        };

        let result = self
            .store
            .list_objects(
                &bucket,
                &ListRequest {
                    prefix,
                    delimiter,
                    max_keys,
                    continuation_token,
                },
            )
            .await
            .map_err(to_s3_error)?;

        let contents: ObjectList = result
            .objects
            .iter()
            .map(|o| Object {
                key: Some(o.key.clone()),
                size: Some(o.size as i64),
                e_tag: Some(ETag::Strong(o.etag.clone())),
                last_modified: Some(last_modified(o.last_modified_nanos)),
                storage_class: Some(ObjectStorageClass::from_static(
                    ObjectStorageClass::STANDARD,
                )),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            contents: Some(contents),
            key_count: Some(result.objects.len() as i32),
            common_prefixes: Some(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            is_truncated: Some(result.is_truncated),
            next_continuation_token: result.next_token,
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // s3s 已将 x-amz-copy-source 解码、去掉前导 '/' 并解析成枚举；这里只接受桶形式
        let CopySource::Bucket {
            ref bucket,
            ref key,
            ..
        } = req.input.copy_source
        else {
            return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
        };

        // 规格 §3.5 边界：M1 无 metadata-directive REPLACE / 存储类变更概念，
        // src == dst（同桶同键）一律拒（S3 语义：InvalidRequest）
        if bucket.as_ref() == req.input.bucket.as_str() && key.as_ref() == req.input.key.as_str() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "copy source and destination are the same"
            ));
        }

        let info = self
            .store
            .copy_object(bucket, key, &req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(info.etag)),
                // 与 get/head_object 同一翻译：返回 dst 对象的真实 mtime，不用 now() 冒充
                last_modified: Some(last_modified(info.last_modified_nanos)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
