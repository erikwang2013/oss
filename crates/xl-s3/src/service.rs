//! s3s 的 `S3` trait 实现。M1 覆盖：桶 CRUD、对象 CRUD、ListObjectsV2、CopyObject、批量删除。
//! Multipart / Range / 版本 / 加密 / 锁 / 复制分别是 M2/M5/M6 的事。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::StatusCode;
use s3s::checksum::ChecksumHasher;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result};
use xl_store::error::Error as StoreError;
use xl_store::meta::ChecksumRecord;
use xl_store::store::{DeleteOutcome, ListRequest, ObjectInfo, Store};

use crate::convert::to_s3_error;

pub struct OssS3 {
    store: Arc<Store>,
    /// 判权/记账（桶归属、可见性过滤）；授权门本体在 `S3ServiceBuilder::set_access`
    /// （同一个 [`Acl`] 实例的克隆）。
    acl: crate::access::Acl,
    /// 单次 PUT 的整份读入上限，见 `put_object` 的 ponytail 取舍。
    max_put_bytes: usize,
}

impl OssS3 {
    /// 桶版本控制是否 Enabled（读 DB；查库失败上抛 500——状态不可知时不写数据）。
    async fn versioned(&self, bucket: &str) -> S3Result<bool> {
        match self.acl.bucket_versioning(bucket).await {
            Ok(s) => Ok(s == "Enabled"),
            Err(e) => {
                tracing::error!(error = %e, "读取桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    pub fn new(store: Arc<Store>, acl: crate::access::Acl, max_put_bytes: usize) -> Self {
        Self {
            store,
            acl,
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

/// 校验和算法（本计划覆盖 s3s 输入里显式声明的那几种；组合语义留 M2b）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChecksumAlgo {
    Md5,
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    Sha512,
    Crc64Nvme,
}

impl ChecksumAlgo {
    fn name(self) -> &'static str {
        match self {
            Self::Md5 => "MD5",
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Sha512 => "SHA512",
            Self::Crc64Nvme => "CRC64NVME",
        }
    }

    fn enable(self, h: &mut ChecksumHasher) {
        match self {
            Self::Md5 => h.md5 = Some(Default::default()),
            Self::Crc32 => h.crc32 = Some(Default::default()),
            Self::Crc32c => h.crc32c = Some(Default::default()),
            Self::Sha1 => h.sha1 = Some(Default::default()),
            Self::Sha256 => h.sha256 = Some(Default::default()),
            Self::Sha512 => h.sha512 = Some(Default::default()),
            Self::Crc64Nvme => h.crc64nvme = Some(Default::default()),
        }
    }

    fn extract(self, c: &Checksum) -> Option<String> {
        match self {
            Self::Md5 => c.checksum_md5.clone(),
            Self::Crc32 => c.checksum_crc32.clone(),
            Self::Crc32c => c.checksum_crc32c.clone(),
            Self::Sha1 => c.checksum_sha1.clone(),
            Self::Sha256 => c.checksum_sha256.clone(),
            Self::Sha512 => c.checksum_sha512.clone(),
            Self::Crc64Nvme => c.checksum_crc64nvme.clone(),
        }
    }
}

/// 从请求里摘出「唯一」声明的校验和（Content-MD5 与 `x-amz-checksum-*` 家族）。
/// 多值并存且不同 → InvalidRequest（S3 未定义交叉校验，明确拒绝优于静默取一）；
/// 只声明算法而无值 → InvalidRequest（CLI 恒带值；无值无法校验，不猜）。
fn declared_checksum(
    input: &PutObjectInput,
) -> Result<Option<(ChecksumAlgo, String)>, s3s::S3Error> {
    let mut declared: Vec<(ChecksumAlgo, String)> = Vec::new();
    let mut push = |algo: ChecksumAlgo, v: &Option<String>| {
        if let Some(v) = v {
            declared.push((algo, v.clone()));
        }
    };
    push(ChecksumAlgo::Md5, &input.content_md5);
    push(ChecksumAlgo::Md5, &input.checksum_md5);
    push(ChecksumAlgo::Crc32, &input.checksum_crc32);
    push(ChecksumAlgo::Crc32c, &input.checksum_crc32c);
    push(ChecksumAlgo::Sha1, &input.checksum_sha1);
    push(ChecksumAlgo::Sha256, &input.checksum_sha256);
    push(ChecksumAlgo::Sha512, &input.checksum_sha512);
    push(ChecksumAlgo::Crc64Nvme, &input.checksum_crc64nvme);

    match declared.len() {
        0 => {
            if input.checksum_algorithm.is_some() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "checksum algorithm declared without a value"
                ));
            }
            Ok(None)
        }
        1 => Ok(Some(declared.remove(0))),
        // 多个声明：**完全同值**（如 content_md5 与 x-amz-checksum-md5 同值）视为一份；
        // 任一不同 → 拒绝（S3 未定义交叉校验，明确拒绝优于静默取一；算法名进日志便于排查）
        _ => {
            let first = declared[0].clone();
            if declared.iter().all(|d| *d == first) {
                return Ok(Some(first));
            }
            tracing::warn!(
                algorithms = ?declared.iter().map(|(a, _)| a.name()).collect::<Vec<_>>(),
                "请求声明了多个不一致的校验和，拒绝"
            );
            Err(s3s::s3_error!(
                InvalidRequest,
                "conflicting checksums declared"
            ))
        }
    }
}

/// 校验和尾流：边转发边算声明算法的摘要；流结束后比对，不匹配在**流末尾注入错误**，
/// 让内核在提交点之前 fail-closed（tmp 清理走既有失败路径，提交协议零改动）。
struct ChecksumTailStream<S> {
    inner: S,
    algo: ChecksumAlgo,
    hasher: ChecksumHasher,
    expected_base64: String,
    done: bool,
}

impl<S> ChecksumTailStream<S> {
    fn new(inner: S, algo: ChecksumAlgo, expected_base64: String) -> Self {
        let mut hasher = ChecksumHasher::default();
        algo.enable(&mut hasher);
        Self {
            inner,
            algo,
            hasher,
            expected_base64,
            done: false,
        }
    }
}

impl<S> futures::Stream for ChecksumTailStream<S>
where
    S: futures::Stream<Item = Result<Bytes, xl_store::error::Error>> + Unpin,
{
    type Item = Result<Bytes, xl_store::error::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.done = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                this.done = true;
                let hasher = std::mem::take(&mut this.hasher);
                let got = this.algo.extract(&hasher.finalize()).unwrap_or_default();
                if got != this.expected_base64 {
                    return Poll::Ready(Some(Err(xl_store::error::Error::BadDigest(format!(
                        "checksum mismatch: expected {}, got {}",
                        this.expected_base64, got
                    )))));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 校验和记录 → GET 输出字段（GET/HEAD 两条回写路径各一份，类型不同不强行抽象）。
fn apply_checksum_get(out: &mut GetObjectOutput, rec: &xl_store::meta::ChecksumRecord) {
    match rec.algorithm.as_str() {
        "MD5" => out.checksum_md5 = Some(rec.value_base64.clone()),
        "CRC32" => out.checksum_crc32 = Some(rec.value_base64.clone()),
        "CRC32C" => out.checksum_crc32c = Some(rec.value_base64.clone()),
        "SHA1" => out.checksum_sha1 = Some(rec.value_base64.clone()),
        "SHA256" => out.checksum_sha256 = Some(rec.value_base64.clone()),
        "SHA512" => out.checksum_sha512 = Some(rec.value_base64.clone()),
        "CRC64NVME" => out.checksum_crc64nvme = Some(rec.value_base64.clone()),
        other => tracing::warn!(algorithm = other, "落盘校验和算法未知，GET 不回写"),
    }
}

fn apply_checksum_head(out: &mut HeadObjectOutput, rec: &xl_store::meta::ChecksumRecord) {
    match rec.algorithm.as_str() {
        "MD5" => out.checksum_md5 = Some(rec.value_base64.clone()),
        "CRC32" => out.checksum_crc32 = Some(rec.value_base64.clone()),
        "CRC32C" => out.checksum_crc32c = Some(rec.value_base64.clone()),
        "SHA1" => out.checksum_sha1 = Some(rec.value_base64.clone()),
        "SHA256" => out.checksum_sha256 = Some(rec.value_base64.clone()),
        "SHA512" => out.checksum_sha512 = Some(rec.value_base64.clone()),
        "CRC64NVME" => out.checksum_crc64nvme = Some(rec.value_base64.clone()),
        other => tracing::warn!(algorithm = other, "落盘校验和算法未知，HEAD 不回写"),
    }
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
    // ponytail: 时间条件按存盘纳秒与客户端时间直比——客户端回显的 Last-Modified 是秒精度，
    // 毫秒级零头会让 If-Modified-Since 偶发多回一次 200；RFC 按秒比，行为无害，留痕即可
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
        // 桶归属记账（授权门已确认调用者身份并塞了 AuthedUser）：建桶者即桶主。
        // C3：多 set 下缺省**自动指派**（桶数最少的 set；单 set 恒 0 零查询）。
        // 记账失败不失败请求（桶已建成；与 console 同口径——缺行只影响归属判定）
        if let Some(user) = req.extensions.get::<crate::access::AuthedUser>().cloned() {
            let k = self.store.cluster().map_or(1, |c| c.sets.sets.len() as u16);
            let set = match self.acl.pick_least_loaded_set(k).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = ?e, bucket, "set 指派失败，回落 set0");
                    0
                }
            };
            if let Err(e) = self
                .acl
                .record_bucket_owner_set(&bucket, user.uid, set)
                .await
            {
                tracing::warn!(error = ?e, bucket, "桶归属记账失败");
            }
            // M5 切片 0（P4-1）：行变更广播（best-effort；关「别节点陈旧行」窗口）
            self.store.broadcast_bucket_row(&bucket, Some(set)).await;
        }
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
        // 记账清理（授权门已确认桶主/管理员）：清授权行 + 桶行 + 生命周期规则
        if let Err(e) = self.acl.forget_bucket(&bucket).await {
            tracing::warn!(error = ?e, bucket, "删桶记账清理失败");
        }
        if let Err(e) = self.acl.clear_lifecycle_rules(&bucket).await {
            tracing::warn!(error = ?e, bucket, "删桶生命周期规则清理失败");
        }
        if let Err(e) = self.acl.clear_bucket_policy(&bucket).await {
            tracing::warn!(error = ?e, bucket, "删桶策略清理失败");
        }
        // M5 切片 0（P4-1）：删行广播（best-effort）
        self.store.broadcast_bucket_row(&bucket, None).await;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.store.list_buckets().await.map_err(to_s3_error)?;
        // 磁盘列表是权威（历史桶可能没有 DB 行）；非管理员与可见集求交
        let visible: Option<std::collections::HashSet<String>> =
            match req.extensions.get::<crate::access::AuthedUser>().cloned() {
                Some(u) if !u.is_admin => Some(
                    self.acl
                        .visible(u.uid)
                        .await
                        .map_err(crate::access::acl_err)?
                        .into_iter()
                        .collect(),
                ),
                _ => None,
            };
        let list = buckets
            .into_iter()
            .filter(|b| visible.as_ref().is_none_or(|v| v.contains(&b.name)))
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
        // 先于解构求值（只借 input）：解构会部分搬走 metadata 等字段
        let declared = declared_checksum(&req.input)?;
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
        // 校验和包装在限流之外：限流先标注 413/断流，校验和流再验证；
        // 注入的 BadDigest 不会被限流标志吞掉（标志只由限流层的 map 置位）。
        let (stream, checksum_record): (LimitedStream, Option<ChecksumRecord>) = match declared {
            Some((algo, expected)) => {
                let rec = ChecksumRecord {
                    algorithm: algo.name().to_string(),
                    value_base64: expected.clone(),
                };
                (
                    Box::pin(ChecksumTailStream::new(stream, algo, expected)),
                    Some(rec),
                )
            }
            None => (stream, None),
        };

        let versioned = self.versioned(&bucket).await?;
        let result = self
            .store
            .put_object_stream_with_meta(
                &bucket,
                &key,
                stream,
                content_type,
                metadata.unwrap_or_default().into_iter().collect(),
                checksum_record,
                versioned,
            )
            .await;
        let info = match result {
            Ok(info) => info,
            Err(e) => return Err(stream_err_to_s3(e, &exceeded, &body_err)),
        };

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            version_id: info.version_id.clone(),
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
        let versioned = self.versioned(&bucket).await?;
        let info = self
            .store
            .complete_multipart_upload_versioned(&bucket, &key, &upload_id, &pairs, versioned)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(info.etag)),
            version_id: info.version_id.clone(),
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
        let version_id = req.input.version_id.clone();

        // 条件求值需要 info：统一先取一次 head（Range 分支本来也要 size）；条件失败不读盘上数据。
        // M4：带 versionId 时点读该版本（marker 版本 → 405 DeleteMarker，经 to_s3_error）。
        let info = match &version_id {
            Some(v) => self.store.head_object_version(&bucket, &key, v).await,
            None => self.store.head_object(&bucket, &key).await,
        }
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
                    match &version_id {
                        Some(v) => self.store.get_object_version(&bucket, &key, v).await,
                        None => self.store.get_object(&bucket, &key).await,
                    }
                    .map_err(to_s3_error)?
                    .1
                } else {
                    match &version_id {
                        Some(v) => {
                            self.store
                                .get_object_range_version(&bucket, &key, v, start..end)
                                .await
                        }
                        None => self.store.get_object_range(&bucket, &key, start..end).await,
                    }
                    .map_err(to_s3_error)?
                };
                // check 保证 end >= 1（start <= last < full_length 且区间非空）
                let cr = format!("bytes {}-{}/{}", start, end - 1, info.size);
                (bytes, Some(cr), StatusCode::PARTIAL_CONTENT)
            }
            None => {
                let bytes = match &version_id {
                    Some(v) => self.store.get_object_version(&bucket, &key, v).await,
                    None => self.store.get_object(&bucket, &key).await,
                }
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
            // 透出实际读到的版本号（点名版本 = 该版本；未点名 = 当前最新）
            version_id: info.version_id.clone(),
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

        // 校验和回写（GET）：仅整读（200）——206 的 body 只是片段，回整对象校验和会让
        // 客户端的响应校验（新版 AWS SDK 默认开启）必然失败；S3 对分段响应也不返回整对象校验和。
        if status != StatusCode::PARTIAL_CONTENT
            && let Some(rec) = &info.checksum
        {
            apply_checksum_get(&mut out, rec);
        }

        Ok(S3Response::with_status(out, status))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let version_id = req.input.version_id.clone();
        let info = match &version_id {
            Some(v) => {
                self.store
                    .head_object_version(&req.input.bucket, &req.input.key, v)
                    .await
            }
            None => {
                self.store
                    .head_object(&req.input.bucket, &req.input.key)
                    .await
            }
        }
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
        let mut out = HeadObjectOutput {
            content_length: Some(info.size as i64),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            metadata: Some(info.user_metadata.into_iter().collect()),
            version_id: info.version_id.clone(),
            ..Default::default()
        };
        // 校验和回写（HEAD）
        if let Some(rec) = &info.checksum {
            apply_checksum_head(&mut out, rec);
        }
        Ok(S3Response::new(out))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;
        let vid = req.input.version_id.clone();
        let versioned = self.versioned(&bucket).await?;
        let outcome = self
            .store
            .delete_object_versioned(&bucket, &key, vid.as_deref(), versioned)
            .await
            .map_err(to_s3_error)?;
        // 原记档「s3s 无 VersionId 字段」不实（verifier 纠错）：DeleteObjectOutput 有
        // version_id 且 s3s 序列化 x-amz-version-id——marker/被删版本都把 id 回给客户端。
        let (dm, vid) = match outcome {
            DeleteOutcome::Marker { version_id } => (Some(true), Some(version_id)),
            DeleteOutcome::Version { version_id } => (None, Some(version_id)),
            DeleteOutcome::Object => (None, None),
        };
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: dm,
            version_id: vid,
            ..Default::default()
        }))
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
        let versioned = self.versioned(&bucket).await?;
        // 逐键授权（M4 切片 7 / verifier 阻断 1c）：批量删的键在请求体里、check() 阶段不可见，
        // 授权在此逐键完成（策略 Deny/Allow/回落表三档，与单键删同口径；AWS 亦逐键授权）。
        // 判定基础设施失败 = 该键 fail-closed；被拒键记 AccessDenied 进 errors 后继续（§3.4 部分失败）。
        let user = req.extensions.get::<crate::access::AuthedUser>().cloned();
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in req.input.delete.objects {
            let decision = match &user {
                Some(u) => self.acl.key_delete_decision(u, &bucket, &obj.key).await,
                None => Ok((false, None)), // check 恒塞身份；防御性 fail-closed
            };
            let (allowed, deny_detail) = match decision {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, key = %obj.key, "批量删逐键判权失败——fail-closed");
                    (false, None)
                }
            };
            if !allowed {
                let uid = user.as_ref().map_or(0, |u| u.uid);
                if let Err(e) = self
                    .acl
                    .write_audit(
                        uid,
                        "DeleteObjects",
                        Some(&bucket),
                        Some(&obj.key),
                        "deny",
                        deny_detail,
                    )
                    .await
                {
                    tracing::warn!(error = %e, "审计落行失败");
                }
                errors.push(Error {
                    key: Some(obj.key),
                    code: Some("AccessDenied".to_string()),
                    message: Some("access denied".to_string()),
                    ..Default::default()
                });
                continue;
            }
            match self
                .store
                .delete_object_versioned(&bucket, &obj.key, obj.version_id.as_deref(), versioned)
                .await
            {
                Ok(outcome) => {
                    let (dm, dmv, vid) = match outcome {
                        DeleteOutcome::Marker { version_id } => {
                            (Some(true), Some(version_id), None)
                        }
                        DeleteOutcome::Version { version_id } => (None, None, Some(version_id)),
                        DeleteOutcome::Object => (None, None, None),
                    };
                    deleted.push(DeletedObject {
                        key: Some(obj.key),
                        delete_marker: dm,
                        delete_marker_version_id: dmv,
                        version_id: vid,
                    })
                }
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

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let bucket = req.input.bucket;
        let cfg = req.input.versioning_configuration;
        // v1：只收 Enabled/Off；Suspended 语义未实现——明确拒绝，不静默接受
        let current = match self.acl.bucket_versioning(&bucket).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "读桶版本控制状态失败");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
        };
        let stored = match cfg.status.map(|s| s.as_str().to_string()).as_deref() {
            Some("Enabled") => "Enabled",
            Some("Off") => {
                // 单向：已启用的桶不接受改回 Off——写路径会把整链替换成单版本 → 旧版本目录
                // 成孤儿被 GC 按龄删除（静默丢历史）。停用语义（Suspended）未实现，明确拒绝。
                if current == "Enabled" {
                    return Err(s3s::s3_error!(
                        NotImplemented,
                        "disabling versioning is not supported"
                    ));
                }
                "Off"
            }
            Some("Suspended") => {
                return Err(s3s::s3_error!(
                    NotImplemented,
                    "versioning suspension is not supported"
                ));
            }
            _ => {
                return Err(s3s::s3_error!(InvalidArgument, "invalid versioning status"));
            }
        };
        match self.acl.set_bucket_versioning(&bucket, stored).await {
            Ok(true) => Ok(S3Response::new(PutBucketVersioningOutput::default())),
            Ok(false) => Err(s3s::s3_error!(NoSuchBucket)),
            Err(e) => {
                tracing::error!(error = %e, "写桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        match self.acl.bucket_versioning(&req.input.bucket).await {
            // 口径注记：未开过版本的桶本系统回 "Off"（AWS 留空）——值域收敛为 Off/Enabled，
            // aws-cli 展示不受影响（记档边界）
            Ok(s) => {
                let status = if s == "Enabled" {
                    BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)
                } else {
                    BucketVersioningStatus::from_static("Off")
                };
                Ok(S3Response::new(GetBucketVersioningOutput {
                    status: Some(status),
                    ..Default::default()
                }))
            }
            Err(e) => {
                tracing::error!(error = %e, "读桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            prefix,
            max_keys,
            key_marker,
            version_id_marker,
            ..
        } = req.input;
        // max-keys<0 同 list_objects_v2 口径（i32 → usize 的 as 会回绕，不猜客户端意图）
        let max_keys = match max_keys {
            Some(m) if m < 0 => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "max-keys must not be negative"
                ));
            }
            other => other.map(|m| m as usize),
        };
        let vl = self
            .store
            .list_object_versions(
                &bucket,
                prefix.as_deref().unwrap_or(""),
                max_keys.unwrap_or(1000),
                key_marker.as_deref(),
                version_id_marker.as_deref(),
            )
            .await
            .map_err(to_s3_error)?;

        let versions: Vec<ObjectVersion> = vl
            .entries
            .iter()
            .filter(|e| !e.is_marker)
            .map(|e| ObjectVersion {
                key: Some(e.key.clone()),
                version_id: Some(e.version_id.clone()),
                is_latest: Some(e.is_latest),
                e_tag: Some(ETag::Strong(e.etag.clone())),
                size: Some(e.size as i64),
                last_modified: Some(last_modified(e.mod_time_nanos)),
                storage_class: Some(ObjectVersionStorageClass::from_static(
                    ObjectVersionStorageClass::STANDARD,
                )),
                ..Default::default()
            })
            .collect();
        let markers: Vec<DeleteMarkerEntry> = vl
            .entries
            .iter()
            .filter(|e| e.is_marker)
            .map(|e| DeleteMarkerEntry {
                key: Some(e.key.clone()),
                version_id: Some(e.version_id.clone()),
                is_latest: Some(e.is_latest),
                last_modified: Some(last_modified(e.mod_time_nanos)),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(bucket),
            prefix,
            is_truncated: Some(vl.is_truncated),
            next_key_marker: vl.next_key_marker,
            next_version_id_marker: vl.next_version_id_marker,
            versions: (!versions.is_empty()).then_some(versions),
            delete_markers: (!markers.is_empty()).then_some(markers),
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
            ref version_id,
            ..
        } = req.input.copy_source
        else {
            return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
        };

        // 规格 §3.5 边界：同桶同键且**无源版本号**一律拒（S3 语义：InvalidRequest）。
        // M4 版本控制：`?versionId=` 的同键 copy 是标准回滚惯用法（旧版本 → 新版本），放行。
        if version_id.is_none()
            && bucket.as_ref() == req.input.bucket.as_str()
            && key.as_ref() == req.input.key.as_str()
        {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "copy source and destination are the same"
            ));
        }

        // 源：点名版本（回滚路径）或最新
        let (src_info, bytes) = match version_id.as_deref() {
            Some(vid) => self
                .store
                .get_object_version(bucket, key, vid)
                .await
                .map_err(to_s3_error)?,
            None => self
                .store
                .get_object(bucket, key)
                .await
                .map_err(to_s3_error)?,
        };
        // 目标桶版本策略随 copy 生效（版本桶 copy 出的是新版本）
        let dst_versioned = self.versioned(&req.input.bucket).await?;
        let info = self
            .store
            .put_object_stream_with_meta(
                &req.input.bucket,
                &req.input.key,
                Box::pin(futures::stream::once(async move { Ok(bytes) })),
                src_info.content_type,
                src_info.user_metadata,
                None,
                dst_versioned,
            )
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(CopyObjectOutput {
            version_id: info.version_id.clone(),
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
