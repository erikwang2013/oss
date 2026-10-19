//! M6 切片 0：事件发射点（S3 面）——`EventSink` trait 注入（裁决：保持 crate 依赖方向，
//! xl-s3 不依赖 oss-server；接收方实现写 `event_outbox`）。
//!
//! 发射点 = 六条 S3 写/删路径（Put/Copy/CompleteMPU/Delete/DeleteMarkerCreated×2 处读删）；
//! 事件类型口径见工作令 R5（对象事件直发；生命周期事件由 oss-server 侧生命周期 worker 直发；
//! ObjectAccessed 本片不实现）。发射失败**绝不影响**请求结果（接收方自吞并 warn）。

use http::HeaderMap;

/// 事件来源（S3 通知 `source` 字段）：host/port = **端点**（AWS 语义，非客户端），
/// 取自请求 Host 头（缺省回退）；user_agent 取自 UA 头；内部路径用占位（host="internal"）。
#[derive(Debug, Clone)]
pub struct EventSource {
    pub host: String,
    pub port: u16,
    pub user_agent: String,
}

impl EventSource {
    /// 自请求头解析：Host（`host[:port]`）+ User-Agent；缺省 `host="unknown", port=0`。
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let host_raw = headers
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown");
        let (host, port) = match host_raw.rsplit_once(':') {
            Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_string(), p.parse::<u16>().unwrap()),
            _ => (host_raw.to_string(), 0),
        };
        let user_agent = headers
            .get(http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        Self {
            host,
            port,
            user_agent,
        }
    }

    /// 内部路径占位（aether / console / lifecycle）。
    pub fn internal(agent: &str) -> Self {
        Self {
            host: "internal".to_string(),
            port: 0,
            user_agent: agent.to_string(),
        }
    }
}

/// 本片发射的对象事件类型（映射到 S3 eventName）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3EventOp {
    /// `s3:ObjectCreated:Put`（P3-3 记档：s3s 的 PostObject 默认实现委托 put_object，
    /// 表单直传的事件名也是 Put——与 AWS 的 Post 命名有差）
    CreatedPut,
    /// `s3:ObjectCreated:Post`（aether 内部摄取直发）
    CreatedPost,
    /// `s3:ObjectCreated:Copy`
    CreatedCopy,
    /// `s3:ObjectCreated:CompleteMultipartUpload`
    CreatedCompleteMultipartUpload,
    /// `s3:ObjectRemoved:Delete`（点名删版本/非版本桶整键删）
    RemovedDelete,
    /// `s3:ObjectRemoved:DeleteMarkerCreated`（无版本号删 → marker）
    RemovedDeleteMarkerCreated,
    /// M8 S4b：`s3:LifecycleTransition`（归档/存储类转换完成）
    LifecycleTransition,
    /// M8 S4b：`s3:ObjectRestore:Post`（Restore 受理）
    RestorePost,
    /// M8 S4b：`s3:ObjectRestore:Completed`（恢复完成可读）
    RestoreCompleted,
}

impl S3EventOp {
    pub fn event_name(self) -> &'static str {
        match self {
            Self::CreatedPut => "s3:ObjectCreated:Put",
            Self::CreatedPost => "s3:ObjectCreated:Post",
            Self::CreatedCopy => "s3:ObjectCreated:Copy",
            Self::CreatedCompleteMultipartUpload => "s3:ObjectCreated:CompleteMultipartUpload",
            Self::RemovedDelete => "s3:ObjectRemoved:Delete",
            Self::RemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            Self::LifecycleTransition => "s3:LifecycleTransition",
            Self::RestorePost => "s3:ObjectRestore:Post",
            Self::RestoreCompleted => "s3:ObjectRestore:Completed",
        }
    }

    /// 是否「删除类」（无 size/etag/新内容信息）。
    pub fn is_removed(self) -> bool {
        matches!(self, Self::RemovedDelete | Self::RemovedDeleteMarkerCreated)
    }
}

/// 事件接收方（oss-server 实现；测试用 MockSink）。**不返回 Result**：发射是
/// post-commit 尽力而为，失败由接收方自行落 warn——S3 请求结果不受影响。
#[async_trait::async_trait]
pub trait EventSink: Send + Sync {
    #[allow(clippy::too_many_arguments)] // 事件字段面平铺（同 build_event_json 口径）
    async fn emit(
        &self,
        op: S3EventOp,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
        source: EventSource,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_from_headers_matrix() {
        let mut h = HeaderMap::new();
        h.insert(http::header::HOST, "s3.example:9000".parse().unwrap());
        h.insert(http::header::USER_AGENT, "aws-cli/2.0".parse().unwrap());
        let s = EventSource::from_headers(&h);
        assert_eq!((s.host.as_str(), s.port), ("s3.example", 9000));
        assert_eq!(s.user_agent, "aws-cli/2.0");
        // 无端口 / 缺头
        let mut h2 = HeaderMap::new();
        h2.insert(http::header::HOST, "plainhost".parse().unwrap());
        let s2 = EventSource::from_headers(&h2);
        assert_eq!((s2.host.as_str(), s2.port), ("plainhost", 0));
        let s3 = EventSource::from_headers(&HeaderMap::new());
        assert_eq!(
            (s3.host.as_str(), s3.port, s3.user_agent.as_str()),
            ("unknown", 0, "")
        );
        let i = EventSource::internal("oss-console");
        assert_eq!((i.host.as_str(), i.port), ("internal", 0));
    }

    #[test]
    fn op_names() {
        assert_eq!(S3EventOp::CreatedPut.event_name(), "s3:ObjectCreated:Put");
        assert_eq!(
            S3EventOp::CreatedCompleteMultipartUpload.event_name(),
            "s3:ObjectCreated:CompleteMultipartUpload"
        );
        assert_eq!(
            S3EventOp::RemovedDeleteMarkerCreated.event_name(),
            "s3:ObjectRemoved:DeleteMarkerCreated"
        );
        assert!(S3EventOp::RemovedDelete.is_removed());
        assert!(!S3EventOp::CreatedCopy.is_removed());
    }
}
