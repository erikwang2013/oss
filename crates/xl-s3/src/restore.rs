//! M8 S4b：RestoreObject 参数解析与恢复入队 sink。
//!
//! xl-s3 只做「标记 pending → sink 入队」，不写 jobs 库（B9）；作业由 oss-server 执行。

use s3s::dto::{RestoreRequest, Tier};

/// 恢复请求语义校验失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreError {
    /// 400 InvalidRequest（body 与头都缺失）。
    InvalidRequest(String),
    /// 400 InvalidArgument（days/tier 非法）。
    InvalidArgument(String),
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(m) | Self::InvalidArgument(m) => write!(f, "{m}"),
        }
    }
}

/// 恢复作业入队（oss-server 注入；单事务写 `tier_restores` + 作业）。返回 job_id。
#[async_trait::async_trait]
pub trait RestoreSink: Send + Sync {
    async fn enqueue_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        days: u32,
    ) -> Result<i64, String>;

    /// 读/HEAD 发现恢复已过期：删行（best-effort；本地副本与 phase 已由 store 处理）。
    async fn forget_restore(&self, bucket: &str, key: &str, version_id: &str)
    -> Result<(), String>;
}

fn valid_tier(t: &str) -> bool {
    [Tier::STANDARD, Tier::BULK, Tier::EXPEDITED].contains(&t)
}

fn parse_days(raw: i64) -> Result<u32, RestoreError> {
    if !(1..=36_500).contains(&raw) {
        return Err(RestoreError::InvalidArgument(format!(
            "Restore days 越界：{raw}（1..=36500）"
        )));
    }
    Ok(raw as u32)
}

/// 解析 `x-amz-restore-request: days=<n>,tier=<tier>`（header 形式，OSS 扩展）。
fn parse_header(raw: &str) -> Result<(Option<u32>, Option<String>), RestoreError> {
    let mut days = None;
    let mut tier = None;
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').ok_or_else(|| {
            RestoreError::InvalidArgument(format!("x-amz-restore-request 片段非法：{part:?}"))
        })?;
        match k.trim() {
            "days" => {
                let n = v.trim().parse::<i64>().map_err(|_| {
                    RestoreError::InvalidArgument(format!("days 非整数：{:?}", v.trim()))
                })?;
                days = Some(parse_days(n)?);
            }
            "tier" => {
                let t = v.trim();
                if !valid_tier(t) {
                    return Err(RestoreError::InvalidArgument(format!(
                        "非法 restore tier：{t}"
                    )));
                }
                tier = Some(t.to_string());
            }
            other => {
                return Err(RestoreError::InvalidArgument(format!(
                    "未知 x-amz-restore-request 字段：{other}"
                )));
            }
        }
    }
    Ok((days, tier))
}

/// XML body 优先；body 缺失时读 header；两者都缺 → 400。days 缺省 1。
pub fn parse_restore_request(
    body: Option<&RestoreRequest>,
    header: Option<&str>,
) -> Result<u32, RestoreError> {
    let Some(body) = body else {
        let Some(header) = header else {
            return Err(RestoreError::InvalidRequest(
                "RestoreObject 需要 XML body 或 x-amz-restore-request 头".into(),
            ));
        };
        return Ok(parse_header(header)?.0.unwrap_or(1));
    };
    for t in [
        body.tier.as_ref(),
        body.glacier_job_parameters.as_ref().map(|g| &g.tier),
    ]
    .into_iter()
    .flatten()
    {
        if !valid_tier(t.as_str()) {
            return Err(RestoreError::InvalidArgument(format!(
                "非法 restore tier：{}",
                t.as_str()
            )));
        }
    }
    match body.days {
        Some(d) => parse_days(i64::from(d)),
        None => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(days: Option<i32>, tier: Option<&str>) -> RestoreRequest {
        RestoreRequest {
            days,
            tier: tier.map(|t| Tier::from(t.to_string())),
            ..Default::default()
        }
    }

    #[test]
    fn body_and_header_paths() {
        assert_eq!(
            parse_restore_request(Some(&body(Some(3), None)), None),
            Ok(3)
        );
        assert_eq!(parse_restore_request(Some(&body(None, None)), None), Ok(1));
        assert_eq!(parse_restore_request(None, Some("days=7,tier=Bulk")), Ok(7));
        assert_eq!(parse_restore_request(None, Some("tier=Standard")), Ok(1));
        assert_eq!(
            parse_restore_request(Some(&body(Some(2), None)), Some("days=9")),
            Ok(2),
            "body 优先"
        );
        assert!(matches!(
            parse_restore_request(None, None),
            Err(RestoreError::InvalidRequest(_))
        ));
        for bad in [0i32, -1, 36_501] {
            assert!(matches!(
                parse_restore_request(Some(&body(Some(bad), None)), None),
                Err(RestoreError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            parse_restore_request(None, Some("days=x")),
            Err(RestoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_restore_request(None, Some("tier=Slow")),
            Err(RestoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_restore_request(Some(&body(None, Some("Slow"))), None),
            Err(RestoreError::InvalidArgument(_))
        ));
    }
}
