//! aetherupload 四路由的宿主实现（取代 crate 的 `integrations::ecat::routes`）。
//!
//! 动机只有一个：**在请求上下文里看见身份**，从而执行「分组 → 桶」的权限门
//! （crate 自带的 handler 不碰身份，扩展里塞的 JWT payload 它读不到）。
//! 写数据必经 preprocess（`save_chunk` 无 partial 直接 409），但**秒传命中分支排在
//! partial 检查之前**、只回既有 saved_path——信息面也必须在这里前置拦掉，否则无权限者
//! 能凭内容 md5 探到对象存在性。
//!
//! 形态与 crate 的 axum 适配层逐字对齐（表单分流、错误映射、响应三元组都复用 crate 的
//! 公开 helper），差异只有两处：解析后先做权限门；成功路径顺带登记上传归属。

use std::sync::Arc;

use aetherupload::integrations::{
    self, FormData, ParseFailure, body_limit, display_route, download_route,
    invalid_request_body_response, is_urlencoded, ok_response, problem_response,
};
use aetherupload::{
    ChunkBody, Problem, ResourceController, Runtime, SavedPath, UploadController,
    save_chunk_ok_to_json, upload_ok_to_json,
};
use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, FromRequest, Multipart, Path, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use jwt_rust::JwtPayload;
use xl_s3::access::{Action, AuthedUser};

use crate::state::AppState;

/// 路由 state：Runtime（crate 内核）+ AppState（权限门要的 db/配置）。
#[derive(Clone)]
pub struct GateState {
    runtime: Arc<Runtime>,
    app: AppState,
}

pub fn routes(runtime: Arc<Runtime>, app: AppState) -> Router {
    let preprocess_path = runtime.config().route_preprocess.clone();
    let uploading_path = runtime.config().route_uploading.clone();
    let display_path = display_route(&runtime);
    let download_path = download_route(&runtime);
    // 框架默认的 body 上限比一个分块还小，不放开会先被 axum 自己拒掉（与 crate 同值）
    let limit = body_limit(&runtime);

    Router::new()
        .route(&preprocess_path, post(preprocess))
        .route(&uploading_path, post(save_chunk))
        .route(&display_path, get(display))
        .route(&download_path, get(download))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(GateState { runtime, app })
}

fn json_response((status, content_type, body): (u16, &'static str, String)) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        [(header::CONTENT_TYPE, content_type)],
        body,
    )
        .into_response()
}

/// 与内核失败同形的 problem（RFC 9457）：JS 取 `title` 直接渲染进输出区。
fn problem(status: u16, code: &'static str, title: impl Into<String>) -> Response {
    json_response(problem_response(&Problem::new(status, code, title, vec![])))
}

/// 权限门：`group → bucket`（oss.toml 的 [[groups]]）→ 该桶的 read/write 权限。
/// 返回 `Some(响应)` = 拒绝；取不到 group、或 group 未映射到任何桶：
/// **放行**——那是参数错误，交给内核按它自己的错误表报（不抢它的 400 语义）。
async fn gate(
    st: &GateState,
    payload: &JwtPayload,
    group: Option<&str>,
    action: Action,
) -> Option<Response> {
    let bucket = st.app.cfg.bucket_for_group(group?)?;
    let Some(uid) = payload.sub().and_then(|s| s.parse::<i64>().ok()) else {
        return Some(problem(401, "invalid_token", "登录态无效，请重新登录"));
    };
    let (is_admin, active) = match st.app.db.get_user(uid).await {
        Ok(Some((_, admin, active))) => (admin, active),
        _ => (false, false),
    };
    if !active {
        // 被停用的账号：旧 JWT 立即作废（与控制台 who() 同口径）
        return Some(problem(401, "invalid_token", "登录态无效，请重新登录"));
    }
    match st
        .app
        .acl()
        .can(AuthedUser { uid, is_admin }, action, bucket)
        .await
    {
        Ok(true) => None,
        Ok(false) => Some(problem(
            403,
            "access_denied",
            format!("无权访问该分组（桶 {bucket}）"),
        )),
        Err(e) => {
            tracing::error!(error = %e, "上传面判权失败");
            Some(problem(500, "internal_error", "internal error"))
        }
    }
}

/// 上传归属登记（best-effort）：末块落盘与秒传命中两条路径都会带 saved_path。
async fn claim_owner(st: &GateState, payload: &JwtPayload, saved_path: &str) {
    if saved_path.is_empty() {
        return;
    }
    let Some(uid) = payload.sub().and_then(|s| s.parse::<i64>().ok()) else {
        return;
    };
    match st.app.db.set_upload_owner(saved_path, uid).await {
        Ok(_) => {} // false = 已有归属/记录不存在：不覆盖、不失败
        Err(e) => tracing::warn!(error = %e, saved_path, "上传归属登记失败"),
    }
}

async fn preprocess(
    State(st): State<GateState>,
    Extension(payload): Extension<JwtPayload>,
    request: Request,
) -> Response {
    let form = match parse_form(request).await {
        Ok(form) => form,
        Err(failure) => return json_response(failure),
    };
    if let Some(r) = gate(&st, &payload, form.text("group"), Action::Write).await {
        return r;
    }
    // 直调 UploadController（而非 Guard::preprocess）：要结构化拿 `PreprocessOk.saved_path`
    // 做归属登记；响应体生成仍用 crate 的 upload_ok_to_json —— 与 Guard 路径逐字节一致
    match UploadController::new(st.runtime.clone()).preprocess(&form.to_preprocess_request()) {
        Ok(ok) => {
            claim_owner(&st, &payload, &ok.saved_path).await;
            json_response(ok_response(upload_ok_to_json(&ok)))
        }
        Err(p) => json_response(problem_response(&p)),
    }
}

async fn save_chunk(
    State(st): State<GateState>,
    Extension(payload): Extension<JwtPayload>,
    multipart: Result<Multipart, axum::extract::multipart::MultipartRejection>,
) -> Response {
    // boundary 缺失/非法这类提取器拒绝也走问题响应（不用 axum 的纯文本 400）
    let Ok(multipart) = multipart else {
        return json_response(invalid_request_body_response(None));
    };
    let form = match parse_multipart(multipart).await {
        Ok(form) => form,
        Err(failure) => return json_response(failure.response()),
    };
    if let Some(r) = gate(&st, &payload, form.text("group"), Action::Write).await {
        return r;
    }
    match UploadController::new(st.runtime.clone()).save_chunk(&form.to_save_chunk_request()) {
        Ok(ok) => {
            claim_owner(&st, &payload, &ok.saved_path).await;
            json_response(ok_response(save_chunk_ok_to_json(&ok)))
        }
        Err(p) => json_response(problem_response(&p)),
    }
}

async fn display(
    State(st): State<GateState>,
    Extension(payload): Extension<JwtPayload>,
    Path(saved_path): Path<String>,
) -> Response {
    // decode 失败：放行给内核按「资源不存在/参数非法」报（不抢它的语义）
    if let Ok(sp) = SavedPath::decode(&saved_path)
        && let Some(r) = gate(&st, &payload, Some(&sp.group), Action::Read).await
    {
        return r;
    }
    integrations::axum::serve(ResourceController::new(st.runtime.clone()).display(&saved_path))
        .await
}

async fn download(
    State(st): State<GateState>,
    Extension(payload): Extension<JwtPayload>,
    Path((saved_path, new_name)): Path<(String, String)>,
) -> Response {
    if let Ok(sp) = SavedPath::decode(&saved_path)
        && let Some(r) = gate(&st, &payload, Some(&sp.group), Action::Read).await
    {
        return r;
    }
    integrations::axum::serve(
        ResourceController::new(st.runtime.clone()).download(&saved_path, &new_name),
    )
    .await
}

// ---- 表单解析（照抄 crate `integrations/axum.rs` 的私有 parse_form/parse_multipart：
// 字段名/错误映射与它逐字一致；crate 未公开这两个函数，重复实现是宿主持有身份的代价）----

async fn parse_form(request: Request) -> Result<FormData, (u16, &'static str, String)> {
    let urlencoded = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_urlencoded);

    if !urlencoded {
        let Ok(multipart) = Multipart::from_request(request, &()).await else {
            // 缺 Content-Type 或 boundary 非法：请求体无法解析
            return Err(invalid_request_body_response(None));
        };
        return parse_multipart(multipart)
            .await
            .map_err(|failure| failure.response());
    }

    // 用 `Bytes` 提取器：上限同样来自路由器的 `DefaultBodyLimit`（上面已挂 crate 同值），
    // 且把「超限」（413）与「读失败」（400）分成两个变体，状态码如实映射
    let bytes = match axum::body::Bytes::from_request(request, &()).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(ParseFailure::new(
                FormData::new(),
                err.status() == StatusCode::PAYLOAD_TOO_LARGE,
            )
            .response());
        }
    };
    Ok(FormData::from_urlencoded(&String::from_utf8_lossy(&bytes)))
}

async fn parse_multipart(mut multipart: Multipart) -> Result<FormData, ParseFailure> {
    let mut form = FormData::new();
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => return Ok(form),
            Err(err) => return Err(parse_failure(form, &err)),
        };
        let name = field.name().unwrap_or_default().to_string();
        if name == "resource_chunk" {
            match field.bytes().await {
                Ok(bytes) => form.push_file(name, ChunkBody::Bytes(bytes.to_vec())),
                Err(err) => return Err(parse_failure(form, &err)),
            }
        } else {
            match field.text().await {
                Ok(value) => form.push_text(name, value),
                Err(err) => return Err(parse_failure(form, &err)),
            }
        }
    }
}

fn parse_failure(form: FormData, err: &axum::extract::multipart::MultipartError) -> ParseFailure {
    ParseFailure::new(form, err.status() == StatusCode::PAYLOAD_TOO_LARGE)
}
