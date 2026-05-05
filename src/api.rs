use crate::registry::SessionMeta;
use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static UPLOAD_SEQ: AtomicU64 = AtomicU64::new(1);
const UPLOAD_MAX: usize = 20 * 1024 * 1024;

#[derive(Deserialize)]
pub struct CreateReq {
    pub id: String,
    pub cwd: Option<String>,
    pub cmd: Option<String>,
}

pub async fn list_sessions(State(state): State<Arc<AppState>>) -> Json<Vec<SessionMeta>> {
    Json(state.registry.list().await)
}

pub async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateReq>,
) -> Response {
    let cwd = req
        .cwd
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(default_cwd);
    let cmd = req
        .cmd
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| state.registry.default_cmd().to_string());
    match state.registry.create(req.id, cwd, cmd, 120, 40).await {
        Ok(s) => (StatusCode::CREATED, Json(SessionMeta::from_session(&s))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

fn default_cwd() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/".into())
}

pub async fn scrollback(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let lines: u32 = q
        .get("lines")
        .and_then(|s| s.parse().ok())
        .filter(|n: &u32| *n > 0 && *n <= 20_000)
        .unwrap_or(2000);
    let session = match state.registry.get(&id).await {
        Some(s) => s,
        None => return (StatusCode::NOT_FOUND, format!("no session '{}'", id)).into_response(),
    };
    match session.capture_tail(lines).await {
        Ok(text) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            text,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response(),
    }
}

pub async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.registry.delete(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

pub async fn upload(headers: HeaderMap, body: Bytes) -> Response {
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty body").into_response();
    }
    if body.len() > UPLOAD_MAX {
        return (StatusCode::PAYLOAD_TOO_LARGE, "max 20 MB").into_response();
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");
    let ext = ext_for_mime(content_type);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::path::Path::new("/tmp/disbot-uploads");
    if let Err(e) = tokio::fs::create_dir_all(&dir).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {e}")).into_response();
    }
    let path = dir.join(format!("paste-{ms}-{seq}.{ext}"));
    if let Err(e) = tokio::fs::write(&path, &body).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")).into_response();
    }
    Json(serde_json::json!({ "path": path.display().to_string() })).into_response()
}

fn ext_for_mime(mime: &str) -> &'static str {
    match mime.split(';').next().unwrap_or("").trim() {
        "image/png" => "png",
        "image/jpeg" | "image/jpg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/avif" => "avif",
        "image/heic" | "image/heif" => "heic",
        "image/svg+xml" => "svg",
        _ => "bin",
    }
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let token_q = q.get("token").cloned();
    let token_h = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());
    let token = token_q.or(token_h);
    if token.as_deref() != Some(state.token.as_str()) {
        return (StatusCode::UNAUTHORIZED, "bad token").into_response();
    }
    next.run(req).await
}
