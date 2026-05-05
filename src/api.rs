use crate::registry::SessionMeta;
use crate::AppState;
use axum::{
    extract::{Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

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
