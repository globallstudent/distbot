mod api;
mod config;
mod registry;
mod session;
mod tg;
mod ws;

use anyhow::Result;
use axum::{
    extract::{DefaultBodyLimit, Query, State, WebSocketUpgrade},
    http::{header, StatusCode},
    middleware,
    response::{Html, IntoResponse},
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::registry::Registry;

const INDEX_HTML: &str = include_str!("../web/index.html");

pub struct AppState {
    pub token: String,
    pub registry: Arc<Registry>,
}

#[derive(Deserialize)]
struct WsParams {
    token: String,
    id: String,
    cols: Option<u16>,
    rows: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,disbot=debug".into()),
        )
        .init();

    let cfg = config::load()?;
    tracing::info!("disbot bind={}", cfg.bind);
    tracing::info!("open: http://{}/?token={}", cfg.bind, cfg.token);

    let meta_path = disbot_state_dir().join("sessions.json");
    let registry = Arc::new(Registry::load_or_init(meta_path, cfg.claude_cmd.clone()).await?);

    let state = Arc::new(AppState {
        token: cfg.token,
        registry: registry.clone(),
    });

    {
        let registry = registry.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_secs(10));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t.tick().await; // first tick fires immediately, skip
            loop {
                t.tick().await;
                if let Err(e) = registry.refresh_from_tmux().await {
                    tracing::warn!("periodic refresh failed: {:#}", e);
                }
            }
        });
    }

    if let Some(token) = cfg.tg_token {
        let owner = cfg.tg_owner;
        let registry = registry.clone();
        tokio::spawn(async move {
            tg::run(token, owner, registry).await;
        });
        tracing::info!("telegram bot started");
    } else {
        tracing::info!("DISBOT_TG_TOKEN unset — telegram disabled");
    }

    let api_routes = Router::new()
        .route("/sessions", get(api::list_sessions).post(api::create_session))
        .route("/sessions/{id}", delete(api::delete_session))
        .route("/sessions/{id}/scrollback", get(api::scrollback))
        .route(
            "/upload",
            post(api::upload).layer(DefaultBodyLimit::max(20 * 1024 * 1024)),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            api::auth_middleware,
        ));

    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .nest("/api", api_routes)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CACHE_CONTROL, "no-store, no-cache, must-revalidate"),
            (header::PRAGMA, "no-cache"),
        ],
        Html(INDEX_HTML),
    )
}

async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    if params.token != state.token {
        return (StatusCode::UNAUTHORIZED, "bad token").into_response();
    }
    let session = match state.registry.get(&params.id).await {
        Some(s) => s,
        None => {
            return (StatusCode::NOT_FOUND, format!("no session '{}'", params.id))
                .into_response();
        }
    };
    let cols = params.cols.unwrap_or(120);
    let rows = params.rows.unwrap_or(40);
    ws.on_upgrade(move |socket| ws::handle(socket, session, cols, rows))
}

fn disbot_state_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".disbot")
}
