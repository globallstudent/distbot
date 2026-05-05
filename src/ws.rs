use crate::session::Session;
use axum::extract::ws::{Message, WebSocket};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(tag = "t")]
enum ClientMsg {
    #[serde(rename = "i")]
    Input { d: String },
    #[serde(rename = "r")]
    Resize { c: u16, r: u16 },
}

pub async fn handle(socket: WebSocket, session: Arc<Session>, init_cols: u16, init_rows: u16) {
    let viewport_id = session.add_viewport(init_cols, init_rows).await;
    let mut rx = session.subscribe().await;
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Send a per-client snapshot to bring this xterm into sync with Claude's
    // current state. Goes only to this WebSocket — other clients aren't
    // affected.
    match session.snapshot().await {
        Ok(bytes) if !bytes.is_empty() => {
            if ws_tx.send(Message::Binary(Bytes::from(bytes))).await.is_err() {
                session.remove_viewport(viewport_id).await;
                return;
            }
        }
        Err(e) => tracing::warn!("snapshot failed: {:#}", e),
        _ => {}
    }

    let session_in = session.clone();
    let mut writer = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    if ws_tx.send(Message::Binary(bytes)).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("ws lagged {} messages", n);
                    continue;
                }
                Err(_) => break,
            }
        }
    });

    let session_for_reader = session.clone();
    let mut reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Text(t) => match serde_json::from_str::<ClientMsg>(t.as_str()) {
                    Ok(ClientMsg::Input { d }) => {
                        if let Err(e) = session_in.send_input(d.as_bytes()).await {
                            tracing::warn!("send_input: {:#}", e);
                        }
                    }
                    Ok(ClientMsg::Resize { c, r }) => {
                        session_for_reader.update_viewport(viewport_id, c, r).await;
                    }
                    Err(e) => tracing::debug!("bad client msg: {}", e),
                },
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = &mut writer => { reader.abort(); }
        _ = &mut reader => { writer.abort(); }
    }

    session.remove_viewport(viewport_id).await;
}
