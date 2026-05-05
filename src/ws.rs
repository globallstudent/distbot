use crate::session::Session;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(tag = "t")]
enum ClientMsg {
    #[serde(rename = "i")]
    Input { d: String },
    #[serde(rename = "r")]
    Resize { c: u16, r: u16 },
}

pub async fn handle(socket: WebSocket, session: Arc<Session>) {
    let mut rx = session.subscribe().await;
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Ask the program inside the pane to redraw so the freshly-attached
    // xterm grid converges to current state. Slight delay so xterm has
    // finished reset() before bytes start arriving.
    let session_for_nudge = session.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        session_for_nudge.nudge_redraw().await;
    });

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
                        if let Err(e) = session_in.resize(c, r).await {
                            tracing::warn!("resize: {:#}", e);
                        }
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
}
