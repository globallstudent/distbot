use crate::registry::Registry;
use crate::session::Session;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use teloxide::prelude::*;
use teloxide::types::{ChatAction, ParseMode};
use teloxide::utils::command::BotCommands;
use teloxide::utils::markdown;
use tokio::sync::{broadcast, RwLock};

#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "disbot commands:")]
enum Command {
    #[command(description = "show this help")]
    Help,
    #[command(description = "register / show status")]
    Start,
    #[command(description = "list sessions")]
    List,
    #[command(description = "switch active session: /use <id>")]
    Use(String),
    #[command(description = "create session: /new <id> [cwd] [cmd]")]
    New(String),
    #[command(description = "kill session: /kill <id> (or current)")]
    Kill(String),
    #[command(description = "show current screen")]
    Status,
    #[command(description = "send Enter")]
    Enter,
    #[command(description = "send Escape")]
    Esc,
    #[command(description = "send Ctrl-<letter>, e.g. /ctrl c")]
    Ctrl(String),
    #[command(description = "send raw hex bytes, e.g. /raw 1b 5b 41")]
    Raw(String),
}

struct State {
    registry: Arc<Registry>,
    owner: RwLock<Option<i64>>,
    current: RwLock<Option<String>>,
    expecting_reply: AtomicBool,
}

pub async fn run(token: String, env_owner: Option<i64>, registry: Arc<Registry>) {
    let initial_owner = env_owner.or_else(load_owner_from_disk);
    if let Some(o) = initial_owner {
        tracing::info!("tg owner: {}", o);
    } else {
        tracing::warn!("tg owner not set — first user to /start becomes owner");
    }

    let initial_current = registry.list().await.first().map(|m| m.id.clone());

    let state = Arc::new(State {
        registry,
        owner: RwLock::new(initial_owner),
        current: RwLock::new(initial_current),
        expecting_reply: AtomicBool::new(false),
    });

    let bot = Bot::new(token);

    let bot_for_out = bot.clone();
    let state_for_out = state.clone();
    tokio::spawn(async move {
        forward_output(bot_for_out, state_for_out).await;
    });

    let handler = Update::filter_message().endpoint(dispatch);

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn dispatch(bot: Bot, msg: Message, state: Arc<State>) -> ResponseResult<()> {
    let from_id = match msg.from.as_ref() {
        Some(u) => u.id.0 as i64,
        None => return Ok(()),
    };

    let owner_now = *state.owner.read().await;
    let is_owner = match owner_now {
        Some(o) => o == from_id,
        None => {
            let mut w = state.owner.write().await;
            if w.is_none() {
                *w = Some(from_id);
                if let Err(e) = save_owner_to_disk(from_id) {
                    tracing::warn!("failed to persist owner: {:#}", e);
                }
                tracing::info!("registered owner: {} (chat {})", from_id, msg.chat.id);
                drop(w);
                bot.send_message(
                    msg.chat.id,
                    format!(
                        "registered as owner ({}). type /help. /new to create a session.",
                        from_id
                    ),
                )
                .await?;
                return Ok(());
            }
            *w == Some(from_id)
        }
    };
    if !is_owner {
        tracing::debug!("ignoring message from {} (not owner)", from_id);
        return Ok(());
    }

    let text = match msg.text() {
        Some(t) => t.to_string(),
        None => return Ok(()),
    };
    if text.is_empty() {
        return Ok(());
    }

    if text.starts_with('/') {
        match Command::parse(&text, "") {
            Ok(cmd) => handle_command(&bot, &msg, &state, cmd).await?,
            Err(_) => {
                bot.send_message(msg.chat.id, "unknown command. /help").await?;
            }
        }
    } else {
        let session = match current_session(&state).await {
            Some(s) => s,
            None => {
                bot.send_message(msg.chat.id, "no session. /new <id> <cwd> [cmd]")
                    .await?;
                return Ok(());
            }
        };
        let mut buf = text.into_bytes();
        buf.push(b'\r');
        state.expecting_reply.store(true, Ordering::Relaxed);
        if let Err(e) = session.send_input(&buf).await {
            bot.send_message(msg.chat.id, format!("send failed: {e:#}"))
                .await?;
        }
    }
    Ok(())
}

async fn current_session(state: &State) -> Option<Arc<Session>> {
    let id = state.current.read().await.clone()?;
    state.registry.get(&id).await
}

async fn handle_command(
    bot: &Bot,
    msg: &Message,
    state: &State,
    cmd: Command,
) -> ResponseResult<()> {
    let chat = msg.chat.id;
    match cmd {
        Command::Help | Command::Start => {
            bot.send_message(chat, Command::descriptions().to_string())
                .await?;
        }
        Command::List => {
            let _ = state.registry.refresh_from_tmux().await;
            let metas = state.registry.list().await;
            let cur = state.current.read().await.clone();
            let body = if metas.is_empty() {
                "no sessions. /new <id> <cwd> [cmd]".to_string()
            } else {
                let mut s = String::from("sessions:\n");
                for m in metas {
                    let mark = if Some(&m.id) == cur.as_ref() { "▶" } else { " " };
                    s.push_str(&format!("{} {} — {} ({})\n", mark, m.id, m.cwd, m.cmd));
                }
                s
            };
            bot.send_message(chat, body).await?;
        }
        Command::Use(id) => {
            let id = id.trim().to_string();
            if id.is_empty() {
                bot.send_message(chat, "usage: /use <id>").await?;
                return Ok(());
            }
            if state.registry.get(&id).await.is_none() {
                bot.send_message(chat, format!("no session '{}'", id)).await?;
                return Ok(());
            }
            *state.current.write().await = Some(id.clone());
            bot.send_message(chat, format!("active: {}", id)).await?;
        }
        Command::New(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.is_empty() {
                bot.send_message(chat, "usage: /new <id> [cwd] [cmd]").await?;
                return Ok(());
            }
            let id = parts[0].to_string();
            let cwd = parts
                .get(1)
                .map(|s| expand_tilde(s))
                .unwrap_or_else(default_cwd);
            let cmd = parts
                .get(2)
                .map(|s| s.to_string())
                .unwrap_or_else(|| state.registry.default_cmd().to_string());
            match state.registry.create(id.clone(), cwd, cmd, 120, 40).await {
                Ok(_) => {
                    *state.current.write().await = Some(id.clone());
                    bot.send_message(chat, format!("created and active: {}", id))
                        .await?;
                }
                Err(e) => {
                    bot.send_message(chat, format!("create failed: {e:#}")).await?;
                }
            }
        }
        Command::Kill(arg) => {
            let id = if arg.trim().is_empty() {
                match state.current.read().await.clone() {
                    Some(c) => c,
                    None => {
                        bot.send_message(chat, "usage: /kill <id>").await?;
                        return Ok(());
                    }
                }
            } else {
                arg.trim().to_string()
            };
            match state.registry.delete(&id).await {
                Ok(()) => {
                    let mut cur = state.current.write().await;
                    if cur.as_deref() == Some(id.as_str()) {
                        *cur = state.registry.list().await.first().map(|m| m.id.clone());
                    }
                    bot.send_message(chat, format!("killed: {}", id)).await?;
                }
                Err(e) => {
                    bot.send_message(chat, format!("kill failed: {e:#}")).await?;
                }
            }
        }
        Command::Status => match current_session(state).await {
            Some(s) => match s.capture_tail(25).await {
                Ok(text) => send_screen(bot, chat, &text).await?,
                Err(e) => {
                    bot.send_message(chat, format!("capture failed: {e:#}"))
                        .await?;
                }
            },
            None => {
                bot.send_message(chat, "no active session. /list").await?;
            }
        },
        Command::Enter => {
            if let Some(s) = current_session(state).await {
                state.expecting_reply.store(true, Ordering::Relaxed);
                let _ = s.send_input(b"\r").await;
            }
        }
        Command::Esc => {
            if let Some(s) = current_session(state).await {
                let _ = s.send_input(&[0x1b]).await;
            }
        }
        Command::Ctrl(s) => {
            let s = s.trim();
            if s.len() != 1 {
                bot.send_message(chat, "usage: /ctrl c").await?;
                return Ok(());
            }
            let c = s.chars().next().unwrap().to_ascii_uppercase();
            if !c.is_ascii_uppercase() {
                bot.send_message(chat, "ctrl letter must be a-z").await?;
                return Ok(());
            }
            let byte = (c as u8) - b'A' + 1;
            if let Some(s) = current_session(state).await {
                let _ = s.send_input(&[byte]).await;
            }
        }
        Command::Raw(s) => {
            let parsed: Result<Vec<u8>, _> = s
                .split_whitespace()
                .map(|t| u8::from_str_radix(t, 16))
                .collect();
            match parsed {
                Ok(b) if !b.is_empty() => {
                    if let Some(s) = current_session(state).await {
                        state.expecting_reply.store(true, Ordering::Relaxed);
                        let _ = s.send_input(&b).await;
                    }
                }
                _ => {
                    bot.send_message(chat, "usage: /raw 1b 5b 41").await?;
                }
            }
        }
    }
    Ok(())
}

async fn send_screen(bot: &Bot, chat: ChatId, raw: &str) -> ResponseResult<()> {
    let trimmed = raw.trim_end_matches(|c: char| c == '\n' || c == ' ');
    if trimmed.trim().is_empty() {
        bot.send_message(chat, "(empty)").await?;
        return Ok(());
    }
    for chunk in chunk_text(trimmed, 3800) {
        let body = format!("```\n{}\n```", markdown::escape_code(&chunk));
        bot.send_message(chat, body)
            .parse_mode(ParseMode::MarkdownV2)
            .await?;
    }
    Ok(())
}

fn chunk_text(s: &str, max: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in s.lines() {
        if line.len() > max {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            let mut p = 0;
            let bytes = line.as_bytes();
            while p < bytes.len() {
                let mut end = (p + max).min(bytes.len());
                while end < bytes.len() && !line.is_char_boundary(end) {
                    end -= 1;
                }
                out.push(line[p..end].to_string());
                p = end;
            }
            continue;
        }
        let extra = if current.is_empty() { 0 } else { 1 };
        if current.len() + extra + line.len() > max {
            out.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

async fn forward_output(bot: Bot, state: Arc<State>) {
    let idle_threshold = Duration::from_millis(1500);
    let typing_throttle = Duration::from_secs(4);

    loop {
        let chat_id = match *state.owner.read().await {
            Some(o) => ChatId(o),
            None => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let session = match current_session(&state).await {
            Some(s) => s,
            None => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let session_id = session.id.clone();

        let (_, mut rx) = session.subscribe().await;
        let mut last_hash: u64 = 0;
        let mut activity_pending = false;
        let mut last_activity = Instant::now();
        let mut last_typing = Instant::now() - Duration::from_secs(60);

        loop {
            // bail out if active session changed
            if state.current.read().await.as_deref() != Some(session_id.as_str()) {
                break;
            }

            let recv = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
            match recv {
                Ok(Ok(_bytes)) => {
                    activity_pending = true;
                    last_activity = Instant::now();
                    if last_typing.elapsed() > typing_throttle {
                        let _ = bot.send_chat_action(chat_id, ChatAction::Typing).await;
                        last_typing = Instant::now();
                    }
                    continue;
                }
                Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!("tg fwd lagged {} msgs", n);
                    continue;
                }
                Ok(Err(_)) => break, // session died
                Err(_) => {}
            }

            if !activity_pending || last_activity.elapsed() < idle_threshold {
                continue;
            }
            activity_pending = false;

            if !state.expecting_reply.swap(false, Ordering::Relaxed) {
                continue;
            }

            let text = match session.capture_tail(40).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("capture: {:#}", e);
                    continue;
                }
            };
            let trimmed = text
                .trim_end_matches(|c: char| c == '\n' || c == ' ')
                .to_string();
            let h = hash_str(&trimmed);
            if h == last_hash || trimmed.trim().is_empty() {
                continue;
            }
            last_hash = h;
            if let Err(e) = send_screen(&bot, chat_id, &trimmed).await {
                tracing::warn!("send_screen: {:#}", e);
            }
        }
    }
}

fn hash_str(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

fn default_cwd() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/".into())
}

fn expand_tilde(p: &str) -> String {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, rest);
        }
    }
    if p == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }
    p.to_string()
}

fn owner_path() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join(".disbot-owner"))
}

fn load_owner_from_disk() -> Option<i64> {
    let p = owner_path()?;
    let s = std::fs::read_to_string(p).ok()?;
    s.trim().parse().ok()
}

fn save_owner_to_disk(id: i64) -> Result<()> {
    if let Some(p) = owner_path() {
        std::fs::write(p, id.to_string())?;
    }
    Ok(())
}
