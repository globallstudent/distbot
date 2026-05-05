use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::broadcast;

const BROADCAST_CAP: usize = 1024;
const READ_CHUNK: usize = 16 * 1024;

pub struct Session {
    pub id: String,
    pub cwd: String,
    pub cmd: String,
    pub created_at: u64,
    tmux_name: String,
    inner: Arc<Inner>,
}

struct Inner {
    tx: broadcast::Sender<Bytes>,
    cols: AtomicU16,
    rows: AtomicU16,
}

pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 64 {
        return Err(anyhow!("id length must be 1..=64"));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(anyhow!("id may only contain a-z, 0-9, '-', '_'"));
    }
    Ok(())
}

impl Session {
    pub async fn open(
        id: String,
        cwd: String,
        cmd: String,
        cols: u16,
        rows: u16,
    ) -> Result<Self> {
        validate_id(&id)?;
        if !Path::new(&cwd).is_dir() {
            return Err(anyhow!("cwd does not exist or is not a directory: {}", cwd));
        }
        let tmux_name = format!("disbot-{}", id);
        let sock_path = PathBuf::from(format!("/tmp/disbot-{}.sock", id));

        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)
            .with_context(|| format!("bind {:?}", sock_path))?;

        let exists = tmux(&["has-session", "-t", &tmux_name]).await.is_ok();
        let created_at = if exists {
            session_created_at(&tmux_name).await.unwrap_or_else(|_| now_secs())
        } else {
            let _ = tmux(&["set-option", "-g", "focus-events", "on"]).await;
            let cols_s = cols.to_string();
            let rows_s = rows.to_string();
            tmux(&[
                "new-session", "-d", "-s", &tmux_name,
                "-c", &cwd,
                "-x", &cols_s, "-y", &rows_s, &cmd,
            ])
            .await
            .context("tmux new-session")?;
            tracing::info!(
                "spawned tmux session {} ({}) cwd={}",
                tmux_name, cmd, cwd
            );
            now_secs()
        };

        let _ = tmux(&["pipe-pane", "-t", &tmux_name]).await;
        let pipe_cmd = format!("exec nc -U {}", sock_path.display());
        tmux(&["pipe-pane", "-t", &tmux_name, "-O", &pipe_cmd])
            .await
            .context("tmux pipe-pane")?;

        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        let inner = Arc::new(Inner {
            tx,
            cols: AtomicU16::new(cols),
            rows: AtomicU16::new(rows),
        });

        let inner_for_task = inner.clone();
        let name_for_task = tmux_name.clone();
        tokio::spawn(async move {
            if let Err(e) = read_loop(listener, inner_for_task).await {
                tracing::warn!("session {} read loop ended: {:#}", name_for_task, e);
            }
        });

        Ok(Self {
            id,
            cwd,
            cmd,
            created_at,
            tmux_name,
            inner,
        })
    }

    pub async fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.inner.tx.subscribe()
    }

    pub async fn nudge_redraw(&self) {
        // Ctrl+L (0x0c, ANSI FF) is the standard "redraw screen" signal that
        // most TUIs (including Claude Code) honor by emitting a full redraw.
        // We rely on this instead of replaying historical bytes or sending a
        // static capture-pane snapshot, both of which produce incoherent
        // xterm state in long-running TUI sessions.
        if let Err(e) = self.send_input(&[0x0c]).await {
            tracing::warn!("nudge_redraw failed: {:#}", e);
        }
    }

    pub async fn send_input(&self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        for chunk in data.chunks(256) {
            let mut args: Vec<String> = vec![
                "send-keys".into(),
                "-t".into(),
                self.tmux_name.clone(),
                "-H".into(),
            ];
            for b in chunk {
                args.push(format!("{:02x}", b));
            }
            let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
            tmux(&args_ref).await.context("tmux send-keys")?;
        }
        Ok(())
    }

    pub async fn capture_tail(&self, lines: u32) -> Result<String> {
        let start = format!("-{}", lines);
        let out = Command::new("tmux")
            .args([
                "capture-pane", "-p", "-J",
                "-t", &self.tmux_name,
                "-S", &start,
            ])
            .output()
            .await
            .context("spawn tmux capture-pane")?;
        if !out.status.success() {
            return Err(anyhow!(
                "capture-pane failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    pub async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        let prev_cols = self.inner.cols.swap(cols, Ordering::Relaxed);
        let prev_rows = self.inner.rows.swap(rows, Ordering::Relaxed);
        if prev_cols == cols && prev_rows == rows {
            return Ok(());
        }
        let cols_s = cols.to_string();
        let rows_s = rows.to_string();
        tmux(&[
            "resize-window", "-t", &self.tmux_name,
            "-x", &cols_s, "-y", &rows_s,
        ])
        .await
        .context("tmux resize-window")?;
        Ok(())
    }

    pub async fn kill(&self) -> Result<()> {
        let _ = tmux(&["kill-session", "-t", &self.tmux_name]).await;
        let sock = PathBuf::from(format!("/tmp/disbot-{}.sock", self.id));
        let _ = std::fs::remove_file(sock);
        Ok(())
    }
}

async fn read_loop(listener: UnixListener, inner: Arc<Inner>) -> Result<()> {
    let (mut stream, _) = listener.accept().await.context("accept nc connection")?;
    tracing::debug!("pipe-pane reader connected");
    let mut buf = vec![0u8; READ_CHUNK];
    loop {
        let n = stream.read(&mut buf).await.context("read pipe-pane")?;
        if n == 0 {
            return Err(anyhow!("pipe-pane closed"));
        }
        let chunk = Bytes::copy_from_slice(&buf[..n]);
        let _ = inner.tx.send(chunk);
    }
}

async fn tmux(args: &[&str]) -> Result<()> {
    let out = Command::new("tmux")
        .args(args)
        .output()
        .await
        .context("spawn tmux")?;
    if !out.status.success() {
        return Err(anyhow!(
            "tmux {:?} failed ({}): {}",
            args,
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(())
}

async fn session_created_at(tmux_name: &str) -> Result<u64> {
    let out = Command::new("tmux")
        .args(["display-message", "-p", "-t", tmux_name, "#{session_created}"])
        .output()
        .await?;
    if !out.status.success() {
        return Err(anyhow!("display-message failed"));
    }
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u64>().map_err(Into::into)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub async fn pane_current_path(tmux_name: &str) -> Option<String> {
    let out = Command::new("tmux")
        .args([
            "display-message", "-p", "-t", tmux_name,
            "#{pane_current_path}",
        ])
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

pub async fn list_disbot_tmux_sessions() -> Result<Vec<String>> {
    let out = Command::new("tmux")
        .args(["list-sessions", "-F", "#{session_name}"])
        .output()
        .await
        .context("tmux list-sessions")?;
    if !out.status.success() {
        // No sessions at all is also an error in tmux; treat as empty.
        return Ok(Vec::new());
    }
    let names: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.strip_prefix("disbot-").map(|s| s.to_string()))
        .collect();
    Ok(names)
}
