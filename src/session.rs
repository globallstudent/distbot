use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex};

const BROADCAST_CAP: usize = 1024;
const READ_CHUNK: usize = 16 * 1024;
const HISTORY_CAP: usize = 50_000;

static NEXT_VIEWPORT_ID: AtomicU64 = AtomicU64::new(1);
pub type ViewportId = u64;

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
    pane_cols: AtomicU16,
    pane_rows: AtomicU16,
    viewports: Mutex<HashMap<ViewportId, (u16, u16)>>,
    /// Server-side virtual terminal that mirrors the pane state, used to
    /// detect "scroll-via-redraw" so we can capture history that tmux can't
    /// (alt-screen content scrolling within Claude TUI, etc.).
    parser: Mutex<vt100::Parser>,
    /// Last observed screen rows (plain text, trimmed). Comparing against
    /// the current screen lets us see what content scrolled off the top.
    last_rows: Mutex<Vec<String>>,
    /// Bounded scrollback of rows that scrolled out of view.
    history: Mutex<VecDeque<String>>,
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
            // Big history-limit so the scrollback view has real history to show
            // for shell sessions. (Claude TUI uses alt-screen; tmux doesn't
            // record alt-screen scrollback regardless of this setting.)
            let _ = tmux(&["set-option", "-g", "history-limit", "50000"]).await;
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
        // vt100 scrollback length 0 — we manage our own scroll-out detection.
        let parser = vt100::Parser::new(rows, cols, 0);
        let inner = Arc::new(Inner {
            tx,
            pane_cols: AtomicU16::new(cols),
            pane_rows: AtomicU16::new(rows),
            viewports: Mutex::new(HashMap::new()),
            parser: Mutex::new(parser),
            last_rows: Mutex::new(Vec::new()),
            history: Mutex::new(VecDeque::new()),
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

    /// Build a per-client snapshot to bring an attaching xterm into sync with
    /// Claude's current state without broadcasting anything. We:
    ///   - enter alt-screen if the pane is in alt-screen mode
    ///   - clear and home
    ///   - write each captured line at absolute row to avoid scroll
    ///   - position the cursor where tmux says it is
    /// Other clients are unaffected because this output goes to one WebSocket only.
    pub async fn snapshot(&self) -> Result<Vec<u8>> {
        let pane = Command::new("tmux")
            .args([
                "capture-pane", "-p", "-e",
                "-t", &self.tmux_name,
            ])
            .output()
            .await
            .context("capture-pane")?;
        if !pane.status.success() {
            return Err(anyhow!(
                "capture-pane: {}",
                String::from_utf8_lossy(&pane.stderr).trim()
            ));
        }
        let info_out = Command::new("tmux")
            .args([
                "display-message", "-p", "-t", &self.tmux_name,
                "#{cursor_y};#{cursor_x};#{alternate_on}",
            ])
            .output()
            .await
            .context("display-message")?;
        let info = String::from_utf8_lossy(&info_out.stdout);
        let parts: Vec<&str> = info.trim().split(';').collect();
        let cy: u16 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let cx: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let alt: bool = parts.get(2).map(|s| s.trim() == "1").unwrap_or(false);

        let mut out = Vec::with_capacity(pane.stdout.len() + 256);
        if alt {
            out.extend_from_slice(b"\x1b[?1049h");
        }
        out.extend_from_slice(b"\x1b[2J\x1b[H");
        let content = String::from_utf8_lossy(&pane.stdout);
        for (i, line) in content.lines().enumerate() {
            let row = i + 1;
            out.extend_from_slice(format!("\x1b[{};1H", row).as_bytes());
            out.extend_from_slice(line.as_bytes());
        }
        out.extend_from_slice(format!("\x1b[{};{}H", cy + 1, cx + 1).as_bytes());
        Ok(out)
    }

    /// Smallest-bounding-box resize: register this viewport and apply
    /// `min(cols)` × `min(rows)` across all attached viewports. Returns
    /// the viewport id so the caller can update or remove later.
    pub async fn add_viewport(&self, cols: u16, rows: u16) -> ViewportId {
        let id = NEXT_VIEWPORT_ID.fetch_add(1, Ordering::Relaxed);
        let mut vs = self.inner.viewports.lock().await;
        vs.insert(id, (cols.max(2), rows.max(2)));
        let (mc, mr) = compute_min(&vs);
        drop(vs);
        if let Err(e) = self.apply_pane_size(mc, mr).await {
            tracing::warn!("add_viewport apply: {:#}", e);
        }
        id
    }

    pub async fn update_viewport(&self, id: ViewportId, cols: u16, rows: u16) {
        let mut vs = self.inner.viewports.lock().await;
        if !vs.contains_key(&id) {
            return;
        }
        vs.insert(id, (cols.max(2), rows.max(2)));
        let (mc, mr) = compute_min(&vs);
        drop(vs);
        if let Err(e) = self.apply_pane_size(mc, mr).await {
            tracing::warn!("update_viewport apply: {:#}", e);
        }
    }

    pub async fn remove_viewport(&self, id: ViewportId) {
        let mut vs = self.inner.viewports.lock().await;
        vs.remove(&id);
        if vs.is_empty() {
            return; // no clients — leave pane size as-is
        }
        let (mc, mr) = compute_min(&vs);
        drop(vs);
        if let Err(e) = self.apply_pane_size(mc, mr).await {
            tracing::warn!("remove_viewport apply: {:#}", e);
        }
    }

    async fn apply_pane_size(&self, cols: u16, rows: u16) -> Result<()> {
        let pc = self.inner.pane_cols.swap(cols, Ordering::Relaxed);
        let pr = self.inner.pane_rows.swap(rows, Ordering::Relaxed);
        if pc == cols && pr == rows {
            return Ok(());
        }
        tmux(&[
            "resize-window", "-t", &self.tmux_name,
            "-x", &cols.to_string(), "-y", &rows.to_string(),
        ])
        .await?;
        {
            let mut p = self.inner.parser.lock().await;
            p.screen_mut().set_size(rows, cols);
            // Last_rows captured at old size; clear so we don't false-positive
            // a "scroll" on the next diff.
            *self.inner.last_rows.lock().await = Vec::new();
        }
        Ok(())
    }

    /// Return the most recent `max_lines` rows from the captured scrollback
    /// history (rows that scrolled off the top of the visible pane).
    pub async fn history_text(&self, max_lines: usize) -> String {
        let h = self.inner.history.lock().await;
        let skip = h.len().saturating_sub(max_lines);
        let mut out = String::new();
        for (i, row) in h.iter().skip(skip).enumerate() {
            if i > 0 {
                out.push('\n');
            }
            out.push_str(row);
        }
        out
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
        feed_parser_and_capture(&inner, &chunk).await;
        let _ = inner.tx.send(chunk);
    }
}

/// Feed the byte chunk to the virtual terminal parser, then look at the
/// resulting screen and figure out whether content scrolled off the top
/// since the last observation. If so, append those rows to the scrollback
/// history. This is what makes Claude TUI history recoverable: Claude
/// redraws the screen each time a new message appears, but our shift
/// detection sees that the previous rows have moved up by N and captures
/// the rows that fell off.
async fn feed_parser_and_capture(inner: &Arc<Inner>, chunk: &[u8]) {
    let curr_rows = {
        let mut p = inner.parser.lock().await;
        p.process(chunk);
        screen_to_rows(p.screen())
    };

    let mut last = inner.last_rows.lock().await;
    let prev_rows = std::mem::take(&mut *last);
    *last = curr_rows.clone();
    drop(last);

    if prev_rows.is_empty() || prev_rows.len() != curr_rows.len() {
        return;
    }

    if let Some(k) = detect_shift(&prev_rows, &curr_rows) {
        let mut hist = inner.history.lock().await;
        for row in &prev_rows[..k] {
            hist.push_back(row.clone());
            if hist.len() > HISTORY_CAP {
                hist.pop_front();
            }
        }
    }
}

fn screen_to_rows(screen: &vt100::Screen) -> Vec<String> {
    let (rows, cols) = screen.size();
    screen
        .rows(0, cols)
        .map(|s| s.trim_end().to_string())
        .take(rows as usize)
        .collect()
}

/// Find K such that the suffix `prev[K..]` matches the prefix `curr[..N-K]`,
/// i.e. the screen has shifted up by K rows. Returns None if no shift is
/// detected. K=0 (no shift) returns None — we only care about real scroll.
fn detect_shift(prev: &[String], curr: &[String]) -> Option<usize> {
    let n = prev.len().min(curr.len());
    if n < 2 {
        return None;
    }
    for k in 1..n {
        if prev[k..n] == curr[..n - k] {
            // Skip the case where everything that scrolled out is blank —
            // it's noise from spinner / cursor tweaks on otherwise empty rows.
            if prev[..k].iter().all(|r| r.trim().is_empty()) {
                return None;
            }
            return Some(k);
        }
    }
    None
}

/// Largest bounding box: pane size = max cols and max rows across all
/// attached viewports. The largest client (typically a laptop) stays at its
/// full size; smaller clients (typically phones) see content clipped to
/// their own xterm grid. The opposite policy (smallest) makes the larger
/// client shrink to fit the smaller — usually worse for everyone.
fn compute_min(vs: &HashMap<ViewportId, (u16, u16)>) -> (u16, u16) {
    let mut mc = 0u16;
    let mut mr = 0u16;
    for &(c, r) in vs.values() {
        if c > mc { mc = c; }
        if r > mr { mr = r; }
    }
    if mc == 0 || mr == 0 {
        (80, 24)
    } else {
        (mc, mr)
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
