use crate::session::{list_disbot_tmux_sessions, pane_current_path, Session};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionMeta {
    pub id: String,
    pub cwd: String,
    pub cmd: String,
    pub created_at: u64,
}

impl SessionMeta {
    pub fn from_session(s: &Session) -> Self {
        Self {
            id: s.id.clone(),
            cwd: s.cwd.clone(),
            cmd: s.cmd.clone(),
            created_at: s.created_at,
        }
    }
}

pub struct Registry {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    meta_path: PathBuf,
    default_cmd: String,
}

impl Registry {
    pub async fn load_or_init(meta_path: PathBuf, default_cmd: String) -> Result<Self> {
        let mut sessions: HashMap<String, Arc<Session>> = HashMap::new();
        let stored: Vec<SessionMeta> = std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let live = list_disbot_tmux_sessions().await.unwrap_or_default();

        for meta in stored {
            if !live.contains(&meta.id) {
                tracing::warn!("metadata had session {} but tmux is gone — dropping", meta.id);
                continue;
            }
            match Session::open(meta.id.clone(), meta.cwd.clone(), meta.cmd.clone(), 120, 40)
                .await
            {
                Ok(s) => {
                    tracing::info!("restored session {}", meta.id);
                    sessions.insert(meta.id.clone(), Arc::new(s));
                }
                Err(e) => tracing::warn!("could not restore session {}: {:#}", meta.id, e),
            }
        }
        // Adopt any live tmux sessions not in the metadata (e.g. created externally).
        for name in live {
            if sessions.contains_key(&name) {
                continue;
            }
            let cwd = pane_current_path(&format!("disbot-{}", name))
                .await
                .unwrap_or_else(default_cwd);
            match Session::open(name.clone(), cwd, default_cmd.clone(), 120, 40).await {
                Ok(s) => {
                    tracing::info!("adopted untracked tmux session {}", name);
                    sessions.insert(name.clone(), Arc::new(s));
                }
                Err(e) => tracing::warn!("could not adopt {}: {:#}", name, e),
            }
        }

        let registry = Self {
            sessions: Mutex::new(sessions),
            meta_path,
            default_cmd,
        };
        registry.save_locked(&*registry.sessions.lock().await)?;
        Ok(registry)
    }

    pub fn default_cmd(&self) -> &str {
        &self.default_cmd
    }

    pub async fn list(&self) -> Vec<SessionMeta> {
        let map = self.sessions.lock().await;
        let mut v: Vec<SessionMeta> = map.values().map(|s| SessionMeta::from_session(s)).collect();
        v.sort_by_key(|m| m.created_at);
        v
    }

    pub async fn get(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().await.get(id).cloned()
    }

    pub async fn create(
        &self,
        id: String,
        cwd: String,
        cmd: String,
        cols: u16,
        rows: u16,
    ) -> Result<Arc<Session>> {
        let mut map = self.sessions.lock().await;
        if map.contains_key(&id) {
            return Err(anyhow!("session '{}' already exists", id));
        }
        let session = Session::open(id.clone(), cwd, cmd, cols, rows).await?;
        let arc = Arc::new(session);
        map.insert(id, arc.clone());
        self.save_locked(&map)?;
        Ok(arc)
    }

    /// Sync registry against live tmux state: adopt new disbot-* sessions, drop dead ones.
    pub async fn refresh_from_tmux(&self) -> Result<usize> {
        let live = list_disbot_tmux_sessions().await.unwrap_or_default();
        let live_set: HashSet<&str> = live.iter().map(|s| s.as_str()).collect();
        let mut map = self.sessions.lock().await;
        let mut changes = 0usize;

        let dead: Vec<String> = map
            .keys()
            .filter(|k| !live_set.contains(k.as_str()))
            .cloned()
            .collect();
        for id in dead {
            tracing::info!("session {} disappeared from tmux — dropping", id);
            map.remove(&id);
            changes += 1;
        }

        for name in live.iter() {
            if map.contains_key(name) {
                continue;
            }
            let cwd = pane_current_path(&format!("disbot-{}", name))
                .await
                .unwrap_or_else(default_cwd);
            match Session::open(name.clone(), cwd, self.default_cmd.clone(), 120, 40).await {
                Ok(s) => {
                    tracing::info!("adopted external tmux session {}", name);
                    map.insert(name.clone(), Arc::new(s));
                    changes += 1;
                }
                Err(e) => tracing::warn!("could not adopt {}: {:#}", name, e),
            }
        }

        if changes > 0 {
            self.save_locked(&map)?;
        }
        Ok(changes)
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        let mut map = self.sessions.lock().await;
        let s = map.remove(id).ok_or_else(|| anyhow!("session not found"))?;
        s.kill().await?;
        self.save_locked(&map)?;
        Ok(())
    }

    fn save_locked(&self, map: &HashMap<String, Arc<Session>>) -> Result<()> {
        if let Some(parent) = self.meta_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut metas: Vec<SessionMeta> =
            map.values().map(|s| SessionMeta::from_session(s)).collect();
        metas.sort_by_key(|m| m.created_at);
        let json = serde_json::to_string_pretty(&metas)?;
        std::fs::write(&self.meta_path, json)?;
        Ok(())
    }
}

fn default_cwd() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/".into())
}
