use anyhow::Result;

pub struct Config {
    pub bind: String,
    pub token: String,
    pub claude_cmd: String,
    pub tg_token: Option<String>,
    pub tg_owner: Option<i64>,
}

pub fn load() -> Result<Config> {
    let bind = std::env::var("DISBOT_BIND").unwrap_or_else(|_| "127.0.0.1:7878".into());
    let token = std::env::var("DISBOT_TOKEN").unwrap_or_else(|_| "letmein".into());
    let claude_cmd = std::env::var("DISBOT_CMD").unwrap_or_else(|_| detect_shell());
    let tg_token = std::env::var("DISBOT_TG_TOKEN").ok().filter(|s| !s.is_empty());
    let tg_owner = std::env::var("DISBOT_TG_OWNER")
        .ok()
        .and_then(|s| s.trim().parse().ok());
    Ok(Config {
        bind,
        token,
        claude_cmd,
        tg_token,
        tg_owner,
    })
}

fn detect_shell() -> String {
    if let Ok(s) = std::env::var("SHELL") {
        if !s.is_empty() && std::path::Path::new(&s).is_file() {
            return s;
        }
    }
    for candidate in ["/bin/zsh", "/bin/bash", "/bin/sh"] {
        if std::path::Path::new(candidate).is_file() {
            return candidate.into();
        }
    }
    "/bin/sh".into()
}
