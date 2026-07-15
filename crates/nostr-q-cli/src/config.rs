use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub state: String,
    pub key_file: String,
}

pub fn expand_tilde(s: &str) -> PathBuf {
    if s == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(s)
}

pub fn default_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("NOSTR_Q_CONFIG").or_else(|_| std::env::var("NQ_CONFIG")) {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("nostr-q.toml");
    if local.exists() {
        return local;
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/nostr-q/config.toml")
}

impl Config {
    pub fn default_new() -> Self {
        Self {
            state: "~/.local/share/nostr-q/state.db".into(),
            key_file: "~/.config/nostr-q/key".into(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| {
            format!(
                "reading config {} - run `nostr-q init` first",
                path.display()
            )
        })?;
        Ok(toml::from_str(&raw)?)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn state_path(&self) -> PathBuf {
        if let Ok(p) = std::env::var("NOSTR_Q_STATE").or_else(|_| std::env::var("NQ_STATE")) {
            return expand_tilde(&p);
        }
        expand_tilde(&self.state)
    }

    pub fn key_path(&self) -> PathBuf {
        expand_tilde(&self.key_file)
    }
}

pub fn load_keys(config: &Config) -> Result<nostr::Keys> {
    if let Ok(sk) =
        std::env::var("NOSTR_Q_PRIVATE_KEY").or_else(|_| std::env::var("NQ_PRIVATE_KEY"))
    {
        return nostr::Keys::parse(sk.trim()).context("parsing NOSTR_Q_PRIVATE_KEY");
    }
    let path = config.key_path();
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "reading key file {} - run `nostr-q key generate` or set NOSTR_Q_PRIVATE_KEY",
            path.display()
        )
    })?;
    nostr::Keys::parse(raw.trim()).context("parsing key file")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_toml_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = Config::default_new();
        cfg.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.state, "~/.local/share/nostr-q/state.db");
        assert_eq!(loaded.key_file, "~/.config/nostr-q/key");
    }

    #[test]
    fn expand_tilde_expands_home() {
        let p = expand_tilde("~/x/y");
        assert!(!p.to_string_lossy().starts_with('~'));
        assert!(p.ends_with("x/y"));
        assert_eq!(
            expand_tilde("/abs/path"),
            std::path::PathBuf::from("/abs/path")
        );
        assert_eq!(expand_tilde("~"), dirs::home_dir().unwrap());
    }

    #[test]
    fn default_config_path_is_xdg_style() {
        // Only meaningful when config env vars are unset and ./nostr-q.toml absent.
        let p = default_config_path();
        assert!(
            p.ends_with(".config/nostr-q/config.toml") || p == std::path::Path::new("nostr-q.toml"),
            "{p:?}"
        );
    }

    #[test]
    fn load_keys_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        let keys = nostr::Keys::generate();
        std::fs::write(&key_path, keys.secret_key().to_secret_hex()).unwrap();
        let cfg = Config {
            state: "unused".into(),
            key_file: key_path.to_string_lossy().into_owned(),
        };
        let loaded = load_keys(&cfg).unwrap();
        assert_eq!(loaded.public_key(), keys.public_key());
    }
}
