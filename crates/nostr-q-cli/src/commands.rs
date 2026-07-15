use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use nostr_q::relay::NostrTransport;
use nostr_q::store_crate::Store;
use nostr_q::NostrQ;

use crate::config::{self, Config};

// `store` and `json` are part of the Ctx public contract consumed by later
// CLI tasks (queue/relay/publish/subscribe commands); the `key` subcommands
// implemented here don't touch them yet.
#[allow(dead_code)]
pub struct Ctx {
    pub config: Config,
    pub store: Arc<Store>,
    pub json: bool,
}

impl Ctx {
    pub fn load(config_path: Option<PathBuf>, json: bool) -> Result<Self> {
        let path = config_path.unwrap_or_else(config::default_config_path);
        let cfg = Config::load(&path)?;
        let store = Arc::new(Store::open(&cfg.state_path())?);
        Ok(Self { config: cfg, store, json })
    }

    // Used by later CLI tasks (queue/relay/publish/subscribe commands).
    #[allow(dead_code)]
    pub async fn connect(&self) -> Result<NostrQ> {
        let keys = config::load_keys(&self.config)?;
        let relays = self.store.list_relays()?;
        let transport = Arc::new(NostrTransport::connect(keys.clone(), &relays).await?);
        Ok(NostrQ::new(keys, self.store.clone(), transport))
    }
}

pub fn init(config_path: Option<PathBuf>) -> Result<()> {
    let path = config_path.unwrap_or_else(config::default_config_path);
    if path.exists() {
        println!("config already exists at {}", path.display());
        return Ok(());
    }
    let cfg = Config::default_new();
    cfg.save(&path)?;
    Store::open(&cfg.state_path())?; // create state db + schema now
    println!("initialized config at {}", path.display());
    println!("state db at {}", cfg.state_path().display());
    println!("next: nq key generate && nq relay add <wss://url>");
    Ok(())
}

pub fn key_generate(ctx: &Ctx) -> Result<()> {
    let path = ctx.config.key_path();
    anyhow::ensure!(
        !path.exists(),
        "key file already exists at {} — refusing to overwrite",
        path.display()
    );
    let keys = nostr::Keys::generate();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, keys.secret_key().to_secret_hex())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    println!("wrote key file {} (private key not displayed)", path.display());
    println!("public key: {}", keys.public_key());
    Ok(())
}

pub fn key_show(ctx: &Ctx) -> Result<()> {
    let keys = config::load_keys(&ctx.config)?;
    println!("public key: {}", keys.public_key());
    Ok(())
}
