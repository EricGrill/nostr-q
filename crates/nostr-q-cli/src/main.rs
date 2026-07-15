mod commands;
mod config;

use clap::{Parser, Subcommand};
use commands::Ctx;

#[derive(Parser)]
#[command(name = "nq", version, about = "Nostr-Q: message queues and pub/sub over Nostr relays")]
struct Cli {
    /// Config file path (default: $NQ_CONFIG, ./nostr-q.toml, ~/.config/nostr-q/config.toml)
    #[arg(long, global = true)]
    config: Option<std::path::PathBuf>,
    /// Emit machine-readable JSON
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create default config and local state db
    Init,
    /// Key management
    Key {
        #[command(subcommand)]
        cmd: KeyCmd,
    },
    /// Relay management
    Relay {
        #[command(subcommand)]
        cmd: RelayCmd,
    },
}

#[derive(Subcommand)]
enum KeyCmd {
    /// Generate a new keypair (private key saved to key file, never printed)
    Generate,
    /// Show the public key
    Show,
}

#[derive(Subcommand)]
enum RelayCmd {
    /// Add a relay URL
    Add { url: String },
    /// List configured relays
    List,
    /// Remove a relay URL
    Remove { url: String },
    /// Check connectivity and latency of configured relays
    Health,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init => commands::init(cli.config),
        Cmd::Key { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                KeyCmd::Generate => commands::key_generate(&ctx),
                KeyCmd::Show => commands::key_show(&ctx),
            }
        }
        Cmd::Relay { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                RelayCmd::Add { url } => commands::relay_add(&ctx, &url),
                RelayCmd::List => commands::relay_list(&ctx),
                RelayCmd::Remove { url } => commands::relay_remove(&ctx, &url),
                RelayCmd::Health => commands::relay_health(&ctx).await,
            }
        }
    }
}
