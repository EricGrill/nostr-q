mod commands;
mod config;

use clap::{Parser, Subcommand};
use commands::Ctx;

#[derive(Parser)]
#[command(
    name = "nq",
    version,
    about = "Nostr-Q: message queues and pub/sub over Nostr relays"
)]
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
    /// Queue/topic management
    Queue {
        #[command(subcommand)]
        cmd: QueueCmd,
    },
    /// Publish a JSON message to a queue or topic
    Pub {
        queue: String,
        /// JSON payload (reads stdin when omitted)
        payload: Option<String>,
        /// Idempotency key (duplicate keys on a queue are dropped)
        #[arg(long)]
        idem: Option<String>,
    },
    /// Subscribe to a pub/sub topic and print events
    Sub { topic: String },
    /// Run a worker against a work queue
    Worker {
        queue: String,
        /// Shell command handler (payload on stdin, NQ_* env vars)
        #[arg(long)]
        exec: Option<String>,
        /// HTTP handler endpoint (POST, 2xx = ack)
        #[arg(long)]
        http: Option<String>,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        /// Lease seconds (default: queue config)
        #[arg(long)]
        lease: Option<u64>,
        /// Override queue max attempts
        #[arg(long)]
        max_attempts: Option<u32>,
        /// Heartbeat interval seconds
        #[arg(long, default_value_t = 15)]
        heartbeat: u64,
    },
    /// Show queue depth, in-flight, acked, DLQ counts
    Inspect { queue: String },
    /// Show the lifecycle timeline for a trace id (or message id)
    Trace { id: String },
    /// Dead-letter queue operations
    Dlq {
        #[command(subcommand)]
        cmd: DlqCmd,
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

#[derive(Subcommand)]
enum QueueCmd {
    /// Create or update a queue/topic
    Create {
        name: String,
        /// work_queue | pubsub
        #[arg(long)]
        mode: String,
        /// best_effort | at_most_once | at_least_once
        #[arg(long)]
        delivery: Option<String>,
        #[arg(long)]
        max_attempts: Option<u32>,
        /// Lease seconds for claims
        #[arg(long)]
        lease: Option<u64>,
    },
    /// List queues/topics
    List,
}

#[derive(Subcommand)]
enum DlqCmd {
    /// List dead-lettered messages
    List {
        #[arg(long)]
        queue: Option<String>,
    },
    /// Requeue a dead-lettered message
    Retry { mid: String },
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
        Cmd::Queue { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                QueueCmd::Create {
                    name,
                    mode,
                    delivery,
                    max_attempts,
                    lease,
                } => commands::queue_create(&ctx, &name, &mode, delivery, max_attempts, lease),
                QueueCmd::List => commands::queue_list(&ctx),
            }
        }
        Cmd::Pub {
            queue,
            payload,
            idem,
        } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::publish(&ctx, &queue, payload, idem).await
        }
        Cmd::Sub { topic } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::subscribe_cmd(&ctx, &topic).await
        }
        Cmd::Worker {
            queue,
            exec,
            http,
            concurrency,
            lease,
            max_attempts,
            heartbeat,
        } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::worker(
                &ctx,
                &queue,
                exec,
                http,
                concurrency,
                lease,
                max_attempts,
                heartbeat,
            )
            .await
        }
        Cmd::Inspect { queue } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::inspect(&ctx, &queue)
        }
        Cmd::Trace { id } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::trace_cmd(&ctx, &id)
        }
        Cmd::Dlq { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                DlqCmd::List { queue } => commands::dlq_list_cmd(&ctx, queue),
                DlqCmd::Retry { mid } => commands::dlq_retry_cmd(&ctx, &mid),
            }
        }
    }
}
