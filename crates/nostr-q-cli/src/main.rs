mod commands;
mod config;

use clap::{Parser, Subcommand};
use commands::Ctx;

#[derive(Parser)]
#[command(
    name = "nostr-q",
    version,
    about = "Nostr-Q: message queues and pub/sub over Nostr relays"
)]
struct Cli {
    /// Config file path (default: $NOSTR_Q_CONFIG, ./nostr-q.toml, ~/.config/nostr-q/config.toml)
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
        /// Delay delivery by a duration (e.g. 30s, 5m, 2h) — mutually
        /// exclusive with --not-before
        #[arg(long, conflicts_with = "not_before")]
        delay: Option<String>,
        /// Delay delivery until an RFC3339 timestamp — mutually exclusive
        /// with --delay
        #[arg(long)]
        not_before: Option<String>,
        /// Expire (become unclaimable) after a duration from now (e.g.
        /// 30s, 5m, 2h) — mutually exclusive with --expires
        #[arg(long, conflicts_with = "expires")]
        ttl: Option<String>,
        /// Expire (become unclaimable) at an RFC3339 timestamp — mutually
        /// exclusive with --ttl
        #[arg(long)]
        expires: Option<String>,
    },
    /// Subscribe to a pub/sub topic and print events
    Sub { topic: String },
    /// Request/reply RPC: publish a request and block for a single
    /// correlated reply, printing its JSON body
    Call {
        queue: String,
        /// JSON payload (reads stdin when omitted)
        payload: Option<String>,
        /// Seconds to wait for a reply before erroring
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
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
    /// Serve Prometheus metrics (GET /metrics) for queue depth and relay health
    Metrics {
        /// Address to bind the metrics HTTP server to
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
        /// Also probe and export per-relay up/latency gauges on each scrape
        /// (does a network health check, so it's opt-in)
        #[arg(long)]
        with_relays: bool,
    },
    /// Run an HTTP publish ingress (POST /pub/<queue>) so any language can
    /// publish without linking the Rust SDK
    Serve {
        /// Address to bind the HTTP ingress to
        #[arg(long, default_value = "127.0.0.1:8787")]
        addr: String,
        /// Bearer token required on /pub/* (also read from NQ_INGRESS_TOKEN).
        /// Required unless --addr is loopback-only.
        #[arg(long)]
        token: Option<String>,
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
    let cli = Cli::parse();
    // `--json` means "machine-readable everywhere": structured JSON logs on
    // stderr (SRS §15.3), keeping stdout free for the command's own JSON
    // output.
    if cli.json {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    match cli.cmd {
        Cmd::Init => commands::init(cli.config, cli.json),
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
            delay,
            not_before,
            ttl,
            expires,
        } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::publish(&ctx, &queue, payload, idem, delay, not_before, ttl, expires).await
        }
        Cmd::Sub { topic } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::subscribe_cmd(&ctx, &topic).await
        }
        Cmd::Call {
            queue,
            payload,
            timeout,
        } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::call_cmd(&ctx, &queue, payload, timeout).await
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
        Cmd::Metrics { addr, with_relays } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::metrics(&ctx, &addr, with_relays).await
        }
        Cmd::Serve { addr, token } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::serve(&ctx, &addr, token).await
        }
    }
}
