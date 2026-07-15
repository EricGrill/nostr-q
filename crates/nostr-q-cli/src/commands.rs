use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nostr_q::relay::{NostrTransport, Transport};
use nostr_q::store_crate::Store;
use nostr_q::NostrQ;
use nostr_q::queue::{Delivery, QueueConfig, QueueMode};
use nostr_q_worker::{handlers::ExecHandler, run_worker, Handler, WorkerOptions};
use tokio_util::sync::CancellationToken;

use crate::config::{self, Config};

// `store` and `json` are part of the Ctx public contract consumed by later
// CLI tasks (queue/relay/publish/subscribe commands).
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

pub fn relay_add(ctx: &Ctx, url: &str) -> Result<()> {
    anyhow::ensure!(
        url.starts_with("wss://") || url.starts_with("ws://"),
        "relay url must start with ws:// or wss://"
    );
    ctx.store.add_relay(url)?;
    println!("added relay {url}");
    Ok(())
}

pub fn relay_list(ctx: &Ctx) -> Result<()> {
    let relays = ctx.store.list_relays()?;
    if ctx.json {
        println!("{}", serde_json::to_string(&relays)?);
    } else if relays.is_empty() {
        println!("no relays configured — add one with `nq relay add <url>`");
    } else {
        for url in relays {
            println!("{url}");
        }
    }
    Ok(())
}

pub fn relay_remove(ctx: &Ctx, url: &str) -> Result<()> {
    ctx.store.remove_relay(url)?;
    println!("removed relay {url}");
    Ok(())
}

pub async fn relay_health(ctx: &Ctx) -> Result<()> {
    let keys = config::load_keys(&ctx.config)?;
    let relays = ctx.store.list_relays()?;
    let transport = NostrTransport::connect(keys, &relays).await?;
    let health = transport.health().await;
    if ctx.json {
        println!("{}", serde_json::to_string(&health)?);
    } else {
        for h in health {
            let latency = h
                .latency_ms
                .map(|ms| format!("{ms}ms"))
                .unwrap_or_else(|| "-".into());
            let status = if h.connected { "connected" } else { "DOWN" };
            println!("{:<40} {:<10} {}", h.url, status, latency);
        }
    }
    Ok(())
}

pub fn queue_create(
    ctx: &Ctx,
    name: &str,
    mode: &str,
    delivery: Option<String>,
    max_attempts: Option<u32>,
    lease: Option<u64>,
) -> Result<()> {
    let mode = QueueMode::from_str(mode).map_err(anyhow::Error::msg)?;
    let mut q = match mode {
        QueueMode::WorkQueue => QueueConfig::work_queue(name),
        QueueMode::Pubsub => QueueConfig::pubsub(name),
    };
    if let Some(d) = delivery {
        q.delivery = Delivery::from_str(&d).map_err(anyhow::Error::msg)?;
    }
    if let Some(m) = max_attempts {
        q.max_attempts = m;
    }
    if let Some(l) = lease {
        q.lease_seconds = l;
    }
    ctx.store.upsert_queue(&q)?;
    println!("created queue '{}' mode={} delivery={}", q.name, q.mode.as_str(), q.delivery.as_str());
    Ok(())
}

pub fn queue_list(ctx: &Ctx) -> Result<()> {
    let queues = ctx.store.list_queues()?;
    if ctx.json {
        println!("{}", serde_json::to_string(&queues)?);
    } else if queues.is_empty() {
        println!("no queues — create one with `nq queue create <name> --mode work_queue`");
    } else {
        for q in queues {
            println!(
                "{:<30} {:<11} {:<14} max_attempts={} lease={}s",
                q.name, q.mode.as_str(), q.delivery.as_str(), q.max_attempts, q.lease_seconds
            );
        }
    }
    Ok(())
}

pub async fn publish(
    ctx: &Ctx,
    queue: &str,
    payload: Option<String>,
    idem: Option<String>,
) -> Result<()> {
    let raw = match payload {
        Some(p) => p,
        None => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            s
        }
    };
    let body: serde_json::Value =
        serde_json::from_str(&raw).context("payload must be valid JSON")?;
    let nq = ctx.connect().await?;
    let receipt = nq.publish(queue, body, idem).await?;
    if ctx.json {
        println!("{}", serde_json::to_string(&receipt)?);
    } else {
        println!("published mid={} trace={} event={}", receipt.mid, receipt.trace_id, receipt.event_id);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn worker(
    ctx: &Ctx,
    queue: &str,
    exec: Option<String>,
    http: Option<String>,
    concurrency: usize,
    lease: Option<u64>,
    max_attempts: Option<u32>,
    heartbeat: u64,
) -> Result<()> {
    let mut qcfg = ctx
        .store
        .get_queue(queue)?
        .ok_or_else(|| anyhow::anyhow!("unknown queue '{queue}' — create it first"))?;
    if let Some(m) = max_attempts {
        qcfg.max_attempts = m;
        ctx.store.upsert_queue(&qcfg)?;
    }
    let handler: Arc<dyn Handler> = match (exec, http) {
        (Some(command), None) => Arc::new(ExecHandler { command }),
        (None, Some(_url)) => anyhow::bail!("--http is implemented in the next task"),
        _ => anyhow::bail!("provide exactly one of --exec or --http"),
    };
    let nq = Arc::new(ctx.connect().await?);
    let opts = WorkerOptions {
        concurrency,
        lease_seconds: lease.unwrap_or(qcfg.lease_seconds),
        heartbeat_seconds: heartbeat,
        settle_ms: 750,
        poll_ms: 500,
    };
    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("shutting down gracefully...");
        sd.cancel();
    });
    run_worker(nq, queue.to_string(), handler, opts, shutdown).await
}

pub async fn subscribe_cmd(ctx: &Ctx, topic: &str) -> Result<()> {
    let nq = ctx.connect().await?;
    let mut rx = nq.subscribe(topic).await?;
    eprintln!("subscribed to '{topic}' — ctrl-c to stop");
    while let Some(msg) = rx.recv().await {
        if ctx.json {
            println!(
                "{}",
                serde_json::json!({
                    "mid": msg.mid, "queue": msg.queue, "trace": msg.trace_id,
                    "attempt": msg.attempt, "body": msg.envelope.body
                })
            );
        } else {
            println!("[{}] mid={} {}", msg.queue, msg.mid, msg.envelope.body);
        }
    }
    Ok(())
}
