use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nostr_q::queue::{Delivery, QueueConfig, QueueMode};
use nostr_q::relay::{NostrTransport, Transport};
use nostr_q::store_crate::Store;
use nostr_q::NostrQ;
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
        Ok(Self {
            config: cfg,
            store,
            json,
        })
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
    println!(
        "wrote key file {} (private key not displayed)",
        path.display()
    );
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
    println!(
        "created queue '{}' mode={} delivery={}",
        q.name,
        q.mode.as_str(),
        q.delivery.as_str()
    );
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
                q.name,
                q.mode.as_str(),
                q.delivery.as_str(),
                q.max_attempts,
                q.lease_seconds
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
        println!(
            "published mid={} trace={} event={}",
            receipt.mid, receipt.trace_id, receipt.event_id
        );
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
        (None, Some(url)) => Arc::new(nostr_q_worker::handlers::HttpHandler::new(url)),
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

pub fn inspect(ctx: &Ctx, queue: &str) -> Result<()> {
    anyhow::ensure!(
        ctx.store.get_queue(queue)?.is_some(),
        "unknown queue '{queue}'"
    );
    let now = chrono::Utc::now().timestamp();
    let stats = ctx.store.stats(queue, now)?;
    if ctx.json {
        println!("{}", serde_json::to_string(&stats)?);
    } else {
        println!("queue:            {queue}");
        println!("pending:          {}", stats.pending);
        println!("in-flight:        {}", stats.in_flight);
        println!("acked:            {}", stats.acked);
        println!("dead-lettered:    {}", stats.dead);
        match stats.oldest_pending_age_secs {
            Some(age) => println!("oldest pending:   {age}s"),
            None => println!("oldest pending:   -"),
        }
    }
    Ok(())
}

pub fn trace_cmd(ctx: &Ctx, id: &str) -> Result<()> {
    // accept either a trace id or a message id
    let mut rows = ctx.store.trace(id)?;
    if rows.is_empty() {
        if let Some(trace_id) = ctx.store.trace_id_for_mid(id)? {
            rows = ctx.store.trace(&trace_id)?;
        }
    }
    anyhow::ensure!(!rows.is_empty(), "no lifecycle events for '{id}'");
    if ctx.json {
        println!("{}", serde_json::to_string(&rows)?);
    } else {
        for row in rows {
            let ts = chrono::DateTime::from_timestamp(row.created_at, 0)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| row.created_at.to_string());
            println!("{ts}  {:<16} mid={} {}", row.kind, row.mid, row.detail);
        }
    }
    Ok(())
}

pub fn dlq_list_cmd(ctx: &Ctx, queue: Option<String>) -> Result<()> {
    let rows = ctx.store.dlq_list(queue.as_deref())?;
    if ctx.json {
        println!("{}", serde_json::to_string(&rows)?);
    } else if rows.is_empty() {
        println!("dead-letter queue is empty");
    } else {
        for r in rows {
            println!("{:<28} {:<24} attempts={} reason={}", r.mid, r.queue, r.attempts, r.reason);
        }
    }
    Ok(())
}

pub fn dlq_retry_cmd(ctx: &Ctx, mid: &str) -> Result<()> {
    let rec = ctx
        .store
        .get_message(mid)?
        .ok_or_else(|| anyhow::anyhow!("unknown message id '{mid}'"))?;
    anyhow::ensure!(rec.status == "dead", "message '{mid}' is not dead-lettered (status: {})", rec.status);
    ctx.store.dlq_retry(mid)?;
    ctx.store.record_lifecycle(mid, &rec.trace_id, "dlq_retried", "manual retry via cli")?;
    println!("requeued {mid} on '{}'", rec.queue);
    Ok(())
}
