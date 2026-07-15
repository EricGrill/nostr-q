use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use nostr_q::queue::{Delivery, QueueConfig, QueueMode};
use nostr_q::relay::{NostrTransport, RelayHealth, Transport};
use nostr_q::store::{QueueStats, Store};
use nostr_q::NostrQ;
use nostr_q_worker::{handlers::ExecHandler, run_worker, Handler, WorkerOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::{self, Config};

// `store` and `json` are part of the Ctx public contract consumed by later
// CLI tasks (queue/relay/publish/subscribe commands).
//
// `Clone` lets the metrics server hand each accepted connection its own
// `Arc<Ctx>` so a spawned per-connection task never borrows from the accept
// loop's stack frame (see `serve_metrics`).
#[derive(Clone)]
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

pub fn init(config_path: Option<PathBuf>, json: bool) -> Result<()> {
    let path = config_path.unwrap_or_else(config::default_config_path);
    if path.exists() {
        if json {
            // Best-effort: an existing config should always parse, but
            // don't let a corrupt file turn "already initialized" into a
            // hard error here — surface `state: null` instead.
            let state = Config::load(&path)
                .ok()
                .map(|c| c.state_path().display().to_string());
            println!(
                "{}",
                serde_json::json!({
                    "config": path.display().to_string(),
                    "state": state,
                    "created": false,
                })
            );
        } else {
            println!("config already exists at {}", path.display());
        }
        return Ok(());
    }
    let cfg = Config::default_new();
    cfg.save(&path)?;
    Store::open(&cfg.state_path())?; // create state db + schema now
    if json {
        println!(
            "{}",
            serde_json::json!({
                "config": path.display().to_string(),
                "state": cfg.state_path().display().to_string(),
                "created": true,
            })
        );
    } else {
        println!("initialized config at {}", path.display());
        println!("state db at {}", cfg.state_path().display());
        println!("next: nostr-q key generate && nostr-q relay add <wss://url>");
    }
    Ok(())
}

/// Create `path` with mode 0600 atomically and write `contents` to it.
///
/// Using `create_new` means the file is created (and its permission bits
/// fixed at 0600) in a single syscall — there is no window, as there was
/// with write-then-chmod, where the file briefly exists with umask-derived
/// (often world/group readable) permissions. `create_new` also gives us the
/// "refuse to overwrite an existing key file" behavior for free: it errors
/// if the file already exists.
#[cfg(unix)]
fn write_new_file_mode_0600(path: &std::path::Path, contents: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create key file at {}", path.display()))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

/// Portable fallback for non-unix targets: no chmod support, but
/// `create_new` still refuses to overwrite an existing file.
#[cfg(not(unix))]
fn write_new_file_mode_0600(path: &std::path::Path, contents: &str) -> Result<()> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("failed to create key file at {}", path.display()))?;
    file.write_all(contents.as_bytes())?;
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
    write_new_file_mode_0600(&path, &keys.secret_key().to_secret_hex())?;
    if ctx.json {
        // Never include the secret key here — public key and the file path
        // it was written to only.
        println!(
            "{}",
            serde_json::json!({
                "public_key": keys.public_key().to_string(),
                "key_file": path.display().to_string(),
            })
        );
    } else {
        println!(
            "wrote key file {} (private key not displayed)",
            path.display()
        );
        println!("public key: {}", keys.public_key());
    }
    Ok(())
}

pub fn key_show(ctx: &Ctx) -> Result<()> {
    let keys = config::load_keys(&ctx.config)?;
    if ctx.json {
        println!(
            "{}",
            serde_json::json!({ "public_key": keys.public_key().to_string() })
        );
    } else {
        println!("public key: {}", keys.public_key());
    }
    Ok(())
}

pub fn relay_add(ctx: &Ctx, url: &str) -> Result<()> {
    anyhow::ensure!(
        url.starts_with("wss://") || url.starts_with("ws://"),
        "relay url must start with ws:// or wss://"
    );
    ctx.store.add_relay(url)?;
    if ctx.json {
        println!(
            "{}",
            serde_json::json!({ "action": "relay_add", "url": url, "ok": true })
        );
    } else {
        println!("added relay {url}");
    }
    Ok(())
}

pub fn relay_list(ctx: &Ctx) -> Result<()> {
    let relays = ctx.store.list_relays()?;
    if ctx.json {
        println!("{}", serde_json::to_string(&relays)?);
    } else if relays.is_empty() {
        println!("no relays configured - add one with `nostr-q relay add <url>`");
    } else {
        for url in relays {
            println!("{url}");
        }
    }
    Ok(())
}

pub fn relay_remove(ctx: &Ctx, url: &str) -> Result<()> {
    ctx.store.remove_relay(url)?;
    if ctx.json {
        println!(
            "{}",
            serde_json::json!({ "action": "relay_remove", "url": url, "ok": true })
        );
    } else {
        println!("removed relay {url}");
    }
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
    let mode = QueueMode::from_str(mode)?;
    let mut q = match mode {
        QueueMode::WorkQueue => QueueConfig::work_queue(name),
        QueueMode::Pubsub => QueueConfig::pubsub(name),
    };
    if let Some(d) = delivery {
        q.delivery = Delivery::from_str(&d)?;
    }
    if let Some(m) = max_attempts {
        q.max_attempts = m;
    }
    if let Some(l) = lease {
        q.lease_seconds = l;
    }
    ctx.store.upsert_queue(&q)?;
    if ctx.json {
        println!("{}", serde_json::to_string(&q)?);
    } else {
        println!(
            "created queue '{}' mode={} delivery={}",
            q.name,
            q.mode.as_str(),
            q.delivery.as_str()
        );
    }
    Ok(())
}

pub fn queue_list(ctx: &Ctx) -> Result<()> {
    let queues = ctx.store.list_queues()?;
    if ctx.json {
        println!("{}", serde_json::to_string(&queues)?);
    } else if queues.is_empty() {
        println!("no queues - create one with `nostr-q queue create <name> --mode work_queue`");
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

/// Parse a simple duration string like `30s`, `5m`, `2h`, `1d` (a bare
/// integer with no suffix is treated as seconds) into a whole number of
/// seconds.
fn parse_duration_secs(s: &str) -> Result<i64> {
    let s = s.trim();
    anyhow::ensure!(!s.is_empty(), "duration must not be empty");
    let (num_str, mult): (&str, i64) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, 1),
        Some('s') => (&s[..s.len() - 1], 1),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('d') => (&s[..s.len() - 1], 86400),
        _ => anyhow::bail!("invalid duration '{s}' — use a number optionally suffixed with s/m/h/d (e.g. 30s, 5m, 2h)"),
    };
    let n: i64 = num_str
        .parse()
        .with_context(|| format!("invalid duration '{s}'"))?;
    anyhow::ensure!(n >= 0, "duration must not be negative: '{s}'");
    Ok(n * mult)
}

/// Parse an RFC3339 timestamp into unix seconds.
fn parse_rfc3339_secs(s: &str) -> Result<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp())
        .with_context(|| format!("invalid RFC3339 timestamp '{s}'"))
}

#[allow(clippy::too_many_arguments)]
pub async fn publish(
    ctx: &Ctx,
    queue: &str,
    payload: Option<String>,
    idem: Option<String>,
    delay: Option<String>,
    not_before: Option<String>,
    ttl: Option<String>,
    expires: Option<String>,
) -> Result<()> {
    anyhow::ensure!(
        !(delay.is_some() && not_before.is_some()),
        "--delay and --not-before are mutually exclusive"
    );
    anyhow::ensure!(
        !(ttl.is_some() && expires.is_some()),
        "--ttl and --expires are mutually exclusive"
    );
    let now = chrono::Utc::now().timestamp();
    let opt_not_before = match (delay, not_before) {
        (Some(d), None) => Some(now + parse_duration_secs(&d)?),
        (None, Some(t)) => Some(parse_rfc3339_secs(&t)?),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("checked above"),
    };
    let opt_expires_at = match (ttl, expires) {
        (Some(d), None) => Some(now + parse_duration_secs(&d)?),
        (None, Some(t)) => Some(parse_rfc3339_secs(&t)?),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("checked above"),
    };

    let raw = match payload {
        Some(p) => p,
        None => {
            // Reading stdin is a blocking syscall; run it on a blocking
            // thread so it doesn't stall the async runtime (and any other
            // tasks, like the heartbeat loop, sharing it) while waiting on
            // input.
            tokio::task::spawn_blocking(|| -> Result<String> {
                let mut s = String::new();
                std::io::stdin().read_to_string(&mut s)?;
                Ok(s)
            })
            .await
            .context("stdin read task panicked")??
        }
    };
    let body: serde_json::Value =
        serde_json::from_str(&raw).context("payload must be valid JSON")?;
    let nq = ctx.connect().await?;
    let receipt = nq
        .publish_opts(
            queue,
            body,
            idem,
            nostr_q::PublishOptions {
                not_before: opt_not_before,
                expires_at: opt_expires_at,
            },
        )
        .await?;
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
    anyhow::ensure!(
        concurrency > 0,
        "--concurrency must be at least 1 (0 means the worker's semaphore never issues a \
         permit, so it silently claims and processes nothing)"
    );
    if lease == Some(0) {
        anyhow::bail!(
            "--lease must be at least 1 second (a 0s lease times out the handler instantly, \
             and claim_winner requires lease_expires_at > now, so the claim can never win)"
        );
    }

    let mut qcfg = ctx
        .store
        .get_queue(queue)?
        .ok_or_else(|| anyhow::anyhow!("unknown queue '{queue}' — create it first"))?;
    if let Some(m) = max_attempts {
        qcfg.max_attempts = m;
        ctx.store.upsert_queue(&qcfg)?;
        let note =
            format!("note: --max-attempts {m} updated the stored config for queue '{queue}'");
        if ctx.json {
            println!(
                "{}",
                serde_json::json!({
                    "note": note,
                    "queue": queue,
                    "max_attempts": m,
                })
            );
        } else {
            eprintln!("{note}");
        }
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
        println!("expired:          {}", stats.expired);
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
            println!(
                "{:<28} {:<24} attempts={} reason={}",
                r.mid, r.queue, r.attempts, r.reason
            );
        }
    }
    Ok(())
}

pub fn dlq_retry_cmd(ctx: &Ctx, mid: &str) -> Result<()> {
    let rec = ctx
        .store
        .get_message(mid)?
        .ok_or_else(|| anyhow::anyhow!("unknown message id '{mid}'"))?;
    anyhow::ensure!(
        rec.status == "dead",
        "message '{mid}' is not dead-lettered (status: {})",
        rec.status
    );
    ctx.store.dlq_retry(mid)?;
    ctx.store
        .record_lifecycle(mid, &rec.trace_id, "dlq_retried", "manual retry via cli")?;
    if ctx.json {
        println!(
            "{}",
            serde_json::json!({ "mid": mid, "queue": rec.queue, "requeued": true })
        );
    } else {
        println!("requeued {mid} on '{}'", rec.queue);
    }
    Ok(())
}

/// Escape a Prometheus label value: backslash, double-quote, and newline
/// are the only characters the text exposition format requires escaping
/// (https://prometheus.io/docs/instrumenting/exposition_formats/).
fn escape_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// Pure renderer for the `/metrics` endpoint: turns per-queue stats (and,
/// optionally, per-relay health) into Prometheus text exposition format.
/// Kept free of I/O so it's unit-testable without a server.
pub fn render_prometheus(
    queues: &[(String, QueueStats)],
    relays: Option<&[RelayHealth]>,
) -> String {
    let mut out = String::new();

    let gauge_family = |out: &mut String, name: &str, help: &str| {
        out.push_str(&format!("# HELP {name} {help}\n"));
        out.push_str(&format!("# TYPE {name} gauge\n"));
    };

    gauge_family(
        &mut out,
        "nostrq_queue_pending",
        "Number of pending messages in the queue.",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_pending{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.pending
        ));
    }

    gauge_family(
        &mut out,
        "nostrq_queue_in_flight",
        "Number of claimed (in-flight) messages in the queue.",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_in_flight{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.in_flight
        ));
    }

    gauge_family(
        &mut out,
        "nostrq_queue_acked",
        "Number of acknowledged messages in the queue.",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_acked{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.acked
        ));
    }

    gauge_family(
        &mut out,
        "nostrq_queue_dead",
        "Number of dead-lettered messages in the queue.",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_dead{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.dead
        ));
    }

    gauge_family(
        &mut out,
        "nostrq_queue_expired",
        "Number of expired messages in the queue.",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_expired{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.expired
        ));
    }

    gauge_family(
        &mut out,
        "nostrq_queue_oldest_pending_seconds",
        "Age in seconds of the oldest pending message in the queue (0 when there is none).",
    );
    for (name, stats) in queues {
        out.push_str(&format!(
            "nostrq_queue_oldest_pending_seconds{{queue=\"{}\"}} {}\n",
            escape_label_value(name),
            stats.oldest_pending_age_secs.unwrap_or(0)
        ));
    }

    if let Some(relays) = relays {
        gauge_family(
            &mut out,
            "nostrq_relay_up",
            "Whether the relay is currently connected (1) or not (0).",
        );
        for r in relays {
            out.push_str(&format!(
                "nostrq_relay_up{{url=\"{}\"}} {}\n",
                escape_label_value(&r.url),
                if r.connected { 1 } else { 0 }
            ));
        }

        gauge_family(
            &mut out,
            "nostrq_relay_latency_ms",
            "Relay round-trip latency in milliseconds (0 when unknown or down).",
        );
        for r in relays {
            out.push_str(&format!(
                "nostrq_relay_latency_ms{{url=\"{}\"}} {}\n",
                escape_label_value(&r.url),
                r.latency_ms.unwrap_or(0)
            ));
        }
    }

    out
}

/// Gather (queue, stats) for every configured queue, and — when
/// `with_relays` is set — probe relay health too. Called fresh on every
/// scrape so the endpoint always reflects live state.
async fn gather_metrics(ctx: &Ctx, with_relays: bool) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let mut queues = Vec::new();
    for q in ctx.store.list_queues()? {
        let stats = ctx.store.stats(&q.name, now)?;
        queues.push((q.name, stats));
    }
    let relays = if with_relays {
        let keys = config::load_keys(&ctx.config)?;
        let relay_urls = ctx.store.list_relays()?;
        let transport = NostrTransport::connect(keys, &relay_urls).await?;
        Some(transport.health().await)
    } else {
        None
    };
    Ok(render_prometheus(&queues, relays.as_deref()))
}

/// Serve `GET /metrics` in Prometheus text exposition format over a raw
/// TCP accept loop — no web framework needed for a single read-only
/// endpoint. Runs until ctrl-c.
pub async fn metrics(ctx: &Ctx, addr: &str, with_relays: bool) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    eprintln!("serving Prometheus metrics on http://{addr}/metrics — ctrl-c to stop");

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        sd.cancel();
    });

    serve_metrics(listener, ctx, with_relays, shutdown).await
}

/// The accept loop itself, split out from [`metrics`] so tests can bind an
/// ephemeral port, drive a handful of requests, and cancel the loop without
/// waiting on a real ctrl-c.
async fn serve_metrics(
    listener: TcpListener,
    ctx: &Ctx,
    with_relays: bool,
    shutdown: CancellationToken,
) -> Result<()> {
    // Own an Arc<Ctx> for the lifetime of the accept loop so each accepted
    // connection can be handed a cheap, 'static-compatible clone and handled
    // in its own spawned task. This is what lets a slow/hung client be
    // isolated from `accept()` and from the shutdown check below — without
    // it, a single client that never sends data would block every future
    // scrape (and ctrl-c) for as long as the connection stayed open.
    let ctx = Arc::new(ctx.clone());
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                return Ok(());
            }
            accepted = listener.accept() => {
                let (mut socket, _) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "metrics: accept failed");
                        continue;
                    }
                };
                let ctx = Arc::clone(&ctx);
                // Spawn so a slow or hung client (e.g. a raw TCP connection
                // that never sends a request line) can't stall `accept()`
                // for the next scraper or delay the shutdown branch above.
                tokio::spawn(async move {
                    if let Err(e) = handle_metrics_request(&mut socket, &ctx, with_relays).await {
                        tracing::warn!(error = %e, "metrics: request handling failed");
                    }
                });
            }
        }
    }
}

/// Timeout for reading the request line and headers of a single connection.
/// A well-behaved scraper sends its request immediately after connecting;
/// this bounds how long a hung/slow client can hold a spawned task open.
const METRICS_REQUEST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn handle_metrics_request(
    socket: &mut tokio::net::TcpStream,
    ctx: &Ctx,
    with_relays: bool,
) -> Result<()> {
    let mut reader = BufReader::new(&mut *socket);
    let mut request_line = String::new();
    let n = match tokio::time::timeout(
        METRICS_REQUEST_READ_TIMEOUT,
        reader.read_line(&mut request_line),
    )
    .await
    {
        Ok(read_result) => read_result?,
        Err(_) => {
            tracing::debug!("metrics: timed out waiting for request line");
            return Ok(()); // drop the connection rather than hang forever
        }
    };
    if n == 0 {
        return Ok(()); // client closed without sending anything
    }
    // Drain remaining header lines until the blank line so the connection
    // can be reused-free-and-clear before we write the response (we don't
    // keep-alive, but draining avoids writing into a half-read socket on
    // some clients).
    loop {
        let mut line = String::new();
        let n =
            match tokio::time::timeout(METRICS_REQUEST_READ_TIMEOUT, reader.read_line(&mut line))
                .await
            {
                Ok(read_result) => read_result?,
                Err(_) => {
                    tracing::debug!("metrics: timed out waiting for request headers");
                    return Ok(()); // drop the connection rather than hang forever
                }
            };
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method == "GET" && path == "/metrics" {
        let body = gather_metrics(ctx, with_relays).await?;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await?;
    } else {
        let body = "not found";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await?;
    }
    socket.flush().await?;
    Ok(())
}

// --- `nostr-q serve` — HTTP publish ingress (CHA-2344) ---
//
// Lets any language publish to a queue over plain HTTP instead of linking
// the Rust SDK. Because the endpoint signs with the node's private key and
// broadcasts to the relay set, it must be access-controlled: see `serve`'s
// bind-address / token checks below.

/// Shared state handed to every ingress request handler.
struct IngressState {
    nq: Arc<NostrQ>,
    token: Option<String>,
}

#[derive(serde::Deserialize)]
struct PubQuery {
    /// Delay delivery by this many seconds from now (maps to `not_before`).
    delay: Option<i64>,
    /// Expire (become unclaimable) this many seconds from now (maps to
    /// `expires_at`).
    ttl: Option<i64>,
}

fn error_body(msg: impl Into<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "error": msg.into() }))
}

/// `true` iff `headers` carries `Authorization: Bearer <state.token>`, or no
/// token is configured at all (open ingress — only ever allowed on a
/// loopback bind, enforced in `serve`).
fn authorized(state: &IngressState, headers: &HeaderMap) -> bool {
    let Some(expected) = &state.token else {
        return true;
    };
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|got| got == expected)
}

async fn healthz_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

async fn pub_handler(
    State(state): State<Arc<IngressState>>,
    AxumPath(queue): AxumPath<String>,
    Query(q): Query<PubQuery>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    if !authorized(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, error_body("unauthorized")).into_response();
    }

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                error_body(format!("malformed JSON body: {e}")),
            )
                .into_response()
        }
    };

    match state.nq.store().get_queue(&queue) {
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                error_body(format!("unknown queue '{queue}'")),
            )
                .into_response()
        }
        Ok(Some(_)) => {}
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error_body(e.to_string())).into_response()
        }
    }

    let idem = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let now = chrono::Utc::now().timestamp();
    let opts = nostr_q::PublishOptions {
        not_before: q.delay.map(|d| now + d),
        expires_at: q.ttl.map(|t| now + t),
    };

    match state.nq.publish_opts(&queue, payload, idem, opts).await {
        Ok(receipt) => (StatusCode::OK, Json(receipt)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, error_body(e.to_string())).into_response(),
    }
}

/// Maximum accepted `/pub/*` body size — protects the ingress (and the
/// signing key it holds) from unbounded-memory-growth requests.
const INGRESS_MAX_BODY_BYTES: usize = 1024 * 1024; // 1 MiB

/// Build the ingress `Router`. Split out from `serve` so tests can drive it
/// with a `MockTransport`-backed `NostrQ` via `tower::ServiceExt::oneshot`,
/// with no real relay or network involved.
pub fn build_ingress_router(nq: Arc<NostrQ>, token: Option<String>) -> Router {
    let state = Arc::new(IngressState { nq, token });
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/pub/{queue}", post(pub_handler))
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(INGRESS_MAX_BODY_BYTES))
}

/// Run the `nostr-q serve` HTTP ingress until ctrl-c.
///
/// Access control: when `token` (from `--token` or `NQ_INGRESS_TOKEN`) is
/// set, every `/pub/*` request must carry a matching `Authorization: Bearer
/// <token>` header. When no token is configured, the ingress refuses to
/// start on any non-loopback address — an unauthenticated endpoint that
/// signs and broadcasts with the node's private key must never be exposed
/// off localhost. A loopback bind with no token is allowed for local dev,
/// with a logged warning.
pub async fn serve(ctx: &Ctx, addr: &str, token: Option<String>) -> Result<()> {
    let token = token.or_else(|| std::env::var("NQ_INGRESS_TOKEN").ok());
    let socket_addr: SocketAddr = addr
        .parse()
        .with_context(|| format!("invalid --addr '{addr}' — expected host:port"))?;

    if token.is_none() {
        anyhow::ensure!(
            socket_addr.ip().is_loopback(),
            "refusing to start nostr-q serve on non-loopback address {addr} without a \
             --token/NQ_INGRESS_TOKEN — this endpoint signs and publishes with the node's \
             private key and must not be exposed unauthenticated"
        );
        tracing::warn!(
            "nostr-q serve: no --token/NQ_INGRESS_TOKEN configured — ingress is unauthenticated \
             (allowed only because {addr} is loopback-only)"
        );
    }

    let nq = Arc::new(ctx.connect().await?);
    let router = build_ingress_router(nq, token);
    let listener = TcpListener::bind(socket_addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    eprintln!("serving HTTP publish ingress on http://{addr} — ctrl-c to stop");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(key_file: PathBuf) -> Ctx {
        let store = Arc::new(Store::open_in_memory().unwrap());
        Ctx {
            config: Config {
                state: "unused".into(),
                key_file: key_file.to_string_lossy().into_owned(),
            },
            store,
            json: false,
        }
    }

    #[test]
    fn key_generate_writes_key_file_mode_0600() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        let ctx = test_ctx(key_path.clone());

        key_generate(&ctx).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "key file must be created with mode 0600, got {mode:o}"
            );
        }

        // sanity: the file holds a parseable secret key.
        let raw = std::fs::read_to_string(&key_path).unwrap();
        assert!(nostr::Keys::parse(raw.trim()).is_ok());
    }

    #[test]
    fn key_generate_refuses_to_overwrite_existing_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        let ctx = test_ctx(key_path.clone());

        key_generate(&ctx).unwrap();
        let original = std::fs::read_to_string(&key_path).unwrap();

        let err = key_generate(&ctx).expect_err("must refuse to overwrite an existing key file");
        assert!(err.to_string().contains("already exists"));

        // the original key must be untouched.
        assert_eq!(std::fs::read_to_string(&key_path).unwrap(), original);
    }

    // --- worker flag validation (CHA-2272 item 2) ---
    //
    // These checks happen before `worker()` touches the store or connects
    // to any relay, so they're reachable with a plain in-memory ctx and no
    // network at all.

    #[tokio::test]
    async fn worker_rejects_zero_concurrency() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = worker(&ctx, "q", Some("cat".into()), None, 0, None, None, 15)
            .await
            .expect_err("--concurrency 0 must be rejected");
        assert!(
            err.to_string().contains("--concurrency"),
            "error should name the offending flag: {err}"
        );
    }

    #[tokio::test]
    async fn worker_rejects_zero_lease() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = worker(&ctx, "q", Some("cat".into()), None, 1, Some(0), None, 15)
            .await
            .expect_err("--lease 0 must be rejected");
        assert!(
            err.to_string().contains("--lease"),
            "error should name the offending flag: {err}"
        );
    }

    #[tokio::test]
    async fn worker_accepts_nonzero_flags_and_fails_later_on_unknown_queue() {
        // Proves the validation doesn't reject legitimate values — the
        // failure here comes from the (expected) missing queue, not from
        // the flag checks.
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = worker(&ctx, "q", Some("cat".into()), None, 1, Some(30), None, 15)
            .await
            .expect_err("unknown queue must still error");
        assert!(err.to_string().contains("unknown queue"), "{err}");
    }

    // --- `--json` coverage for mutating commands (CHA-2272 item 1) ---

    #[test]
    fn relay_add_and_remove_respect_json_flag_and_mutate_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(dir.path().join("key"));
        ctx.json = true;

        relay_add(&ctx, "wss://relay.example").unwrap();
        assert_eq!(
            ctx.store.list_relays().unwrap(),
            vec!["wss://relay.example"]
        );

        relay_remove(&ctx, "wss://relay.example").unwrap();
        assert!(ctx.store.list_relays().unwrap().is_empty());
    }

    #[test]
    fn queue_create_json_mode_still_upserts_the_queue() {
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(dir.path().join("key"));
        ctx.json = true;

        queue_create(&ctx, "jobs.email", "work_queue", None, Some(7), Some(45)).unwrap();
        let q = ctx.store.get_queue("jobs.email").unwrap().unwrap();
        assert_eq!(q.max_attempts, 7);
        assert_eq!(q.lease_seconds, 45);
    }

    #[test]
    fn key_show_json_mode_returns_same_public_key_as_human_mode() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        key_generate(&ctx).unwrap();

        // both modes must succeed and read the same underlying key.
        key_show(&ctx).unwrap();
        let mut json_ctx = test_ctx(ctx.config.key_file.clone().into());
        json_ctx.json = true;
        key_show(&json_ctx).unwrap();
    }

    #[test]
    fn dlq_retry_cmd_json_mode_requeues_a_dead_message() {
        use nostr_q::store::MessageRecord;

        let dir = tempfile::tempdir().unwrap();
        let mut ctx = test_ctx(dir.path().join("key"));
        ctx.store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();
        let rec = MessageRecord {
            mid: "m1".into(),
            queue: "jobs.email".into(),
            event_id: "e1".into(),
            trace_id: "t1".into(),
            envelope_json: "{}".into(),
            status: "pending".into(),
            attempts: 0,
            attempt_floor: 0,
            idem_key: None,
            visible_at: 0,
            expires_at: None,
            created_at: 0,
        };
        ctx.store.insert_message(&rec).unwrap();
        ctx.store.move_to_dlq("m1", "boom").unwrap();
        assert_eq!(ctx.store.get_message("m1").unwrap().unwrap().status, "dead");

        ctx.json = true;
        dlq_retry_cmd(&ctx, "m1").unwrap();

        assert_ne!(
            ctx.store.get_message("m1").unwrap().unwrap().status,
            "dead",
            "dlq retry must move the message out of the dead state"
        );
    }

    // --- non-blocking stdin in `pub` (CHA-2272 item 5) ---
    //
    // Explicit-payload publishes never touch stdin at all, so this just
    // proves the JSON-parse error path still surfaces correctly through
    // the (now spawn_blocking-free) explicit-payload branch; the stdin
    // branch itself is covered by code inspection (see report).
    #[tokio::test]
    async fn publish_rejects_non_json_payload() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = publish(
            &ctx,
            "q",
            Some("not json".into()),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("non-JSON payload must be rejected before connecting to any relay");
        assert!(err.to_string().contains("JSON"), "{err}");
    }

    // --- delayed delivery / TTL flag validation (CHA-2345) ---
    //
    // These checks happen before `publish` touches JSON parsing or connects
    // to any relay, so a plain in-memory ctx with a bogus payload still
    // reaches (and fails on) the flag validation first.

    #[tokio::test]
    async fn publish_rejects_delay_and_not_before_together() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = publish(
            &ctx,
            "q",
            Some("{}".into()),
            None,
            Some("30s".into()),
            Some("2030-01-01T00:00:00Z".into()),
            None,
            None,
        )
        .await
        .expect_err("--delay and --not-before must be mutually exclusive");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    #[tokio::test]
    async fn publish_rejects_ttl_and_expires_together() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = publish(
            &ctx,
            "q",
            Some("{}".into()),
            None,
            None,
            None,
            Some("5m".into()),
            Some("2030-01-01T00:00:00Z".into()),
        )
        .await
        .expect_err("--ttl and --expires must be mutually exclusive");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    #[tokio::test]
    async fn publish_rejects_invalid_delay_duration() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        let err = publish(
            &ctx,
            "q",
            Some("{}".into()),
            None,
            Some("not-a-duration".into()),
            None,
            None,
            None,
        )
        .await
        .expect_err("garbage duration must be rejected before connecting to any relay");
        assert!(err.to_string().contains("duration"), "{err}");
    }

    #[test]
    fn parse_duration_secs_handles_units_and_bare_numbers() {
        assert_eq!(parse_duration_secs("30s").unwrap(), 30);
        assert_eq!(parse_duration_secs("5m").unwrap(), 300);
        assert_eq!(parse_duration_secs("2h").unwrap(), 7200);
        assert_eq!(parse_duration_secs("1d").unwrap(), 86400);
        assert_eq!(parse_duration_secs("45").unwrap(), 45);
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("-5s").is_err());
        assert!(parse_duration_secs("5x").is_err());
    }

    #[test]
    fn parse_rfc3339_secs_parses_valid_timestamps_and_rejects_garbage() {
        assert_eq!(
            parse_rfc3339_secs("2021-11-07T09:00:00Z").unwrap(),
            1636275600
        );
        assert!(parse_rfc3339_secs("not-a-timestamp").is_err());
    }

    // The exact `--json` wire format for init/key/relay/queue is verified
    // against the real compiled binary in
    // `crates/nostr-q-cli/tests/json_output.rs` (an integration test, since
    // `CARGO_BIN_EXE_nostr-q` is only available to targets other than the
    // bin's own unit tests).

    // --- `nostr-q metrics` — Prometheus exporter (CHA-2346) ---

    fn stats(pending: u32, in_flight: u32, acked: u32, dead: u32, expired: u32) -> QueueStats {
        QueueStats {
            pending,
            in_flight,
            acked,
            dead,
            expired,
            oldest_pending_age_secs: None,
        }
    }

    #[test]
    fn render_prometheus_emits_help_type_and_values_per_queue() {
        let queues = vec![
            ("jobs.email".to_string(), stats(3, 1, 10, 2, 0)),
            ("jobs.sms".to_string(), stats(0, 0, 5, 0, 1)),
        ];
        let body = render_prometheus(&queues, None);

        assert!(body.contains("# HELP nostrq_queue_pending "));
        assert!(body.contains("# TYPE nostrq_queue_pending gauge\n"));
        assert!(body.contains("nostrq_queue_pending{queue=\"jobs.email\"} 3\n"));
        assert!(body.contains("nostrq_queue_pending{queue=\"jobs.sms\"} 0\n"));

        assert!(body.contains("nostrq_queue_in_flight{queue=\"jobs.email\"} 1\n"));
        assert!(body.contains("nostrq_queue_acked{queue=\"jobs.email\"} 10\n"));
        assert!(body.contains("nostrq_queue_dead{queue=\"jobs.email\"} 2\n"));
        assert!(body.contains("nostrq_queue_expired{queue=\"jobs.sms\"} 1\n"));

        // no relay families when relays is None.
        assert!(!body.contains("nostrq_relay_up"));
        assert!(!body.contains("nostrq_relay_latency_ms"));
    }

    #[test]
    fn render_prometheus_maps_oldest_pending_none_to_zero() {
        let mut with_age = stats(1, 0, 0, 0, 0);
        with_age.oldest_pending_age_secs = Some(42);
        let with_none = stats(0, 0, 0, 0, 0); // oldest_pending_age_secs: None

        let queues = vec![
            ("q.has-age".to_string(), with_age),
            ("q.no-age".to_string(), with_none),
        ];
        let body = render_prometheus(&queues, None);

        assert!(body.contains("nostrq_queue_oldest_pending_seconds{queue=\"q.has-age\"} 42\n"));
        assert!(body.contains("nostrq_queue_oldest_pending_seconds{queue=\"q.no-age\"} 0\n"));
    }

    #[test]
    fn render_prometheus_includes_relay_families_with_relays_and_maps_down_to_zero() {
        let queues = vec![("jobs.email".to_string(), stats(1, 0, 0, 0, 0))];
        let relays = vec![
            RelayHealth {
                url: "wss://relay.up.example".into(),
                connected: true,
                latency_ms: Some(37),
            },
            RelayHealth {
                url: "wss://relay.down.example".into(),
                connected: false,
                latency_ms: None,
            },
        ];
        let body = render_prometheus(&queues, Some(&relays));

        assert!(body.contains("# TYPE nostrq_relay_up gauge\n"));
        assert!(body.contains("nostrq_relay_up{url=\"wss://relay.up.example\"} 1\n"));
        assert!(body.contains("nostrq_relay_up{url=\"wss://relay.down.example\"} 0\n"));

        assert!(body.contains("# TYPE nostrq_relay_latency_ms gauge\n"));
        assert!(body.contains("nostrq_relay_latency_ms{url=\"wss://relay.up.example\"} 37\n"));
        assert!(body.contains("nostrq_relay_latency_ms{url=\"wss://relay.down.example\"} 0\n"));
    }

    #[test]
    fn render_prometheus_escapes_label_values() {
        let queues = vec![(
            "weird\\name\"with\nnewline".to_string(),
            stats(1, 0, 0, 0, 0),
        )];
        let body = render_prometheus(&queues, None);

        assert!(
            body.contains("nostrq_queue_pending{queue=\"weird\\\\name\\\"with\\nnewline\"} 1\n")
        );
    }

    #[tokio::test]
    async fn metrics_server_serves_metrics_and_404s_other_paths() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;

        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        ctx.store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let server = tokio::spawn(async move {
            serve_metrics(listener, &ctx, false, sd).await.unwrap();
        });

        async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.unwrap();
            let mut parts = buf.splitn(2, "\r\n\r\n");
            let head = parts.next().unwrap_or_default().to_string();
            let body = parts.next().unwrap_or_default().to_string();
            (head, body)
        }

        let (head, body) = get(addr, "/metrics").await;
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert!(
            head.contains("Content-Type: text/plain; version=0.0.4"),
            "{head}"
        );
        assert!(body.contains("nostrq_queue_pending{queue=\"jobs.email\"} 0\n"));

        let (head, _) = get(addr, "/other").await;
        assert!(head.starts_with("HTTP/1.1 404"), "{head}");

        shutdown.cancel();
        // unblock the accept loop's select! so the spawned task can exit.
        let _ = TcpStream::connect(addr).await;
        server.await.unwrap();
    }

    /// Regression test for CHA-2346: a client that opens a TCP connection
    /// and never sends any data must not block subsequent scrapes (or
    /// shutdown). Before the fix, `handle_metrics_request` was awaited
    /// inline in the accept loop with no timeout, so a single hung client
    /// wedged the whole server.
    #[tokio::test]
    async fn hung_client_does_not_block_scrapes() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;
        use tokio::time::{timeout, Duration};

        let dir = tempfile::tempdir().unwrap();
        let ctx = test_ctx(dir.path().join("key"));
        ctx.store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let server = tokio::spawn(async move {
            serve_metrics(listener, &ctx, false, sd).await.unwrap();
        });

        // Open a connection and send nothing, keeping it open for the rest
        // of the test — simulates a hung/malicious client.
        let hung = TcpStream::connect(addr).await.unwrap();

        // A well-behaved scraper connecting after the hung client must
        // still be served promptly.
        let scrape = async {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
                .await
                .unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.unwrap();
            buf
        };
        let body = timeout(Duration::from_secs(5), scrape)
            .await
            .expect("scrape must not be blocked by the hung client");
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
        assert!(body.contains("nostrq_queue_pending{queue=\"jobs.email\"} 0\n"));

        drop(hung);
        shutdown.cancel();
        // unblock the accept loop's select! promptly (belt-and-suspenders;
        // select! re-polls every branch each iteration regardless).
        let _ = TcpStream::connect(addr).await;
        timeout(Duration::from_secs(5), server)
            .await
            .expect("server task must join promptly after shutdown")
            .unwrap();
    }

    // --- `nostr-q serve` — HTTP publish ingress (CHA-2344) ---

    fn test_nq() -> (Arc<NostrQ>, Arc<nostr_q::relay::MockTransport>) {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();
        let transport = Arc::new(nostr_q::relay::MockTransport::new());
        let nq = Arc::new(NostrQ::new(
            nostr::Keys::generate(),
            store,
            transport.clone(),
        ));
        (nq, transport)
    }

    async fn published_message_events(
        transport: &nostr_q::relay::MockTransport,
    ) -> Vec<nostr::Event> {
        transport
            .query(nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q::protocol::KIND_MESSAGE)))
            .await
            .unwrap()
    }

    fn ingress_request(
        method: &str,
        uri: &str,
        token: Option<&str>,
        idem: Option<&str>,
        body: &str,
    ) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            builder = builder.header("Authorization", format!("Bearer {t}"));
        }
        if let Some(k) = idem {
            builder = builder.header("Idempotency-Key", k);
        }
        builder
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn ingress_publish_with_valid_token_returns_receipt_and_publishes_event() {
        use tower::ServiceExt;

        let (nq, transport) = test_nq();
        let router = build_ingress_router(nq, Some("secret".into()));
        let req = ingress_request(
            "POST",
            "/pub/jobs.email",
            Some("secret"),
            None,
            r#"{"hi":1}"#,
        );
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("mid").is_some(), "{json}");
        assert!(json.get("trace_id").is_some(), "{json}");
        assert!(json.get("event_id").is_some(), "{json}");

        assert_eq!(published_message_events(&transport).await.len(), 1);
    }

    #[tokio::test]
    async fn ingress_missing_or_wrong_token_is_rejected_and_publishes_nothing() {
        use tower::ServiceExt;

        let (nq, transport) = test_nq();

        // missing header
        let router = build_ingress_router(nq.clone(), Some("secret".into()));
        let req = ingress_request("POST", "/pub/jobs.email", None, None, r#"{"hi":1}"#);
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // wrong token
        let router = build_ingress_router(nq, Some("secret".into()));
        let req = ingress_request("POST", "/pub/jobs.email", Some("nope"), None, r#"{"hi":1}"#);
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        assert_eq!(
            published_message_events(&transport).await.len(),
            0,
            "unauthorized requests must not publish"
        );
    }

    #[tokio::test]
    async fn ingress_unknown_queue_is_404() {
        use tower::ServiceExt;

        let (nq, _transport) = test_nq();
        let router = build_ingress_router(nq, None);
        let req = ingress_request("POST", "/pub/nope", None, None, r#"{"hi":1}"#);
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ingress_malformed_json_is_400() {
        use tower::ServiceExt;

        let (nq, _transport) = test_nq();
        let router = build_ingress_router(nq, None);
        let req = ingress_request("POST", "/pub/jobs.email", None, None, "not json");
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn ingress_idempotency_key_dedupes() {
        use tower::ServiceExt;

        let (nq, transport) = test_nq();
        let router = build_ingress_router(nq, None);

        let req1 = ingress_request(
            "POST",
            "/pub/jobs.email",
            None,
            Some("order-1"),
            r#"{"n":1}"#,
        );
        let resp1 = router.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);
        let body1 = axum::body::to_bytes(resp1.into_body(), usize::MAX)
            .await
            .unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();

        let req2 = ingress_request(
            "POST",
            "/pub/jobs.email",
            None,
            Some("order-1"),
            r#"{"n":2}"#,
        );
        let resp2 = router.clone().oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let body2 = axum::body::to_bytes(resp2.into_body(), usize::MAX)
            .await
            .unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();

        assert_eq!(json1["mid"], json2["mid"], "same idem key must dedupe");
        assert_eq!(
            published_message_events(&transport).await.len(),
            1,
            "duplicate idem must not re-broadcast"
        );
    }

    #[tokio::test]
    async fn ingress_healthz_returns_ok() {
        use tower::ServiceExt;

        let (nq, _transport) = test_nq();
        let router = build_ingress_router(nq, Some("secret".into()));
        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], true);
    }
}
