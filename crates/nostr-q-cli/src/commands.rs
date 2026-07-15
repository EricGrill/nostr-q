use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nostr_q::queue::{Delivery, QueueConfig, QueueMode};
use nostr_q::relay::{NostrTransport, Transport};
use nostr_q::store::Store;
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

pub async fn publish(
    ctx: &Ctx,
    queue: &str,
    payload: Option<String>,
    idem: Option<String>,
) -> Result<()> {
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
        let err = publish(&ctx, "q", Some("not json".into()), None)
            .await
            .expect_err("non-JSON payload must be rejected before connecting to any relay");
        assert!(err.to_string().contains("JSON"), "{err}");
    }

    // The exact `--json` wire format for init/key/relay/queue is verified
    // against the real compiled binary in
    // `crates/nostr-q-cli/tests/json_output.rs` (an integration test, since
    // `CARGO_BIN_EXE_nostr-q` is only available to targets other than the
    // bin's own unit tests).
}
