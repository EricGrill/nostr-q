# Nostr-Q

Nostr-Q turns ordinary Nostr relays into signed message queues, work queues,
and pub/sub topics. It ships as a Rust SDK plus the `nq` CLI, with local
SQLite state for claims, retries, traces, and dead-letter records.

It is useful when you want lightweight queue coordination without running a
central broker: producers, workers, and subscribers exchange signed Nostr
events through relays you control.

## Status

Nostr-Q is pre-1.0 and moving fast. The current MVP includes:

- Signed queue protocol events for publish, claim, ack, nack, DLQ, and
  heartbeat.
- Work queues with at-least-once delivery, lease-bounded claims, retry
  backoff, and graceful shutdown.
- Pub/sub topics for best-effort fanout.
- A SQLite local store for relays, queues, message state, traces, and DLQ
  records.
- A `nostr_q` SDK facade for embedding queue behavior in Rust apps.
- An `nq` CLI for setup, relay management, queue operations, workers,
  inspection, tracing, and DLQ retry.

Read the protocol details in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Install

The public command is `nq`, provided by the `nq` package
(`crates/nostr-q-cli`).

### From Source

```sh
cargo install --path crates/nostr-q-cli
nq --help
```

Or run without installing:

```sh
cargo run -p nq -- --help
```

There is no published release yet (no Homebrew tap, no installer script, no
CI badge) — build from source for now.

## Local Development Relay

For local development, run a disposable relay in Docker:

```sh
mkdir -p .local/nostr-rs-relay
docker run --rm -it \
  --name nostr-q-relay \
  -p 7000:8080 \
  --mount "src=$(pwd)/.local/nostr-rs-relay,target=/usr/src/app/db,type=bind" \
  scsibug/nostr-rs-relay:latest
```

Then use `ws://localhost:7000` in the quickstart below.

If you prefer a tiny native relay for demos, [`nak`](https://github.com/fiatjaf/nak)
also works:

```sh
brew install nak
nak serve
```

`nak serve` listens on `ws://localhost:10547` by default.

## Quick Start

This walkthrough isolates config and state under `/tmp` so it does not touch
your normal Nostr-Q config. It assumes a relay is already running (see
above) at `ws://localhost:10547`.

```sh
export NQ_CONFIG=/tmp/nqdemo/config.toml
export NQ_STATE=/tmp/nqdemo/state.db

nq init
nq key generate
nq relay add ws://localhost:10547
nq relay health
```

```sh
nq queue create jobs.email --mode work_queue --delivery at_least_once
```

Publish a message and run a worker to process it:

```sh
nq pub jobs.email '{"to":"user@example.com","template":"welcome"}'
nq worker jobs.email --exec 'cat > /dev/null'
```

The worker claims the message, runs the handler (here, a no-op that just
drains stdin), and acks it. Stop the worker with `Ctrl-C` once it has
processed the message — it shuts down gracefully.

Inspect what happened:

```sh
nq inspect jobs.email
nq trace <trace-id>
nq dlq list
```

`nq inspect jobs.email` should show `acked: 1` once the worker has
processed the message above.

## CLI

```text
nq init
nq key generate|show
nq relay add|list|remove|health
nq queue create|list
nq pub <queue-or-topic> <json>
nq sub <topic>
nq worker <queue> --exec <cmd>
nq worker <queue> --http <url>
nq inspect <queue>
nq trace <trace-id-or-message-id>
nq dlq list|retry
```

## Guarantees

- Work queues are **at-least-once**, not exactly-once. Handlers should be
  idempotent.
- Pub/sub topics are best-effort and do not use claim/ack tracking.
- Public relays are fine for demos, but production queue workloads should use
  private or self-hosted relays with retention and availability you control.
- Message payloads are plaintext Nostr events today. Do not put secrets in
  payloads until NIP-04/NIP-44 encryption support lands.
- Losing a claim race is not free of duplicates: relay partitions,
  settle-window races, and lease expiry mid-handler can all still cause a
  message to be processed more than once. See
  [docs/PROTOCOL.md](docs/PROTOCOL.md) for the full list of causes.

## Configuration

Default locations:

- Config: `~/.config/nostr-q/config.toml`
- Local SQLite state: `~/.local/share/nostr-q/state.db`
- Project override: `./nostr-q.toml`
- Environment overrides: `NQ_CONFIG`, `NQ_STATE`, `NQ_PRIVATE_KEY`

Example config:

```toml
state = "~/.local/share/nostr-q/state.db"
key_file = "~/.config/nostr-q/key"
```

Never commit private keys, local databases, or relay state.

## Rust SDK

```rust
use std::sync::Arc;
use nostr_q::{NostrQ, relay::NostrTransport, store_crate::Store};

let store = Arc::new(Store::open("state.db".as_ref())?);
let keys = nostr::Keys::parse(&std::env::var("NQ_PRIVATE_KEY")?)?;
let transport = Arc::new(
    NostrTransport::connect(keys.clone(), &["wss://relay.example.com".into()]).await?,
);

let queue = NostrQ::new(keys, store, transport);
queue
    .publish("jobs.email", serde_json::json!({"to": "a@b.c"}), None)
    .await?;
```

## Repository Layout

```text
crates/
  nostr-q-core/    Protocol types, event kinds, tags, IDs, and envelopes
  nostr-q-store/   SQLite state store
  nostr-q-relay/   Transport trait, mock transport, and nostr-sdk transport
  nostr-q/         SDK facade
  nostr-q-worker/  Worker runtime
  nostr-q-cli/     CLI package (crate name `nq`), installed as `nq`
docs/
  PROTOCOL.md      Event kinds, tags, envelopes, and delivery semantics
```

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Automated tests use the in-memory `MockTransport`, so a live relay is not
required for `cargo test --workspace`.

## License

MIT. See [LICENSE](LICENSE).
