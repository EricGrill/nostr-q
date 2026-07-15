# Nostr-Q

[![CI](https://github.com/EricGrill/nostr-q/actions/workflows/ci.yml/badge.svg)](https://github.com/EricGrill/nostr-q/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Nostr-Q turns ordinary Nostr relays into signed message queues, work queues,
and pub/sub topics. It ships as a Rust SDK plus the `nostr-q` CLI, with local
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
- A `nostr-q` CLI for setup, relay management, queue operations, workers,
  inspection, tracing, and DLQ retry.

Read the protocol details in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Install

The public command is `nostr-q`.

### macOS

After the first GitHub release and Homebrew tap setup:

```sh
brew install EricGrill/tap/nostr-q
```

`brew install nq` is intentionally not used. Homebrew Core already owns `nq`
for an unrelated command-line queue utility, so Nostr-Q publishes the
unambiguous `nostr-q` binary.

### Linux

After the first GitHub release:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.sh | sh
```

### Windows

After the first GitHub release:

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.ps1 | iex"
```

Winget and Scoop manifests are good follow-up package targets once the first
stable release artifacts exist. See [docs/INSTALL.md](docs/INSTALL.md) for
the full install matrix.

### From Source

```sh
cargo install --path crates/nostr-q-cli --locked
nostr-q --help
```

Or run without installing:

```sh
cargo run -p nostr-q-cli -- --help
```

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

`nak serve` listens on `ws://localhost:10547`.

## Quick Start

This walkthrough isolates config and state under `/tmp` so it does not touch
your normal Nostr-Q config.

```sh
export NOSTR_Q_CONFIG=/tmp/nostrq/config.toml
export NOSTR_Q_STATE=/tmp/nostrq/state.db

nostr-q init
nostr-q key generate
nostr-q relay add ws://localhost:7000
nostr-q relay health

nostr-q queue create jobs.email --mode work_queue --delivery at_least_once
nostr-q queue create events.user.created --mode pubsub
```

Publish and consume a pub/sub event:

```sh
nostr-q sub events.user.created
```

In another terminal:

```sh
export NOSTR_Q_CONFIG=/tmp/nostrq/config.toml
export NOSTR_Q_STATE=/tmp/nostrq/state.db
nostr-q pub events.user.created '{"id":7}'
```

Run a work-queue handler:

```sh
nostr-q pub jobs.email '{"to":"user@example.com","template":"welcome"}'
nostr-q worker jobs.email --exec 'cat > /tmp/nostrq/handled.json'
```

Inspect what happened:

```sh
nostr-q inspect jobs.email
nostr-q trace <trace-id>
nostr-q dlq list
```

## CLI

```text
nostr-q init
nostr-q key generate|show
nostr-q relay add|list|remove|health
nostr-q queue create|list
nostr-q pub <queue-or-topic> <json>
nostr-q sub <topic>
nostr-q worker <queue> --exec <cmd>
nostr-q worker <queue> --http <url>
nostr-q inspect <queue>
nostr-q trace <trace-id-or-message-id>
nostr-q dlq list|retry
```

## Guarantees

- Work queues are **at-least-once**, not exactly-once. Handlers should be
  idempotent.
- Pub/sub topics are best-effort and do not use claim/ack tracking.
- Public relays are fine for demos, but production queue workloads should use
  private or self-hosted relays with retention and availability you control.
- Message payloads are plaintext Nostr events today. Do not put secrets in
  payloads until NIP-04/NIP-44 encryption support lands.

## Configuration

Default locations:

- Config: `~/.config/nostr-q/config.toml`
- Local SQLite state: `~/.local/share/nostr-q/state.db`
- Project override: `./nostr-q.toml`
- Environment overrides: `NOSTR_Q_CONFIG`, `NOSTR_Q_STATE`,
  `NOSTR_Q_PRIVATE_KEY`

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
let keys = nostr::Keys::parse(&std::env::var("NOSTR_Q_PRIVATE_KEY")?)?;
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
  nostr-q-cli/     CLI package, installed as `nostr-q`
docs/
  INSTALL.md       Install and package-manager notes
  PROTOCOL.md      Event kinds, tags, envelopes, and delivery semantics
```

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Automated tests use the in-memory `MockTransport`, so a live relay is not
required for `cargo test --workspace`.

## Contributing

Issues, bug reports, and pull requests are welcome. Start with
[CONTRIBUTING.md](CONTRIBUTING.md), and please run the development checks
before opening a PR.

Security reports should follow [SECURITY.md](SECURITY.md). General usage
questions belong in GitHub Discussions once the repository is public; see
[SUPPORT.md](SUPPORT.md).

## License

MIT. See [LICENSE](LICENSE).
