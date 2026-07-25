<p align="center">
  <img src="docs/assets/nostr-q-banner.svg" alt="Nostr-Q - signed queues over Nostr relays" width="100%">
</p>

<p align="center">
  <a href="https://github.com/EricGrill/nostr-q/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/EricGrill/nostr-q/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-2f80ed.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-2021-f46623.svg">
  <img alt="Status" src="https://img.shields.io/badge/status-pre--1.0-8a63d2.svg">
</p>

<h3 align="center">Signed message queues, work queues, and pub/sub over Nostr relays.</h3>

<p align="center">
  Nostr-Q is a Rust SDK and CLI that lets producers, workers, and subscribers
  coordinate through signed Nostr events while keeping local SQLite state for
  claims, retries, traces, and dead letters.
</p>

<p align="center">
  <a href="#install"><strong>Install</strong></a>
  ·
  <a href="#quick-start"><strong>Quick Start</strong></a>
  ·
  <a href="docs/PROTOCOL.md"><strong>Protocol</strong></a>
  ·
  <a href="CONTRIBUTING.md"><strong>Contribute</strong></a>
</p>

---

## Why Nostr-Q

Most queue systems start with a broker. Nostr-Q starts with signed events and
relays you can run yourself.

| Capability | What it gives you |
| --- | --- |
| **Signed queue traffic** | Every publish, claim, ack, nack, heartbeat, and DLQ event is a signed Nostr event. |
| **No central queue broker** | Coordinate through one or more Nostr relays instead of adding another service dependency. |
| **Local operational state** | Each participant tracks relay config, queue state, retries, traces, and DLQ records in SQLite. |
| **CLI-first workflow** | Initialize keys, add relays, create queues, publish messages, run workers, inspect, trace, and retry from one binary. |
| **Embeddable Rust SDK** | The CLI is a thin layer over the same `nostr_q` primitives available to Rust applications. |

## Architecture

<p align="center">
  <img src="docs/assets/architecture.svg" alt="Nostr-Q architecture diagram" width="100%">
</p>

> **Local state is per-node.** Each Nostr-Q node keeps its own SQLite
> database reflecting only what *that node* has published, ingested, or
> observed — there is no shared or global store. A producer-only node's
> rows can stay `pending` forever even after another node's worker acks or
> dead-letters them, and `nostr-q inspect`/`dlq list` report that node's
> local view, not global truth. This is by design; see
> [docs/PROTOCOL.md](docs/PROTOCOL.md#local-state-is-per-node) for details
> and operational guidance.
>
> **One keypair per worker instance.** Claim-winner identity is decided by
> claimer pubkey. Two worker instances sharing the same
> `NOSTR_Q_PRIVATE_KEY` will both believe they won every claim and will
> **duplicate all processing**. Give each worker instance (or at least each
> machine) its own key with `nostr-q key generate`. See
> [docs/PROTOCOL.md](docs/PROTOCOL.md#one-keypair-per-worker-instance).

Nostr-Q has a small, layered workspace:

```text
crates/
  nostr-q-core/    Protocol types, event kinds, tags, IDs, and envelopes
  nostr-q-store/   SQLite state store
  nostr-q-relay/   Transport trait, mock transport, and nostr-sdk transport
  nostr-q/         SDK facade
  nostr-q-worker/  Worker runtime
  nostr-q-cli/     CLI package, installed as `nostr-q`
```

Read the wire protocol in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Install

The public command is `nostr-q`.

| Platform | Command | Status |
| --- | --- | --- |
| macOS | `brew install EricGrill/tap/nostr-q` | Available after first GitHub release and tap setup |
| Linux | `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.sh \| sh` | Available after first GitHub release |
| Windows | `powershell -ExecutionPolicy Bypass -c "irm https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.ps1 \| iex"` | Available after first GitHub release |
| Rust source | `cargo install --path crates/nostr-q-cli --locked` | Works from a local checkout |

`brew install nq` is intentionally not used. Homebrew Core already owns `nq`
for an unrelated command-line queue utility, so this project publishes the
unambiguous `nostr-q` binary.

See [docs/INSTALL.md](docs/INSTALL.md) for Cargo, Homebrew, shell installer,
PowerShell, Winget, and Scoop notes.

## Quickstart: nostr-q dev

The fastest way to see a working job flow — no Docker, no external relay, no
config to write by hand. `nostr-q dev` starts a minimal NIP-01 relay
*embedded in the CLI process itself*, provisions a disposable config/state/key
under a dev-labeled directory, registers the embedded relay, and creates two
example queues (`jobs.email`, a work queue, and `events.demo`, a pub/sub
topic):

```sh
nostr-q dev --with-sample
```

```text
nostr-q dev: environment ready

  relay:   ws://127.0.0.1:10547 (embedded, in-memory, dev-only)
  config:  /Users/you/.config/nostr-q/dev/config.toml
  state:   /Users/you/.config/nostr-q/dev/state.db
  queues:  jobs.email (work_queue), events.demo (pubsub)

Point other terminals at this environment:
  export NQ_CONFIG=/Users/you/.config/nostr-q/dev/config.toml

Try it (in another terminal, after exporting NQ_CONFIG above):
  nostr-q pub jobs.email '{"hello":"world"}'
  nostr-q worker jobs.email --exec 'cat'
  nostr-q sub events.demo

Ctrl-C to stop the embedded relay.
[dev] publishing a sample job to 'jobs.email'...
[dev] published mid=01... trace=01...
[dev] worker claimed mid=01... payload={"from":"nostr-q dev --with-sample","hello":"nostr-q"}
[dev] worker acked mid=01...
```

`--with-sample` runs an in-process sample worker and publishes one sample job
so you see the full publish -> claim -> ack flow within a couple of seconds,
without needing a second terminal. Leave it running and, from another
terminal, `export NQ_CONFIG` to the path printed above and drive the same
environment with the ordinary CLI (`pub`, `worker`, `sub`, `inspect`, ...).

The dev environment is disposable and isolated from your real config:

- `--addr <host:port>` picks the relay's bind address (default
  `127.0.0.1:10547`); if it's already taken, `nostr-q dev` falls back to an
  ephemeral port and reports the address it actually bound.
- `--dir <path>` puts a self-contained `config.toml` / `state.db` / `key`
  under that directory instead of the default dev-labeled location.
  `NQ_CONFIG`/`NQ_STATE` are honored the same way they are everywhere else in
  the CLI if set and `--dir` is omitted.
- Ctrl-C stops the embedded relay (and any sample worker) gracefully; nothing
  written by `nostr-q dev` touches your regular `nostr-q init` config or
  state — it's a separate, clearly-labeled `dev` path.

The embedded relay speaks enough of NIP-01 (`EVENT`/`OK`, `REQ`/`EOSE`,
`CLOSE`) to interoperate with any real Nostr client, including the same
`NostrTransport` (nostr-sdk) the rest of the CLI uses — it's not a special
test double, just a minimal, in-memory, single-process relay meant for
kicking the tires.

## Quick Start (external relay)

For a setup closer to production — a real, persistent relay you also use
outside this project — run a disposable local relay in Docker:

```sh
mkdir -p .local/nostr-rs-relay
docker run --rm -it \
  --name nostr-q-relay \
  -p 7000:8080 \
  --mount "src=$(pwd)/.local/nostr-rs-relay,target=/usr/src/app/db,type=bind" \
  scsibug/nostr-rs-relay:latest
```

In another terminal, create isolated local config and queues:

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

## Message Lifecycle

<p align="center">
  <img src="docs/assets/message-lifecycle.svg" alt="Nostr-Q work queue message lifecycle" width="100%">
</p>

Work queues are lease based:

1. A producer publishes a signed message event.
2. Workers ingest the event into local SQLite state.
3. Competing workers publish signed claim events.
4. The deterministic claim winner processes the message.
5. The worker signs an ack, nack, retry, or dead-letter event.
6. Operators can inspect depth and trace a message by trace ID or message ID.

## CLI Surface

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
nostr-q metrics --addr <host:port> [--with-relays]
nostr-q serve --addr <host:port> [--token <secret>]
nostr-q dev [--addr <host:port>] [--dir <path>] [--with-sample]
```

## Metrics

`nostr-q metrics` serves a Prometheus text-exposition endpoint at `GET /metrics`,
computed fresh from local queue state on every scrape:

```sh
nostr-q metrics --addr 127.0.0.1:9090
curl -s localhost:9090/metrics
```

For each queue it emits gauges: `nostrq_queue_pending`, `nostrq_queue_in_flight`,
`nostrq_queue_acked`, `nostrq_queue_dead`, `nostrq_queue_expired`, and
`nostrq_queue_oldest_pending_seconds` (0 when there's no pending message), each
labeled `queue="<name>"`.

Pass `--with-relays` to additionally probe configured relays on every scrape and
export `nostrq_relay_up` (1/0) and `nostrq_relay_latency_ms` (0 when down or
unknown) labeled `url="<relay-url>"`. This does a live network health check per
scrape, so it's opt-in.

### Health endpoints

The same server exposes two probe endpoints for orchestrators:

| Endpoint | Meaning | Codes |
|---|---|---|
| `GET /healthz` | **Liveness.** The process is up and still accepting connections. | always `200` |
| `GET /readyz` | **Readiness.** The store answers queries, and — with `--with-relays` — at least one relay is reachable. | `200` / `503` |

`/healthz` deliberately does not touch the store or relays: a liveness probe that
fails during a dependency outage gets a healthy process restarted for no reason.
Use `/readyz` to drain traffic instead.

`/readyz` reports *which* dependency is unhappy, so a failing probe is
actionable without digging through logs:

```console
$ curl -s localhost:9090/readyz          # healthy
ready
store=ok
relays=not_checked

$ curl -si localhost:9090/readyz | head -1   # relay set unreachable
HTTP/1.1 503 Service Unavailable
not ready
store=ok
relays=unreachable
detail=no configured relay answered
```

Any path other than `GET /metrics`, `GET /healthz` or `GET /readyz` returns `404`.

## HTTP Ingress

`nostr-q serve` runs an HTTP publish ingress so any language can publish a
message without linking the Rust SDK:

```sh
nostr-q serve --addr 127.0.0.1:8787 --token devsecret
curl -s -XPOST -H 'Authorization: Bearer devsecret' \
  -H 'Idempotency-Key: order-1' \
  -d '{"to":"a@b.c"}' \
  'localhost:8787/pub/jobs.email'
```

Routes:

| Route | Behavior |
| --- | --- |
| `POST /pub/<queue>` | Publishes the JSON request body to `<queue>`. Returns `200` with `{"mid","trace_id","event_id"}`. `404` for an unknown queue, `400` for malformed JSON, `413` over the 1 MiB body limit. |
| `GET /healthz` | `200 {"ok":true}`, unauthenticated. |

Optional `/pub/<queue>` inputs:

- `Idempotency-Key: <key>` header — dedupes repeat publishes (same receipt returned, no re-broadcast).
- `?delay=<seconds>` — delay delivery (maps to `not_before`).
- `?ttl=<seconds>` — expire the message after N seconds (maps to `expires_at`).

**Access control (required):** the ingress signs and publishes with the
node's private key, so it is access-controlled by default:

- Default bind is `127.0.0.1` (localhost only).
- `--token <secret>` (or env `NQ_INGRESS_TOKEN`) requires every `/pub/*`
  request to carry `Authorization: Bearer <secret>`; missing/wrong token is
  `401`.
- With no token configured, `nostr-q serve` **refuses to start** on any
  non-loopback `--addr` — an unauthenticated signing endpoint must never be
  exposed off localhost. A loopback bind with no token is allowed for local
  dev (logs a warning).

See [`examples/`](examples/) for Python and TypeScript publisher clients and
a matching `nostr-q worker --http` consumer.

## Guarantees

| Area | Current behavior |
| --- | --- |
| Work queues | At-least-once delivery with lease-bounded claims. Handlers should be idempotent. |
| Pub/sub | Best-effort fanout without claim or ack tracking. |
| Relays | Public relays are fine for demos; production workloads should use private or self-hosted relays. |
| Payloads | Plaintext Nostr events today. Do not put secrets in payloads until NIP-04/NIP-44 support lands. |
| Tests | Automated tests use `MockTransport`, so a live relay is not required. |

## Configuration

Default locations:

| Setting | Default |
| --- | --- |
| Config | `~/.config/nostr-q/config.toml` |
| SQLite state | `~/.local/share/nostr-q/state.db` |
| Project override | `./nostr-q.toml` |
| Environment overrides | `NOSTR_Q_CONFIG`, `NOSTR_Q_STATE`, `NOSTR_Q_PRIVATE_KEY` |

Example config:

```toml
state = "~/.local/share/nostr-q/state.db"
key_file = "~/.config/nostr-q/key"
```

Never commit private keys, local databases, or relay state.

## Rust SDK

```rust
use std::sync::Arc;
use nostr_q::{NostrQ, relay::NostrTransport, store::Store};

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

## Examples

[`examples/`](examples/) has short, self-contained polyglot clients that talk
to `nostr-q serve`'s HTTP ingress instead of linking the Rust SDK:

| File | What it does |
| --- | --- |
| [`examples/python/publish.py`](examples/python/publish.py) | Publish via stdlib `urllib` — no pip install required. |
| [`examples/python/worker_handler.py`](examples/python/worker_handler.py) | Stdlib `http.server` job consumer for `nostr-q worker --http`. |
| [`examples/typescript/publish.ts`](examples/typescript/publish.ts) | Publish via Node 18+ global `fetch`. |

See [`examples/README.md`](examples/README.md) for the full run-through.

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

## Release Packaging

Releases are configured with `cargo-dist`:

| Artifact | Output |
| --- | --- |
| macOS | Apple Silicon and Intel archives plus Homebrew formula |
| Linux | x64 and ARM64 archives plus shell installer |
| Windows | x64 archive plus PowerShell installer |
| GitHub | Release notes, checksums, and downloadable installers |

Release maintainers should follow [RELEASING.md](RELEASING.md).

## Roadmap

- `nostr-q dev` for a one-command local relay and config sandbox.
- NIP-04/NIP-44 payload encryption.
- Queue config get/set/show/delete commands.
- Standalone ack, nack, and retry commands.
- Prometheus or OpenTelemetry metrics.
- Additional state stores beyond SQLite.

## Contributing

Issues, bug reports, and pull requests are welcome. Start with
[CONTRIBUTING.md](CONTRIBUTING.md), and please run the development checks
before opening a PR.

Security reports should follow [SECURITY.md](SECURITY.md). General usage
questions belong in GitHub Discussions once the repository is public; see
[SUPPORT.md](SUPPORT.md).

## License

MIT. See [LICENSE](LICENSE).
