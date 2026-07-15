# Nostr-Q

Nostr-Q is an experimental Rust toolkit for message queue, work queue, and
pub/sub workflows over Nostr relays. The goal is to make Nostr usable where a
small team might otherwise reach for RabbitMQ, Redis Streams, NATS, SQS, or a
lightweight internal event bus.

## Status

This repository is in early MVP development.

- Implemented today: workspace scaffold, core Nostr-Q event protocol helpers,
  SQLite state store, relay transport abstraction, `nostr-sdk` transport, mock
  transport, SDK-level tests for publish, subscribe, claim, ack, nack, retry,
  and DLQ behavior, plus the initial `nq init` and `nq key` commands.
- In progress: relay, queue, publish, subscribe, inspect, trace, DLQ, and worker
  commands in the `nq` CLI and worker runtime.
- Deferred: `nq dev`, TUI, encryption implementation, request/reply RPC,
  production relay scoring, and non-SQLite state adapters.

Nostr itself is a protocol, not something Nostr-Q installs as a daemon. For
local development, run a compatible Nostr relay and point Nostr-Q at it. The
recommended Docker path below uses
[`nostr-rs-relay`](https://github.com/scsibug/nostr-rs-relay), a Rust relay that
persists events with SQLite.

## Requirements

- Rust toolchain with Cargo
- Docker Engine or Docker Desktop for a local relay
- Optional: `jq` for inspecting relay metadata JSON

This workspace uses Rust 2021 and currently pins `nostr` / `nostr-sdk` to
`0.39`.

## Quick Start

Clone the repository and verify the Rust workspace:

```sh
git clone https://github.com/<owner>/nostr-q.git
cd nostr-q

cargo check --workspace
cargo test --workspace
```

Inspect the current CLI surface:

```sh
cargo run -p nq -- --help
```

The current executable commands are:

```sh
cargo run -p nq -- init
cargo run -p nq -- key generate
cargo run -p nq -- key show
```

The remaining operational `nq` commands listed later in this README are the
intended CLI contract for the MVP as the CLI implementation lands.

## Local Nostr Relay With Docker

Start a local relay on host port `7000`:

```sh
mkdir -p .local/nostr-rs-relay

docker run --rm -it \
  --name nostr-q-relay \
  -p 7000:8080 \
  --mount "src=$(pwd)/.local/nostr-rs-relay,target=/usr/src/app/db,type=bind" \
  scsibug/nostr-rs-relay:latest
```

The local relay URL is:

```text
ws://127.0.0.1:7000
```

In another terminal, check the relay information document:

```sh
curl -s \
  -H 'Accept: application/nostr+json' \
  http://127.0.0.1:7000 | jq
```

Stop the relay with `Ctrl-C`. Because the command uses `--rm`, Docker removes
the container after it exits. Relay SQLite data remains in
`.local/nostr-rs-relay/`.

To reset the local relay state:

```sh
rm -rf .local/nostr-rs-relay
```

## Local Relay Without Docker

If you prefer not to use Docker, build and run `nostr-rs-relay` directly:

```sh
git clone https://github.com/scsibug/nostr-rs-relay.git
cd nostr-rs-relay
cargo run --release
```

By default, that relay listens on port `8080`, so the local URL is:

```text
ws://127.0.0.1:8080
```

Any NIP-01 compatible local relay should work for development. Automated tests
in this repo use `MockTransport`, so no live relay is required to run the test
suite.

## Intended `nq` Local Workflow

The `nq init` and `nq key` commands are implemented. The rest describe the MVP
CLI target and should become executable as the CLI crate is implemented:

```sh
nq init
nq key generate
nq relay add ws://127.0.0.1:7000
nq relay health

nq queue create jobs.email \
  --mode work_queue \
  --delivery at_least_once

nq queue create events.user.created \
  --mode pubsub \
  --delivery best_effort

nq pub jobs.email '{"to":"user@example.com","template":"welcome"}'
nq worker jobs.email --exec './send-email.sh'
nq sub events.user.created
nq inspect jobs.email
nq trace <trace-id>
nq dlq list
```

Nostr-Q work queues are at-least-once by default. Duplicate processing is
possible, so handlers should be idempotent and producers should use idempotency
keys when the CLI and SDK expose that path.

## Configuration Targets

The planned default locations are:

- Config: `~/.config/nostr-q/config.toml`
- Local SQLite state: `~/.local/share/nostr-q/state.db`
- Project override: `./nostr-q.toml`
- Environment override prefix: `NQ_`
- Private key input: `NQ_PRIVATE_KEY` or a key file with restrictive
  permissions

Example target config:

```toml
[profile.default]
state = "sqlite://~/.local/share/nostr-q/state.db"
keys = "env:NQ_PRIVATE_KEY"

[[relays]]
url = "ws://127.0.0.1:7000"
role = ["publish", "subscribe"]

[queues."jobs.email"]
mode = "work_queue"
delivery = "at_least_once"
encryption = "none"
max_attempts = 5
lease_seconds = 60

[queues."events.user.created"]
mode = "pubsub"
delivery = "best_effort"
encryption = "none"
```

Never commit private keys or generated local state.

## Workspace Layout

```text
crates/
  nostr-q-core/    Protocol types, event kinds, tags, IDs, and envelopes
  nostr-q-store/   SQLite state store for queues, relays, messages, DLQ, traces
  nostr-q-relay/   Transport trait, mock transport, and nostr-sdk transport
  nostr-q/         SDK facade for publish, subscribe, claim, ack, nack, DLQ
  nostr-q-worker/  Worker runtime target crate
  nostr-q-cli/     `nq` CLI target crate
```

Supporting docs:

- [`nostr-q.srs.md`](nostr-q.srs.md): product requirements and roadmap
- [`docs/superpowers/plans/2026-07-15-nostr-q-mvp.md`](docs/superpowers/plans/2026-07-15-nostr-q-mvp.md):
  MVP implementation plan

## Development Checks

Run these before opening a pull request:

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

If a check fails because the CLI or worker runtime is still in progress, keep the
failure in the pull request notes and link the issue or task that will close the
gap.

## Production Notes

Use private or self-hosted relays for production queue workloads. Public relays
are useful for experiments and low-criticality public event streams, but they
should not be the only transport for durable work queues because retention,
rate limits, moderation policy, and availability are outside your control.

Nostr-Q v1 messages are signed plaintext by default. Encryption modes are
modeled in queue config but not implemented yet.

## License

MIT
