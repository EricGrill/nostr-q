# Nostr-Q MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Nostr-Q MVP (SRS §21.1): a Rust workspace providing a message-queue SDK over Nostr relays plus the `nq` CLI with init/key/relay/queue/pub/sub/worker/inspect/trace/dlq commands, backed by local SQLite state.

**Architecture:** Layered Cargo workspace — `nostr-q-core` (pure types + Nostr protocol mapping), `nostr-q-store` (SQLite state), `nostr-q-relay` (a `Transport` trait with a real nostr-sdk implementation and an in-memory mock for tests), `nostr-q` (the SDK facade with publish/subscribe/claim/ack/nack/DLQ engine), `nostr-q-worker` (worker runtime with `--exec`/`--http` handlers), and `nostr-q-cli` (the `nq` binary). Everything except the one nostr-sdk transport file is testable offline against the mock transport and temp SQLite files.

**Tech Stack:** Rust 2021, tokio, nostr/nostr-sdk 0.39, rusqlite (bundled SQLite), clap 4 (derive), serde/serde_json/toml, ulid, chrono, reqwest, wiremock (tests).

## Global Constraints (from SRS)

- CLI command is `nq`. Scriptable by default; human-readable output by default; `--json` available for automation.
- Work queues default to `at_least_once` delivery; pub/sub topics default to `best_effort`. True exactly-once is explicitly NOT guaranteed — duplicates are possible and must be documented.
- Local state lives in SQLite by default. Default state path: `~/.local/share/nostr-q/state.db`.
- Config file default path: `~/.config/nostr-q/config.toml`; project-local override `./nostr-q.toml`; environment override prefix `NQ_`.
- v1 messages are signed plaintext (`encryption = "none"`). Encryption is config-modeled but not implemented in MVP.
- Never print private keys. Key material is read from `NQ_PRIVATE_KEY` env or a `0600` key file.
- Payload content is a standardized JSON envelope: `{"version":"0.1","content_type":"application/json","body":{},"headers":{},"created_at":"RFC3339"}`.
- Queue config example values (SRS §19): `max_attempts = 5`, `lease_seconds = 60` are the defaults.
- Workers support: configurable concurrency, lease timeout, heartbeat interval, graceful shutdown, max attempts, exponential retry backoff, DLQ policy, idempotency key exposure, structured logs.
- The SDK exposes the same primitives the CLI uses; applications can embed Nostr-Q without shelling out to `nq`.

## Decisions Resolving SRS Open Questions (§23/§24)

These are locked for MVP; each is revisable post-MVP without breaking the plan:

1. **Rust Nostr library:** `nostr` + `nostr-sdk` (rust-nostr project), pinned to `0.39`. All nostr-sdk usage is isolated in one file (`nostr_transport.rs`) behind our own `Transport` trait, so a future version bump touches one file. If a pinned-version API differs from the code shown in Task 9, consult `https://docs.rs/nostr-sdk/0.39` and adapt only that file — the trait contract must not change.
2. **Event kinds (contiguous block, base 4620, regular events; ephemeral for heartbeat; addressable reserved for config):**
   - `4620` message published, `4621` claim, `4622` ack, `4623` nack, `4624` dead-letter, `24620` consumer heartbeat (ephemeral), `34620` queue config snapshot (reserved, not published in MVP).
3. **Envelope:** standardized envelope (SRS-recommended), stored as the Nostr event `content`.
4. **Queue name filtering:** queue/topic name is duplicated into the single-letter `t` tag (relay-indexed, filterable via `#t`) AND the `q` tag (SRS-required). Lifecycle events reference the message event via the `e` tag (relay-indexed).
5. **Claims/acks representation:** BOTH Nostr events (source of truth for cross-node coordination) and local SQLite rows (fast local queries). Claim conflict resolution: after publishing a claim, wait a settle window (750 ms), fetch all claims for the message, and the winner is the unexpired claim with the lowest `(created_at, event_id_hex)`. Losers back off. Duplicates remain possible (at-least-once).
6. **Queue config storage:** local SQLite only for MVP. Kind 34620 is reserved for future relay publication.
7. **Repository/naming:** this repository (`/Users/eric/code/nostr-q`), workspace crates named `nostr-q-*` per SRS §20.1 (with `nostr-q-relay` instead of `nostr-q-nostr` for clarity), SDK facade crate named `nostr-q`, CLI binary named `nq`.
8. **Local dev relay:** MVP does not ship `nq dev` (not in SRS §14.3 MVP list). Docs recommend `nak serve` for local testing. All automated tests use `MockTransport` — no relay needed.
9. **Deferred to post-MVP:** request/reply RPC, routing keys/exchanges, attachments/blobs (JSON only), relay scoring, encryption implementation, signed admin events (DLQ retry is local-confirmation only), `nq tui`.
10. **IDs:** Nostr-Q message ids (`mid`) and trace ids are ULIDs (matches SRS example `01JABCDEF123456789`).

## File Structure

```
nostr-q/
├── Cargo.toml                          # workspace root
├── .gitignore
├── README.md                           # Task 19
├── docs/PROTOCOL.md                    # Task 19 — the protocol profile mini-spec
├── crates/
│   ├── nostr-q-core/                   # pure types + protocol mapping (no I/O)
│   │   └── src/{lib.rs, ids.rs, envelope.rs, queue.rs, protocol.rs}
│   ├── nostr-q-store/                  # SQLite state
│   │   └── src/{lib.rs, store.rs}
│   ├── nostr-q-relay/                  # Transport trait + impls
│   │   └── src/{lib.rs, transport.rs, mock.rs, nostr_transport.rs}
│   ├── nostr-q/                        # SDK facade: NostrQ engine (publish/subscribe/claim/ack/nack)
│   │   └── src/lib.rs
│   ├── nostr-q-worker/                 # worker runtime + exec/http handlers
│   │   └── src/{lib.rs, handlers.rs}
│   └── nostr-q-cli/                    # the `nq` binary
│       └── src/{main.rs, config.rs, commands.rs}
```

Dependency direction: `cli → worker → nostr-q → {core, store, relay}`; `store → core`; `relay → core`. `core` depends only on `nostr` (types), serde, ulid, chrono.

---

### Task 1: Workspace Scaffold

**Files:**
- Create: `Cargo.toml`, `.gitignore`, and six crates via `cargo new`

**Interfaces:**
- Produces: a compiling workspace with crates `nostr-q-core`, `nostr-q-store`, `nostr-q-relay`, `nostr-q`, `nostr-q-worker`, `nostr-q-cli` (bin `nq`) and shared `[workspace.dependencies]`.

- [ ] **Step 1: Initialize git and crates**

```bash
cd /Users/eric/code/nostr-q
git init
git add nostr-q.srs.md docs/superpowers/plans/2026-07-15-nostr-q-mvp.md
git commit -m "docs: add SRS and implementation plan"
cargo new --lib crates/nostr-q-core
cargo new --lib crates/nostr-q-store
cargo new --lib crates/nostr-q-relay
cargo new --lib crates/nostr-q
cargo new --lib crates/nostr-q-worker
cargo new crates/nostr-q-cli --name nq
```

- [ ] **Step 2: Write root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "crates/nostr-q-core",
    "crates/nostr-q-store",
    "crates/nostr-q-relay",
    "crates/nostr-q",
    "crates/nostr-q-worker",
    "crates/nostr-q-cli",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "MIT"

[workspace.dependencies]
anyhow = "1"
thiserror = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
tokio-util = "0.7"
async-trait = "0.1"
ulid = "1"
chrono = { version = "0.4", features = ["serde"] }
nostr = "0.39"
nostr-sdk = "0.39"
rusqlite = { version = "0.31", features = ["bundled"] }
clap = { version = "4", features = ["derive"] }
reqwest = { version = "0.12", features = ["json"] }
toml = "0.8"
dirs = "5"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tempfile = "3"
wiremock = "0.6"
nostr-q-core = { path = "crates/nostr-q-core" }
nostr-q-store = { path = "crates/nostr-q-store" }
nostr-q-relay = { path = "crates/nostr-q-relay" }
nostr-q = { path = "crates/nostr-q" }
nostr-q-worker = { path = "crates/nostr-q-worker" }
```

And `.gitignore`:

```
/target
*.db
```

- [ ] **Step 3: Verify workspace compiles**

Run: `cargo check --workspace`
Expected: `Finished` with no errors (each crate is still the `cargo new` stub).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "chore: scaffold cargo workspace with six crates"
```

---

### Task 2: Core — IDs and Envelope

**Files:**
- Create: `crates/nostr-q-core/src/ids.rs`, `crates/nostr-q-core/src/envelope.rs`
- Modify: `crates/nostr-q-core/src/lib.rs`, `crates/nostr-q-core/Cargo.toml`

**Interfaces:**
- Produces: `nostr_q_core::ids::{new_mid() -> String, new_trace_id() -> String}`; `nostr_q_core::envelope::Envelope { version: String, content_type: String, body: serde_json::Value, headers: BTreeMap<String,String>, created_at: chrono::DateTime<Utc> }` with `Envelope::new(body: serde_json::Value) -> Envelope`, `to_json(&self) -> Result<String>`, `from_json(&str) -> Result<Envelope>`.

- [ ] **Step 1: Add dependencies to `crates/nostr-q-core/Cargo.toml`**

```toml
[package]
name = "nostr-q-core"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
ulid.workspace = true
chrono.workspace = true
nostr.workspace = true
```

- [ ] **Step 2: Write the failing tests**

In `crates/nostr-q-core/src/envelope.rs` (tests at bottom of file):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelope_defaults() {
        let env = Envelope::new(json!({"to": "user@example.com"}));
        assert_eq!(env.version, "0.1");
        assert_eq!(env.content_type, "application/json");
        assert!(env.headers.is_empty());
    }

    #[test]
    fn envelope_json_roundtrip() {
        let env = Envelope::new(json!({"n": 1}));
        let parsed = Envelope::from_json(&env.to_json().unwrap()).unwrap();
        assert_eq!(env, parsed);
    }

    #[test]
    fn ids_are_unique_ulids() {
        let a = crate::ids::new_mid();
        let b = crate::ids::new_mid();
        assert_ne!(a, b);
        assert_eq!(a.len(), 26); // ULID canonical length
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p nostr-q-core`
Expected: compile error — `Envelope` not defined.

- [ ] **Step 4: Implement**

`crates/nostr-q-core/src/ids.rs`:

```rust
use ulid::Ulid;

/// Nostr-Q message id (mid). ULID: sortable, 26 chars.
pub fn new_mid() -> String {
    Ulid::new().to_string()
}

/// Trace/correlation id. ULID.
pub fn new_trace_id() -> String {
    Ulid::new().to_string()
}
```

`crates/nostr-q-core/src/envelope.rs` (above the tests):

```rust
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Standardized Nostr-Q payload envelope (SRS §11.4). This struct is the
/// Nostr event `content`, serialized as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: String,
    pub content_type: String,
    pub body: serde_json::Value,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl Envelope {
    pub fn new(body: serde_json::Value) -> Self {
        Self {
            version: "0.1".to_string(),
            content_type: "application/json".to_string(),
            body,
            headers: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}
```

`crates/nostr-q-core/src/lib.rs`:

```rust
pub mod envelope;
pub mod ids;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p nostr-q-core`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(core): ULID ids and standardized JSON envelope"
```

---

### Task 3: Core — Queue Configuration Model

**Files:**
- Create: `crates/nostr-q-core/src/queue.rs`
- Modify: `crates/nostr-q-core/src/lib.rs`

**Interfaces:**
- Produces: `nostr_q_core::queue::{QueueMode, Delivery, Encryption, QueueConfig}`.
  - `QueueMode::{WorkQueue, Pubsub}` with `as_str() -> &'static str` (`"work_queue"`/`"pubsub"`) and `FromStr`.
  - `Delivery::{BestEffort, AtMostOnce, AtLeastOnce}` with `as_str()` (`"best_effort"`/`"at_most_once"`/`"at_least_once"`) and `FromStr`.
  - `Encryption::{None, Nip04, Nip44}` with `as_str()` (`"none"`/`"nip04"`/`"nip44"`), `FromStr`, `Default = None`.
  - `QueueConfig { name: String, mode: QueueMode, delivery: Delivery, encryption: Encryption, max_attempts: u32, lease_seconds: u64, retry_base_seconds: u64 }` with constructors `QueueConfig::work_queue(name: &str)` (at_least_once, max_attempts 5, lease 60, retry base 5) and `QueueConfig::pubsub(name: &str)` (best_effort).

- [ ] **Step 1: Write the failing tests** (bottom of `queue.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn work_queue_defaults() {
        let q = QueueConfig::work_queue("jobs.email");
        assert_eq!(q.mode, QueueMode::WorkQueue);
        assert_eq!(q.delivery, Delivery::AtLeastOnce);
        assert_eq!(q.encryption, Encryption::None);
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.lease_seconds, 60);
        assert_eq!(q.retry_base_seconds, 5);
    }

    #[test]
    fn pubsub_defaults() {
        let q = QueueConfig::pubsub("events.user.created");
        assert_eq!(q.mode, QueueMode::Pubsub);
        assert_eq!(q.delivery, Delivery::BestEffort);
    }

    #[test]
    fn parse_from_cli_strings() {
        assert_eq!(QueueMode::from_str("work_queue").unwrap(), QueueMode::WorkQueue);
        assert_eq!(Delivery::from_str("at_least_once").unwrap(), Delivery::AtLeastOnce);
        assert!(QueueMode::from_str("bogus").is_err());
        assert_eq!(QueueMode::WorkQueue.as_str(), "work_queue");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q-core queue`
Expected: compile error — module/types not defined.

- [ ] **Step 3: Implement** (`queue.rs` above tests; add `pub mod queue;` to `lib.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    WorkQueue,
    Pubsub,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    BestEffort,
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Encryption {
    #[default]
    None,
    Nip04,
    Nip44,
}

macro_rules! str_enum {
    ($ty:ty { $($variant:path => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(&self) -> &'static str {
                match self { $($variant => $s),+ }
            }
        }
        impl FromStr for $ty {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($s => Ok($variant),)+
                    other => Err(format!("invalid value '{other}' for {}", stringify!($ty))),
                }
            }
        }
    };
}

str_enum!(QueueMode { QueueMode::WorkQueue => "work_queue", QueueMode::Pubsub => "pubsub" });
str_enum!(Delivery {
    Delivery::BestEffort => "best_effort",
    Delivery::AtMostOnce => "at_most_once",
    Delivery::AtLeastOnce => "at_least_once",
});
str_enum!(Encryption { Encryption::None => "none", Encryption::Nip04 => "nip04", Encryption::Nip44 => "nip44" });

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub mode: QueueMode,
    pub delivery: Delivery,
    #[serde(default)]
    pub encryption: Encryption,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_lease_seconds")]
    pub lease_seconds: u64,
    #[serde(default = "default_retry_base_seconds")]
    pub retry_base_seconds: u64,
}

fn default_max_attempts() -> u32 { 5 }
fn default_lease_seconds() -> u64 { 60 }
fn default_retry_base_seconds() -> u64 { 5 }

impl QueueConfig {
    pub fn work_queue(name: &str) -> Self {
        Self {
            name: name.to_string(),
            mode: QueueMode::WorkQueue,
            delivery: Delivery::AtLeastOnce,
            encryption: Encryption::None,
            max_attempts: default_max_attempts(),
            lease_seconds: default_lease_seconds(),
            retry_base_seconds: default_retry_base_seconds(),
        }
    }

    pub fn pubsub(name: &str) -> Self {
        Self {
            name: name.to_string(),
            mode: QueueMode::Pubsub,
            delivery: Delivery::BestEffort,
            encryption: Encryption::None,
            max_attempts: default_max_attempts(),
            lease_seconds: default_lease_seconds(),
            retry_base_seconds: default_retry_base_seconds(),
        }
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q-core`
Expected: all pass (previous 3 + new 3).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): queue config model with modes, delivery, encryption"
```

---

### Task 4: Core — Protocol Profile (Event Build/Parse)

**Files:**
- Create: `crates/nostr-q-core/src/protocol.rs`
- Modify: `crates/nostr-q-core/src/lib.rs`

**Interfaces:**
- Consumes: `Envelope` (Task 2), `QueueMode` (Task 3), `nostr::{Keys, Event, EventId, EventBuilder, Kind, Tag, TagKind, PublicKey}`.
- Produces (all in `nostr_q_core::protocol`):
  - Kind constants: `KIND_MESSAGE: u16 = 4620`, `KIND_CLAIM: u16 = 4621`, `KIND_ACK: u16 = 4622`, `KIND_NACK: u16 = 4623`, `KIND_DLQ: u16 = 4624`, `KIND_HEARTBEAT: u16 = 24620`, `KIND_QUEUE_CONFIG: u16 = 34620`.
  - `NqMessage { mid: String, queue: String, trace_id: String, attempt: u32, idem: Option<String>, envelope: Envelope }`
  - `build_message_event(keys: &Keys, mode: QueueMode, msg: &NqMessage) -> Result<Event, ProtocolError>`
  - `parse_message_event(event: &Event) -> Result<NqMessage, ProtocolError>`
  - `build_claim_event(keys, message_event_id: EventId, queue: &str, mid: &str, trace_id: &str, lease_expires_at: i64) -> Result<Event, ProtocolError>`
  - `build_ack_event(keys, message_event_id, queue, mid, trace_id) -> Result<Event, ProtocolError>`
  - `build_nack_event(keys, message_event_id, queue, mid, trace_id, attempt: u32, reason: &str) -> Result<Event, ProtocolError>`
  - `build_dlq_event(keys, message_event_id, queue, mid, trace_id, reason: &str) -> Result<Event, ProtocolError>`
  - `build_heartbeat_event(keys, queue: &str) -> Result<Event, ProtocolError>`
  - `ClaimInfo { claimer: PublicKey, claim_event_id: EventId, created_at: i64, lease_expires_at: i64 }`, `parse_claim_event(event: &Event) -> Result<ClaimInfo, ProtocolError>`
  - `claim_winner(claims: &[ClaimInfo], now: i64) -> Option<&ClaimInfo>` — lowest `(created_at, event_id hex)` among claims with `lease_expires_at > now`.
  - `tag_value(event: &Event, name: &str) -> Option<String>` helper.
  - `ProtocolError` (thiserror) with variants `MissingTag(String)`, `BadPayload(String)`, `Signing(String)`.

- [ ] **Step 1: Write the failing tests** (bottom of `protocol.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::Envelope;
    use crate::queue::QueueMode;
    use nostr::Keys;
    use serde_json::json;

    fn sample_msg() -> NqMessage {
        NqMessage {
            mid: crate::ids::new_mid(),
            queue: "jobs.email".into(),
            trace_id: crate::ids::new_trace_id(),
            attempt: 0,
            idem: Some("order-42".into()),
            envelope: Envelope::new(json!({"to": "a@b.c"})),
        }
    }

    #[test]
    fn message_event_roundtrip() {
        let keys = Keys::generate();
        let msg = sample_msg();
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert_eq!(event.kind.as_u16(), KIND_MESSAGE);
        assert_eq!(tag_value(&event, "t").as_deref(), Some("jobs.email"));
        assert_eq!(tag_value(&event, "mode").as_deref(), Some("work_queue"));
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.mid, msg.mid);
        assert_eq!(parsed.queue, "jobs.email");
        assert_eq!(parsed.idem.as_deref(), Some("order-42"));
        assert_eq!(parsed.envelope.body, msg.envelope.body);
    }

    #[test]
    fn parse_rejects_missing_mid() {
        let keys = Keys::generate();
        // valid envelope content but no nostr-q tags at all
        let content = Envelope::new(json!({})).to_json().unwrap();
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_MESSAGE), content)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(parse_message_event(&event), Err(ProtocolError::MissingTag(_))));
    }

    #[test]
    fn claim_roundtrip_and_winner() {
        let keys_a = Keys::generate();
        let keys_b = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let a = build_claim_event(&keys_a, msg_id, "jobs.email", "m1", "t1", 2_000_000_000).unwrap();
        let b = build_claim_event(&keys_b, msg_id, "jobs.email", "m1", "t1", 2_000_000_000).unwrap();
        let ca = parse_claim_event(&a).unwrap();
        let cb = parse_claim_event(&b).unwrap();
        assert_eq!(ca.lease_expires_at, 2_000_000_000);
        let claims = vec![ca.clone(), cb.clone()];
        let winner = claim_winner(&claims, 0).unwrap();
        // deterministic: same-second claims break ties by event id hex
        let expect = if ca.claim_event_id.to_hex() <= cb.claim_event_id.to_hex() { &ca } else { &cb };
        assert_eq!(winner.claim_event_id, expect.claim_event_id);
        // expired claims never win
        assert!(claim_winner(&claims, 3_000_000_000).is_none());
    }

    #[test]
    fn lifecycle_events_carry_refs() {
        let keys = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let ack = build_ack_event(&keys, msg_id, "jobs.email", "m1", "t1").unwrap();
        assert_eq!(ack.kind.as_u16(), KIND_ACK);
        assert_eq!(tag_value(&ack, "mid").as_deref(), Some("m1"));
        assert_eq!(tag_value(&ack, "e"), Some(msg_id.to_hex()));
        let nack = build_nack_event(&keys, msg_id, "jobs.email", "m1", "t1", 3, "boom").unwrap();
        assert_eq!(tag_value(&nack, "attempt").as_deref(), Some("3"));
        assert_eq!(tag_value(&nack, "reason").as_deref(), Some("boom"));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q-core protocol`
Expected: compile error — types not defined.

- [ ] **Step 3: Implement** (`protocol.rs` above tests; add `pub mod protocol;` to `lib.rs`)

```rust
use nostr::{Event, EventBuilder, EventId, Keys, Kind, PublicKey, Tag, TagKind};
use thiserror::Error;

use crate::envelope::Envelope;
use crate::queue::QueueMode;

pub const KIND_MESSAGE: u16 = 4620;
pub const KIND_CLAIM: u16 = 4621;
pub const KIND_ACK: u16 = 4622;
pub const KIND_NACK: u16 = 4623;
pub const KIND_DLQ: u16 = 4624;
pub const KIND_HEARTBEAT: u16 = 24620;
pub const KIND_QUEUE_CONFIG: u16 = 34620; // reserved, unused in MVP

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("missing required tag '{0}'")]
    MissingTag(String),
    #[error("bad payload: {0}")]
    BadPayload(String),
    #[error("signing error: {0}")]
    Signing(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NqMessage {
    pub mid: String,
    pub queue: String,
    pub trace_id: String,
    pub attempt: u32,
    pub idem: Option<String>,
    pub envelope: Envelope,
}

fn custom_tag(name: &str, value: impl Into<String>) -> Tag {
    Tag::custom(TagKind::custom(name.to_string()), [value.into()])
}

/// First value of the first tag whose name matches. Works for both
/// single-letter ("t", "e") and custom multi-letter tags.
pub fn tag_value(event: &Event, name: &str) -> Option<String> {
    event.tags.iter().find_map(|t| {
        let s = t.as_slice();
        if s.first().map(String::as_str) == Some(name) {
            s.get(1).cloned()
        } else {
            None
        }
    })
}

fn require_tag(event: &Event, name: &str) -> Result<String, ProtocolError> {
    tag_value(event, name).ok_or_else(|| ProtocolError::MissingTag(name.to_string()))
}

fn sign(builder: EventBuilder, keys: &Keys) -> Result<Event, ProtocolError> {
    builder
        .sign_with_keys(keys)
        .map_err(|e| ProtocolError::Signing(e.to_string()))
}

pub fn build_message_event(
    keys: &Keys,
    mode: QueueMode,
    msg: &NqMessage,
) -> Result<Event, ProtocolError> {
    let content = msg
        .envelope
        .to_json()
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    let mut tags = vec![
        Tag::hashtag(&msg.queue),                 // "t": relay-indexed, filterable
        custom_tag("q", &msg.queue),              // SRS-required duplicate
        custom_tag("mode", mode.as_str()),
        custom_tag("mid", &msg.mid),
        custom_tag("trace", &msg.trace_id),
        custom_tag("attempt", msg.attempt.to_string()),
    ];
    if let Some(idem) = &msg.idem {
        tags.push(custom_tag("idem", idem));
    }
    sign(EventBuilder::new(Kind::Custom(KIND_MESSAGE), content).tags(tags), keys)
}

pub fn parse_message_event(event: &Event) -> Result<NqMessage, ProtocolError> {
    let envelope = Envelope::from_json(&event.content)
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    Ok(NqMessage {
        mid: require_tag(event, "mid")?,
        queue: require_tag(event, "t")?,
        trace_id: require_tag(event, "trace")?,
        attempt: tag_value(event, "attempt")
            .and_then(|a| a.parse().ok())
            .unwrap_or(0),
        idem: tag_value(event, "idem"),
        envelope,
    })
}

fn lifecycle_tags(message_event_id: EventId, queue: &str, mid: &str, trace_id: &str) -> Vec<Tag> {
    vec![
        Tag::event(message_event_id),
        Tag::hashtag(queue),
        custom_tag("mid", mid),
        custom_tag("trace", trace_id),
    ]
}

pub fn build_claim_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    lease_expires_at: i64,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("lease_exp", lease_expires_at.to_string()));
    sign(EventBuilder::new(Kind::Custom(KIND_CLAIM), "").tags(tags), keys)
}

pub fn build_ack_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
) -> Result<Event, ProtocolError> {
    let tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    sign(EventBuilder::new(Kind::Custom(KIND_ACK), "").tags(tags), keys)
}

pub fn build_nack_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    attempt: u32,
    reason: &str,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("attempt", attempt.to_string()));
    tags.push(custom_tag("reason", reason));
    sign(EventBuilder::new(Kind::Custom(KIND_NACK), "").tags(tags), keys)
}

pub fn build_dlq_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    reason: &str,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("reason", reason));
    sign(EventBuilder::new(Kind::Custom(KIND_DLQ), "").tags(tags), keys)
}

pub fn build_heartbeat_event(keys: &Keys, queue: &str) -> Result<Event, ProtocolError> {
    let tags = vec![Tag::hashtag(queue)];
    sign(EventBuilder::new(Kind::Custom(KIND_HEARTBEAT), "").tags(tags), keys)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimInfo {
    pub claimer: PublicKey,
    pub claim_event_id: EventId,
    pub created_at: i64,
    pub lease_expires_at: i64,
}

pub fn parse_claim_event(event: &Event) -> Result<ClaimInfo, ProtocolError> {
    let lease_expires_at = require_tag(event, "lease_exp")?
        .parse()
        .map_err(|_| ProtocolError::BadPayload("lease_exp not an integer".into()))?;
    Ok(ClaimInfo {
        claimer: event.pubkey,
        claim_event_id: event.id,
        created_at: event.created_at.as_u64() as i64,
        lease_expires_at,
    })
}

/// Deterministic claim conflict resolution: earliest created_at wins,
/// ties broken by event id hex. Expired claims are ignored.
pub fn claim_winner(claims: &[ClaimInfo], now: i64) -> Option<&ClaimInfo> {
    claims
        .iter()
        .filter(|c| c.lease_expires_at > now)
        .min_by_key(|c| (c.created_at, c.claim_event_id.to_hex()))
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q-core`
Expected: all pass. If `nostr` 0.39 API names differ (e.g., `Tag::custom` signature, `event.tags.iter()`, `kind.as_u16()`), fix against `https://docs.rs/nostr/0.39` — keep the public function signatures in this task's Interfaces block unchanged.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core): protocol profile - event kinds, tags, build/parse, claim resolution"
```

---

### Task 5: Store — Open + Schema Migration

**Files:**
- Create: `crates/nostr-q-store/src/store.rs`
- Modify: `crates/nostr-q-store/src/lib.rs`, `crates/nostr-q-store/Cargo.toml`

**Interfaces:**
- Consumes: nothing yet (schema only).
- Produces: `nostr_q_store::Store` with `Store::open(path: &std::path::Path) -> anyhow::Result<Store>` (creates parent dirs, runs migrations, WAL mode) and `Store::open_in_memory() -> anyhow::Result<Store>` (for tests). Internally: `Mutex<rusqlite::Connection>`. `Store` is `Send + Sync` and is shared as `Arc<Store>`.

- [ ] **Step 1: `crates/nostr-q-store/Cargo.toml`**

```toml
[package]
name = "nostr-q-store"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
anyhow.workspace = true
rusqlite.workspace = true
chrono.workspace = true
serde_json.workspace = true
nostr-q-core.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 2: Write the failing tests** (bottom of `store.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_schema_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.schema_version().unwrap(), 1);
        }
        // reopening an existing db must not fail or re-run migrations destructively
        let store = Store::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }

    #[test]
    fn in_memory_store_works() {
        let store = Store::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p nostr-q-store`
Expected: compile error — `Store` not defined.

- [ ] **Step 4: Implement** (`store.rs`; `lib.rs` gets `mod store; pub use store::*;`)

```rust
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
  name TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  delivery TEXT NOT NULL,
  encryption TEXT NOT NULL DEFAULT 'none',
  max_attempts INTEGER NOT NULL DEFAULT 5,
  lease_seconds INTEGER NOT NULL DEFAULT 60,
  retry_base_seconds INTEGER NOT NULL DEFAULT 5,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS relays (
  url TEXT PRIMARY KEY,
  added_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  mid TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  trace_id TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', -- pending | claimed | acked | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  idem_key TEXT,
  consumer TEXT,
  lease_expires_at INTEGER,
  visible_at INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idem
  ON messages(queue, idem_key) WHERE idem_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_queue_status ON messages(queue, status);
CREATE TABLE IF NOT EXISTS dlq (
  mid TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  reason TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  dead_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS lifecycle (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mid TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_trace ON lifecycle(trace_id);
CREATE INDEX IF NOT EXISTS idx_lifecycle_mid ON lifecycle(mid);
"#;

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening sqlite db {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::from_conn(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_conn(Connection::open_in_memory()?)
    }

    fn from_conn(conn: Connection) -> Result<Self> {
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version < 1 {
            conn.execute_batch(SCHEMA_V1)?;
            conn.pragma_update(None, "user_version", 1)?;
        }
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn schema_version(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row("PRAGMA user_version", [], |r| r.get(0))?)
    }

    pub(crate) fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test -p nostr-q-store`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(store): sqlite store with versioned schema migration"
```

---

### Task 6: Store — Queue and Relay CRUD

**Files:**
- Modify: `crates/nostr-q-store/src/store.rs`

**Interfaces:**
- Consumes: `QueueConfig`, `QueueMode`, `Delivery`, `Encryption` from `nostr_q_core::queue` (Task 3).
- Produces (methods on `Store`):
  - `upsert_queue(&self, q: &QueueConfig) -> Result<()>`
  - `get_queue(&self, name: &str) -> Result<Option<QueueConfig>>`
  - `list_queues(&self) -> Result<Vec<QueueConfig>>`
  - `add_relay(&self, url: &str) -> Result<()>` (idempotent)
  - `list_relays(&self) -> Result<Vec<String>>`
  - `remove_relay(&self, url: &str) -> Result<()>`

- [ ] **Step 1: Write the failing tests** (append to tests module in `store.rs`)

```rust
#[test]
fn queue_crud_roundtrip() {
    use nostr_q_core::queue::QueueConfig;
    let store = Store::open_in_memory().unwrap();
    let q = QueueConfig::work_queue("jobs.email");
    store.upsert_queue(&q).unwrap();
    assert_eq!(store.get_queue("jobs.email").unwrap().unwrap(), q);
    assert!(store.get_queue("nope").unwrap().is_none());
    // upsert overwrites
    let mut q2 = q.clone();
    q2.max_attempts = 9;
    store.upsert_queue(&q2).unwrap();
    assert_eq!(store.get_queue("jobs.email").unwrap().unwrap().max_attempts, 9);
    store.upsert_queue(&QueueConfig::pubsub("events.x")).unwrap();
    assert_eq!(store.list_queues().unwrap().len(), 2);
}

#[test]
fn relay_crud() {
    let store = Store::open_in_memory().unwrap();
    store.add_relay("wss://relay.example.com").unwrap();
    store.add_relay("wss://relay.example.com").unwrap(); // idempotent
    assert_eq!(store.list_relays().unwrap(), vec!["wss://relay.example.com".to_string()]);
    store.remove_relay("wss://relay.example.com").unwrap();
    assert!(store.list_relays().unwrap().is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q-store`
Expected: compile error — methods not defined.

- [ ] **Step 3: Implement** (append `impl Store` block in `store.rs`)

```rust
use std::str::FromStr;
use nostr_q_core::queue::{Delivery, Encryption, QueueConfig, QueueMode};

impl Store {
    pub fn upsert_queue(&self, q: &QueueConfig) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO queues (name, mode, delivery, encryption, max_attempts, lease_seconds, retry_base_seconds, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(name) DO UPDATE SET
               mode=excluded.mode, delivery=excluded.delivery, encryption=excluded.encryption,
               max_attempts=excluded.max_attempts, lease_seconds=excluded.lease_seconds,
               retry_base_seconds=excluded.retry_base_seconds",
            rusqlite::params![
                q.name, q.mode.as_str(), q.delivery.as_str(), q.encryption.as_str(),
                q.max_attempts, q.lease_seconds as i64, q.retry_base_seconds as i64, Self::now()
            ],
        )?;
        Ok(())
    }

    fn row_to_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueueConfig> {
        Ok(QueueConfig {
            name: row.get(0)?,
            mode: QueueMode::from_str(&row.get::<_, String>(1)?).unwrap_or(QueueMode::WorkQueue),
            delivery: Delivery::from_str(&row.get::<_, String>(2)?).unwrap_or(Delivery::AtLeastOnce),
            encryption: Encryption::from_str(&row.get::<_, String>(3)?).unwrap_or_default(),
            max_attempts: row.get(4)?,
            lease_seconds: row.get::<_, i64>(5)? as u64,
            retry_base_seconds: row.get::<_, i64>(6)? as u64,
        })
    }

    const QUEUE_COLS: &'static str =
        "name, mode, delivery, encryption, max_attempts, lease_seconds, retry_base_seconds";

    pub fn get_queue(&self, name: &str) -> Result<Option<QueueConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM queues WHERE name = ?1", Self::QUEUE_COLS
        ))?;
        let mut rows = stmt.query_map([name], Self::row_to_queue)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_queues(&self) -> Result<Vec<QueueConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM queues ORDER BY name", Self::QUEUE_COLS
        ))?;
        let rows = stmt.query_map([], Self::row_to_queue)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn add_relay(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO relays (url, added_at) VALUES (?1, ?2)",
            rusqlite::params![url, Self::now()],
        )?;
        Ok(())
    }

    pub fn list_relays(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT url FROM relays ORDER BY url")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn remove_relay(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM relays WHERE url = ?1", [url])?;
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q-store`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(store): queue and relay CRUD"
```

---

### Task 7: Store — Message Lifecycle, DLQ, Trace, Stats

**Files:**
- Modify: `crates/nostr-q-store/src/store.rs`

**Interfaces:**
- Produces (types in `nostr_q_store`):
  - `MessageRecord { mid: String, queue: String, event_id: String, trace_id: String, envelope_json: String, status: String, attempts: u32, idem_key: Option<String>, visible_at: i64, created_at: i64 }`
  - `DlqRecord { mid: String, queue: String, reason: String, attempts: u32, dead_at: i64 }`
  - `LifecycleRecord { mid: String, trace_id: String, kind: String, detail: String, created_at: i64 }`
  - `QueueStats { pending: u32, in_flight: u32, acked: u32, dead: u32, oldest_pending_age_secs: Option<i64> }` (all types derive `Debug, Clone, serde::Serialize`)
- Produces (methods on `Store`):
  - `insert_message(&self, rec: &MessageRecord) -> Result<bool>` — returns `false` on duplicate `event_id` or duplicate `(queue, idem_key)` (INSERT OR IGNORE semantics)
  - `get_message(&self, mid: &str) -> Result<Option<MessageRecord>>`
  - `claimable(&self, queue: &str, now: i64, limit: u32) -> Result<Vec<MessageRecord>>` — pending-and-visible OR claimed-with-expired-lease, oldest first
  - `mark_claimed(&self, mid: &str, consumer: &str, lease_expires_at: i64) -> Result<()>`
  - `mark_acked(&self, mid: &str) -> Result<()>`
  - `mark_pending(&self, mid: &str, visible_at: i64) -> Result<()>`
  - `incr_attempts(&self, mid: &str) -> Result<u32>` — returns new attempt count
  - `move_to_dlq(&self, mid: &str, reason: &str) -> Result<()>` — sets status `dead` + inserts dlq row
  - `dlq_list(&self, queue: Option<&str>) -> Result<Vec<DlqRecord>>`
  - `dlq_retry(&self, mid: &str) -> Result<()>` — removes dlq row, resets status `pending`, attempts 0, visible now
  - `record_lifecycle(&self, mid: &str, trace_id: &str, kind: &str, detail: &str) -> Result<()>`
  - `trace(&self, trace_id: &str) -> Result<Vec<LifecycleRecord>>` (oldest first)
  - `trace_id_for_mid(&self, mid: &str) -> Result<Option<String>>`
  - `stats(&self, queue: &str, now: i64) -> Result<QueueStats>`

- [ ] **Step 1: Write the failing tests** (append to tests module)

```rust
fn rec(mid: &str, queue: &str) -> MessageRecord {
    MessageRecord {
        mid: mid.into(),
        queue: queue.into(),
        event_id: format!("ev-{mid}"),
        trace_id: format!("tr-{mid}"),
        envelope_json: "{}".into(),
        status: "pending".into(),
        attempts: 0,
        idem_key: None,
        visible_at: 0,
        created_at: 100,
    }
}

#[test]
fn message_lifecycle_pending_claim_ack() {
    let store = Store::open_in_memory().unwrap();
    assert!(store.insert_message(&rec("m1", "q")).unwrap());
    assert!(!store.insert_message(&rec("m1", "q")).unwrap()); // dup event_id ignored

    let claimable = store.claimable("q", 1000, 10).unwrap();
    assert_eq!(claimable.len(), 1);
    store.mark_claimed("m1", "pubkey-a", 2000).unwrap();
    assert!(store.claimable("q", 1000, 10).unwrap().is_empty()); // lease active
    assert_eq!(store.claimable("q", 2001, 10).unwrap().len(), 1); // lease expired -> reclaimable
    store.mark_acked("m1").unwrap();
    assert!(store.claimable("q", 3000, 10).unwrap().is_empty());
    assert_eq!(store.get_message("m1").unwrap().unwrap().status, "acked");
}

#[test]
fn idempotency_key_dedupes() {
    let store = Store::open_in_memory().unwrap();
    let mut a = rec("m1", "q");
    a.idem_key = Some("order-42".into());
    let mut b = rec("m2", "q");
    b.idem_key = Some("order-42".into());
    assert!(store.insert_message(&a).unwrap());
    assert!(!store.insert_message(&b).unwrap());
}

#[test]
fn visible_at_defers_retry() {
    let store = Store::open_in_memory().unwrap();
    store.insert_message(&rec("m1", "q")).unwrap();
    store.mark_pending("m1", 5000).unwrap();
    assert!(store.claimable("q", 4999, 10).unwrap().is_empty());
    assert_eq!(store.claimable("q", 5000, 10).unwrap().len(), 1);
}

#[test]
fn dlq_flow() {
    let store = Store::open_in_memory().unwrap();
    store.insert_message(&rec("m1", "q")).unwrap();
    assert_eq!(store.incr_attempts("m1").unwrap(), 1);
    assert_eq!(store.incr_attempts("m1").unwrap(), 2);
    store.move_to_dlq("m1", "handler exit 1").unwrap();
    assert!(store.claimable("q", 9999, 10).unwrap().is_empty());
    let dlq = store.dlq_list(Some("q")).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].reason, "handler exit 1");
    assert_eq!(dlq[0].attempts, 2);
    store.dlq_retry("m1").unwrap();
    assert!(store.dlq_list(None).unwrap().is_empty());
    let m = store.get_message("m1").unwrap().unwrap();
    assert_eq!(m.status, "pending");
    assert_eq!(m.attempts, 0);
}

#[test]
fn trace_and_stats() {
    let store = Store::open_in_memory().unwrap();
    store.insert_message(&rec("m1", "q")).unwrap();
    store.record_lifecycle("m1", "tr-m1", "published", "q").unwrap();
    store.record_lifecycle("m1", "tr-m1", "claimed", "pubkey-a").unwrap();
    let t = store.trace("tr-m1").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[0].kind, "published");
    assert_eq!(store.trace_id_for_mid("m1").unwrap().as_deref(), Some("tr-m1"));

    let stats = store.stats("q", 200).unwrap();
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.oldest_pending_age_secs, Some(100)); // created_at=100, now=200
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q-store`
Expected: compile error.

- [ ] **Step 3: Implement** (append to `store.rs`)

```rust
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct MessageRecord {
    pub mid: String,
    pub queue: String,
    pub event_id: String,
    pub trace_id: String,
    pub envelope_json: String,
    pub status: String,
    pub attempts: u32,
    pub idem_key: Option<String>,
    pub visible_at: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DlqRecord {
    pub mid: String,
    pub queue: String,
    pub reason: String,
    pub attempts: u32,
    pub dead_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LifecycleRecord {
    pub mid: String,
    pub trace_id: String,
    pub kind: String,
    pub detail: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub pending: u32,
    pub in_flight: u32,
    pub acked: u32,
    pub dead: u32,
    pub oldest_pending_age_secs: Option<i64>,
}

const MSG_COLS: &str =
    "mid, queue, event_id, trace_id, envelope_json, status, attempts, idem_key, visible_at, created_at";

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    Ok(MessageRecord {
        mid: row.get(0)?,
        queue: row.get(1)?,
        event_id: row.get(2)?,
        trace_id: row.get(3)?,
        envelope_json: row.get(4)?,
        status: row.get(5)?,
        attempts: row.get(6)?,
        idem_key: row.get(7)?,
        visible_at: row.get(8)?,
        created_at: row.get(9)?,
    })
}

impl Store {
    pub fn insert_message(&self, rec: &MessageRecord) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "INSERT OR IGNORE INTO messages
               (mid, queue, event_id, trace_id, envelope_json, status, attempts, idem_key, visible_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                rec.mid, rec.queue, rec.event_id, rec.trace_id, rec.envelope_json,
                rec.status, rec.attempts, rec.idem_key, rec.visible_at, rec.created_at, Self::now()
            ],
        )?;
        Ok(n == 1)
    }

    pub fn get_message(&self, mid: &str) -> Result<Option<MessageRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare(&format!("SELECT {MSG_COLS} FROM messages WHERE mid = ?1"))?;
        let mut rows = stmt.query_map([mid], row_to_message)?;
        Ok(rows.next().transpose()?)
    }

    pub fn claimable(&self, queue: &str, now: i64, limit: u32) -> Result<Vec<MessageRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {MSG_COLS} FROM messages
             WHERE queue = ?1 AND (
               (status = 'pending' AND visible_at <= ?2)
               OR (status = 'claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?2)
             )
             ORDER BY created_at ASC LIMIT ?3"
        ))?;
        let rows = stmt.query_map(rusqlite::params![queue, now, limit], row_to_message)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn update(&self, sql: &str, params: impl rusqlite::Params) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(sql, params)?;
        Ok(())
    }

    pub fn mark_claimed(&self, mid: &str, consumer: &str, lease_expires_at: i64) -> Result<()> {
        self.update(
            "UPDATE messages SET status='claimed', consumer=?2, lease_expires_at=?3, updated_at=?4 WHERE mid=?1",
            rusqlite::params![mid, consumer, lease_expires_at, Self::now()],
        )
    }

    pub fn mark_acked(&self, mid: &str) -> Result<()> {
        self.update(
            "UPDATE messages SET status='acked', lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )
    }

    pub fn mark_pending(&self, mid: &str, visible_at: i64) -> Result<()> {
        self.update(
            "UPDATE messages SET status='pending', lease_expires_at=NULL, visible_at=?2, updated_at=?3 WHERE mid=?1",
            rusqlite::params![mid, visible_at, Self::now()],
        )
    }

    pub fn incr_attempts(&self, mid: &str) -> Result<u32> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE messages SET attempts = attempts + 1, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        Ok(conn.query_row("SELECT attempts FROM messages WHERE mid=?1", [mid], |r| r.get(0))?)
    }

    pub fn move_to_dlq(&self, mid: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE messages SET status='dead', lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO dlq (mid, queue, reason, attempts, dead_at)
             SELECT mid, queue, ?2, attempts, ?3 FROM messages WHERE mid=?1",
            rusqlite::params![mid, reason, Self::now()],
        )?;
        Ok(())
    }

    pub fn dlq_list(&self, queue: Option<&str>) -> Result<Vec<DlqRecord>> {
        let conn = self.conn.lock().unwrap();
        let map = |row: &rusqlite::Row<'_>| -> rusqlite::Result<DlqRecord> {
            Ok(DlqRecord {
                mid: row.get(0)?, queue: row.get(1)?, reason: row.get(2)?,
                attempts: row.get(3)?, dead_at: row.get(4)?,
            })
        };
        let sql_all = "SELECT mid, queue, reason, attempts, dead_at FROM dlq ORDER BY dead_at";
        let sql_q = "SELECT mid, queue, reason, attempts, dead_at FROM dlq WHERE queue=?1 ORDER BY dead_at";
        let out = match queue {
            Some(q) => {
                let mut stmt = conn.prepare(sql_q)?;
                let rows = stmt.query_map([q], map)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            }
            None => {
                let mut stmt = conn.prepare(sql_all)?;
                let rows = stmt.query_map([], map)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            }
        };
        Ok(out)
    }

    pub fn dlq_retry(&self, mid: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE messages SET status='pending', attempts=0, visible_at=0, lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        conn.execute("DELETE FROM dlq WHERE mid=?1", [mid])?;
        Ok(())
    }

    pub fn record_lifecycle(&self, mid: &str, trace_id: &str, kind: &str, detail: &str) -> Result<()> {
        self.update(
            "INSERT INTO lifecycle (mid, trace_id, kind, detail, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![mid, trace_id, kind, detail, Self::now()],
        )
    }

    pub fn trace(&self, trace_id: &str) -> Result<Vec<LifecycleRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT mid, trace_id, kind, detail, created_at FROM lifecycle WHERE trace_id=?1 ORDER BY id",
        )?;
        let rows = stmt.query_map([trace_id], |row| {
            Ok(LifecycleRecord {
                mid: row.get(0)?, trace_id: row.get(1)?, kind: row.get(2)?,
                detail: row.get(3)?, created_at: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn trace_id_for_mid(&self, mid: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT trace_id FROM messages WHERE mid=?1")?;
        let mut rows = stmt.query_map([mid], |r| r.get(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn stats(&self, queue: &str, now: i64) -> Result<QueueStats> {
        let conn = self.conn.lock().unwrap();
        let count = |status: &str| -> rusqlite::Result<u32> {
            conn.query_row(
                "SELECT COUNT(*) FROM messages WHERE queue=?1 AND status=?2",
                rusqlite::params![queue, status],
                |r| r.get(0),
            )
        };
        let oldest: Option<i64> = conn
            .query_row(
                "SELECT MIN(created_at) FROM messages WHERE queue=?1 AND status='pending'",
                [queue],
                |r| r.get(0),
            )
            .unwrap_or(None);
        Ok(QueueStats {
            pending: count("pending")?,
            in_flight: count("claimed")?,
            acked: count("acked")?,
            dead: count("dead")?,
            oldest_pending_age_secs: oldest.map(|c| now - c),
        })
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q-store`
Expected: all pass (9 tests total in the crate).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(store): message lifecycle, claims/leases, DLQ, trace, stats"
```

---

### Task 8: Relay — Transport Trait + MockTransport

**Files:**
- Create: `crates/nostr-q-relay/src/transport.rs`, `crates/nostr-q-relay/src/mock.rs`
- Modify: `crates/nostr-q-relay/src/lib.rs`, `crates/nostr-q-relay/Cargo.toml`

**Interfaces:**
- Produces (`nostr_q_relay`):
  - `RelayHealth { url: String, connected: bool, latency_ms: Option<u64> }` (derives `Debug, Clone, serde::Serialize`)
  - trait `Transport: Send + Sync` (async-trait):
    - `async fn publish(&self, event: nostr::Event) -> anyhow::Result<nostr::EventId>`
    - `async fn subscribe(&self, filter: nostr::Filter) -> anyhow::Result<tokio::sync::mpsc::Receiver<nostr::Event>>` — replays matching stored events, then streams live matches
    - `async fn query(&self, filter: nostr::Filter) -> anyhow::Result<Vec<nostr::Event>>`
    - `async fn health(&self) -> Vec<RelayHealth>`
  - `MockTransport::new() -> MockTransport` — in-memory implementation for tests (also used by downstream crates' tests; exported publicly).

- [ ] **Step 1: `crates/nostr-q-relay/Cargo.toml`**

```toml
[package]
name = "nostr-q-relay"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
anyhow.workspace = true
async-trait.workspace = true
tokio.workspace = true
serde.workspace = true
nostr.workspace = true
nostr-sdk.workspace = true
nostr-q-core.workspace = true
tracing.workspace = true
```

- [ ] **Step 2: Write the failing tests** (bottom of `mock.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;
    use nostr::{EventBuilder, Filter, Keys, Kind};

    fn event(keys: &Keys, kind: u16, topic: &str) -> nostr::Event {
        EventBuilder::new(Kind::Custom(kind), "")
            .tags(vec![nostr::Tag::hashtag(topic)])
            .sign_with_keys(keys)
            .unwrap()
    }

    #[tokio::test]
    async fn publish_then_query_filters_by_kind_and_tag() {
        let t = MockTransport::new();
        let keys = Keys::generate();
        t.publish(event(&keys, 4620, "a")).await.unwrap();
        t.publish(event(&keys, 4620, "b")).await.unwrap();
        t.publish(event(&keys, 4622, "a")).await.unwrap();
        let found = t
            .query(Filter::new().kind(Kind::Custom(4620)).hashtag("a"))
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
    }

    #[tokio::test]
    async fn subscribe_replays_stored_and_streams_live() {
        let t = MockTransport::new();
        let keys = Keys::generate();
        t.publish(event(&keys, 4620, "a")).await.unwrap(); // stored before subscribe
        let mut rx = t
            .subscribe(Filter::new().kind(Kind::Custom(4620)).hashtag("a"))
            .await
            .unwrap();
        let replayed = rx.recv().await.unwrap();
        assert_eq!(replayed.kind.as_u16(), 4620);
        t.publish(event(&keys, 4620, "a")).await.unwrap(); // live
        t.publish(event(&keys, 4620, "other")).await.unwrap(); // filtered out
        let live = rx.recv().await.unwrap();
        assert_eq!(live.kind.as_u16(), 4620);
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p nostr-q-relay`
Expected: compile error.

- [ ] **Step 4: Implement**

`crates/nostr-q-relay/src/transport.rs`:

```rust
use async_trait::async_trait;
use nostr::{Event, EventId, Filter};
use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize)]
pub struct RelayHealth {
    pub url: String,
    pub connected: bool,
    pub latency_ms: Option<u64>,
}

/// Abstraction over one-or-more Nostr relays. The only nostr-sdk-aware
/// implementation is NostrTransport; tests use MockTransport.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, event: Event) -> anyhow::Result<EventId>;
    /// Replays stored matching events, then streams live matches.
    async fn subscribe(&self, filter: Filter) -> anyhow::Result<mpsc::Receiver<Event>>;
    async fn query(&self, filter: Filter) -> anyhow::Result<Vec<Event>>;
    async fn health(&self) -> Vec<RelayHealth>;
}
```

`crates/nostr-q-relay/src/mock.rs`:

```rust
use std::sync::Mutex;

use async_trait::async_trait;
use nostr::{Event, EventId, Filter};
use tokio::sync::{broadcast, mpsc};

use crate::transport::{RelayHealth, Transport};

/// In-memory Transport for tests: stores every published event and
/// broadcasts to live subscribers. Filter matching uses nostr's own
/// Filter::match_event.
pub struct MockTransport {
    events: Mutex<Vec<Event>>,
    tx: broadcast::Sender<Event>,
}

impl MockTransport {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { events: Mutex::new(Vec::new()), tx }
    }
}

impl Default for MockTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for MockTransport {
    async fn publish(&self, event: Event) -> anyhow::Result<EventId> {
        let id = event.id;
        self.events.lock().unwrap().push(event.clone());
        let _ = self.tx.send(event); // no subscribers is fine
        Ok(id)
    }

    async fn subscribe(&self, filter: Filter) -> anyhow::Result<mpsc::Receiver<Event>> {
        let (out_tx, out_rx) = mpsc::channel(256);
        let stored: Vec<Event> = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| filter.match_event(e))
            .cloned()
            .collect();
        let mut live = self.tx.subscribe();
        tokio::spawn(async move {
            for e in stored {
                if out_tx.send(e).await.is_err() {
                    return;
                }
            }
            while let Ok(e) = live.recv().await {
                if filter.match_event(&e) && out_tx.send(e).await.is_err() {
                    return;
                }
            }
        });
        Ok(out_rx)
    }

    async fn query(&self, filter: Filter) -> anyhow::Result<Vec<Event>> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| filter.match_event(e))
            .cloned()
            .collect())
    }

    async fn health(&self) -> Vec<RelayHealth> {
        vec![RelayHealth { url: "mock://".into(), connected: true, latency_ms: Some(0) }]
    }
}
```

Note: the `subscribe` replay has a benign race (an event published between the stored-snapshot and the live-subscription could appear twice — never be lost — because `tx.subscribe()` is called before spawning but after snapshotting; keep that ordering). Duplicates are acceptable: the store dedupes by `event_id`.

`crates/nostr-q-relay/src/lib.rs`:

```rust
pub mod mock;
pub mod transport;

pub use mock::MockTransport;
pub use transport::{RelayHealth, Transport};
```

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test -p nostr-q-relay`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(relay): Transport trait and in-memory MockTransport"
```

---

### Task 9: Relay — NostrTransport (nostr-sdk) + Health Check

**Files:**
- Create: `crates/nostr-q-relay/src/nostr_transport.rs`
- Modify: `crates/nostr-q-relay/src/lib.rs`

**Interfaces:**
- Consumes: `Transport`, `RelayHealth` (Task 8).
- Produces: `NostrTransport` with `NostrTransport::connect(keys: nostr::Keys, relays: &[String]) -> anyhow::Result<NostrTransport>` implementing `Transport`.

This is the ONLY file that touches `nostr-sdk`. It cannot be unit-tested offline; it is verified by compile + the manual end-to-end check in Task 19. If the pinned nostr-sdk 0.39 API differs from below, adapt this file only (docs: `https://docs.rs/nostr-sdk/0.39`).

- [ ] **Step 1: Implement** (`nostr_transport.rs`; add `pub mod nostr_transport; pub use nostr_transport::NostrTransport;` to `lib.rs`)

```rust
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nostr::{Event, EventId, Filter, Keys};
use nostr_sdk::prelude::*;
use tokio::sync::mpsc;

use crate::transport::{RelayHealth, Transport};

pub struct NostrTransport {
    client: Client,
}

impl NostrTransport {
    pub async fn connect(keys: Keys, relays: &[String]) -> anyhow::Result<Self> {
        anyhow::ensure!(!relays.is_empty(), "no relays configured — run `nq relay add <url>` first");
        let client = Client::new(keys);
        for url in relays {
            client.add_relay(url.clone()).await?;
        }
        client.connect().await;
        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for NostrTransport {
    async fn publish(&self, event: Event) -> anyhow::Result<EventId> {
        let output = self.client.send_event(event).await?;
        anyhow::ensure!(
            !output.success.is_empty(),
            "no relay accepted the event (failed: {:?})",
            output.failed
        );
        Ok(*output.id())
    }

    async fn subscribe(&self, filter: Filter) -> anyhow::Result<mpsc::Receiver<Event>> {
        self.client.subscribe(vec![filter.clone()], None).await?;
        let mut notifications = self.client.notifications();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Ok(notification) = notifications.recv().await {
                if let RelayPoolNotification::Event { event, .. } = notification {
                    if filter.match_event(&event) && tx.send(*event).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(rx)
    }

    async fn query(&self, filter: Filter) -> anyhow::Result<Vec<Event>> {
        let events = self
            .client
            .fetch_events(vec![filter], Duration::from_secs(5))
            .await?;
        Ok(events.into_iter().collect())
    }

    async fn health(&self) -> Vec<RelayHealth> {
        let mut out = Vec::new();
        for (url, relay) in self.client.relays().await {
            let connected = relay.status() == RelayStatus::Connected;
            let latency_ms = if connected {
                let start = Instant::now();
                let probe = self
                    .client
                    .fetch_events_from(
                        [url.clone()],
                        vec![Filter::new().limit(1)],
                        Duration::from_secs(5),
                    )
                    .await;
                probe.ok().map(|_| start.elapsed().as_millis() as u64)
            } else {
                None
            };
            out.push(RelayHealth { url: url.to_string(), connected, latency_ms });
        }
        out
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p nostr-q-relay`
Expected: no errors. (Adapt to the pinned nostr-sdk API if needed; the `Transport` trait contract must not change.)

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(relay): NostrTransport over nostr-sdk with relay health probe"
```

---

### Task 10: SDK — `NostrQ` Facade + Publish

**Files:**
- Modify: `crates/nostr-q/Cargo.toml`, `crates/nostr-q/src/lib.rs`

**Interfaces:**
- Consumes: `Store`, `MessageRecord` (Tasks 5–7); `Transport`, `MockTransport` (Task 8); `protocol::*`, `Envelope`, `ids`, `QueueConfig` (Tasks 2–4).
- Produces (`nostr_q`):
  - `NostrQ { ... }` with `NostrQ::new(keys: nostr::Keys, store: Arc<Store>, transport: Arc<dyn Transport>) -> NostrQ`, plus accessors `store(&self) -> &Arc<Store>` and `keys(&self) -> &nostr::Keys`.
  - `PublishReceipt { mid: String, trace_id: String, event_id: String }` (derives `Debug, Clone, serde::Serialize`)
  - `async fn publish(&self, queue: &str, body: serde_json::Value, idem: Option<String>) -> anyhow::Result<PublishReceipt>` — errors on unknown queue; builds envelope+event, publishes, records message row (status `pending` for work queues, `acked` for pubsub — pubsub needs no ack tracking) and lifecycle `published`.
  - Re-exports: `pub use nostr_q_core::{envelope, ids, protocol, queue};`, `pub use nostr_q_store as store_crate;`, `pub use nostr_q_relay as relay;` so applications embed one crate.

- [ ] **Step 1: `crates/nostr-q/Cargo.toml`**

```toml
[package]
name = "nostr-q"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
anyhow.workspace = true
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
chrono.workspace = true
nostr.workspace = true
tracing.workspace = true
nostr-q-core.workspace = true
nostr-q-store.workspace = true
nostr-q-relay.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 2: Write the failing test** (in `crates/nostr-q/src/lib.rs` tests module)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use nostr::Keys;
    use nostr_q_core::queue::QueueConfig;
    use nostr_q_relay::{MockTransport, Transport};
    use nostr_q_store::Store;
    use serde_json::json;

    pub(crate) fn setup() -> (NostrQ, Arc<MockTransport>) {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.upsert_queue(&QueueConfig::work_queue("jobs.email")).unwrap();
        store.upsert_queue(&QueueConfig::pubsub("events.user.created")).unwrap();
        let transport = Arc::new(MockTransport::new());
        let nq = NostrQ::new(Keys::generate(), store, transport.clone());
        (nq, transport)
    }

    #[tokio::test]
    async fn publish_records_message_and_sends_event() {
        let (nq, transport) = setup();
        let receipt = nq
            .publish("jobs.email", json!({"to": "a@b.c"}), Some("order-1".into()))
            .await
            .unwrap();
        assert_eq!(receipt.mid.len(), 26);

        // event landed on the transport
        let events = transport
            .query(nostr::Filter::new().kind(nostr::Kind::Custom(
                nostr_q_core::protocol::KIND_MESSAGE,
            )))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        // local state recorded, pending, with trace
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(rec.status, "pending");
        assert_eq!(rec.queue, "jobs.email");
        assert_eq!(rec.trace_id, receipt.trace_id);
        assert_eq!(nq.store().trace(&receipt.trace_id).unwrap()[0].kind, "published");
    }

    #[tokio::test]
    async fn publish_to_unknown_queue_errors() {
        let (nq, _) = setup();
        assert!(nq.publish("nope", json!({}), None).await.is_err());
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p nostr-q`
Expected: compile error — `NostrQ` not defined.

- [ ] **Step 4: Implement** (`lib.rs` above tests)

```rust
use std::sync::Arc;

use anyhow::{anyhow, Result};
use nostr::Keys;
use serde::Serialize;
use serde_json::Value;

use nostr_q_core::envelope::Envelope;
use nostr_q_core::ids::{new_mid, new_trace_id};
use nostr_q_core::protocol::{build_message_event, NqMessage};
use nostr_q_core::queue::QueueMode;
use nostr_q_relay::Transport;
use nostr_q_store::{MessageRecord, Store};

pub use nostr_q_core::{envelope, ids, protocol, queue};
pub use nostr_q_relay as relay;
pub use nostr_q_store as store_crate;

#[derive(Debug, Clone, Serialize)]
pub struct PublishReceipt {
    pub mid: String,
    pub trace_id: String,
    pub event_id: String,
}

pub struct NostrQ {
    keys: Keys,
    store: Arc<Store>,
    transport: Arc<dyn Transport>,
}

impl NostrQ {
    pub fn new(keys: Keys, store: Arc<Store>, transport: Arc<dyn Transport>) -> Self {
        Self { keys, store, transport }
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    pub async fn publish(
        &self,
        queue: &str,
        body: Value,
        idem: Option<String>,
    ) -> Result<PublishReceipt> {
        let config = self
            .store
            .get_queue(queue)?
            .ok_or_else(|| anyhow!("unknown queue '{queue}' — create it with `nq queue create {queue}`"))?;
        let msg = NqMessage {
            mid: new_mid(),
            queue: queue.to_string(),
            trace_id: new_trace_id(),
            attempt: 0,
            idem: idem.clone(),
            envelope: Envelope::new(body),
        };
        let event = build_message_event(&self.keys, config.mode, &msg)?;
        let event_id = self.transport.publish(event).await?;
        // pubsub topics need no ack tracking; record as acked so they never
        // show up as claimable work.
        let status = match config.mode {
            QueueMode::WorkQueue => "pending",
            QueueMode::Pubsub => "acked",
        };
        let rec = MessageRecord {
            mid: msg.mid.clone(),
            queue: msg.queue.clone(),
            event_id: event_id.to_hex(),
            trace_id: msg.trace_id.clone(),
            envelope_json: msg.envelope.to_json()?,
            status: status.to_string(),
            attempts: 0,
            idem_key: idem,
            visible_at: 0,
            created_at: Self::now(),
        };
        self.store.insert_message(&rec)?;
        self.store
            .record_lifecycle(&msg.mid, &msg.trace_id, "published", queue)?;
        Ok(PublishReceipt {
            mid: msg.mid,
            trace_id: msg.trace_id,
            event_id: event_id.to_hex(),
        })
    }
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test -p nostr-q`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(sdk): NostrQ facade with publish"
```

---

### Task 11: SDK — Subscribe + Ingest

**Files:**
- Modify: `crates/nostr-q/src/lib.rs`

**Interfaces:**
- Produces (methods on `NostrQ`):
  - `async fn subscribe(&self, topic: &str) -> anyhow::Result<tokio::sync::mpsc::Receiver<NqMessage>>` — pub/sub consumption: parses message events on the topic and delivers everything, including the subscriber's own messages (fanout semantics); malformed events are skipped with a `tracing::warn!`.
  - `async fn spawn_ingest(&self, queue: &str) -> anyhow::Result<tokio::task::JoinHandle<()>>` — background task that subscribes to a queue's message events and inserts them into the local store as `pending` work (deduped by `event_id`/idem via `insert_message` returning false). Records lifecycle `seen` for newly ingested foreign messages. Used by workers so remotely-published jobs become locally claimable.

- [ ] **Step 1: Write the failing tests** (append to tests module)

```rust
#[tokio::test]
async fn subscribe_delivers_pubsub_messages() {
    let (nq, _) = setup();
    let mut rx = nq.subscribe("events.user.created").await.unwrap();
    nq.publish("events.user.created", json!({"id": 7}), None).await.unwrap();
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg.queue, "events.user.created");
    assert_eq!(msg.envelope.body, json!({"id": 7}));
}

#[tokio::test]
async fn ingest_stores_remote_messages_as_pending() {
    // producer and worker share a transport but have separate stores/keys
    let transport = Arc::new(MockTransport::new());
    let mk = |t: Arc<MockTransport>| {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.upsert_queue(&QueueConfig::work_queue("jobs.email")).unwrap();
        NostrQ::new(Keys::generate(), store, t)
    };
    let producer = mk(transport.clone());
    let worker = mk(transport.clone());

    let _ingest = worker.spawn_ingest("jobs.email").await.unwrap();
    let receipt = producer.publish("jobs.email", json!({"n": 1}), None).await.unwrap();

    // poll until the ingest task lands the row (max ~2s)
    let mut found = None;
    for _ in 0..40 {
        if let Some(rec) = worker.store().get_message(&receipt.mid).unwrap() {
            found = Some(rec);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let rec = found.expect("ingest should store the remote message");
    assert_eq!(rec.status, "pending");
    assert_eq!(rec.event_id, receipt.event_id);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q`
Expected: compile error — methods not defined.

- [ ] **Step 3: Implement** (append to `impl NostrQ`)

```rust
use nostr::{Filter, Kind};
use nostr_q_core::protocol::{parse_message_event, KIND_MESSAGE};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

impl NostrQ {
    fn message_filter(topic: &str) -> Filter {
        Filter::new().kind(Kind::Custom(KIND_MESSAGE)).hashtag(topic)
    }

    pub async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<NqMessage>> {
        let mut events = self.transport.subscribe(Self::message_filter(topic)).await?;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match parse_message_event(&event) {
                    Ok(msg) => {
                        if tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "skipping malformed nostr-q event"),
                }
            }
        });
        Ok(rx)
    }

    pub async fn spawn_ingest(&self, queue: &str) -> Result<JoinHandle<()>> {
        let mut events = self.transport.subscribe(Self::message_filter(queue)).await?;
        let store = self.store.clone();
        Ok(tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                let msg = match parse_message_event(&event) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, "skipping malformed nostr-q event");
                        continue;
                    }
                };
                let rec = MessageRecord {
                    mid: msg.mid.clone(),
                    queue: msg.queue.clone(),
                    event_id: event.id.to_hex(),
                    trace_id: msg.trace_id.clone(),
                    envelope_json: msg.envelope.to_json().unwrap_or_else(|_| "{}".into()),
                    status: "pending".to_string(),
                    attempts: msg.attempt,
                    idem_key: msg.idem.clone(),
                    visible_at: 0,
                    created_at: event.created_at.as_u64() as i64,
                };
                match store.insert_message(&rec) {
                    Ok(true) => {
                        let _ = store.record_lifecycle(&msg.mid, &msg.trace_id, "seen", &msg.queue);
                    }
                    Ok(false) => {} // duplicate: already published/ingested locally
                    Err(e) => tracing::warn!(error = %e, "failed to store ingested message"),
                }
            }
        }))
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(sdk): pubsub subscribe and work-queue ingest"
```

---

### Task 12: SDK — Claim / Ack / Nack / Retry / DLQ

**Files:**
- Modify: `crates/nostr-q/src/lib.rs`

**Interfaces:**
- Produces:
  - `NackOutcome` enum: `Retry { attempt: u32, visible_at: i64 } | DeadLettered` (derives `Debug, PartialEq`)
  - `async fn try_claim(&self, rec: &MessageRecord, lease_seconds: u64, settle_ms: u64) -> anyhow::Result<bool>` — publishes a claim event, waits `settle_ms`, queries competing claims, returns true iff our claim wins (then marks claimed locally + lifecycle `claimed`).
  - `async fn ack(&self, mid: &str) -> anyhow::Result<()>` — publishes ack event, marks acked, lifecycle `acked`.
  - `async fn nack(&self, mid: &str, reason: &str) -> anyhow::Result<NackOutcome>` — increments attempts, publishes nack event, lifecycle `nacked`; if `attempts >= queue.max_attempts` publishes DLQ event, moves to DLQ, lifecycle `dead_lettered`; otherwise sets pending with exponential backoff `visible_at = now + retry_base_seconds * 2^(attempts-1)` capped at 3600s (lifecycle `retry_scheduled`).
  - `fn backoff_secs(retry_base_seconds: u64, attempt: u32) -> u64` — pure, public for tests.

- [ ] **Step 1: Write the failing tests** (append to tests module)

```rust
#[tokio::test]
async fn claim_ack_happy_path() {
    let (nq, transport) = setup();
    let receipt = nq.publish("jobs.email", json!({"n": 1}), None).await.unwrap();
    let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();

    assert!(nq.try_claim(&rec, 60, 10).await.unwrap());
    assert_eq!(nq.store().get_message(&receipt.mid).unwrap().unwrap().status, "claimed");

    nq.ack(&receipt.mid).await.unwrap();
    assert_eq!(nq.store().get_message(&receipt.mid).unwrap().unwrap().status, "acked");

    // claim + ack events were published
    let claims = transport
        .query(nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_CLAIM)))
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let kinds: Vec<String> = nq.store().trace(&receipt.trace_id).unwrap()
        .iter().map(|l| l.kind.clone()).collect();
    assert_eq!(kinds, vec!["published", "claimed", "acked"]);
}

#[tokio::test]
async fn competing_claims_only_one_winner() {
    // two workers, shared transport, same message
    let transport = Arc::new(MockTransport::new());
    let mk = |t: Arc<MockTransport>| {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.upsert_queue(&QueueConfig::work_queue("jobs.email")).unwrap();
        NostrQ::new(Keys::generate(), store, t)
    };
    let producer = mk(transport.clone());
    let w1 = mk(transport.clone());
    let w2 = mk(transport.clone());
    let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
    let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
    let receipt = producer.publish("jobs.email", json!({"n": 1}), None).await.unwrap();

    // wait for both ingests
    for w in [&w1, &w2] {
        for _ in 0..40 {
            if w.store().get_message(&receipt.mid).unwrap().is_some() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
    let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
    let (a, b) = tokio::join!(w1.try_claim(&r1, 60, 300), w2.try_claim(&r2, 60, 300));
    let wins = [a.unwrap(), b.unwrap()];
    assert_eq!(wins.iter().filter(|w| **w).count(), 1, "exactly one worker must win the claim");
}

#[tokio::test]
async fn nack_retries_then_dead_letters() {
    let (nq, transport) = setup();
    // tighten policy for the test
    let mut q = nq.store().get_queue("jobs.email").unwrap().unwrap();
    q.max_attempts = 2;
    nq.store().upsert_queue(&q).unwrap();

    let receipt = nq.publish("jobs.email", json!({"n": 1}), None).await.unwrap();

    let out1 = nq.nack(&receipt.mid, "boom").await.unwrap();
    match out1 {
        NackOutcome::Retry { attempt, visible_at } => {
            assert_eq!(attempt, 1);
            assert!(visible_at > chrono::Utc::now().timestamp());
        }
        other => panic!("expected retry, got {other:?}"),
    }
    assert_eq!(nq.store().get_message(&receipt.mid).unwrap().unwrap().status, "pending");

    let out2 = nq.nack(&receipt.mid, "boom again").await.unwrap();
    assert_eq!(out2, NackOutcome::DeadLettered);
    assert_eq!(nq.store().get_message(&receipt.mid).unwrap().unwrap().status, "dead");
    assert_eq!(nq.store().dlq_list(Some("jobs.email")).unwrap().len(), 1);

    let dlq_events = transport
        .query(nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_DLQ)))
        .await
        .unwrap();
    assert_eq!(dlq_events.len(), 1);
}

#[test]
fn backoff_is_exponential_and_capped() {
    assert_eq!(backoff_secs(5, 1), 5);
    assert_eq!(backoff_secs(5, 2), 10);
    assert_eq!(backoff_secs(5, 3), 20);
    assert_eq!(backoff_secs(5, 30), 3600); // capped
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q`
Expected: compile error.

- [ ] **Step 3: Implement** (append to `lib.rs`)

```rust
use nostr::EventId;
use nostr_q_core::protocol::{
    build_ack_event, build_claim_event, build_dlq_event, build_nack_event, claim_winner,
    parse_claim_event, ClaimInfo, KIND_CLAIM,
};

#[derive(Debug, PartialEq)]
pub enum NackOutcome {
    Retry { attempt: u32, visible_at: i64 },
    DeadLettered,
}

/// Exponential backoff: base * 2^(attempt-1), capped at 1 hour.
pub fn backoff_secs(retry_base_seconds: u64, attempt: u32) -> u64 {
    retry_base_seconds
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(20))
        .min(3600)
}

impl NostrQ {
    pub async fn try_claim(
        &self,
        rec: &MessageRecord,
        lease_seconds: u64,
        settle_ms: u64,
    ) -> Result<bool> {
        let now = Self::now();
        let lease_expires_at = now + lease_seconds as i64;
        let message_event_id = EventId::from_hex(&rec.event_id)?;
        let claim = build_claim_event(
            &self.keys,
            message_event_id,
            &rec.queue,
            &rec.mid,
            &rec.trace_id,
            lease_expires_at,
        )?;
        let our_claim_id = claim.id;
        self.transport.publish(claim).await?;
        tokio::time::sleep(std::time::Duration::from_millis(settle_ms)).await;

        let filter = Filter::new()
            .kind(Kind::Custom(KIND_CLAIM))
            .event(message_event_id);
        let claims: Vec<ClaimInfo> = self
            .transport
            .query(filter)
            .await?
            .iter()
            .filter_map(|e| parse_claim_event(e).ok())
            .collect();
        let we_won = claim_winner(&claims, now)
            .map(|w| w.claim_event_id == our_claim_id)
            .unwrap_or(false);
        if we_won {
            let consumer = self.keys.public_key().to_hex();
            self.store.mark_claimed(&rec.mid, &consumer, lease_expires_at)?;
            self.store
                .record_lifecycle(&rec.mid, &rec.trace_id, "claimed", &consumer)?;
        }
        Ok(we_won)
    }

    fn message_ref(&self, mid: &str) -> Result<(MessageRecord, EventId)> {
        let rec = self
            .store
            .get_message(mid)?
            .ok_or_else(|| anyhow!("unknown message id '{mid}'"))?;
        let event_id = EventId::from_hex(&rec.event_id)?;
        Ok((rec, event_id))
    }

    pub async fn ack(&self, mid: &str) -> Result<()> {
        let (rec, event_id) = self.message_ref(mid)?;
        let event = build_ack_event(&self.keys, event_id, &rec.queue, mid, &rec.trace_id)?;
        self.transport.publish(event).await?;
        self.store.mark_acked(mid)?;
        self.store.record_lifecycle(mid, &rec.trace_id, "acked", "")?;
        Ok(())
    }

    pub async fn nack(&self, mid: &str, reason: &str) -> Result<NackOutcome> {
        let (rec, event_id) = self.message_ref(mid)?;
        let config = self
            .store
            .get_queue(&rec.queue)?
            .ok_or_else(|| anyhow!("unknown queue '{}'", rec.queue))?;
        let attempts = self.store.incr_attempts(mid)?;
        let event = build_nack_event(
            &self.keys, event_id, &rec.queue, mid, &rec.trace_id, attempts, reason,
        )?;
        self.transport.publish(event).await?;
        self.store.record_lifecycle(mid, &rec.trace_id, "nacked", reason)?;

        if attempts >= config.max_attempts {
            let dlq = build_dlq_event(&self.keys, event_id, &rec.queue, mid, &rec.trace_id, reason)?;
            self.transport.publish(dlq).await?;
            self.store.move_to_dlq(mid, reason)?;
            self.store
                .record_lifecycle(mid, &rec.trace_id, "dead_lettered", reason)?;
            Ok(NackOutcome::DeadLettered)
        } else {
            let visible_at = Self::now() + backoff_secs(config.retry_base_seconds, attempts) as i64;
            self.store.mark_pending(mid, visible_at)?;
            self.store.record_lifecycle(
                mid,
                &rec.trace_id,
                "retry_scheduled",
                &format!("attempt {attempts}, visible_at {visible_at}"),
            )?;
            Ok(NackOutcome::Retry { attempt: attempts, visible_at })
        }
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p nostr-q`
Expected: 8 passed. `competing_claims_only_one_winner` is the critical one — if flaky, the settle window (300 ms in test) vs. claim ordering is the place to look; the winner rule is deterministic once both claims are visible.

- [ ] **Step 5: Run full workspace check**

Run: `cargo test --workspace`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(sdk): claim with conflict resolution, ack, nack with retry backoff and DLQ"
```

---

### Task 13: CLI — Scaffold, Config, `nq init`, `nq key`

**Files:**
- Create: `crates/nostr-q-cli/src/config.rs`, `crates/nostr-q-cli/src/commands.rs`
- Modify: `crates/nostr-q-cli/src/main.rs`, `crates/nostr-q-cli/Cargo.toml`

**Interfaces:**
- Consumes: `Store` (Task 5), `NostrQ` (Task 10), `NostrTransport` (Task 9).
- Produces (`config.rs`):
  - `Config { state: String, key_file: String }` with `Config::default_new()` (state `~/.local/share/nostr-q/state.db`, key_file `~/.config/nostr-q/key`), `load(path: &Path) -> Result<Config>`, `save(&self, path: &Path) -> Result<()>`, `state_path(&self) -> PathBuf` (env `NQ_STATE` overrides), `key_path(&self) -> PathBuf`.
  - `default_config_path() -> PathBuf` — `NQ_CONFIG` env, else `./nostr-q.toml` if it exists, else `~/.config/nostr-q/config.toml` (via `dirs::config_dir()`).
  - `expand_tilde(s: &str) -> PathBuf`; `load_keys(config: &Config) -> Result<nostr::Keys>` — `NQ_PRIVATE_KEY` env first, else key file.
- Produces (`commands.rs`):
  - `Ctx { config: Config, store: Arc<Store>, json: bool }` with `Ctx::load(config_path: Option<PathBuf>, json: bool) -> Result<Ctx>` and `async fn connect(&self) -> Result<NostrQ>` (loads keys, connects `NostrTransport` to relays from the store).
  - `init(config_path: Option<PathBuf>) -> Result<()>`, `key_generate(ctx: &Ctx) -> Result<()>`, `key_show(ctx: &Ctx) -> Result<()>`.

- [ ] **Step 1: `crates/nostr-q-cli/Cargo.toml`**

```toml
[package]
name = "nq"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
anyhow.workspace = true
clap.workspace = true
serde.workspace = true
serde_json.workspace = true
toml.workspace = true
dirs.workspace = true
tokio.workspace = true
tokio-util.workspace = true
chrono.workspace = true
nostr.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
nostr-q.workspace = true
nostr-q-worker.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 2: Write the failing tests** (bottom of `config.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_toml_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = Config::default_new();
        cfg.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.state, "~/.local/share/nostr-q/state.db");
        assert_eq!(loaded.key_file, "~/.config/nostr-q/key");
    }

    #[test]
    fn expand_tilde_expands_home() {
        let p = expand_tilde("~/x/y");
        assert!(!p.to_string_lossy().starts_with('~'));
        assert!(p.ends_with("x/y"));
        assert_eq!(expand_tilde("/abs/path"), std::path::PathBuf::from("/abs/path"));
    }

    #[test]
    fn load_keys_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("key");
        let keys = nostr::Keys::generate();
        std::fs::write(&key_path, keys.secret_key().to_secret_hex()).unwrap();
        let cfg = Config {
            state: "unused".into(),
            key_file: key_path.to_string_lossy().into_owned(),
        };
        let loaded = load_keys(&cfg).unwrap();
        assert_eq!(loaded.public_key(), keys.public_key());
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p nq`
Expected: compile error.

- [ ] **Step 4: Implement**

`crates/nostr-q-cli/src/config.rs`:

```rust
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub state: String,
    pub key_file: String,
}

pub fn expand_tilde(s: &str) -> PathBuf {
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(s)
}

pub fn default_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("NQ_CONFIG") {
        return PathBuf::from(p);
    }
    let local = PathBuf::from("nostr-q.toml");
    if local.exists() {
        return local;
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("nostr-q/config.toml")
}

impl Config {
    pub fn default_new() -> Self {
        Self {
            state: "~/.local/share/nostr-q/state.db".into(),
            key_file: "~/.config/nostr-q/key".into(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| {
            format!("reading config {} — run `nq init` first", path.display())
        })?;
        Ok(toml::from_str(&raw)?)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn state_path(&self) -> PathBuf {
        if let Ok(p) = std::env::var("NQ_STATE") {
            return PathBuf::from(p);
        }
        expand_tilde(&self.state)
    }

    pub fn key_path(&self) -> PathBuf {
        expand_tilde(&self.key_file)
    }
}

pub fn load_keys(config: &Config) -> Result<nostr::Keys> {
    if let Ok(sk) = std::env::var("NQ_PRIVATE_KEY") {
        return nostr::Keys::parse(sk.trim()).context("parsing NQ_PRIVATE_KEY");
    }
    let path = config.key_path();
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "reading key file {} — run `nq key generate` or set NQ_PRIVATE_KEY",
            path.display()
        )
    })?;
    nostr::Keys::parse(raw.trim()).context("parsing key file")
}
```

`crates/nostr-q-cli/src/commands.rs` (initial content — later tasks append):

```rust
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use nostr_q::relay::NostrTransport;
use nostr_q::store_crate::Store;
use nostr_q::NostrQ;

use crate::config::{self, Config};

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
```

`crates/nostr-q-cli/src/main.rs`:

```rust
mod commands;
mod config;

use clap::{Parser, Subcommand};
use commands::Ctx;

#[derive(Parser)]
#[command(name = "nq", version, about = "Nostr-Q: message queues and pub/sub over Nostr relays")]
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
}

#[derive(Subcommand)]
enum KeyCmd {
    /// Generate a new keypair (private key saved to key file, never printed)
    Generate,
    /// Show the public key
    Show,
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
    }
}
```

- [ ] **Step 5: Run tests, then verify manually**

Run: `cargo test -p nq`
Expected: 3 passed.

Manual check (isolated via env overrides):

```bash
export NQ_CONFIG=/tmp/nqtest/config.toml NQ_STATE=/tmp/nqtest/state.db
cargo run -p nq -- init
cargo run -p nq -- init            # second run: "config already exists"
```

Expected: config file and state db created; second run is a no-op. Note the default key path is still `~/.config/nostr-q/key`, so skip `key generate` here or edit `/tmp/nqtest/config.toml`'s `key_file` to `/tmp/nqtest/key` first, then:

```bash
cargo run -p nq -- key generate    # prints public key only
cargo run -p nq -- key show
```

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(cli): nq scaffold with config, init, key generate/show"
```

---

### Task 14: CLI — `nq relay add|list|remove|health`

**Files:**
- Modify: `crates/nostr-q-cli/src/commands.rs`, `crates/nostr-q-cli/src/main.rs`

**Interfaces:**
- Consumes: `Store::{add_relay, list_relays, remove_relay}` (Task 6), `NostrTransport::connect` + `Transport::health` (Tasks 8–9), `config::load_keys`.
- Produces: `relay_add(ctx, url: String)`, `relay_list(ctx)`, `relay_remove(ctx, url: String)`, `relay_health(ctx) -> async` in `commands.rs`; `Relay` subcommand in `main.rs`.

- [ ] **Step 1: Add to `commands.rs`**

```rust
use nostr_q::relay::Transport;

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
```

- [ ] **Step 2: Wire into `main.rs`**

Add to `Cmd`:

```rust
    /// Relay management
    Relay {
        #[command(subcommand)]
        cmd: RelayCmd,
    },
```

New subcommand enum and dispatch arm:

```rust
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
```

```rust
        Cmd::Relay { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                RelayCmd::Add { url } => commands::relay_add(&ctx, &url),
                RelayCmd::List => commands::relay_list(&ctx),
                RelayCmd::Remove { url } => commands::relay_remove(&ctx, &url),
                RelayCmd::Health => commands::relay_health(&ctx).await,
            }
        }
```

- [ ] **Step 3: Verify**

Run: `cargo test -p nq && cargo check -p nq`

Manual (same `NQ_CONFIG`/`NQ_STATE` env as Task 13):

```bash
cargo run -p nq -- relay add wss://relay.damus.io
cargo run -p nq -- relay list
cargo run -p nq -- relay add notaurl        # expect error message
cargo run -p nq -- relay health             # expect connected + latency (needs network)
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cli): relay add/list/remove/health"
```

---

### Task 15: CLI — `nq queue create|list`, `nq pub`, `nq sub`

**Files:**
- Modify: `crates/nostr-q-cli/src/commands.rs`, `crates/nostr-q-cli/src/main.rs`

**Interfaces:**
- Consumes: `QueueConfig`/`QueueMode`/`Delivery` (Task 3), `Store::{upsert_queue, list_queues}` (Task 6), `NostrQ::{publish, subscribe}` (Tasks 10–11).
- Produces: `queue_create(ctx, name, mode, delivery, max_attempts, lease)`, `queue_list(ctx)`, `publish(ctx, queue, payload, idem) -> async`, `subscribe_cmd(ctx, topic) -> async`.

- [ ] **Step 1: Add to `commands.rs`**

```rust
use std::io::Read;
use std::str::FromStr;

use nostr_q::queue::{Delivery, QueueConfig, QueueMode};

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
```

Also add `use anyhow::Context;` to the imports at the top of `commands.rs`.

- [ ] **Step 2: Wire into `main.rs`**

Add to `Cmd`:

```rust
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
```

```rust
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
```

Dispatch arms:

```rust
        Cmd::Queue { cmd } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            match cmd {
                QueueCmd::Create { name, mode, delivery, max_attempts, lease } => {
                    commands::queue_create(&ctx, &name, &mode, delivery, max_attempts, lease)
                }
                QueueCmd::List => commands::queue_list(&ctx),
            }
        }
        Cmd::Pub { queue, payload, idem } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::publish(&ctx, &queue, payload, idem).await
        }
        Cmd::Sub { topic } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::subscribe_cmd(&ctx, &topic).await
        }
```

- [ ] **Step 3: Verify**

Run: `cargo test --workspace && cargo check -p nq`

Manual (needs a reachable relay, e.g. `nak serve` on `ws://localhost:10547`):

```bash
cargo run -p nq -- queue create jobs.email --mode work_queue --delivery at_least_once
cargo run -p nq -- queue create events.user.created --mode pubsub
cargo run -p nq -- queue list
cargo run -p nq -- sub events.user.created &   # terminal 2 in practice
cargo run -p nq -- pub events.user.created '{"id":7}'
# expect the subscriber to print the event
```

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cli): queue create/list, pub, sub"
```

---

### Task 16: Worker Runtime + ExecHandler + `nq worker --exec`

**Files:**
- Create: `crates/nostr-q-worker/src/handlers.rs`
- Modify: `crates/nostr-q-worker/src/lib.rs`, `crates/nostr-q-worker/Cargo.toml`, `crates/nostr-q/src/lib.rs` (add `heartbeat`), `crates/nostr-q-cli/src/commands.rs`, `crates/nostr-q-cli/src/main.rs`

**Interfaces:**
- Consumes: `NostrQ::{spawn_ingest, try_claim, ack, nack, store, keys}` (Tasks 10–12), `Store::claimable`, `build_heartbeat_event` (Task 4).
- Produces (`nostr_q_worker`):
  - `JobContext { mid: String, queue: String, trace_id: String, attempt: u32, idem: Option<String>, payload: serde_json::Value }` with `JobContext::from_record(rec: &MessageRecord) -> JobContext`
  - `HandlerOutcome::{Success, Failure(String)}`
  - trait `Handler: Send + Sync` (async-trait): `async fn handle(&self, job: &JobContext) -> HandlerOutcome`
  - `ExecHandler { command: String }` implementing `Handler` — runs `sh -c <command>`, payload JSON on stdin, env `NQ_MID`, `NQ_QUEUE`, `NQ_TRACE`, `NQ_ATTEMPT`, `NQ_IDEM`; exit 0 → Success, else Failure with exit code + stderr.
  - `WorkerOptions { concurrency: usize, lease_seconds: u64, heartbeat_seconds: u64, settle_ms: u64, poll_ms: u64 }`
  - `async fn run_worker(nq: Arc<NostrQ>, queue: String, handler: Arc<dyn Handler>, opts: WorkerOptions, shutdown: tokio_util::sync::CancellationToken) -> anyhow::Result<()>`
- Produces (on `NostrQ`): `async fn heartbeat(&self, queue: &str) -> anyhow::Result<()>` — publishes the ephemeral heartbeat event.
- Produces (CLI): `nq worker <queue> --exec <cmd> [--concurrency N] [--lease S] [--max-attempts N] [--heartbeat S]`.

- [ ] **Step 1: `crates/nostr-q-worker/Cargo.toml`**

```toml
[package]
name = "nostr-q-worker"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
anyhow.workspace = true
async-trait.workspace = true
tokio.workspace = true
tokio-util.workspace = true
chrono.workspace = true
serde_json.workspace = true
reqwest.workspace = true
tracing.workspace = true
nostr-q.workspace = true

[dev-dependencies]
nostr.workspace = true
tempfile.workspace = true
wiremock.workspace = true
```

- [ ] **Step 2: Add `heartbeat` to `crates/nostr-q/src/lib.rs`** (append to an `impl NostrQ` block)

```rust
    pub async fn heartbeat(&self, queue: &str) -> Result<()> {
        let event = nostr_q_core::protocol::build_heartbeat_event(&self.keys, queue)?;
        self.transport.publish(event).await?;
        Ok(())
    }
```

- [ ] **Step 3: Write the failing tests** (in `crates/nostr-q-worker/src/lib.rs` tests module)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use nostr::Keys;
    use nostr_q::queue::QueueConfig;
    use nostr_q::relay::MockTransport;
    use nostr_q::store_crate::Store;
    use nostr_q::NostrQ;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    fn make_nq() -> Arc<NostrQ> {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.upsert_queue(&QueueConfig::work_queue("jobs.email")).unwrap();
        Arc::new(NostrQ::new(Keys::generate(), store, Arc::new(MockTransport::new())))
    }

    fn job() -> JobContext {
        JobContext {
            mid: "m1".into(),
            queue: "jobs.email".into(),
            trace_id: "t1".into(),
            attempt: 0,
            idem: Some("i1".into()),
            payload: json!({"n": 1}),
        }
    }

    #[tokio::test]
    async fn exec_handler_success_on_exit_zero() {
        let h = crate::handlers::ExecHandler {
            // proves stdin + env are wired: fails unless payload and NQ_MID arrive
            command: r#"payload=$(cat); test "$payload" = '{"n":1}' && test "$NQ_MID" = m1"#.into(),
        };
        assert!(matches!(h.handle(&job()).await, HandlerOutcome::Success));
    }

    #[tokio::test]
    async fn exec_handler_failure_captures_exit_and_stderr() {
        let h = crate::handlers::ExecHandler { command: "echo oops >&2; exit 3".into() };
        match h.handle(&job()).await {
            HandlerOutcome::Failure(reason) => {
                assert!(reason.contains('3'), "reason should mention exit code: {reason}");
                assert!(reason.contains("oops"), "reason should include stderr: {reason}");
            }
            HandlerOutcome::Success => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn worker_loop_claims_runs_and_acks() {
        let nq = make_nq();
        let receipt = nq.publish("jobs.email", json!({"n": 1}), None).await.unwrap();

        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 2,
            lease_seconds: 60,
            heartbeat_seconds: 3600,
            settle_ms: 10,
            poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(crate::handlers::ExecHandler { command: "cat > /dev/null".into() }),
            opts,
            shutdown.clone(),
        ));

        let mut acked = false;
        for _ in 0..100 {
            if nq.store().get_message(&receipt.mid).unwrap().unwrap().status == "acked" {
                acked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(acked, "worker should claim, run handler, and ack");
    }

    #[tokio::test]
    async fn worker_loop_nacks_failures() {
        let nq = make_nq();
        let receipt = nq.publish("jobs.email", json!({"n": 1}), None).await.unwrap();
        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 1, lease_seconds: 60, heartbeat_seconds: 3600, settle_ms: 10, poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(crate::handlers::ExecHandler { command: "exit 1".into() }),
            opts,
            shutdown.clone(),
        ));
        // attempts should start climbing (retry backoff defers re-runs)
        let mut nacked = false;
        for _ in 0..100 {
            if nq.store().get_message(&receipt.mid).unwrap().unwrap().attempts >= 1 {
                nacked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(nacked);
    }
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p nostr-q-worker`
Expected: compile error.

- [ ] **Step 5: Implement**

`crates/nostr-q-worker/src/lib.rs`:

```rust
pub mod handlers;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nostr_q::envelope::Envelope;
use nostr_q::store_crate::MessageRecord;
use nostr_q::NostrQ;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct JobContext {
    pub mid: String,
    pub queue: String,
    pub trace_id: String,
    pub attempt: u32,
    pub idem: Option<String>,
    pub payload: serde_json::Value,
}

impl JobContext {
    pub fn from_record(rec: &MessageRecord) -> Self {
        let payload = Envelope::from_json(&rec.envelope_json)
            .map(|e| e.body)
            .unwrap_or(serde_json::Value::Null);
        Self {
            mid: rec.mid.clone(),
            queue: rec.queue.clone(),
            trace_id: rec.trace_id.clone(),
            attempt: rec.attempts,
            idem: rec.idem_key.clone(),
            payload,
        }
    }
}

#[derive(Debug)]
pub enum HandlerOutcome {
    Success,
    Failure(String),
}

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome;
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub concurrency: usize,
    pub lease_seconds: u64,
    pub heartbeat_seconds: u64,
    pub settle_ms: u64,
    pub poll_ms: u64,
}

pub async fn run_worker(
    nq: Arc<NostrQ>,
    queue: String,
    handler: Arc<dyn Handler>,
    opts: WorkerOptions,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ingest = nq.spawn_ingest(&queue).await?;

    // heartbeat loop (ephemeral events; best effort)
    {
        let nq = nq.clone();
        let queue = queue.clone();
        let shutdown = shutdown.clone();
        let every = Duration::from_secs(opts.heartbeat_seconds.max(1));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = nq.heartbeat(&queue).await {
                            tracing::debug!(error = %e, "heartbeat publish failed");
                        }
                    }
                    _ = shutdown.cancelled() => break,
                }
            }
        });
    }

    let semaphore = Arc::new(Semaphore::new(opts.concurrency));
    tracing::info!(queue = %queue, concurrency = opts.concurrency, "worker started");

    while !shutdown.is_cancelled() {
        let now = chrono::Utc::now().timestamp();
        let batch = nq.store().claimable(&queue, now, opts.concurrency as u32)?;
        if batch.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(opts.poll_ms)) => {}
                _ = shutdown.cancelled() => break,
            }
            continue;
        }
        for rec in batch {
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break, // all slots busy; re-poll after they free up
            };
            let nq = nq.clone();
            let handler = handler.clone();
            let (lease, settle) = (opts.lease_seconds, opts.settle_ms);
            tokio::spawn(async move {
                let _permit = permit;
                match nq.try_claim(&rec, lease, settle).await {
                    Ok(true) => {
                        let job = JobContext::from_record(&rec);
                        tracing::info!(mid = %job.mid, attempt = job.attempt, "running handler");
                        let outcome = handler.handle(&job).await;
                        let settled = match outcome {
                            HandlerOutcome::Success => nq.ack(&rec.mid).await,
                            HandlerOutcome::Failure(reason) => {
                                tracing::warn!(mid = %rec.mid, reason = %reason, "handler failed");
                                nq.nack(&rec.mid, &reason).await.map(|_| ())
                            }
                        };
                        if let Err(e) = settled {
                            tracing::error!(mid = %rec.mid, error = %e, "failed to settle job");
                        }
                    }
                    Ok(false) => tracing::debug!(mid = %rec.mid, "lost claim race"),
                    Err(e) => tracing::warn!(mid = %rec.mid, error = %e, "claim attempt failed"),
                }
            });
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(opts.poll_ms)) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    // graceful shutdown: wait for in-flight jobs to finish
    let _drain = semaphore.acquire_many(opts.concurrency as u32).await;
    tracing::info!("worker stopped");
    Ok(())
}
```

`crates/nostr-q-worker/src/handlers.rs`:

```rust
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::{Handler, HandlerOutcome, JobContext};

/// Runs `sh -c <command>` with the payload JSON on stdin and job metadata
/// in NQ_* environment variables. Exit 0 => ack, anything else => nack.
pub struct ExecHandler {
    pub command: String,
}

#[async_trait]
impl Handler for ExecHandler {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&self.command)
            .env("NQ_MID", &job.mid)
            .env("NQ_QUEUE", &job.queue)
            .env("NQ_TRACE", &job.trace_id)
            .env("NQ_ATTEMPT", job.attempt.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(idem) = &job.idem {
            cmd.env("NQ_IDEM", idem);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return HandlerOutcome::Failure(format!("spawn failed: {e}")),
        };
        if let Some(mut stdin) = child.stdin.take() {
            let payload = job.payload.to_string();
            if let Err(e) = stdin.write_all(payload.as_bytes()).await {
                return HandlerOutcome::Failure(format!("stdin write failed: {e}"));
            }
        }
        match child.wait_with_output().await {
            Ok(out) if out.status.success() => HandlerOutcome::Success,
            Ok(out) => HandlerOutcome::Failure(format!(
                "exit {}: {}",
                out.status.code().map(|c| c.to_string()).unwrap_or_else(|| "signal".into()),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => HandlerOutcome::Failure(format!("wait failed: {e}")),
        }
    }
}
```

- [ ] **Step 6: Run tests to verify pass**

Run: `cargo test -p nostr-q-worker`
Expected: 4 passed.

- [ ] **Step 7: Wire the CLI command**

Add to `commands.rs`:

```rust
use nostr_q_worker::{handlers::ExecHandler, run_worker, Handler, WorkerOptions};
use tokio_util::sync::CancellationToken;

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
```

Add to `Cmd` in `main.rs`:

```rust
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
```

Dispatch arm:

```rust
        Cmd::Worker { queue, exec, http, concurrency, lease, max_attempts, heartbeat } => {
            let ctx = Ctx::load(cli.config, cli.json)?;
            commands::worker(&ctx, &queue, exec, http, concurrency, lease, max_attempts, heartbeat).await
        }
```

- [ ] **Step 8: Verify**

Run: `cargo test --workspace`
Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "feat(worker): worker runtime with exec handler, leases, heartbeat, graceful shutdown"
```

---

### Task 17: Worker — HttpHandler + `nq worker --http`

**Files:**
- Modify: `crates/nostr-q-worker/src/handlers.rs`, `crates/nostr-q-cli/src/commands.rs`

**Interfaces:**
- Produces: `HttpHandler` with `HttpHandler::new(url: String) -> HttpHandler` implementing `Handler` — POSTs `{"mid","queue","trace","attempt","idem","payload"}` as JSON; 2xx → Success, non-2xx or transport error → Failure.

- [ ] **Step 1: Write the failing tests** (append to worker tests module in `lib.rs`)

```rust
#[tokio::test]
async fn http_handler_acks_on_2xx_and_nacks_on_500() {
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs/ok"))
        .and(body_partial_json(serde_json::json!({"mid": "m1", "payload": {"n": 1}})))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/jobs/fail"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let ok = crate::handlers::HttpHandler::new(format!("{}/jobs/ok", server.uri()));
    assert!(matches!(ok.handle(&job()).await, HandlerOutcome::Success));

    let fail = crate::handlers::HttpHandler::new(format!("{}/jobs/fail", server.uri()));
    match fail.handle(&job()).await {
        HandlerOutcome::Failure(reason) => assert!(reason.contains("500"), "{reason}"),
        HandlerOutcome::Success => panic!("expected failure"),
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p nostr-q-worker http_handler`
Expected: compile error — `HttpHandler` not defined.

- [ ] **Step 3: Implement** (append to `handlers.rs`)

```rust
/// POSTs job JSON to an HTTP endpoint. 2xx response => ack, else nack.
pub struct HttpHandler {
    url: String,
    client: reqwest::Client,
}

impl HttpHandler {
    pub fn new(url: String) -> Self {
        Self { url, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl Handler for HttpHandler {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome {
        let body = serde_json::json!({
            "mid": job.mid,
            "queue": job.queue,
            "trace": job.trace_id,
            "attempt": job.attempt,
            "idem": job.idem,
            "payload": job.payload,
        });
        match self.client.post(&self.url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => HandlerOutcome::Success,
            Ok(resp) => HandlerOutcome::Failure(format!("http status {}", resp.status())),
            Err(e) => HandlerOutcome::Failure(format!("http request failed: {e}")),
        }
    }
}
```

- [ ] **Step 4: Replace the `--http` bail in CLI `commands.rs`**

```rust
        (None, Some(url)) => Arc::new(nostr_q_worker::handlers::HttpHandler::new(url)),
```

(replaces the `anyhow::bail!("--http is implemented in the next task")` arm)

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test --workspace`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(worker): http handler mapping 2xx to ack"
```

---

### Task 18: CLI — `nq inspect`, `nq trace`, `nq dlq list|retry`

**Files:**
- Modify: `crates/nostr-q-cli/src/commands.rs`, `crates/nostr-q-cli/src/main.rs`

**Interfaces:**
- Consumes: `Store::{stats, trace, trace_id_for_mid, dlq_list, dlq_retry, get_message}` (Task 7).
- Produces: `inspect(ctx, queue)`, `trace_cmd(ctx, id)` (accepts a trace id OR a mid), `dlq_list_cmd(ctx, queue)`, `dlq_retry_cmd(ctx, mid)`.

- [ ] **Step 1: Add to `commands.rs`**

```rust
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
```

- [ ] **Step 2: Wire into `main.rs`**

Add to `Cmd`:

```rust
    /// Show queue depth, in-flight, acked, DLQ counts
    Inspect { queue: String },
    /// Show the lifecycle timeline for a trace id (or message id)
    Trace { id: String },
    /// Dead-letter queue operations
    Dlq {
        #[command(subcommand)]
        cmd: DlqCmd,
    },
```

```rust
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
```

Dispatch arms:

```rust
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
```

- [ ] **Step 3: Verify**

Run: `cargo test --workspace && cargo check -p nq`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(cli): inspect, trace, dlq list/retry"
```

---

### Task 19: Docs, Protocol Spec, End-to-End Verification

**Files:**
- Create: `README.md`, `docs/PROTOCOL.md`

**Interfaces:**
- Consumes: everything. This task validates the SRS §21.1 acceptance criteria end-to-end against a real relay.

- [ ] **Step 1: Write `docs/PROTOCOL.md`**

Content must document (pulling exact values from the code, not from memory):

```markdown
# Nostr-Q Protocol Profile v0.1

## Event kinds
| Kind  | Meaning                | Class      |
|-------|-------------------------|-----------|
| 4620  | message published       | regular   |
| 4621  | message claimed         | regular   |
| 4622  | message acked           | regular   |
| 4623  | message nacked          | regular   |
| 4624  | message dead-lettered   | regular   |
| 24620 | consumer heartbeat      | ephemeral |
| 34620 | queue config snapshot   | addressable (reserved, unused in v0.1) |

## Tags
Message events (4620): `t` (queue/topic, relay-indexed), `q` (queue name),
`mode` (work_queue|pubsub), `mid` (ULID), `trace` (ULID), `attempt`,
optional `idem`.
Lifecycle events (4621-4624): `e` (message event id), `t`, `mid`, `trace`;
claims add `lease_exp` (unix seconds); nack adds `attempt` and `reason`;
dlq adds `reason`.

## Content envelope
{"version":"0.1","content_type":"application/json","body":{},"headers":{},"created_at":"RFC3339"}

## Claim protocol (competing consumers)
1. Worker publishes a 4621 claim referencing the message event, with
   `lease_exp = now + lease_seconds`.
2. Worker waits a settle window (default 750 ms), fetches all 4621 events
   for the message, and computes the winner: the unexpired claim with the
   lowest (created_at, event id hex).
3. Only the winner runs the handler. Expired leases make the message
   claimable again.

## Delivery guarantees
Work queues are at-least-once: duplicate processing is possible (e.g.
relay partitions, settle-window races, lease expiry mid-handler). Use
idempotency keys (`idem` tag) for dedupe. Exactly-once is NOT provided.
```

- [ ] **Step 2: Write `README.md`**

Must include: one-paragraph description; install (`cargo install --path crates/nostr-q-cli`); the quickstart below; a pointer to `docs/PROTOCOL.md`; a "Guarantees and caveats" section stating at-least-once semantics, duplicate possibility, and the private-relay-for-production recommendation (SRS §13.1); embedding example:

```rust
use std::sync::Arc;
use nostr_q::{NostrQ, relay::NostrTransport, store_crate::Store};

let store = Arc::new(Store::open("state.db".as_ref())?);
let keys = nostr::Keys::parse(&std::env::var("NQ_PRIVATE_KEY")?)?;
let transport = Arc::new(NostrTransport::connect(keys.clone(), &["wss://relay.example.com".into()]).await?);
let nq = NostrQ::new(keys, store, transport);
nq.publish("jobs.email", serde_json::json!({"to": "a@b.c"}), None).await?;
```

- [ ] **Step 3: End-to-end verification against a local relay**

Install a local relay if not present (`nak` is a single binary: `go install github.com/fiatjaf/nak@latest` or `brew install nak`), then walk the full MVP acceptance path:

```bash
nak serve &                                    # ws://localhost:10547
export NQ_CONFIG=/tmp/nqe2e/config.toml NQ_STATE=/tmp/nqe2e/state.db
cargo build --workspace
alias nq=./target/debug/nq

nq init
# point key_file somewhere disposable for the test
sed -i '' 's|~/.config/nostr-q/key|/tmp/nqe2e/key|' /tmp/nqe2e/config.toml
nq key generate
nq relay add ws://localhost:10547
nq relay health                                # expect: connected + latency
nq queue create jobs.email --mode work_queue --delivery at_least_once
nq queue create events.user.created --mode pubsub
nq queue list

# pub/sub
nq sub events.user.created &                   # prints incoming events
nq pub events.user.created '{"id":7}'          # subscriber prints it
kill %2

# work queue happy path
nq pub jobs.email '{"to":"user@example.com","template":"welcome"}'
nq worker jobs.email --exec 'cat > /tmp/nqe2e/handled.json' &
sleep 3; kill -INT %2                          # graceful shutdown
cat /tmp/nqe2e/handled.json                    # payload arrived
nq inspect jobs.email                          # acked: 1

# failure -> retry -> DLQ
nq pub jobs.email '{"boom":true}'
nq worker jobs.email --exec 'exit 1' --max-attempts 2 &
sleep 20; kill -INT %2                         # allow retries (5s then 10s backoff)
nq dlq list                                    # shows the message with reason
nq trace $(nq dlq list --json | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["mid"])')
nq dlq retry <mid-from-above>
nq inspect jobs.email
kill %1                                        # stop nak
```

Expected: every step behaves as commented. Fix anything that doesn't before proceeding.

- [ ] **Step 4: Lint pass**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean (fix any findings).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "docs: README quickstart and protocol profile spec"
```

---

## Acceptance Criteria Mapping (SRS §21.1)

| Criterion | Where |
|---|---|
| Initialize local config | Task 13 (`nq init`) |
| Add at least one relay | Task 14 (`nq relay add`) |
| Create work queue + pub/sub topic | Task 15 (`nq queue create`) |
| Publish JSON to a work queue | Tasks 10, 15 (`nq pub`) |
| Worker claims + runs shell handler | Tasks 12, 16 (`nq worker --exec`) |
| Worker acks completion | Tasks 12, 16 |
| Failed handler → nack/retry → DLQ | Tasks 12, 16 (verified E2E in Task 19) |
| Subscribe to pub/sub topic | Tasks 11, 15 (`nq sub`) |
| Inspect queue status | Task 18 (`nq inspect`) |
| Trace message lifecycle by trace id | Tasks 7, 18 (`nq trace`) |
| Relay health from CLI | Tasks 9, 14 (`nq relay health`) |
| State persisted in SQLite | Tasks 5–7 |
| SDK exposes the same primitives as CLI | Tasks 10–12 (CLI is a thin client of `nostr_q::NostrQ`) |
| `nq worker --http` (SRS §14.3) | Task 17 |
| `nq dlq list` / `nq dlq retry` (SRS §14.3) | Task 18 |

## Out of Scope (deferred, per header decisions)

`nq dev`, `nq tui`, encryption implementation (nip04/nip44), `nq config get/set`, `nq queue show/delete`, standalone `nq ack/nack/retry` commands, `nq dlq show/purge`, allow/deny authorization rules, Prometheus/OTel, additional state stores, queue-config publication to relays. Each maps to SRS Phases 4–5 and should get its own plan.



