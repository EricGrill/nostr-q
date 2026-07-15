# Nostr-Q Protocol Profile v0.1

This document describes the wire protocol Nostr-Q uses on top of Nostr:
event kinds, tags, the content envelope, and the claim algorithm that
implements competing-consumer work queues. It is derived directly from
`crates/nostr-q-core/src/protocol.rs` and `crates/nostr-q-core/src/envelope.rs`.

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

All kinds are Nostr custom kinds (`Kind::Custom`), constructed and signed
with the publishing consumer's/producer's keypair.

## Tags

**Message events (kind 4620):**

| Tag | Meaning |
|-----|---------|
| `t` | queue/topic name, relay-indexed via Nostr's standard hashtag filter |
| `q` | queue name (duplicate of `t`, kept for explicitness per SRS) |
| `mode` | `work_queue` or `pubsub` |
| `mid` | message id (ULID) |
| `trace` | trace id (ULID) |
| `attempt` | delivery attempt number |
| `idem` | optional idempotency key |

**Lifecycle events (kinds 4621-4624 — claim, ack, nack, dlq):**

| Tag | Meaning |
|-----|---------|
| `e` | event id of the referenced 4620 message event |
| `t` | queue name |
| `mid` | message id |
| `trace` | trace id |
| `lease_exp` | (claim only) lease expiry, unix seconds |
| `attempt` | (claim) the delivery attempt this claim is for; (nack) delivery attempt number |
| `reason` | (nack, dlq) failure reason string |

Heartbeat events (24620) carry only a `t` tag for the queue being worked.

## Content envelope

The Nostr event `content` field is always this JSON envelope (message
events carry a populated `body`; lifecycle and heartbeat events use an
empty string as content and carry all state in tags):

```json
{
  "version": "0.1",
  "content_type": "application/json",
  "body": {},
  "headers": {},
  "created_at": "RFC3339"
}
```

- `version` — envelope schema version, currently `"0.1"`.
- `content_type` — MIME type of `body`, currently always `application/json`.
- `body` — the caller-supplied JSON payload.
- `headers` — optional string map, empty by default.
- `created_at` — RFC 3339 timestamp set at envelope construction.

## Claim protocol (competing consumers)

Work-queue delivery uses a lease-based, deterministic-tiebreak claim
protocol so that multiple workers subscribed to the same queue do not both
process the same message:

1. A worker that wants to process a pending message publishes a kind 4621
   claim event referencing the message event (`e` tag), with
   `lease_exp = now + lease_seconds`.
2. The worker waits a settle window (default 750 ms) to let other workers'
   competing claims propagate through the relay(s), then fetches all 4621
   claim events for that message.
3. The winner is computed deterministically: among claims whose
   `lease_exp` has not yet passed, the one with the lowest
   `(created_at, claim_event_id_hex)` tuple wins. This tiebreak is
   reproducible by any observer with the same claim set, so no
   coordination beyond the relay's event set is required. Claims declare
   the attempt they are for; a nack at attempt N releases all claims for
   earlier attempts, so retries are not blocked by their own stale claims.
4. Only the winner runs the message handler. If the winner's handler does
   not ack or nack before `lease_exp`, the lease is considered expired and
   the message becomes claimable again (any consumer, including the
   original claimant, may re-claim it).

## Delivery guarantees

Work queues are **at-least-once**: duplicate processing is possible. Known
causes include relay partitions (a worker's claim doesn't reach all
relays), settle-window races (two claims resolve to the same winner
locally before converging), and lease expiry mid-handler (the handler
finishes after the lease was already considered expired and the message
reclaimed).

Use idempotency keys (the `idem` tag on message events, set via the
`--idem`/`idem` publish parameter) so consumers can deduplicate on their
side. **Exactly-once delivery is NOT provided** by this protocol or by
Nostr-Q's SDK/CLI.

Pub/sub topics (`mode = pubsub`) are best-effort fanout: there is no
claim/lease/ack cycle, and delivery depends entirely on subscriber uptime
and relay retention at subscribe time.
