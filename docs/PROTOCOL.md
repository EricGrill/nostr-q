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
| `attempt` | (claim) the delivery attempt this claim is for; (nack, dlq) the global attempt/retry generation observed at publish time |
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
process the same message. A worker attempting to claim a message
(`try_claim`) runs three phases, in order:

1. **Terminal check.** Before publishing anything, the worker queries the
   relay for kind 4622 (ack) or kind 4624 (dlq) events referencing the
   message. If an ack event exists, the message is done: the worker marks
   its local row acked and stops. If a dlq event exists, the worker checks
   whether that dlq event's `attempt` tag exceeds the worker's local
   `attempt_floor` (see "DLQ-terminal rule" below); if so, the message is
   genuinely dead and the worker marks its local row dead-lettered and
   stops. Either way the worker publishes no claim this round. This is
   what lets a worker that keeps losing the claim race for a message some
   other worker already finished converge instead of livelocking: without
   it, a permanent loser never learns the message is done, so its local
   row stays `pending` and it republishes a fresh claim on every poll
   forever.
2. **Survey.** If the message is not terminal, the worker queries existing
   4621 claim events and 4623 nack events for the message *before*
   publishing anything, and computes the deterministic winner (below)
   over what it currently sees. If a live winner already exists:
   - If the winner's claimer pubkey is the worker's own, the worker
     already holds the win — it records `claimed` locally and returns
     without publishing a duplicate claim.
   - If the winner is a different pubkey, the worker backs off and
     publishes nothing this round. It will re-poll after the foreign
     lease expires, or after it observes that worker's eventual ack/dlq
     event via phase 1.
3. **Contend.** Only if the survey found no live winner (no claims, or all
   expired/stale relative to the current attempt generation) does the
   worker publish its own kind 4621 claim event referencing the message
   event (`e` tag), with `lease_exp = now + lease_seconds` and
   `attempt = max(local attempts, highest attempt seen in nacks)`. It then
   waits a settle window (default 750 ms) to let other workers' competing
   claims propagate through the relay(s), and re-fetches all 4621 claim
   events (and 4623 nacks, to catch a newer generation that landed during
   the settle window) for that message to compute the winner.

The winner is computed deterministically in both the survey and contend
phases: among claims whose `lease_exp` has not yet passed and whose
`attempt` is at least the highest attempt seen in nacks, the one with the
lowest `(created_at, claim_event_id_hex)` tuple wins. This tiebreak is
reproducible by any observer with the same claim set, so no coordination
beyond the relay's event set is required. Claims declare the attempt they
are for; a nack at attempt N releases all claims for earlier attempts, so
retries are not blocked by their own stale claims.

Winner identity — whether "we" won — is compared by **claimer pubkey**,
not claim event id: a worker's own older (but still unexpired,
still-current-generation) claim is still a win for that worker, so it is
never re-published redundantly.

Only the winner runs the message handler. If the winner's handler does
not ack or nack before `lease_exp`, the lease is considered expired and
the message becomes claimable again (any consumer, including the
original claimant, may re-claim it).

### DLQ-terminal rule

A dead-letter (kind 4624) event on the relay is not, by itself, permanent
proof that a message should never be reclaimed: an operator can run
`nq dlq retry`, which requeues the message locally and grants it a fresh
attempt budget (`attempt_floor = attempts` at retry time — see
`Store::dlq_retry`), while the old dlq event remains on the relay forever
(events are never deleted). If a worker's terminal check treated *any*
dlq event referencing the message as permanently final, a locally-retried
message could never be reclaimed by a worker that observes the stale dlq
event.

The rule: a dlq event is terminal for a given local row only if
`event_attempt(dlq_event) > rec.attempt_floor`, where `event_attempt`
reads the dlq event's `attempt` tag (the global attempt/retry generation
observed at dead-letter time — see `nack`'s DLQ branch, which tags the
dlq event with the same `attempts` value it just wrote to the local dlq
record). Since `dlq_retry` sets `attempt_floor = attempts` at the moment
of retry, and `attempts` never resets, an old (pre-retry) dlq event's
`attempt` tag is always `<= attempt_floor` after the retry — not
terminal — while a genuinely-final dlq event (one dead-lettered *after*
the current floor was set) always carries `attempt > attempt_floor` —
terminal. A worker that never observed the retry (`attempt_floor == 0`)
correctly treats any real dlq event (`attempt >= 1`) as terminal.

## Delivery guarantees

Work queues are **at-least-once**: duplicate processing is possible. Known
causes include relay partitions (a worker's claim doesn't reach all
relays), settle-window races (two claims resolve to the same winner
locally before converging), lease expiry mid-handler (the handler
finishes after the lease was already considered expired and the message
reclaimed), and losing workers back off without publishing — a worker
whose survey phase sees a live foreign claim or an observed terminal
event does not contend, which avoids claim-spam but means the losing
worker's own state converges only once it re-polls and observes the
winner's ack/dlq event or the winner's lease expires.

Use idempotency keys (the `idem` tag on message events, set via the
`--idem`/`idem` publish parameter) so consumers can deduplicate on their
side. **Exactly-once delivery is NOT provided** by this protocol or by
Nostr-Q's SDK/CLI.

Pub/sub topics (`mode = pubsub`) are best-effort fanout: there is no
claim/lease/ack cycle, and delivery depends entirely on subscriber uptime
and relay retention at subscribe time.
