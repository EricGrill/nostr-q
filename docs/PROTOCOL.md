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
| 4625  | RPC reply               | regular   |
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
| `nbf` | optional not-before, unix seconds — message is not claimable before this time (delayed delivery) |
| `exp` | optional expiry, unix seconds (SRS §11.3 reserved tag) — message must not be claimed once this time has passed (TTL) |
| `reply` | optional — present only on RPC requests (see "Request/reply (RPC)" below); value is the requester's pubkey (hex) |

**Reply events (kind 4625):**

| Tag | Meaning |
|-----|---------|
| `e` | event id of the request's 4620 message event (via `Tag::event`) |
| `p` | the requester's pubkey (via `Tag::public_key`) — addresses the reply back to whoever set `reply` on the request |
| `parent` | the request's `mid`, for human-readable correlation alongside `e` |

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
`nostr-q dlq retry`, which requeues the message locally and grants it a fresh
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

This rule also has to hold for a node that never nacked the message
itself and only *observed* someone else's dlq event. When that happens,
`try_claim` heals the local `attempts` counter up to the observed dlq
event's `attempt` generation before marking the row dead
(`Store::move_to_dlq_at`), the same MAX-healing pattern `mark_claimed`
uses for claims. Without this, a node whose local `attempts` was still 0
(because it never nacked) would set `attempt_floor = attempts = 0` on its
next `dlq retry`, and the very same historical dlq event — whose
`attempt` tag is still greater than that stale floor — would immediately
re-trip the terminal check above on the next claim survey, silently
undoing the retry. Healing `attempts` first means the floor `dlq_retry`
sets is high enough that the historical event is no longer terminal.

### Protocol versioning note

Dead-letter events (kind 4624) carry an `attempt` tag recording the global
attempt/retry generation at dead-letter time (see the DLQ-terminal rule
above). `event_attempt` (`crates/nostr-q-core/src/protocol.rs`) parses this
tag defensively: an older Nostr-Q implementation, or any other
implementation that omits the `attempt` tag on a 4624 event, is parsed as
`attempt = 0`. Since the DLQ-terminal check requires `attempt >
attempt_floor` (and `attempt_floor` starts at 0), a tagless dlq event is
never terminal on a node whose row has never been retried — it is treated
as informational rather than authoritative. Implementations that want their
dead-letter events to be reliably terminal for other Nostr-Q nodes must set
the `attempt` tag.

## Delayed delivery and TTL

Message events (kind 4620) may carry two optional scheduling tags,
`nbf` (not-before) and `exp` (expiry), both unix seconds:

- **`nbf` — delayed delivery.** A message with an `nbf` tag is not
  claimable until that time. Locally this is implemented by seeding the
  message row's `visible_at` from `nbf` at publish/ingest time (the same
  `visible_at` column that already defers retries after a nack); `claimable`
  already filters on `visible_at <= now`, so no new claimability logic was
  needed for this half. Set it via `NostrQ::publish_opts` (`PublishOptions
  { not_before: Some(ts), .. }`) or `nostr-q pub <queue> <payload> --delay
  <dur>` / `--not-before <rfc3339>` (mutually exclusive with each other).
- **`exp` — TTL / expiry.** A message with an `exp` tag must never be
  claimed once that time has passed, whether or not it was ever claimed
  before. `claimable` excludes any row with `expires_at <= now`. Separately,
  `Store::expire_due(queue, now)` finds `pending`/`claimed` rows past their
  `expires_at`, moves them to a new terminal status, `'expired'`, and
  returns their mids. `NostrQ::try_claim` calls this (via an internal
  helper) before doing any relay I/O when the surveyed row is already past
  its TTL, and `NostrQ::sweep_expired(queue)` — called once per poll cycle
  by `run_worker` — sweeps the whole queue so a message that is *never*
  claimed (idle queue, all workers busy) still expires on schedule instead
  of sitting `pending` forever. Set it via `PublishOptions { expires_at:
  Some(ts), .. }` or `nostr-q pub <queue> <payload> --ttl <dur>` /
  `--expires <rfc3339>` (mutually exclusive with each other).

**Expired messages go to `'expired'`, not the DLQ.** The dead-letter queue
(`dlq`/`status = 'dead'`) represents *processing failures* — a handler that
kept nacking until it exhausted its retry budget. Expiry is a scheduling
outcome decided before any handler ever ran (or without one ever running at
all), which is a different condition an operator needs to distinguish from
"a handler kept failing." Expired messages are therefore a separate
terminal status, visible in `QueueStats.expired` / `nostr-q inspect`
(human output: `expired: N`), and are not requeued by `dlq retry` — there is
currently no CLI verb to resurrect an expired message; republish a fresh
one instead.

## Request/reply (RPC)

An RPC call is an ordinary work-queue request that additionally asks for a
single correlated reply. There is no separate claim/lease machinery for
RPC — it reuses the exact same competing-consumer claim protocol described
above, so exactly one responder processes a given request just like any
other work-queue message.

**Flow:**

1. The caller (`NostrQ::call(queue, body, timeout)`) publishes a normal
   kind 4620 message, additionally setting the `reply` tag to its own
   pubkey (hex). This marks the message as an RPC request; `NqMessage`
   exposes this as `reply_to: Option<String>`.
2. The caller opens a subscription filtered to
   `kind = 4625 AND #e = <request's event id>` immediately after
   publishing, and also issues a direct `query` against the same filter
   (covering a reply that a real relay delivered in the gap between
   publish and subscribe). It then waits, bounded by `timeout`, for the
   first matching kind 4625 event and returns its envelope `body`.
3. Some responder claims the request through the normal claim protocol
   (`try_claim`) — the same one-winner-per-message semantics as any other
   work-queue message apply, so only the claim winner ever processes a
   given request. `nostr-q-worker::run_worker` is the reference responder:
   its `Handler::handle` returns `HandlerOutcome::Success { reply: Some(body)
   }` to produce a reply (or `Success { reply: None }` for an ordinary,
   non-reply-bearing success). `ExecHandler` treats non-empty JSON on
   stdout as the reply body; `HttpHandler` treats a non-empty JSON 2xx
   response body the same way.
4. After a successful handler run that produced a reply body, the
   responder publishes a kind 4625 event (`build_reply_event`) — `e`
   referencing the request event, `p` addressing the requester pubkey from
   the request's `reply` tag, `parent` carrying the request's `mid` — and
   only then acks the request. If the reply publish itself fails, the
   responder logs it and acks anyway: the request WAS processed correctly,
   and a lost reply shouldn't cause the request to be redelivered.

**Duplicate replies.** Because request delivery is at-least-once (see
"Delivery guarantees" below) and because nothing prevents a second,
independent reply from being published for the same request (e.g. a
retried delivery, or a misbehaving responder), a caller may observe more
than one kind 4625 event correlated to its request. `NostrQ::call` takes
only the *first* matching reply and ignores any later ones — callers that
need stronger de-duplication should include an idempotency marker in the
request body itself and check it on the response side.

**One responder, by claim.** Because request processing goes through the
same claim protocol as any other work-queue message, only the claim winner
runs the handler and (potentially) publishes a reply — other workers
subscribed to the same queue back off exactly as they would for a non-RPC
message. This means an RPC call has the same at-least-once processing
characteristics as the underlying work queue: a claim winner whose reply
event never reaches the relay (network partition, crash after ack) leaves
the caller to time out, even though the request itself was processed
exactly once from the responder's point of view.

## Local state is per-node

Every Nostr-Q node — producer, worker, or CLI invocation — keeps its own
SQLite state (`Store`). There is no shared or global database, and no node
ever ingests or replays another node's full history. This is by design
(SRS §10: state is local operational bookkeeping, not a source of truth for
the queue), but it has consequences operators must understand:

- **A node's rows reflect only what that node has published, ingested, or
  observed.** `insert_message` runs when a node either publishes a message
  itself or has an active subscription/worker ingest loop (`spawn_ingest`)
  running against a queue. A node that has never subscribed to a queue has
  no rows for it at all, regardless of how much traffic has flowed through
  that queue on the relay.
- **Ack/DLQ lifecycle events are consumed opportunistically, not globally
  ingested.** A node only learns that some *other* node acked or
  dead-lettered a message when its own `try_claim` terminal-state check
  (phase 1, above) happens to query the relay for that specific message —
  which only happens while the node still has a `pending`/`claimed` local
  row for it and is actively polling. A pure producer (or a worker that
  never lost a claim race for that message) never runs this check, and so
  never local-syncs the outcome.
- **A producer-only node keeps rows `pending` indefinitely.** If a node
  only ever calls `publish` and never runs a worker/ingest loop against
  that queue, its local rows for the messages it published stay `pending`
  forever in its own database — even after some other node's worker acks
  or dead-letters them on the relay. This is expected, not a bug: the
  producer's local state was never wired to observe that lifecycle.
- **`nostr-q inspect` stats are that node's local view, not global truth.**
  Depth, in-flight, and DLQ counts reported by `inspect` (and by
  `dlq list`/`dlq retry`) are computed entirely from the invoking node's
  own SQLite file. Two nodes pointed at the same queue on the same relay(s)
  can — and routinely will — report different numbers. Neither is "wrong";
  each is an accurate reflection of that node's own participation.

**Operationally**, this means:

- Don't use one node's `inspect` output as a global dashboard for a queue
  unless that node runs a worker/ingest loop against every queue you care
  about and you accept eventual (not real-time) convergence with other
  nodes' views.
- If you need a global view, aggregate relay-level truth directly (query
  the relay for all lifecycle events on a queue) rather than trusting any
  single node's local database.
- `dlq retry` only requeues the row in the *retrying node's* local
  database. See the "DLQ-terminal rule" section above — including how a
  node that only *observed* a remote dlq event (rather than dead-lettering
  the message itself) still gets a correctly-healed retry floor.

## One keypair per worker instance

> **Warning:** Never share `NOSTR_Q_PRIVATE_KEY` (or a `key_file`) across
> more than one running worker instance.

Claim-winner identity in the protocol is decided by **claimer pubkey**
(see "Claim protocol" above: "Winner identity ... is compared by claimer
pubkey, not claim event id"). If two worker processes are configured with
the same private key, every claim event either of them publishes carries
the identical pubkey, so from the relay's point of view they are
indistinguishable — and from each worker's own point of view, *any* claim
tagged with "our" pubkey looks like a claim *we* won, including the one
the other instance just published. Both instances will conclude they won
the same claim and will both run the handler, duplicating all processing
for every message the queue delivers. This defeats the entire
competing-consumer mechanism and is not caught by any retry, lease, or
ack/nack logic — it looks like normal single-winner claiming from each
instance's own perspective.

Give each worker **instance** (or, at minimum, each machine) its own
keypair (`nostr-q key generate`). It is fine — expected, even — for a
producer and its downstream workers to use different keys, and for
multiple distinct workers on the same queue to use different keys; that is
what makes them "competing consumers" in the first place.

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
