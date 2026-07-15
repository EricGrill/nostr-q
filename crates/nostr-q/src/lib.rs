use std::sync::Arc;

use anyhow::{anyhow, Result};
use nostr::{EventId, Filter, Keys, Kind};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use nostr_q_core::envelope::Envelope;
use nostr_q_core::ids::{new_mid, new_trace_id};
use nostr_q_core::protocol::{
    build_ack_event, build_claim_event, build_dlq_event, build_message_event, build_nack_event,
    claim_winner, event_attempt, parse_claim_event, parse_message_event, ClaimInfo, NqMessage,
    KIND_ACK, KIND_CLAIM, KIND_DLQ, KIND_MESSAGE, KIND_NACK,
};
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

pub struct NostrQ {
    keys: Keys,
    store: Arc<Store>,
    transport: Arc<dyn Transport>,
}

impl NostrQ {
    pub fn new(keys: Keys, store: Arc<Store>, transport: Arc<dyn Transport>) -> Self {
        Self {
            keys,
            store,
            transport,
        }
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
        let config = self.store.get_queue(queue)?.ok_or_else(|| {
            anyhow!("unknown queue '{queue}' — create it with `nq queue create {queue}`")
        })?;
        // Idempotent publish: a repeat (queue, idem) returns the original
        // receipt without re-broadcasting.
        if let Some(key) = &idem {
            if let Some(existing) = self.store.find_by_idem(queue, key)? {
                return Ok(PublishReceipt {
                    mid: existing.mid,
                    trace_id: existing.trace_id,
                    event_id: existing.event_id,
                });
            }
        }
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
            attempt_floor: 0,
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

    fn message_filter(topic: &str) -> Filter {
        Filter::new()
            .kind(Kind::Custom(KIND_MESSAGE))
            .hashtag(topic)
    }

    // `try_claim` runs in three phases:
    //
    // 1. Terminal check — before doing anything else, ask the relay whether
    //    this message already has an ack or dlq event. Without this, a
    //    worker that keeps losing the claim race for a message some other
    //    worker already finished would never learn that fact: its local row
    //    stays `pending` forever, and it republishes a fresh claim on every
    //    poll (claim-spam) without ever converging. This phase is what lets
    //    losing workers stop.
    // 2. Survey — query existing claims (and nacks, to know the current
    //    retry generation) BEFORE publishing anything. If a winner already
    //    exists, either we already hold it (recognize the win, no
    //    duplicate claim published) or someone else holds it (back off
    //    without publishing — no claim-spam while a foreign lease is
    //    live).
    // 3. Contend — only if the survey found no live winner do we publish a
    //    claim of our own and settle, exactly as before.
    //
    // Winner identity is compared by claimer PUBKEY, not claim event id:
    // a worker's own older (but still unexpired, still-current-generation)
    // claim is still a win for that worker — there is no reason to publish
    // a second, redundant claim for a message it already holds.
    pub async fn try_claim(
        &self,
        rec: &MessageRecord,
        lease_seconds: u64,
        settle_ms: u64,
    ) -> Result<bool> {
        let now = Self::now();
        let lease_expires_at = now + lease_seconds as i64;
        let message_event_id = EventId::from_hex(&rec.event_id)?;
        let our_pubkey = self.keys.public_key();
        let nack_filter = Filter::new()
            .kind(Kind::Custom(KIND_NACK))
            .event(message_event_id);
        let claim_filter = Filter::new()
            .kind(Kind::Custom(KIND_CLAIM))
            .event(message_event_id);

        // ---- Phase 1: terminal-state check ---------------------------
        let terminal_filter = Filter::new()
            .kinds([Kind::Custom(KIND_ACK), Kind::Custom(KIND_DLQ)])
            .event(message_event_id);
        let terminal_events = self.transport.query(terminal_filter).await?;
        if terminal_events
            .iter()
            .any(|e| e.kind == Kind::Custom(KIND_ACK))
        {
            self.store.mark_acked(&rec.mid)?;
            self.store
                .record_lifecycle(&rec.mid, &rec.trace_id, "acked", "observed remote ack")?;
            return Ok(false);
        }
        // A remote DLQ event is only terminal for THIS row if its
        // generation postdates our local retry floor. `nack`'s DLQ branch
        // tags each dlq event's `attempt` with the attempts-count observed
        // at dead-letter time, and `Store::dlq_retry` sets
        // `attempt_floor = attempts` at retry time. So an old DLQ event
        // (from before this row was locally requeued) always carries
        // attempt <= attempt_floor, while a genuinely-final DLQ event
        // always carries attempt > attempt_floor (nothing but
        // `dlq_retry` moves the floor forward). This lets a
        // locally-requeued message keep contending even though its old
        // DLQ event is still sitting on the relay forever, while a worker
        // that never saw the retry correctly treats the message as dead.
        let max_remote_dlq_attempt = terminal_events
            .iter()
            .filter(|e| e.kind == Kind::Custom(KIND_DLQ))
            .map(event_attempt)
            .max();
        if let Some(dlq_attempt) = max_remote_dlq_attempt {
            if dlq_attempt > rec.attempt_floor {
                self.store
                    .move_to_dlq(&rec.mid, "dead-lettered by remote worker")?;
                self.store.record_lifecycle(
                    &rec.mid,
                    &rec.trace_id,
                    "dead_lettered",
                    "observed remote dlq",
                )?;
                return Ok(false);
            }
        }

        // ---- Phase 2: survey existing claims before contending -------
        // Local attempt counters are per-worker: `rec.attempts` only
        // advances when THIS worker nacks. If we tagged our claim with the
        // local count alone, a worker that never nacked would keep
        // publishing claims at attempt 0, which `claim_winner` permanently
        // filters out once any other worker's nack has pushed the
        // generation forward. Tag with the max of local and globally
        // observed attempt instead, so a worker that never nacked can
        // still reclaim after a foreign nack.
        let observed_attempt = self
            .transport
            .query(nack_filter.clone())
            .await?
            .iter()
            .map(event_attempt)
            .max()
            .unwrap_or(0);
        let existing_claims: Vec<ClaimInfo> = self
            .transport
            .query(claim_filter.clone())
            .await?
            .iter()
            .filter_map(|e| parse_claim_event(e).ok())
            .collect();
        if let Some(winner) = claim_winner(&existing_claims, observed_attempt, now) {
            if winner.claimer == our_pubkey {
                let consumer = our_pubkey.to_hex();
                self.store.mark_claimed(
                    &rec.mid,
                    &consumer,
                    winner.lease_expires_at,
                    winner.attempt,
                )?;
                self.store
                    .record_lifecycle(&rec.mid, &rec.trace_id, "claimed", &consumer)?;
                return Ok(true);
            }
            // Someone else holds the winning claim: don't publish a
            // competing claim. We'll re-poll after their lease expires, or
            // after we observe their eventual ack/dlq event.
            return Ok(false);
        }

        // ---- Phase 3: contend ------------------------------------------
        // No live winner exists yet (no claims, or all expired/stale) —
        // publish our own and settle.
        let claim_attempt = rec.attempts.max(observed_attempt);
        let claim = build_claim_event(
            &self.keys,
            message_event_id,
            &rec.queue,
            &rec.mid,
            &rec.trace_id,
            lease_expires_at,
            claim_attempt,
        )?;
        self.transport.publish(claim).await?;
        tokio::time::sleep(std::time::Duration::from_millis(settle_ms)).await;

        let claims: Vec<ClaimInfo> = self
            .transport
            .query(claim_filter)
            .await?
            .iter()
            .filter_map(|e| parse_claim_event(e).ok())
            .collect();
        // Re-query nacks post-settle: if a newer nack landed during the
        // settle window, our claim is stale relative to the new generation
        // and we correctly lose this round — the next poll retries with the
        // higher observed attempt.
        let post_settle_max_attempt = self
            .transport
            .query(nack_filter)
            .await?
            .iter()
            .map(event_attempt)
            .max()
            .unwrap_or(0);
        let min_attempt = observed_attempt.max(post_settle_max_attempt);
        let we_won = claim_winner(&claims, min_attempt, now)
            .map(|w| w.claimer == our_pubkey)
            .unwrap_or(false);
        if we_won {
            let consumer = our_pubkey.to_hex();
            self.store
                .mark_claimed(&rec.mid, &consumer, lease_expires_at, claim_attempt)?;
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
        self.store
            .record_lifecycle(mid, &rec.trace_id, "acked", "")?;
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
            &self.keys,
            event_id,
            &rec.queue,
            mid,
            &rec.trace_id,
            attempts,
            reason,
        )?;
        self.transport.publish(event).await?;
        self.store
            .record_lifecycle(mid, &rec.trace_id, "nacked", reason)?;

        // `attempts` is the global retry generation and stays monotonic
        // across a DLQ retry (see `Store::dlq_retry`), so the DLQ budget is
        // measured relative to `rec.attempt_floor` — the generation at which
        // the last retry granted a fresh budget — rather than against the
        // raw counter.
        let attempts_since_floor = attempts.saturating_sub(rec.attempt_floor);
        if attempts_since_floor >= config.max_attempts {
            let dlq = build_dlq_event(
                &self.keys,
                event_id,
                &rec.queue,
                mid,
                &rec.trace_id,
                attempts,
                reason,
            )?;
            self.transport.publish(dlq).await?;
            self.store.move_to_dlq(mid, reason)?;
            self.store
                .record_lifecycle(mid, &rec.trace_id, "dead_lettered", reason)?;
            Ok(NackOutcome::DeadLettered)
        } else {
            let visible_at =
                Self::now() + backoff_secs(config.retry_base_seconds, attempts_since_floor) as i64;
            self.store.mark_pending(mid, visible_at)?;
            self.store.record_lifecycle(
                mid,
                &rec.trace_id,
                "retry_scheduled",
                &format!("attempt {attempts}, visible_at {visible_at}"),
            )?;
            Ok(NackOutcome::Retry {
                attempt: attempts,
                visible_at,
            })
        }
    }

    pub async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<NqMessage>> {
        let mut events = self
            .transport
            .subscribe(Self::message_filter(topic))
            .await?;
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

    pub async fn heartbeat(&self, queue: &str) -> Result<()> {
        let event = nostr_q_core::protocol::build_heartbeat_event(&self.keys, queue)?;
        self.transport.publish(event).await?;
        Ok(())
    }

    pub async fn spawn_ingest(&self, queue: &str) -> Result<JoinHandle<()>> {
        let mut events = self
            .transport
            .subscribe(Self::message_filter(queue))
            .await?;
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
                    attempt_floor: 0,
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
        store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();
        store
            .upsert_queue(&QueueConfig::pubsub("events.user.created"))
            .unwrap();
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
            .query(
                nostr::Filter::new()
                    .kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_MESSAGE)),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        // local state recorded, pending, with trace
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(rec.status, "pending");
        assert_eq!(rec.queue, "jobs.email");
        assert_eq!(rec.trace_id, receipt.trace_id);
        assert_eq!(
            nq.store().trace(&receipt.trace_id).unwrap()[0].kind,
            "published"
        );
    }

    #[tokio::test]
    async fn publish_to_unknown_queue_errors() {
        let (nq, _) = setup();
        assert!(nq.publish("nope", json!({}), None).await.is_err());
    }

    #[tokio::test]
    async fn duplicate_idem_returns_existing_receipt_without_rebroadcast() {
        let (nq, transport) = setup();
        let first = nq
            .publish("jobs.email", json!({"n": 1}), Some("order-1".into()))
            .await
            .unwrap();
        let second = nq
            .publish("jobs.email", json!({"n": 2}), Some("order-1".into()))
            .await
            .unwrap();
        assert_eq!(second.mid, first.mid);
        assert_eq!(second.event_id, first.event_id);
        let events = transport
            .query(
                nostr::Filter::new()
                    .kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_MESSAGE)),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "duplicate idem must not re-broadcast");
    }

    #[tokio::test]
    async fn subscribe_delivers_pubsub_messages() {
        let (nq, _) = setup();
        let mut rx = nq.subscribe("events.user.created").await.unwrap();
        nq.publish("events.user.created", json!({"id": 7}), None)
            .await
            .unwrap();
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
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let worker = mk(transport.clone());

        let _ingest = worker.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

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

    #[tokio::test]
    async fn claim_ack_happy_path() {
        let (nq, transport) = setup();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();

        assert!(nq.try_claim(&rec, 60, 10).await.unwrap());
        assert_eq!(
            nq.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "claimed"
        );

        nq.ack(&receipt.mid).await.unwrap();
        assert_eq!(
            nq.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "acked"
        );

        // claim + ack events were published
        let claims = transport
            .query(
                nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_CLAIM)),
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let kinds: Vec<String> = nq
            .store()
            .trace(&receipt.trace_id)
            .unwrap()
            .iter()
            .map(|l| l.kind.clone())
            .collect();
        assert_eq!(kinds, vec!["published", "claimed", "acked"]);
    }

    #[tokio::test]
    async fn competing_claims_only_one_winner() {
        // two workers, shared transport, same message
        let transport = Arc::new(MockTransport::new());
        let mk = |t: Arc<MockTransport>| {
            let store = Arc::new(Store::open_in_memory().unwrap());
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let w1 = mk(transport.clone());
        let w2 = mk(transport.clone());
        let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
        let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        // wait for both ingests
        for w in [&w1, &w2] {
            for _ in 0..40 {
                if w.store().get_message(&receipt.mid).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        let (a, b) = tokio::join!(w1.try_claim(&r1, 60, 300), w2.try_claim(&r2, 60, 300));
        let wins = [a.unwrap(), b.unwrap()];
        assert_eq!(
            wins.iter().filter(|w| **w).count(),
            1,
            "exactly one worker must win the claim"
        );
    }

    #[tokio::test]
    async fn other_worker_can_reclaim_after_foreign_nack() {
        let transport = Arc::new(MockTransport::new());
        let mk = |t: Arc<MockTransport>| {
            let store = Arc::new(Store::open_in_memory().unwrap());
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let w1 = mk(transport.clone());
        let w2 = mk(transport.clone());
        let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
        let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        for w in [&w1, &w2] {
            for _ in 0..40 {
                if w.store().get_message(&receipt.mid).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // w1 claims and nacks; its local attempts becomes 1, w2's stays 0
        let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(w1.try_claim(&r1, 60, 10).await.unwrap());
        matches!(
            w1.nack(&receipt.mid, "boom").await.unwrap(),
            NackOutcome::Retry { .. }
        );

        // w2 never nacked (local attempts 0) but must still be able to reclaim
        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(r2.attempts, 0);
        assert!(
            w2.try_claim(&r2, 60, 10).await.unwrap(),
            "a worker that never nacked must not be locked out after a foreign nack"
        );
    }

    #[tokio::test]
    async fn takeover_worker_retry_wins_after_its_own_nack() {
        let transport = Arc::new(MockTransport::new());
        let mk = |t: Arc<MockTransport>| {
            let store = Arc::new(Store::open_in_memory().unwrap());
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let w1 = mk(transport.clone());
        let w2 = mk(transport.clone());
        let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
        let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        for w in [&w1, &w2] {
            for _ in 0..40 {
                if w.store().get_message(&receipt.mid).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // generation 0: w1 claims and nacks (global generation -> 1)
        let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(w1.try_claim(&r1, 60, 10).await.unwrap());
        w1.nack(&receipt.mid, "boom").await.unwrap();

        // generation 1: w2 takes over, then nacks (its healed counter -> 2)
        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(w2.try_claim(&r2, 60, 10).await.unwrap());
        assert_eq!(
            w2.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .attempts,
            1,
            "claim must heal the local counter"
        );
        w2.nack(&receipt.mid, "boom again").await.unwrap();

        // generation 2: w2's own retry must beat its stale generation-1 claim
        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(
            w2.try_claim(&r2, 60, 10).await.unwrap(),
            "takeover worker's retry must not lose to its own stale claim"
        );
    }

    #[tokio::test]
    async fn nack_retries_then_dead_letters() {
        let (nq, transport) = setup();
        // tighten policy for the test
        let mut q = nq.store().get_queue("jobs.email").unwrap().unwrap();
        q.max_attempts = 2;
        nq.store().upsert_queue(&q).unwrap();

        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        let out1 = nq.nack(&receipt.mid, "boom").await.unwrap();
        match out1 {
            NackOutcome::Retry {
                attempt,
                visible_at,
            } => {
                assert_eq!(attempt, 1);
                assert!(visible_at > chrono::Utc::now().timestamp());
            }
            other => panic!("expected retry, got {other:?}"),
        }
        assert_eq!(
            nq.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );

        let out2 = nq.nack(&receipt.mid, "boom again").await.unwrap();
        assert_eq!(out2, NackOutcome::DeadLettered);
        assert_eq!(
            nq.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "dead"
        );
        assert_eq!(nq.store().dlq_list(Some("jobs.email")).unwrap().len(), 1);

        let dlq_events = transport
            .query(nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_DLQ)))
            .await
            .unwrap();
        assert_eq!(dlq_events.len(), 1);
    }

    #[tokio::test]
    async fn retry_claim_wins_immediately_after_nack() {
        let (nq, _) = setup();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(nq.try_claim(&rec, 60, 10).await.unwrap());
        // handler fails -> nack schedules a retry
        match nq.nack(&receipt.mid, "boom").await.unwrap() {
            NackOutcome::Retry { .. } => {}
            other => panic!("expected retry, got {other:?}"),
        }
        // the retry must win the claim race NOW, not after the 60s lease expires
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(rec.attempts, 1);
        assert!(
            nq.try_claim(&rec, 60, 10).await.unwrap(),
            "retry claim must not lose to its own stale pre-nack claim"
        );
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(backoff_secs(5, 1), 5);
        assert_eq!(backoff_secs(5, 2), 10);
        assert_eq!(backoff_secs(5, 3), 20);
        assert_eq!(backoff_secs(5, 30), 3600); // capped
    }

    #[tokio::test]
    async fn dlq_retry_grants_fresh_attempt_budget() {
        let (nq, _) = setup();
        let mut q = nq.store().get_queue("jobs.email").unwrap().unwrap();
        q.max_attempts = 2;
        nq.store().upsert_queue(&q).unwrap();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        // drive to DLQ: two failures
        nq.nack(&receipt.mid, "f1").await.unwrap();
        assert_eq!(
            nq.nack(&receipt.mid, "f2").await.unwrap(),
            NackOutcome::DeadLettered
        );

        // operator retries: fresh budget of 2 despite historical nacks on the relay
        nq.store().dlq_retry(&receipt.mid).unwrap();
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(
            nq.try_claim(&rec, 60, 10).await.unwrap(),
            "post-retry claim must win"
        );
        match nq.nack(&receipt.mid, "f3").await.unwrap() {
            NackOutcome::Retry { .. } => {}
            other => panic!("first post-retry failure must be Retry, got {other:?}"),
        }
        assert_eq!(
            nq.nack(&receipt.mid, "f4").await.unwrap(),
            NackOutcome::DeadLettered,
            "budget exhausts after max_attempts relative failures"
        );
    }

    #[tokio::test]
    async fn loser_observes_ack_and_stops_contending() {
        let transport = Arc::new(MockTransport::new());
        let mk = |t: Arc<MockTransport>| {
            let store = Arc::new(Store::open_in_memory().unwrap());
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let w1 = mk(transport.clone());
        let w2 = mk(transport.clone());
        let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
        let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        for w in [&w1, &w2] {
            for _ in 0..40 {
                if w.store().get_message(&receipt.mid).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(w1.try_claim(&r1, 60, 10).await.unwrap());
        w1.ack(&receipt.mid).await.unwrap();

        // w2 must observe the ack, mark its local row acked, and not claim
        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(!w2.try_claim(&r2, 60, 10).await.unwrap());
        assert_eq!(
            w2.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "acked"
        );
        // and it must not have published any claim of its own
        let claims = transport
            .query(
                nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_CLAIM)),
            )
            .await
            .unwrap();
        assert_eq!(
            claims.len(),
            1,
            "loser must not add claim events after observing the ack"
        );
    }

    #[tokio::test]
    async fn loser_does_not_spam_claims_while_foreign_claim_active() {
        let transport = Arc::new(MockTransport::new());
        let mk = |t: Arc<MockTransport>| {
            let store = Arc::new(Store::open_in_memory().unwrap());
            store
                .upsert_queue(&QueueConfig::work_queue("jobs.email"))
                .unwrap();
            NostrQ::new(Keys::generate(), store, t)
        };
        let producer = mk(transport.clone());
        let w1 = mk(transport.clone());
        let w2 = mk(transport.clone());
        let _i1 = w1.spawn_ingest("jobs.email").await.unwrap();
        let _i2 = w2.spawn_ingest("jobs.email").await.unwrap();
        let receipt = producer
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        for w in [&w1, &w2] {
            for _ in 0..40 {
                if w.store().get_message(&receipt.mid).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // w1 wins the claim; its lease is active (60s) so w2's survey must
        // see a live foreign winner and back off without publishing.
        let r1 = w1.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(w1.try_claim(&r1, 60, 10).await.unwrap());

        let r2 = w2.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(
            !w2.try_claim(&r2, 60, 10).await.unwrap(),
            "loser must not win while a foreign claim's lease is still active"
        );
        assert_eq!(
            w2.store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status,
            "pending",
            "loser's local row must remain pending, not falsely marked claimed"
        );

        // the transport must still hold exactly w1's single claim event —
        // w2 surveyed the existing claim and declined to publish its own
        let claims = transport
            .query(
                nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_CLAIM)),
            )
            .await
            .unwrap();
        assert_eq!(
            claims.len(),
            1,
            "loser must not add a competing claim event while another worker's lease is active"
        );
    }

    #[tokio::test]
    async fn winner_recognizes_its_own_prior_claim() {
        let (nq, transport) = setup();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(nq.try_claim(&rec, 60, 10).await.unwrap());
        let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert!(
            nq.try_claim(&rec, 60, 10).await.unwrap(),
            "own unexpired claim is still a win"
        );
        let claims = transport
            .query(
                nostr::Filter::new().kind(nostr::Kind::Custom(nostr_q_core::protocol::KIND_CLAIM)),
            )
            .await
            .unwrap();
        assert_eq!(
            claims.len(),
            1,
            "no duplicate claim published for an already-held message"
        );
    }
}
