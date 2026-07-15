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
    claim_winner, parse_claim_event, parse_message_event, ClaimInfo, NqMessage, KIND_CLAIM,
    KIND_MESSAGE,
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
            self.store
                .mark_claimed(&rec.mid, &consumer, lease_expires_at)?;
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

        if attempts >= config.max_attempts {
            let dlq =
                build_dlq_event(&self.keys, event_id, &rec.queue, mid, &rec.trace_id, reason)?;
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

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(backoff_secs(5, 1), 5);
        assert_eq!(backoff_secs(5, 2), 10);
        assert_eq!(backoff_secs(5, 3), 20);
        assert_eq!(backoff_secs(5, 30), 3600); // capped
    }
}
