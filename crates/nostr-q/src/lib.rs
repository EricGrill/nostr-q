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
            .query(nostr::Filter::new().kind(nostr::Kind::Custom(
                nostr_q_core::protocol::KIND_MESSAGE,
            )))
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "duplicate idem must not re-broadcast");
    }
}
