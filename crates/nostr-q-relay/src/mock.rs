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
        // Snapshot stored events first, then subscribe to the broadcast
        // channel BEFORE spawning the forwarding task. An event published
        // between the snapshot and this subscribe call may be delivered
        // twice (once via replay, once live) — that's acceptable since
        // downstream dedupes by event_id. Events must never be lost.
        let mut live = self.tx.subscribe();
        tokio::spawn(async move {
            for e in stored {
                if out_tx.send(e).await.is_err() {
                    return;
                }
            }
            loop {
                match live.recv().await {
                    Ok(e) => {
                        if filter.match_event(&e) && out_tx.send(e).await.is_err() {
                            return;
                        }
                    }
                    // Lagged skips missed events but the channel is still live —
                    // keep forwarding rather than silently dropping the subscriber.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
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

    #[tokio::test]
    async fn subscriber_survives_broadcast_lag() {
        let t = MockTransport::new();
        let keys = Keys::generate();
        let mut rx = t
            .subscribe(Filter::new().kind(Kind::Custom(4620)).hashtag("a"))
            .await
            .unwrap();
        // Overflow the 1024-capacity broadcast channel while the forwarding
        // task may be behind, then confirm the subscription still delivers.
        for _ in 0..1100 {
            t.publish(event(&keys, 4620, "other")).await.unwrap();
        }
        t.publish(event(&keys, 4620, "a")).await.unwrap();
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let e = rx.recv().await.expect("subscription must stay alive");
                if e.tags.iter().any(|t| t.as_slice().get(1).map(String::as_str) == Some("a")) {
                    return e;
                }
            }
        })
        .await
        .expect("subscriber should still receive after lag");
        assert_eq!(got.kind.as_u16(), 4620);
    }
}
