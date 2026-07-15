use std::time::{Duration, Instant};

use async_trait::async_trait;
use nostr::{Event, EventId, Filter, Keys};
use nostr_sdk::prelude::*;
use tokio::sync::mpsc;

use crate::transport::{RelayHealth, Transport};

/// The only nostr-sdk-aware `Transport` implementation. Wraps a `nostr_sdk::Client`
/// connected to one or more relays.
pub struct NostrTransport {
    client: Client,
}

impl NostrTransport {
    pub async fn connect(keys: Keys, relays: &[String]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !relays.is_empty(),
            "no relays configured — run `nq relay add <url>` first"
        );
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
        Ok(output.val)
    }

    async fn subscribe(&self, filter: Filter) -> anyhow::Result<mpsc::Receiver<Event>> {
        self.client.subscribe(filter.clone(), None).await?;
        let mut notifications = self.client.notifications();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(notification) => {
                        if let RelayPoolNotification::Event { event, .. } = notification {
                            if filter.match_event(&event) && tx.send(*event).await.is_err() {
                                return;
                            }
                        }
                    }
                    // Lagged skips missed notifications but the channel is still live —
                    // keep forwarding rather than silently dropping the subscriber.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(rx)
    }

    async fn query(&self, filter: Filter) -> anyhow::Result<Vec<Event>> {
        let events = self
            .client
            .fetch_events(filter, Duration::from_secs(5))
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
                        Filter::new().limit(1),
                        Duration::from_secs(5),
                    )
                    .await;
                probe.ok().map(|_| start.elapsed().as_millis() as u64)
            } else {
                None
            };
            out.push(RelayHealth {
                url: url.to_string(),
                connected,
                latency_ms,
            });
        }
        out
    }
}
