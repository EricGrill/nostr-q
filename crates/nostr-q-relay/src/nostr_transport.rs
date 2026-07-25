use std::time::{Duration, Instant};

use async_trait::async_trait;
use nostr::{filter::MatchEventOptions, Event, EventId, Filter, Keys};
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
            "no relays configured - run `nostr-q relay add <url>` first"
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
        let output = self.client.send_event(&event).await?;
        anyhow::ensure!(
            !output.success.is_empty(),
            "no relay accepted the event (failed: {:?})",
            output.failed
        );
        // Any-one-accepted is still success (unchanged semantics), but a
        // partial failure means some relays now diverge from the rest of
        // the pool on this event — surface that instead of silently
        // dropping `output.failed`.
        if !output.failed.is_empty() {
            tracing::warn!(
                event_id = %output.val,
                failed_relays = ?output.failed,
                "event accepted by at least one relay, but rejected/failed on others"
            );
        }
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
                            if filter.match_event(&event, MatchEventOptions::new())
                                && tx.send(*event).await.is_err()
                            {
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
        // Each relay's probe below can wait up to a 3s handshake window plus
        // a 5s fetch timeout. Probing relays one at a time would make total
        // latency scale with relay count (N x up to 8s); run every relay's
        // probe concurrently instead so the whole health check takes as
        // long as the single slowest relay.
        let probes = self.client.relays().await.into_iter().map(|(url, relay)| {
            let client = self.client.clone();
            async move {
                // `Client::connect()` kicks off the websocket handshake in the
                // background and returns immediately, so a status read right
                // after connect() races the handshake and reads it as `not
                // connected` even when the relay is healthy. Give each relay a
                // bounded window to finish the handshake before deciding it's
                // down.
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut connected = relay.status() == RelayStatus::Connected;
                while !connected && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    connected = relay.status() == RelayStatus::Connected;
                }
                let latency_ms = if connected {
                    let start = Instant::now();
                    let probe = client
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
                RelayHealth {
                    url: url.to_string(),
                    connected,
                    latency_ms,
                }
            }
        });
        futures::future::join_all(probes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_relay::serve_dev_relay;
    use nostr::{EventBuilder, Kind};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Unique-content 4620 event so its content-addressed id never collides
    /// with another built at the same call site within the same wall-clock
    /// second (mirrors the helper in `dev_relay`'s own tests).
    fn signed_event(keys: &Keys, topic: &str) -> Event {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        EventBuilder::new(Kind::Custom(4620), format!("body-{n}"))
            .tags(vec![nostr::Tag::hashtag(topic)])
            .sign_with_keys(keys)
            .unwrap()
    }

    #[tokio::test]
    async fn connect_rejects_empty_relay_list() {
        // A transport with nowhere to publish is a configuration error, not a
        // silently-inert client that would swallow every publish downstream.
        // `NostrTransport` isn't `Debug`, so match rather than `expect_err`.
        let err = match NostrTransport::connect(Keys::generate(), &[]).await {
            Ok(_) => panic!("connecting with no relays must error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("no relays configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn query_returns_events_stored_on_the_relay() {
        // Exercises the real `query()` (REQ -> stored replay -> EOSE) path
        // against the embedded relay, which the publish/subscribe interop
        // tests never touch. A separate reader client avoids nostr-sdk's
        // local-echo dedup.
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .expect("dev relay must start");
        let url = relay.url();
        let keys = Keys::generate();
        let publisher = NostrTransport::connect(keys.clone(), std::slice::from_ref(&url))
            .await
            .unwrap();
        let reader = NostrTransport::connect(Keys::generate(), &[url])
            .await
            .unwrap();

        let event = signed_event(&keys, "query-me");
        // publish() resolves only after the relay's OK, so the event is stored.
        publisher.publish(event.clone()).await.unwrap();

        let filter = Filter::new().kind(Kind::Custom(4620)).hashtag("query-me");
        let found = reader.query(filter).await.expect("query must succeed");
        assert!(
            found.iter().any(|e| e.id == event.id),
            "query must return the event stored on the relay"
        );
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn health_reports_connected_with_latency_for_a_live_relay() {
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .expect("dev relay must start");
        let url = relay.url();
        let transport = NostrTransport::connect(Keys::generate(), &[url])
            .await
            .unwrap();

        let health = transport.health().await;
        assert_eq!(health.len(), 1, "one configured relay => one health entry");
        assert!(health[0].connected, "a live relay must report connected");
        assert!(
            health[0].latency_ms.is_some(),
            "a connected relay must report a latency probe"
        );
        relay.shutdown().await;
    }

    #[tokio::test]
    async fn health_reports_disconnected_for_an_unreachable_relay() {
        // Start then stop a relay so its port is no longer accepting, giving
        // us a well-formed but dead ws:// URL. `connect()` still returns Ok
        // (nostr-sdk connects in the background), and `health()` must ride out
        // its handshake window and then report the relay down with no latency.
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .expect("dev relay must start");
        let url = relay.url();
        relay.shutdown().await;

        let transport = NostrTransport::connect(Keys::generate(), &[url])
            .await
            .expect("connect returns Ok even when the relay is down");
        let health = transport.health().await;
        assert_eq!(health.len(), 1);
        assert!(
            !health[0].connected,
            "an unreachable relay must report not connected"
        );
        assert!(
            health[0].latency_ms.is_none(),
            "a disconnected relay must not report a latency probe"
        );
    }
}
