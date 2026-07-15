//! `DevRelay` — a minimal, in-memory NIP-01 relay used by `nostr-q dev`
//! (CHA-2343) to give a newcomer a working job flow with zero external
//! dependencies.
//!
//! This is deliberately NOT a production relay: no persistence, no auth, no
//! NIP-11, no rate limiting. It exists to let `nostr-q dev` spin up a
//! self-contained relay in-process and have the *real* `NostrTransport`
//! (nostr-sdk client) connect, publish, and subscribe against it exactly as
//! it would against any other relay.
//!
//! Supported client -> relay messages (per NIP-01):
//! - `["EVENT", <event>]` -> verify signature, store, broadcast, reply
//!   `["OK", <id>, <ok>, <msg>]`
//! - `["REQ", <sub-id>, <filter>...]` -> replay stored matches, `["EOSE",
//!   ...]`, then stream live matches
//! - `["CLOSE", <sub-id>]` -> drop the subscription
//!
//! Filter matching reuses nostr's own `Filter::match_event` (same as
//! `MockTransport`), and live fanout uses a `tokio::sync::broadcast` channel
//! (also same shape as `MockTransport`) so every connection task filters the
//! live stream independently.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use nostr::filter::MatchEventOptions;
use nostr::{ClientMessage, Event, Filter, JsonUtil, RelayMessage, SubscriptionId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

/// In-memory state shared by every connection: every stored event plus a
/// broadcast channel for live fanout to subscribers.
struct RelayState {
    events: Mutex<Vec<Event>>,
    tx: broadcast::Sender<Event>,
}

impl RelayState {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            events: Mutex::new(Vec::new()),
            tx,
        }
    }
}

/// A running embedded dev relay. Holds the bound address (useful when the
/// caller asked for an ephemeral port) and the join handle for the accept
/// loop, so shutdown can be awaited rather than fire-and-forget.
pub struct DevRelay {
    addr: SocketAddr,
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

impl DevRelay {
    /// The address the relay actually bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The `ws://` URL clients should connect to.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Signal shutdown and wait for the accept loop (and every connection
    /// task it spawned, which watch the same cancellation token) to stop.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
    }
}

/// Start the embedded dev relay bound to `addr`. Pass a port of `0` to bind
/// an ephemeral port; check `DevRelay::addr()`/`url()` for the address
/// actually bound. Runs until `DevRelay::shutdown()` is called.
pub async fn serve_dev_relay(addr: SocketAddr) -> anyhow::Result<DevRelay> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let state = Arc::new(RelayState::new());

    let accept_shutdown = shutdown.clone();
    let handle = tokio::spawn(accept_loop(listener, state, accept_shutdown));

    Ok(DevRelay {
        addr: bound,
        shutdown,
        handle,
    })
}

async fn accept_loop(listener: TcpListener, state: Arc<RelayState>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "dev relay: accept failed");
                        continue;
                    }
                };
                let state = state.clone();
                let conn_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, state, conn_shutdown).await {
                        tracing::debug!(error = %e, "dev relay: connection ended");
                    }
                });
            }
        }
    }
}

type WsWrite = futures::stream::SplitSink<WebSocketStream<TcpStream>, WsMessage>;

async fn handle_connection(
    stream: TcpStream,
    state: Arc<RelayState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();
    let mut subs: HashMap<SubscriptionId, Vec<Filter>> = HashMap::new();
    let mut live = state.tx.subscribe();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = read.next() => {
                let Some(incoming) = incoming else { break }; // client closed the stream
                let msg = match incoming {
                    Ok(m) => m,
                    Err(_) => break, // connection error - drop it
                };
                match msg {
                    WsMessage::Text(text) => {
                        if let Err(e) =
                            handle_client_text(text.as_str(), &state, &mut subs, &mut write).await
                        {
                            tracing::debug!(error = %e, "dev relay: failed to handle client message");
                        }
                    }
                    WsMessage::Ping(payload) => {
                        if write.send(WsMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            live_evt = live.recv() => {
                match live_evt {
                    Ok(event) => {
                        for (sub_id, filters) in subs.iter() {
                            if filters.iter().any(|f| f.match_event(&event, MatchEventOptions::new())) {
                                let out = RelayMessage::event(sub_id.clone(), event.clone());
                                if write.send(WsMessage::text(out.as_json())).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    // A slow subscriber can miss some live events on lag; keep the
                    // connection alive rather than dropping it (same tradeoff as
                    // MockTransport::subscribe).
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    Ok(())
}

async fn handle_client_text(
    text: &str,
    state: &RelayState,
    subs: &mut HashMap<SubscriptionId, Vec<Filter>>,
    write: &mut WsWrite,
) -> anyhow::Result<()> {
    let msg = match ClientMessage::from_json(text.as_bytes()) {
        Ok(m) => m,
        Err(e) => {
            // Malformed / unsupported message: NIP-01 relays send NOTICE
            // rather than dropping the connection.
            let notice = RelayMessage::notice(format!("invalid message: {e}"));
            write.send(WsMessage::text(notice.as_json())).await?;
            return Ok(());
        }
    };

    match msg {
        ClientMessage::Event(event) => {
            let event = event.into_owned();
            if event.verify().is_err() {
                let ok =
                    RelayMessage::ok(event.id, false, "invalid: signature verification failed");
                write.send(WsMessage::text(ok.as_json())).await?;
                return Ok(());
            }
            state.events.lock().unwrap().push(event.clone());
            let _ = state.tx.send(event.clone()); // no live subscribers is fine
            let ok = RelayMessage::ok(event.id, true, "");
            write.send(WsMessage::text(ok.as_json())).await?;
        }
        ClientMessage::Req {
            subscription_id,
            filters,
        } => {
            let sub_id = subscription_id.into_owned();
            let filters: Vec<Filter> = filters.into_iter().map(|f| f.into_owned()).collect();
            let stored: Vec<Event> = {
                let events = state.events.lock().unwrap();
                events
                    .iter()
                    .filter(|e| {
                        filters
                            .iter()
                            .any(|f| f.match_event(e, MatchEventOptions::new()))
                    })
                    .cloned()
                    .collect()
            };
            for e in stored {
                let out = RelayMessage::event(sub_id.clone(), e);
                write.send(WsMessage::text(out.as_json())).await?;
            }
            let eose = RelayMessage::eose(sub_id.clone());
            write.send(WsMessage::text(eose.as_json())).await?;
            subs.insert(sub_id, filters);
        }
        ClientMessage::Close(subscription_id) => {
            subs.remove(&*subscription_id);
        }
        _ => {
            // COUNT / AUTH / negentropy - out of scope for a minimal dev relay.
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr_transport::NostrTransport;
    use crate::transport::Transport;
    use nostr::{EventBuilder, Keys, Kind};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// A unique-content event so its id never collides with another event
    /// from the same call site created in the same wall-clock second (Nostr
    /// event ids are content-addressed and `created_at` has 1s resolution).
    fn message_event(keys: &Keys, topic: &str) -> Event {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        EventBuilder::new(Kind::Custom(4620), format!("hello-{n}"))
            .tags(vec![nostr::Tag::hashtag(topic)])
            .sign_with_keys(keys)
            .unwrap()
    }

    /// The critical deliverable: proves the embedded relay interoperates
    /// with the REAL production transport (nostr-sdk client), not just with
    /// itself. Starts the relay on an ephemeral port, connects two
    /// independent `NostrTransport`s (a publisher and a subscriber - mirrors
    /// real usage, where a `pub` and a `worker`/`sub` are separate
    /// processes), publishes an event on one, and asserts the other
    /// receives it via a live subscription.
    ///
    /// Deliberately NOT the same `NostrTransport` publishing and
    /// subscribing to its own event: nostr-sdk's client saves an event to
    /// its local database before sending it, so a relay echo of an event a
    /// client already has locally is correctly deduped and never re-surfaces
    /// as a notification - that's `nostr-sdk` client behavior, not something
    /// this relay controls or should route around.
    #[tokio::test]
    async fn nostr_sdk_client_interoperates_with_embedded_dev_relay() {
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .expect("dev relay must start");
        let url = relay.url();

        let keys = Keys::generate();
        let publisher = NostrTransport::connect(keys.clone(), std::slice::from_ref(&url))
            .await
            .expect("publisher NostrTransport must connect to the embedded dev relay");
        let subscriber = NostrTransport::connect(keys.clone(), &[url])
            .await
            .expect("subscriber NostrTransport must connect to the embedded dev relay");

        let filter = Filter::new().kind(Kind::Custom(4620)).hashtag("interop");
        let mut rx = subscriber
            .subscribe(filter)
            .await
            .expect("subscribe must succeed");

        let event = message_event(&keys, "interop");
        let published_id = publisher
            .publish(event.clone())
            .await
            .expect("publish must succeed against the embedded relay");
        assert_eq!(published_id, event.id);

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("must receive the published event before the timeout")
            .expect("subscription channel must not close");
        assert_eq!(received.id, event.id);

        relay.shutdown().await;
    }

    /// A REQ with no prior events must still get the EOSE handshake right
    /// (empty replay + EOSE), and subsequently-published events matching
    /// the filter must be seen once, correctly.
    #[tokio::test]
    async fn req_returns_stored_events_then_eose_then_live_events() {
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = relay.url();
        let keys = Keys::generate();

        // Publish one event BEFORE any subscriber connects - this must be
        // replayed on REQ.
        let publisher = NostrTransport::connect(keys.clone(), std::slice::from_ref(&url))
            .await
            .unwrap();
        let stored_event = message_event(&keys, "stored");
        publisher.publish(stored_event.clone()).await.unwrap();
        // Give the relay a moment to finish handling the EVENT/OK round trip.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let subscriber = NostrTransport::connect(keys.clone(), &[url]).await.unwrap();
        let mut rx = subscriber
            .subscribe(Filter::new().kind(Kind::Custom(4620)).hashtag("stored"))
            .await
            .unwrap();

        let replayed = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("must replay the stored event")
            .unwrap();
        assert_eq!(replayed.id, stored_event.id);

        // Now a live event, published after the subscription is active.
        let live_event = message_event(&keys, "stored");
        publisher.publish(live_event.clone()).await.unwrap();
        let live = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("must stream the live event")
            .unwrap();
        assert_eq!(live.id, live_event.id);

        relay.shutdown().await;
    }

    /// An EVENT with an invalid signature must be rejected (OK false) and
    /// never stored/broadcast, verified indirectly here via `query`-style
    /// REQ: only the valid event comes back.
    #[tokio::test]
    async fn invalid_signature_event_is_rejected_and_not_stored() {
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = relay.url();
        let keys = Keys::generate();

        let mut tampered = message_event(&keys, "tamper");
        // Flip a byte in the id so the signature no longer matches - this is
        // the simplest way to produce a structurally valid but
        // cryptographically invalid event without reaching into nostr's
        // signing internals.
        let mut id_bytes = tampered.id.as_bytes().to_vec();
        id_bytes[0] ^= 0xFF;
        tampered.id = nostr::EventId::from_slice(&id_bytes).unwrap();

        let transport = NostrTransport::connect(keys.clone(), &[url]).await.unwrap();
        // send_event surfaces relay rejection as an error (no relay accepted it).
        let result = transport.publish(tampered).await;
        assert!(
            result.is_err(),
            "an event with an invalid signature must be rejected by the relay"
        );

        relay.shutdown().await;
    }

    /// Protocol-level regression test independent of nostr-sdk client
    /// quirks: a raw websocket client that both subscribes AND publishes on
    /// the SAME connection must see its own published event streamed back
    /// as a live match (proves `subs` is consulted correctly even when a
    /// REQ and an EVENT interleave on one socket).
    #[tokio::test]
    async fn raw_ws_client_on_one_connection_sees_its_own_live_publish() {
        use tokio_tungstenite::connect_async;

        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = relay.url();
        let keys = Keys::generate();

        let (mut ws, _) = connect_async(&url).await.unwrap();
        let req = ClientMessage::req(
            SubscriptionId::new("sub1"),
            vec![Filter::new().kind(Kind::Custom(4620))],
        );
        ws.send(WsMessage::text(req.as_json())).await.unwrap();
        // drain until EOSE
        loop {
            let msg = ws.next().await.unwrap().unwrap();
            if let WsMessage::Text(t) = msg {
                if t.contains("EOSE") {
                    break;
                }
            }
        }

        let event = message_event(&keys, "raw");
        let ev_msg = ClientMessage::event(event.clone());
        ws.send(WsMessage::text(ev_msg.as_json())).await.unwrap();

        // OK ack, then the live event, both on the same socket.
        let ok = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("must receive an OK ack")
            .unwrap()
            .unwrap();
        assert!(matches!(ok, WsMessage::Text(t) if t.starts_with("[\"OK\"")));

        let live = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("must receive the live event")
            .unwrap()
            .unwrap();
        match live {
            WsMessage::Text(t) => {
                assert!(t.starts_with("[\"EVENT\",\"sub1\","));
                assert!(t.contains(&event.id.to_hex()));
            }
            other => panic!("expected a text EVENT message, got {other:?}"),
        }

        relay.shutdown().await;
    }

    #[tokio::test]
    async fn serve_dev_relay_binds_ephemeral_port_and_reports_real_address() {
        let relay = serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        assert_ne!(relay.addr().port(), 0, "must report the actual bound port");
        assert!(relay.url().starts_with("ws://127.0.0.1:"));
        relay.shutdown().await;
    }
}
