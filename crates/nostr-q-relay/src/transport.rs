use async_trait::async_trait;
use nostr::{Event, EventId, Filter};
use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize)]
pub struct RelayHealth {
    pub url: String,
    pub connected: bool,
    pub latency_ms: Option<u64>,
}

/// Abstraction over one-or-more Nostr relays. The only nostr-sdk-aware
/// implementation is NostrTransport; tests use MockTransport.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, event: Event) -> anyhow::Result<EventId>;
    /// Replays stored matching events, then streams live matches.
    async fn subscribe(&self, filter: Filter) -> anyhow::Result<mpsc::Receiver<Event>>;
    async fn query(&self, filter: Filter) -> anyhow::Result<Vec<Event>>;
    async fn health(&self) -> Vec<RelayHealth>;
}
