use nostr::{Event, EventBuilder, EventId, Keys, Kind, PublicKey, Tag, TagKind};
use thiserror::Error;

use crate::envelope::Envelope;
use crate::queue::QueueMode;

pub const KIND_MESSAGE: u16 = 4620;
pub const KIND_CLAIM: u16 = 4621;
pub const KIND_ACK: u16 = 4622;
pub const KIND_NACK: u16 = 4623;
pub const KIND_DLQ: u16 = 4624;
pub const KIND_HEARTBEAT: u16 = 24620;
pub const KIND_QUEUE_CONFIG: u16 = 34620; // reserved, unused in MVP

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("missing required tag '{0}'")]
    MissingTag(String),
    #[error("bad payload: {0}")]
    BadPayload(String),
    #[error("signing error: {0}")]
    Signing(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NqMessage {
    pub mid: String,
    pub queue: String,
    pub trace_id: String,
    pub attempt: u32,
    pub idem: Option<String>,
    pub envelope: Envelope,
}

fn custom_tag(name: &str, value: impl Into<String>) -> Tag {
    Tag::custom(TagKind::custom(name.to_string()), [value.into()])
}

/// First value of the first tag whose name matches. Works for both
/// single-letter ("t", "e") and custom multi-letter tags.
pub fn tag_value(event: &Event, name: &str) -> Option<String> {
    event.tags.iter().find_map(|t| {
        let s = t.as_slice();
        if s.first().map(String::as_str) == Some(name) {
            s.get(1).cloned()
        } else {
            None
        }
    })
}

fn require_tag(event: &Event, name: &str) -> Result<String, ProtocolError> {
    tag_value(event, name).ok_or_else(|| ProtocolError::MissingTag(name.to_string()))
}

fn sign(builder: EventBuilder, keys: &Keys) -> Result<Event, ProtocolError> {
    builder
        .sign_with_keys(keys)
        .map_err(|e| ProtocolError::Signing(e.to_string()))
}

pub fn build_message_event(
    keys: &Keys,
    mode: QueueMode,
    msg: &NqMessage,
) -> Result<Event, ProtocolError> {
    let content = msg
        .envelope
        .to_json()
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    let mut tags = vec![
        Tag::hashtag(&msg.queue),    // "t": relay-indexed, filterable
        custom_tag("q", &msg.queue), // SRS-required duplicate
        custom_tag("mode", mode.as_str()),
        custom_tag("mid", &msg.mid),
        custom_tag("trace", &msg.trace_id),
        custom_tag("attempt", msg.attempt.to_string()),
    ];
    if let Some(idem) = &msg.idem {
        tags.push(custom_tag("idem", idem));
    }
    sign(
        EventBuilder::new(Kind::Custom(KIND_MESSAGE), content).tags(tags),
        keys,
    )
}

pub fn parse_message_event(event: &Event) -> Result<NqMessage, ProtocolError> {
    let envelope = Envelope::from_json(&event.content)
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    Ok(NqMessage {
        mid: require_tag(event, "mid")?,
        queue: require_tag(event, "t")?,
        trace_id: require_tag(event, "trace")?,
        attempt: tag_value(event, "attempt")
            .and_then(|a| a.parse().ok())
            .unwrap_or(0),
        idem: tag_value(event, "idem"),
        envelope,
    })
}

fn lifecycle_tags(message_event_id: EventId, queue: &str, mid: &str, trace_id: &str) -> Vec<Tag> {
    vec![
        Tag::event(message_event_id),
        Tag::hashtag(queue),
        custom_tag("mid", mid),
        custom_tag("trace", trace_id),
    ]
}

pub fn build_claim_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    lease_expires_at: i64,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("lease_exp", lease_expires_at.to_string()));
    sign(
        EventBuilder::new(Kind::Custom(KIND_CLAIM), "").tags(tags),
        keys,
    )
}

pub fn build_ack_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
) -> Result<Event, ProtocolError> {
    let tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    sign(
        EventBuilder::new(Kind::Custom(KIND_ACK), "").tags(tags),
        keys,
    )
}

pub fn build_nack_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    attempt: u32,
    reason: &str,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("attempt", attempt.to_string()));
    tags.push(custom_tag("reason", reason));
    sign(
        EventBuilder::new(Kind::Custom(KIND_NACK), "").tags(tags),
        keys,
    )
}

pub fn build_dlq_event(
    keys: &Keys,
    message_event_id: EventId,
    queue: &str,
    mid: &str,
    trace_id: &str,
    reason: &str,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("reason", reason));
    sign(
        EventBuilder::new(Kind::Custom(KIND_DLQ), "").tags(tags),
        keys,
    )
}

pub fn build_heartbeat_event(keys: &Keys, queue: &str) -> Result<Event, ProtocolError> {
    let tags = vec![Tag::hashtag(queue)];
    sign(
        EventBuilder::new(Kind::Custom(KIND_HEARTBEAT), "").tags(tags),
        keys,
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimInfo {
    pub claimer: PublicKey,
    pub claim_event_id: EventId,
    pub created_at: i64,
    pub lease_expires_at: i64,
}

pub fn parse_claim_event(event: &Event) -> Result<ClaimInfo, ProtocolError> {
    let lease_expires_at = require_tag(event, "lease_exp")?
        .parse()
        .map_err(|_| ProtocolError::BadPayload("lease_exp not an integer".into()))?;
    Ok(ClaimInfo {
        claimer: event.pubkey,
        claim_event_id: event.id,
        created_at: event.created_at.as_u64() as i64,
        lease_expires_at,
    })
}

/// Deterministic claim conflict resolution: earliest created_at wins,
/// ties broken by event id hex. Expired claims are ignored.
pub fn claim_winner(claims: &[ClaimInfo], now: i64) -> Option<&ClaimInfo> {
    claims
        .iter()
        .filter(|c| c.lease_expires_at > now)
        .min_by_key(|c| (c.created_at, c.claim_event_id.to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::Envelope;
    use crate::queue::QueueMode;
    use nostr::Keys;
    use serde_json::json;

    fn sample_msg() -> NqMessage {
        NqMessage {
            mid: crate::ids::new_mid(),
            queue: "jobs.email".into(),
            trace_id: crate::ids::new_trace_id(),
            attempt: 0,
            idem: Some("order-42".into()),
            envelope: Envelope::new(json!({"to": "a@b.c"})),
        }
    }

    #[test]
    fn message_event_roundtrip() {
        let keys = Keys::generate();
        let msg = sample_msg();
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert_eq!(event.kind.as_u16(), KIND_MESSAGE);
        assert_eq!(tag_value(&event, "t").as_deref(), Some("jobs.email"));
        assert_eq!(tag_value(&event, "mode").as_deref(), Some("work_queue"));
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.mid, msg.mid);
        assert_eq!(parsed.queue, "jobs.email");
        assert_eq!(parsed.idem.as_deref(), Some("order-42"));
        assert_eq!(parsed.envelope.body, msg.envelope.body);
    }

    #[test]
    fn parse_rejects_missing_mid() {
        let keys = Keys::generate();
        // valid envelope content but no nostr-q tags at all
        let content = Envelope::new(json!({})).to_json().unwrap();
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_MESSAGE), content)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_message_event(&event),
            Err(ProtocolError::MissingTag(_))
        ));
    }

    #[test]
    fn claim_roundtrip_and_winner() {
        let keys_a = Keys::generate();
        let keys_b = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let a =
            build_claim_event(&keys_a, msg_id, "jobs.email", "m1", "t1", 2_000_000_000).unwrap();
        let b =
            build_claim_event(&keys_b, msg_id, "jobs.email", "m1", "t1", 2_000_000_000).unwrap();
        let ca = parse_claim_event(&a).unwrap();
        let cb = parse_claim_event(&b).unwrap();
        assert_eq!(ca.lease_expires_at, 2_000_000_000);
        let claims = vec![ca.clone(), cb.clone()];
        let winner = claim_winner(&claims, 0).unwrap();
        // deterministic: same-second claims break ties by event id hex
        let expect = if ca.claim_event_id.to_hex() <= cb.claim_event_id.to_hex() {
            &ca
        } else {
            &cb
        };
        assert_eq!(winner.claim_event_id, expect.claim_event_id);
        // expired claims never win
        assert!(claim_winner(&claims, 3_000_000_000).is_none());
    }

    #[test]
    fn lifecycle_events_carry_refs() {
        let keys = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let ack = build_ack_event(&keys, msg_id, "jobs.email", "m1", "t1").unwrap();
        assert_eq!(ack.kind.as_u16(), KIND_ACK);
        assert_eq!(tag_value(&ack, "mid").as_deref(), Some("m1"));
        assert_eq!(tag_value(&ack, "e"), Some(msg_id.to_hex()));
        let nack = build_nack_event(&keys, msg_id, "jobs.email", "m1", "t1", 3, "boom").unwrap();
        assert_eq!(tag_value(&nack, "attempt").as_deref(), Some("3"));
        assert_eq!(tag_value(&nack, "reason").as_deref(), Some("boom"));
    }
}
