use nostr::{Event, EventBuilder, EventId, Keys, Kind, PublicKey, Tag, TagKind};
use thiserror::Error;

use crate::envelope::Envelope;
use crate::queue::QueueMode;

pub const KIND_MESSAGE: u16 = 4620;
pub const KIND_CLAIM: u16 = 4621;
pub const KIND_ACK: u16 = 4622;
pub const KIND_NACK: u16 = 4623;
pub const KIND_DLQ: u16 = 4624;
pub const KIND_REPLY: u16 = 4625;
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
    /// Not-before (unix seconds): the message is not claimable until this
    /// time. `None` means immediately claimable (the default). Carried on
    /// the wire as the `nbf` tag (SRS §11.3 reserves `exp`; `nbf` mirrors
    /// it for delayed delivery).
    pub not_before: Option<i64>,
    /// Expiry (unix seconds): the message must not be claimed once this
    /// time has passed. `None` means the message never expires. Carried on
    /// the wire as the reserved `exp` tag (SRS §11.3).
    pub expires_at: Option<i64>,
    /// RPC request/reply mode: when set, this is the requester's pubkey
    /// (hex) and the message expects a single correlated `KIND_REPLY`
    /// event addressed back to it. `None` means this is an ordinary
    /// fire-and-forget message. Carried on the wire as the `reply` tag.
    pub reply_to: Option<String>,
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
    if let Some(nbf) = msg.not_before {
        tags.push(custom_tag("nbf", nbf.to_string()));
    }
    if let Some(exp) = msg.expires_at {
        tags.push(custom_tag("exp", exp.to_string()));
    }
    if let Some(reply_to) = &msg.reply_to {
        tags.push(custom_tag("reply", reply_to));
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
        not_before: tag_value(event, "nbf").and_then(|v| v.parse().ok()),
        expires_at: tag_value(event, "exp").and_then(|v| v.parse().ok()),
        reply_to: tag_value(event, "reply"),
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
    attempt: u32,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("lease_exp", lease_expires_at.to_string()));
    tags.push(custom_tag("attempt", attempt.to_string()));
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
    attempt: u32,
    reason: &str,
) -> Result<Event, ProtocolError> {
    let mut tags = lifecycle_tags(message_event_id, queue, mid, trace_id);
    tags.push(custom_tag("attempt", attempt.to_string()));
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

/// Build a `KIND_REPLY` (4625) event correlated to a request. `e` (via
/// `Tag::event`) references the request's event id, `p` (via
/// `Tag::public_key`) addresses the requester, and the custom `parent` tag
/// carries the request's `mid` for human-readable correlation alongside the
/// `e` tag. Content is an `Envelope` wrapping `body`, matching how request
/// bodies are carried (SRS §11.4).
pub fn build_reply_event(
    keys: &Keys,
    request_event_id: EventId,
    request_mid: &str,
    requester: PublicKey,
    body: &serde_json::Value,
) -> Result<Event, ProtocolError> {
    let content = Envelope::new(body.clone())
        .to_json()
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    let tags = vec![
        Tag::event(request_event_id),
        Tag::public_key(requester),
        custom_tag("parent", request_mid),
    ];
    sign(
        EventBuilder::new(Kind::Custom(KIND_REPLY), content).tags(tags),
        keys,
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplyInfo {
    pub request_event_id: EventId,
    pub request_mid: String,
    pub body: serde_json::Value,
}

pub fn parse_reply_event(event: &Event) -> Result<ReplyInfo, ProtocolError> {
    let request_event_id = require_tag(event, "e")?;
    let request_event_id = EventId::from_hex(&request_event_id)
        .map_err(|e| ProtocolError::BadPayload(format!("bad 'e' tag: {e}")))?;
    let request_mid = require_tag(event, "parent")?;
    let envelope = Envelope::from_json(&event.content)
        .map_err(|e| ProtocolError::BadPayload(e.to_string()))?;
    Ok(ReplyInfo {
        request_event_id,
        request_mid,
        body: envelope.body,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimInfo {
    pub claimer: PublicKey,
    pub claim_event_id: EventId,
    pub created_at: i64,
    pub lease_expires_at: i64,
    pub attempt: u32,
}

pub fn parse_claim_event(event: &Event) -> Result<ClaimInfo, ProtocolError> {
    let lease_expires_at = require_tag(event, "lease_exp")?
        .parse()
        .map_err(|_| ProtocolError::BadPayload("lease_exp not an integer".into()))?;
    Ok(ClaimInfo {
        claimer: event.pubkey,
        claim_event_id: event.id,
        created_at: event.created_at.as_secs() as i64,
        lease_expires_at,
        attempt: tag_value(event, "attempt")
            .and_then(|a| a.parse().ok())
            .unwrap_or(0),
    })
}

/// Deterministic claim conflict resolution. `min_attempt` is the highest
/// attempt number seen in nack events for this message: a nack at attempt
/// N releases every claim made for an earlier attempt, so retries don't
/// lose to their own stale claims. Among unexpired claims with
/// attempt >= min_attempt, earliest created_at wins, ties broken by event
/// id hex.
pub fn claim_winner(claims: &[ClaimInfo], min_attempt: u32, now: i64) -> Option<&ClaimInfo> {
    claims
        .iter()
        .filter(|c| c.lease_expires_at > now && c.attempt >= min_attempt)
        .min_by_key(|c| (c.created_at, c.claim_event_id.to_hex()))
}

/// Attempt number carried by a nack event (0 when absent).
pub fn event_attempt(event: &Event) -> u32 {
    tag_value(event, "attempt")
        .and_then(|a| a.parse().ok())
        .unwrap_or(0)
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
            not_before: None,
            expires_at: None,
            reply_to: None,
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
    fn message_event_carries_nbf_and_exp_tags_when_set() {
        let keys = Keys::generate();
        let mut msg = sample_msg();
        msg.not_before = Some(1_700_000_100);
        msg.expires_at = Some(1_700_003_600);
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert_eq!(tag_value(&event, "nbf").as_deref(), Some("1700000100"));
        assert_eq!(tag_value(&event, "exp").as_deref(), Some("1700003600"));
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.not_before, Some(1_700_000_100));
        assert_eq!(parsed.expires_at, Some(1_700_003_600));
    }

    #[test]
    fn message_event_omits_nbf_and_exp_tags_by_default() {
        let keys = Keys::generate();
        let msg = sample_msg();
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert!(tag_value(&event, "nbf").is_none());
        assert!(tag_value(&event, "exp").is_none());
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.not_before, None);
        assert_eq!(parsed.expires_at, None);
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
    fn parse_message_event_rejects_missing_t_tag() {
        let keys = Keys::generate();
        let content = Envelope::new(json!({})).to_json().unwrap();
        // mid + trace present, but no "t" (queue) tag.
        let tags = vec![
            custom_tag("mid", crate::ids::new_mid()),
            custom_tag("trace", crate::ids::new_trace_id()),
        ];
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_MESSAGE), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_message_event(&event),
            Err(ProtocolError::MissingTag(tag)) if tag == "t"
        ));
    }

    #[test]
    fn parse_message_event_rejects_missing_trace_tag() {
        let keys = Keys::generate();
        let content = Envelope::new(json!({})).to_json().unwrap();
        // mid + t present, but no "trace" tag.
        let tags = vec![
            custom_tag("mid", crate::ids::new_mid()),
            Tag::hashtag("jobs.email"),
        ];
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_MESSAGE), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_message_event(&event),
            Err(ProtocolError::MissingTag(tag)) if tag == "trace"
        ));
    }

    #[test]
    fn parse_message_event_rejects_malformed_envelope_content() {
        let keys = Keys::generate();
        // valid nostr-q tags, but content is not a valid Envelope JSON.
        let tags = vec![
            custom_tag("mid", crate::ids::new_mid()),
            Tag::hashtag("jobs.email"),
            custom_tag("trace", crate::ids::new_trace_id()),
        ];
        let event =
            nostr::EventBuilder::new(nostr::Kind::Custom(KIND_MESSAGE), "not valid json at all")
                .tags(tags)
                .sign_with_keys(&keys)
                .unwrap();
        assert!(matches!(
            parse_message_event(&event),
            Err(ProtocolError::BadPayload(_))
        ));
    }

    #[test]
    fn parse_claim_event_rejects_non_integer_lease_exp() {
        let keys = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let mut tags = lifecycle_tags(msg_id, "jobs.email", "m1", "t1");
        tags.push(custom_tag("lease_exp", "not-a-number"));
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_CLAIM), "")
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_claim_event(&event),
            Err(ProtocolError::BadPayload(_))
        ));
    }

    #[test]
    fn parse_claim_event_rejects_missing_lease_exp() {
        let keys = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        // lifecycle tags only — no lease_exp tag at all.
        let tags = lifecycle_tags(msg_id, "jobs.email", "m1", "t1");
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_CLAIM), "")
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_claim_event(&event),
            Err(ProtocolError::MissingTag(tag)) if tag == "lease_exp"
        ));
    }

    #[test]
    fn claim_roundtrip_and_winner() {
        let keys_a = Keys::generate();
        let keys_b = Keys::generate();
        let msg_id = nostr::EventId::all_zeros();
        let a =
            build_claim_event(&keys_a, msg_id, "jobs.email", "m1", "t1", 2_000_000_000, 0).unwrap();
        let b =
            build_claim_event(&keys_b, msg_id, "jobs.email", "m1", "t1", 2_000_000_000, 0).unwrap();
        let ca = parse_claim_event(&a).unwrap();
        let cb = parse_claim_event(&b).unwrap();
        assert_eq!(ca.lease_expires_at, 2_000_000_000);
        assert_eq!(ca.attempt, 0);
        let claims = vec![ca.clone(), cb.clone()];
        let winner = claim_winner(&claims, 0, 0).unwrap();
        // deterministic: same-second claims break ties by event id hex
        let expect = if ca.claim_event_id.to_hex() <= cb.claim_event_id.to_hex() {
            &ca
        } else {
            &cb
        };
        assert_eq!(winner.claim_event_id, expect.claim_event_id);
        // expired claims never win
        assert!(claim_winner(&claims, 0, 3_000_000_000).is_none());
    }

    #[test]
    fn claim_winner_ignores_claims_from_before_a_nack() {
        // Built as ClaimInfo directly (rather than via build_claim_event's
        // wall-clock created_at) so `stale` is deterministically earlier
        // than `retry` — build_claim_event has no way to control
        // created_at, and two claims minted moments apart in a test
        // usually land in the same second, making the created_at tiebreak
        // fall through to a coin-flip on event id hex.
        let claimer = Keys::generate().public_key();
        let stale = ClaimInfo {
            claimer,
            claim_event_id: EventId::from_slice(&[0xaa; 32]).unwrap(),
            created_at: 1_000_000_000,
            lease_expires_at: 2_000_000_000,
            attempt: 0,
        };
        let retry = ClaimInfo {
            claimer,
            claim_event_id: EventId::from_slice(&[0xbb; 32]).unwrap(),
            created_at: 1_000_000_005,
            lease_expires_at: 2_000_000_000,
            attempt: 1,
        };
        let claims = vec![stale.clone(), retry.clone()];
        // nack at attempt 1 releases the attempt-0 claim
        let winner = claim_winner(&claims, 1, 0).unwrap();
        assert_eq!(winner.claim_event_id, retry.claim_event_id);
        // without the nack, the earlier claim still wins
        assert_eq!(claim_winner(&claims, 0, 0).unwrap().attempt, 0);
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
        let dlq = build_dlq_event(&keys, msg_id, "jobs.email", "m1", "t1", 4, "dead").unwrap();
        assert_eq!(dlq.kind.as_u16(), KIND_DLQ);
        assert_eq!(tag_value(&dlq, "attempt").as_deref(), Some("4"));
        assert_eq!(tag_value(&dlq, "reason").as_deref(), Some("dead"));
    }

    #[test]
    fn message_event_carries_reply_to_tag_when_set() {
        let keys = Keys::generate();
        let requester = Keys::generate().public_key().to_hex();
        let mut msg = sample_msg();
        msg.reply_to = Some(requester.clone());
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert_eq!(
            tag_value(&event, "reply").as_deref(),
            Some(requester.as_str())
        );
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.reply_to.as_deref(), Some(requester.as_str()));
    }

    #[test]
    fn message_event_omits_reply_tag_by_default() {
        let keys = Keys::generate();
        let msg = sample_msg();
        let event = build_message_event(&keys, QueueMode::WorkQueue, &msg).unwrap();
        assert!(tag_value(&event, "reply").is_none());
        let parsed = parse_message_event(&event).unwrap();
        assert_eq!(parsed.reply_to, None);
    }

    #[test]
    fn reply_event_roundtrip() {
        let keys = Keys::generate();
        let requester = Keys::generate().public_key();
        let request_event_id = EventId::from_slice(&[0x11; 32]).unwrap();
        let body = json!({"result": 42});
        let event =
            build_reply_event(&keys, request_event_id, "req-mid-1", requester, &body).unwrap();
        assert_eq!(event.kind.as_u16(), KIND_REPLY);
        assert_eq!(tag_value(&event, "e"), Some(request_event_id.to_hex()));
        assert_eq!(tag_value(&event, "p"), Some(requester.to_hex()));
        assert_eq!(tag_value(&event, "parent").as_deref(), Some("req-mid-1"));

        let info = parse_reply_event(&event).unwrap();
        assert_eq!(info.request_event_id, request_event_id);
        assert_eq!(info.request_mid, "req-mid-1");
        assert_eq!(info.body, body);
    }

    #[test]
    fn parse_reply_event_rejects_missing_e_tag() {
        let keys = Keys::generate();
        let content = Envelope::new(json!({})).to_json().unwrap();
        let tags = vec![custom_tag("parent", "req-mid-1")];
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_REPLY), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_reply_event(&event),
            Err(ProtocolError::MissingTag(tag)) if tag == "e"
        ));
    }

    #[test]
    fn parse_reply_event_rejects_missing_parent_tag() {
        let keys = Keys::generate();
        let content = Envelope::new(json!({})).to_json().unwrap();
        let request_event_id = EventId::from_slice(&[0x22; 32]).unwrap();
        let tags = vec![Tag::event(request_event_id)];
        let event = nostr::EventBuilder::new(nostr::Kind::Custom(KIND_REPLY), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .unwrap();
        assert!(matches!(
            parse_reply_event(&event),
            Err(ProtocolError::MissingTag(tag)) if tag == "parent"
        ));
    }
}
