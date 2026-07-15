use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Standardized Nostr-Q payload envelope (SRS §11.4). This struct is the
/// Nostr event `content`, serialized as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: String,
    pub content_type: String,
    pub body: serde_json::Value,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

impl Envelope {
    pub fn new(body: serde_json::Value) -> Self {
        Self {
            version: "0.1".to_string(),
            content_type: "application/json".to_string(),
            body,
            headers: BTreeMap::new(),
            created_at: Utc::now(),
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelope_defaults() {
        let env = Envelope::new(json!({"to": "user@example.com"}));
        assert_eq!(env.version, "0.1");
        assert_eq!(env.content_type, "application/json");
        assert!(env.headers.is_empty());
    }

    #[test]
    fn envelope_json_roundtrip() {
        let env = Envelope::new(json!({"n": 1}));
        let parsed = Envelope::from_json(&env.to_json().unwrap()).unwrap();
        assert_eq!(env, parsed);
    }

    #[test]
    fn ids_are_unique_ulids() {
        let a = crate::ids::new_mid();
        let b = crate::ids::new_mid();
        assert_ne!(a, b);
        assert_eq!(a.len(), 26); // ULID canonical length
    }

    /// Golden-JSON pin on the `Envelope` wire shape (SRS §11.4). This is the
    /// Nostr event `content`, so third parties parse it directly — the
    /// field NAMES and TYPES below are a public contract. Adding a field is
    /// fine; renaming/retyping one of these is a breaking change and must
    /// not slip through unnoticed.
    #[test]
    fn golden_json_wire_shape() {
        let created_at = DateTime::parse_from_rfc3339("2026-07-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut headers = BTreeMap::new();
        headers.insert("x-trace".to_string(), "abc123".to_string());
        let env = Envelope {
            version: "0.1".to_string(),
            content_type: "application/json".to_string(),
            body: json!({"to": "user@example.com"}),
            headers,
            created_at,
        };

        let value: serde_json::Value = serde_json::from_str(&env.to_json().unwrap()).unwrap();
        let obj = value.as_object().expect("envelope serializes to an object");

        // Field names present.
        for field in ["version", "content_type", "body", "headers", "created_at"] {
            assert!(obj.contains_key(field), "missing field '{field}'");
        }

        // Field types/values pinned.
        assert_eq!(obj["version"], json!("0.1"));
        assert_eq!(obj["content_type"], json!("application/json"));
        assert_eq!(obj["body"], json!({"to": "user@example.com"}));
        assert!(
            obj["headers"].is_object(),
            "headers must serialize as a JSON object"
        );
        assert_eq!(
            obj["headers"]["x-trace"],
            json!("abc123"),
            "headers must be a string->string map"
        );

        // created_at must be an RFC3339 string, not a numeric timestamp.
        let created_at_str = obj["created_at"]
            .as_str()
            .expect("created_at must serialize as a string");
        let reparsed =
            DateTime::parse_from_rfc3339(created_at_str).expect("created_at must be valid RFC3339");
        assert_eq!(reparsed.with_timezone(&Utc), created_at);
    }
}
