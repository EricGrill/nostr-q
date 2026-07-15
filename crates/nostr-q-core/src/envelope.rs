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
}
