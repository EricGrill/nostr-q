use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    WorkQueue,
    Pubsub,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    BestEffort,
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Encryption {
    #[default]
    None,
    Nip04,
    Nip44,
}

macro_rules! str_enum {
    ($ty:ty { $($variant:path => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(&self) -> &'static str {
                match self { $($variant => $s),+ }
            }
        }
        impl FromStr for $ty {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($s => Ok($variant),)+
                    other => Err(format!("invalid value '{other}' for {}", stringify!($ty))),
                }
            }
        }
    };
}

str_enum!(QueueMode { QueueMode::WorkQueue => "work_queue", QueueMode::Pubsub => "pubsub" });
str_enum!(Delivery {
    Delivery::BestEffort => "best_effort",
    Delivery::AtMostOnce => "at_most_once",
    Delivery::AtLeastOnce => "at_least_once",
});
str_enum!(Encryption { Encryption::None => "none", Encryption::Nip04 => "nip04", Encryption::Nip44 => "nip44" });

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub mode: QueueMode,
    pub delivery: Delivery,
    #[serde(default)]
    pub encryption: Encryption,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_lease_seconds")]
    pub lease_seconds: u64,
    #[serde(default = "default_retry_base_seconds")]
    pub retry_base_seconds: u64,
}

fn default_max_attempts() -> u32 {
    5
}
fn default_lease_seconds() -> u64 {
    60
}
fn default_retry_base_seconds() -> u64 {
    5
}

impl QueueConfig {
    pub fn work_queue(name: &str) -> Self {
        Self {
            name: name.to_string(),
            mode: QueueMode::WorkQueue,
            delivery: Delivery::AtLeastOnce,
            encryption: Encryption::None,
            max_attempts: default_max_attempts(),
            lease_seconds: default_lease_seconds(),
            retry_base_seconds: default_retry_base_seconds(),
        }
    }

    pub fn pubsub(name: &str) -> Self {
        Self {
            name: name.to_string(),
            mode: QueueMode::Pubsub,
            delivery: Delivery::BestEffort,
            encryption: Encryption::None,
            max_attempts: default_max_attempts(),
            lease_seconds: default_lease_seconds(),
            retry_base_seconds: default_retry_base_seconds(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn work_queue_defaults() {
        let q = QueueConfig::work_queue("jobs.email");
        assert_eq!(q.mode, QueueMode::WorkQueue);
        assert_eq!(q.delivery, Delivery::AtLeastOnce);
        assert_eq!(q.encryption, Encryption::None);
        assert_eq!(q.max_attempts, 5);
        assert_eq!(q.lease_seconds, 60);
        assert_eq!(q.retry_base_seconds, 5);
    }

    #[test]
    fn pubsub_defaults() {
        let q = QueueConfig::pubsub("events.user.created");
        assert_eq!(q.mode, QueueMode::Pubsub);
        assert_eq!(q.delivery, Delivery::BestEffort);
    }

    #[test]
    fn parse_from_cli_strings() {
        assert_eq!(
            QueueMode::from_str("work_queue").unwrap(),
            QueueMode::WorkQueue
        );
        assert_eq!(
            Delivery::from_str("at_least_once").unwrap(),
            Delivery::AtLeastOnce
        );
        assert!(QueueMode::from_str("bogus").is_err());
        assert_eq!(QueueMode::WorkQueue.as_str(), "work_queue");
    }

    #[test]
    fn serde_snake_case_roundtrip() {
        let q = QueueConfig::work_queue("jobs.email");
        let json = serde_json::to_string(&q).unwrap();
        assert!(json.contains("\"mode\":\"work_queue\""), "{json}");
        assert!(json.contains("\"delivery\":\"at_least_once\""), "{json}");
        assert!(json.contains("\"encryption\":\"none\""), "{json}");
        let back: QueueConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, q);
        // encryption/max_attempts/lease/retry_base may be omitted and default
        let sparse: QueueConfig =
            serde_json::from_str(r#"{"name":"x","mode":"pubsub","delivery":"best_effort"}"#)
                .unwrap();
        assert_eq!(sparse.encryption, Encryption::None);
        assert_eq!(sparse.max_attempts, 5);
        assert_eq!(sparse.lease_seconds, 60);
        assert_eq!(sparse.retry_base_seconds, 5);
    }
}
