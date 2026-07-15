use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{Context, Result};
use nostr_q_core::queue::{Delivery, Encryption, QueueConfig, QueueMode};
use rusqlite::Connection;

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
  name TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  delivery TEXT NOT NULL,
  encryption TEXT NOT NULL DEFAULT 'none',
  max_attempts INTEGER NOT NULL DEFAULT 5,
  lease_seconds INTEGER NOT NULL DEFAULT 60,
  retry_base_seconds INTEGER NOT NULL DEFAULT 5,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS relays (
  url TEXT PRIMARY KEY,
  added_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  mid TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  trace_id TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', -- pending | claimed | acked | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  idem_key TEXT,
  consumer TEXT,
  lease_expires_at INTEGER,
  visible_at INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idem
  ON messages(queue, idem_key) WHERE idem_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_queue_status ON messages(queue, status);
CREATE TABLE IF NOT EXISTS dlq (
  mid TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  reason TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  dead_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS lifecycle (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mid TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_trace ON lifecycle(trace_id);
CREATE INDEX IF NOT EXISTS idx_lifecycle_mid ON lifecycle(mid);
"#;

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening sqlite db {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::from_conn(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_conn(Connection::open_in_memory()?)
    }

    fn from_conn(conn: Connection) -> Result<Self> {
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version < 1 {
            conn.execute_batch(SCHEMA_V1)?;
            conn.pragma_update(None, "user_version", 1)?;
        }
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn schema_version(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row("PRAGMA user_version", [], |r| r.get(0))?)
    }

    pub fn upsert_queue(&self, q: &QueueConfig) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO queues (name, mode, delivery, encryption, max_attempts, lease_seconds, retry_base_seconds, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(name) DO UPDATE SET
               mode=excluded.mode, delivery=excluded.delivery, encryption=excluded.encryption,
               max_attempts=excluded.max_attempts, lease_seconds=excluded.lease_seconds,
               retry_base_seconds=excluded.retry_base_seconds",
            rusqlite::params![
                q.name, q.mode.as_str(), q.delivery.as_str(), q.encryption.as_str(),
                q.max_attempts, q.lease_seconds as i64, q.retry_base_seconds as i64, Self::now()
            ],
        )?;
        Ok(())
    }

    fn row_to_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueueConfig> {
        Ok(QueueConfig {
            name: row.get(0)?,
            mode: QueueMode::from_str(&row.get::<_, String>(1)?).unwrap_or(QueueMode::WorkQueue),
            delivery: Delivery::from_str(&row.get::<_, String>(2)?).unwrap_or(Delivery::AtLeastOnce),
            encryption: Encryption::from_str(&row.get::<_, String>(3)?).unwrap_or_default(),
            max_attempts: row.get(4)?,
            lease_seconds: row.get::<_, i64>(5)? as u64,
            retry_base_seconds: row.get::<_, i64>(6)? as u64,
        })
    }

    const QUEUE_COLS: &'static str =
        "name, mode, delivery, encryption, max_attempts, lease_seconds, retry_base_seconds";

    pub fn get_queue(&self, name: &str) -> Result<Option<QueueConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM queues WHERE name = ?1", Self::QUEUE_COLS
        ))?;
        let mut rows = stmt.query_map([name], Self::row_to_queue)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_queues(&self) -> Result<Vec<QueueConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM queues ORDER BY name", Self::QUEUE_COLS
        ))?;
        let rows = stmt.query_map([], Self::row_to_queue)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn add_relay(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO relays (url, added_at) VALUES (?1, ?2)",
            rusqlite::params![url, Self::now()],
        )?;
        Ok(())
    }

    pub fn list_relays(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT url FROM relays ORDER BY url")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn remove_relay(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM relays WHERE url = ?1", [url])?;
        Ok(())
    }

    pub(crate) fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_schema_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.schema_version().unwrap(), 1);
        }
        // reopening an existing db must not fail or re-run migrations destructively
        let store = Store::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }

    #[test]
    fn in_memory_store_works() {
        let store = Store::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), 1);
    }

    #[test]
    fn queue_crud_roundtrip() {
        use nostr_q_core::queue::QueueConfig;
        let store = Store::open_in_memory().unwrap();
        let q = QueueConfig::work_queue("jobs.email");
        store.upsert_queue(&q).unwrap();
        assert_eq!(store.get_queue("jobs.email").unwrap().unwrap(), q);
        assert!(store.get_queue("nope").unwrap().is_none());
        // upsert overwrites
        let mut q2 = q.clone();
        q2.max_attempts = 9;
        store.upsert_queue(&q2).unwrap();
        assert_eq!(store.get_queue("jobs.email").unwrap().unwrap().max_attempts, 9);
        store.upsert_queue(&QueueConfig::pubsub("events.x")).unwrap();
        assert_eq!(store.list_queues().unwrap().len(), 2);
    }

    #[test]
    fn relay_crud() {
        let store = Store::open_in_memory().unwrap();
        store.add_relay("wss://relay.example.com").unwrap();
        store.add_relay("wss://relay.example.com").unwrap(); // idempotent
        assert_eq!(store.list_relays().unwrap(), vec!["wss://relay.example.com".to_string()]);
        store.remove_relay("wss://relay.example.com").unwrap();
        assert!(store.list_relays().unwrap().is_empty());
    }
}
