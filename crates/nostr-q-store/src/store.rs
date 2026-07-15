use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{Context, Result};
use nostr_q_core::queue::{Delivery, Encryption, QueueConfig, QueueMode};
use rusqlite::Connection;
use serde::Serialize;

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
  attempt_floor INTEGER NOT NULL DEFAULT 0,
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

#[derive(Debug, Clone, Serialize)]
pub struct MessageRecord {
    pub mid: String,
    pub queue: String,
    pub event_id: String,
    pub trace_id: String,
    pub envelope_json: String,
    pub status: String,
    pub attempts: u32,
    pub attempt_floor: u32,
    pub idem_key: Option<String>,
    pub visible_at: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DlqRecord {
    pub mid: String,
    pub queue: String,
    pub reason: String,
    pub attempts: u32,
    pub dead_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LifecycleRecord {
    pub mid: String,
    pub trace_id: String,
    pub kind: String,
    pub detail: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub pending: u32,
    pub in_flight: u32,
    pub acked: u32,
    pub dead: u32,
    pub oldest_pending_age_secs: Option<i64>,
}

const MSG_COLS: &str = "mid, queue, event_id, trace_id, envelope_json, status, attempts, attempt_floor, idem_key, visible_at, created_at";

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    Ok(MessageRecord {
        mid: row.get(0)?,
        queue: row.get(1)?,
        event_id: row.get(2)?,
        trace_id: row.get(3)?,
        envelope_json: row.get(4)?,
        status: row.get(5)?,
        attempts: row.get(6)?,
        attempt_floor: row.get(7)?,
        idem_key: row.get(8)?,
        visible_at: row.get(9)?,
        created_at: row.get(10)?,
    })
}

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
        Ok(Self {
            conn: Mutex::new(conn),
        })
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
            delivery: Delivery::from_str(&row.get::<_, String>(2)?)
                .unwrap_or(Delivery::AtLeastOnce),
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
            "SELECT {} FROM queues WHERE name = ?1",
            Self::QUEUE_COLS
        ))?;
        let mut rows = stmt.query_map([name], Self::row_to_queue)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_queues(&self) -> Result<Vec<QueueConfig>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM queues ORDER BY name",
            Self::QUEUE_COLS
        ))?;
        let rows = stmt.query_map([], Self::row_to_queue)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Delete a queue's config. A no-op (not an error) if the queue doesn't
    /// exist, matching `remove_relay`'s idempotent-delete semantics — unlike
    /// the message mutators below, callers don't need "did this exist" here.
    pub fn remove_queue(&self, name: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM queues WHERE name = ?1", [name])?;
        Ok(())
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

    pub fn insert_message(&self, rec: &MessageRecord) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "INSERT OR IGNORE INTO messages
               (mid, queue, event_id, trace_id, envelope_json, status, attempts, attempt_floor, idem_key, visible_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                rec.mid, rec.queue, rec.event_id, rec.trace_id, rec.envelope_json,
                rec.status, rec.attempts, rec.attempt_floor, rec.idem_key, rec.visible_at, rec.created_at, Self::now()
            ],
        )?;
        Ok(n == 1)
    }

    pub fn get_message(&self, mid: &str) -> Result<Option<MessageRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!("SELECT {MSG_COLS} FROM messages WHERE mid = ?1"))?;
        let mut rows = stmt.query_map([mid], row_to_message)?;
        Ok(rows.next().transpose()?)
    }

    pub fn find_by_idem(&self, queue: &str, idem_key: &str) -> Result<Option<MessageRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {MSG_COLS} FROM messages WHERE queue = ?1 AND idem_key = ?2"
        ))?;
        let mut rows = stmt.query_map(rusqlite::params![queue, idem_key], row_to_message)?;
        Ok(rows.next().transpose()?)
    }

    pub fn claimable(&self, queue: &str, now: i64, limit: u32) -> Result<Vec<MessageRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {MSG_COLS} FROM messages
             WHERE queue = ?1 AND (
               (status = 'pending' AND visible_at <= ?2)
               OR (status = 'claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?2)
             )
             ORDER BY created_at ASC LIMIT ?3"
        ))?;
        let rows = stmt.query_map(rusqlite::params![queue, now, limit], row_to_message)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Run an UPDATE/INSERT and error if it affected zero rows, naming `mid`
    /// in the error. Every mutator here operates on a specific message id,
    /// so an unknown mid is treated as a caller error (matching
    /// `incr_attempts`'s existing "unknown mid is an error" stance) rather
    /// than a silent no-op.
    fn update(&self, sql: &str, params: impl rusqlite::Params, mid: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(sql, params)?;
        if n == 0 {
            anyhow::bail!("no message with mid {mid}");
        }
        Ok(())
    }

    /// Mark a message claimed by `consumer`, healing the local `attempts`
    /// counter up to `attempt` in the process.
    ///
    /// The winning claim's `attempt` is the globally observed retry
    /// generation (see `try_claim`'s `claim_attempt`), which may be ahead of
    /// this worker's own local counter when another worker advanced the
    /// generation first. Persisting `MAX(attempts, attempt)` here keeps this
    /// worker's counter in sync with the generation it just won, so that if
    /// this worker goes on to nack the message, `incr_attempts` advances the
    /// generation forward instead of colliding with (or trailing behind) the
    /// claim it just made.
    pub fn mark_claimed(
        &self,
        mid: &str,
        consumer: &str,
        lease_expires_at: i64,
        attempt: u32,
    ) -> Result<()> {
        self.update(
            "UPDATE messages SET status='claimed', consumer=?2, lease_expires_at=?3, \
             attempts = MAX(attempts, ?4), updated_at=?5 WHERE mid=?1",
            rusqlite::params![mid, consumer, lease_expires_at, attempt, Self::now()],
            mid,
        )
    }

    pub fn mark_acked(&self, mid: &str) -> Result<()> {
        self.update(
            "UPDATE messages SET status='acked', lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
            mid,
        )
    }

    pub fn mark_pending(&self, mid: &str, visible_at: i64) -> Result<()> {
        self.update(
            "UPDATE messages SET status='pending', lease_expires_at=NULL, visible_at=?2, updated_at=?3 WHERE mid=?1",
            rusqlite::params![mid, visible_at, Self::now()],
            mid,
        )
    }

    pub fn incr_attempts(&self, mid: &str) -> Result<u32> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE messages SET attempts = attempts + 1, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        Ok(
            conn.query_row("SELECT attempts FROM messages WHERE mid=?1", [mid], |r| {
                r.get(0)
            })?,
        )
    }

    pub fn move_to_dlq(&self, mid: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE messages SET status='dead', lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        if n == 0 {
            anyhow::bail!("no message with mid {mid}");
        }
        conn.execute(
            "INSERT OR REPLACE INTO dlq (mid, queue, reason, attempts, dead_at)
             SELECT mid, queue, ?2, attempts, ?3 FROM messages WHERE mid=?1",
            rusqlite::params![mid, reason, Self::now()],
        )?;
        Ok(())
    }

    /// Mark a message dead because this worker *observed* a remote DLQ
    /// event (rather than dead-lettering it itself), healing the local
    /// `attempts` counter up to that event's `attempt` generation in the
    /// process.
    ///
    /// Without this healing, a worker that never nacked a message locally
    /// (so `attempts` stayed at 0) but later saw a remote dlq event with
    /// `attempt > 0` would mark the row dead with `attempts` still at 0. A
    /// subsequent `dlq_retry` sets `attempt_floor = attempts = 0`, which
    /// leaves the historical remote dlq event's `attempt` (> 0) still
    /// greater than the new floor — so `try_claim`'s terminal check
    /// immediately re-kills the row on the very next survey, silently
    /// undoing the retry. Healing `attempts` to `MAX(attempts, attempt)`
    /// here (the same pattern `mark_claimed` uses) means `dlq_retry` sets
    /// `attempt_floor` to that healed value, so the historical dlq event's
    /// `attempt` is `<= attempt_floor` afterward — not terminal — and the
    /// retry grants a real fresh budget instead of being immediately
    /// undone.
    pub fn move_to_dlq_at(&self, mid: &str, reason: &str, attempt: u32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE messages SET status='dead', attempts = MAX(attempts, ?3), \
             lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now(), attempt],
        )?;
        if n == 0 {
            anyhow::bail!("no message with mid {mid}");
        }
        conn.execute(
            "INSERT OR REPLACE INTO dlq (mid, queue, reason, attempts, dead_at)
             SELECT mid, queue, ?2, attempts, ?3 FROM messages WHERE mid=?1",
            rusqlite::params![mid, reason, Self::now()],
        )?;
        Ok(())
    }

    pub fn dlq_list(&self, queue: Option<&str>) -> Result<Vec<DlqRecord>> {
        let conn = self.conn.lock().unwrap();
        let map = |row: &rusqlite::Row<'_>| -> rusqlite::Result<DlqRecord> {
            Ok(DlqRecord {
                mid: row.get(0)?,
                queue: row.get(1)?,
                reason: row.get(2)?,
                attempts: row.get(3)?,
                dead_at: row.get(4)?,
            })
        };
        let sql_all = "SELECT mid, queue, reason, attempts, dead_at FROM dlq ORDER BY dead_at";
        let sql_q =
            "SELECT mid, queue, reason, attempts, dead_at FROM dlq WHERE queue=?1 ORDER BY dead_at";
        let out = match queue {
            Some(q) => {
                let mut stmt = conn.prepare(sql_q)?;
                let rows = stmt.query_map([q], map)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            }
            None => {
                let mut stmt = conn.prepare(sql_all)?;
                let rows = stmt.query_map([], map)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            }
        };
        Ok(out)
    }

    /// Requeue a dead-lettered message for retry.
    ///
    /// `attempts` is the global retry generation and must stay monotonic:
    /// `try_claim` heals it up to the highest attempt observed across all
    /// historical nack events on the relay (which are never deleted), so
    /// resetting it here would just have `try_claim` heal it right back to
    /// its pre-retry value. Instead we leave `attempts` untouched and record
    /// an `attempt_floor` marking where the fresh DLQ budget starts; `nack`
    /// then measures the budget as `attempts - attempt_floor`.
    pub fn dlq_retry(&self, mid: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE messages SET status='pending', attempt_floor=attempts, visible_at=0, lease_expires_at=NULL, updated_at=?2 WHERE mid=?1",
            rusqlite::params![mid, Self::now()],
        )?;
        if n == 0 {
            anyhow::bail!("no message with mid {mid}");
        }
        conn.execute("DELETE FROM dlq WHERE mid=?1", [mid])?;
        Ok(())
    }

    pub fn record_lifecycle(
        &self,
        mid: &str,
        trace_id: &str,
        kind: &str,
        detail: &str,
    ) -> Result<()> {
        self.update(
            "INSERT INTO lifecycle (mid, trace_id, kind, detail, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![mid, trace_id, kind, detail, Self::now()],
            mid,
        )
    }

    pub fn trace(&self, trace_id: &str) -> Result<Vec<LifecycleRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT mid, trace_id, kind, detail, created_at FROM lifecycle WHERE trace_id=?1 ORDER BY id",
        )?;
        let rows = stmt.query_map([trace_id], |row| {
            Ok(LifecycleRecord {
                mid: row.get(0)?,
                trace_id: row.get(1)?,
                kind: row.get(2)?,
                detail: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn trace_id_for_mid(&self, mid: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT trace_id FROM messages WHERE mid=?1")?;
        let mut rows = stmt.query_map([mid], |r| r.get(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn stats(&self, queue: &str, now: i64) -> Result<QueueStats> {
        let conn = self.conn.lock().unwrap();
        let count = |status: &str| -> rusqlite::Result<u32> {
            conn.query_row(
                "SELECT COUNT(*) FROM messages WHERE queue=?1 AND status=?2",
                rusqlite::params![queue, status],
                |r| r.get(0),
            )
        };
        let oldest: Option<i64> = conn.query_row(
            "SELECT MIN(created_at) FROM messages WHERE queue=?1 AND status='pending'",
            [queue],
            |r| r.get(0),
        )?;
        Ok(QueueStats {
            pending: count("pending")?,
            in_flight: count("claimed")?,
            acked: count("acked")?,
            dead: count("dead")?,
            oldest_pending_age_secs: oldest.map(|c| now - c),
        })
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
        assert_eq!(
            store.get_queue("jobs.email").unwrap().unwrap().max_attempts,
            9
        );
        store
            .upsert_queue(&QueueConfig::pubsub("events.x"))
            .unwrap();
        assert_eq!(store.list_queues().unwrap().len(), 2);
    }

    #[test]
    fn relay_crud() {
        let store = Store::open_in_memory().unwrap();
        store.add_relay("wss://relay.example.com").unwrap();
        store.add_relay("wss://relay.example.com").unwrap(); // idempotent
        assert_eq!(
            store.list_relays().unwrap(),
            vec!["wss://relay.example.com".to_string()]
        );
        store.remove_relay("wss://relay.example.com").unwrap();
        assert!(store.list_relays().unwrap().is_empty());
    }

    fn rec(mid: &str, queue: &str) -> MessageRecord {
        MessageRecord {
            mid: mid.into(),
            queue: queue.into(),
            event_id: format!("ev-{mid}"),
            trace_id: format!("tr-{mid}"),
            envelope_json: "{}".into(),
            status: "pending".into(),
            attempts: 0,
            attempt_floor: 0,
            idem_key: None,
            visible_at: 0,
            created_at: 100,
        }
    }

    #[test]
    fn message_lifecycle_pending_claim_ack() {
        let store = Store::open_in_memory().unwrap();
        assert!(store.insert_message(&rec("m1", "q")).unwrap());
        assert!(!store.insert_message(&rec("m1", "q")).unwrap()); // dup event_id ignored

        let claimable = store.claimable("q", 1000, 10).unwrap();
        assert_eq!(claimable.len(), 1);
        store.mark_claimed("m1", "pubkey-a", 2000, 0).unwrap();
        assert!(store.claimable("q", 1000, 10).unwrap().is_empty()); // lease active
        assert_eq!(store.claimable("q", 2001, 10).unwrap().len(), 1); // lease expired -> reclaimable
        store.mark_acked("m1").unwrap();
        assert!(store.claimable("q", 3000, 10).unwrap().is_empty());
        assert_eq!(store.get_message("m1").unwrap().unwrap().status, "acked");

        // mark_claimed heals the local attempts counter to the claim's generation
        assert!(store.insert_message(&rec("m2", "q")).unwrap());
        store.mark_claimed("m2", "pk", 2000, 3).unwrap();
        assert_eq!(store.get_message("m2").unwrap().unwrap().attempts, 3);
    }

    #[test]
    fn idempotency_key_dedupes() {
        let store = Store::open_in_memory().unwrap();
        let mut a = rec("m1", "q");
        a.idem_key = Some("order-42".into());
        let mut b = rec("m2", "q");
        b.idem_key = Some("order-42".into());
        assert!(store.insert_message(&a).unwrap());
        assert!(!store.insert_message(&b).unwrap());
    }

    #[test]
    fn find_by_idem_returns_existing() {
        let store = Store::open_in_memory().unwrap();
        let mut a = rec("m1", "q");
        a.idem_key = Some("order-42".into());
        store.insert_message(&a).unwrap();
        let found = store.find_by_idem("q", "order-42").unwrap().unwrap();
        assert_eq!(found.mid, "m1");
        assert!(store.find_by_idem("q", "nope").unwrap().is_none());
    }

    #[test]
    fn visible_at_defers_retry() {
        let store = Store::open_in_memory().unwrap();
        store.insert_message(&rec("m1", "q")).unwrap();
        store.mark_pending("m1", 5000).unwrap();
        assert!(store.claimable("q", 4999, 10).unwrap().is_empty());
        assert_eq!(store.claimable("q", 5000, 10).unwrap().len(), 1);
    }

    #[test]
    fn dlq_flow() {
        let store = Store::open_in_memory().unwrap();
        store.insert_message(&rec("m1", "q")).unwrap();
        assert_eq!(store.incr_attempts("m1").unwrap(), 1);
        assert_eq!(store.incr_attempts("m1").unwrap(), 2);
        store.move_to_dlq("m1", "handler exit 1").unwrap();
        assert!(store.claimable("q", 9999, 10).unwrap().is_empty());
        let dlq = store.dlq_list(Some("q")).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].reason, "handler exit 1");
        assert_eq!(dlq[0].attempts, 2);
        store.dlq_retry("m1").unwrap();
        assert!(store.dlq_list(None).unwrap().is_empty());
        let m = store.get_message("m1").unwrap().unwrap();
        assert_eq!(m.status, "pending");
        assert_eq!(m.attempts, 2, "attempts stays monotonic across a DLQ retry");
        assert_eq!(
            m.attempt_floor, 2,
            "floor marks where the fresh budget starts"
        );
    }

    #[test]
    fn missing_mid_mutators_error_instead_of_silently_no_op() {
        let store = Store::open_in_memory().unwrap();
        let err = store.mark_claimed("ghost", "pk", 1000, 0).unwrap_err();
        assert!(err.to_string().contains("ghost"));
        assert!(store.mark_acked("ghost").is_err());
        assert!(store.mark_pending("ghost", 1000).is_err());
        assert!(store.move_to_dlq("ghost", "reason").is_err());
        assert!(store.dlq_retry("ghost").is_err());
    }

    #[test]
    fn remove_queue_deletes_config_and_is_idempotent() {
        use nostr_q_core::queue::QueueConfig;
        let store = Store::open_in_memory().unwrap();
        store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();
        assert!(store.get_queue("jobs.email").unwrap().is_some());
        store.remove_queue("jobs.email").unwrap();
        assert!(store.get_queue("jobs.email").unwrap().is_none());
        store.remove_queue("jobs.email").unwrap(); // idempotent, not an error
    }

    #[test]
    fn trace_and_stats() {
        let store = Store::open_in_memory().unwrap();
        store.insert_message(&rec("m1", "q")).unwrap();
        store
            .record_lifecycle("m1", "tr-m1", "published", "q")
            .unwrap();
        store
            .record_lifecycle("m1", "tr-m1", "claimed", "pubkey-a")
            .unwrap();
        let t = store.trace("tr-m1").unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].kind, "published");
        assert_eq!(
            store.trace_id_for_mid("m1").unwrap().as_deref(),
            Some("tr-m1")
        );

        let stats = store.stats("q", 200).unwrap();
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.oldest_pending_age_secs, Some(100)); // created_at=100, now=200
    }
}
