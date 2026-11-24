//! `findings_changed` notices (`event.broca.findings_changed.v1`): one stored event each time
//! the set of findings `diagnostics.findings` reports changes, so a flow triggered by the event
//! reads the findings within seconds instead of on its next scheduled pass.
//!
//! The shape is the one synapse publishes, the other module the self-diagnosis flow is
//! triggered by: a claim-check notice whose payload is empty, whose message id and `event_key`
//! header are the event's key, and whose digest is the stored body's. A reader fetches the body
//! with `events_get` for [`FINDINGS_RETENTION_MS`] after it was written. The body says only how
//! many changes there have been and the worst severity now; what the findings are is read from
//! `diagnostics.findings` itself.
//!
//! The event is written in the transaction that records the new findings digest, so a change is
//! never recorded without its event, and a crash before the bus acknowledges it leaves the row
//! for the drain to send again, under the same message id the broker deduplicates on.

use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::Headers;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::bus_events::Publication;
use crate::{SqliteStore, StoreError};

pub const FINDINGS_EVENT: &str = "findings_changed";
pub const FINDINGS_VERSION: u32 = 1;
/// How long an event's body stays readable with `events_get`.
pub const FINDINGS_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// The notice header carrying the event's key, declared with the event in broca's manifest.
pub const EVENT_KEY_HEADER: &str = "event_key";

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS findings_event_outbox (
            event_key TEXT PRIMARY KEY, generation INTEGER NOT NULL, subject TEXT NOT NULL,
            body BLOB NOT NULL, at_ms INTEGER NOT NULL, published_ms INTEGER
         );
         CREATE INDEX IF NOT EXISTS findings_event_unpublished
            ON findings_event_outbox(at_ms) WHERE published_ms IS NULL;
         -- The digest of the findings the newest event reported, and how many events there have
         -- been. One row; absent until the first event.
         CREATE TABLE IF NOT EXISTS findings_state (
            id INTEGER PRIMARY KEY CHECK (id = 1), generation INTEGER NOT NULL,
            digest TEXT NOT NULL
         );",
    )
}

/// An event's stored body, `{event_id, generation, worst_severity}`, in that member order.
#[derive(Serialize)]
struct Body<'a> {
    event_id: &'a str,
    generation: u64,
    worst_severity: &'a str,
}

/// A stored event, as `events_get` answers it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredFindingsEvent {
    pub body: Vec<u8>,
    /// When the event was written, which is also when its findings were recorded.
    pub at_ms: u64,
}

/// A random version 4 UUID, the event key synapse's events use too.
fn event_key() -> Result<String, StoreError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| StoreError::BusEvent(error.to_string()))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

fn publication(event_key: String, subject: String, body: Vec<u8>) -> Publication {
    Publication {
        headers: Headers::from([(EVENT_KEY_HEADER.into(), event_key.clone())]),
        event_key,
        subject,
        body,
        digest: None,
        inline: false,
    }
}

impl SqliteStore {
    /// Records the findings `diagnostics.findings` reports now: `digest` names them and
    /// `worst_severity` is `ok`, `warn` or `error`. When `digest` differs from the last one
    /// recorded, writes an event for it and returns the event's key; otherwise records nothing.
    /// Also records nothing until the bus account is known, since the event's subject names it,
    /// so the change is recorded on the first call after that.
    pub fn record_findings(
        &self,
        digest: &str,
        worst_severity: &str,
        now_ms: u64,
    ) -> Result<Option<String>, StoreError> {
        let Some(account) = self.bus_events.account.lock().unwrap().clone() else {
            return Ok(None);
        };
        let subject = subject(&account)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let last: Option<(u64, String)> = tx
            .query_row(
                "SELECT generation,digest FROM findings_state WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if last.as_ref().is_some_and(|(_, last)| last == digest) {
            return Ok(None);
        }
        let generation = last.map_or(1, |(generation, _)| generation + 1);
        let key = event_key()?;
        let body = serde_json::to_vec(&Body {
            event_id: &key,
            generation,
            worst_severity,
        })?;
        tx.execute(
            "INSERT INTO findings_state(id,generation,digest) VALUES(1,?1,?2)
             ON CONFLICT(id) DO UPDATE SET generation=?1, digest=?2",
            params![generation, digest],
        )?;
        tx.execute(
            "INSERT INTO findings_event_outbox(event_key,generation,subject,body,at_ms)
             VALUES(?1,?2,?3,?4,?5)",
            params![key, generation, subject, body, now_ms],
        )?;
        tx.commit()?;
        Ok(Some(key))
    }

    /// The stored event `event_key`, while it is kept; readable before its notice is sent.
    pub fn findings_event(
        &self,
        event_key: &str,
    ) -> Result<Option<StoredFindingsEvent>, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT body,at_ms FROM findings_event_outbox WHERE event_key=?1",
                [event_key],
                |r| {
                    Ok(StoredFindingsEvent {
                        body: r.get(0)?,
                        at_ms: r.get(1)?,
                    })
                },
            )
            .optional()?)
    }
}

fn subject(account: &AccountNames) -> Result<String, StoreError> {
    account
        .event_subject("broca", FINDINGS_EVENT, FINDINGS_VERSION)
        .map_err(|error| StoreError::BusEvent(error.to_string()))
}

/// The oldest event not yet acknowledged by the bus.
pub(crate) fn pending(conn: &Connection) -> Result<Option<Publication>, StoreError> {
    Ok(conn
        .query_row(
            "SELECT event_key,subject,body FROM findings_event_outbox
             INDEXED BY findings_event_unpublished
             WHERE published_ms IS NULL ORDER BY at_ms, rowid LIMIT 1",
            [],
            |r| Ok(publication(r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?)
}

/// Marks event `event_key` sent; returns how many rows that changed (zero or one).
pub(crate) fn acknowledge(
    conn: &Connection,
    event_key: &str,
    now_ms: u64,
) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "UPDATE findings_event_outbox SET published_ms=?2 WHERE event_key=?1 AND published_ms IS NULL",
        params![event_key, now_ms],
    )
}

/// Deletes events older than [`FINDINGS_RETENTION_MS`], sent or not: a notice that old would
/// wake a flow to read findings that have long changed.
pub(crate) fn prune(conn: &Connection, now_ms: u64) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "DELETE FROM findings_event_outbox WHERE at_ms<?1",
        [now_ms.saturating_sub(FINDINGS_RETENTION_MS)],
    )
}

#[cfg(test)]
#[path = "findings_events_tests.rs"]
mod tests;
