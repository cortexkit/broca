//! Durable claim-check bodies and pending bus publications, recorded from records a session
//! barrier made durable.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use broca_core::session_refs::agent_session_ref;
use broca_protocol::{
    run_events::{EncodeError, RunEvent, RunEventDetail},
    send_states::SendStateNotice,
    BindIdentity,
};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{ContentDigest, Headers, PublishReceipt};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SqliteStore, StoreError};

pub const RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

/// How old a queued run event may be, by its own `at_ms`, and still be published when a bus
/// connection comes up. Older ones are replaced by one `run.skipped` marker per session.
///
/// Core polls every run while events are down, so an event this old tells it nothing new, and
/// replaying it does harm: each stale `run.ended` sets off a status poll, so a backlog becomes
/// a burst, and a stale `run.progress` refreshes a worker's liveness clock with replay time,
/// hiding a worker that is stuck. A replay shorter than two minutes cannot hide anything, and
/// publishing it keeps a brief broker blip from delaying the run ends Core is waiting for.
pub const STALE_BACKLOG_MS: u64 = 2 * 60 * 1000;

/// The event name of the marker that stands in for skipped stale events of one session.
pub const SKIPPED_EVENT: &str = "run.skipped";

/// What one stale-backlog pass replaced with markers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SkipSummary {
    /// Sessions that received a marker.
    pub sessions: u64,
    /// Queued events the markers replaced.
    pub skipped: u64,
}

/// The body of a `run.skipped` marker: which of one session's events were not published.
/// `from_seq` and `to_seq` are the first and last skipped events in record order; `from_ms` and
/// `to_ms` are the earliest and latest of their own clocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipMarker {
    pub session_ref: String,
    pub from_seq: u64,
    pub to_seq: u64,
    pub from_ms: u64,
    pub to_ms: u64,
    pub count: u64,
}

#[derive(Default)]
pub struct Metrics {
    pub bus_events_published: AtomicU64,
    pub bus_outbox_depth: AtomicU64,
    /// Unpublished advance notices only. A publish receipt is not a consumer acknowledgement.
    pub bus_publish_failures: AtomicU64,
    pub bus_events_oversize: AtomicU64,
    pub bus_outbox_dropped: AtomicU64,
}

impl Metrics {
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "bus_events_published": self.bus_events_published.load(Ordering::Relaxed),
            "bus_outbox_depth": self.bus_outbox_depth.load(Ordering::Relaxed),
            "bus_publish_failures": self.bus_publish_failures.load(Ordering::Relaxed),
            "bus_events_oversize": self.bus_events_oversize.load(Ordering::Relaxed),
            "bus_outbox_dropped": self.bus_outbox_dropped.load(Ordering::Relaxed),
        })
    }
}

#[derive(Default)]
pub(crate) struct State {
    account: Mutex<Option<AccountNames>>,
    pub(crate) metrics: Arc<Metrics>,
    #[cfg(test)]
    before_write_unlock: Mutex<Option<WriteObserver>>,
}

/// Adds a run event's `session` header: the plain session name of the session's bind triple,
/// which a module that sees only its own route's session name (no project root, so it can't
/// compute `session_ref`) can match on. A NATS header holds printable ASCII only, so a name
/// with any other character is left out rather than failing the publish.
fn with_session_header(mut headers: Headers, session: &str) -> Headers {
    if !session.is_empty() && session.bytes().all(|byte| (0x20..0x7f).contains(&byte)) {
        headers.insert("session".into(), session.into());
    }
    headers
}

#[cfg(test)]
type WriteObserver = Box<dyn Fn(&Connection) + Send + Sync>;

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS bus_event_outbox (
            event_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, seq INTEGER NOT NULL,
            subject TEXT NOT NULL, epoch TEXT NOT NULL, session TEXT NOT NULL,
            body BLOB NOT NULL, at_ms INTEGER NOT NULL, published_ms INTEGER,
            best_effort INTEGER NOT NULL DEFAULT 0,
            UNIQUE(epoch, run_id, seq, subject)
         );
         CREATE INDEX IF NOT EXISTS bus_event_pending ON bus_event_outbox(published_ms, at_ms);
         -- Unpublished lifecycle rows only, in insertion order (every key is 0, so entries sort
         -- by rowid). The drain's next-row query and the startup depth count read this index
         -- instead of scanning a table that also holds a day of published bodies.
         CREATE INDEX IF NOT EXISTS bus_event_unpublished ON bus_event_outbox(best_effort)
            WHERE published_ms IS NULL AND best_effort=0;
         CREATE INDEX IF NOT EXISTS bus_event_age ON bus_event_outbox(at_ms);
         -- Send state notices (`event.broca.send_state.v1.{sender}`), one row per change of a
         -- keyed send. The message id is the notice's own (`{session_ref}:{send_id}:{seq}`), so
         -- recording a change twice adds nothing. Published after run events, in insertion order.
         CREATE TABLE IF NOT EXISTS send_state_outbox (
            message_id TEXT PRIMARY KEY, subject TEXT NOT NULL, session_ref TEXT NOT NULL,
            send_id TEXT NOT NULL, seq INTEGER NOT NULL, body BLOB NOT NULL,
            at_ms INTEGER NOT NULL, published_ms INTEGER
         );
         CREATE INDEX IF NOT EXISTS send_state_unpublished
            ON send_state_outbox(at_ms) WHERE published_ms IS NULL;",
    )
}

#[derive(Debug, Clone)]
pub struct Publication {
    /// The bus message id: the event's claim-check key for a run event,
    /// `<session_ref>:advanced:<seq>` for a compaction advance notice, and
    /// `<session_ref>:<send_id>:<seq>` for a send state notice.
    pub event_key: String,
    pub subject: String,
    pub headers: Headers,
    pub body: Vec<u8>,
    /// The bus digest when it is not the body's: a compaction advance notice carries no body,
    /// and its digest is the notice digest over its headers.
    pub digest: Option<ContentDigest>,
    /// Whether the body travels as the message's payload. A send state notice does: it is
    /// small, and its subscriber reads it straight off the message. The others carry an empty
    /// payload and a digest: a run event's body is fetched with `run.event.get`, and a compaction
    /// advance notice has no body (its digest covers its headers).
    pub inline: bool,
}

impl Publication {
    pub fn digest(&self) -> ContentDigest {
        self.digest
            .unwrap_or_else(|| ContentDigest::of_bytes(&self.body))
    }
}

#[async_trait]
pub trait BusPublisher: Send + Sync {
    /// Success is a JetStream acknowledgement, never merely a successful socket write.
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum DrainResult {
    Empty,
    Published,
    Retry,
}

/// Durable lifecycle publications waiting for the bus. A broker acknowledgement is required
/// before a pending publication is retired.
pub trait RunEventStore: Send + Sync {
    fn enable_bus_events(&self, account: &str) -> Result<(), String>;
    fn bus_metrics(&self) -> &Metrics;
    fn bus_metrics_arc(&self) -> Arc<Metrics>;
    fn retain_bus_progress(
        &self,
        session: &BindIdentity,
        epoch: &str,
        event: &RunEvent,
        now_ms: u64,
    ) -> Result<Option<Publication>, StoreError>;
    fn get_bus_event(&self, event_key: &str)
        -> Result<Option<(BindIdentity, Vec<u8>)>, StoreError>;
    fn pending_bus_event(&self) -> Result<Option<Publication>, StoreError>;
    fn acknowledge_bus_event(
        &self,
        event: &Publication,
        receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError>;
    fn skip_stale_bus_events(&self, now_ms: u64) -> Result<SkipSummary, StoreError>;
    fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError>;
    /// Records the run events a session derived from records a barrier made durable, as
    /// lifecycle publications. `false`, recording nothing, until [`Self::enable_bus_events`]
    /// has named the bus account, since each event's subject and id name it.
    fn record_run_events(
        &self,
        session: &BindIdentity,
        epoch: &str,
        events: &mut [RunEvent],
        now_ms: u64,
    ) -> Result<bool, StoreError>;
    /// Records send state notices, each with the module it is for, from records a barrier
    /// made durable. `false`, recording nothing, until [`Self::enable_bus_events`] has named
    /// the bus account.
    fn record_send_states(
        &self,
        changes: &[(String, SendStateNotice)],
        now_ms: u64,
    ) -> Result<bool, StoreError>;
}

/// Drain orchestration belongs to the publisher, not the object-safe storage port.
pub async fn drain_bus_event<S: RunEventStore + ?Sized>(
    store: &S,
    publisher: &dyn BusPublisher,
    now_ms: u64,
) -> Result<DrainResult, StoreError> {
    let Some(event) = store.pending_bus_event()? else {
        return Ok(DrainResult::Empty);
    };
    match publisher.publish(&event).await {
        Ok(receipt) => {
            store.acknowledge_bus_event(&event, receipt, now_ms)?;
            Ok(DrainResult::Published)
        }
        Err(_) => {
            store
                .bus_metrics()
                .bus_publish_failures
                .fetch_add(1, Ordering::Relaxed);
            Ok(DrainResult::Retry)
        }
    }
}

impl RunEventStore for SqliteStore {
    fn enable_bus_events(&self, account: &str) -> Result<(), String> {
        self.enable_bus_events(account)
    }
    fn bus_metrics(&self) -> &Metrics {
        self.bus_metrics()
    }
    fn bus_metrics_arc(&self) -> Arc<Metrics> {
        self.bus_metrics_arc()
    }
    fn retain_bus_progress(
        &self,
        session: &BindIdentity,
        epoch: &str,
        event: &RunEvent,
        now_ms: u64,
    ) -> Result<Option<Publication>, StoreError> {
        self.retain_bus_progress(session, epoch, event, now_ms)
    }
    fn get_bus_event(
        &self,
        event_key: &str,
    ) -> Result<Option<(BindIdentity, Vec<u8>)>, StoreError> {
        self.get_bus_event(event_key)
    }
    fn pending_bus_event(&self) -> Result<Option<Publication>, StoreError> {
        self.pending_bus_event()
    }
    fn acknowledge_bus_event(
        &self,
        event: &Publication,
        receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        self.acknowledge_bus_event(event, receipt, now_ms)
    }
    fn skip_stale_bus_events(&self, now_ms: u64) -> Result<SkipSummary, StoreError> {
        self.skip_stale_bus_events(now_ms)
    }
    fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError> {
        self.prune_bus_events(now_ms)
    }
    fn record_run_events(
        &self,
        session: &BindIdentity,
        epoch: &str,
        events: &mut [RunEvent],
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        self.event_store()
            .record_run_events(session, epoch, events, now_ms)
    }
    fn record_send_states(
        &self,
        changes: &[(String, SendStateNotice)],
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        self.event_store().record_send_states(changes, now_ms)
    }
}

impl SqliteStore {
    pub fn enable_bus_events(&self, account: &str) -> Result<(), String> {
        self.event_store().enable_bus_events(account)
    }
    pub fn bus_metrics(&self) -> &Metrics {
        &self.bus_events.metrics
    }
    pub fn bus_metrics_arc(&self) -> Arc<Metrics> {
        self.bus_events.metrics.clone()
    }
    pub fn retain_bus_progress(
        &self,
        session: &BindIdentity,
        epoch: &str,
        event: &RunEvent,
        now_ms: u64,
    ) -> Result<Option<Publication>, StoreError> {
        self.event_store()
            .retain_bus_progress(session, epoch, event, now_ms)
    }
    pub fn get_bus_event(
        &self,
        event_key: &str,
    ) -> Result<Option<(BindIdentity, Vec<u8>)>, StoreError> {
        self.event_store().get_bus_event(event_key)
    }
    pub fn pending_bus_event(&self) -> Result<Option<Publication>, StoreError> {
        self.event_store().pending_bus_event()
    }
    pub fn acknowledge_bus_event(
        &self,
        event: &Publication,
        receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        self.event_store()
            .acknowledge_bus_event(event, receipt, now_ms)
    }
    pub fn skip_stale_bus_events(&self, now_ms: u64) -> Result<SkipSummary, StoreError> {
        self.event_store().skip_stale_bus_events(now_ms)
    }
    pub fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError> {
        self.event_store().prune_bus_events(now_ms)
    }

    fn event_store(&self) -> EventStore<'_> {
        EventStore {
            conn: &self.conn,
            bus_events: &self.bus_events,
            legacy: true,
        }
    }

    pub async fn drain_bus_event(
        &self,
        publisher: &dyn BusPublisher,
        now_ms: u64,
    ) -> Result<DrainResult, StoreError> {
        let Some(event) = self.pending_bus_event()? else {
            return Ok(DrainResult::Empty);
        };
        match publisher.publish(&event).await {
            Ok(receipt) => {
                self.acknowledge_bus_event(&event, receipt, now_ms)?;
                Ok(DrainResult::Published)
            }
            Err(_) => {
                self.bus_metrics()
                    .bus_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                Ok(DrainResult::Retry)
            }
        }
    }
}

pub(crate) struct EventStore<'a> {
    pub(crate) conn: &'a Mutex<Connection>,
    pub(crate) bus_events: &'a State,
    pub(crate) legacy: bool,
}

impl EventStore<'_> {
    fn write<T>(
        &self,
        legacy_transaction: bool,
        write: impl FnOnce(&Connection) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.write_with_locked_effect(legacy_transaction, write, |_| {})
    }

    fn write_with_locked_effect<T>(
        &self,
        legacy_transaction: bool,
        write: impl FnOnce(&Connection) -> Result<T, StoreError>,
        after_write: impl FnOnce(&T),
    ) -> Result<T, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        if !self.legacy {
            conn.pragma_update(None, "synchronous", "FULL")?;
        }
        let result = if legacy_transaction || !self.legacy {
            let behavior = if self.legacy {
                rusqlite::TransactionBehavior::Deferred
            } else {
                rusqlite::TransactionBehavior::Immediate
            };
            let tx = conn.transaction_with_behavior(behavior)?;
            let result = write(&tx)?;
            tx.commit()?;
            Ok(result)
        } else {
            write(&conn)
        };
        if let Ok(value) = &result {
            // A depth resync takes the same mutex, so notice gauges must move before unlock.
            after_write(value);
            #[cfg(test)]
            if let Some(observe) = &*self.bus_events.before_write_unlock.lock().unwrap() {
                observe(&conn);
            }
        }
        result
    }

    /// Called only after the bus client attests its machine account and dotted event support.
    pub fn enable_bus_events(&self, account: &str) -> Result<(), String> {
        let names = AccountNames::derive(account).map_err(|e| e.to_string())?;
        let mut configured = self.bus_events.account.lock().unwrap();
        if configured.as_ref().is_some_and(|old| old != &names) {
            return Err("run event account changed during this process".into());
        }
        let first = configured.is_none();
        *configured = Some(names);
        drop(configured);
        if first {
            self.resync_bus_depth().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn bus_metrics(&self) -> &Metrics {
        &self.bus_events.metrics
    }

    pub fn retain_bus_progress(
        &self,
        session: &BindIdentity,
        epoch: &str,
        event: &RunEvent,
        now_ms: u64,
    ) -> Result<Option<Publication>, StoreError> {
        let Some(account) = self.bus_events.account.lock().unwrap().clone() else {
            return Ok(None);
        };
        let bytes = match event.jcs() {
            Ok(bytes) => bytes,
            Err(EncodeError::Oversize(_)) => {
                self.bus_metrics()
                    .bus_events_oversize
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(target: "bus", "refusing oversize run progress event");
                return Ok(None);
            }
            Err(e) => return Err(StoreError::BusEvent(e.to_string())),
        };
        // Progress is retained for claim-check reads but never retried as lifecycle work, so it
        // is written best-effort and never counts toward the outbox depth.
        let (event_key, _) = self.write(false, |conn| {
            insert_event(conn, &account, session, epoch, event, &bytes, now_ms, true)
        })?;
        Ok(Some(Publication {
            inline: false,
            event_key,
            subject: event
                .subject(&account)
                .map_err(|e| StoreError::BusEvent(e.to_string()))?,
            headers: with_session_header(
                Headers::from([
                    ("kind".into(), event.detail.name().into()),
                    ("run_id".into(), event.run_id.clone()),
                    ("session_ref".into(), event.session_ref.clone()),
                    ("epoch".into(), epoch.into()),
                    ("seq".into(), event.seq.to_string()),
                ]),
                &session.session,
            ),
            body: bytes,
            digest: None,
        }))
    }

    /// See [`RunEventStore::record_run_events`]. Each event gets its publication id, headers
    /// and retention rules here, and recording one twice adds nothing.
    pub fn record_run_events(
        &self,
        session: &BindIdentity,
        epoch: &str,
        events: &mut [RunEvent],
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let Some(account) = self.bus_events.account.lock().unwrap().clone() else {
            return Ok(false);
        };
        let (inserted, oversize, dropped) = self.write(true, |tx| {
            let (mut inserted, mut oversize, mut dropped) = (0, 0, 0);
            for event in events.iter_mut() {
                match event.fit_send_ids() {
                    Ok(_)
                        if event.at_ms != 0
                            && event.at_ms.saturating_add(RETENTION_MS) < now_ms =>
                    {
                        dropped += 1;
                    }
                    Ok(bytes) => {
                        let (_, new_row) = insert_event(
                            tx, &account, session, epoch, event, &bytes, now_ms, false,
                        )?;
                        inserted += u64::from(new_row);
                    }
                    Err(EncodeError::Oversize(_)) => {
                        oversize += 1;
                        tracing::warn!(target: "bus", seq = event.seq, "refusing oversize run lifecycle event");
                    }
                    Err(error) => return Err(StoreError::BusEvent(error.to_string())),
                }
            }
            Ok((inserted, oversize, dropped))
        })?;
        let metrics = self.bus_metrics();
        metrics
            .bus_outbox_depth
            .fetch_add(inserted, Ordering::Relaxed);
        metrics
            .bus_events_oversize
            .fetch_add(oversize, Ordering::Relaxed);
        if dropped > 0 {
            metrics
                .bus_outbox_dropped
                .fetch_add(dropped, Ordering::Relaxed);
            tracing::warn!(target: "bus", dropped, "dropping run events older than 24 hours");
        }
        Ok(true)
    }

    /// See [`RunEventStore::record_send_states`]. A notice over the size limit or whose sender
    /// is no module id is refused with a warning and the rest are recorded.
    pub fn record_send_states(
        &self,
        changes: &[(String, SendStateNotice)],
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let Some(account) = self.bus_events.account.lock().unwrap().clone() else {
            return Ok(false);
        };
        let refused = self.write(true, |tx| {
            let mut refused = 0u64;
            for (sender, notice) in changes {
                let (Ok(subject), Ok(body)) = (notice.subject(&account, sender), notice.to_bytes())
                else {
                    refused += 1;
                    continue;
                };
                tx.execute(
                    "INSERT OR IGNORE INTO send_state_outbox(message_id,subject,session_ref,send_id,seq,body,at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![notice.message_id(), subject, notice.session_ref, notice.send_id, notice.seq, body, now_ms],
                )?;
            }
            Ok(refused)
        })?;
        if refused > 0 {
            self.bus_metrics()
                .bus_events_oversize
                .fetch_add(refused, Ordering::Relaxed);
            tracing::warn!(target: "bus", refused, "refusing send state notices over the size limit or for a sender that is no module id");
        }
        Ok(true)
    }

    /// The oldest send state notice not yet published.
    fn pending_send_state(&self) -> Result<Option<Publication>, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT message_id,subject,session_ref,send_id,seq,body FROM send_state_outbox INDEXED BY send_state_unpublished WHERE published_ms IS NULL ORDER BY at_ms, rowid LIMIT 1",
                [],
                |r| {
                    let headers = Headers::from([
                        ("kind".into(), "send_state".into()),
                        ("session_ref".into(), r.get(2)?),
                        ("send_id".into(), r.get(3)?),
                        ("seq".into(), r.get::<_, u64>(4)?.to_string()),
                    ]);
                    Ok(Publication {
                        event_key: r.get(0)?,
                        subject: r.get(1)?,
                        headers,
                        body: r.get(5)?,
                        digest: None,
                        inline: true,
                    })
                },
            )
            .optional()?)
    }

    pub fn get_bus_event(
        &self,
        event_key: &str,
    ) -> Result<Option<(BindIdentity, Vec<u8>)>, StoreError> {
        let value: Option<(String, Vec<u8>)> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT session,body FROM bus_event_outbox WHERE event_key=?1",
                [event_key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        value
            .map(|(session, body)| Ok((serde_json::from_str(&session)?, body)))
            .transpose()
    }

    pub fn pending_bus_event(&self) -> Result<Option<Publication>, StoreError> {
        // Bound to a name so the connection lock is released before a notice is looked up.
        let lifecycle = self.conn.lock().unwrap().query_row(
            "SELECT event_key,subject,epoch,run_id,seq,session,body FROM bus_event_outbox INDEXED BY bus_event_unpublished WHERE published_ms IS NULL AND best_effort=0 ORDER BY rowid LIMIT 1", [],
            |r| {
                let body: Vec<u8> = r.get(6)?;
                // Read the session_ref header from the queued body, not the session column,
                // so replayed events keep the exact reference minted when they were queued.
                let queued: serde_json::Value = serde_json::from_slice(&body).map_err(|e| rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Blob, Box::new(e)))?;
                let reference = queued.get("session_ref").and_then(serde_json::Value::as_str)
                    .ok_or(rusqlite::Error::InvalidColumnType(6, "session_ref".into(), rusqlite::types::Type::Blob))?;
                let subject: String = r.get(1)?;
                let mut headers = Headers::from([
                    ("kind".into(), subject.split(".event.broca.").nth(1).unwrap_or_default().trim_end_matches(".v1").into()),
                    ("epoch".into(),r.get(2)?), ("run_id".into(),r.get(3)?),
                    ("seq".into(),r.get::<_,u64>(4)?.to_string()), ("session_ref".into(),reference.into()),
                ]);
                // A skip marker spans runs, so it names none.
                if headers.get("run_id").is_some_and(String::is_empty) {
                    headers.remove("run_id");
                }
                let session: String = r.get(5)?;
                if let Ok(session) = serde_json::from_str::<BindIdentity>(&session) {
                    headers = with_session_header(headers, &session.session);
                }
                Ok(Publication { event_key:r.get(0)?, headers, subject, body, digest: None, inline: false })
            },
        ).optional()?;
        match lifecycle {
            Some(lifecycle) => Ok(Some(lifecycle)),
            None => self.pending_send_state(),
        }
    }

    /// Replace queued run events older than [`STALE_BACKLOG_MS`] with one `run.skipped` marker
    /// per session. Call it when a bus connection comes up, before the first publish on it.
    ///
    /// Every unpublished lifecycle row either stays to be published or leaves the outbox in
    /// the same transaction that inserts the marker covering it, so no row is ever lost
    /// without a marker, and a crash at any point leaves either the rows (the next pass
    /// replaces them again) or the marker (published like any other event), never both.
    ///
    /// A session's skipped range is everything queued for it up to its last stale event, so
    /// a younger event queued between stale ones is covered too and nothing published for
    /// the session predates its marker. The marker takes the rowid of the first row it
    /// replaces, which keeps it ahead of the session's remaining events in the drain's
    /// rowid order. Markers already queued are never folded into a new one.
    pub fn skip_stale_bus_events(&self, now_ms: u64) -> Result<SkipSummary, StoreError> {
        let account = self
            .bus_events
            .account
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| StoreError::BusEvent("run event account is not known yet".into()))?;
        let subject = account
            .event_subject("broca", SKIPPED_EVENT, 1)
            .map_err(|e| StoreError::BusEvent(e.to_string()))?;
        let cutoff = now_ms.saturating_sub(STALE_BACKLOG_MS);
        struct Row {
            rowid: i64,
            seq: u64,
            epoch: String,
            at_ms: u64,
        }
        let summary = self.write(true, |tx| {
        // Sessions in the order their first queued row appears, each with its rows in order.
        let mut sessions: Vec<(String, Vec<Row>)> = Vec::new();
        {
            let mut statement = tx.prepare(
                "SELECT rowid,session,seq,epoch,body FROM bus_event_outbox INDEXED BY bus_event_unpublished \
                 WHERE published_ms IS NULL AND best_effort=0 AND subject<>?1 ORDER BY rowid",
            )?;
            let mut rows = statement.query([&subject])?;
            let mut index: std::collections::HashMap<String, usize> = Default::default();
            while let Some(r) = rows.next()? {
                let session: String = r.get(1)?;
                let body: Vec<u8> = r.get(4)?;
                // An event without a readable clock cannot prove it is young.
                let at_ms = serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.get("at_ms").and_then(serde_json::Value::as_u64))
                    .unwrap_or(0);
                let row = Row {
                    rowid: r.get(0)?,
                    seq: r.get(2)?,
                    epoch: r.get(3)?,
                    at_ms,
                };
                let slot = *index.entry(session.clone()).or_insert_with(|| {
                    sessions.push((session, Vec::new()));
                    sessions.len() - 1
                });
                sessions[slot].1.push(row);
            }
        }
        let mut summary = SkipSummary::default();
        for (session_json, rows) in sessions {
            let Some(last_stale) = rows.iter().rposition(|row| row.at_ms < cutoff) else {
                continue;
            };
            let skipped = &rows[..=last_stale];
            let (first, last) = (&skipped[0], &skipped[last_stale]);
            let session: BindIdentity = serde_json::from_str(&session_json)?;
            let marker = SkipMarker {
                session_ref: agent_session_ref(&session),
                from_seq: first.seq,
                to_seq: last.seq,
                from_ms: skipped.iter().map(|row| row.at_ms).min().unwrap_or(0),
                to_ms: skipped.iter().map(|row| row.at_ms).max().unwrap_or(0),
                count: skipped.len() as u64,
            };
            let body = serde_jcs::to_vec(&marker)?;
            // The same idempotency style as run events: a key hashed from what the marker
            // names, so a republish after a crash carries the identical key and digest.
            let key_bytes = serde_jcs::to_vec(&(
                &last.epoch,
                &marker.session_ref,
                marker.from_seq,
                marker.to_seq,
                &subject,
            ))?;
            let event_key = format!("{:x}", Sha256::digest(key_bytes));
            for row in skipped {
                tx.execute("DELETE FROM bus_event_outbox WHERE rowid=?1", [row.rowid])?;
            }
            tx.execute(
                "INSERT INTO bus_event_outbox(rowid,event_key,run_id,seq,subject,epoch,session,body,at_ms,best_effort) \
                 VALUES(?1,?2,'',?3,?4,?5,?6,?7,?8,0)",
                params![
                    first.rowid,
                    event_key,
                    marker.to_seq,
                    subject,
                    last.epoch,
                    session_json,
                    body,
                    now_ms
                ],
            )?;
            summary.sessions += 1;
            summary.skipped += marker.count;
        }
        Ok(summary)
        })?;
        // Each session's rows became one pending marker.
        self.lower_bus_depth(summary.skipped - summary.sessions);
        Ok(summary)
    }

    pub fn acknowledge_bus_event(
        &self,
        event: &Publication,
        _receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let (lifecycle, lifecycle_updated, sends_updated) = self.write_with_locked_effect(false, |conn| {
        let lifecycle: Option<bool> = conn
            .query_row(
                "SELECT best_effort=0 FROM bus_event_outbox WHERE event_key=?1 AND published_ms IS NULL",
                [&event.event_key],
                |r| r.get(0),
            )
            .optional()?;
        let lifecycle_updated = conn.execute(
            "UPDATE bus_event_outbox SET published_ms=?2 WHERE event_key=?1 AND published_ms IS NULL",
            params![event.event_key, now_ms],
        )? as u64;
        // A send state notice keeps no gauge: it is counted only as published.
        let sends_updated = conn.execute(
            "UPDATE send_state_outbox SET published_ms=?2 WHERE message_id=?1 AND published_ms IS NULL",
            params![event.event_key, now_ms],
        )? as u64;
        Ok((lifecycle, lifecycle_updated, sends_updated))
        }, |_| ())?;
        let updated = lifecycle_updated + sends_updated;
        self.bus_metrics()
            .bus_events_published
            .fetch_add(updated, Ordering::Relaxed);
        if updated > 0 && lifecycle == Some(true) {
            self.lower_bus_depth(updated);
        }
        Ok(())
    }

    pub fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError> {
        let (deleted, dropped) = self.write_with_locked_effect(false, |conn| {
        let cutoff = now_ms.saturating_sub(RETENTION_MS);
        let dropped: u64 = conn.query_row(
            "SELECT count(*) FROM bus_event_outbox INDEXED BY bus_event_unpublished WHERE published_ms IS NULL AND best_effort=0 AND at_ms<?1",
            [cutoff],
            |r| r.get(0),
        )?;
        let sends_dropped: u64 = conn.query_row(
            "SELECT count(*) FROM send_state_outbox INDEXED BY send_state_unpublished WHERE published_ms IS NULL AND at_ms<?1",
            [cutoff], |r| r.get(0),
        )?;
        if sends_dropped > 0 {
            tracing::warn!(target: "bus", dropped = sends_dropped, "dropping send state notices older than 24 hours without a publish acknowledgement");
        }
        let deleted = conn.execute("DELETE FROM bus_event_outbox WHERE at_ms<?1", [cutoff])?
            + conn.execute("DELETE FROM send_state_outbox WHERE at_ms<?1", [cutoff])?;
        Ok((deleted, dropped))
        }, |_| ())?;
        if dropped > 0 {
            self.lower_bus_depth(dropped);
            self.bus_metrics()
                .bus_outbox_dropped
                .fetch_add(dropped, Ordering::Relaxed);
            tracing::warn!(target: "bus", dropped, "dropping run events older than 24 hours without a publish acknowledgement");
        }
        Ok(deleted)
    }

    /// Set the depth gauge from the table, once when events are enabled. Every other change
    /// moves the counter with the row it inserts, publishes or prunes: the count is a pass over
    /// every pending row, and running it for every recorded batch would hold the shared
    /// connection for the length of that pass while every other store operation waited.
    fn resync_bus_depth(&self) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        let depth = conn.query_row(
            "SELECT count(*) FROM bus_event_outbox INDEXED BY bus_event_unpublished WHERE published_ms IS NULL AND best_effort=0",
            [],
            |r| r.get(0),
        )?;
        self.bus_metrics()
            .bus_outbox_depth
            .store(depth, Ordering::Relaxed);
        Ok(())
    }

    fn lower_bus_depth(&self, by: u64) {
        let depth = &self.bus_metrics().bus_outbox_depth;
        let mut current = depth.load(Ordering::Relaxed);
        while let Err(seen) = depth.compare_exchange_weak(
            current,
            current.saturating_sub(by),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            current = seen;
        }
    }
}

/// Insert one outbox row; returns its key and whether the row is new.
#[allow(clippy::too_many_arguments)]
fn insert_event(
    conn: &Connection,
    account: &AccountNames,
    session: &BindIdentity,
    epoch: &str,
    event: &RunEvent,
    bytes: &[u8],
    now_ms: u64,
    best_effort: bool,
) -> Result<(String, bool), StoreError> {
    let subject = event
        .subject(account)
        .map_err(|e| StoreError::BusEvent(e.to_string()))?;
    let observation =
        matches!(event.detail, RunEventDetail::Progress { .. }).then_some(event.at_ms);
    let key_bytes = serde_jcs::to_vec(&(epoch, &event.run_id, event.seq, &subject, observation))?;
    let event_key = format!("{:x}", Sha256::digest(key_bytes));
    let inserted = conn.execute("INSERT OR IGNORE INTO bus_event_outbox(event_key,run_id,seq,subject,epoch,session,body,at_ms,best_effort) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![event_key,event.run_id,event.seq,subject,epoch,serde_json::to_string(session)?,bytes,now_ms,best_effort])?;
    Ok((event_key, inserted == 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_event_schema_matches_frozen_catalog() {
        // The fixture pins the outbox tables' schema: an older build reads these tables after a
        // rollback, so any change to them must be deliberate.
        let expected: Vec<(String, String)> =
            serde_json::from_str(include_str!("../tests/fixtures/legacy-bus-schema.json")).unwrap();
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.lock().unwrap();
        let actual = conn.prepare("SELECT name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name LIKE 'bus_event_%' ORDER BY name").unwrap()
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert_eq!(actual, expected);
    }

    use broca_protocol::RunFinishReason;
    use std::sync::atomic::AtomicBool;

    fn session() -> BindIdentity {
        BindIdentity::new("/project", "test", "session")
    }
    /// One run event of run `r1` of [`session`], as a session in the store derives it from
    /// its records; [`record`] gives it the session it is recorded for.
    fn event(seq: u64, at_ms: u64, detail: RunEventDetail) -> RunEvent {
        RunEvent {
            v: 1,
            session_ref: agent_session_ref(&session()),
            run_id: "r1".into(),
            seq,
            at_ms,
            detail,
        }
    }
    fn start() -> RunEvent {
        event(1, 1000, RunEventDetail::Started {})
    }
    fn finish(seq: u64, reason: RunFinishReason) -> RunEvent {
        event(
            seq,
            2000,
            RunEventDetail::Ended {
                terminal: reason,
                error_class: None,
                structured_output: None,
            },
        )
    }
    /// Records `events` for `session` through the path sessions in the store use. `false`,
    /// recording nothing, until `enable_bus_events` has named the bus account.
    fn record(
        store: &SqliteStore,
        session: &BindIdentity,
        epoch: &str,
        events: &[RunEvent],
        now: u64,
    ) -> bool {
        let mut events = events.to_vec();
        for event in &mut events {
            event.session_ref = agent_session_ref(session);
        }
        store
            .record_run_events(session, epoch, &mut events, now)
            .unwrap()
    }
    fn enabled() -> SqliteStore {
        let s = SqliteStore::open_in_memory().unwrap();
        s.enable_bus_events("box_test").unwrap();
        s
    }

    #[test]
    fn queued_v1_run_event_keeps_minted_reference_in_body_and_headers() {
        let store = enabled();
        record(
            &store,
            &session(),
            "epoch",
            &[start(), finish(2, RunFinishReason::Completed)],
            2000,
        );
        let old = broca_core::session_refs::legacy_session_ref("runner", "/synthetic/old", "s1");
        let event = store.pending_bus_event().unwrap().unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&event.body).unwrap();
        body["session_ref"] = old.clone().into();
        let minted = serde_json::to_vec(&body).unwrap();
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE bus_event_outbox SET body=?1 WHERE event_key=?2",
                rusqlite::params![minted, event.event_key],
            )
            .unwrap();
        let queued = store.pending_bus_event().unwrap().unwrap();
        assert_eq!(queued.body, minted);
        assert_eq!(queued.headers["session_ref"], old);
    }

    /// A run event names its session in a `session` header, unless the name has a character a
    /// NATS header can't hold; the event is published either way.
    #[test]
    fn run_events_carry_the_plain_session_name_when_a_header_can_hold_it() {
        for (name, expected) in [
            ("alfonso:head:agent_1", Some("alfonso:head:agent_1")),
            ("tab\tin", None),
            ("çay", None),
        ] {
            let store = enabled();
            let mut session = session();
            session.session = name.into();
            record(
                &store,
                &session,
                "epoch",
                &[start(), finish(2, RunFinishReason::Completed)],
                2000,
            );
            let event = store.pending_bus_event().unwrap().unwrap();
            assert_eq!(
                event.headers.get("session").map(String::as_str),
                expected,
                "{name}"
            );
            assert_eq!(event.headers["kind"], "run.started");
        }
    }
    type OutboxRow = (String, u64, String, Vec<u8>, Option<u64>);
    fn rows(store: &SqliteStore) -> Vec<OutboxRow> {
        store.conn.lock().unwrap().prepare("SELECT event_key,seq,subject,body,published_ms FROM bus_event_outbox ORDER BY rowid").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap().collect::<Result<_,_>>().unwrap()
    }

    #[test]
    fn bus_outbox_off_writes_nothing() {
        let store = SqliteStore::open_in_memory().unwrap();
        record(&store, &session(), "epoch", &[start()], 1000);
        assert!(rows(&store).is_empty());
    }

    struct Fake {
        store: Arc<SqliteStore>,
        ack: AtomicBool,
        seen: Mutex<Vec<(String, ContentDigest)>>,
    }
    #[async_trait]
    impl BusPublisher for Fake {
        async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
            assert!(
                rows(&self.store)
                    .iter()
                    .find(|r| r.0 == event.event_key)
                    .unwrap()
                    .4
                    .is_none(),
                "row must remain pending until acknowledgement"
            );
            self.seen
                .lock()
                .unwrap()
                .push((event.event_key.clone(), event.digest()));
            if !self.ack.load(Ordering::Relaxed) {
                return Err("outage".into());
            }
            Ok(PublishReceipt::new(1, self.seen.lock().unwrap().len() > 1))
        }
    }

    #[tokio::test]
    async fn bus_outbox_ack_then_mark_published_retains_claim_check() {
        let store = Arc::new(enabled());
        record(&store, &session(), "epoch", &[start()], 1000);
        let bus = Fake {
            store: store.clone(),
            ack: AtomicBool::new(false),
            seen: Mutex::new(vec![]),
        };
        assert_eq!(
            store.drain_bus_event(&bus, 1100).await.unwrap(),
            DrainResult::Retry
        );
        assert!(rows(&store)[0].4.is_none());
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            1
        );
        bus.ack.store(true, Ordering::Relaxed);
        assert_eq!(
            store.drain_bus_event(&bus, 1200).await.unwrap(),
            DrainResult::Published
        );
        assert_eq!(
            store.drain_bus_event(&bus, 1300).await.unwrap(),
            DrainResult::Empty
        );
        let seen = bus.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], seen[1]);
        let key = &seen[0].0;
        assert!(store.get_bus_event(key).unwrap().is_some());
        assert_eq!(rows(&store)[0].4, Some(1200));
        assert_eq!(
            store
                .bus_metrics()
                .bus_events_published
                .load(Ordering::Relaxed),
            1
        );
    }

    /// The depth gauge follows the rows each call inserts and acknowledges; it is not counted
    /// again from the table on every recording.
    #[test]
    fn bus_outbox_depth_moves_with_its_own_rows_without_a_recount() {
        let store = enabled();
        record(&store, &session(), "epoch", &[start()], 1000);
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            1
        );
        // Set the `bus_outbox_depth` gauge to 100 while `bus_event_outbox` holds one pending
        // row. A recording that counts only its own new row leaves 101; one that recounted
        // the table would set 2.
        store
            .bus_metrics()
            .bus_outbox_depth
            .store(100, Ordering::Relaxed);
        record(
            &store,
            &session(),
            "epoch",
            &[start(), finish(2, RunFinishReason::Completed)],
            2000,
        );
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            101,
            "a recording must add its own rows, not recount the table"
        );
        let next = store.pending_bus_event().unwrap().unwrap();
        store
            .acknowledge_bus_event(&next, PublishReceipt::new(1, false), 3000)
            .unwrap();
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            100
        );
    }

    #[test]
    fn bus_outbox_oversize_is_refused_and_counted() {
        let store = enabled();
        let mut frame = start();
        frame.run_id = "x".repeat(512);
        record(&store, &session(), "epoch", &[frame], 1000);
        assert!(rows(&store).is_empty());
        assert_eq!(
            store
                .bus_metrics()
                .bus_events_oversize
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn bus_outbox_retention_drops_pending_with_counter_and_prunes_published() {
        let store = enabled();
        record(&store, &session(), "epoch", &[start()], 1000);
        assert_eq!(store.prune_bus_events(1000 + RETENTION_MS).unwrap(), 0);
        assert_eq!(store.prune_bus_events(1001 + RETENTION_MS).unwrap(), 1);
        assert_eq!(
            store
                .bus_metrics()
                .bus_outbox_dropped
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            0
        );
    }

    fn started_at(seq: u64, run_id: &str, ts: u64) -> RunEvent {
        RunEvent {
            run_id: run_id.into(),
            ..event(seq, ts, RunEventDetail::Started {})
        }
    }
    fn finished_at(seq: u64, run_id: &str, ts: u64) -> RunEvent {
        RunEvent {
            run_id: run_id.into(),
            at_ms: ts,
            ..finish(seq, RunFinishReason::Completed)
        }
    }

    type Recorded = (String, String, String, Vec<u8>);
    /// Every publication, as `(kind, session_ref, event_key, body)`, acknowledged or not.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<Recorded>>);
    #[async_trait]
    impl BusPublisher for Recorder {
        async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
            let mut seen = self.0.lock().unwrap();
            seen.push((
                event.headers["kind"].clone(),
                event.headers["session_ref"].clone(),
                event.event_key.clone(),
                event.body.clone(),
            ));
            Ok(PublishReceipt::new(seen.len() as u64, false))
        }
    }
    async fn drain_all(store: &SqliteStore, bus: &Recorder, now: u64) {
        while store.drain_bus_event(bus, now).await.unwrap() == DrainResult::Published {}
    }

    const NOW: u64 = 1_800_000_000_000;
    const MINUTE: u64 = 60_000;

    /// Three sessions with queued events: `a` has an old finished run and a young run, `b`
    /// only old events, `c` only a young one. Returns them in that order.
    fn mixed_backlog(store: &SqliteStore) -> [BindIdentity; 3] {
        let [a, b, c] = ["a", "b", "c"].map(|n| BindIdentity::new("/project", "test", n));
        record(
            store,
            &a,
            "epoch-a",
            &[
                started_at(1, "a-old", NOW - 10 * MINUTE),
                finished_at(2, "a-old", NOW - 9 * MINUTE),
                started_at(3, "a-young", NOW - MINUTE / 2),
            ],
            NOW,
        );
        record(
            store,
            &b,
            "epoch-b",
            &[
                started_at(1, "b-old", NOW - 5 * MINUTE),
                finished_at(2, "b-old", NOW - 4 * MINUTE),
            ],
            NOW,
        );
        record(
            store,
            &c,
            "epoch-c",
            &[started_at(1, "c-young", NOW - 10_000)],
            NOW,
        );
        [a, b, c]
    }

    /// A backlog found when a connection comes up: each session's events older than two
    /// minutes become exactly one marker naming their range, published before that
    /// session's younger events, and no stale event is published.
    #[tokio::test]
    async fn bus_stale_backlog_becomes_one_marker_per_session_ahead_of_its_young_events() {
        let store = enabled();
        let [a, b, c] = mixed_backlog(&store);
        let stale_keys: Vec<String> = rows(&store)
            .into_iter()
            .filter(|row| {
                let body: serde_json::Value = serde_json::from_slice(&row.3).unwrap();
                body["at_ms"].as_u64().unwrap() < NOW - STALE_BACKLOG_MS
            })
            .map(|row| row.0)
            .collect();
        assert_eq!(stale_keys.len(), 4);
        assert_eq!(
            store.skip_stale_bus_events(NOW).unwrap(),
            SkipSummary {
                sessions: 2,
                skipped: 4
            }
        );
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            4,
            "two markers and two young events remain"
        );
        let bus = Recorder::default();
        drain_all(&store, &bus, NOW).await;
        let seen = bus.0.lock().unwrap().clone();
        let order: Vec<(&str, String)> = seen
            .iter()
            .map(|(kind, session_ref, ..)| (kind.as_str(), session_ref.clone()))
            .collect();
        assert_eq!(
            order,
            [
                ("run.skipped", agent_session_ref(&a)),
                ("run.started", agent_session_ref(&a)),
                ("run.skipped", agent_session_ref(&b)),
                ("run.started", agent_session_ref(&c)),
            ],
            "each marker goes out before its session's younger events"
        );
        for (_, _, key, _) in &seen {
            assert!(!stale_keys.contains(key), "a stale event was published");
        }
        let marker = |n: usize| serde_json::from_slice::<SkipMarker>(&seen[n].3).unwrap();
        assert_eq!(
            marker(0),
            SkipMarker {
                session_ref: agent_session_ref(&a),
                from_seq: 1,
                to_seq: 2,
                from_ms: NOW - 10 * MINUTE,
                to_ms: NOW - 9 * MINUTE,
                count: 2,
            }
        );
        assert_eq!(
            marker(2),
            SkipMarker {
                session_ref: agent_session_ref(&b),
                from_seq: 1,
                to_seq: 2,
                from_ms: NOW - 5 * MINUTE,
                to_ms: NOW - 4 * MINUTE,
                count: 2,
            }
        );
        // The marker body is JCS and served by claim check like any run event.
        assert_eq!(seen[0].3, serde_jcs::to_vec(&marker(0)).unwrap());
        let (owner, body) = store.get_bus_event(&seen[0].2).unwrap().unwrap();
        assert_eq!((owner, body), (a.clone(), seen[0].3.clone()));
        assert_eq!(
            store.bus_metrics().bus_outbox_depth.load(Ordering::Relaxed),
            0
        );
        // Every row that left the pending set was published or is covered by a published
        // marker: the stale rows are gone, everything else is marked published.
        let remaining = rows(&store);
        assert_eq!(remaining.len(), 4);
        assert!(remaining.iter().all(|row| row.4.is_some()));
        // A second pass finds nothing stale: the markers themselves are never folded.
        assert_eq!(
            store.skip_stale_bus_events(NOW + 10 * MINUTE).unwrap(),
            SkipSummary::default()
        );
    }

    /// A crash inside the replacement leaves the stale rows, never a gap; a crash after it
    /// leaves exactly the marker, which is published after restart; a crash after the
    /// marker's publish but before its acknowledgement republishes the identical marker. At
    /// no point is a stale event published.
    #[tokio::test]
    async fn bus_stale_backlog_replacement_survives_a_crash_at_each_point() {
        let dir = broca_test_support::scratch_root().join(format!(
            "bus-skip-crash-{}-{}",
            std::process::id(),
            NOW
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("run-index.db");
        let open = || {
            let store = SqliteStore::open(&path).unwrap();
            store.enable_bus_events("box_test").unwrap();
            store
        };
        let store = open();
        mixed_backlog(&store);
        let before = rows(&store);
        // A failure between deleting the stale rows and inserting their marker rolls the
        // whole replacement back.
        store.conn.lock().unwrap().execute_batch(
            "CREATE TRIGGER fail_marker BEFORE INSERT ON bus_event_outbox WHEN NEW.subject LIKE '%.run.skipped.v1' \
             BEGIN SELECT RAISE(ABORT,'injected crash'); END;",
        ).unwrap();
        assert!(store.skip_stale_bus_events(NOW).is_err());
        assert_eq!(
            rows(&store),
            before,
            "a failed replacement must change nothing"
        );
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_marker;")
            .unwrap();
        // The replacement commits, then the process stops before publishing anything.
        store.skip_stale_bus_events(NOW).unwrap();
        let stale_keys: Vec<String> = before
            .iter()
            .map(|row| row.0.clone())
            .filter(|key| !rows(&store).iter().any(|row| &row.0 == key))
            .collect();
        assert_eq!(stale_keys.len(), 4);
        drop(store);
        let store = open();
        let marker = store.pending_bus_event().unwrap().unwrap();
        assert_eq!(marker.headers["kind"], "run.skipped");
        assert!(!marker.headers.contains_key("run_id"));
        // The marker reaches the broker, then the process stops before acknowledging it.
        let bus = Recorder::default();
        let _receipt = bus.publish(&marker).await.unwrap();
        drop(store);
        let store = open();
        assert_eq!(
            store.skip_stale_bus_events(NOW + MINUTE).unwrap(),
            SkipSummary::default(),
            "the replaced rows are not replaced again"
        );
        drain_all(&store, &bus, NOW + MINUTE).await;
        let seen = bus.0.lock().unwrap().clone();
        let markers: Vec<_> = seen.iter().filter(|p| p.0 == "run.skipped").collect();
        assert_eq!(
            markers.len(),
            3,
            "one republished marker and the second session's"
        );
        assert_eq!(
            markers[0].2, markers[1].2,
            "the republish has the identical key"
        );
        assert_eq!(markers[0].3, markers[1].3);
        assert_ne!(markers[1].2, markers[2].2);
        for (_, _, key, _) in &seen {
            assert!(!stale_keys.contains(key), "a stale event was published");
        }
        assert!(store.pending_bus_event().unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn send_state(send_id: &str, seq: u64, state: &'static str) -> SendStateNotice {
        SendStateNotice {
            v: 1,
            session_ref: "ses_ref".into(),
            send_id: send_id.into(),
            seq,
            state,
        }
    }

    /// Send state notices wait for the account, then publish after any pending run event, on
    /// the sender's subject under their own message id; recording one twice adds nothing, an
    /// acknowledgement retires it, and a notice for no module id is refused alone.
    #[test]
    fn send_state_notices_publish_after_run_events_once_each() {
        let store = SqliteStore::open_in_memory().unwrap();
        let notice = send_state("send-1", 4, "pending");
        assert!(!store
            .record_send_states(&[("aft".into(), notice.clone())], 1000)
            .unwrap());
        store.enable_bus_events("box_test").unwrap();
        record(&store, &session(), "epoch", &[start()], 1000);
        assert!(store
            .record_send_states(
                &[
                    ("aft".into(), notice.clone()),
                    ("aft".into(), notice.clone()),
                    ("not a module".into(), send_state("send-2", 5, "pending")),
                ],
                1000,
            )
            .unwrap());
        let run_event = store.pending_bus_event().unwrap().unwrap();
        assert!(run_event.subject.ends_with(".run.started.v1"));
        store
            .acknowledge_bus_event(&run_event, PublishReceipt::new(1, false), 1001)
            .unwrap();
        let published = store.pending_bus_event().unwrap().unwrap();
        assert_eq!(published.event_key, "ses_ref:send-1:4");
        assert!(
            published
                .subject
                .ends_with(".event.broca.send_state.v1.aft"),
            "{}",
            published.subject
        );
        assert_eq!(published.body, notice.to_bytes().unwrap());
        assert_eq!(published.headers["kind"], "send_state");
        assert_eq!(published.headers["send_id"], "send-1");
        store
            .acknowledge_bus_event(&published, PublishReceipt::new(2, false), 1002)
            .unwrap();
        assert!(store.pending_bus_event().unwrap().is_none());
        assert_eq!(
            store
                .bus_metrics()
                .bus_events_oversize
                .load(Ordering::Relaxed),
            1
        );
        let rows: u64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM send_state_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
        store.prune_bus_events(1000 + RETENTION_MS + 1).unwrap();
        let rows: u64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM send_state_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a day-old notice is pruned");
    }

    /// Writes sample files of send state notices, for the modules that read them to check
    /// their decoders against, when `BROCA_SEND_STATE_SAMPLES` names a folder: one file per state as the publisher sends it (subject, message id, headers and
    /// the body's exact bytes), recorded and read back through the outbox.
    #[test]
    fn send_state_reader_samples() {
        let Some(folder) = std::env::var_os("BROCA_SEND_STATE_SAMPLES") else {
            return;
        };
        let folder = std::path::PathBuf::from(folder);
        std::fs::create_dir_all(&folder).unwrap();
        let store = SqliteStore::open_in_memory().unwrap();
        store.enable_bus_events("box_test").unwrap();
        let reference = agent_session_ref(&session());
        let cases: [(&str, &str, u64, &'static str); 8] = [
            ("aft", "aft-notice-1", 41, "pending"),
            ("aft", "aft-notice-1", 44, "active"),
            ("aft", "aft-notice-2", 45, "superseded"),
            ("aft", "aft-notice-3", 46, "expired"),
            ("occipital", "app-send-7", 50, "pending"),
            ("occipital", "app-send-7", 51, "active"),
            ("occipital", "app-send-7", 60, "paused"),
            ("occipital", "app-send-7", 72, "finished"),
        ];
        for (index, (sender, send_id, seq, state)) in cases.into_iter().enumerate() {
            let notice = SendStateNotice {
                v: 1,
                session_ref: reference.clone(),
                send_id: send_id.into(),
                seq,
                state,
            };
            store
                .record_send_states(&[(sender.into(), notice)], 1000)
                .unwrap();
            let publication = store.pending_bus_event().unwrap().unwrap();
            let sample = serde_json::json!({
                "subject": publication.subject,
                "nats_msg_id": publication.event_key,
                "headers": publication.headers,
                "body": String::from_utf8(publication.body.clone()).unwrap(),
            });
            std::fs::write(
                folder.join(format!("{:02}-{sender}-{state}.json", index + 1)),
                serde_json::to_vec_pretty(&sample).unwrap(),
            )
            .unwrap();
            store
                .acknowledge_bus_event(
                    &publication,
                    PublishReceipt::new(index as u64 + 1, false),
                    1001,
                )
                .unwrap();
        }
    }
}
