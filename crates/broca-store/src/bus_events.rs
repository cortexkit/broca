//! Durable claim-check bodies and pending publications, projected from fsynced WAL ranges.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use broca_core::wal_writer::agent_session_ref;
use broca_protocol::{
    run_events::{EncodeError, RunEvent, RunEventDetail, StructuredOutputStatus},
    BindIdentity,
};
use broca_wal::{FramedRecord, WalRecord};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_trait::{ContentDigest, Headers, PublishReceipt};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SqliteStore, StoreError};

pub const RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Default)]
pub struct Metrics {
    pub bus_events_published: AtomicU64,
    pub bus_outbox_depth: AtomicU64,
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
    metrics: Arc<Metrics>,
}

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
         CREATE TABLE IF NOT EXISTS bus_event_projection (
            session TEXT PRIMARY KEY, epoch TEXT NOT NULL, seq INTEGER NOT NULL,
            state_json TEXT NOT NULL
         );",
    )
}

#[derive(Default, Serialize, Deserialize)]
struct Projection {
    run_id: Option<String>,
    last_at_ms: u64,
    pending: u64,
    send_ids: Vec<String>,
    truncated: bool,
    structured_output: Option<StructuredOutputStatus>,
}

#[derive(Debug, Clone)]
pub struct Publication {
    pub event_key: String,
    pub subject: String,
    pub headers: Headers,
    pub body: Vec<u8>,
}

impl Publication {
    pub fn digest(&self) -> ContentDigest {
        ContentDigest::of_bytes(&self.body)
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

impl SqliteStore {
    /// Called only after the bus client attests its machine account and dotted event support.
    pub fn enable_bus_events(&self, account: &str) -> Result<(), String> {
        let names = AccountNames::derive(account).map_err(|e| e.to_string())?;
        let mut configured = self.bus_events.account.lock().unwrap();
        if configured.as_ref().is_some_and(|old| old != &names) {
            return Err("run event account changed during this process".into());
        }
        *configured = Some(names);
        self.refresh_bus_depth().map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn bus_metrics(&self) -> &Metrics {
        &self.bus_events.metrics
    }

    pub fn bus_metrics_arc(&self) -> Arc<Metrics> {
        self.bus_events.metrics.clone()
    }

    pub fn bus_event_sessions(&self) -> Result<Vec<BindIdentity>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut sessions = conn
            .prepare("SELECT DISTINCT session FROM run_index")?
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|r| Ok(serde_json::from_str(&r?)?))
            .collect::<Result<Vec<BindIdentity>, StoreError>>()?;
        let candidates = conn
            .prepare("SELECT project_root,harness,session FROM agent_data_session_ref")?
            .query_map([], |r| {
                Ok(BindIdentity {
                    project_root: std::path::PathBuf::from(r.get::<_, String>(0)?),
                    harness: r.get(1)?,
                    session: r.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        sessions.extend(candidates);
        sessions.sort_by_key(Self::key);
        sessions.dedup();
        Ok(sessions)
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
        let conn = self.conn.lock().unwrap();
        let event_key = insert_event(&conn, &account, session, epoch, event, &bytes, now_ms)?;
        // Progress is retained for claim-check reads but never retried as lifecycle work.
        conn.execute(
            "UPDATE bus_event_outbox SET best_effort=1 WHERE event_key=?1",
            params![event_key],
        )?;
        Ok(Some(Publication {
            event_key,
            subject: event
                .subject(&account)
                .map_err(|e| StoreError::BusEvent(e.to_string()))?,
            headers: Headers::from([
                ("kind".into(), event.detail.name().into()),
                ("run_id".into(), event.run_id.clone()),
                ("session_ref".into(), event.session_ref.clone()),
                ("epoch".into(), epoch.into()),
                ("seq".into(), event.seq.to_string()),
            ]),
            body: bytes,
        }))
    }

    pub fn bus_event_position(
        &self,
        session: &BindIdentity,
        epoch: &str,
    ) -> Result<u64, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT seq FROM bus_event_projection WHERE session=?1 AND epoch=?2",
                params![Self::key(session), epoch],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    /// `records` must be bounded by a successful writer or read-side durability barrier.
    /// The event checkpoint is independent of the transcript's turn-boundary watermark:
    /// starting and pausing a run must not wait for a complete assistant turn.
    pub fn project_bus_events(
        &self,
        session: &BindIdentity,
        epoch: &str,
        records: &[FramedRecord],
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let Some(account) = self.bus_events.account.lock().unwrap().clone() else {
            return Ok(());
        };
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let key = Self::key(session);
        let saved: Option<(String, u64, String)> = tx
            .query_row(
                "SELECT epoch,seq,state_json FROM bus_event_projection WHERE session=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (mut seq, mut state) = match saved {
            Some((old_epoch, seq, json)) if old_epoch == epoch => {
                (seq, serde_json::from_str::<Projection>(&json)?)
            }
            _ => (0, Projection::default()),
        };
        let mut oversize = 0;
        let mut dropped = 0;
        for frame in records.iter().filter(|r| r.seq > seq).collect::<Vec<_>>() {
            if frame.seq != seq + 1 {
                return Err(StoreError::BusEvent(
                    "run event WAL range has a sequence gap".into(),
                ));
            }
            let detail = match &frame.record {
                WalRecord::RunStarted {
                    run_id,
                    ts_ms,
                    submission_id,
                    request_identity_digest,
                    title,
                    ..
                } => {
                    state = Projection {
                        run_id: Some(run_id.0.clone()),
                        last_at_ms: ts_ms.unwrap_or(0),
                        ..Projection::default()
                    };
                    tx.execute("INSERT OR IGNORE INTO run_index(run_id,session,submission_id,request_identity_digest,state,state_changed_ms,title) VALUES(?1,?2,?3,?4,'active',?5,?6)",
                        params![run_id.0, serde_json::to_string(session)?, submission_id.as_ref().map(|s| &s.0), request_identity_digest, ts_ms, title])?;
                    Some(RunEventDetail::Started {})
                }
                WalRecord::Steered { send_id, ts_ms, .. } => {
                    state.pending += 1;
                    state.last_at_ms = ts_ms.unwrap_or(state.last_at_ms);
                    // Once this prefix cannot fit, later ids cannot join it without reordering.
                    if let Some(id) = send_id {
                        if !state.truncated
                            && state.send_ids.iter().map(String::len).sum::<usize>() + id.len()
                                <= 512
                        {
                            state.send_ids.push(id.clone());
                        } else {
                            state.truncated = true;
                        }
                    }
                    None
                }
                WalRecord::StepStarted { step_id } if state.pending > 0 => {
                    let detail = RunEventDetail::SteerConsumed {
                        step: step_id.0,
                        consumed: state.pending,
                        send_ids: std::mem::take(&mut state.send_ids),
                        truncated: state.truncated,
                    };
                    state.pending = 0;
                    state.truncated = false;
                    Some(detail)
                }
                WalRecord::StructuredOutputResult { result, .. } => {
                    state.structured_output = Some(match result.validation.as_str() {
                        "passed" => StructuredOutputStatus::Passed,
                        "failed" => StructuredOutputStatus::Failed,
                        _ => StructuredOutputStatus::Absent,
                    });
                    None
                }
                WalRecord::RunPaused {
                    run_id,
                    ts_ms,
                    reason,
                    ..
                } => {
                    state.last_at_ms = ts_ms.unwrap_or(state.last_at_ms);
                    tx.execute("UPDATE run_index SET state='paused',state_changed_ms=?2 WHERE run_id=?1 AND state IN ('active','paused')", params![run_id.0, state.last_at_ms])?;
                    Some(RunEventDetail::Paused {
                        pause_reason: *reason,
                    })
                }
                WalRecord::RunFinished {
                    reason,
                    ts_ms,
                    error,
                    usage,
                    retries_used,
                } => {
                    state.last_at_ms = ts_ms.unwrap_or(state.last_at_ms);
                    if let Some(run_id) = &state.run_id {
                        let terminal = serde_json::to_value(reason)?.as_str().unwrap().to_owned();
                        tx.execute("UPDATE run_index SET state=?2,state_changed_ms=?3,usage_json=?4,retries_used=?5 WHERE run_id=?1", params![run_id, terminal, state.last_at_ms, serde_json::to_string(usage)?, retries_used])?;
                    }
                    Some(RunEventDetail::Ended {
                        terminal: *reason,
                        error_class: error.as_ref().map(|e| e.class.shared_class()),
                        structured_output: Some(
                            state
                                .structured_output
                                .unwrap_or(StructuredOutputStatus::Absent),
                        ),
                    })
                }
                _ => None,
            };
            if let (Some(detail), Some(run_id)) = (detail, state.run_id.as_ref()) {
                let mut event = RunEvent {
                    v: 1,
                    session_ref: agent_session_ref(session),
                    run_id: run_id.clone(),
                    seq: frame.seq,
                    at_ms: state.last_at_ms,
                    detail,
                };
                match event.fit_send_ids() {
                    Ok(_)
                        if event.at_ms != 0
                            && event.at_ms.saturating_add(RETENTION_MS) < now_ms =>
                    {
                        dropped += 1;
                    }
                    Ok(bytes) => {
                        insert_event(&tx, &account, session, epoch, &event, &bytes, now_ms)?;
                    }
                    Err(EncodeError::Oversize(_)) => {
                        oversize += 1;
                        tracing::warn!(target: "bus", seq=frame.seq, "refusing oversize run lifecycle event");
                    }
                    Err(error) => return Err(StoreError::BusEvent(error.to_string())),
                }
            }
            seq = frame.seq;
        }
        tx.execute("INSERT INTO bus_event_projection(session,epoch,seq,state_json) VALUES(?1,?2,?3,?4) ON CONFLICT(session) DO UPDATE SET epoch=excluded.epoch,seq=excluded.seq,state_json=excluded.state_json",
            params![key,epoch,seq,serde_json::to_string(&state)?])?;
        tx.commit()?;
        self.bus_metrics()
            .bus_events_oversize
            .fetch_add(oversize, Ordering::Relaxed);
        if dropped > 0 {
            self.bus_metrics()
                .bus_outbox_dropped
                .fetch_add(dropped, Ordering::Relaxed);
            tracing::warn!(target: "bus", dropped, "dropping recovered run events older than 24 hours");
        }
        drop(conn);
        self.refresh_bus_depth()?;
        Ok(())
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
        self.conn.lock().unwrap().query_row(
            "SELECT event_key,subject,epoch,run_id,seq,session,body FROM bus_event_outbox WHERE published_ms IS NULL AND best_effort=0 ORDER BY rowid LIMIT 1", [],
            |r| {
                let session: String = r.get(5)?;
                let session: BindIdentity = serde_json::from_str(&session).map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e)))?;
                let subject: String = r.get(1)?;
                Ok(Publication { event_key:r.get(0)?, headers: Headers::from([
                    ("kind".into(), subject.split(".event.broca.").nth(1).unwrap_or_default().trim_end_matches(".v1").into()),
                    ("epoch".into(),r.get(2)?), ("run_id".into(),r.get(3)?),
                    ("seq".into(),r.get::<_,u64>(4)?.to_string()), ("session_ref".into(),agent_session_ref(&session)),
                ]), subject, body:r.get(6)? })
            },
        ).optional().map_err(StoreError::from)
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

    pub fn acknowledge_bus_event(
        &self,
        event: &Publication,
        _receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let updated=self.conn.lock().unwrap().execute("UPDATE bus_event_outbox SET published_ms=?2 WHERE event_key=?1 AND published_ms IS NULL",params![event.event_key,now_ms])?;
        self.bus_metrics()
            .bus_events_published
            .fetch_add(updated as u64, Ordering::Relaxed);
        self.refresh_bus_depth()
    }

    pub fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError> {
        let conn = self.conn.lock().unwrap();
        let dropped: u64 = conn.query_row(
            "SELECT count(*) FROM bus_event_outbox WHERE at_ms<?1 AND published_ms IS NULL AND best_effort=0",
            [now_ms.saturating_sub(RETENTION_MS)],
            |r| r.get(0),
        )?;
        let deleted = conn.execute(
            "DELETE FROM bus_event_outbox WHERE at_ms<?1",
            [now_ms.saturating_sub(RETENTION_MS)],
        )?;
        if dropped > 0 {
            self.bus_metrics()
                .bus_outbox_dropped
                .fetch_add(dropped, Ordering::Relaxed);
            tracing::warn!(target: "bus", dropped, "dropping run events older than 24 hours without a publish acknowledgement");
        }
        drop(conn);
        self.refresh_bus_depth()?;
        Ok(deleted)
    }

    fn refresh_bus_depth(&self) -> Result<(), StoreError> {
        let depth = self.conn.lock().unwrap().query_row(
            "SELECT count(*) FROM bus_event_outbox WHERE published_ms IS NULL AND best_effort=0",
            [],
            |r| r.get(0),
        )?;
        self.bus_metrics()
            .bus_outbox_depth
            .store(depth, Ordering::Relaxed);
        Ok(())
    }
}

fn insert_event(
    conn: &Connection,
    account: &AccountNames,
    session: &BindIdentity,
    epoch: &str,
    event: &RunEvent,
    bytes: &[u8],
    now_ms: u64,
) -> Result<String, StoreError> {
    let subject = event
        .subject(account)
        .map_err(|e| StoreError::BusEvent(e.to_string()))?;
    let observation =
        matches!(event.detail, RunEventDetail::Progress { .. }).then_some(event.at_ms);
    let key_bytes = serde_jcs::to_vec(&(epoch, &event.run_id, event.seq, &subject, observation))?;
    let event_key = format!("{:x}", Sha256::digest(key_bytes));
    conn.execute("INSERT OR IGNORE INTO bus_event_outbox(event_key,run_id,seq,subject,epoch,session,body,at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![event_key,event.run_id,event.seq,subject,epoch,serde_json::to_string(session)?,bytes,now_ms])?;
    Ok(event_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::{RunIndex, RunIndexState};
    use broca_protocol::{request::ModelHandle, RunFinishReason, StepId, Usage};
    use std::sync::atomic::AtomicBool;

    fn session() -> BindIdentity {
        BindIdentity::new("/project", "test", "session")
    }
    fn start() -> FramedRecord {
        let mut frame = crate::conformance::run_started(
            1,
            "r1",
            &session(),
            "secret prompt",
            ModelHandle::new("test", "model"),
        );
        if let WalRecord::RunStarted { ts_ms, .. } = &mut frame.record {
            *ts_ms = Some(1000);
        }
        frame
    }
    fn finish(seq: u64, reason: RunFinishReason) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record: WalRecord::RunFinished {
                reason,
                ts_ms: Some(2000),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        }
    }
    fn enabled() -> SqliteStore {
        let s = SqliteStore::open_in_memory().unwrap();
        s.enable_bus_events("box_test").unwrap();
        s
    }
    type OutboxRow = (String, u64, String, Vec<u8>, Option<u64>);
    fn rows(store: &SqliteStore) -> Vec<OutboxRow> {
        store.conn.lock().unwrap().prepare("SELECT event_key,seq,subject,body,published_ms FROM bus_event_outbox ORDER BY rowid").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap().collect::<Result<_,_>>().unwrap()
    }

    #[test]
    fn bus_outbox_off_writes_nothing() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .project_bus_events(&session(), "epoch", &[start()], 1000)
            .unwrap();
        assert!(rows(&store).is_empty());
        assert_eq!(store.bus_event_position(&session(), "epoch").unwrap(), 0);
    }

    #[test]
    fn bus_outbox_projection_is_ordered_idempotent_and_atomic_with_run_index() {
        let store = enabled();
        let records = [start(), finish(2, RunFinishReason::Completed)];
        store
            .project_bus_events(&session(), "epoch", &records, 2000)
            .unwrap();
        store
            .project_bus_events(&session(), "epoch", &records, 3000)
            .unwrap();
        let values = rows(&store);
        assert_eq!(values.len(), 2);
        assert_eq!(values.iter().map(|r| r.1).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(
            store.lookup_run("r1").unwrap().unwrap().state,
            RunIndexState::Completed
        );
        for (key, _, _, body, _) in &values {
            assert_eq!(store.get_bus_event(key).unwrap().unwrap().1, *body);
            assert!(!String::from_utf8_lossy(body).contains("secret prompt"));
            let json: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(serde_jcs::to_vec(&json).unwrap(), *body);
        }
        let broken = enabled();
        broken.conn.lock().unwrap().execute_batch("CREATE TRIGGER reject_event BEFORE INSERT ON bus_event_outbox BEGIN SELECT RAISE(ABORT,'injected outbox failure'); END;").unwrap();
        assert!(broken
            .project_bus_events(&session(), "epoch", &records, 2000)
            .is_err());
        assert!(broken.lookup_run("r1").unwrap().is_none());
        assert_eq!(broken.bus_event_position(&session(), "epoch").unwrap(), 0);
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
        store
            .project_bus_events(&session(), "epoch", &[start()], 1000)
            .unwrap();
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

    #[test]
    fn bus_outbox_steers_consumed_only_by_the_following_step_and_seals_end_interrupted() {
        let store = enabled();
        let mut records = vec![start()];
        for (seq, id) in [(2, "a"), (3, "b")] {
            records.push(FramedRecord {
                seq,
                fence: 1,
                record: WalRecord::Steered {
                    retention: None,
                    ts_ms: Some(1100),
                    send_id: Some(id.into()),
                    request_identity_digest: None,
                    delivery: None,
                    title: None,
                    input: Arc::new(vec![]),
                    hooks: None,
                },
            });
        }
        store
            .project_bus_events(&session(), "epoch", &records, 1200)
            .unwrap();
        assert_eq!(rows(&store).len(), 1);
        records.push(FramedRecord {
            seq: 4,
            fence: 1,
            record: WalRecord::StepStarted { step_id: StepId(3) },
        });
        records.push(finish(5, RunFinishReason::Interrupted));
        store
            .project_bus_events(&session(), "epoch", &records, 2000)
            .unwrap();
        let events = rows(&store);
        assert_eq!(events.iter().map(|r| r.1).collect::<Vec<_>>(), [1, 4, 5]);
        let consumed: serde_json::Value = serde_json::from_slice(&events[1].3).unwrap();
        assert_eq!(consumed["send_ids"], serde_json::json!(["a", "b"]));
        assert_eq!(consumed["consumed"], 2);
        assert_eq!(consumed["step"], 3);
        let ended: serde_json::Value = serde_json::from_slice(&events[2].3).unwrap();
        assert_eq!(ended["terminal"], "interrupted");
    }

    #[test]
    fn bus_outbox_oversize_is_refused_and_counted() {
        let store = enabled();
        let mut frame = start();
        if let WalRecord::RunStarted { run_id, .. } = &mut frame.record {
            run_id.0 = "x".repeat(512);
        }
        store
            .project_bus_events(&session(), "epoch", &[frame], 1000)
            .unwrap();
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
        store
            .project_bus_events(&session(), "epoch", &[start()], 1000)
            .unwrap();
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
}
