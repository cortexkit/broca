//! Authoritative bus work is independent of rebuildable, authenticated compaction locators.

use super::{DbError, LogLocation, OverhaulDb};
use crate::bus_events::{self, EventStore, State};
use crate::bus_events::{Metrics, PeriodStart, Publication, SkipSummary};
use crate::compaction_facts::CompactionFacts;
use crate::compaction_receipts::{self, CompactionReceiptStore, ReceiptWrite, ViewReceipt};
use crate::StoreError;
use broca_core::replay::SeedFallback;
use broca_core::{SessionRefResolver, SessionRefs};
use broca_protocol::run_events::RunEvent;
use broca_protocol::BindIdentity;
use broca_wal::file_wal::DurablePublication;
use broca_wal::{access_counter, FramedRecord, WalRecord};
use cortexkit_bus_trait::PublishReceipt;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(super) fn initialize(conn: &Connection) -> Result<(), DbError> {
    bus_events::init_event_schema(conn, false)?;
    // Retention completion owns receipt cleanup, not a rebuild of locator rows.
    compaction_receipts::init_schema(conn)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS compaction_resolution_source (
            sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            seq INTEGER NOT NULL, frame_off INTEGER NOT NULL, frame_end INTEGER NOT NULL,
            frame_identity BLOB NOT NULL, PRIMARY KEY(sid,seq), CHECK(frame_end>frame_off)
         );
         CREATE TABLE IF NOT EXISTS compaction_resolution_projection (
            sid TEXT PRIMARY KEY REFERENCES session(sid) ON DELETE CASCADE,
            version INTEGER NOT NULL, through_seq INTEGER NOT NULL, source_count INTEGER NOT NULL
         );",
    )?;
    Ok(())
}

/// Both indexing entry points reject a cursor whose compaction locators are incomplete.
pub(super) fn projection_current(conn: &Connection, sid: &str) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session s LEFT JOIN compaction_resolution_projection p USING(sid)
         WHERE s.sid=?1 AND ((s.indexed_seq IS NULL AND NOT EXISTS(SELECT 1 FROM compaction_resolution_source c WHERE c.sid=s.sid))
           OR (p.version=1 AND p.through_seq=s.indexed_seq
           AND p.source_count=(SELECT count(*) FROM compaction_resolution_source c WHERE c.sid=s.sid))))",
        [sid], |r| r.get(0),
    )?)
}

pub(super) fn observe(
    conn: &Connection,
    sid: &str,
    frame: &super::index::IndexedFrame,
) -> Result<(), DbError> {
    let relevant = matches!(
        frame.framed.record,
        WalRecord::CompactionMessage {
            kind: broca_wal::CompactionMessageKind::Setup,
            ..
        } | WalRecord::CompactionViewApplied { .. }
            | WalRecord::CompactionNotice { .. }
    );
    let inserted = if relevant {
        conn.execute(
            "INSERT INTO compaction_resolution_source VALUES(?1,?2,?3,?4,?5)",
            params![sid, frame.framed.seq, frame.off, frame.end, frame.identity],
        )?
    } else {
        0
    };
    conn.execute(
        "INSERT INTO compaction_resolution_projection VALUES(?1,1,?2,?3)
        ON CONFLICT(sid) DO UPDATE SET through_seq=excluded.through_seq,
        source_count=compaction_resolution_projection.source_count+excluded.source_count",
        params![sid, frame.framed.seq, inserted],
    )?;
    Ok(())
}

/// Shares the `OverhaulDb` instance; it never constructs a legacy compatibility store.
pub struct OverhaulRunEventStore {
    db: Arc<OverhaulDb>,
    state: State,
}

impl OverhaulRunEventStore {
    pub fn new(db: Arc<OverhaulDb>) -> Self {
        Self {
            db,
            state: State::default(),
        }
    }
    fn events(&self) -> EventStore<'_> {
        EventStore {
            conn: &self.db.conn,
            bus_events: &self.state,
            legacy: false,
        }
    }
}

fn store_error(error: DbError) -> StoreError {
    StoreError::BusEvent(error.to_string())
}
fn db_error(error: StoreError) -> DbError {
    DbError::Invalid(error.to_string())
}

type PublicationSource = dyn Fn(&BindIdentity) -> Result<DurablePublication, String> + Send + Sync;

struct DatabaseCompactionReceiptStore {
    db: Arc<OverhaulDb>,
    publication: Box<PublicationSource>,
}

impl OverhaulDb {
    /// The publication source must name the writer's synced prefix, including boot catch-up.
    pub fn compaction_receipt_store(
        self: &Arc<Self>,
        publication: impl Fn(&BindIdentity) -> Result<DurablePublication, String>
            + Send
            + Sync
            + 'static,
    ) -> Arc<dyn CompactionReceiptStore> {
        Arc::new(DatabaseCompactionReceiptStore {
            db: self.clone(),
            publication: Box::new(publication),
        })
    }
}

impl SessionRefResolver for DatabaseCompactionReceiptStore {
    fn resolve_refs(&self, refs: &[&str]) -> Result<SessionRefs, String> {
        self.db.resolve_refs(refs)
    }
    fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String> {
        self.db.named_refs(name, limit)
    }
}

impl CompactionReceiptStore for DatabaseCompactionReceiptStore {
    fn receive_compaction_view(&self, receipt: &ViewReceipt) -> Result<ReceiptWrite, StoreError> {
        self.db
            .authoritative_transaction(|tx| {
                compaction_receipts::receive(tx, receipt).map_err(db_error)
            })
            .map_err(store_error)
    }
    fn compaction_receipts(
        &self,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ViewReceipt>, StoreError> {
        self.db
            .read_snapshot(|tx| compaction_receipts::list(tx, after, limit).map_err(db_error))
            .map_err(store_error)
    }
    fn clear_compaction_receipt(&self, receipt: &ViewReceipt) -> Result<(), StoreError> {
        self.db
            .authoritative_transaction(|tx| {
                compaction_receipts::clear(tx, receipt).map_err(db_error)
            })
            .map_err(store_error)
    }
    fn current_compaction_facts(
        &self,
        _wal: &dyn broca_core::WalStore,
        session: &BindIdentity,
    ) -> Result<CompactionFacts, String> {
        let publication = (self.publication)(session)?;
        self.db
            .compaction_resolution(session, publication)
            .map_err(|e| e.to_string())
    }
}

impl bus_events::RunEventStore for OverhaulRunEventStore {
    fn enable_bus_events(&self, account: &str) -> Result<(), String> {
        self.events().enable_bus_events(account)
    }
    fn bus_metrics(&self) -> &Metrics {
        &self.state.metrics
    }
    fn bus_metrics_arc(&self) -> Arc<Metrics> {
        self.events().bus_metrics_arc()
    }
    fn begin_bus_event_period(&self, now_ms: u64) -> Result<PeriodStart, StoreError> {
        self.events().begin_bus_event_period(now_ms)
    }
    fn end_bus_event_period(&self) -> Result<bool, StoreError> {
        self.events().end_bus_event_period()
    }
    fn bus_event_open_sessions(&self) -> Result<Vec<BindIdentity>, StoreError> {
        self.events().bus_event_open_sessions()
    }
    fn bus_event_period_started_ms(&self) -> u64 {
        self.events().bus_event_period_started_ms()
    }
    fn bus_event_checkpoint(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<(String, u64)>, StoreError> {
        self.events().bus_event_checkpoint(session)
    }
    fn bus_event_position(&self, session: &BindIdentity, epoch: &str) -> Result<u64, StoreError> {
        self.events().bus_event_position(session, epoch)
    }
    fn project_new_bus_events(
        &self,
        session: &BindIdentity,
        epoch: &str,
        records: &[FramedRecord],
        now_ms: u64,
    ) -> Result<(), StoreError> {
        self.events()
            .project_new_bus_events(session, epoch, records, now_ms)
    }
    fn retain_bus_progress(
        &self,
        session: &BindIdentity,
        epoch: &str,
        event: &RunEvent,
        now_ms: u64,
    ) -> Result<Option<Publication>, StoreError> {
        self.events()
            .retain_bus_progress(session, epoch, event, now_ms)
    }
    fn get_bus_event(
        &self,
        event_key: &str,
    ) -> Result<Option<(BindIdentity, Vec<u8>)>, StoreError> {
        self.events().get_bus_event(event_key)
    }
    fn pending_bus_event(&self) -> Result<Option<Publication>, StoreError> {
        self.events().pending_bus_event()
    }
    fn acknowledge_bus_event(
        &self,
        event: &Publication,
        receipt: PublishReceipt,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        self.events().acknowledge_bus_event(event, receipt, now_ms)
    }
    fn skip_stale_bus_events(&self, now_ms: u64) -> Result<SkipSummary, StoreError> {
        self.events().skip_stale_bus_events(now_ms)
    }
    fn prune_bus_events(&self, now_ms: u64) -> Result<usize, StoreError> {
        self.events().prune_bus_events(now_ms)
    }
}

fn bad(reason: &'static str) -> DbError {
    DbError::SeedFallback(SeedFallback::new(reason))
}

impl OverhaulDb {
    fn compaction_resolution(
        self: &Arc<Self>,
        session: &BindIdentity,
        publication: DurablePublication,
    ) -> Result<CompactionFacts, DbError> {
        // Catch-up also repairs an older database whose row cursor predates these locators.
        self.publish(
            session,
            &broca_core::agent_session_ref(session),
            LogLocation::Live,
            &publication,
        )?;
        let lineage = publication.lineage().to_string();
        let reader = self.episode_reader(session.clone(), publication)?;
        let read = |off, len| reader.read_at(off, len);
        let (_, header) = broca_core::projection::log_header_info(&read)?;
        if header.map(|id| id.to_string()).as_deref() != Some(&lineage) {
            return Err(bad("compaction resolution lineage mismatch"));
        }
        let sources = self.read_snapshot(|tx| {
            let key = super::index::sid(session)?;
            if !projection_current(tx, &key)? {
                return Err(bad("compaction resolution locators incomplete"));
            }
            let indexed: (String, u64, u64) = tx.query_row("SELECT lineage_id,indexed_seq,indexed_end FROM session WHERE sid=?1", [&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            if indexed != (lineage.clone(), reader.sequence(), reader.byte_end()) {
                return Err(bad("compaction resolution publication mismatch"));
            }
            let mut query = tx.prepare("SELECT seq,frame_off,frame_end,frame_identity FROM compaction_resolution_source WHERE sid=?1 ORDER BY seq")?;
            let rows = query.query_map([&key], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,u64>(1)?,r.get::<_,u64>(2)?,r.get::<_,Vec<u8>>(3)?)))?;
            let mut frames = Vec::new();
            for row in rows {
                let (seq, off, end, identity) = row?;
                let len = end.checked_sub(off).filter(|n| *n>0 && end<=reader.byte_end()).ok_or_else(|| bad("compaction resolution locator range invalid"))?;
                let bytes = access_counter::read_at(&read, off, len)?;
                let frame = match access_counter::decode_next(&bytes) {
                    broca_wal::framing::DecodeStep::Record { framed, consumed }
                        if consumed==bytes.len() && framed.seq==seq && Sha256::digest(&bytes).as_slice()==identity => *framed,
                    _ => return Err(bad("compaction resolution locator identity mismatch")),
                };
                if !matches!(frame.record, WalRecord::CompactionMessage { kind: broca_wal::CompactionMessageKind::Setup, .. } | WalRecord::CompactionViewApplied { .. } | WalRecord::CompactionNotice { .. }) {
                    return Err(bad("compaction resolution locator kind mismatch"));
                }
                frames.push(frame);
            }
            Ok(frames)
        });
        let frames = match sources {
            Ok(frames) => frames,
            // A damaged locator never decides whether a receipt may be cleared.
            Err(DbError::SeedFallback(_)) => reader.complete_read()?,
            Err(error) => return Err(error),
        };
        resolution_facts(&lineage, reader.sequence(), reader.byte_end(), &frames)
    }
}

fn resolution_facts(
    lineage: &str,
    head: u64,
    end: u64,
    frames: &[FramedRecord],
) -> Result<CompactionFacts, DbError> {
    let mut facts = CompactionFacts {
        lineage_id: lineage.into(),
        watermark: head,
        byte_end: Some(end),
        ..Default::default()
    };
    for frame in frames {
        match &frame.record {
            WalRecord::CompactionMessage {
                kind: broca_wal::CompactionMessageKind::Setup,
                ..
            } => facts.setup = true,
            WalRecord::CompactionViewApplied { view_version, .. } => {
                facts.applied = facts.applied.max(*view_version)
            }
            WalRecord::CompactionNotice { headers, .. } => {
                if headers.get("lineage_id").map(String::as_str) != Some(lineage) {
                    return Err(bad("compaction resolution notice lineage mismatch"));
                }
                if let Some(version) = headers
                    .get("applied_view_version")
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    facts.noticed = facts.noticed.max(version);
                }
                if let Some(events) = headers.get("events") {
                    for event in events.split(',') {
                        if let Some(version) = event
                            .strip_prefix("view_refused:")
                            .and_then(|s| s.split_once(':'))
                            .and_then(|(v, _)| v.parse::<u64>().ok())
                        {
                            facts.refused = facts.refused.max(version);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus_events::{
        BusPublisher, DrainResult, RunEventStore, RETENTION_MS, STALE_BACKLOG_MS,
    };
    use broca_protocol::{ModelHandle, RunFinishReason, Usage};
    use broca_wal::Wal;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    };

    struct Root(std::path::PathBuf);
    impl Root {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "outbox-db-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
            Self(root)
        }
        fn db(&self) -> Arc<OverhaulDb> {
            Arc::new(OverhaulDb::open(&self.0).unwrap())
        }
        fn no_legacy(&self) {
            for name in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
                assert!(!self.0.join(name).exists(), "created {name}");
            }
        }
    }
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn session() -> BindIdentity {
        BindIdentity::new("/test", "runner", "outbox")
    }
    fn start(seq: u64, run: &str, at: u64) -> FramedRecord {
        let mut frame = crate::conformance::run_started(
            seq,
            run,
            &session(),
            "SENTINEL_PROMPT",
            ModelHandle::new("test", "model"),
        );
        if let WalRecord::RunStarted { ts_ms, title, .. } = &mut frame.record {
            *ts_ms = Some(at);
            *title = Some("SENTINEL_TITLE".into());
        }
        frame
    }
    fn finish(seq: u64, at: u64) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record: WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                interrupt_reason: None,
                ts_ms: Some(at),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        }
    }
    fn armed(db: Arc<OverhaulDb>, at: u64) -> OverhaulRunEventStore {
        let events = OverhaulRunEventStore::new(db);
        normal(&events.db);
        events.begin_bus_event_period(at).unwrap();
        full(&events.db);
        events.enable_bus_events("box_test").unwrap();
        events
    }
    fn scalar(db: &OverhaulDb, sql: &str) -> u64 {
        db.read_snapshot(|tx| Ok(tx.query_row(sql, [], |r| r.get(0))?))
            .unwrap()
    }
    fn full(db: &OverhaulDb) {
        assert_eq!(scalar(db, "PRAGMA synchronous"), 2);
    }
    fn normal(db: &OverhaulDb) {
        db.transaction(false, |_| Ok(())).unwrap();
        assert_eq!(scalar(db, "PRAGMA synchronous"), 1);
    }
    fn receipt(version: u64, lineage: &str) -> ViewReceipt {
        ViewReceipt {
            ready: broca_core::compaction_provider::notice::ViewReady::new(
                broca_core::agent_session_ref(&session()),
                version,
                0,
                "a".repeat(64),
            ),
            lineage_id: lineage.into(),
            received_at_ms: 1,
        }
    }
    struct Publisher {
        fail: bool,
        sent: Mutex<Vec<(String, Vec<u8>)>>,
    }
    #[async_trait::async_trait]
    impl BusPublisher for Publisher {
        async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
            self.sent
                .lock()
                .unwrap()
                .push((event.event_key.clone(), event.body.clone()));
            if self.fail {
                Err("offline".into())
            } else {
                Ok(PublishReceipt::new(1, false))
            }
        }
    }

    #[tokio::test]
    async fn overhaul_outbox_restart_retry_ack_prune_and_idempotence() {
        let root = Root::new();
        let events = armed(root.db(), 1000);
        let records = [start(1, "r1", 1000), finish(2, 2000)];
        normal(&events.db);
        events
            .project_new_bus_events(&session(), "epoch", &records, 2000)
            .unwrap();
        full(&events.db);
        events
            .project_new_bus_events(&session(), "epoch", &records, 2000)
            .unwrap();
        assert_eq!(
            events
                .bus_metrics()
                .bus_outbox_depth
                .load(Ordering::Relaxed),
            2
        );
        let pending = events.pending_bus_event().unwrap().unwrap();
        let offline = Publisher {
            fail: true,
            sent: Mutex::new(Vec::new()),
        };
        assert_eq!(
            events.drain_bus_event(&offline, 2001).await.unwrap(),
            DrainResult::Retry
        );
        assert_eq!(
            events.pending_bus_event().unwrap().unwrap().body,
            pending.body
        );
        assert_eq!(
            events
                .bus_metrics()
                .bus_publish_failures
                .load(Ordering::Relaxed),
            1
        );
        drop(events);

        let events = armed(root.db(), 3000);
        assert_eq!(
            events.bus_event_checkpoint(&session()).unwrap(),
            Some(("epoch".into(), 2))
        );
        assert_eq!(
            events
                .bus_metrics()
                .bus_outbox_depth
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            events.pending_bus_event().unwrap().unwrap().event_key,
            pending.event_key
        );
        let online = Publisher {
            fail: false,
            sent: Mutex::new(Vec::new()),
        };
        normal(&events.db);
        assert_eq!(
            events.drain_bus_event(&online, 3000).await.unwrap(),
            DrainResult::Published
        );
        full(&events.db);
        assert_eq!(
            events.get_bus_event(&pending.event_key).unwrap().unwrap().1,
            pending.body
        );
        events
            .acknowledge_bus_event(&pending, PublishReceipt::new(1, true), 3001)
            .unwrap();
        assert_eq!(
            events
                .bus_metrics()
                .bus_outbox_depth
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            events
                .bus_metrics()
                .bus_events_published
                .load(Ordering::Relaxed),
            1
        );
        normal(&events.db);
        assert_eq!(events.prune_bus_events(RETENTION_MS + 2001).unwrap(), 2);
        full(&events.db);
        assert_eq!(
            events
                .bus_metrics()
                .bus_outbox_dropped
                .load(Ordering::Relaxed),
            1
        );
        assert!(events.get_bus_event(&pending.event_key).unwrap().is_none());
        assert!(events.pending_bus_event().unwrap().is_none());
        root.no_legacy();
    }

    #[test]
    fn overhaul_disabled_periods_drop_only_unpublished_work_and_never_replay_history() {
        let root = Root::new();
        let events = OverhaulRunEventStore::new(root.db());
        assert!(!events.end_bus_event_period().unwrap());
        events
            .project_new_bus_events(&session(), "epoch", &[start(1, "old", 100)], 100)
            .unwrap();
        assert!(events.bus_event_checkpoint(&session()).unwrap().is_none());
        let first = events.begin_bus_event_period(1000).unwrap();
        assert!(first.fresh);
        events.enable_bus_events("box_test").unwrap();
        let records = [
            start(1, "old", 100),
            finish(2, 200),
            start(3, "new", 1000),
            finish(4, 1100),
        ];
        events
            .project_new_bus_events(&session(), "epoch", &records, 1100)
            .unwrap();
        assert_eq!(
            events.pending_bus_event().unwrap().unwrap().headers["run_id"],
            "new"
        );
        let event = events.pending_bus_event().unwrap().unwrap();
        events
            .acknowledge_bus_event(&event, PublishReceipt::new(1, false), 1200)
            .unwrap();
        assert_eq!(events.bus_event_position(&session(), "epoch").unwrap(), 4);
        assert!(!events.begin_bus_event_period(1300).unwrap().fresh);
        assert!(events.end_bus_event_period().unwrap());
        drop(events);
        let events = OverhaulRunEventStore::new(root.db());
        let next = events.begin_bus_event_period(3000).unwrap();
        assert_eq!((next.fresh, next.discarded), (true, 1));
        assert!(events.bus_event_checkpoint(&session()).unwrap().is_none());
        events.enable_bus_events("box_test").unwrap();
        events
            .project_new_bus_events(&session(), "epoch", &records, 3000)
            .unwrap();
        assert!(events.pending_bus_event().unwrap().is_none());
        assert!(events.get_bus_event(&event.event_key).unwrap().is_some());
        full(&events.db);
        root.no_legacy();
    }

    #[test]
    fn overhaul_stale_replacement_and_projection_failure_are_atomic() {
        let root = Root::new();
        let db = root.db();
        let events = armed(db.clone(), 1000);
        db.authoritative_transaction(|tx| { tx.execute_batch("CREATE TRIGGER fail_projection BEFORE INSERT ON bus_event_projection BEGIN SELECT RAISE(ABORT,'stop'); END;")?; Ok(()) }).unwrap();
        assert!(events
            .project_new_bus_events(&session(), "epoch", &[start(1, "r", 1000)], 1000)
            .is_err());
        assert!(events.pending_bus_event().unwrap().is_none());
        assert!(events.bus_event_checkpoint(&session()).unwrap().is_none());
        db.authoritative_transaction(|tx| {
            tx.execute_batch("DROP TRIGGER fail_projection;")?;
            Ok(())
        })
        .unwrap();
        events
            .project_new_bus_events(
                &session(),
                "epoch",
                &[start(1, "r", 1000), finish(2, 1100)],
                1100,
            )
            .unwrap();
        db.authoritative_transaction(|tx| { tx.execute_batch("CREATE TRIGGER fail_marker BEFORE INSERT ON bus_event_outbox WHEN NEW.run_id='' BEGIN SELECT RAISE(ABORT,'stop'); END;")?; Ok(()) }).unwrap();
        assert!(events
            .skip_stale_bus_events(1101 + STALE_BACKLOG_MS)
            .is_err());
        assert_eq!(scalar(&db, "SELECT count(*) FROM bus_event_outbox"), 2);
        db.authoritative_transaction(|tx| {
            tx.execute_batch("DROP TRIGGER fail_marker;")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            events
                .skip_stale_bus_events(1101 + STALE_BACKLOG_MS)
                .unwrap(),
            SkipSummary {
                sessions: 1,
                skipped: 2
            }
        );
        full(&db);
        let marker = events.pending_bus_event().unwrap().unwrap();
        assert_eq!(marker.headers["kind"], "run.skipped");
        drop(events);
        drop(db);
        let events = armed(root.db(), 2000);
        assert_eq!(
            events.pending_bus_event().unwrap().unwrap().body,
            marker.body
        );
        assert_eq!(
            events
                .skip_stale_bus_events(1102 + STALE_BACKLOG_MS)
                .unwrap(),
            SkipSummary::default()
        );
    }

    #[test]
    fn overhaul_receipts_restart_monotonic_replacement_compare_delete_and_retention() {
        let root = Root::new();
        let db = root.db();
        let store = db.compaction_receipt_store(|_| Err("unused".into()));
        let old = receipt(2, "lineage");
        normal(&db);
        assert_eq!(
            store.receive_compaction_view(&old).unwrap(),
            ReceiptWrite::Recorded { replaced: None }
        );
        full(&db);
        assert_eq!(
            store
                .receive_compaction_view(&receipt(1, "lineage"))
                .unwrap(),
            ReceiptWrite::Held
        );
        assert_eq!(
            store.receive_compaction_view(&old).unwrap(),
            ReceiptWrite::Held
        );
        drop(store);
        drop(db);
        let db = root.db();
        let store = db.compaction_receipt_store(|_| Err("unused".into()));
        assert_eq!(
            store.compaction_receipts("", 256).unwrap(),
            std::slice::from_ref(&old)
        );
        let newer = receipt(u64::MAX, "lineage");
        assert!(matches!(
            store.receive_compaction_view(&newer).unwrap(),
            ReceiptWrite::Recorded { replaced: Some(_) }
        ));
        normal(&db);
        store.clear_compaction_receipt(&old).unwrap();
        full(&db);
        assert_eq!(
            store.compaction_receipts("", usize::MAX).unwrap(),
            std::slice::from_ref(&newer)
        );
        let replacement = receipt(1, "replacement");
        assert!(matches!(
            store.receive_compaction_view(&replacement).unwrap(),
            ReceiptWrite::Recorded { replaced: Some(_) }
        ));
        store.clear_compaction_receipt(&newer).unwrap();
        assert_eq!(store.compaction_receipts("", 256).unwrap(), [replacement]);
        db.authoritative_transaction(|tx| {
            tx.execute(
                "INSERT INTO retention_deletion VALUES(?1,?2)",
                params!["replacement", serde_json::to_string(&session()).unwrap()],
            )?;
            tx.execute("DELETE FROM retention_deletion", [])?;
            Ok(())
        })
        .unwrap();
        assert!(store.compaction_receipts("", 256).unwrap().is_empty());
        root.no_legacy();
    }

    fn setup() -> WalRecord {
        WalRecord::CompactionMessage {
            provider: "magic-context".into(),
            kind: broca_wal::CompactionMessageKind::Setup,
            request_id: None,
            cursor: None,
            compaction_id: "head".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 0 },
            replacement: Arc::new(vec![]),
            sha256: broca_wal::compaction_replacement_sha256(&[]).unwrap(),
            call_when: None,
            stability: None,
            ceiling: None,
            tag_checkpoint: None,
            ride_wait_ms: None,
            view_features: None,
            view_due: None,
        }
    }
    fn writer(root: &Root) -> broca_wal::FileWal {
        broca_wal::FileWal::open(root.0.join("session.wal"))
            .unwrap()
            .with_feature_floor(root.0.join("wal-floor"))
            .with_wal_config(broca_wal::WalConfig::OVERHAUL)
            .unwrap()
    }
    fn corpus(lineage: &str) -> Vec<WalRecord> {
        use broca_core::compaction_provider::{
            lineage::MessageRef,
            notice::{AdvanceNotice, Event},
            view::{CompactionView, ViewRange},
        };
        let view = CompactionView::new(
            broca_core::agent_session_ref(&session()),
            2,
            lineage,
            MessageRef {
                ordinal: 0,
                mid: "m0".into(),
            },
            vec![ViewRange::replace(0, 1, "[]")],
        );
        let mut notice = AdvanceNotice::new(
            broca_core::agent_session_ref(&session()),
            1,
            lineage,
            0,
            "a".repeat(64),
            10,
            "m",
        );
        notice.applied_view_version = Some(2);
        notice.events.push(Event::ViewRefused {
            view_version: 3,
            reason: "provider_constraint".into(),
        });
        vec![
            start(1, "r1", 1000).record,
            setup(),
            WalRecord::CompactionViewApplied {
                provider: "magic-context".into(),
                view_version: 2,
                view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                    .unwrap(),
                choice: broca_wal::CompactionViewChoice::Applied,
            },
            WalRecord::CompactionNotice {
                seq: 1,
                headers: notice.to_headers(),
            },
            finish(5, 2000).record,
        ]
    }
    fn append(
        db: &OverhaulDb,
        wal: &mut broca_wal::FileWal,
        record: &WalRecord,
    ) -> super::super::IndexOutcome {
        wal.append(1, record, false).unwrap();
        wal.sync().unwrap();
        db.publish(
            &session(),
            &broca_core::agent_session_ref(&session()),
            LogLocation::Live,
            &wal.durable_publication().unwrap(),
        )
        .unwrap()
    }
    fn assert_no_content(db: &OverhaulDb) {
        let conn = db.conn.lock().unwrap();
        for table in [
            "compaction_resolution_source",
            "compaction_resolution_projection",
        ] {
            let columns = conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert!(columns.iter().all(|c| [
                "sid",
                "seq",
                "frame_off",
                "frame_end",
                "frame_identity",
                "version",
                "through_seq",
                "source_count"
            ]
            .contains(&c.as_str())));
        }
        assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('compaction_facts','run_index','meta')",[],|r| r.get::<_,u64>(0)).unwrap(),0);
        let mut query = conn
            .prepare("SELECT state_json FROM bus_event_projection")
            .unwrap();
        for row in query.query_map([], |r| r.get::<_, String>(0)).unwrap() {
            let row = row.unwrap();
            assert!(!row.contains("SENTINEL_PROMPT") && !row.contains("SENTINEL_TITLE"));
        }
    }

    #[test]
    fn overhaul_compaction_locators_work_with_events_disabled_and_match_rebuild() {
        let root = Root::new();
        let db = root.db();
        let mut wal = writer(&root);
        // Mint the lineage before constructing notices that must name that lineage.
        wal.append(1, &start(1, "r1", 1000).record, false).unwrap();
        wal.sync().unwrap();
        let lineage = wal.durable_publication().unwrap().lineage().to_string();
        let records = corpus(&lineage);
        db.publish(
            &session(),
            &broca_core::agent_session_ref(&session()),
            LogLocation::Live,
            &wal.durable_publication().unwrap(),
        )
        .unwrap();
        for record in &records[1..] {
            append(&db, &mut wal, record);
        }
        assert_eq!(scalar(&db, "PRAGMA synchronous"), 1);
        assert_eq!(scalar(&db, "SELECT count(*) FROM bus_event_outbox"), 0);
        assert_eq!(scalar(&db, "SELECT count(*) FROM bus_event_period"), 0);
        let facts = db
            .compaction_resolution(&session(), wal.durable_publication().unwrap())
            .unwrap();
        assert!(facts.setup);
        assert_eq!(
            (facts.watermark, facts.applied, facts.noticed, facts.refused),
            (5, 2, 2, 3)
        );
        assert!(facts.resolved(2) && facts.resolved(3) && !facts.resolved(4));
        let receipt_store = db.compaction_receipt_store(|_| Err("unused".into()));
        receipt_store
            .receive_compaction_view(&receipt(3, &lineage))
            .unwrap();
        let events = armed(db.clone(), 1000);
        let framed = records
            .iter()
            .enumerate()
            .map(|(i, r)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record: r.clone(),
            })
            .collect::<Vec<_>>();
        events
            .project_new_bus_events(&session(), &lineage, &framed, 2000)
            .unwrap();
        let pending = events.pending_bus_event().unwrap().unwrap();
        assert_no_content(&db);
        let bytes = std::fs::read(root.0.join("session.wal")).unwrap();
        db.rebuild_session(&super::super::PublishedSession {
            bind: &session(),
            session_ref: &broca_core::agent_session_ref(&session()),
            location: LogLocation::Live,
            bytes: &bytes,
        })
        .unwrap();
        assert_eq!(
            db.compaction_resolution(&session(), wal.durable_publication().unwrap())
                .unwrap(),
            facts
        );
        assert_eq!(
            events.pending_bus_event().unwrap().unwrap().event_key,
            pending.event_key
        );
        assert_eq!(receipt_store.compaction_receipts("", 256).unwrap().len(), 1);
        assert_eq!(
            events.bus_event_checkpoint(&session()).unwrap(),
            Some((lineage.clone(), 5))
        );
        let publication = wal.durable_publication().unwrap();
        let store = db.compaction_receipt_store(move |_| Ok(publication.clone()));
        let unused = broca_core::FileWalStore::new(&root.0);
        assert_eq!(
            store.current_compaction_facts(&unused, &session()).unwrap(),
            facts
        );
        root.no_legacy();
    }

    #[test]
    fn overhaul_old_database_incremental_publish_rebuilds_locators_only_once() {
        let root = Root::new();
        let db = root.db();
        let mut wal = writer(&root);
        append(&db, &mut wal, &start(1, "r1", 1000).record);
        let lineage = wal.durable_publication().unwrap().lineage().to_string();
        for record in &corpus(&lineage)[1..] {
            append(&db, &mut wal, record);
        }
        let store = db.compaction_receipt_store(|_| Err("unused".into()));
        store
            .receive_compaction_view(&receipt(3, &lineage))
            .unwrap();
        // Removing just the new tables reproduces the immediately preceding database schema.
        db.authoritative_transaction(|tx| { tx.execute_batch("DROP TABLE compaction_resolution_source; DROP TABLE compaction_resolution_projection;")?; Ok(()) }).unwrap();
        drop(store);
        drop(db);
        let db = root.db();
        let counter = access_counter::WalAccessCounter::default();
        let first = counter.operation("old database").run(|| {
            db.publish(
                &session(),
                &broca_core::agent_session_ref(&session()),
                LogLocation::Live,
                &wal.durable_publication().unwrap(),
            )
            .unwrap()
        });
        assert!(first.rebuilt);
        assert_eq!(
            counter.snapshot()[0].seed_fallbacks,
            [
                "compaction resolution locators incomplete",
                "publication continuation rebuild"
            ]
        );
        assert_eq!(
            scalar(&db, "SELECT count(*) FROM compaction_resolution_source"),
            3
        );
        let second = counter
            .operation("already upgraded")
            .run(|| append(&db, &mut wal, &start(6, "r2", 3000).record));
        assert!(!second.rebuilt);
        assert!(counter.snapshot()[1].seed_fallbacks.is_empty());
        assert_eq!(
            scalar(&db, "SELECT count(*) FROM compaction_view_receipt"),
            1
        );
        let facts = db
            .compaction_resolution(&session(), wal.durable_publication().unwrap())
            .unwrap();
        assert_eq!((facts.applied, facts.noticed, facts.refused), (2, 2, 3));
        root.no_legacy();
    }

    #[test]
    fn overhaul_recovery_discovers_unprojected_runs_and_progress_is_never_retried() {
        let root = Root::new();
        let db = root.db();
        let mut wal = writer(&root);
        append(&db, &mut wal, &start(1, "r1", 1000).record);
        let lineage = wal.durable_publication().unwrap().lineage().to_string();
        let events = armed(db.clone(), 1000);
        assert_eq!(events.bus_event_open_sessions().unwrap(), [session()]);
        assert!(events.bus_event_checkpoint(&session()).unwrap().is_none());
        events
            .project_new_bus_events(&session(), &lineage, &[start(1, "r1", 1000)], 1000)
            .unwrap();
        append(&db, &mut wal, &finish(2, 2000).record);
        assert_eq!(
            events.bus_event_open_sessions().unwrap(),
            [session()],
            "the outbox checkpoint still has an open run"
        );
        events
            .project_new_bus_events(&session(), &lineage, &[finish(2, 2000)], 2000)
            .unwrap();
        assert!(events.bus_event_open_sessions().unwrap().is_empty());
        let event = RunEvent {
            v: 1,
            session_ref: broca_core::agent_session_ref(&session()),
            run_id: "r1".into(),
            seq: 2,
            at_ms: 2100,
            detail: broca_protocol::run_events::RunEventDetail::Progress {
                step: 1,
                last_content_at_ms: None,
                max_content_gap_ms: None,
            },
        };
        normal(&db);
        let progress = events
            .retain_bus_progress(&session(), &lineage, &event, 2100)
            .unwrap()
            .unwrap();
        full(&db);
        assert_eq!(
            events
                .get_bus_event(&progress.event_key)
                .unwrap()
                .unwrap()
                .1,
            progress.body
        );
        assert_eq!(
            events
                .bus_metrics()
                .bus_outbox_depth
                .load(Ordering::Relaxed),
            2
        );
        assert_ne!(
            events.pending_bus_event().unwrap().unwrap().event_key,
            progress.event_key
        );
        assert_no_content(&db);
        root.no_legacy();
    }

    #[test]
    fn overhaul_existing_operational_v0_fixture_opens_without_losing_authoritative_state() {
        let root = Root::new();
        std::fs::write(
            root.0.join("broca.db"),
            include_bytes!("../../tests/fixtures/operational-v0/broca.db"),
        )
        .unwrap();
        Connection::open(root.0.join("broca.db"))
            .unwrap()
            .execute(
                "INSERT INTO log_reader VALUES('preserved-reader',1,'config')",
                [],
            )
            .unwrap();
        let before = Connection::open(root.0.join("broca.db"))
            .unwrap()
            .query_row("SELECT count(*) FROM log_reader", [], |r| {
                r.get::<_, u64>(0)
            })
            .unwrap();
        let db = root.db();
        assert_eq!(scalar(&db, "SELECT count(*) FROM log_reader"), before);
        assert_eq!(
            scalar(&db, "SELECT count(*) FROM compaction_resolution_source"),
            0
        );
        assert_eq!(
            scalar(&db, "SELECT count(*) FROM compaction_view_receipt"),
            0
        );
        root.no_legacy();
    }

    #[test]
    fn overhaul_compaction_damaged_locator_falls_back_by_name_without_trusting_offsets() {
        for (sql, reason) in [
            (
                "DELETE FROM compaction_resolution_source WHERE seq=4",
                "compaction resolution locators incomplete",
            ),
            (
                "UPDATE compaction_resolution_source SET frame_identity=zeroblob(32) WHERE seq=4",
                "compaction resolution locator identity mismatch",
            ),
            (
                "UPDATE compaction_resolution_source SET frame_off=1 WHERE seq=4",
                "compaction resolution locator identity mismatch",
            ),
        ] {
            let root = Root::new();
            let db = root.db();
            let mut wal = writer(&root);
            append(&db, &mut wal, &start(1, "r1", 1000).record);
            let lineage = wal.durable_publication().unwrap().lineage().to_string();
            for record in &corpus(&lineage)[1..] {
                append(&db, &mut wal, record);
            }
            db.transaction(false, |tx| {
                tx.execute_batch(sql)?;
                Ok(())
            })
            .unwrap();
            let counter = access_counter::WalAccessCounter::default();
            let facts = counter.operation(reason).run(|| {
                db.compaction_resolution(&session(), wal.durable_publication().unwrap())
                    .unwrap()
            });
            assert_eq!((facts.applied, facts.noticed, facts.refused), (2, 2, 3));
            let expected = if reason == "compaction resolution locators incomplete" {
                vec![reason, "publication continuation rebuild"]
            } else {
                vec![reason]
            };
            assert_eq!(counter.snapshot()[0].seed_fallbacks, expected);
            assert_eq!(
                counter.snapshot()[0]
                    .reads
                    .iter()
                    .filter(|r| r.class == access_counter::ReadClass::Origin)
                    .count(),
                1
            );
        }
    }
}
