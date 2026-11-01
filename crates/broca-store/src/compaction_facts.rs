//! Durable facts for deciding whether a `view_ready` applies without reading the conversation.
use crate::{SqliteStore, StoreError};
use broca_core::{compaction::CompactionCheckpoint, MaterializedStore};
use broca_protocol::BindIdentity;
use broca_wal::{CompactionMessageKind, FramedRecord, WalByteAnchor, WalRangeRead, WalRecord};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

#[cfg(test)]
thread_local! {
    static AFTER_SEED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_after_seed_hook(hook: impl FnOnce() + 'static) {
    AFTER_SEED.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn after_seed() {
    let hook = AFTER_SEED.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS compaction_facts (
        session TEXT PRIMARY KEY, lineage_id TEXT NOT NULL, facts TEXT NOT NULL
    ) WITHOUT ROWID;
    CREATE TRIGGER IF NOT EXISTS compaction_facts_lineage_ended AFTER DELETE ON retention_deletion BEGIN
        DELETE FROM compaction_facts WHERE lineage_id=OLD.lineage_id;
    END;")
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionFacts {
    pub lineage_id: String,
    pub watermark: u64,
    pub setup: bool,
    pub applied: u64,
    /// The latest view acknowledged by a durable notice with its new request-token
    /// estimate. Receipt cleanup waits for this notice, not only the applied-view record.
    pub noticed: u64,
    pub refused: u64,
    pub file_id: Option<u64>,
    pub byte_end: Option<u64>,
}

/// Capture before reading WAL bytes. A later facts write must still see the same
/// session lineage and recovery epoch; a token from another connection cannot
/// bypass a pending recovery fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionProjectionEpoch {
    state: Option<(String, Option<String>, i64)>,
}
impl CompactionFacts {
    pub fn resolved(&self, version: u64) -> bool {
        self.noticed.max(self.refused) >= version
    }
    pub fn current_file(&self, identity: Option<u64>, bytes: Option<u64>) -> bool {
        identity.is_some() && identity == self.file_id && bytes.is_some() && bytes == self.byte_end
    }
}

pub(crate) fn is_compaction_record(record: &WalRecord) -> bool {
    matches!(
        record,
        WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionViewApplied { .. }
            | WalRecord::CompactionNotice { .. }
    )
}

fn load(conn: &Connection, key: &str) -> Result<Option<CompactionFacts>, StoreError> {
    let json: Option<String> = conn
        .query_row(
            "SELECT facts FROM compaction_facts WHERE session=?1",
            [key],
            |r| r.get(0),
        )
        .optional()?;
    json.map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
}
fn save(conn: &Connection, key: &str, facts: &CompactionFacts) -> Result<(), StoreError> {
    conn.execute("INSERT INTO compaction_facts VALUES(?1,?2,?3) ON CONFLICT(session) DO UPDATE SET lineage_id=excluded.lineage_id,facts=excluded.facts", params![key, facts.lineage_id, serde_json::to_string(facts)?])?;
    Ok(())
}

impl SqliteStore {
    pub fn compaction_projection_epoch(
        &self,
        session: &BindIdentity,
    ) -> Result<CompactionProjectionEpoch, StoreError> {
        if !self.wal_config.is_overhaul() {
            return Ok(CompactionProjectionEpoch { state: None });
        }
        let key = Self::key(session);
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let (_, lineage, epoch) = crate::current_projection_state(&tx, &key, true)?;
        if crate::wal_recovery::read_fence(&tx, &key)?.is_some_and(|fence| fence.pending) {
            return Err(StoreError::InconsistentSnapshot(
                "compaction facts unavailable during pending WAL recovery",
            ));
        }
        tx.commit()?;
        Ok(CompactionProjectionEpoch {
            state: Some((key, lineage, epoch)),
        })
    }

    fn compaction_epoch_matches(
        &self,
        tx: &Transaction<'_>,
        key: &str,
        captured: &CompactionProjectionEpoch,
    ) -> Result<bool, StoreError> {
        if !self.wal_config.is_overhaul() {
            return Ok(captured.state.is_none());
        }
        let Some((session, lineage, epoch)) = &captured.state else {
            return Ok(false);
        };
        if session != key {
            return Ok(false);
        }
        let (_, current_lineage, current_epoch) = crate::current_projection_state(tx, key, true)?;
        Ok(current_lineage == *lineage
            && current_epoch == *epoch
            && !crate::wal_recovery::read_fence(tx, key)?.is_some_and(|fence| fence.pending))
    }

    pub fn current_compaction_facts(
        &self,
        wal: &dyn broca_core::WalStore,
        session: &BindIdentity,
    ) -> Result<CompactionFacts, String> {
        if wal.wal_config().is_overhaul() {
            return Err(
                "legacy compaction-facts writer refuses an overhaul WAL destination".into(),
            );
        }
        let epoch = self
            .compaction_projection_epoch(session)
            .map_err(|e| e.to_string())?;
        let facts = self.compaction_facts(session).map_err(|e| e.to_string())?;
        let identity = wal.file_identity(session).map_err(|e| e.to_string())?;
        let bytes = wal.live_wal_bytes(session).map_err(|e| e.to_string())?;
        if let Some(facts) = facts.filter(|f| f.current_file(identity, bytes)) {
            if self
                .compaction_projection_epoch(session)
                .map_err(|e| e.to_string())?
                != epoch
            {
                return Err("compaction facts changed during recovery".into());
            }
            return Ok(facts);
        }
        // A bounded suffix keeps projection lag from turning receipt intake into a full read.
        let range = wal
            .read_records_tail_bounded(session, FACTS_TAIL_BYTES)
            .map_err(|e| e.to_string())?
            .ok_or("compaction facts projection is behind the bounded WAL suffix")?;
        let flushed = wal.flush_for_read(session).map_err(|e| e.to_string())?;
        if range
            .next_anchor
            .is_some_and(|a| Some(a.file_id) != flushed)
        {
            return Err("compaction WAL changed during its durability proof".into());
        }
        self.project_compaction_facts_at_epoch(session, &range, &epoch)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "compaction facts projection cannot cover the bounded WAL suffix".into())
    }

    /// Older databases can seed the new facts row from their existing admission checkpoint.
    /// An uncaptured compaction checkpoint is unknown, never evidence of absent Setup.
    pub fn compaction_facts(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        let captured = self.compaction_projection_epoch(session)?;
        let key = Self::key(session);
        let loaded = load(&self.conn.lock().unwrap(), &key)?;
        if let Some(facts) = loaded {
            return Ok((self.compaction_projection_epoch(session)? == captured).then_some(facts));
        }
        let snapshot = self.read_admission_snapshot(session).map_err(|_| {
            StoreError::InconsistentSnapshot("compaction admission checkpoint unreadable")
        })?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let Some(lineage_id) = snapshot.lineage_id else {
            return Ok(None);
        };
        let Some(compaction) = snapshot.checkpoint.and_then(|c| c.compaction) else {
            return Ok(None);
        };
        let mut facts = CompactionFacts {
            lineage_id,
            watermark: snapshot.watermark_seq,
            file_id: snapshot.anchor.map(|a| a.file_id),
            byte_end: snapshot.anchor.map(|a| a.byte_end),
            ..Default::default()
        };
        if let CompactionCheckpoint::State(state) = compaction {
            facts.setup = true;
            facts.applied = state.applied_view_version(&facts.lineage_id).unwrap_or(0);
            facts.noticed = state
                .announced
                .as_ref()
                .filter(|a| a.lineage_id.as_deref() == Some(facts.lineage_id.as_str()))
                .and_then(|a| a.applied_view_version)
                .unwrap_or(0);
        }
        if !facts.setup {
            return Ok((self.compaction_projection_epoch(session)? == captured).then_some(facts));
        }
        #[cfg(test)]
        after_seed();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(if self.wal_config.is_overhaul() {
            TransactionBehavior::Immediate
        } else {
            TransactionBehavior::Deferred
        })?;
        if !self.compaction_epoch_matches(&tx, &key, &captured)? {
            return Ok(None);
        }
        let facts = match load(&tx, &key)? {
            Some(current) => current,
            None => {
                save(&tx, &key, &facts)?;
                facts
            }
        };
        tx.commit()?;
        Ok(Some(facts))
    }

    /// Fold an already durability-proven range, independently of turn projection boundaries.
    pub fn project_compaction_facts(
        &self,
        session: &BindIdentity,
        range: &WalRangeRead,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        let Some(lineage) = range.lineage_id else {
            return Ok(None);
        };
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            return Ok(None);
        };
        self.project_compaction_facts_records(
            session,
            &lineage.to_string(),
            records,
            range.next_anchor,
            range.complete_byte_end,
        )
    }

    pub(crate) fn project_compaction_facts_records(
        &self,
        session: &BindIdentity,
        lineage: &str,
        records: &[FramedRecord],
        anchor: Option<WalByteAnchor>,
        complete_end: Option<u64>,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        self.project_compaction_facts_records_guarded(
            session,
            lineage,
            records,
            anchor,
            complete_end,
            None,
        )
    }

    pub fn project_compaction_facts_at_epoch(
        &self,
        session: &BindIdentity,
        range: &WalRangeRead,
        epoch: &CompactionProjectionEpoch,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        let Some(lineage) = range.lineage_id else {
            return Ok(None);
        };
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            return Ok(None);
        };
        self.project_compaction_facts_records_guarded(
            session,
            &lineage.to_string(),
            records,
            range.next_anchor,
            range.complete_byte_end,
            Some(epoch),
        )
    }

    fn project_compaction_facts_records_guarded(
        &self,
        session: &BindIdentity,
        lineage: &str,
        records: &[FramedRecord],
        anchor: Option<WalByteAnchor>,
        complete_end: Option<u64>,
        source_epoch: Option<&CompactionProjectionEpoch>,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        let captured = self.compaction_projection_epoch(session)?;
        if source_epoch.is_some_and(|epoch| *epoch != captured) {
            return Ok(None);
        }
        let seed = self.compaction_facts(session)?;
        #[cfg(test)]
        after_seed();
        let key = Self::key(session);
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(if self.wal_config.is_overhaul() {
            TransactionBehavior::Immediate
        } else {
            TransactionBehavior::Deferred
        })?;
        if !self.compaction_epoch_matches(&tx, &key, &captured)? {
            return Ok(None);
        }
        if let Some((_, Some(expected_lineage), _)) = &captured.state {
            if expected_lineage != lineage {
                return Ok(None);
            }
        }
        if self.wal_config.is_overhaul() && source_epoch.is_none() {
            // Without a token captured before the WAL read, only a source carrying
            // the complete recovery history can prove it is not a pre-cut range.
            let required = crate::wal_recovery::required_cut_ids_in_tx(&tx, &key)?;
            if required.iter().any(|id| {
                !records.iter().any(|frame| {
                    matches!(&frame.record,
                WalRecord::RecoveryCut { recovery } if &recovery.cut_id == id)
                })
            }) {
                return Ok(None);
            }
        }
        let held = load(&tx, &key)?.or(seed);
        let mut facts =
            held.filter(|f| f.lineage_id == lineage)
                .unwrap_or_else(|| CompactionFacts {
                    lineage_id: lineage.into(),
                    ..Default::default()
                });
        // Setup occurs once per lineage and precedes every asynchronous view. A suffix
        // containing Setup can initialize facts without the older conversation.
        if !facts.setup {
            if let Some(setup) = records.iter().find(|r| {
                r.seq > facts.watermark
                    && matches!(
                        r.record,
                        WalRecord::CompactionMessage {
                            kind: CompactionMessageKind::Setup,
                            ..
                        }
                    )
            }) {
                facts.watermark = setup.seq.saturating_sub(1);
            }
        }
        if records.last().is_some_and(|r| r.seq < facts.watermark) {
            return Ok(None);
        }
        let newer: Vec<_> = records.iter().filter(|r| r.seq > facts.watermark).collect();
        if newer
            .first()
            .is_some_and(|r| r.seq != facts.watermark.saturating_add(1))
            || newer
                .windows(2)
                .any(|r| r[0].seq.checked_add(1) != Some(r[1].seq))
        {
            return Ok(None);
        }
        let previous = facts.clone();
        for frame in newer {
            match &frame.record {
                WalRecord::CompactionMessage {
                    kind: CompactionMessageKind::Setup,
                    ..
                } => facts.setup = true,
                WalRecord::CompactionViewApplied { view_version, .. } => {
                    facts.applied = facts.applied.max(*view_version)
                }
                WalRecord::CompactionNotice { headers, .. } => {
                    if headers.get("lineage_id").map(String::as_str) != Some(lineage) {
                        // The outbox preserves original notice headers across projection epochs.
                        // A header lineage_id different from the supplied range identity cannot
                        // prove compaction facts for the current lineage.
                        return Ok(None);
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
            facts.watermark = frame.seq;
        }
        if let Some(anchor) = anchor.filter(|a| a.seq == facts.watermark) {
            facts.file_id = Some(anchor.file_id);
            facts.byte_end = complete_end;
        } else if facts.watermark != previous.watermark {
            facts.file_id = None;
            facts.byte_end = None;
        }
        if facts.setup && facts != previous {
            save(&tx, &key, &facts)?;
        }
        tx.commit()?;
        Ok(Some(facts))
    }
}

pub const FACTS_TAIL_BYTES: u64 = 256 * 1024;

#[cfg(test)]
mod tests {
    #[test]
    fn legacy_compaction_writer_refuses_overhaul_before_wal_access() {
        let store = crate::SqliteStore::open_in_memory().unwrap();
        let wal = broca_core::FileWalStore::with_config(
            "unused-overhaul-root/wal",
            broca_wal::WalConfig::OVERHAUL,
        );
        let error = store
            .current_compaction_facts(&wal, &broca_protocol::BindIdentity::new("/", "test", "s"))
            .unwrap_err();
        assert_eq!(
            error,
            "legacy compaction-facts writer refuses an overhaul WAL destination"
        );
    }

    use super::*;
    use broca_core::compaction_provider::{
        lineage::MessageRef,
        notice::{AdvanceNotice, Event},
        view::{CompactionView, ViewRange},
    };

    fn frame(seq: u64, record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record,
        }
    }
    fn setup() -> WalRecord {
        WalRecord::CompactionMessage {
            provider: "magic-context".into(),
            kind: CompactionMessageKind::Setup,
            request_id: None,
            cursor: None,
            compaction_id: "head".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 0 },
            replacement: std::sync::Arc::new(vec![]),
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

    #[test]
    fn durable_bus_projection_keeps_compaction_outcomes_and_rejects_gaps() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/test", "runner", "facts");
        let reference = broca_core::agent_session_ref(&session);
        let view = CompactionView::new(
            &reference,
            2,
            "lineage",
            MessageRef {
                ordinal: 0,
                mid: "m0".into(),
            },
            vec![ViewRange::replace(0, 1, "[]")],
        );
        let records = vec![
            frame(1, setup()),
            frame(
                2,
                WalRecord::CompactionViewApplied {
                    provider: "magic-context".into(),
                    view_version: 2,
                    view: broca_wal::OpaqueJson::from_json_text(
                        serde_json::to_string(&view).unwrap(),
                    )
                    .unwrap(),
                    choice: broca_wal::CompactionViewChoice::Applied,
                },
            ),
        ];
        store
            .project_bus_events(&session, "lineage", &records, 1)
            .unwrap();
        let facts = store.compaction_facts(&session).unwrap().unwrap();
        assert!(facts.setup);
        assert_eq!(facts.applied, 2);
        assert!(
            !facts.resolved(2),
            "view record without its updated notice resolved too early"
        );
        let mut notice = AdvanceNotice::new(reference, 1, "lineage", 0, "a".repeat(64), 10, "m");
        notice.applied_view_version = Some(2);
        notice.events.push(Event::ViewRefused {
            view_version: 3,
            reason: "provider_constraint".into(),
        });
        store
            .project_bus_events(
                &session,
                "lineage",
                &[frame(
                    3,
                    WalRecord::CompactionNotice {
                        seq: 1,
                        headers: notice.to_headers(),
                    },
                )],
                2,
            )
            .unwrap();
        let facts = store.compaction_facts(&session).unwrap().unwrap();
        assert_eq!(
            (facts.watermark, facts.applied, facts.noticed, facts.refused),
            (3, 2, 2, 3)
        );
        assert!(facts.resolved(2) && facts.resolved(3) && !facts.resolved(4));
        store
            .project_bus_events(&session, "lineage", &records, 3)
            .unwrap();
        assert_eq!(store.compaction_facts(&session).unwrap().unwrap(), facts);
        store
            .project_bus_events(&session, "next-lineage", &[frame(1, setup())], 4)
            .unwrap();
        assert_eq!(
            store.compaction_facts(&session).unwrap().unwrap().refused,
            0
        );
        store
            .project_bus_events(&session, "next-lineage", &[frame(3, setup())], 5)
            .unwrap();
        assert_eq!(
            store.compaction_facts(&session).unwrap().unwrap().watermark,
            1,
            "gap advanced the facts watermark"
        );
        store
            .retention_deletion_pending("lineage", &session, true)
            .unwrap();
        store
            .retention_deletion_pending("lineage", &session, false)
            .unwrap();
        assert!(store.compaction_facts(&session).unwrap().is_some());
        store
            .retention_deletion_pending("next-lineage", &session, true)
            .unwrap();
        store
            .retention_deletion_pending("next-lineage", &session, false)
            .unwrap();
        assert!(store.compaction_facts(&session).unwrap().is_none());
        store
            .project_bus_events(&session, "bootstrap-lineage", &[frame(200, setup())], 6)
            .unwrap();
        let facts = store.compaction_facts(&session).unwrap().unwrap();
        assert!(facts.setup);
        assert_eq!(
            facts.watermark, 200,
            "Setup in a bounded suffix did not initialize facts"
        );
        store
            .project_bus_events(
                &session,
                "bootstrap-lineage",
                &[frame(
                    201,
                    WalRecord::CompactionNotice {
                        seq: 1,
                        headers: notice.to_headers(),
                    },
                )],
                7,
            )
            .unwrap();
        assert_eq!(
            store.compaction_facts(&session).unwrap().unwrap(),
            facts,
            "a different lineage's notice advanced the facts projection"
        );
    }
}
