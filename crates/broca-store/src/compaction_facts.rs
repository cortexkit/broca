//! Durable facts for deciding whether a `view_ready` applies without reading the conversation.
use crate::{SqliteStore, StoreError};
use broca_core::{compaction::CompactionCheckpoint, MaterializedStore};
use broca_protocol::BindIdentity;
use broca_wal::{CompactionMessageKind, FramedRecord, WalByteAnchor, WalRangeRead, WalRecord};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

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
    /// Older databases can seed the new facts row from their existing admission checkpoint.
    /// An uncaptured compaction checkpoint is unknown, never evidence of absent Setup.
    pub fn compaction_facts(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<CompactionFacts>, StoreError> {
        let key = Self::key(session);
        if let Some(facts) = load(&self.conn.lock().unwrap(), &key)? {
            return Ok(Some(facts));
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
            return Ok(Some(facts));
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
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
        let seed = self.compaction_facts(session)?;
        let key = Self::key(session);
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
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

#[cfg(test)]
mod tests {
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
