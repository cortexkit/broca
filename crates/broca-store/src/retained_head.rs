//! Retained spend and grants have their own checkpoint sequence. Advancing it must not
//! advance the message store's watermark, which would hide unmaterialized WAL messages.

use crate::{SqliteStore, StoreError};
use broca_core::head::{HeadProjection, RetainedHeadCheckpoint};
use broca_protocol::BindIdentity;
use broca_wal::FramedRecord;
use rusqlite::{params, OptionalExtension};

pub(crate) fn init(conn: &rusqlite::Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS retained_head (
        lineage_id TEXT PRIMARY KEY, session TEXT NOT NULL, at_seq INTEGER NOT NULL,
        facts_json TEXT NOT NULL
    );",
    )?;
    Ok(())
}

impl SqliteStore {
    pub(crate) fn checkpoint_retained_head_inner(
        &self,
        session: &BindIdentity,
        checkpoint: &RetainedHeadCheckpoint,
    ) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        write(
            &tx,
            &Self::key(session),
            &checkpoint.lineage_id,
            checkpoint.at_seq,
            &checkpoint.facts,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn read_retained_head_inner(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
    ) -> Result<Option<RetainedHeadCheckpoint>, StoreError> {
        let conn = self.conn.lock().unwrap();
        read(&conn, &Self::key(session), lineage_id)
    }

    pub(crate) fn project_retained_head_inner(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
        records: &[FramedRecord],
    ) -> Result<(), StoreError> {
        let Some(newest) = records.last().map(|frame| frame.seq) else {
            return Ok(());
        };
        let key = Self::key(session);
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let expired: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM retention_lineage WHERE lineage_id=?1 AND expired_at_ms IS NOT NULL)",
            [lineage_id], |row| row.get(0),
        )?;
        if expired {
            return Ok(());
        }
        // Without a usable checkpoint, rebuild only from the lineage's origin. A suffix alone
        // cannot prove the earlier spend, so it must return an inconsistency error.
        let prior = read(&tx, &key, lineage_id).ok().flatten();
        if prior.as_ref().is_some_and(|prior| prior.at_seq > newest) {
            return Ok(());
        }
        let usable = prior.filter(|prior| {
            records
                .first()
                .is_some_and(|first| first.seq <= prior.at_seq.saturating_add(1))
        });
        if usable.is_none() && !broca_core::retention::starts_at_lineage_origin(records) {
            return Err(StoreError::InconsistentSnapshot(
                "retained head requires a complete WAL",
            ));
        }
        let after = usable.as_ref().map(|prior| prior.at_seq);
        let mut facts = usable.map_or_else(HeadProjection::default, |prior| prior.facts);
        for frame in records
            .iter()
            .filter(|frame| after.is_none_or(|after| frame.seq > after))
        {
            facts.observe(&frame.record);
        }
        write(&tx, &key, lineage_id, newest, &facts)?;
        tx.commit()?;
        Ok(())
    }
}

fn read(
    conn: &rusqlite::Connection,
    session: &str,
    lineage_id: &str,
) -> Result<Option<RetainedHeadCheckpoint>, StoreError> {
    let row: Option<(u64, String)> = conn
        .query_row(
            "SELECT at_seq,facts_json FROM retained_head WHERE lineage_id=?1 AND session=?2",
            params![lineage_id, session],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(at_seq, json)| {
        let facts: HeadProjection = serde_json::from_str(&json)?;
        if facts
            .usage
            .as_ref()
            .is_some_and(|usage| !usage.consistent())
        {
            return Err(StoreError::InconsistentSnapshot(
                "retained usage is inconsistent",
            ));
        }
        Ok(RetainedHeadCheckpoint {
            lineage_id: lineage_id.into(),
            at_seq,
            facts,
        })
    })
    .transpose()
}

fn write(
    tx: &rusqlite::Transaction<'_>,
    session: &str,
    lineage_id: &str,
    at_seq: u64,
    facts: &HeadProjection,
) -> Result<(), StoreError> {
    let expired: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM retention_lineage WHERE lineage_id=?1 AND expired_at_ms IS NOT NULL)",
        [lineage_id], |row| row.get(0))?;
    if expired {
        return Ok(());
    }
    let prior: Option<(String, i64)> = tx
        .query_row(
            "SELECT session,at_seq FROM retained_head WHERE lineage_id=?1",
            [lineage_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((prior_session, prior_seq)) = prior {
        if prior_session != session {
            return Err(StoreError::InconsistentSnapshot(
                "retained head belongs to another session",
            ));
        }
        if prior_seq > broca_core::projection_plan::u64_to_i64(at_seq)? {
            return Ok(());
        }
    }
    tx.execute("INSERT INTO retained_head(lineage_id,session,at_seq,facts_json) VALUES(?1,?2,?3,?4)
        ON CONFLICT(lineage_id) DO UPDATE SET at_seq=excluded.at_seq,facts_json=excluded.facts_json",
        params![lineage_id,session,broca_core::projection_plan::u64_to_i64(at_seq)?,serde_json::to_string(facts)?])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{base_records, session, text_turn};

    fn records(session: &BindIdentity) -> Vec<FramedRecord> {
        let mut records = base_records(session);
        if let broca_wal::WalRecord::RunStarted {
            retention,
            ts_ms,
            title,
            input,
            principal,
            manifest,
            ..
        } = &mut records[0].record
        {
            *retention = Some(3600);
            *ts_ms = Some(1);
            *title = Some("SECRET_TITLE".into());
            *input = std::sync::Arc::new(vec![broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "SECRET_INPUT".into(),
                }],
            )]);
            *principal = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "owner".into(),
            });
            *manifest = Some(Box::new(broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(serde_json::json!({"composition":{"secret":"SECRET_COMPOSITION"},
                    "senders":[{"module":"sender","templates":[{"id":"t","text":"SECRET_TEMPLATE","slots":{}}]}]}).to_string()).unwrap(),
                plan_identity: "id".into(), composition_digest:"digest".into(), tools: Default::default(),
                system_text:"SECRET_SYSTEM".into(), system_digest:"digest".into(), join_version:1,
                role_versions:Default::default(), fetches:vec![], declarations:vec![],
            }));
        }
        records.extend(text_turn(2, 1, "SECRET_ASSISTANT"));
        records
    }

    #[test]
    fn retained_checkpoint_is_content_free_lineage_bound_and_deleted_at_expiry() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = session("retained-head");
        let records = records(&session);
        store
            .project_retained_head_inner(&session, "old", &records)
            .unwrap();
        let old = store
            .read_retained_head_inner(&session, "old")
            .unwrap()
            .unwrap();
        assert!(old
            .facts
            .sender(
                Some(&broca_protocol::BindPrincipal::Reserved {
                    module_id: "sender".into()
                }),
                &broca_protocol::SendScope::default(),
                &[broca_protocol::DeliveryMode::Steer]
            )
            .is_some());
        assert_eq!(old.facts.usage.as_ref().unwrap().totals.steps, 1);
        assert!(store
            .read_retained_head_inner(&session, "new")
            .unwrap()
            .is_none());
        {
            let conn = store.conn.lock().unwrap();
            let json: String = conn
                .query_row(
                    "SELECT facts_json FROM retained_head WHERE lineage_id='old'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(!json.contains("SECRET"));
            for table in ["message", "meta"] {
                let count: i64 = conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(
                    count, 0,
                    "retained head must not advance transcript projection"
                );
            }
        }
        store
            .project_retained_head_inner(&session, "new", &records)
            .unwrap();
        let new = store
            .read_retained_head_inner(&session, "new")
            .unwrap()
            .unwrap();
        assert_eq!(new.facts.usage.as_ref().unwrap().totals.steps, 1);
        let expired = |id: &str| crate::retention::RetentionLineageRow {
            lineage_id: id.into(),
            session: session.clone(),
            retention_s: 3600,
            last_activity_ms: 1,
            expires_at_ms: None,
            wal_bytes: 1,
            expired_at_ms: Some(3600001),
        };
        store
            .upsert_retention_lineage_with_withdrawals(&expired("old"), &[], &[])
            .unwrap();
        assert!(store
            .read_retained_head_inner(&session, "old")
            .unwrap()
            .is_none());
        assert!(store
            .read_retained_head_inner(&session, "new")
            .unwrap()
            .is_some());
        store
            .checkpoint_retained_head_inner(&session, &old)
            .unwrap();
        assert!(
            store
                .read_retained_head_inner(&session, "old")
                .unwrap()
                .is_none(),
            "late head writes cannot resurrect expired metadata"
        );
        store
            .upsert_retention_lineage_with_withdrawals(&expired("new"), &[], &[])
            .unwrap();
        assert!(store
            .read_retained_head_inner(&session, "new")
            .unwrap()
            .is_none());
    }

    #[test]
    fn corrupt_retained_checkpoint_requires_full_reconstruction() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = session("retained-head-recovery");
        let records = records(&session);
        store
            .project_retained_head_inner(&session, "lineage", &records)
            .unwrap();
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE retained_head SET facts_json='broken' WHERE lineage_id='lineage'",
                [],
            )
            .unwrap();
        assert!(store.read_retained_head_inner(&session, "lineage").is_err());
        assert!(store
            .project_retained_head_inner(&session, "lineage", &records[1..])
            .is_err());
        store
            .project_retained_head_inner(&session, "lineage", &records)
            .unwrap();
        let recovered = store
            .read_retained_head_inner(&session, "lineage")
            .unwrap()
            .unwrap();
        assert_eq!(recovered.facts.usage.as_ref().unwrap().totals.steps, 1);
        assert_eq!(
            recovered.facts.usage.as_ref().unwrap().totals.input_tokens,
            Some(10)
        );
    }
}
