use super::{log_state, EvidenceError, SessionLogEvidence, SessionLogState};
use crate::db::{DbError, OverhaulDb};
use broca_protocol::BindIdentity;
use rusqlite::{params, OptionalExtension, Transaction};
use std::collections::BTreeSet;

impl OverhaulDb {
    pub(crate) fn session_log_state(
        &self,
        session: &BindIdentity,
        wal: &dyn broca_core::WalStore,
    ) -> Result<SessionLogState, String> {
        log_state(
            || {
                self.read_snapshot(|tx| evidence(tx, session))
                    .map_err(|error| Box::new(error) as EvidenceError)
            },
            session,
            wal,
        )
        .map_err(|error| error.to_string())
    }
}

fn evidence(tx: &Transaction<'_>, session: &BindIdentity) -> Result<SessionLogEvidence, DbError> {
    session.validate_keyable().map_err(DbError::Invalid)?;
    let sid = format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.display(),
        session.harness,
        session.session
    );
    let encoded =
        serde_json::to_string(session).map_err(|error| DbError::Invalid(error.to_string()))?;
    // A session row is an indexed WAL observation, unlike session_identity's
    // bind-only reservation. All message/episode/control projections reference
    // this parent with foreign keys, so it also covers non-run WAL content.
    let indexed: Option<Option<String>> = tx
        .query_row(
            "SELECT lineage_id FROM session WHERE sid=?1",
            [&sid],
            |row| row.get(0),
        )
        .optional()?;
    let has_runs = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM run WHERE sid=?1)",
        [&sid],
        |row| row.get::<_, bool>(0),
    )?;
    let mut lineages = BTreeSet::new();
    if let Some(Some(lineage)) = &indexed {
        lineages.insert(lineage.clone());
    }
    let mut retired = BTreeSet::new();
    let mut content = indexed.is_some() || has_runs;
    let mut progress =
        tx.prepare("SELECT lineage_id,expired_at_ms FROM retention_progress WHERE session=?1")?;
    let rows = progress.query_map([&encoded], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (lineage, expired) = row?;
        if expired.is_some_and(|at| at < 0) {
            return Err(DbError::Invalid(
                "negative retention expiry evidence".into(),
            ));
        }
        content = true;
        lineages.insert(lineage.clone());
        if expired.is_some() {
            retired.insert(lineage);
        }
    }
    for (query, key) in [
        (
            "SELECT lineage_id FROM condemned WHERE addr=?1",
            broca_core::session_addr(session),
        ),
        (
            "SELECT lineage_id FROM retention_deletion WHERE session=?1",
            encoded.clone(),
        ),
    ] {
        let mut query = tx.prepare(query)?;
        for lineage in query.query_map([key], |row| row.get::<_, String>(0))? {
            retired.insert(lineage?);
            content = true;
        }
    }
    let mut withdrawals =
        tx.prepare("SELECT DISTINCT lineage_id FROM retention_withdrawal WHERE session=?1")?;
    for lineage in withdrawals.query_map([&encoded], |row| row.get::<_, String>(0))? {
        lineages.insert(lineage?);
        content = true;
    }
    let imported = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM imported_run WHERE project_root=?1 AND harness=?2 AND session_key=?3)",
        params![session.project_root.to_string_lossy(), session.harness, session.session],
        |row| row.get::<_, bool>(0),
    )?;
    let floor = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM address_floor WHERE addr=?1)",
        [broca_core::session_addr(session)],
        |row| row.get::<_, bool>(0),
    )?;
    content |= imported || floor;
    // Indexed rows can outlive an expired lineage until the next publication
    // replaces them. They must not condemn a fresh subscriber's empty lineage.
    let indexed_retired = indexed
        .as_ref()
        .and_then(Option::as_ref)
        .is_some_and(|lineage| retired.contains(lineage));
    let has_run_history = (has_runs && !indexed_retired) || imported;
    lineages.retain(|lineage| !retired.contains(lineage));
    if content && !has_run_history && lineages.is_empty() && retired.is_empty() {
        return Err(DbError::Invalid(
            "session_log_evidence_unidentified: content evidence has no run or lineage proof"
                .into(),
        ));
    }
    // Old deletion evidence cannot excuse an unidentified indexed lineage or
    // imported runs whose lineage was never proven. Known current lineages must
    // each have their own retirement proof before removal wins.
    let unidentified = indexed.as_ref().is_some_and(Option::is_none)
        || (has_runs && indexed.is_none())
        || imported;
    Ok(SessionLogEvidence {
        content,
        has_runs: has_run_history,
        removed: !retired.is_empty() && !unidentified && lineages.is_empty(),
        lineages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionAddressResolver;
    use broca_core::FileWalStore;

    fn fixture(label: &str) -> (std::path::PathBuf, OverhaulDb, BindIdentity, FileWalStore) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "overhaul-log-evidence-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(broca_wal::state_format::MARKER_FILENAME),
            b"broca-state/2\n",
        )
        .unwrap();
        let db = OverhaulDb::open(&root).unwrap();
        let session = BindIdentity::new(root.join("project"), "broca", label);
        let wal = FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
        (root, db, session, wal)
    }

    #[test]
    fn overhaul_log_evidence_query_failure_is_not_available_or_unwritten() {
        let (root, db, session, wal) = fixture("unreadable");
        let conn = rusqlite::Connection::open(root.join("broca.db")).unwrap();
        conn.execute_batch("DROP TABLE run").unwrap();
        let error = SessionAddressResolver::session_log_state(&db, &session, &wal).unwrap_err();
        assert!(error.contains("run"), "{error}");
        assert!(!root.join("run-index.db").exists());
    }

    #[test]
    fn overhaul_log_evidence_unidentified_floor_is_an_error() {
        let (root, db, session, wal) = fixture("unidentified");
        let conn = rusqlite::Connection::open(root.join("broca.db")).unwrap();
        conn.execute(
            "INSERT INTO address_floor VALUES(?1,1,0)",
            [broca_core::session_addr(&session)],
        )
        .unwrap();
        let error = SessionAddressResolver::session_log_state(&db, &session, &wal).unwrap_err();
        assert!(
            error.contains("session_log_evidence_unidentified"),
            "{error}"
        );
    }

    #[test]
    fn overhaul_log_evidence_old_retirement_does_not_hide_a_current_lineage() {
        let (_, db, session, wal) = fixture("retirement");
        let mut row = crate::retention::RetentionLineageRow {
            lineage_id: "retired".into(),
            session: session.clone(),
            retention_s: 1,
            last_activity_ms: 1,
            expires_at_ms: None,
            wal_bytes: 69,
            expired_at_ms: Some(1001),
        };
        db.upsert_retention_lineage(&row, &[]).unwrap();
        assert_eq!(
            SessionAddressResolver::session_log_state(&db, &session, &wal).unwrap(),
            SessionLogState::Removed
        );
        row.lineage_id = "current".into();
        row.expires_at_ms = Some(2001);
        row.expired_at_ms = None;
        db.upsert_retention_lineage(&row, &[]).unwrap();
        assert_eq!(
            SessionAddressResolver::session_log_state(&db, &session, &wal).unwrap(),
            SessionLogState::Missing
        );
    }
}
