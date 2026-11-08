//! Identity discovery and move exclusions, independent of transcript projections.
use super::{DbError, OverhaulDb};
use broca_core::{
    agent_session_ref, session_addr, ExportError, ImportedRunExclusions, RecoveryDiscovery,
    SessionInventory, SessionRefIndex, SessionRefResolver, SessionRefs,
};
use broca_protocol::{BindIdentity, RunId};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::path::Path;

pub(super) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS session_identity (
            address TEXT PRIMARY KEY, project_root TEXT NOT NULL, harness TEXT NOT NULL,
            session_key TEXT NOT NULL, legacy_ref TEXT NOT NULL UNIQUE, session_ref TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS session_identity_name ON session_identity(session_key);
         CREATE INDEX IF NOT EXISTS session_identity_ref ON session_identity(session_ref);
         CREATE TABLE IF NOT EXISTS inventory_status (
            id INTEGER PRIMARY KEY CHECK(id=1), complete INTEGER NOT NULL CHECK(complete IN (0,1))
         );
         INSERT OR IGNORE INTO inventory_status VALUES(1,0);
         CREATE TABLE IF NOT EXISTS imported_run (
            project_root TEXT NOT NULL, harness TEXT NOT NULL, session_key TEXT NOT NULL,
            run_id TEXT NOT NULL, cut_id TEXT NOT NULL,
            PRIMARY KEY(project_root,harness,session_key,run_id)
         );",
    )?;
    conn.execute_batch(crate::session_address::SCOPE_RESERVATION_SCHEMA)
}

pub(super) fn insert(
    conn: &Connection,
    address: &str,
    session: &BindIdentity,
) -> Result<(), DbError> {
    if session_addr(session) != address {
        // A moved WAL retains historical bind triples, which are not aliases
        // for the receiver's address.
        return Ok(());
    }
    let changed = conn.execute(
        "INSERT INTO session_identity VALUES(?1,?2,?3,?4,?5,?6)
         ON CONFLICT(address) DO UPDATE SET address=excluded.address
         WHERE project_root=excluded.project_root AND harness=excluded.harness
             AND session_key=excluded.session_key",
        params![
            address,
            session.project_root.to_string_lossy(),
            session.harness,
            session.session,
            broca_core::wal_writer::legacy_session_ref(
                &session.harness,
                &session.project_root.to_string_lossy(),
                &session.session
            ),
            agent_session_ref(session)
        ],
    )?;
    if changed != 1 {
        return Err(DbError::Invalid(
            "conflicting session identities at one WAL address".into(),
        ));
    }
    Ok(())
}

impl SessionRefIndex for OverhaulDb {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let deadline = std::time::Instant::now() + crate::STORE_BUSY_TIMEOUT;
        let mut conn = loop {
            match self.conn.try_lock() {
                Ok(conn) => break conn,
                Err(std::sync::TryLockError::Poisoned(error)) => return Err(error.to_string()),
                Err(std::sync::TryLockError::WouldBlock)
                    if std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
                Err(_) => return Err("session-ref inventory connection wait expired".into()),
            }
        };
        conn.busy_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .map_err(|e| e.to_string())?;
        let result = (|| {
            conn.pragma_update(None, "synchronous", "FULL")?;
            let tx = conn.transaction()?;
            insert(&tx, address, session)?;
            tx.commit()?;
            Ok::<_, DbError>(())
        })();
        conn.busy_timeout(crate::STORE_BUSY_TIMEOUT)
            .map_err(|e| e.to_string())?;
        result.map_err(|e| e.to_string())
    }
}

impl OverhaulDb {
    fn inventory_snapshot(
        &self,
        select: impl FnOnce(&Connection) -> Result<BTreeMap<String, (String, BindIdentity)>, DbError>,
    ) -> Result<SessionRefs, DbError> {
        let gap = || broca_core::session_projection_incomplete(&self.root);
        let before = gap()?;
        let mut found = self.read_snapshot(|tx| {
            let complete = tx.query_row(
                "SELECT complete FROM inventory_status WHERE id=1",
                [],
                |r| r.get(0),
            )?;
            Ok(SessionRefs {
                sessions: select(tx)?,
                complete,
            })
        })?;
        found.complete &= !before && !gap()?;
        Ok(found)
    }

    /// Call only while boot excludes WAL replacement. The durable WAL publisher
    /// and boot reconciler must finish updating session rows before this census
    /// so indexed identities cover re-keyed and zero-run sessions.
    pub fn reconcile_inventory(&self) -> Result<(), DbError> {
        self.authoritative_transaction(|tx| {
            tx.execute("UPDATE inventory_status SET complete=0 WHERE id=1", [])?;
            let identities = tx
                .prepare("SELECT sid,project_root,harness,session_key FROM session")?
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        BindIdentity::new(
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                        ),
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (_, session) in identities {
                insert(tx, &session_addr(&session), &session)?;
            }
            Ok(())
        })?;
        let repair = broca_core::ProjectionRepair::begin(&self.root)?;
        let reader = broca_core::ReadOnlyWalReader::new(&self.root);
        let mut complete = true;
        for address in reader
            .addresses()
            .map_err(|e| DbError::Invalid(e.to_string()))?
        {
            let represented = self.read_snapshot(|tx| {
                Ok(tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM session_identity WHERE address=?1)",
                    [&address],
                    |r| r.get::<_, bool>(0),
                )?)
            })?;
            if represented {
                if reader.probe(&address).is_err() {
                    complete = false;
                }
                continue;
            }
            let (records, _) = match reader.records(&address) {
                Ok(read) => read,
                Err(_) => {
                    complete = false;
                    continue;
                }
            };
            let identities: Vec<_> = records
                .iter()
                .filter_map(|frame| match &frame.record {
                    broca_wal::WalRecord::RunStarted { session, .. }
                        if session_addr(session) == address =>
                    {
                        Some(session)
                    }
                    _ => None,
                })
                .collect();
            if identities.is_empty() {
                complete = false;
                continue;
            }
            self.authoritative_transaction(|tx| {
                for identity in identities {
                    insert(tx, &address, identity)?;
                }
                Ok(())
            })?;
        }
        self.authoritative_transaction(|tx| {
            tx.execute(
                "UPDATE inventory_status SET complete=?1 WHERE id=1",
                [complete],
            )?;
            Ok(())
        })?;
        if let Some(repair) = repair.filter(|_| complete) {
            repair.finish()?;
        }
        Ok(())
    }
}

impl SessionRefResolver for OverhaulDb {
    fn resolve_refs(&self, refs: &[&str]) -> Result<SessionRefs, String> {
        self.inventory_snapshot(|tx| {
            let mut sessions = BTreeMap::new();
            let mut stmt = tx.prepare("SELECT address,project_root,harness,session_key FROM session_identity WHERE legacy_ref=?1 OR session_ref=?1")?;
            for reference in refs {
                let entries = stmt.query_map([reference], |r| Ok((r.get::<_, String>(0)?, BindIdentity::new(
                    r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?,
                ))))?.collect::<rusqlite::Result<Vec<_>>>()?;
                if entries.len() > 1 { return Err(DbError::Invalid("session_ref_ambiguous".into())); }
                if let Some(entry) = entries.into_iter().next() { sessions.insert((*reference).into(), entry); }
            }
            Ok(sessions)
        }).map_err(|e| e.to_string())
    }

    fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String> {
        self.inventory_snapshot(|tx| {
            let mut stmt = tx.prepare("SELECT legacy_ref,address,project_root,harness,session_key FROM session_identity WHERE session_key=?1 ORDER BY legacy_ref LIMIT ?2")?;
            let rows = stmt.query_map(params![name, limit], |r| Ok((r.get(0)?, (r.get(1)?, BindIdentity::new(
                r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?,
            )))))?.collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
            Ok(rows)
        }).map_err(|e| e.to_string())
    }
}

impl SessionInventory for OverhaulDb {
    fn prepare_inventory(&self, _root: &Path) -> Result<(), String> {
        self.reconcile_inventory()
            .map_err(|e| format!("reconcile overhaul session inventory: {e}"))
    }
    fn rebuild_inventory(&self, _root: &Path) -> Result<(), String> {
        // Repair must not hold the serving connection's mutex while rebuilding
        // the session inventory. WAL-mode readers can resolve known identities.
        Self::open(&self.root)
            .and_then(|repair| repair.reconcile_inventory())
            .map_err(|e| e.to_string())
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        OverhaulDb::retained_sessions(self).map_err(|e| e.to_string())
    }
    fn forget_projection(&self, session: &BindIdentity) -> Result<(), String> {
        self.forget_session(session).map_err(|e| e.to_string())
    }
}

impl ImportedRunExclusions for OverhaulDb {
    fn record_imported_runs(
        &self,
        session: &BindIdentity,
        cut_id: &str,
        runs: &[RunId],
    ) -> Result<(), ExportError> {
        self.authoritative_transaction(|tx| {
            // Persist exclusions before the staged WAL can be served or billed.
            // The cut coordinator's pending intent fences interrupted installs;
            // this SQL commit does not contain the rename or cut completion.
            insert(tx, &session_addr(session), session)?;
            for run in runs {
                tx.execute(
                    "INSERT OR IGNORE INTO imported_run VALUES(?1,?2,?3,?4,?5)",
                    params![
                        session.project_root.to_string_lossy(),
                        session.harness,
                        session.session,
                        run.0,
                        cut_id,
                    ],
                )?;
            }
            Ok(())
        })
        .map_err(|e| ExportError::Storage(e.to_string()))
    }
    fn is_imported_run(&self, session: &BindIdentity, run: &RunId) -> Result<bool, ExportError> {
        self.read_snapshot(|tx| Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM imported_run WHERE project_root=?1 AND harness=?2 AND session_key=?3 AND run_id=?4)",
            params![session.project_root.to_string_lossy(), session.harness, session.session, run.0], |r| r.get(0),
        )?)).map_err(|e| ExportError::Storage(e.to_string()))
    }
}

impl SessionRefResolver for crate::SqliteStore {
    fn resolve_refs(&self, refs: &[&str]) -> Result<SessionRefs, String> {
        self.session_refs(refs.iter().copied())
            .map_err(|e| e.to_string())
    }
    fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String> {
        self.session_refs_named(name, limit)
            .map_err(|e| e.to_string())
    }
}

impl SessionInventory for crate::SqliteStore {
    fn prepare_inventory(&self, root: &Path) -> Result<(), String> {
        crate::session_refs::cleanup_legacy_inventory(root)
            .map(|_| ())
            .map_err(|e| format!("cleanup_legacy_inventory: {e}"))
    }
    fn rebuild_inventory(&self, root: &Path) -> Result<(), String> {
        crate::session_refs::migrate(
            &root.join("run-index.db"),
            &broca_core::ReadOnlyWalReader::new(root),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        crate::SqliteStore::retained_sessions(self).map_err(|e| e.to_string())
    }
    fn forget_projection(&self, session: &BindIdentity) -> Result<(), String> {
        self.drop_session_projection(session)
    }
}

impl RecoveryDiscovery for crate::SqliteStore {
    fn withdrawal_candidates(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        broca_core::RunIndex::runs_with_withdrawals_owed(self)
    }
    fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        crate::SqliteStore::active_runs(self).map_err(|e| e.to_string())
    }
    fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        crate::SqliteStore::restart_resume_candidates(self).map_err(|e| e.to_string())
    }
}
impl RecoveryDiscovery for OverhaulDb {
    fn withdrawal_candidates(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        self.runs_with_withdrawals_owed()
    }
    fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        OverhaulDb::active_runs(self).map_err(|e| e.to_string())
    }
    fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        OverhaulDb::restart_resume_candidates(self).map_err(|e| e.to_string())
    }
}

/// A malformed marker is never treated as a legacy root. Read-only tools do not
/// initialize either format, including when the selected database is absent.
pub fn open_session_resolver(root: &Path) -> Result<Box<dyn SessionRefResolver>, String> {
    match std::fs::read(root.join("state-format")) {
        Ok(bytes) if bytes == b"broca-state/2" || bytes == b"broca-state/2\n" => {
            OverhaulDb::open_read_only(root)
                .map(|db| Box::new(db) as Box<dyn SessionRefResolver>)
                .map_err(|e| e.to_string())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            crate::SqliteStore::open_read_only(root.join("run-index.db"))
                .map(|db| Box::new(db) as Box<dyn SessionRefResolver>)
                .map_err(|e| e.to_string())
        }
        Ok(_) => Err("unsupported state-format marker for session-ref inventory".into()),
        Err(e) => Err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) struct Scratch(pub std::path::PathBuf);
    impl Scratch {
        pub(super) fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "operational-ports-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(root.join("wal")).unwrap();
            Self(root)
        }
        pub(super) fn overhaul(&self) -> OverhaulDb {
            std::fs::write(self.0.join("state-format"), b"broca-state/2\n").unwrap();
            OverhaulDb::open(&self.0).unwrap()
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn operational_inventory_ports_preserve_candidates_completeness_aliases_and_projection_forget()
    {
        for overhaul in [false, true] {
            let root = Scratch::new();
            let index: Box<dyn SessionInventory> = if overhaul {
                Box::new(root.overhaul())
            } else {
                Box::new(crate::SqliteStore::open(root.0.join("run-index.db")).unwrap())
            };
            let session = BindIdentity::new("/project", "runner", "name");
            let address = session_addr(&session);
            assert!(!index.resolve_refs(&[]).unwrap().complete);
            index.record_session(&address, &session).unwrap();
            assert!(!index.named_refs("name", 101).unwrap().complete);
            index.prepare_inventory(&root.0).unwrap();
            index.rebuild_inventory(&root.0).unwrap();
            assert!(index.named_refs("name", 101).unwrap().complete);
            let reference = agent_session_ref(&session);
            assert_eq!(
                index.resolve_refs(&[&reference]).unwrap().sessions[&reference].1,
                session
            );
            let old = broca_core::wal_writer::legacy_session_ref("runner", "/project", "name");
            assert_eq!(
                index.resolve_refs(&[&old]).unwrap().sessions[&old].1,
                session
            );
            let historical = BindIdentity::new("/old-project", "runner", "old-name");
            index.record_session(&address, &historical).unwrap();
            assert!(index
                .named_refs("old-name", 101)
                .unwrap()
                .sessions
                .is_empty());
            index.forget_projection(&session).unwrap();
            assert_eq!(index.named_refs("name", 101).unwrap().sessions.len(), 1);
            std::fs::write(root.0.join(broca_core::SESSION_PROJECTION_INCOMPLETE), b"").unwrap();
            assert!(!index.named_refs("name", 101).unwrap().complete);
            index.rebuild_inventory(&root.0).unwrap();
            assert!(index.named_refs("name", 101).unwrap().complete);
            let other = BindIdentity::new("/other", "runner", "name");
            index.record_session(&session_addr(&other), &other).unwrap();
            assert!(index.resolve_refs(&[&reference]).is_err());
            assert_eq!(index.named_refs("name", 1).unwrap().sessions.len(), 1);
        }
    }

    #[test]
    fn operational_overhaul_inventory_does_not_certify_an_unknown_wal() {
        let root = Scratch::new();
        let db = root.overhaul();
        let path = root.0.join("wal/0123456789abcdef.wal");
        std::fs::write(&path, b"").unwrap();
        db.reconcile_inventory().unwrap();
        assert!(!db.resolve_refs(&[]).unwrap().complete);
        drop(db);
        let db = OverhaulDb::open(&root.0).unwrap();
        assert!(!db.named_refs("unknown", 101).unwrap().complete);
        std::fs::remove_file(path).unwrap();
        db.reconcile_inventory().unwrap();
        assert!(db.resolve_refs(&[]).unwrap().complete);
        assert!(!root.0.join("run-index.db").exists());
    }

    #[test]
    fn operational_overhaul_move_identity_and_imported_exclusions_commit_or_rollback_together() {
        let root = Scratch::new();
        let db = root.overhaul();
        let session = BindIdentity::new("/receiver", "runner", "moved");
        let run = RunId("foreign-run".into());
        db.authoritative_transaction(|tx| {
            tx.execute_batch("CREATE TRIGGER cut_exclusion BEFORE INSERT ON imported_run BEGIN SELECT RAISE(ABORT,'move identity cut'); END")?;
            Ok(())
        }).unwrap();
        assert!(db
            .record_imported_runs(&session, "cut", std::slice::from_ref(&run))
            .is_err());
        assert!(db.named_refs("moved", 101).unwrap().sessions.is_empty());
        assert!(!db.is_imported_run(&session, &run).unwrap());
        db.authoritative_transaction(|tx| {
            tx.execute_batch("DROP TRIGGER cut_exclusion")?;
            Ok(())
        })
        .unwrap();
        db.record_imported_runs(&session, "cut", std::slice::from_ref(&run))
            .unwrap();
        let retained = crate::retention::RetentionLineageRow {
            lineage_id: "expired".into(),
            session: session.clone(),
            retention_s: 1,
            last_activity_ms: 0,
            expires_at_ms: None,
            wal_bytes: 0,
            expired_at_ms: Some(1000),
        };
        let debt = crate::retention_withdrawals::RetentionWithdrawal {
            lineage_id: "expired".into(),
            session: session.clone(),
            call_key: "owed".into(),
            scope: None,
            question: crate::retention_withdrawals::RetentionQuestionId::Unknown,
            provider_id: "approvals".into(),
            provider_incarnation: None,
            expires_at_ms: 2000,
        };
        db.upsert_retention_lineage_with_withdrawals(&retained, &[], &[debt])
            .unwrap();
        db.retention_deletion_pending("expired", &session, true)
            .unwrap();
        db.forget_projection(&session).unwrap();
        db.rebuild(
            std::iter::empty(),
            &super::super::LedgerRows {
                condemned: &[],
                address_floors: &[],
            },
        )
        .unwrap();
        drop(db);
        let db = OverhaulDb::open(&root.0).unwrap();
        assert!(db.is_imported_run(&session, &run).unwrap());
        assert_eq!(db.pending_retention_withdrawals().unwrap().len(), 1);
        assert_eq!(
            db.retention_pending_deletions().unwrap(),
            vec![session.clone()]
        );
        assert_eq!(
            db.lookup_retention_lineage("expired").unwrap(),
            Some(retained)
        );
        assert_eq!(db.named_refs("moved", 101).unwrap().sessions.len(), 1);
        db.record_imported_runs(&session, "cut-retry", &[]).unwrap();
        assert!(db.is_imported_run(&session, &run).unwrap());
        assert!(!root.0.join("run-index.db").exists());
    }
}
