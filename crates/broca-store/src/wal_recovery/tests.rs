use super::*;
use crate::conformance::{base_records, session, text_turn};
use crate::ProjectionCommit;
use broca_core::context::ReadView;
use broca_protocol::RunFinishReason;
use broca_wal::{
    file_wal::FileWal, recovery::RecoveryHooks, Wal, WalConfig, WalError, WalRangeRecords,
};
use rusqlite::hooks::{AuthContext, Authorization};
use std::path::PathBuf;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc, Mutex};

struct Fixture {
    root: PathBuf,
    session: BindIdentity,
    store: Arc<SqliteStore>,
    wals: FileWalStore,
    path: PathBuf,
    damaged: Vec<u8>,
    before: Vec<FramedRecord>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_damage(true)
    }

    fn with_damage(damage: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "sqlite-wal-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let session = session("recovery");
        let wals = FileWalStore::with_config(root.join("wal"), WalConfig::OVERHAUL);
        let path = wals.wal_path(&session);
        let mut records = base_records(&session);
        records.extend(text_turn(2, 1, "retained"));
        records.extend(text_turn(4, 2, "removed"));
        records.push(FramedRecord {
            seq: 6,
            fence: 1,
            record: WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                usage: Usage {
                    input_tokens: Some(30),
                    ..Usage::default()
                },
                retries_used: 2,
                error: None,
                interrupt_reason: None,
                ts_ms: Some(42),
            },
        });
        let mut wal =
            FileWal::open_with_recovery(&path, WalConfig::OVERHAUL, wals.feature_floor_path())
                .unwrap()
                .with_feature_floor(wals.feature_floor_path())
                .with_wal_config(WalConfig::OVERHAUL)
                .unwrap();
        let mut cut = 0;
        for (index, record) in records.iter().enumerate() {
            if index == 3 {
                cut = std::fs::metadata(&path).unwrap().len() as usize;
            }
            wal.append(1, &record.record, true).unwrap();
        }
        drop(wal);
        let store = Arc::new(SqliteStore::open(root.join("store.db")).unwrap());
        let range =
            broca_wal::file_wal::read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
                .unwrap();
        let WalRangeRecords::Complete(before) = range.records() else {
            panic!("complete fixture")
        };
        let before = before.to_vec();
        store
            .project_records(&session, &before, Some(&range))
            .unwrap();
        assert_eq!(
            store
                .read_page_with_attribution(&session, None, 10, ReadView::Raw)
                .unwrap()
                .rows
                .len(),
            3
        );
        store
            .project_retained_head_inner(&session, &range.lineage_id.unwrap().to_string(), &before)
            .unwrap();
        store
            .index_run_active(&RunIndexRow {
                run_id: "r".into(),
                session: session.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
                title: None,
                withdrawals_owed: false,
            })
            .unwrap();
        store
            .index_run_terminal(
                "r",
                RunIndexState::Completed,
                Some(Usage::default()),
                Some(FinishReason::Stop),
                2,
            )
            .unwrap();
        store.conn.lock().unwrap().execute_batch(
            "INSERT INTO export_facts(fact_id,run_id,segment_json) VALUES('immutable','r','{}');"
        ).unwrap();
        wals.bind_recovery_projection(Some(&session), store.clone());
        let mut damaged = std::fs::read(&path).unwrap();
        if damage {
            damaged[cut + 4] = 0;
        }
        std::fs::write(&path, &damaged).unwrap();
        Self {
            root,
            session,
            store,
            wals,
            path,
            damaged,
            before,
        }
    }

    fn recover(&self) -> Result<(), WalError> {
        self.wals.recover_existing(&self.session)
    }

    fn tables(&self, tables: &[&str]) -> Vec<String> {
        let conn = self.store.conn.lock().unwrap();
        tables
            .iter()
            .map(|table| {
                let mut query = conn
                    .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                    .unwrap();
                let columns = query.column_count();
                let rows = query
                    .query_map([], |row| {
                        Ok((0..columns)
                            .map(|col| format!("{:?}", row.get_ref(col).unwrap()))
                            .collect::<Vec<_>>())
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                format!("{table}: {rows:?}")
            })
            .collect()
    }

    fn pending(&self) -> bool {
        let mut conn = self.store.conn.lock().unwrap();
        let tx = conn.transaction().unwrap();
        read_fence(&tx, &SqliteStore::key(&self.session))
            .unwrap()
            .is_some_and(|f| f.pending)
    }

    fn assert_rebuilt(&self) {
        let page = self
            .store
            .read_page_with_attribution(&self.session, None, 10, ReadView::Raw)
            .unwrap();
        let json = serde_json::to_string(&page.rows.iter().map(|r| &r.message).collect::<Vec<_>>())
            .unwrap();
        assert!(json.contains("retained"));
        assert!(!json.contains("removed"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(
            page.head.unwrap().usage.unwrap().totals.input_tokens,
            Some(10)
        );
        let run = self.store.lookup_run("r").unwrap().unwrap();
        assert_eq!(run.state, RunIndexState::Interrupted);
        assert_eq!(run.usage, None);
        assert_eq!(run.retries_used, 0);
        assert!(!self.pending());
        assert_eq!(
            self.store
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT segment_json FROM export_facts WHERE fact_id='immutable'",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "{}"
        );
        let records = self.wals.read_records(&self.session).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.record, WalRecord::RecoveryCut { .. }))
                .count(),
            1
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

// Denying one authorization callback exercises real SQLite errors at transaction,
// read and write boundaries, without exposing a fault switch in the shipping store.
fn authorize(store: &SqliteStore, fail: Option<usize>) -> Arc<Mutex<Vec<String>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    store
        .conn
        .lock()
        .unwrap()
        .authorizer(Some(move |context: AuthContext<'_>| {
            let action = format!("{:?}", context.action);
            if action.contains("Rollback") {
                return Authorization::Allow;
            }
            let mut events = captured.lock().unwrap();
            let index = events.len();
            events.push(action);
            if fail == Some(index) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }));
    events
}

fn clear_authorizer(store: &SqliteStore) {
    store
        .conn
        .lock()
        .unwrap()
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
}

const REBUILDABLE: &[&str] = &[
    "meta",
    "message",
    "message_tag",
    "episode_span",
    "run_index",
    "retained_head",
    "compaction_facts",
    "compaction_view_receipt",
    "export_facts",
    "export_meta",
];

const FAULT_SHARDS: usize = 32;

fn sqlite_fault_shard(shard: usize) {
    let baseline = Fixture::new();
    let trace = authorize(&baseline.store, None);
    baseline.recover().unwrap();
    clear_authorizer(&baseline.store);
    baseline.assert_rebuilt();
    let trace = trace.lock().unwrap().clone();
    let invalidation_commit = trace
        .iter()
        .position(|s| s.starts_with("Transaction") && !s.contains("Begin"))
        .unwrap();
    assert!(trace.iter().any(|s| s.contains("retained_head")));
    assert!(trace.iter().any(|s| s.contains("compaction_view_receipt")));
    let mut checked = 0;
    for (fail, action) in trace
        .iter()
        .enumerate()
        .filter(|(index, _)| index % FAULT_SHARDS == shard)
    {
        checked += 1;
        let f = Fixture::new();
        let before = f.tables(REBUILDABLE);
        let exports = f.tables(&["export_facts", "export_meta"]);
        let events = authorize(&f.store, Some(fail));
        let error = f.recover().expect_err(action);
        clear_authorizer(&f.store);
        assert!(
            events.lock().unwrap().len() > fail,
            "fault not reached: {fail}: {action}"
        );
        assert!(
            error.to_string().contains("WAL recovery"),
            "{fail}: {action}: {error}"
        );
        if fail <= invalidation_commit {
            assert_eq!(
                std::fs::read(&f.path).unwrap(),
                f.damaged,
                "{fail}: {action}"
            );
            assert_eq!(f.tables(REBUILDABLE), before, "{fail}: {action}");
        } else {
            assert!(f.pending(), "{fail}: {action}");
            assert!(matches!(
                f.store
                    .read_page_with_attribution(&f.session, None, 10, ReadView::Raw),
                Err(broca_core::StorePageReadError::AttributionStale)
            ));
        }
        f.recover().unwrap();
        f.assert_rebuilt();
        assert_eq!(f.tables(&["export_facts", "export_meta"]), exports);
    }
    assert!(checked > 0);
    eprintln!(
        "SQLite recovery fault shard {shard}: {checked} of {} boundaries",
        trace.len()
    );
}

// Bound work per test on slow synced filesystems while preserving every SQLite
// fault boundary; the registration check below pins complete, disjoint coverage.
macro_rules! sqlite_fault_tests {
    ($($name:ident => $shard:expr),* $(,)?) => {
        const FAULT_SHARD_IDS: &[usize] = &[$($shard),*];
        $(#[test] fn $name() { sqlite_fault_shard($shard); })*
    };
}

sqlite_fault_tests! {
    recovery_sqlite_failure_at_each_step_s00 => 0, recovery_sqlite_failure_at_each_step_s01 => 1,
    recovery_sqlite_failure_at_each_step_s02 => 2, recovery_sqlite_failure_at_each_step_s03 => 3,
    recovery_sqlite_failure_at_each_step_s04 => 4, recovery_sqlite_failure_at_each_step_s05 => 5,
    recovery_sqlite_failure_at_each_step_s06 => 6, recovery_sqlite_failure_at_each_step_s07 => 7,
    recovery_sqlite_failure_at_each_step_s08 => 8, recovery_sqlite_failure_at_each_step_s09 => 9,
    recovery_sqlite_failure_at_each_step_s10 => 10, recovery_sqlite_failure_at_each_step_s11 => 11,
    recovery_sqlite_failure_at_each_step_s12 => 12, recovery_sqlite_failure_at_each_step_s13 => 13,
    recovery_sqlite_failure_at_each_step_s14 => 14, recovery_sqlite_failure_at_each_step_s15 => 15,
    recovery_sqlite_failure_at_each_step_s16 => 16, recovery_sqlite_failure_at_each_step_s17 => 17,
    recovery_sqlite_failure_at_each_step_s18 => 18, recovery_sqlite_failure_at_each_step_s19 => 19,
    recovery_sqlite_failure_at_each_step_s20 => 20, recovery_sqlite_failure_at_each_step_s21 => 21,
    recovery_sqlite_failure_at_each_step_s22 => 22, recovery_sqlite_failure_at_each_step_s23 => 23,
    recovery_sqlite_failure_at_each_step_s24 => 24, recovery_sqlite_failure_at_each_step_s25 => 25,
    recovery_sqlite_failure_at_each_step_s26 => 26, recovery_sqlite_failure_at_each_step_s27 => 27,
    recovery_sqlite_failure_at_each_step_s28 => 28, recovery_sqlite_failure_at_each_step_s29 => 29,
    recovery_sqlite_failure_at_each_step_s30 => 30, recovery_sqlite_failure_at_each_step_s31 => 31,
}

#[test]
fn recovery_sqlite_fault_shards_are_complete_and_disjoint() {
    assert_eq!(FAULT_SHARD_IDS, (0..FAULT_SHARDS).collect::<Vec<_>>());
}

struct StopAfterInvalidation {
    store: Arc<SqliteStore>,
    session: BindIdentity,
}

impl RecoveryHooks for StopAfterInvalidation {
    fn before_cut(&self, cut: &RecoveryCut) -> Result<(), WalError> {
        self.store
            .invalidate_wal_recovery_inner(&self.session, cut)
            .unwrap();
        Err(WalError::Io(std::io::Error::other(
            "stop after durable invalidation",
        )))
    }
    fn after_repair(&self, _: &Path, _: &BTreeSet<String>) -> Result<(), WalError> {
        unreachable!()
    }
}

fn stop_after_invalidation(f: &Fixture) {
    let hooks = Arc::new(StopAfterInvalidation {
        store: f.store.clone(),
        session: f.session.clone(),
    });
    assert!(FileWal::open_with_manifest_and_recovery_hooks(
        &f.path,
        f.wals.manifest_path(),
        session_addr(&f.session),
        String::new(),
        broca_core::wal_writer::agent_session_ref(&f.session),
        (WalConfig::OVERHAUL, f.wals.feature_floor_path()),
        hooks,
    )
    .is_err());
    assert!(f.pending());
    assert_eq!(std::fs::read(&f.path).unwrap(), f.damaged);
}

#[test]
fn recovery_pending_fence_refuses_lease_free_summary_readers() {
    let f = Fixture::new();
    stop_after_invalidation(&f);
    let reopened = SqliteStore::open(f.root.join("store.db")).unwrap();
    for store in [&*f.store, &reopened] {
        let error = store
            .lookup_run("r")
            .expect_err("pending rows must not be reported as live");
        assert!(error.contains("recovery") && error.contains("r"));
        let error = store
            .diagnostic_evidence(0, i64::MAX)
            .err()
            .expect("pending rows are not diagnostic facts");
        assert!(error.contains("recovery"));
    }
    f.recover().unwrap();
    assert_eq!(
        reopened.lookup_run("r").unwrap().unwrap().state,
        RunIndexState::Interrupted
    );
    assert!(reopened.diagnostic_evidence(0, i64::MAX).is_ok());
}

#[test]
fn recovery_invalidation_and_rebuild_enforce_full_synchronous() {
    let f = Fixture::new();
    f.store
        .conn
        .lock()
        .unwrap()
        .pragma_update(None, "synchronous", "OFF")
        .unwrap();
    stop_after_invalidation(&f);
    let mode: i64 = f
        .store
        .conn
        .lock()
        .unwrap()
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .unwrap();
    assert_eq!(
        mode, 2,
        "the pre-cut invalidation must commit under synchronous=FULL"
    );
    f.store
        .conn
        .lock()
        .unwrap()
        .pragma_update(None, "synchronous", "NORMAL")
        .unwrap();
    f.recover().unwrap();
    let mode: i64 = f
        .store
        .conn
        .lock()
        .unwrap()
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .unwrap();
    assert_eq!(
        mode, 2,
        "reprojection must also publish its completed fence durably"
    );
    f.assert_rebuilt();
}

#[test]
fn recovery_crash_after_invalidation_reopens_without_stale_projection() {
    let mut f = Fixture::new();
    let exports = f.tables(&["export_facts", "export_meta"]);
    let snapshot = f
        .store
        .projection_snapshot(&SqliteStore::key(&f.session))
        .unwrap();
    let stale = SqliteStore::prepare_projection(snapshot, &f.before, None).unwrap();
    let hooks = Arc::new(StopAfterInvalidation {
        store: f.store.clone(),
        session: f.session.clone(),
    });
    assert!(FileWal::open_with_manifest_and_recovery_hooks(
        &f.path,
        f.wals.manifest_path(),
        session_addr(&f.session),
        String::new(),
        broca_core::wal_writer::agent_session_ref(&f.session),
        (WalConfig::OVERHAUL, f.wals.feature_floor_path()),
        hooks,
    )
    .is_err());
    assert!(f.pending());
    assert_eq!(std::fs::read(&f.path).unwrap(), f.damaged);
    assert!(matches!(
        f.store
            .try_commit_projection(&SqliteStore::key(&f.session), stale)
            .unwrap(),
        ProjectionCommit::Conflict
    ));
    assert!(
        f.store.project(&f.session, &f.before).is_err(),
        "pre-cut records cannot remove the durable fence"
    );
    f.store = Arc::new(SqliteStore::open(f.root.join("store.db")).unwrap());
    f.wals = FileWalStore::with_config(f.root.join("wal"), WalConfig::OVERHAUL);
    f.wals
        .bind_recovery_projection(Some(&f.session), f.store.clone());
    assert!(f.pending(), "the fence must survive reopening SQLite");
    f.recover().unwrap();
    f.assert_rebuilt();
    assert_eq!(f.tables(&["export_facts", "export_meta"]), exports);
    // Once RecoveryCut is projected, reopening compares its identifier with
    // the stored checkpoint instead of writing or replaying the full session WAL.
    let before = f.tables(REBUILDABLE);
    let events = authorize(&f.store, None);
    f.recover().unwrap();
    clear_authorizer(&f.store);
    assert!(!events
        .lock()
        .unwrap()
        .iter()
        .any(|a| a.starts_with("Insert") || a.starts_with("Update") || a.starts_with("Delete")));
    assert_eq!(f.tables(REBUILDABLE), before);
}

#[test]
fn recovery_normal_open_does_not_access_sqlite() {
    let f = Fixture::with_damage(false);
    let before = f.tables(REBUILDABLE);
    let events = authorize(&f.store, Some(0));
    f.recover().unwrap();
    clear_authorizer(&f.store);
    assert!(events.lock().unwrap().is_empty());
    assert_eq!(f.tables(REBUILDABLE), before);
}

#[test]
fn recovery_restores_surviving_terminal_attribution() {
    let f = Fixture::with_damage(false);
    let mut bytes = std::fs::read(&f.path).unwrap();
    bytes.extend_from_slice(&[0; 128]);
    std::fs::write(&f.path, bytes).unwrap();
    f.store.conn.lock().unwrap().execute_batch(
        "UPDATE run_index SET state_changed_ms=777,error_class='stale',error_code='stale',pause_reason='stale',interrupt_reason='stale';"
    ).unwrap();
    f.recover().unwrap();
    let run = f.store.lookup_run("r").unwrap().unwrap();
    assert_eq!(run.state, RunIndexState::Completed);
    assert_eq!(run.usage.unwrap().input_tokens, Some(30));
    assert_eq!(run.retries_used, 2);
    let attribution = f.store.conn.lock().unwrap().query_row(
        "SELECT state_changed_ms,error_class,error_code,pause_reason,interrupt_reason FROM run_index WHERE run_id='r'", [],
        |row| Ok((row.get::<_, Option<i64>>(0)?,row.get::<_, Option<String>>(1)?,row.get::<_, Option<String>>(2)?,row.get::<_, Option<String>>(3)?,row.get::<_, Option<String>>(4)?))
    ).unwrap();
    assert_eq!(attribution, (Some(42), None, None, None, None));
}
