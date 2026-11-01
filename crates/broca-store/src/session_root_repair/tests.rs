use super::*;
use broca_core::{MaterializedStore, Persister, RunIndex, RunIndexRow, WalStore};
use broca_protocol::{caller::RunFinishReason, ModelHandle};
use broca_wal::WalRecord;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

#[path = "one_wrong_root_tests.rs"]
mod one_wrong_root;

struct Fixture {
    root: PathBuf,
    store: SqliteStore,
    live: BindIdentity,
    stale: BindIdentity,
}

impl Fixture {
    fn new() -> Self {
        Self::with_recorded_harness("broca")
    }

    fn with_recorded_harness(harness: &str) -> Self {
        Self::with_first_terminal(harness, Some(RunFinishReason::Completed))
    }

    fn with_first_terminal(harness: &str, first_terminal: Option<RunFinishReason>) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "session-root-repair-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let live = BindIdentity::new(
            "/Users/ufukaltinok/Work/Projects/CortexKit/alfonso-tui",
            "broca",
            "cktui-dogfood-1",
        );
        let stale = BindIdentity::new(
            "/Users/ufukaltinok/Work/Projects/CortexKit/brocatui",
            "broca",
            "cktui-dogfood-1",
        );
        assert_eq!(session_addr(&live), "a88b374690b4cdd7");
        assert_eq!(session_addr(&stale), "a0b598f8b007a2b3");
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        let wal_store = broca_core::FileWalStore::new(root.join("wal"));
        let mut wal = wal_store.open(&live).unwrap();
        for n in 0..24 {
            let identity = if n < 6 { &stale } else { &live };
            let mut recorded_identity = identity.clone();
            if n < 6 {
                recorded_identity.harness = harness.to_owned();
            }
            let run_id = format!("run-{n:02}");
            let start = crate::conformance::run_started(
                0,
                &run_id,
                &recorded_identity,
                "user history",
                ModelHandle::new("stub", "model"),
            );
            wal.append(1, &start.record, true).unwrap();
            for frame in crate::conformance::text_turn(0, 1, &format!("reply-{n:02}")) {
                wal.append(1, &frame.record, true).unwrap();
            }
            if n != 0 || first_terminal.is_some() {
                wal.append(
                    1,
                    &WalRecord::RunFinished {
                        interrupt_reason: None,
                        reason: if n == 0 {
                            first_terminal.unwrap()
                        } else {
                            RunFinishReason::Completed
                        },
                        ts_ms: Some(2),
                        usage: Default::default(),
                        error: None,
                        retries_used: 0,
                    },
                    true,
                )
                .unwrap();
            }
            store
                .index_run_active(&RunIndexRow {
                    run_id,
                    session: identity.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Completed,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: Some(format!("title-{n}")),
                    withdrawals_owed: false,
                })
                .unwrap();
        }
        drop(wal);
        // Reproduce discovery from historical run rows, rather than installing
        // the expected inventory directly in the fixture.
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch(
                "DELETE FROM agent_data_session_ref; DELETE FROM agent_data_session_ref_v2;
             DELETE FROM agent_data_projection; DELETE FROM agent_data_backfill_cursor;",
            )
            .unwrap();
        let counts =
            session_refs::migrate(&root.join("run-index.db"), &ReadOnlyWalReader::new(&root))
                .unwrap();
        assert!(counts.complete);
        let census = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT address FROM agent_data_census WHERE covered=1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(census, "a88b374690b4cdd7");
        Self {
            root,
            store,
            live,
            stale,
        }
    }

    fn preview(&self) -> Result<SessionRootRepair> {
        self.store
            .preview_session_root_repair(&self.live.harness, &self.live.session)
    }

    fn wal_path(&self) -> PathBuf {
        self.root.join("wal/a88b374690b4cdd7.wal")
    }

    fn seed_orphaned_projection(&self, include_live: bool) {
        let wal = broca_core::FileWalStore::new(self.root.join("wal"));
        let prefix = wal
            .read_records_range(&self.live, 0, Some(32), None)
            .unwrap();
        self.store.project_range(&self.stale, &prefix).unwrap();
        assert_eq!(
            self.store
                .read_prefix_with_watermark(&self.stale)
                .unwrap()
                .prefix
                .len(),
            16
        );
        if include_live {
            let complete = wal.read_records_range(&self.live, 0, None, None).unwrap();
            self.store.project_range(&self.live, &complete).unwrap();
        }
    }

    fn null_lineage_prefix() -> Self {
        let fixture = Self::with_recorded_harness("runner");
        fixture.seed_orphaned_projection(true);
        fixture
            .store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET lineage_id=NULL WHERE session=?1",
                [SqliteStore::key(&fixture.stale)],
            )
            .unwrap();
        fixture
    }

    fn projection_rows(
        &self,
        identity: &BindIdentity,
    ) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
        let conn = self.store.conn.lock().unwrap();
        let mut snapshot = BTreeMap::new();
        for table in PROJECTION_TABLES {
            let mut statement = conn
                .prepare(&format!("SELECT * FROM {table} WHERE session=?1"))
                .unwrap();
            let columns = statement.column_count();
            let mut rows = statement
                .query_map([SqliteStore::key(identity)], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows.sort_by_key(|row| format!("{row:?}"));
            snapshot.insert(table.to_string(), rows);
        }
        snapshot
    }

    fn archive(&self, identity: &BindIdentity, directory: &str) {
        let address = session_addr(identity);
        let live = self.root.join("wal").join(format!("{address}.wal"));
        let archives = self.root.join(directory);
        std::fs::create_dir_all(&archives).unwrap();
        let mut writer = broca_wal::archive::ArchiveWriter::create(archives.join("fold-1.ark"))
            .unwrap()
            .without_feature_floor();
        writer
            .append(&address, &std::fs::read(&live).unwrap())
            .unwrap();
        writer.seal().unwrap();
        std::fs::remove_file(live).unwrap();
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        let conn = self.store.conn.lock().unwrap();
        let snapshot = conn
            .prepare(
                "SELECT run_id,session FROM run_index
            UNION ALL SELECT session_ref,address FROM agent_data_session_ref
            UNION ALL SELECT session_ref,address FROM agent_data_session_ref_v2
            UNION ALL SELECT address,CAST(covered AS TEXT) FROM agent_data_census
            ORDER BY 1,2",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        snapshot
    }

    fn refuses_unchanged(&self, reason: &str) {
        let before = self.snapshot();
        let bytes = std::fs::read(self.wal_path()).unwrap();
        let error = self.preview().unwrap_err().to_string();
        assert!(error.contains(reason), "{error}");
        assert_eq!(self.snapshot(), before);
        assert_eq!(std::fs::read(self.wal_path()).unwrap(), bytes);
    }
}

#[test]
fn null_lineage_exact_prefix_is_repaired_without_changing_live_history() {
    let f = Fixture::null_lineage_prefix();
    let live_before = f.projection_rows(&f.live);
    let transcript_before = f.store.read_prefix_with_watermark(&f.live).unwrap();
    assert_eq!(transcript_before.prefix.len(), 48);
    let bytes = std::fs::read(f.wal_path()).unwrap();
    let plan = f.preview().unwrap();
    assert_eq!(plan.repoint_runs.len(), 6);
    assert_eq!(plan.stale_meta.as_ref().unwrap().lineage_id, None);
    assert_eq!(
        plan.stale_meta.as_ref().unwrap().last_run_id.as_deref(),
        Some("run-07")
    );
    assert_eq!(
        plan.stale_projection_evidence,
        Some(ProjectionEvidence {
            kind: ProjectionEvidenceKind::NullLineagePrefixMatch,
            compared_message_rows: 16,
        })
    );
    let report = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        report["stale_projection_evidence"]["kind"],
        "null_lineage_prefix_match"
    );
    assert_eq!(
        report["stale_projection_evidence"]["compared_message_rows"],
        16
    );
    f.store.apply_session_root_repair(&plan).unwrap();
    let resolved = f.store.resolve_session_address(&f.stale, None).unwrap();
    assert_eq!(resolved, f.live);
    for n in 0..24 {
        assert_eq!(
            f.store
                .lookup_run(&format!("run-{n:02}"))
                .unwrap()
                .unwrap()
                .session,
            resolved
        );
    }
    assert!(f.projection_rows(&f.stale).values().all(Vec::is_empty));
    assert_eq!(f.projection_rows(&f.live), live_before);
    assert_eq!(
        f.store.read_prefix_with_watermark(&resolved).unwrap(),
        transcript_before
    );
    assert_eq!(std::fs::read(f.wal_path()).unwrap(), bytes);
    let second = f.preview().unwrap();
    assert!(second.stale_identity.is_none());
    assert!(second.stale_projection_evidence.is_none());
    f.store.apply_session_root_repair(&second).unwrap();
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn null_lineage_prefix_requires_identical_bytes_at_every_ordinal() {
    for missing_live_row in [false, true] {
        let f = Fixture::null_lineage_prefix();
        if missing_live_row {
            f.store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "DELETE FROM message WHERE session=?1 AND ord=5",
                    [SqliteStore::key(&f.live)],
                )
                .unwrap();
        } else {
            // Even JSON with identical parsed content is not byte-identical evidence.
            f.store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE message SET json=json || ' ' WHERE session=?1 AND ord=5",
                    [SqliteStore::key(&f.stale)],
                )
                .unwrap();
        }
        let stale_before = f.projection_rows(&f.stale);
        let live_before = f.projection_rows(&f.live);
        f.refuses_unchanged("null_lineage_prefix_mismatch: stale message ord 5");
        assert_eq!(f.projection_rows(&f.stale), stale_before);
        assert_eq!(f.projection_rows(&f.live), live_before);
    }
}

#[test]
fn null_lineage_prefix_requires_last_run_id_in_the_verified_log() {
    for last_run_id in [Some("not-in-log"), None] {
        let f = Fixture::null_lineage_prefix();
        f.store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET last_run_id=?1 WHERE session=?2",
                params![last_run_id, SqliteStore::key(&f.stale)],
            )
            .unwrap();
        let stale_before = f.projection_rows(&f.stale);
        let live_before = f.projection_rows(&f.live);
        f.refuses_unchanged("null_lineage_last_run_missing: stale meta.last_run_id");
        assert_eq!(f.projection_rows(&f.stale), stale_before);
        assert_eq!(f.projection_rows(&f.live), live_before);
    }
}

#[test]
fn null_lineage_prefix_proof_is_rechecked_before_writing() {
    let f = Fixture::null_lineage_prefix();
    let plan = f.preview().unwrap();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE message SET json=json || ' ' WHERE session=?1 AND ord=5",
            [SqliteStore::key(&f.stale)],
        )
        .unwrap();
    let rows_before = f.snapshot();
    let stale_before = f.projection_rows(&f.stale);
    let live_before = f.projection_rows(&f.live);
    assert!(f
        .store
        .apply_session_root_repair(&plan)
        .unwrap_err()
        .to_string()
        .contains("null_lineage_prefix_mismatch"));
    assert_eq!(f.snapshot(), rows_before);
    assert_eq!(f.projection_rows(&f.stale), stale_before);
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn historical_harness_rekey_repairs_by_run_id_and_preserves_all_24_runs() {
    let f = Fixture::with_recorded_harness("runner");
    f.seed_orphaned_projection(true);
    let before = f.store.read_prefix_with_watermark(&f.live).unwrap();
    assert_eq!(before.prefix.len(), 48);
    let bytes = std::fs::read(f.wal_path()).unwrap();
    assert_eq!(
        f.store
            .resolve_session_address(&f.live, None)
            .unwrap_err()
            .code(),
        "session_address_ambiguous"
    );
    let plan = f.preview().unwrap();
    assert_eq!(plan.repoint_runs.len(), 6);
    for change in &plan.repoint_runs {
        assert_eq!(change.from.harness, "broca");
        assert_eq!(change.episode_session.harness, "runner");
        assert_eq!(change.episode_session.project_root, f.stale.project_root);
        assert_eq!(change.episode_session.session, "cktui-dogfood-1");
        assert_eq!(change.episode_state, "completed");
    }
    let report = serde_json::to_value(&plan).unwrap();
    assert_eq!(report["repoint_runs"][0]["from"]["harness"], "broca");
    assert_eq!(
        report["repoint_runs"][0]["episode_session"]["harness"],
        "runner"
    );
    f.store.apply_session_root_repair(&plan).unwrap();
    let resolved = f.store.resolve_session_address(&f.stale, None).unwrap();
    assert_eq!(resolved, f.live);
    for n in 0..24 {
        let row = f.store.lookup_run(&format!("run-{n:02}")).unwrap().unwrap();
        assert_eq!(row.session, resolved);
        assert_eq!(row.state, RunIndexState::Completed);
        assert_eq!(row.title, Some(format!("title-{n}")));
        assert!(before.prefix.iter().any(|message| message.content.iter().any(|part| {
            matches!(part, broca_protocol::ContentBlock::Text { text } if text == &format!("reply-{n:02}"))
        })), "missing history from run {n}");
    }
    assert_eq!(
        f.store.read_prefix_with_watermark(&resolved).unwrap(),
        before
    );
    assert!(f.projection_rows(&f.stale).values().all(Vec::is_empty));
    assert_eq!(std::fs::read(f.wal_path()).unwrap(), bytes);
    let second = f.preview().unwrap();
    assert!(second.stale_identity.is_none());
}

#[test]
fn missing_run_id_after_harness_rekey_refuses_without_cleanup() {
    let f = Fixture::with_recorded_harness("runner");
    f.seed_orphaned_projection(true);
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE run_index SET run_id='not-in-log' WHERE run_id='run-00'",
            [],
        )
        .unwrap();
    let stale_before = f.projection_rows(&f.stale);
    let live_before = f.projection_rows(&f.live);
    f.refuses_unchanged("run not-in-log is missing from the surviving log");
    assert_eq!(f.projection_rows(&f.stale), stale_before);
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn unfinished_episode_after_harness_rekey_refuses() {
    let f = Fixture::with_first_terminal("runner", None);
    f.refuses_unchanged("run run-00 has no durable run_finished");
}

#[test]
fn terminal_row_states_must_match_the_recorded_run_finished() {
    for (reason, state) in [
        (RunFinishReason::Completed, "completed"),
        (RunFinishReason::MaxSteps, "max_steps"),
        (RunFinishReason::BudgetExhausted, "budget_exhausted"),
        (RunFinishReason::ContextCeiling, "context_ceiling"),
        (RunFinishReason::Cancelled, "cancelled"),
        (RunFinishReason::Interrupted, "interrupted"),
        (RunFinishReason::Error, "error"),
        (
            RunFinishReason::TransformUnavailable,
            "transform_unavailable",
        ),
    ] {
        let f = Fixture::with_first_terminal("runner", Some(reason));
        f.store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE run_index SET state=?1 WHERE run_id='run-00'",
                [state],
            )
            .unwrap();
        let plan = f.preview().unwrap();
        assert_eq!(plan.repoint_runs[0].state, state);
        assert_eq!(plan.repoint_runs[0].episode_state, state);
        let wrong = if state == "completed" {
            "error"
        } else {
            "completed"
        };
        f.store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE run_index SET state=?1 WHERE run_id='run-00'",
                [wrong],
            )
            .unwrap();
        f.refuses_unchanged("does not match durable run_finished state");
    }
}

#[test]
fn later_unfinished_start_cannot_reuse_an_earlier_terminal() {
    let f = Fixture::with_recorded_harness("runner");
    let start = crate::conformance::run_started(
        0,
        "run-00",
        &f.stale,
        "resumed",
        ModelHandle::new("stub", "model"),
    );
    let mut writer = broca_core::FileWalStore::new(f.root.join("wal"))
        .open(&f.live)
        .unwrap();
    writer.append(1, &start.record, true).unwrap();
    drop(writer);
    f.refuses_unchanged("run run-00 has no durable run_finished");
}

#[test]
fn run_row_lineage_must_match_the_verified_surviving_log() {
    for matching in [true, false] {
        let f = Fixture::with_recorded_harness("runner");
        let (_, lineage) = broca_core::FileWalStore::new(f.root.join("wal"))
            .read_records_with_lineage(&f.live)
            .unwrap();
        let lineage = if matching {
            lineage.unwrap().to_string()
        } else {
            "11111111-2222-4333-8444-555555555555".to_owned()
        };
        f.store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE run_index SET lineage_id=?1 WHERE run_id='run-00'",
                [&lineage],
            )
            .unwrap();
        if matching {
            let plan = f.preview().unwrap();
            assert_eq!(plan.repoint_runs[0].row_lineage_id.as_ref(), Some(&lineage));
            f.store.apply_session_root_repair(&plan).unwrap();
            assert_eq!(
                f.store.resolve_session_address(&f.stale, None).unwrap(),
                f.live
            );
        } else {
            f.refuses_unchanged("row lineage_id");
        }
    }
}

#[test]
fn orphaned_projection_repair_preserves_all_24_run_transcript() {
    let f = Fixture::new();
    f.seed_orphaned_projection(true);
    let live_before = f.store.read_prefix_with_watermark(&f.live).unwrap();
    let live_rows_before = f.projection_rows(&f.live);
    let stale_before = f.store.read_prefix_with_watermark(&f.stale).unwrap();
    let stale_rows_before = f.projection_rows(&f.stale);
    assert_eq!(live_before.prefix.len(), 48);
    let bytes = std::fs::read(f.wal_path()).unwrap();
    assert_eq!(
        f.store
            .resolve_session_address(&f.live, None)
            .unwrap_err()
            .code(),
        "session_address_ambiguous"
    );
    let plan = f.preview().unwrap();
    assert_eq!(
        plan.stale_meta,
        Some(ProjectionMeta {
            lineage_id: stale_before.lineage_id.clone(),
            watermark_seq: stale_before.watermark_seq,
            last_run_id: Some("run-07".to_owned()),
        })
    );
    assert_eq!(
        plan.stale_projection_evidence,
        Some(ProjectionEvidence {
            kind: ProjectionEvidenceKind::LineageMatch,
            compared_message_rows: 0,
        })
    );
    assert_eq!(
        serde_json::to_value(&plan).unwrap()["stale_projection_evidence"]["kind"],
        "lineage_match"
    );
    assert_eq!(plan.live_log_lineage, live_before.lineage_id);
    assert!(plan.live_meta_present);
    let counts: BTreeMap<_, _> = plan
        .stale_projection_tables
        .iter()
        .map(|table| (table.table.as_str(), table.rows))
        .collect();
    assert_eq!(counts["meta"], 1);
    assert_eq!(counts["message"], 16);
    assert_eq!(counts["retention_lineage"], 0);
    assert_eq!(
        plan.stale_projection_tables
            .iter()
            .filter(|table| table.remove)
            .map(|table| table.table.as_str())
            .collect::<Vec<_>>(),
        vec![
            "bus_event_open",
            "bus_event_projection",
            "compaction_facts",
            "episode_span",
            "message",
            "message_tag",
            "meta",
            "retained_head",
            "retention_lineage"
        ]
    );
    assert_eq!(
        f.projection_rows(&f.stale),
        stale_rows_before,
        "preview must not delete orphaned projections"
    );
    f.store.apply_session_root_repair(&plan).unwrap();
    let resolved = f.store.resolve_session_address(&f.stale, None).unwrap();
    assert_eq!(resolved, f.live);
    let transcript = f.store.read_prefix_with_watermark(&resolved).unwrap();
    assert_eq!(transcript, live_before);
    for n in 0..24 {
        assert!(transcript.prefix.iter().any(|message| message.content.iter().any(|part| {
            matches!(part, broca_protocol::ContentBlock::Text { text } if text == &format!("reply-{n:02}"))
        })), "missing reply from run {n}");
    }
    assert!(f
        .store
        .read_prefix_with_watermark(&f.stale)
        .unwrap()
        .prefix
        .is_empty());
    assert!(f.projection_rows(&f.stale).values().all(Vec::is_empty));
    assert_eq!(f.projection_rows(&f.live), live_rows_before);
    assert_eq!(std::fs::read(f.wal_path()).unwrap(), bytes);
    let second = f.preview().unwrap();
    assert!(second.stale_identity.is_none());
    assert!(second.stale_projection_tables.is_empty());
    assert!(second.stale_meta.is_none());
    let before_second = f.projection_rows(&f.live);
    f.store.apply_session_root_repair(&second).unwrap();
    assert_eq!(
        f.store.read_prefix_with_watermark(&f.live).unwrap(),
        live_before
    );
    assert_eq!(f.projection_rows(&f.live), before_second);
}

#[test]
fn orphaned_projection_with_a_different_lineage_is_left_alone() {
    let f = Fixture::new();
    f.seed_orphaned_projection(true);
    let different = "11111111-2222-4333-8444-555555555555";
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE meta SET lineage_id=?1 WHERE session=?2",
            params![different, SqliteStore::key(&f.stale)],
        )
        .unwrap();
    let stale_before = f.projection_rows(&f.stale);
    let live_before = f.projection_rows(&f.live);
    f.refuses_unchanged("stale meta lineage_id");
    let error = f.preview().unwrap_err().to_string();
    assert!(error.contains(different), "{error}");
    assert!(
        error.contains("differs from verified live log lineage"),
        "{error}"
    );
    assert_eq!(f.projection_rows(&f.stale), stale_before);
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn orphaned_projection_without_live_meta_is_rebuildable_from_live_or_archived_log() {
    for archive in [false, true] {
        let f = Fixture::new();
        f.seed_orphaned_projection(false);
        if archive {
            f.archive(&f.live, "wal-archive-durable");
        }
        let plan = f.preview().unwrap();
        assert!(!plan.live_meta_present);
        assert!(plan.stale_meta.is_some());
        f.store.apply_session_root_repair(&plan).unwrap();
        assert!(
            f.projection_rows(&f.live).values().all(Vec::is_empty),
            "repair must not write a live projection"
        );
        let resolved = f.store.resolve_session_address(&f.stale, None).unwrap();
        let range = broca_core::FileWalStore::new(f.root.join("wal"))
            .read_records_range(&resolved, 0, None, None)
            .unwrap();
        f.store.project_range(&resolved, &range).unwrap();
        assert_eq!(
            f.store
                .read_prefix_with_watermark(&resolved)
                .unwrap()
                .prefix
                .len(),
            48
        );
        assert!(f.projection_rows(&f.stale).values().all(Vec::is_empty));
    }
}

#[test]
fn orphaned_projection_cleanup_includes_all_flat_key_projection_tables() {
    let f = Fixture::new();
    f.seed_orphaned_projection(true);
    let mut conn = f.store.conn.lock().unwrap();
    let tx = conn.transaction().unwrap();
    let key = SqliteStore::key(&f.stale);
    let live = SqliteStore::key(&f.live);
    let lineage = tx
        .query_row(
            "SELECT lineage_id FROM meta WHERE session=?1",
            [&key],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    tx.execute("INSERT INTO message_tag VALUES(?1,1,0,0)", [&key])
        .unwrap();
    for session in [&key, &live] {
        tx.execute(
            "INSERT INTO compaction_facts VALUES(?1,?2,'{}')",
            params![session, lineage],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO bus_event_projection VALUES(?1,'epoch',1,'{}')",
            [session],
        )
        .unwrap();
        tx.execute("INSERT INTO bus_event_open VALUES(?1,'{}')", [session])
            .unwrap();
    }
    tx.execute(
        "INSERT INTO retained_head(lineage_id,session,at_seq,facts_json) VALUES(?1,?2,1,'{}')",
        params![lineage, key],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO retention_lineage VALUES(?1,?2,1,0,NULL,1,NULL)",
        params![lineage, key],
    )
    .unwrap();
    tx.execute("INSERT INTO retention_discovery VALUES(?1)", [&lineage])
        .unwrap();
    tx.commit().unwrap();
    drop(conn);
    let live_before = f.projection_rows(&f.live);
    let plan = f.preview().unwrap();
    assert!(plan
        .stale_projection_tables
        .iter()
        .filter(|table| table.remove)
        .all(|table| table.rows > 0));
    assert_eq!(plan.remove_retention_discovery_rows, 1);
    f.store.apply_session_root_repair(&plan).unwrap();
    assert!(f.projection_rows(&f.stale).values().all(Vec::is_empty));
    assert_eq!(f.projection_rows(&f.live), live_before);
    assert_eq!(
        f.store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM retention_discovery", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn orphaned_projection_cleanup_failure_rolls_back_the_entire_repair() {
    let f = Fixture::new();
    f.seed_orphaned_projection(true);
    let plan = f.preview().unwrap();
    let before = f.snapshot();
    let stale_before = f.projection_rows(&f.stale);
    let live_before = f.projection_rows(&f.live);
    f.store
        .conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TEMP TRIGGER fail_orphan_cleanup BEFORE DELETE ON message
         BEGIN SELECT RAISE(ABORT,'injected orphan cleanup failure'); END;",
        )
        .unwrap();
    assert!(f
        .store
        .apply_session_root_repair(&plan)
        .unwrap_err()
        .to_string()
        .contains("injected orphan cleanup failure"));
    assert_eq!(f.snapshot(), before);
    assert_eq!(f.projection_rows(&f.stale), stale_before);
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn unknown_stale_key_table_refuses_instead_of_discarding_authority() {
    let f = Fixture::new();
    f.seed_orphaned_projection(true);
    f.store
        .conn
        .lock()
        .unwrap()
        .execute_batch("CREATE TABLE unrecognized_authority(session TEXT, payload TEXT)")
        .unwrap();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO unrecognized_authority VALUES(?1,'keep')",
            [SqliteStore::key(&f.stale)],
        )
        .unwrap();
    let stale_before = f.projection_rows(&f.stale);
    let live_before = f.projection_rows(&f.live);
    f.refuses_unchanged(
        "unrecognized_authority has stale-key rows but is not a known per-session projection",
    );
    assert_eq!(f.projection_rows(&f.stale), stale_before);
    assert_eq!(f.projection_rows(&f.live), live_before);
}

#[test]
fn split_root_repair_resolves_bind_and_preserves_all_24_runs() {
    let f = Fixture::new();
    let error = f.store.resolve_session_address(&f.live, None).unwrap_err();
    assert_eq!(error.code(), "session_address_ambiguous");
    let bytes = std::fs::read(f.wal_path()).unwrap();
    let mut original: Vec<_> = (0..24)
        .map(|n| f.store.lookup_run(&format!("run-{n:02}")).unwrap().unwrap())
        .collect();
    let plan = f.preview().unwrap();
    assert_eq!(plan.repoint_runs.len(), 6);
    assert_eq!(plan.remove_v1_refs.len(), 1);
    assert_eq!(plan.remove_v2_aliases.len(), 1);
    for (n, row) in plan.repoint_runs.iter().enumerate() {
        assert_eq!(row.run_id, format!("run-{n:02}"));
        assert_eq!(row.from, f.stale);
        assert_eq!(row.to, f.live);
    }
    let maintenance =
        SqliteStore::open_existing_for_session_root_repair(&f.root.join("run-index.db")).unwrap();
    maintenance.apply_session_root_repair(&plan).unwrap();
    drop(maintenance);
    let resolved = f.store.resolve_session_address(&f.stale, None).unwrap();
    assert_eq!(resolved, f.live);
    let history = broca_core::FileWalStore::new(f.root.join("wal"))
        .read_records(&resolved)
        .unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|r| matches!(r.record, WalRecord::RunStarted { .. }))
            .count(),
        24
    );
    assert_eq!(
        history
            .iter()
            .filter(|r| matches!(r.record, WalRecord::RunFinished { .. }))
            .count(),
        24
    );
    assert_eq!(
        history
            .iter()
            .filter(|r| matches!(r.record, WalRecord::ModelStepFinished { .. }))
            .count(),
        24
    );
    for row in &mut original {
        row.session = f.live.clone();
        assert_eq!(
            f.store.lookup_run(&row.run_id).unwrap().as_ref(),
            Some(&*row)
        );
    }
    assert_eq!(std::fs::read(f.wal_path()).unwrap(), bytes);
    assert_eq!(
        f.store
            .session_refs(std::iter::once(agent_session_ref(&f.live).as_str()))
            .unwrap()
            .sessions
            .len(),
        1
    );
    drop(f.store);
    let reopened = SqliteStore::open(f.root.join("run-index.db")).unwrap();
    session_refs::migrate(
        &f.root.join("run-index.db"),
        &ReadOnlyWalReader::new(&f.root),
    )
    .unwrap();
    assert_eq!(
        reopened.resolve_session_address(&f.stale, None).unwrap(),
        f.live
    );
}

#[test]
fn both_roots_with_logs_are_left_alone_including_archived_logs() {
    for archived in [false, true] {
        let f = Fixture::new();
        std::fs::copy(f.wal_path(), f.root.join("wal/a0b598f8b007a2b3.wal")).unwrap();
        if archived {
            f.archive(&f.stale, "wal-archive-durable");
        }
        f.refuses_unchanged("exactly one address with a log, found 2");
    }
}

#[test]
fn missing_root_with_deletion_records_is_left_alone() {
    for ledger in ["pending", "tombstone", "deleting"] {
        let f = Fixture::new();
        match ledger {
            "pending" => f
                .store
                .retention_deletion_pending("dead-lineage", &f.stale, true)
                .unwrap(),
            "tombstone" => {
                let dir = f.root.join("wal-ephemeral/a0b598f8b007a2b3.expired");
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("dead-lineage"), b"{}").unwrap();
            }
            _ => {
                std::fs::create_dir_all(f.root.join("wal-ephemeral")).unwrap();
                std::fs::write(
                    f.root
                        .join("wal-ephemeral/a0b598f8b007a2b3.dead-lineage.deleting"),
                    b"deletion residue",
                )
                .unwrap();
            }
        }
        f.refuses_unchanged("deletion");
    }
}

#[test]
fn dry_run_is_read_only_and_reports_each_change() {
    let f = Fixture::new();
    let before = f.snapshot();
    let changes = f.store.conn.lock().unwrap().total_changes();
    let readonly = SqliteStore::open_read_only(f.root.join("run-index.db")).unwrap();
    let plan = readonly
        .preview_session_root_repair("broca", "cktui-dogfood-1")
        .unwrap();
    let json = serde_json::to_value(&plan).unwrap();
    assert_eq!(json["repoint_runs"].as_array().unwrap().len(), 6);
    assert_eq!(
        json["remove_v1_refs"][0]["identity"]["project_root"],
        f.stale.project_root.to_string_lossy().as_ref()
    );
    assert_eq!(
        json["remove_v2_aliases"][0]["identity"]["session"],
        "cktui-dogfood-1"
    );
    assert_eq!(f.store.conn.lock().unwrap().total_changes(), changes);
    assert_eq!(f.snapshot(), before);
    assert!(!f.root.join("leases").exists());
    assert_eq!(
        f.store
            .resolve_session_address(&f.live, None)
            .unwrap_err()
            .code(),
        "session_address_ambiguous"
    );
}

#[test]
fn second_repair_is_a_no_op_without_even_acquiring_leases() {
    let f = Fixture::new();
    f.store
        .apply_session_root_repair(&f.preview().unwrap())
        .unwrap();
    let before = f.snapshot();
    let files = |directory: &Path| -> Vec<(PathBuf, Vec<u8>)> {
        let mut files: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|e| {
                let path = e.unwrap().path();
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        files.sort();
        files
    };
    let leases_before = files(&f.root.join("leases"));
    let plan = f.preview().unwrap();
    assert!(plan.stale_identity.is_none());
    assert!(plan.repoint_runs.is_empty());
    assert!(plan.remove_v1_refs.is_empty());
    assert!(plan.remove_v2_aliases.is_empty());
    f.store.apply_session_root_repair(&plan).unwrap();
    assert_eq!(f.snapshot(), before);
    assert_eq!(files(&f.root.join("leases")), leases_before);
}

#[test]
fn archived_survivor_is_repaired_without_extraction() {
    for directory in ["wal-archive", "wal-archive-durable"] {
        let f = Fixture::new();
        let bytes = std::fs::read(f.wal_path()).unwrap();
        f.archive(&f.live, directory);
        let archive = f.root.join(directory).join("fold-1.ark");
        let archive_before = std::fs::read(&archive).unwrap();
        f.store
            .apply_session_root_repair(&f.preview().unwrap())
            .unwrap();
        assert_eq!(
            f.store.resolve_session_address(&f.stale, None).unwrap(),
            f.live
        );
        let (records, size) = ReadOnlyWalReader::new(&f.root)
            .records("a88b374690b4cdd7")
            .unwrap();
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r.record, WalRecord::RunFinished { .. }))
                .count(),
            24
        );
        assert!(!f.wal_path().exists());
        assert_eq!(std::fs::read(archive).unwrap(), archive_before);
    }
}

#[test]
fn move_and_cut_fences_are_not_bypassed() {
    for fence in ["moving", "moved", "cut", "corrupt-cut"] {
        let f = Fixture::new();
        match fence {
            "moving" | "moved" => {
                let value = if fence == "moving" {
                    serde_json::json!({"cutId":"cut", "endOffset":0, "lineage_id":"lineage"})
                } else {
                    serde_json::json!({"cutId":"cut", "holder":"elsewhere", "lineage_id":"lineage"})
                };
                std::fs::write(
                    f.root.join("wal").join(format!("a0b598f8b007a2b3.{fence}")),
                    value.to_string(),
                )
                .unwrap();
            }
            _ => {
                let cuts = f.root.join("agent-data/cuts");
                std::fs::create_dir_all(&cuts).unwrap();
                let value = serde_json::json!({"refs":[{"ref_kind":"broca-session","ref":agent_session_ref(&f.stale)}],
                    "intent":{"op":"release","request":{}}});
                std::fs::write(
                    cuts.join("cut.json"),
                    if fence == "corrupt-cut" {
                        "not-json".to_owned()
                    } else {
                        value.to_string()
                    },
                )
                .unwrap();
            }
        }
        f.refuses_unchanged(if fence == "corrupt-cut" {
            "move/cut fence at address"
        } else if fence == "moved" {
            "session_moved"
        } else {
            "session_moving"
        });
    }
}

#[test]
fn leases_on_either_root_exclude_repair() {
    use broca_core::lease::{FileLeaseStore, LeaseStore};
    for stale in [false, true] {
        let f = Fixture::new();
        let plan = f.preview().unwrap();
        let before = f.snapshot();
        let leases = FileLeaseStore::new(f.root.join("leases"));
        let identity = if stale { &f.stale } else { &f.live };
        let _held = leases
            .acquire(&broca_core::lease::lease_key_for(identity))
            .unwrap();
        assert!(f.store.apply_session_root_repair(&plan).is_err());
        assert_eq!(f.snapshot(), before);
    }
}

#[test]
fn unproven_or_nonterminal_runs_are_not_reassigned() {
    for state in ["active", "paused", "not-in-wal"] {
        let f = Fixture::new();
        let conn = f.store.conn.lock().unwrap();
        if state == "not-in-wal" {
            conn.execute(
                "UPDATE run_index SET run_id='unrelated' WHERE run_id='run-00'",
                [],
            )
            .unwrap();
        } else {
            conn.execute(
                "UPDATE run_index SET state=?1 WHERE run_id='run-00'",
                [state],
            )
            .unwrap();
        }
        drop(conn);
        f.refuses_unchanged(if state == "not-in-wal" {
            "cannot re-point exactly"
        } else {
            "not terminal"
        });
    }
}

#[test]
fn failed_transaction_rolls_back_runs_refs_and_writer_marker() {
    let f = Fixture::new();
    let plan = f.preview().unwrap();
    let before = f.snapshot();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TEMP TRIGGER fail_root_repair BEFORE UPDATE OF session ON run_index
         WHEN OLD.run_id='run-01' BEGIN SELECT RAISE(ABORT,'injected repair failure'); END;",
        )
        .unwrap();
    assert!(f
        .store
        .apply_session_root_repair(&plan)
        .unwrap_err()
        .to_string()
        .contains("injected repair failure"));
    assert_eq!(f.snapshot(), before);
    assert_eq!(
        f.store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM agent_data_writer", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn changed_preview_is_refused_and_obsolete_census_rows_are_reported() {
    let f = Fixture::new();
    let plan = f.preview().unwrap();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO agent_data_census VALUES('a0b598f8b007a2b3',1)",
            [],
        )
        .unwrap();
    let before = f.snapshot();
    assert!(f
        .store
        .apply_session_root_repair(&plan)
        .unwrap_err()
        .to_string()
        .contains("preview changed"));
    assert_eq!(f.snapshot(), before);
    let updated = f.preview().unwrap();
    assert_eq!(
        updated.remove_discovery_rows,
        vec![DiscoveryRemoval {
            table: "agent_data_census".into(),
            address: "a0b598f8b007a2b3".into(),
        }]
    );
    f.store.apply_session_root_repair(&updated).unwrap();
    assert!(f.preview().unwrap().remove_discovery_rows.is_empty());
}

#[test]
fn audit_streams_missing_and_forked_refs_with_matching_alternates_read_only() {
    for stub in [false, true] {
        let fixture = Fixture::new();
        fixture.seed_orphaned_projection(false);
        let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
        if stub {
            drop(wal.open(&fixture.stale).unwrap());
        }
        let before = fixture.snapshot();
        let real = std::fs::read(fixture.wal_path()).unwrap();
        let stale = std::fs::read(wal.wal_path(&fixture.stale)).ok();
        let mut findings = Vec::new();
        assert_eq!(
            fixture
                .store
                .audit_session_refs(|finding| {
                    findings.push(serde_json::to_value(finding)?);
                    Ok(())
                })
                .unwrap(),
            1
        );
        assert_eq!(findings[0]["identity"]["session"], fixture.stale.session);
        assert_eq!(
            findings[0]["state"],
            if stub { "forked_stub" } else { "missing" }
        );
        assert_eq!(findings[0]["alternatives"].as_array().unwrap().len(), 1);
        assert_eq!(
            findings[0]["alternatives"][0]["address"],
            session_addr(&fixture.live)
        );
        assert_eq!(fixture.snapshot(), before);
        assert_eq!(std::fs::read(fixture.wal_path()).unwrap(), real);
        assert_eq!(std::fs::read(wal.wal_path(&fixture.stale)).ok(), stale);
        assert!(!fixture.root.join("leases").exists());
    }
}
