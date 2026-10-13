//! Shared materialized-store conformance tests.
//!
//! The functions in this module are generic over the stable `SessionStore` trait so
//! every backend is checked against the same WAL projection invariants. The module
//! lives in `broca-store` behind a test-only feature because the SQLite crate already
//! owns the reference tests and all required test fixtures; a separate dev crate
//! would add another workspace member without reducing coupling.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{
    effective_context, project_control, read_session_page, replay, AdmissionCheckpointV1,
    ChargeBasis, Cursor, ExportCursor, ExportError, ExportIndex, MaterializedStore, Persister,
    RunIndex, RunIndexRow, RunIndexState, SessionStore, StorePageSnapshot, WalStore,
};
use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    ids::{BatchId, MessageId, ToolCallId},
    request::{BudgetConfig, BudgetKind, BudgetNudge, ModelHandle},
    BindIdentity, FinishReason, RunId, StepId, Usage,
};
use broca_wal::{
    AuthSelection, DispatchOutcome, FramedRecord, RunConfig, RunOrigin, SeedOriginKind, Wal,
    WalByteAnchor, WalError, WalRangeRead, WalRecord,
};

pub mod migration_order;

mod attribution;
mod billing_floor;
mod compaction;
pub use attribution::{
    imported_and_pre_run_id_messages_omit_run, interrupted_tool_result_is_final,
    rollback_rows_require_attribution_backfill, run_attribution_follows_each_episode,
    run_attribution_waits_for_terminal,
};
pub use billing_floor::{
    billing_floor_blocks_emission, billing_floor_excludes_backfill,
    billing_floor_preserves_local_and_new_runs,
};
pub use compaction::{
    absent_compaction_checkpoint_refolds_complete_wal,
    model_pages_return_atomic_compaction_and_rows,
};

pub(crate) mod deferred;
pub use deferred::{
    deferred_projection_matches_replay_golden,
    retained_deferred_projection_keeps_only_withdrawal_flags,
    withdrawal_projection_excludes_answered_calls,
};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageUsageBackfillCounts {
    pub scanned: u64,
    pub converted: u64,
    pub empty: u64,
    pub declined: u64,
}

/// Test-only controls for exercising page metadata states that normal new writes cannot create.
pub trait PageUsageConformance: SessionStore {
    fn erase_attribution_for_conformance(
        &self,
        session: &BindIdentity,
        upgraded: bool,
    ) -> Result<(), String>;
    fn reset_projection_for_conformance(&self, session: &BindIdentity) -> Result<(), String>;
    fn replace_admission_checkpoint_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint: &AdmissionCheckpointV1,
    ) -> Result<(), String>;

    fn replace_admission_checkpoint_json_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint_json: Option<&str>,
    ) -> Result<(), String>;

    fn clear_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<(), String>;

    fn stored_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<String>, String>;

    fn backfill_page_usage_for_conformance(&self) -> Result<PageUsageBackfillCounts, String>;
}

/// DELIBERATELY NARROWER than the crate's struct of the same name, which also carries
/// `declined_no_wal`.
///
/// That field splits the permanent cause out of `declined` by asking the WAL store whether
/// a session's log EXISTS, and a store that has no filesystem to consult cannot answer --
/// `WalStore::wal_present` returns `None` for exactly that reason. This struct is the
/// contract an EXTERNAL store implementor satisfies, so requiring the field here would
/// oblige every implementor to produce a number it may have no way to compute, and the
/// honest ones would return zero, which reads as "none were permanent" rather than "I
/// cannot tell".
///
/// So the two are not drifted and should not be reconciled by adding the field here. If a
/// future external store CAN answer, the right move is widening `wal_present`'s contract,
/// not widening this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControlHeadRepairCounts {
    pub scanned: u64,
    pub repaired: u64,
    pub declined: u64,
}

/// Test-only controls for constructing legacy control-head metadata states.
pub trait ControlHeadConformance: SessionStore {
    fn clear_control_head_for_conformance(&self, session: &BindIdentity) -> Result<(), String>;

    fn stored_control_head_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<Cursor>, String>;

    fn repair_control_heads_for_conformance(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, String>;
}

impl PageUsageConformance for crate::SqliteStore {
    fn erase_attribution_for_conformance(
        &self,
        session: &BindIdentity,
        upgraded: bool,
    ) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE meta SET run_attribution_version=?2 WHERE session=?1",
            rusqlite::params![key, i64::from(upgraded)],
        )
        .map_err(|e| e.to_string())?;
        // An old-format INSERT OR REPLACE names no attribution column and restores SQL NULL.
        tx.execute("INSERT OR REPLACE INTO message(session, ord, json) SELECT session, ord, json FROM message WHERE session=?1 AND ord=(SELECT MAX(ord) FROM message WHERE session=?1)", [&key]).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    fn reset_projection_for_conformance(&self, session: &BindIdentity) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM message WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM meta WHERE session=?1", [&key])
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    fn replace_admission_checkpoint_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint: &AdmissionCheckpointV1,
    ) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let checkpoint_json = broca_core::encode_admission_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        let changed = conn
            .execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                rusqlite::params![key, checkpoint_json],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn replace_admission_checkpoint_json_for_conformance(
        &self,
        session: &BindIdentity,
        checkpoint_json: Option<&str>,
    ) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        let changed = conn
            .execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                rusqlite::params![key, checkpoint_json],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn clear_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        let changed = conn
            .execute(
                "UPDATE meta SET last_episode_usage_json=NULL WHERE session=?1",
                [key],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn stored_last_episode_usage_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<String>, String> {
        let key = crate::SqliteStore::key(session);
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        conn.query_row(
            "SELECT last_episode_usage_json FROM meta WHERE session=?1",
            [key],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())
    }

    fn backfill_page_usage_for_conformance(&self) -> Result<PageUsageBackfillCounts, String> {
        crate::SqliteStore::backfill_page_usage(self)
            .map(|counts| PageUsageBackfillCounts {
                scanned: counts.scanned,
                converted: counts.converted,
                empty: counts.empty,
                declined: counts.declined,
            })
            .map_err(|error| error.to_string())
    }
}

impl ControlHeadConformance for crate::SqliteStore {
    fn clear_control_head_for_conformance(&self, session: &BindIdentity) -> Result<(), String> {
        let key = crate::SqliteStore::key(session);
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        let changed = conn
            .execute(
                "UPDATE meta
                    SET control_head_wal_seq=NULL, control_head_sub_index=NULL
                  WHERE session=?1",
                [key],
            )
            .map_err(|error| error.to_string())?;
        (changed == 1)
            .then_some(())
            .ok_or_else(|| "conformance session metadata is missing".to_string())
    }

    fn stored_control_head_for_conformance(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<Cursor>, String> {
        let key = crate::SqliteStore::key(session);
        let conn = self.conn.lock().map_err(|error| error.to_string())?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|error| error.to_string())?;
        let head = crate::current_control_head(&tx, &key).map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(head)
    }

    fn repair_control_heads_for_conformance(
        &self,
        wal_store: &dyn WalStore,
    ) -> Result<ControlHeadRepairCounts, String> {
        crate::SqliteStore::repair_control_heads(self, wal_store)
            .map(|counts| ControlHeadRepairCounts {
                scanned: counts.scanned,
                repaired: counts.repaired,
                declined: counts.declined,
            })
            .map_err(|error| error.to_string())
    }
}

/// Run the complete materialized-store conformance suite against fresh stores.
pub fn assert_conformance<S, F>(make_store: F)
where
    S: SessionStore + PageUsageConformance + ControlHeadConformance + RunIndex,
    F: Fn() -> S,
{
    incremental_projection_resumes_from_watermark(make_store());
    run_attribution_follows_each_episode(make_store());
    run_attribution_waits_for_terminal(make_store());
    interrupted_tool_result_is_final(make_store());
    imported_and_pre_run_id_messages_omit_run(make_store());
    rollback_rows_require_attribution_backfill(make_store());
    re_projecting_same_wal_is_idempotent(make_store());
    uncommitted_tail_is_not_materialized_but_merge_recovers_it(make_store());
    projection_boundary_keeps_terminal_record_in_tail(make_store());
    interrupted_import_projection_is_replaced_on_repair(make_store());
    paged_reads_return_atomic_meta_and_rows(make_store());
    backfill_page_usage_preserves_read_semantics(make_store());
    repair_control_heads_preserves_page_semantics(make_store());
    paged_reads_distinguish_null_usage_from_unpopulated_metadata(make_store());
    paged_reads_preserve_trailing_origin(make_store());
    wal_anchor_roundtrip_and_stale_filtering(make_store());
    incremental_projection_matches_full_prefix_corpus(make_store());
    budgeted_incremental_projection_matches_full_prefix(make_store());
    projection_retention_policy_survives_suffixes_and_full_prefix_repair(make_store());
    retained_deferred_projection_keeps_only_withdrawal_flags(make_store());
    admission_checkpoint_empty_roundtrip_and_incremental_equivalence(make_store());
    projection_range_co_commits_anchor(make_store());
    failed_projection_keeps_checkpoint_and_messages_atomic(make_store());
    model_pages_return_atomic_compaction_and_rows(make_store());
    absent_compaction_checkpoint_refolds_complete_wal(make_store());
    projection_keeps_the_sessions_newest_title(make_store(), make_store());
    deferred_projection_matches_replay_golden(make_store());
    withdrawal_projection_excludes_answered_calls(make_store());
}

/// Run the durable run-index conformance suite against a fresh store. Separate from the
/// SessionStore suite because RunIndex is a distinct trait (a store may impl one without
/// the other), but both backends (SQLite + Postgres) route through this so the index
/// behaves identically.
pub fn assert_run_index_conformance<S, F>(make_store: F)
where
    S: RunIndex,
    F: Fn() -> S,
{
    run_index_active_then_terminal_roundtrip(make_store());
    run_index_lookup_miss_is_none(make_store());
    run_index_active_is_idempotent_and_terminal_wins(make_store());
    run_index_title_round_trips_and_the_first_title_wins(make_store());
}

/// Run the usage-export conformance suite against a fresh store. This is intentionally small
/// but exercises the shared epoch, cursor, immutable-fact, and idempotent-insert contract for
/// every backend.
pub fn assert_export_conformance<S, F>(make_store: F)
where
    S: ExportIndex + RunIndex,
    F: Fn() -> S,
{
    let store = make_store();
    let session = session("export-conformance");
    let run_id = RunId("r".into());
    let mut records = base_records(&session);
    records.push(framed(
        1,
        WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ts_ms: Some(2),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    store
        .emit_run(&records, &run_id, &resolver)
        .expect("usage export emission");
    store
        .emit_run(&records, &run_id, &resolver)
        .expect("repeated usage export emission");
    assert_eq!(store.high_water(), 1);
    let page = store.read_page(None, 1000).expect("usage export page read");
    assert_eq!(page.source_epoch, 1);
    assert_eq!(page.high_water, 1);
    assert_eq!(page.facts.len(), 1);
    assert_eq!(page.facts[0].export_seq, 1);
    assert_eq!(page.facts[0].segment.run_id, run_id);
    assert_eq!(
        page.next,
        Some(ExportCursor {
            source_instance_id: page.source_instance_id.clone(),
            source_epoch: 1,
            export_seq: 1
        })
    );
    let exhausted = store
        .read_page(page.next.clone(), 1000)
        .expect("same-source cursor page read");
    assert!(exhausted.facts.is_empty());
    assert_eq!(exhausted.source_instance_id, page.source_instance_id);

    let replaced_source = store
        .read_page(
            Some(ExportCursor {
                source_instance_id: "replaced-export-source".into(),
                source_epoch: page.source_epoch,
                export_seq: 0,
            }),
            1000,
        )
        .expect_err("a cursor from another source instance must reset");
    assert_eq!(
        replaced_source,
        ExportError::EpochChanged {
            source_instance_id: page.source_instance_id.clone(),
            source_epoch: page.source_epoch,
            high_water: page.high_water,
        }
    );

    let pending = RunIndexRow {
        withdrawals_owed: false,
        title: None,
        run_id: "pending-export".into(),
        session: session.clone(),
        submission_id: None,
        request_identity_digest: None,
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    store.index_run_active(&pending).expect("index pending run");
    // Membership checks, not global emptiness: shared-database backends (live Postgres) keep
    // terminal rows from earlier conformance sections, which are legitimate catch-up work.
    assert!(
        !store
            .missing_terminal_runs()
            .expect("query active run")
            .iter()
            .any(|missing| missing.run_id.0 == pending.run_id),
        "active rows are not export catch-up work"
    );
    store
        .index_run_terminal(
            &pending.run_id,
            RunIndexState::Completed,
            Some(Usage::default()),
            None,
            0,
        )
        .expect("finish pending run");
    let expected = broca_core::MissingExportRun {
        run_id: RunId(pending.run_id),
        session,
    };
    assert!(
        store
            .missing_terminal_runs()
            .expect("query terminal run")
            .contains(&expected),
        "a terminal row without facts is catch-up work"
    );

    export_account_ref_round_trips_and_legacy_absent(make_store());
}

/// A run frozen with a vault `account_ref` round-trips it through the export fact verbatim, and a
/// run with no vault account stores the fact WITHOUT an `account_ref` key (the legacy/unlabeled
/// shape) that reads back as `None` — identically on every backend.
pub fn export_account_ref_round_trips_and_legacy_absent<S>(store: S)
where
    S: ExportIndex + RunIndex,
{
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);

    // Stamped: the frozen vault account_id rides the fact verbatim.
    let stamped_session = session("export-account-ref");
    let stamped_run = RunId("acct-stamped".into());
    let stamped =
        export_records_with_account(&stamped_session, &stamped_run, Some("acct-uuid-321"));
    store
        .emit_run(&stamped, &stamped_run, &resolver)
        .expect("emit stamped fact");
    let page = store.read_page(None, 1000).expect("read stamped page");
    let stamped_fact = page
        .facts
        .iter()
        .find(|fact| fact.segment.run_id == stamped_run)
        .expect("stamped fact present");
    assert_eq!(
        stamped_fact.segment.account_ref.as_deref(),
        Some("acct-uuid-321"),
        "the frozen vault account_id round-trips verbatim through the export fact"
    );

    // Unlabeled: a credential with no vault account stores no account_ref key, reading back None.
    let bare_session = session("export-account-absent");
    let bare_run = RunId("acct-bare".into());
    let bare = export_records_with_account(&bare_session, &bare_run, None);
    store
        .emit_run(&bare, &bare_run, &resolver)
        .expect("emit bare fact");
    let page = store.read_page(None, 1000).expect("read bare page");
    let bare_fact = page
        .facts
        .iter()
        .find(|fact| fact.segment.run_id == bare_run)
        .expect("bare fact present");
    assert_eq!(
        bare_fact.segment.account_ref, None,
        "a run with no vault account reads back with account_ref absent"
    );
    // The stored shape omits the key entirely (legacy rows stay unlabeled, never null/empty):
    // re-serializing the read-back segment must not introduce an account_ref member.
    let reserialized = serde_json::to_value(&bare_fact.segment).expect("reserialize bare segment");
    assert!(
        reserialized.get("account_ref").is_none(),
        "an absent account_ref stays omitted from the stored fact shape"
    );
}

/// A terminal run whose frozen `AuthSelection` carries `account_ref` when `account_ref` is
/// `Some`, and a credential with no vault account when it is `None`.
fn export_records_with_account(
    session: &BindIdentity,
    run_id: &RunId,
    account_ref: Option<&str>,
) -> Vec<FramedRecord> {
    let mut start = run_started(0, &run_id.0, session, "hi", ModelHandle::new("stub", "m"));
    if let WalRecord::RunStarted { config, .. } = &mut start.record {
        config.auth_selection = Some(AuthSelection {
            credential_id: "chatgpt:openai".into(),
            account_ref: account_ref.map(str::to_string),
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
    }
    vec![
        start,
        framed(
            1,
            WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: Some(2),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]
}

/// index_run_active records a row resolvable by run_id; index_run_terminal updates its
/// state; the full session triple round-trips (not the lossy flat key).
pub fn run_index_active_then_terminal_roundtrip<S: RunIndex>(store: S) {
    let session = session("run-index-rt");
    let row = RunIndexRow {
        withdrawals_owed: false,
        title: None,
        run_id: "run-1".into(),
        session: session.clone(),
        submission_id: Some("sub-1".into()),
        request_identity_digest: Some("digest-1".into()),
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    store.index_run_active(&row).expect("index active");

    let got = store.lookup_run("run-1").expect("lookup").expect("present");
    assert_eq!(got.session, session, "the full session triple round-trips");
    assert_eq!(got.submission_id, Some("sub-1".to_string()));
    assert_eq!(got.request_identity_digest, Some("digest-1".to_string()));
    assert_eq!(got.state, RunIndexState::Active);

    let terminal_usage = Usage {
        input_tokens: Some(13),
        output_tokens: Some(5),
        ..Usage::default()
    };
    store
        .index_run_terminal(
            "run-1",
            RunIndexState::Completed,
            Some(terminal_usage),
            Some(FinishReason::Length),
            0,
        )
        .expect("index terminal");
    let got = store.lookup_run("run-1").expect("lookup").expect("present");
    assert_eq!(
        got.state,
        RunIndexState::Completed,
        "terminal state updated"
    );
    assert!(got.state.is_terminal());
    assert_eq!(got.usage, Some(terminal_usage));
    assert_eq!(
        got.final_step_finish_reason,
        Some(FinishReason::Length),
        "terminal metadata round-trips with the state"
    );
}

/// A lookup for an unknown run_id is `None` (the caller falls back to the WAL).
pub fn run_index_lookup_miss_is_none<S: RunIndex>(store: S) {
    assert_eq!(
        store.lookup_run("nope").expect("lookup"),
        None,
        "an unknown run_id resolves to None"
    );
}

/// index_run_active on an existing run_id is idempotent, and a terminal already recorded
/// always wins over a later stale Active write for the same run.
pub fn run_index_active_is_idempotent_and_terminal_wins<S: RunIndex>(store: S) {
    let session = session("run-index-idem");
    let row = RunIndexRow {
        withdrawals_owed: false,
        title: None,
        run_id: "run-x".into(),
        session,
        submission_id: None,
        request_identity_digest: Some("digest-x".into()),
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    store.index_run_active(&row).expect("active 1");
    store
        .index_run_active(&row)
        .expect("active 2 (idempotent upsert)");
    store
        .index_run_terminal("run-x", RunIndexState::Interrupted, None, None, 0)
        .expect("terminal without metadata");
    store
        .index_run_active(&row)
        .expect("stale active after terminal is ignored");
    let got = store.lookup_run("run-x").expect("lookup").expect("present");
    assert_eq!(
        got.state,
        RunIndexState::Interrupted,
        "a stale Active write must not reactivate a terminal run"
    );
    assert_eq!(got.usage, None, "legacy terminal usage remains absent");
    assert_eq!(
        got.final_step_finish_reason, None,
        "legacy terminal finish reason remains absent"
    );
    assert_eq!(
        got.request_identity_digest,
        Some("digest-x".to_string()),
        "terminal updates preserve the admission identity"
    );
}

/// The run index keeps the title its admission wrote, and the first title written for a run
/// wins: a later admission of the same run id (a resume, which writes no `RunStarted` and so
/// has no title of its own, or a keyed retry carrying another title) never replaces it, and the
/// terminal update keeps it. A row written without a title reads back with none.
pub fn run_index_title_round_trips_and_the_first_title_wins<S: RunIndex>(store: S) {
    let row = |run_id: &str, title: Option<&str>| RunIndexRow {
        withdrawals_owed: false,
        title: title.map(str::to_owned),
        run_id: run_id.into(),
        session: session("run-index-title"),
        submission_id: None,
        request_identity_digest: Some("digest".into()),
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    let title_of = |run_id: &str| {
        store
            .lookup_run(run_id)
            .expect("lookup")
            .expect("present")
            .title
    };
    store
        .index_run_active(&row("run-titled", Some("Fix the login bug")))
        .expect("index titled");
    assert_eq!(title_of("run-titled").as_deref(), Some("Fix the login bug"));
    store
        .index_run_active(&row("run-titled", Some("An edited title")))
        .expect("re-admit with another title");
    store
        .index_run_active(&row("run-titled", None))
        .expect("re-admit with no title");
    assert_eq!(
        title_of("run-titled").as_deref(),
        Some("Fix the login bug"),
        "the first title written for a run is kept"
    );
    store
        .index_run_terminal("run-titled", RunIndexState::Completed, None, None, 0)
        .expect("terminal");
    assert_eq!(title_of("run-titled").as_deref(), Some("Fix the login bug"));
    store
        .index_run_active(&row("run-untitled", None))
        .expect("index untitled");
    assert_eq!(title_of("run-untitled"), None, "absent stays absent");
}

/// The projection keeps the session's newest caller title from `RunStarted`, `Steered` and
/// `Queued`, newest by WAL position, up to its watermark. A batch that carries no title keeps
/// the one already folded, so a title many turns before the watermark is still read; a titled
/// record past the watermark is not folded yet (a reader overlays it from the WAL tail); and a
/// store rebuilt by projecting the WAL from the start reaches the same title.
pub fn projection_keeps_the_sessions_newest_title<S>(store: S, rebuilt: S)
where
    S: SessionStore,
{
    let s = session("title");
    let title_of = |store: &S| {
        MaterializedStore::read_page_with_watermark(
            store,
            &s,
            None,
            1,
            broca_core::context::ReadView::Raw,
        )
        .expect("page read")
        .last_title
    };
    let mut first = run_started(1, "r1", &s, "hi", ModelHandle::new("stub", "m"));
    if let WalRecord::RunStarted { title, .. } = &mut first.record {
        *title = Some("Old title".into());
    }
    let mut recs = vec![first];
    recs.extend(text_turn(2, 1, "first"));
    assert_eq!(Persister::project(&store, &s, &recs).expect("project"), 3);
    assert_eq!(title_of(&store).as_deref(), Some("Old title"));

    // Several untitled turns later, projected as suffixes that carry no title themselves.
    for step in 2..=4 {
        let seq = recs.last().unwrap().seq + 1;
        let turn = text_turn(seq, step, "more");
        recs.extend(turn.clone());
        Persister::project(&store, &s, &turn).expect("project suffix");
    }
    assert_eq!(
        title_of(&store).as_deref(),
        Some("Old title"),
        "a title many turns before the watermark is still read"
    );

    // A steer's title, folded once the turn that renders the steer reaches a boundary.
    let seq = recs.last().unwrap().seq + 1;
    let mut suffix = vec![
        framed(
            seq,
            WalRecord::Steered {
                retention: None,
                ts_ms: None,
                send_id: None,
                request_identity_digest: None,
                delivery: Some(broca_protocol::Delivery::Steer),
                title: Some("Steer title".into()),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "steered".into(),
                    }],
                )]
                .into(),
                hooks: None,
            },
        ),
        framed(seq + 1, WalRecord::StepStarted { step_id: StepId(5) }),
    ];
    suffix.extend(text_turn(seq + 2, 5, "after the steer"));
    recs.extend(suffix.clone());
    Persister::project(&store, &s, &suffix).expect("project the steer");
    assert_eq!(title_of(&store).as_deref(), Some("Steer title"));

    // A queued send's title past the last boundary is not folded yet.
    let seq = recs.last().unwrap().seq + 1;
    let queued: WalRecord = serde_json::from_value(serde_json::json!({
        "type": "queued", "submission_id": "sub-r2", "title": "Queued title",
        "request": {"model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [{"role": "user", "content": [{"type": "text", "text": "next"}]}],
            "policy_allowed": [], "tool_choice": {"type": "auto"}, "generation": {}, "stop_when": []}
    }))
    .expect("queued record");
    recs.push(framed(seq, queued));
    Persister::project(&store, &s, &recs[recs.len() - 1..]).expect("project the queued send");
    assert_eq!(
        title_of(&store).as_deref(),
        Some("Steer title"),
        "a title past the watermark waits in the WAL tail"
    );

    // Rebuilt from the WAL in one pass, the store reaches the same title.
    Persister::project(&rebuilt, &s, &recs).expect("rebuild from the WAL");
    assert_eq!(title_of(&rebuilt).as_deref(), Some("Steer title"));

    // A session with no titled send has no title.
    let untitled = session("untitled");
    let mut plain = base_records(&untitled);
    plain.extend(text_turn(2, 1, "plain"));
    Persister::project(&store, &untitled, &plain).expect("project untitled");
    assert_eq!(
        MaterializedStore::read_page_with_watermark(
            &store,
            &untitled,
            None,
            1,
            broca_core::context::ReadView::Raw
        )
        .expect("page read")
        .last_title,
        None
    );
}

/// Atomic page reads return the projection metadata and only the requested ordinal rows.
pub fn paged_reads_return_atomic_meta_and_rows<S>(store: S)
where
    S: SessionStore,
{
    let s = session("paged");
    let empty = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        Some(0),
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("empty page read succeeds");
    assert_eq!(empty.watermark_seq, 0);
    assert_eq!(empty.prefix_len, 0);
    assert!(empty.rows.is_empty());

    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));
    Persister::project(&store, &s, &recs).expect("projection succeeds");

    let first = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        Some(0),
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("first page read succeeds");
    assert_eq!(first.watermark_seq, 3);
    assert_eq!(
        first.control_head,
        project_control(&recs)
            .into_iter()
            .filter(|(cursor, _)| cursor.wal_seq <= first.watermark_seq)
            .map(|(cursor, _)| cursor)
            .next_back()
    );
    assert_eq!(first.prefix_len, 2);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].ordinal, 0);
    assert_eq!(first.rows[0].message.ordinal, Some(0));
    assert_eq!(first.last_run_id, Some(RunId("r".into())));
    assert_eq!(
        first.trailing_origin,
        Some(ModelHandle::new("stub", "m")),
        "tail replay seed is captured with the page rows"
    );

    let newest = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        None,
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("newest page read succeeds");
    assert_eq!(newest.prefix_len, 2);
    assert_eq!(newest.rows.len(), 1);
    assert_eq!(newest.rows[0].ordinal, 1);

    recs.extend(text_turn(4, 2, "second"));
    Persister::project(&store, &s, &recs).expect("incremental projection succeeds");
    let extended = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        None,
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("extended page read succeeds");
    assert_eq!(extended.prefix_len, 3);
    assert_eq!(extended.rows[0].ordinal, 2);
    assert_store_state_matches_full_projection(&store, &s, &recs);
}

/// Legacy page-usage rows are converted only when their checkpoints prove the value, while
/// declined rows retain the checkpoint fallback and all result categories remain auditable.
pub fn backfill_page_usage_preserves_read_semantics<S>(store: S)
where
    S: PageUsageConformance,
{
    // Live Postgres conformance databases may retain deliberately declined rows from an earlier
    // run. Establish their count before creating this run's five cases so the assertions remain
    // exact without weakening the production operation to accept a test-only session filter.
    let baseline = store
        .backfill_page_usage_for_conformance()
        .expect("baseline page-usage backfill succeeds");
    assert_eq!(
        baseline.scanned,
        baseline.converted + baseline.empty + baseline.declined,
        "baseline counts cover every scanned row"
    );

    let expected_usage = Usage {
        input_tokens: Some(144),
        output_tokens: Some(21),
        ..Usage::default()
    };
    let value_session = session("page-usage-backfill-value");
    let empty_session = session("page-usage-backfill-empty");
    let malformed_session = session("page-usage-backfill-malformed");
    let missing_session = session("page-usage-backfill-missing");
    let mismatch_session = session("page-usage-backfill-mismatch");

    project_checkpoint_usage(&store, &value_session, expected_usage);
    let malformed_checkpoint = project_checkpoint_usage(&store, &malformed_session, expected_usage);
    let missing_checkpoint = project_checkpoint_usage(&store, &missing_session, expected_usage);
    let mismatch_checkpoint = project_checkpoint_usage(&store, &mismatch_session, expected_usage);

    let mut empty_records = base_records(&empty_session);
    empty_records.extend(text_turn(2, 1, "usage not reported yet"));
    Persister::project(&store, &empty_session, &empty_records)
        .expect("usage-free episode projection succeeds");
    let empty_checkpoint = MaterializedStore::read_admission_snapshot(&store, &empty_session)
        .expect("usage-free admission read succeeds")
        .expect("usage-free row has admission metadata")
        .checkpoint
        .expect("usage-free row has a checkpoint");
    assert_eq!(
        empty_checkpoint
            .last_episode
            .as_ref()
            .expect("usage-free checkpoint retains its episode")
            .usage,
        None
    );

    for legacy_session in [
        &value_session,
        &empty_session,
        &malformed_session,
        &missing_session,
        &mismatch_session,
    ] {
        store
            .clear_last_episode_usage_for_conformance(legacy_session)
            .expect("simulate a legacy SQL NULL page-usage value");
    }
    store
        .replace_admission_checkpoint_json_for_conformance(
            &malformed_session,
            Some("not a checkpoint"),
        )
        .expect("install malformed checkpoint");
    store
        .replace_admission_checkpoint_json_for_conformance(&missing_session, None)
        .expect("remove checkpoint");
    let mut mismatched_checkpoint = mismatch_checkpoint.clone();
    mismatched_checkpoint
        .last_episode
        .as_mut()
        .expect("terminal checkpoint retains its episode")
        .run_id = RunId("a-different-run".into());
    store
        .replace_admission_checkpoint_for_conformance(&mismatch_session, &mismatched_checkpoint)
        .expect("install mismatched checkpoint");

    let page_usage = |session| {
        MaterializedStore::read_page_with_watermark(
            &store,
            session,
            Some(0),
            10,
            broca_core::context::ReadView::Raw,
        )
        .expect("page metadata read succeeds")
        .last_episode_usage
    };
    let before_value = page_usage(&value_session);
    assert_eq!(before_value, Some(expected_usage));
    assert_eq!(
        page_usage(&mismatch_session),
        None,
        "the legacy fallback refuses usage from a different run"
    );

    let counts = store
        .backfill_page_usage_for_conformance()
        .expect("page-usage backfill succeeds");
    assert_eq!(
        counts.scanned,
        counts.converted + counts.empty + counts.declined,
        "every scanned row has exactly one outcome"
    );
    assert_eq!(counts.converted, 1, "one checkpoint carries usage");
    assert_eq!(counts.empty, 1, "one checkpoint carries no usage");
    assert_eq!(
        counts.declined,
        baseline.declined + 3,
        "missing, malformed, and run-id-mismatched checkpoints are all declined"
    );

    assert_eq!(
        store
            .stored_last_episode_usage_for_conformance(&value_session)
            .expect("read stored value"),
        Some(serde_json::to_string(&Some(expected_usage)).expect("serialize expected usage"))
    );
    assert_eq!(
        store
            .stored_last_episode_usage_for_conformance(&empty_session)
            .expect("read stored empty value"),
        Some("null".into()),
        "a decoded matching episode with no usage gets explicit JSON null"
    );
    for declined_session in [&malformed_session, &missing_session, &mismatch_session] {
        assert_eq!(
            store
                .stored_last_episode_usage_for_conformance(declined_session)
                .expect("read declined storage state"),
            None,
            "declined rows must remain SQL NULL"
        );
    }

    assert_eq!(
        page_usage(&value_session),
        before_value,
        "backfilling a value is observationally invisible to page reads"
    );
    assert_eq!(
        page_usage(&mismatch_session),
        None,
        "a declined mismatch still takes the filtered checkpoint fallback"
    );

    let conflicting_usage = Usage {
        input_tokens: Some(999),
        output_tokens: Some(888),
        ..Usage::default()
    };
    let mut conflicting_empty_checkpoint = empty_checkpoint;
    conflicting_empty_checkpoint
        .last_episode
        .as_mut()
        .expect("usage-free checkpoint retains its episode")
        .usage = Some(conflicting_usage);
    store
        .replace_admission_checkpoint_for_conformance(&empty_session, &conflicting_empty_checkpoint)
        .expect("replace usage-free checkpoint after backfill");
    assert_eq!(
        page_usage(&empty_session),
        None,
        "explicit JSON null prevents a later checkpoint fallback"
    );

    for (declined_session, original_checkpoint) in [
        (&malformed_session, &malformed_checkpoint),
        (&missing_session, &missing_checkpoint),
        (&mismatch_session, &mismatch_checkpoint),
    ] {
        store
            .replace_admission_checkpoint_for_conformance(declined_session, original_checkpoint)
            .expect("restore declined checkpoint");
        assert_eq!(
            page_usage(declined_session),
            Some(expected_usage),
            "restoring a declined row's checkpoint proves its SQL NULL fallback remains active"
        );
    }
}

/// In-memory WAL backing for the conformance suite.
///
/// THIS SHIPS ON PURPOSE, despite the name reading as test-only. The whole module is public API:
/// `assert_conformance` is how a downstream store implementor proves a new backend against the
/// real suite rather than a copy of it, so the harness has to be present in the compiled library
/// and cannot live behind `cfg(test)`.
///
/// Named here because a type that looks like test residue invites deletion during cleanup, and a
/// name-based sweep can mislabel it in either direction — as gated when it is not, or as dead
/// when it is load-bearing for anyone outside this crate.
#[derive(Default)]
struct ConformanceWalStore {
    records: HashMap<BindIdentity, Vec<FramedRecord>>,
}

impl ConformanceWalStore {
    fn with_records(session: BindIdentity, records: Vec<FramedRecord>) -> Self {
        Self {
            records: HashMap::from([(session, records)]),
        }
    }
}

impl WalStore for ConformanceWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Err(WalError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "conformance WAL store is read-only",
        )))
    }

    fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        self.records.get(session).cloned().ok_or_else(|| {
            WalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "conformance WAL is missing",
            ))
        })
    }
}

/// Legacy control-head rows are repaired only from a derivable WAL cursor, without changing the
/// messages or lineage state returned by the full-WAL fallback.
pub fn repair_control_heads_preserves_page_semantics<S>(store: S)
where
    S: ControlHeadConformance,
{
    let baseline = store
        .repair_control_heads_for_conformance(&ConformanceWalStore::default())
        .expect("baseline control-head repair succeeds");
    assert_eq!(baseline.repaired, 0);
    assert_eq!(baseline.scanned, baseline.declined);

    let repaired_session = session("control-head-repair");
    let missing_session = session("control-head-missing-wal");
    let mut repaired_records = base_records(&repaired_session);
    repaired_records.extend(text_turn(2, 1, "repairable"));
    repaired_records.push(framed(
        4,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let mut missing_records = base_records(&missing_session);
    missing_records.extend(text_turn(2, 1, "must keep fallback"));
    missing_records.push(framed(
        4,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));

    Persister::project(&store, &repaired_session, &repaired_records)
        .expect("repairable projection succeeds");
    Persister::project(&store, &missing_session, &missing_records)
        .expect("missing-WAL projection succeeds");
    let projector_head = store
        .stored_control_head_for_conformance(&repaired_session)
        .expect("read projector control head")
        .expect("projector derives a control head");
    store
        .clear_control_head_for_conformance(&repaired_session)
        .expect("simulate repairable legacy row");
    store
        .clear_control_head_for_conformance(&missing_session)
        .expect("simulate missing-WAL legacy row");

    let read_page = |session: &BindIdentity, records: &[FramedRecord]| {
        match MaterializedStore::read_page_with_watermark(
            &store,
            session,
            Some(0),
            10,
            broca_core::context::ReadView::Raw,
        ) {
            Ok(snapshot) => {
                let tail: Vec<_> = records
                    .iter()
                    .filter(|record| record.seq > snapshot.watermark_seq)
                    .cloned()
                    .collect();
                (
                    read_session_page(&snapshot, &tail, Some(0), Some(10))
                        .expect("assemble store-backed page"),
                    false,
                )
            }
            Err(error) => {
                assert!(
                    error.contains("store page metadata predates control-head tracking"),
                    "only missing control-head metadata should trigger this fallback: {error}"
                );
                (
                    read_session_page(&StorePageSnapshot::empty(), records, Some(0), Some(10))
                        .expect("assemble full-WAL fallback page"),
                    true,
                )
            }
        }
    };

    let (before, used_fallback_before) = read_page(&repaired_session, &repaired_records);
    assert!(used_fallback_before, "legacy row uses full-WAL fallback");
    let repair_wals =
        ConformanceWalStore::with_records(repaired_session.clone(), repaired_records.clone());
    let counts = store
        .repair_control_heads_for_conformance(&repair_wals)
        .expect("control-head repair succeeds");

    assert_eq!(
        store
            .stored_control_head_for_conformance(&missing_session)
            .expect("read declined control head"),
        None,
        "a row with no WAL must remain untouched"
    );
    assert_eq!(counts.repaired, 1, "only the derivable cursor is written");
    assert_eq!(counts.declined, baseline.declined + 1);
    assert_eq!(
        counts.scanned,
        counts.repaired + counts.declined,
        "every scanned row has exactly one outcome"
    );
    assert_eq!(
        store
            .stored_control_head_for_conformance(&repaired_session)
            .expect("read repaired control head"),
        Some(projector_head),
        "repair derives the exact cursor previously written by the projector"
    );

    let (after, used_fallback_after) = read_page(&repaired_session, &repaired_records);
    assert!(!used_fallback_after, "repaired row uses the paged path");
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.lineage_state, before.lineage_state);
    assert_eq!(after.head, before.head);

    let (_, missing_still_uses_fallback) = read_page(&missing_session, &missing_records);
    assert!(
        missing_still_uses_fallback,
        "a declined missing-WAL row must keep the loud full-WAL fallback"
    );
}

/// Page usage metadata preserves JSON `null` as a populated value and reserves SQL `NULL`
/// for rows written before the metadata column existed.
pub fn paged_reads_distinguish_null_usage_from_unpopulated_metadata<S>(store: S)
where
    S: PageUsageConformance,
{
    let session = session("page-usage");
    let mut records = base_records(&session);
    records.extend(text_turn(2, 1, "first"));
    Persister::project(&store, &session, &records).expect("projection succeeds");

    let pause_tail = [framed(
        4,
        WalRecord::RunPaused {
            run_id: RunId("r".into()),
            ts_ms: None,
            reason: PauseReason::AuthRequired,
            error: None,
        },
    )];
    let read_usage = || {
        let snapshot = MaterializedStore::read_page_with_watermark(
            &store,
            &session,
            Some(0),
            10,
            broca_core::context::ReadView::Raw,
        )
        .expect("page metadata read succeeds");
        read_session_page(&snapshot, &pause_tail, Some(0), Some(10))
            .expect("page assembly succeeds")
            .lineage_state
            .usage
    };
    assert_eq!(
        read_usage(),
        None,
        "a newly projected running episode reports no terminal usage"
    );

    let mut conflicting_checkpoint = MaterializedStore::read_admission_snapshot(&store, &session)
        .expect("admission metadata read succeeds")
        .expect("projected row has admission metadata")
        .checkpoint
        .expect("projected row has a checkpoint");
    let conflicting_usage = Usage {
        input_tokens: Some(91),
        output_tokens: Some(7),
        ..Default::default()
    };
    conflicting_checkpoint
        .last_episode
        .as_mut()
        .expect("checkpoint retains the episode")
        .usage = Some(conflicting_usage);
    store
        .replace_admission_checkpoint_for_conformance(&session, &conflicting_checkpoint)
        .expect("replace checkpoint without changing page usage metadata");
    assert_eq!(
        read_usage(),
        None,
        "JSON null is populated metadata, so page reads must not fall back to the checkpoint"
    );

    store
        .clear_last_episode_usage_for_conformance(&session)
        .expect("simulate a row written before the page usage column existed");
    assert_eq!(
        read_usage(),
        Some(conflicting_usage),
        "SQL NULL deliberately exercises the checkpoint fallback"
    );
}

/// Page reads preserve the projection-boundary origin for every page shape.
pub fn paged_reads_preserve_trailing_origin<S>(store: S)
where
    S: SessionStore,
{
    let s = session("page-tail-origin");
    let expected_origin = ModelHandle::new("stub", "m");
    let mut records = base_records(&s);
    records.extend(text_turn(2, 1, "first"));
    records.extend(tool_turn(4, 2));
    Persister::project(&store, &s, &records).expect("projection succeeds");

    let tail_pair = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        None,
        2,
        broca_core::context::ReadView::Raw,
    )
    .expect("newest tail page read succeeds");
    assert_eq!(tail_pair.rows.len(), 2);
    assert_eq!(tail_pair.rows[0].message.role, Role::Assistant);
    assert_eq!(tail_pair.rows[1].message.role, Role::Tool);
    assert_eq!(
        tail_pair.trailing_origin,
        Some(expected_origin.clone()),
        "the newest page scans backward past its tool result"
    );

    let older = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        Some(0),
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("older page read succeeds");
    assert_eq!(older.rows[0].ordinal, 0);
    assert_eq!(
        older.trailing_origin,
        Some(expected_origin.clone()),
        "an older page reads the producing origin from the real transcript tail"
    );

    let tool_tail = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        Some(tail_pair.prefix_len - 1),
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("tool-only tail page read succeeds");
    assert_eq!(tool_tail.rows[0].message.role, Role::Tool);
    assert_eq!(
        tool_tail.trailing_origin,
        Some(expected_origin),
        "a tail page without an origin falls back to the final two transcript rows"
    );

    let no_origin_session = session("page-tail-no-origin");
    let mut no_origin_records = vec![run_started(
        0,
        "import",
        &no_origin_session,
        "imported input",
        ModelHandle::new("import", "model"),
    )];
    let WalRecord::RunStarted { origin, .. } = &mut no_origin_records[0].record else {
        unreachable!("fixture starts a run");
    };
    *origin = RunOrigin::SeededFrom {
        origin_kind: SeedOriginKind::Import,
        source_ref: "fixture:no-origin".into(),
        origin_device_pseudonym: None,
        base_seq: None,
    };
    no_origin_records.push(framed(
        1,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    Persister::project(&store, &no_origin_session, &no_origin_records)
        .expect("origin-free projection succeeds");
    let no_origin = MaterializedStore::read_page_with_watermark(
        &store,
        &no_origin_session,
        None,
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("origin-free tail page read succeeds");
    assert!(no_origin
        .rows
        .iter()
        .all(|row| row.message.origin.is_none()));
    assert_eq!(
        no_origin.trailing_origin, None,
        "a transcript with no message origin has no replay seed"
    );
}

/// WAL anchors round-trip only while they identify the store's current watermark.
pub fn wal_anchor_roundtrip_and_stale_filtering<S>(store: S)
where
    S: SessionStore,
{
    let s = session("wal-anchor");
    let mut records = base_records(&s);
    records.extend(text_turn(2, 1, "first"));
    assert_eq!(Persister::project(&store, &s, &records).unwrap(), 3);

    let seed = Persister::projection_seed(&store, &s)
        .expect("seed read succeeds")
        .expect("projection produces a seed");
    assert_eq!(seed.anchor, None, "NULL anchor metadata is optional");
    let page = MaterializedStore::read_page_with_watermark(
        &store,
        &s,
        None,
        1,
        broca_core::context::ReadView::Raw,
    )
    .expect("page read succeeds");
    assert_eq!(page.anchor, None);

    let first_anchor = WalByteAnchor {
        file_id: u64::MAX,
        seq: 3,
        byte_end: 4_096,
    };
    Persister::persist_wal_anchor(&store, &s, Some(&first_anchor))
        .expect("current watermark anchor persists");
    assert_eq!(
        Persister::projection_seed(&store, &s)
            .unwrap()
            .unwrap()
            .anchor,
        Some(first_anchor)
    );
    assert_eq!(
        MaterializedStore::read_page_with_watermark(
            &store,
            &s,
            None,
            1,
            broca_core::context::ReadView::Raw
        )
        .unwrap()
        .anchor,
        Some(first_anchor)
    );

    records.extend(text_turn(4, 2, "second"));
    assert_eq!(Persister::project(&store, &s, &records).unwrap(), 5);
    assert_eq!(
        Persister::projection_seed(&store, &s)
            .unwrap()
            .unwrap()
            .anchor,
        None,
        "an anchor from an older watermark is not returned"
    );
    assert_eq!(
        MaterializedStore::read_page_with_watermark(
            &store,
            &s,
            None,
            1,
            broca_core::context::ReadView::Raw
        )
        .unwrap()
        .anchor,
        None
    );

    let current_anchor = WalByteAnchor {
        seq: 5,
        byte_end: 8_192,
        ..first_anchor
    };
    Persister::persist_wal_anchor(&store, &s, Some(&current_anchor))
        .expect("new watermark anchor persists");
    assert_eq!(
        Persister::projection_seed(&store, &s)
            .unwrap()
            .unwrap()
            .anchor,
        Some(current_anchor)
    );
}

/// Suffix-only projection must match full-prefix replay at every corpus checkpoint.
pub fn incremental_projection_matches_full_prefix_corpus<S>(store: S)
where
    S: SessionStore,
{
    let s = session("projection-equivalence");
    let mut records = base_records(&s);
    records.extend(text_turn(2, 1, "episode-one"));
    let first_watermark = Persister::project(&store, &s, &records).expect("first projection");
    assert_eq!(first_watermark, 3);
    assert_store_state_matches_full_projection(&store, &s, &records);

    records.push(framed(
        4,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    records.push(run_started(
        5,
        "r2",
        &s,
        "episode two input",
        ModelHandle::new("stub", "m2"),
    ));
    records.extend(text_turn(6, 1, "episode-two"));
    let second_tail: Vec<FramedRecord> = records
        .iter()
        .filter(|record| record.seq > first_watermark)
        .cloned()
        .collect();
    let second_watermark = Persister::project(&store, &s, &second_tail).expect("second projection");
    assert_eq!(second_watermark, 7);
    assert_store_state_matches_full_projection(&store, &s, &records);

    records.push(text_step(8, 2, "terminal"));
    records.push(framed(
        9,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let terminal_tail: Vec<FramedRecord> = records
        .iter()
        .filter(|record| record.seq > second_watermark)
        .cloned()
        .collect();
    assert_eq!(
        Persister::project(&store, &s, &terminal_tail).expect("terminal projection"),
        8
    );
    assert_store_state_matches_full_projection(&store, &s, &records);
}

/// A budgeted suffix with persisted state must match replaying all records from the beginning.
pub fn budgeted_incremental_projection_matches_full_prefix<S>(store: S)
where
    S: SessionStore,
{
    let s = session("budget-projection-equivalence");
    let mut records = base_records(&s);
    let WalRecord::RunStarted { config, .. } = &mut records[0].record else {
        unreachable!("base record starts a run");
    };
    config.budget = Some(BudgetConfig {
        nudges: vec![BudgetNudge {
            at_tool_calls: 0,
            text: "budget reminder".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    records.extend(text_turn(1, 1, "first"));
    assert_eq!(
        Persister::project(&store, &s, &records).expect("project budget prefix"),
        2
    );

    records.push(framed(
        3,
        WalRecord::BudgetFired {
            run_id: RunId("r".into()),
            kind: BudgetKind::Nudge,
            threshold: 0,
            ordinal: 2,
            caller_text: Some("budget reminder".into()),
        },
    ));
    records.push(text_step(4, 2, "terminal"));
    records.push(framed(
        5,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let tail: Vec<_> = records
        .iter()
        .filter(|record| record.seq > 2)
        .cloned()
        .collect();
    assert_eq!(
        Persister::project(&store, &s, &tail).expect("project budget suffix"),
        4
    );
    assert_store_state_matches_full_projection(&store, &s, &records);
}

/// Both sequence origins supported by projection must freeze the same first-run
/// policy, preserve it on suffixes, and recover it without usable store metadata.
pub fn projection_retention_policy_survives_suffixes_and_full_prefix_repair<S>(store: S)
where
    S: SessionStore + PageUsageConformance,
{
    use broca_core::LineageRetention;
    const MARKER: &str = "retained-projection-policy-marker-c8341f";
    for base in [0, 1] {
        let s = session(&format!("policy-origin-{base}"));
        let mut records = vec![run_started(
            base,
            "r",
            &s,
            "forever control",
            ModelHandle::new("stub", "m"),
        )];
        records.extend(text_turn(base + 1, 1, "first"));
        assert_eq!(Persister::project(&store, &s, &records).unwrap(), base + 2);
        let first = MaterializedStore::read_admission_snapshot(&store, &s)
            .unwrap()
            .unwrap();
        assert_eq!(
            first.checkpoint.unwrap().lineage_retention,
            Some(LineageRetention::Forever)
        );
        assert_eq!(
            AdmissionCheckpointV1::from_frames(&records)
                .unwrap()
                .lineage_retention,
            Some(LineageRetention::Forever)
        );

        let suffix = text_turn(base + 3, 2, "suffix");
        assert_eq!(Persister::project(&store, &s, &suffix).unwrap(), base + 4);
        records.extend(suffix);
        assert_store_state_matches_full_projection(&store, &s, &records);

        store
            .replace_admission_checkpoint_json_for_conformance(&s, None)
            .unwrap();
        assert!(Persister::projection_seed(&store, &s).unwrap().is_none());
        let suffix = text_turn(base + 5, 3, "repaired");
        // With a damaged checkpoint and no first run in the batch the policy
        // really is unknown. Refusal must leave the existing projection intact.
        let before = MaterializedStore::read_prefix_with_watermark(&store, &s).unwrap();
        assert!(Persister::project(&store, &s, &suffix).is_err());
        assert_eq!(
            MaterializedStore::read_prefix_with_watermark(&store, &s).unwrap(),
            before
        );
        records.extend(suffix);
        assert_eq!(Persister::project(&store, &s, &records).unwrap(), base + 6);
        assert_store_state_matches_full_projection(&store, &s, &records);

        let retained = session(&format!("retained-policy-origin-{base}"));
        let mut private = vec![run_started(
            base,
            "private",
            &retained,
            MARKER,
            ModelHandle::new("stub", "m"),
        )];
        let WalRecord::RunStarted {
            retention, title, ..
        } = &mut private[0].record
        else {
            unreachable!()
        };
        *retention = Some(3600);
        *title = Some(MARKER.into());
        private.extend(text_turn(base + 1, 1, MARKER));
        assert_eq!(Persister::project(&store, &retained, &private).unwrap(), 0);
        assert!(
            MaterializedStore::read_admission_snapshot(&store, &retained)
                .unwrap()
                .is_none()
        );
        let prefix = MaterializedStore::read_prefix_with_watermark(&store, &retained).unwrap();
        assert!(prefix.prefix.is_empty());
        assert_eq!(prefix.watermark_seq, 0);
        assert!(!format!("{prefix:?}").contains(MARKER));
    }
}

/// Incremental projection must reconstruct the same prefix as replaying the WAL.
pub fn incremental_projection_resumes_from_watermark<S>(store: S)
where
    S: SessionStore,
{
    let s = session("incremental");

    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));
    let wm1 = Persister::project(&store, &s, &recs).expect("projection succeeds");
    assert_eq!(wm1, 3, "watermark at turn 1's TurnFinished");
    let p1 =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_eq!(p1.watermark_seq, 3);
    assert_eq!(p1.prefix.len(), 2, "user + assistant(first) materialized");
    assert_prefix_matches_replay(&p1, &recs, wm1);

    recs.extend(text_turn(4, 2, "second"));
    let wm2 = Persister::project(&store, &s, &recs).expect("projection succeeds");
    assert_eq!(wm2, 5);
    let p2 =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_eq!(
        p2.prefix.len(),
        3,
        "user + first + second materialized without double-inserting turn 1"
    );

    let full = replay(&records_only(&recs)).expect("replay");
    // The prefix reads back with unstamped ordinals (not persisted); stamp by position to
    // compare against the full replay (see `assert_prefix_matches_replay`).
    let mut stamped = p2.prefix.clone();
    broca_protocol::content::stamp_ordinals(&mut stamped);
    assert_eq!(stamped, full.prompt);
    assert_eq!(p2.completed_steps, full.completed_steps);
    assert_eq!(p2.usage, full.usage);
    assert_store_state_matches_full_projection(&store, &s, &recs);
}

/// Replaying an already-materialized WAL range must not duplicate rows or counters.
pub fn re_projecting_same_wal_is_idempotent<S>(store: S)
where
    S: SessionStore,
{
    let s = session("idempotent");
    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));

    let a = Persister::project(&store, &s, &recs).expect("first projection succeeds");
    let b = Persister::project(&store, &s, &recs).expect("second projection succeeds");
    assert_eq!(a, b);

    let prefix =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_eq!(prefix.prefix.len(), 2, "no duplicate materialized messages");
    assert_prefix_matches_replay(&prefix, &recs, a);
    assert_store_state_matches_full_projection(&store, &s, &recs);
}

/// A no-tool step without `RunFinished` stays WAL-only and is recovered by prefix ⊕ tail.
pub fn uncommitted_tail_is_not_materialized_but_merge_recovers_it<S>(store: S)
where
    S: SessionStore,
{
    let s = session("tail");
    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));
    recs.push(text_step(4, 2, "second-tail-only"));

    Persister::project(&store, &s, &recs).expect("projection succeeds");
    let prefix =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_eq!(
        prefix.watermark_seq, 3,
        "step 2 stays WAL-only without a durable RunFinished"
    );
    assert_eq!(prefix.prefix.len(), 2, "step 2 is not materialized");
    assert_prefix_matches_replay(&prefix, &recs, prefix.watermark_seq);

    let tail: Vec<WalRecord> = recs
        .iter()
        .filter(|f| f.seq > prefix.watermark_seq)
        .map(|f| f.record.clone())
        .collect();
    let merged = effective_context(&prefix, &tail).expect("merge");
    let full = replay(&records_only(&recs)).expect("replay").prompt;
    assert_eq!(merged, full, "merge recovers the WAL-only tail");
    assert_store_state_matches_full_projection(&store, &s, &recs);
}

/// Completed no-tool steps materialize at the step boundary while terminal state stays in WAL.
pub fn projection_boundary_keeps_terminal_record_in_tail<S>(store: S)
where
    S: SessionStore,
{
    let s = session("watermark");
    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));
    recs.push(framed(
        4,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));

    let wm = Persister::project(&store, &s, &recs).expect("projection succeeds");
    assert_eq!(
        wm, 3,
        "RunFinished does not advance the watermark; it stays at the last TurnFinished"
    );
    let prefix =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_prefix_matches_replay(&prefix, &recs, wm);
    assert_store_state_matches_full_projection(&store, &s, &recs);

    let s = session("natural-end");
    let mut recs = base_records(&s);
    recs.extend(text_turn(2, 1, "first"));
    recs.push(text_step(4, 2, "natural terminal"));
    recs.push(framed(
        5,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));

    let wm = Persister::project(&store, &s, &recs).expect("projection succeeds");
    assert_eq!(
        wm, 4,
        "a durable RunFinished licenses the no-tool step, but is not itself materialized"
    );
    let prefix =
        MaterializedStore::read_prefix_with_watermark(&store, &s).expect("snapshot read succeeds");
    assert_eq!(
        prefix.prefix.len(),
        3,
        "the no-tool assistant message is materialized"
    );
    assert_prefix_matches_replay(&prefix, &recs, wm);

    let tail_frames: Vec<&FramedRecord> = recs
        .iter()
        .filter(|f| f.seq > prefix.watermark_seq)
        .collect();
    assert_eq!(
        tail_frames.len(),
        1,
        "only the terminal record stays in WAL"
    );
    assert!(matches!(
        tail_frames[0].record,
        WalRecord::RunFinished { .. }
    ));
    let tail: Vec<WalRecord> = tail_frames.into_iter().map(|f| f.record.clone()).collect();
    assert_eq!(
        effective_context(&prefix, &tail).expect("merge"),
        replay(&records_only(&recs)).expect("replay").prompt,
        "store-prefix plus terminal WAL tail equals full replay at the no-tool boundary"
    );
    assert_store_state_matches_full_projection(&store, &s, &recs);
}

/// Repairing an import replaces a previously materialized torn-import prompt.
pub fn interrupted_import_projection_is_replaced_on_repair<S>(store: S)
where
    S: SessionStore,
{
    let session = session("interrupted-import-repair");
    let mut old_start = run_started(
        1,
        "old-import",
        &session,
        "ghost prompt",
        ModelHandle::new("import", "import"),
    );
    let WalRecord::RunStarted { origin, .. } = &mut old_start.record else {
        unreachable!("fixture starts a run")
    };
    *origin = broca_wal::RunOrigin::SeededFrom {
        origin_kind: broca_wal::SeedOriginKind::Import,
        source_ref: "cc:old".into(),
        origin_device_pseudonym: None,
        base_seq: None,
    };
    let mut records = vec![
        old_start,
        framed(
            2,
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Interrupted,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ];
    assert_eq!(
        Persister::project(&store, &session, &records).expect("project torn import"),
        1
    );
    assert_eq!(
        MaterializedStore::read_prefix_with_watermark(&store, &session)
            .expect("read torn prefix")
            .prefix
            .len(),
        1
    );

    let mut replacement = run_started(
        3,
        "replacement-import",
        &session,
        "replacement prompt",
        ModelHandle::new("import", "import"),
    );
    let WalRecord::RunStarted { origin, .. } = &mut replacement.record else {
        unreachable!("fixture starts a run")
    };
    *origin = broca_wal::RunOrigin::SeededFrom {
        origin_kind: broca_wal::SeedOriginKind::Import,
        source_ref: "cc:replacement".into(),
        origin_device_pseudonym: None,
        base_seq: None,
    };
    records.extend([
        replacement,
        framed(
            4,
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]);
    Persister::project(&store, &session, &records).expect("project repaired import");
    let prefix = MaterializedStore::read_prefix_with_watermark(&store, &session)
        .expect("read repaired prefix");
    assert_eq!(prefix.prefix.len(), 1);
    assert!(matches!(
        &prefix.prefix[0].content[0],
        ContentBlock::Text { text } if text == "replacement prompt"
    ));
}

/// Admission metadata starts unavailable, then matches full derivation after both full and
/// suffix-only projections.
pub fn admission_checkpoint_empty_roundtrip_and_incremental_equivalence<S: SessionStore>(store: S) {
    let session = session("admission-checkpoint");
    assert_eq!(
        MaterializedStore::read_admission_snapshot(&store, &session).expect("empty admission read"),
        None
    );

    let mut records = base_records(&session);
    records.extend(text_turn(2, 1, "first"));
    let first_watermark = Persister::project(&store, &session, &records).expect("full projection");
    assert_eq!(first_watermark, 3);
    assert_store_state_matches_full_projection(&store, &session, &records);

    records.push(framed(
        4,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    records.push(run_started(
        5,
        "r2",
        &session,
        "second input",
        ModelHandle::new("stub", "m2"),
    ));
    records.extend(text_turn(6, 1, "second"));
    let tail: Vec<_> = records
        .iter()
        .filter(|record| record.seq > first_watermark)
        .cloned()
        .collect();
    assert_eq!(
        Persister::project(&store, &session, &tail).expect("incremental projection"),
        7
    );
    assert_store_state_matches_full_projection(&store, &session, &records);
}

/// The range-aware persister stores its watermark anchor in the projection transaction.
pub fn projection_range_co_commits_anchor<S: SessionStore>(store: S) {
    let session = session("admission-anchor");
    let mut records = base_records(&session);
    records.extend(text_turn(2, 1, "anchored"));
    let anchors: Vec<_> = records
        .iter()
        .enumerate()
        .map(|(index, record)| WalByteAnchor {
            file_id: 42,
            seq: record.seq,
            byte_end: ((index + 1) * 1_024) as u64,
        })
        .collect();
    let range = WalRangeRead::with_anchors(records, anchors.clone());
    assert_eq!(
        Persister::project_range(&store, &session, &range).expect("range projection"),
        3
    );
    let snapshot = MaterializedStore::read_admission_snapshot(&store, &session)
        .expect("admission read")
        .expect("metadata row");
    assert_eq!(snapshot.anchor, anchors.last().copied());
    assert_eq!(snapshot.watermark_seq, 3);
    assert_eq!(snapshot.checkpoint.expect("checkpoint").at_seq, 3);
}

/// A replay failure aborts the transaction, leaving messages and admission metadata at the old
/// watermark together.
pub fn failed_projection_keeps_checkpoint_and_messages_atomic<S: SessionStore>(store: S) {
    let session = session("admission-atomic");
    let mut records = base_records(&session);
    records.extend(text_turn(2, 1, "stable"));
    Persister::project(&store, &session, &records).expect("initial projection");
    let old_page = MaterializedStore::read_page_with_watermark(
        &store,
        &session,
        Some(0),
        u64::MAX,
        broca_core::context::ReadView::Raw,
    )
    .expect("old page");
    let old_admission =
        MaterializedStore::read_admission_snapshot(&store, &session).expect("old admission");

    let mut corrupt_tail = vec![run_started(
        4,
        "r2",
        &session,
        "illegal overlap",
        ModelHandle::new("stub", "m2"),
    )];
    corrupt_tail.extend(text_turn(5, 1, "must roll back"));
    assert!(Persister::project(&store, &session, &corrupt_tail).is_err());
    assert_eq!(
        MaterializedStore::read_page_with_watermark(
            &store,
            &session,
            Some(0),
            u64::MAX,
            broca_core::context::ReadView::Raw
        )
        .expect("page after failure"),
        old_page
    );
    assert_eq!(
        MaterializedStore::read_admission_snapshot(&store, &session)
            .expect("admission after failure"),
        old_admission
    );
}

fn assert_store_state_matches_full_projection<S: SessionStore>(
    store: &S,
    session: &BindIdentity,
    records: &[FramedRecord],
) {
    let prefix = MaterializedStore::read_prefix_with_watermark(store, session)
        .expect("projection snapshot reads");
    assert_prefix_matches_replay(&prefix, records, prefix.watermark_seq);

    let page = MaterializedStore::read_page_with_watermark(
        store,
        session,
        Some(0),
        u64::MAX,
        broca_core::context::ReadView::Raw,
    )
    .expect("complete projection page reads");
    let expected_records: Vec<WalRecord> = records
        .iter()
        .filter(|record| record.seq <= prefix.watermark_seq)
        .map(|record| record.record.clone())
        .collect();
    let expected = replay(&expected_records).expect("full-prefix replay oracle");
    let actual_messages: Vec<Message> = page.rows.iter().map(|row| row.message.clone()).collect();
    assert_eq!(
        actual_messages, expected.prompt,
        "messages and ordinals match"
    );
    assert_eq!(page.watermark_seq, prefix.watermark_seq);
    assert_eq!(page.prefix_len, expected.prompt.len() as u64);
    let admission = MaterializedStore::read_admission_snapshot(store, session)
        .expect("admission metadata reads")
        .expect("a projection creates admission metadata");
    let expected_checkpoint = AdmissionCheckpointV1::from_frames(
        &records
            .iter()
            .filter(|record| record.seq <= prefix.watermark_seq)
            .cloned()
            .collect::<Vec<_>>(),
    )
    .expect("full-WAL admission oracle");
    assert_eq!(admission.watermark_seq, prefix.watermark_seq);
    assert_eq!(admission.checkpoint, Some(expected_checkpoint.clone()));
    assert_eq!(
        page.last_episode_usage,
        expected_checkpoint
            .last_episode
            .and_then(|episode| episode.usage),
        "the page transaction projects the same episode usage as the admission checkpoint"
    );
    assert_eq!(
        serde_json::to_vec(&actual_messages).expect("serialize actual projection"),
        serde_json::to_vec(&expected.prompt).expect("serialize replay oracle"),
        "stored message bytes match the full-prefix replay path"
    );
}

fn assert_prefix_matches_replay(
    prefix: &broca_core::StorePrefix,
    recs: &[FramedRecord],
    watermark: u64,
) {
    let committed: Vec<WalRecord> = recs
        .iter()
        .filter(|f| f.seq <= watermark)
        .map(|f| f.record.clone())
        .collect();
    let expected = replay(&committed).expect("replay");
    assert_eq!(prefix.watermark_seq, watermark);
    assert_eq!(prefix.context_epoch, 0);
    // The store does NOT persist `Message.ordinal` (it is `#[serde(skip)]` — re-derived by
    // position whenever a prompt is finalized), so the raw prefix reads back with `None`
    // ordinals while `replay` stamps them. Stamp the prefix by position (the same
    // finalization the merge paths apply) before comparing: the prefix is the first
    // `watermark` messages of the lineage, so its positions 0..n match the replay's.
    let mut stamped_prefix = prefix.prefix.clone();
    broca_protocol::content::stamp_ordinals(&mut stamped_prefix);
    assert_eq!(stamped_prefix, expected.prompt);
    assert_eq!(prefix.completed_steps, expected.completed_steps);
    assert_eq!(prefix.usage, expected.usage);
    assert_eq!(prefix.last_step_usage, expected.last_step_usage);
    assert_eq!(
        prefix.last_step_finish_reason,
        expected.last_step_finish_reason
    );
}

pub(crate) fn session(label: &str) -> BindIdentity {
    let n = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    BindIdentity::new(
        format!(
            "/tmp/broca-store-conformance-{label}-{}-{n}",
            std::process::id()
        ),
        "harness",
        "session",
    )
}

fn framed(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

pub(crate) fn text_step(seq: u64, step: u64, text: &str) -> FramedRecord {
    framed(
        seq,
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(step),
            assistant_message: (AssistantMessage {
                message_id: MessageId(format!("m{step}")),
                content: vec![ContentBlock::Text { text: text.into() }],
            })
            .into(),
            usage: Usage {
                input_tokens: Some(step * 10),
                output_tokens: Some(step),
                ..Default::default()
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
    )
}

pub(crate) fn text_turn(seq_base: u64, step: u64, text: &str) -> Vec<FramedRecord> {
    vec![
        text_step(seq_base, step, text),
        framed(
            seq_base + 1,
            WalRecord::TurnFinished {
                step_id: StepId(step),
            },
        ),
    ]
}

fn tool_turn(seq_base: u64, step: u64) -> Vec<FramedRecord> {
    let batch_id = BatchId(format!("b{step}"));
    let tool_call_id = ToolCallId(format!("c{step}"));
    vec![
        framed(
            seq_base,
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(step),
                assistant_message: AssistantMessage {
                    message_id: MessageId(format!("m{step}")),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        tool_call_id: tool_call_id.clone(),
                        tool_name: "lookup".into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    })],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
        ),
        framed(
            seq_base + 1,
            WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(step),
                planned_calls: vec![tool_call_id.clone()],
            },
        ),
        framed(
            seq_base + 2,
            WalRecord::ToolResult {
                hooks: None,
                batch_id: batch_id.clone(),
                result: ToolResult {
                    tool_call_id,
                    output: ToolOutput::Text { text: "ok".into() },
                    is_error: false,
                }
                .into(),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
        ),
        framed(seq_base + 3, WalRecord::ToolBatchFinished { batch_id }),
        framed(
            seq_base + 4,
            WalRecord::TurnFinished {
                step_id: StepId(step),
            },
        ),
    ]
}

pub(crate) fn base_records(session: &BindIdentity) -> Vec<FramedRecord> {
    vec![run_started(
        0,
        "r",
        session,
        "hi",
        ModelHandle::new("stub", "m"),
    )]
}

fn project_checkpoint_usage<S>(
    store: &S,
    session: &BindIdentity,
    usage: Usage,
) -> AdmissionCheckpointV1
where
    S: PageUsageConformance,
{
    let mut records = base_records(session);
    records.extend(text_turn(2, 1, "checkpoint usage"));
    Persister::project(store, session, &records).expect("episode projection succeeds");
    let mut checkpoint = MaterializedStore::read_admission_snapshot(store, session)
        .expect("admission read succeeds")
        .expect("row has admission metadata")
        .checkpoint
        .expect("row has a checkpoint");
    checkpoint
        .last_episode
        .as_mut()
        .expect("checkpoint retains its episode")
        .usage = Some(usage);
    store
        .replace_admission_checkpoint_for_conformance(session, &checkpoint)
        .expect("install legacy checkpoint usage");
    checkpoint
}

pub(crate) fn run_started(
    seq: u64,
    run_id: &str,
    session: &BindIdentity,
    input: &str,
    model: ModelHandle,
) -> FramedRecord {
    framed(
        seq,
        WalRecord::RunStarted {
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId(run_id.into()),
            session: session.clone(),
            config: Box::new(RunConfig {
                model,
                tools: vec![].into(),
                tool_choice: Default::default(),
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text { text: input.into() }],
            }]
            .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
    )
}

fn records_only(recs: &[FramedRecord]) -> Vec<WalRecord> {
    recs.iter().map(|f| f.record.clone()).collect()
}
