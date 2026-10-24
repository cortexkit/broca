use super::*;
use broca_core::{
    model_step_fact::ModelStepFact, system_one_fact::UsageExportSegment, ChargeBasis, ExportIndex,
    RunIndex, RunIndexRow, RunIndexState,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{AttemptOutcome, ContentProgress},
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    FinishReason, MessageId, RunId, StepId, Usage,
};
use broca_wal::{AuthSelection, RunConfig, RunOrigin, WalRecord};

const LINEAGE: &str = "00112233-4455-4677-8899-aabbccddeeff";
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// A wall clock comfortably later than every fixture timestamp.
const NOW: u64 = 1_800_000_000_000;

fn session() -> BindIdentity {
    BindIdentity::new("/tmp/steps", "runner", "session")
}

fn frame(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

fn started(seq: u64, run_id: &str, ts_ms: u64) -> FramedRecord {
    frame(
        seq,
        WalRecord::RunStarted {
            tags: None,
            session_class: None,
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
            run_id: RunId(run_id.into()),
            session: session(),
            ts_ms: Some(ts_ms),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: ModelHandle::new("anthropic", "claude"),
                tools: vec![].into(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                response_format: ResponseFormat::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::json!({"family_id": "anthropic_messages"}),
                auth_selection: Some(AuthSelection {
                    credential_id: "apikey:anthropic:work".into(),
                    account_ref: None,
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                }),
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: vec![],
                context_management: None,
            }),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "hi".into() }],
            )]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        },
    )
}

fn attempt(seq: u64, step: u64, end_ms: u64) -> FramedRecord {
    frame(
        seq,
        WalRecord::ModelAttemptFinished {
            step_id: StepId(step),
            attempt: 0,
            request_body_len: 100,
            request_body_sha256: "a".repeat(64),
            http_status: Some(200),
            content_type: None,
            response_body_len: 10,
            response_body_sha256: "b".repeat(64),
            response_body_cap: None,
            outcome: AttemptOutcome::Complete,
            ts_ms: end_ms,
            content_progress: Box::new(ContentProgress::default()),
            sent_at_ms: Some(end_ms - 10),
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            credential_refresh: None,
            served_account_ref: None,
        },
    )
}

fn step(seq: u64, id: u64) -> FramedRecord {
    frame(
        seq,
        WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(id),
            assistant_message: AssistantMessage {
                message_id: MessageId(format!("m{id}")),
                content: vec![ContentBlock::Text { text: "ok".into() }],
            }
            .into(),
            usage: Usage {
                input_tokens: Some(10),
                cached_input_tokens: Some(90),
                output_tokens: Some(2),
                ..Usage::default()
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
    )
}

fn finished(seq: u64, ts_ms: u64) -> FramedRecord {
    frame(
        seq,
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: Some(ts_ms),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    )
}

/// One run with two committed steps, finished at `t`, `t + 1000` and `t + 2000`.
fn two_step_run(run_id: &str, t: u64) -> Vec<FramedRecord> {
    vec![
        started(1, run_id, t - 100),
        attempt(2, 1, t),
        step(3, 1),
        attempt(4, 2, t + 1000),
        step(5, 2),
        finished(6, t + 2000),
    ]
}

fn resolver(_: &str, _: &str) -> (ChargeBasis, u32) {
    (ChargeBasis::MeteredApi, 1)
}

/// Every row of `export_facts` as (export_seq, fact_id, run_id column, segment_json).
fn rows(store: &SqliteStore) -> Vec<(i64, String, String, String)> {
    store
        .conn
        .lock()
        .unwrap()
        .prepare("SELECT export_seq, fact_id, run_id, segment_json FROM export_facts ORDER BY export_seq")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn step_facts(store: &SqliteStore) -> Vec<ModelStepFact> {
    store
        .read_export_page(None, 1000)
        .unwrap()
        .facts
        .into_iter()
        .filter_map(|fact| match fact.segment {
            UsageExportSegment::ModelStep(fact) => Some(*fact),
            _ => None,
        })
        .collect()
}

/// Session (billing) facts only, by their full stored bytes.
fn session_rows(store: &SqliteStore) -> Vec<(String, String, String)> {
    rows(store)
        .into_iter()
        .filter(|(_, _, _, json)| {
            serde_json::from_str::<serde_json::Value>(json).unwrap()["kind"].is_null()
        })
        .map(|(_, fact_id, run_id, json)| (fact_id, run_id, json))
        .collect()
}

#[test]
fn a_committed_step_is_written_at_once_with_its_checkpoint() {
    let store = SqliteStore::open_in_memory().unwrap();
    let records = two_step_run("run-1", NOW - DAY_MS);
    // Only the first step is durable so far: the run is still going.
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records[..3], NOW)
        .unwrap();
    assert_eq!((report.written, report.through_seq), (1, 3));
    let facts = step_facts(&store);
    assert_eq!(facts.len(), 1);
    assert_eq!((facts[0].run_id.as_str(), facts[0].step_id), ("run-1", 1));
    assert_eq!(facts[0].finished_at_ms, Some(NOW - DAY_MS));
    assert_eq!(
        store.high_water(),
        1,
        "a step fact moves the export high-water"
    );
    assert_eq!(
        store.model_step_checkpoint(&session()).unwrap(),
        Some(ModelStepCheckpoint {
            lineage_id: LINEAGE.into(),
            seq: 3
        })
    );
    // The next durable range carries on from the checkpoint and writes only the new step.
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!((report.written, report.through_seq), (1, 6));
    assert_eq!(
        step_facts(&store)
            .iter()
            .map(|fact| fact.step_id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let expiry: Vec<(i64, i64)> = store
        .conn
        .lock()
        .unwrap()
        .prepare("SELECT export_seq, expires_at_ms FROM export_fact_expiry ORDER BY export_seq")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        expiry,
        [
            (1, (NOW - DAY_MS + MODEL_STEP_FACT_RETENTION_MS) as i64),
            (
                2,
                (NOW - DAY_MS + 1000 + MODEL_STEP_FACT_RETENTION_MS) as i64
            )
        ]
    );
}

#[test]
fn a_range_that_skips_past_the_checkpoint_is_refused() {
    let store = SqliteStore::open_in_memory().unwrap();
    let records = two_step_run("run-1", NOW - DAY_MS);
    store
        .project_model_step_facts(&session(), LINEAGE, &records[..2], NOW)
        .unwrap();
    assert!(store
        .project_model_step_facts(&session(), LINEAGE, &records[3..], NOW)
        .is_err());
    assert!(step_facts(&store).is_empty());
}

#[test]
fn a_crash_before_the_checkpoint_commits_leaves_nothing_and_the_replay_writes_once() {
    let store = SqliteStore::open_in_memory().unwrap();
    let records = two_step_run("run-1", NOW - DAY_MS);
    MODEL_STEP_COMMIT_HOOK.with(|hook| hook.set(true));
    assert!(store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .is_err());
    assert!(
        rows(&store).is_empty(),
        "the facts roll back with the checkpoint"
    );
    assert_eq!(store.model_step_checkpoint(&session()).unwrap(), None);
    assert_eq!(store.high_water(), 0);

    // The restarted writer reads the same durable range again: each step lands once.
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!(report.written, 2);
    // And a writer that crashed AFTER committing reads the range once more: nothing new.
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!((report.written, report.already_present), (0, 0));
    assert_eq!(step_facts(&store).len(), 2);
}

#[test]
fn rederiving_without_a_checkpoint_hits_the_same_fact_ids_and_changes_no_bytes() {
    let store = SqliteStore::open_in_memory().unwrap();
    let records = two_step_run("run-1", NOW - DAY_MS);
    store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    let before = rows(&store);
    store
        .conn
        .lock()
        .unwrap()
        .execute("DELETE FROM model_step_projection", [])
        .unwrap();
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!((report.written, report.already_present), (0, 2));
    assert_eq!(rows(&store), before);
}

#[test]
fn steps_already_past_their_lifetime_are_never_written() {
    let store = SqliteStore::open_in_memory().unwrap();
    let t = NOW - MODEL_STEP_FACT_RETENTION_MS - 500;
    // Step 1 finished more than 30 days ago; step 2 finished exactly 30 days ago (expired at
    // the boundary); the run's later steps are inside the window.
    let records = vec![
        started(1, "run-old", t - 100),
        attempt(2, 1, t),
        step(3, 1),
        attempt(4, 2, t + 500),
        step(5, 2),
        attempt(6, 3, t + 501),
        step(7, 3),
    ];
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!((report.written, report.expired), (1, 2));
    assert_eq!(step_facts(&store)[0].step_id, 3);
}

#[test]
fn steps_of_a_run_installed_from_another_machine_are_not_written() {
    let store = SqliteStore::open_in_memory().unwrap();
    store
        .record_imported_runs(&session(), "cut", &[RunId("foreign".into())])
        .unwrap();
    let report = store
        .project_model_step_facts(
            &session(),
            LINEAGE,
            &two_step_run("foreign", NOW - DAY_MS),
            NOW,
        )
        .unwrap();
    assert_eq!((report.written, report.imported), (0, 2));
    assert!(rows(&store).is_empty());
}

#[test]
fn expiry_deletes_step_facts_at_exactly_thirty_days_and_never_a_billing_fact() {
    let store = SqliteStore::open_in_memory().unwrap();
    let t = NOW - DAY_MS;
    let records = two_step_run("run-1", t);
    store
        .emit_export_run(&records, &RunId("run-1".into()), &resolver)
        .unwrap();
    store
        .emit_system_one_fact(&broca_core::system_one_fact::SystemOneFact {
            fact_id: "s1".into(),
            batch_id: "batch".into(),
            item_index: 0,
            provider: "p".into(),
            model: "m".into(),
            auth_selection: None,
            account_ref: None,
            run_id: Some("run-1".into()),
            flow_id: None,
            charge_basis: ChargeBasis::MeteredApi,
            charge_basis_mapping_version: 1,
            charge_basis_overrides: "none".into(),
            usage: broca_core::system_one_fact::SystemOneUsage {
                input_tokens: Some(1),
                output_tokens: None,
            },
            occurred_at_ms: 1,
        })
        .unwrap();
    store
        .project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    // A bookkeeping row naming a BILLING fact (it should never exist) must still not delete it.
    store
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO export_fact_expiry (export_seq, expires_at_ms) VALUES (1, 0), (2, 0)",
            [],
        )
        .unwrap();
    let billing = |store: &SqliteStore| {
        rows(store)
            .into_iter()
            .filter(|(_, _, _, json)| !json.contains("\"kind\":\"model_step\""))
            .collect::<Vec<_>>()
    };
    let billing_before = billing(&store);
    assert_eq!(
        billing_before.len(),
        2,
        "one session fact and one System One fact"
    );

    // One millisecond before step 1's 30 days are up: nothing but the bogus rows is due.
    let step_one_expiry = t + MODEL_STEP_FACT_RETENTION_MS;
    assert_eq!(
        store
            .expire_model_step_facts(step_one_expiry - 1, 100)
            .unwrap(),
        0
    );
    assert_eq!(step_facts(&store).len(), 2);
    // Exactly 30 days after step 1 finished: step 1 goes, step 2 (a second later) stays.
    assert_eq!(
        store.expire_model_step_facts(step_one_expiry, 100).unwrap(),
        1
    );
    assert_eq!(
        step_facts(&store)
            .iter()
            .map(|fact| fact.step_id)
            .collect::<Vec<_>>(),
        [2]
    );
    // Far in the future every step fact is gone and every billing fact is still there.
    assert_eq!(store.expire_model_step_facts(u64::MAX / 4, 100).unwrap(), 1);
    assert!(step_facts(&store).is_empty());
    assert_eq!(billing(&store), billing_before);
    assert_eq!(
        store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM export_fact_expiry", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn the_sweep_is_bounded_per_call() {
    let store = SqliteStore::open_in_memory().unwrap();
    store
        .project_model_step_facts(
            &session(),
            LINEAGE,
            &two_step_run("run-1", NOW - DAY_MS),
            NOW,
        )
        .unwrap();
    assert_eq!(store.expire_model_step_facts(u64::MAX / 4, 1).unwrap(), 1);
    assert_eq!(step_facts(&store).len(), 1);
}

/// A file-backed store whose export source instance id is fixed, so two stores derive the
/// same session `fact_id`s and their rows can be compared byte for byte.
fn store_with_source(name: &str) -> (SqliteStore, std::path::PathBuf) {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-model-step-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE export_meta (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 source_epoch INTEGER NOT NULL DEFAULT 1,
                 source_instance_id TEXT NOT NULL,
                 high_water INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO export_meta (id, source_epoch, source_instance_id, high_water)
             VALUES (1, 1, 'fixed-source', 0);",
        )
        .unwrap();
    (SqliteStore::open(&db).unwrap(), root)
}

#[test]
fn a_runs_session_facts_are_byte_identical_with_and_without_step_facts() {
    let records = two_step_run("run-1", NOW - DAY_MS);
    let run_id = RunId("run-1".into());

    let (without, without_root) = store_with_source("without");
    without
        .emit_export_run(&records, &run_id, &resolver)
        .unwrap();

    let (with, with_root) = store_with_source("with");
    // Step facts before, and again after, the run's session facts are derived.
    with.project_model_step_facts(&session(), LINEAGE, &records[..3], NOW)
        .unwrap();
    with.emit_export_run(&records, &run_id, &resolver).unwrap();
    with.project_model_step_facts(&session(), LINEAGE, &records, NOW)
        .unwrap();
    assert_eq!(step_facts(&with).len(), 2);

    let billing = session_rows(&without);
    assert_eq!(billing.len(), 1);
    assert_eq!(
        session_rows(&with),
        billing,
        "same fact_id, run_id column and bytes"
    );
    drop((with, without));
    std::fs::remove_dir_all(with_root).unwrap();
    std::fs::remove_dir_all(without_root).unwrap();
}

#[test]
fn a_run_missing_its_session_facts_is_still_caught_up_when_it_has_step_facts() {
    let store = SqliteStore::open_in_memory().unwrap();
    let row = RunIndexRow {
        withdrawals_owed: false,
        title: None,
        run_id: "run-1".into(),
        session: session(),
        submission_id: None,
        request_identity_digest: None,
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    store.index_run_active(&row).unwrap();
    store
        .index_run_terminal("run-1", RunIndexState::Completed, None, None, 0)
        .unwrap();
    store
        .project_model_step_facts(
            &session(),
            LINEAGE,
            &two_step_run("run-1", NOW - DAY_MS),
            NOW,
        )
        .unwrap();
    assert_eq!(step_facts(&store).len(), 2);
    assert!(
        rows(&store)
            .iter()
            .all(|(_, _, run_id, _)| run_id.is_empty()),
        "a step fact never files itself under the run's id"
    );
    let missing = store.missing_terminal_runs().unwrap();
    assert_eq!(
        missing
            .iter()
            .map(|run| run.run_id.0.as_str())
            .collect::<Vec<_>>(),
        ["run-1"],
        "step facts must not hide a run whose billing facts were never written"
    );
}

#[test]
fn the_walk_queue_lists_recent_and_live_sessions_once_each_in_bounded_fills() {
    let store = SqliteStore::open_in_memory().unwrap();
    let other = BindIdentity::new("/tmp/other", "runner", "session");
    let old = BindIdentity::new("/tmp/old", "runner", "session");
    let runs = [
        ("a", session(), RunIndexState::Active),
        ("b", session(), RunIndexState::Active),
        ("c", other.clone(), RunIndexState::Active),
        ("d", old.clone(), RunIndexState::Completed),
    ];
    for (run_id, session, _) in &runs {
        store
            .index_run_active(&RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: (*run_id).into(),
                session: session.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .unwrap();
    }
    store
        .index_run_terminal("d", RunIndexState::Completed, None, None, 0)
        .unwrap();
    // A finished run whose state changed before `since` is not listed; active runs are listed
    // whatever their age. One row per fill: four fills cover the table.
    let since = u64::MAX / 4;
    let mut after = 0;
    let mut fills = 0;
    while let Some(next) = store.fill_model_step_walk_queue(since, after, 1).unwrap() {
        after = next;
        fills += 1;
    }
    assert_eq!(fills, 3);
    assert_eq!(store.model_step_walk_queue_len().unwrap(), 2);
    let page = store.model_step_walk_page(10).unwrap();
    let mut expected = vec![session(), other];
    expected.sort_by_key(|session| serde_json::to_string(session).unwrap());
    assert_eq!(
        page.iter()
            .map(|(_, session)| session.clone())
            .collect::<Vec<_>>(),
        expected
    );
    // Filling again adds nothing twice; visiting removes a session.
    store.fill_model_step_walk_queue(since, 0, 100).unwrap();
    assert_eq!(store.model_step_walk_queue_len().unwrap(), 2);
    store.model_step_walk_visited(&[page[0].0.clone()]).unwrap();
    assert_eq!(store.model_step_walk_page(10).unwrap(), page[1..]);
    store.clear_model_step_walk_queue().unwrap();
    assert_eq!(store.model_step_walk_queue_len().unwrap(), 0);

    assert_eq!(store.model_step_walk().unwrap(), None);
    let walk = ModelStepWalk {
        since_ms: 1,
        started_ms: 2,
        build_rowid: Some(7),
        done: false,
    };
    store.save_model_step_walk(&walk).unwrap();
    assert_eq!(store.model_step_walk().unwrap(), Some(walk));
}

/// `runs` finished two-step runs, one after another in a single session's WAL, each step
/// finishing at about `t`. One projection call, so the facts commit in a few transactions.
fn history_runs(store: &SqliteStore, runs: u64, t: u64) {
    let session = BindIdentity::new("/tmp/steps", "runner", "history");
    let mut records = Vec::new();
    for run in 0..runs {
        for mut frame in two_step_run(&format!("run-{run}"), t) {
            frame.seq += run * 6;
            records.push(frame);
        }
    }
    store
        .project_model_step_facts(&session, LINEAGE, &records, NOW)
        .unwrap();
}

#[test]
fn history_steps_wait_in_the_backlog_and_live_steps_are_written_at_once() {
    let store = SqliteStore::open_in_memory().unwrap();
    let cutoff = NOW - 1000;
    assert_eq!(store.begin_model_step_backfill(cutoff).unwrap(), cutoff);
    // A later start keeps the first cutoff.
    assert_eq!(store.begin_model_step_backfill(NOW).unwrap(), cutoff);
    // 3,000 runs of history fill the backlog without touching export_facts.
    history_runs(&store, 3_000, NOW - DAY_MS);
    assert_eq!(store.model_step_backlog_len().unwrap(), 6_000);
    assert!(rows(&store).is_empty());
    // The pace has room for one burst now, then nothing until time passes.
    assert_eq!(
        store.publish_model_step_backlog(NOW).unwrap().published,
        PUBLISH_BURST
    );
    assert_eq!(store.publish_model_step_backlog(NOW).unwrap().published, 0);
    // A live step (finished after the cutoff) is written at once, ahead of the waiting history.
    let live = vec![
        started(1, "run-live", NOW - 100),
        attempt(2, 1, NOW),
        step(3, 1),
    ];
    let report = store
        .project_model_step_facts(&session(), LINEAGE, &live, NOW)
        .unwrap();
    assert_eq!((report.written, report.queued), (1, 0));
    assert_eq!(
        store.model_step_backlog_len().unwrap(),
        6_000 - PUBLISH_BURST
    );
    let newest = rows(&store).pop().unwrap();
    assert!(newest.3.contains("\"run_id\":\"run-live\""));
    // Deriving the same history again from scratch (checkpoints lost) queues nothing twice,
    // whether its row was already published or is still waiting.
    store
        .conn
        .lock()
        .unwrap()
        .execute("DELETE FROM model_step_projection", [])
        .unwrap();
    history_runs(&store, 3_000, NOW - DAY_MS);
    assert_eq!(
        store.model_step_backlog_len().unwrap(),
        6_000 - PUBLISH_BURST
    );
}

/// The cap astrocyte asked for, over a backfill that spans many windows, with the publisher
/// called on a virtual clock every 250 ms (as the daemon does) and the store reopened halfway
/// (a restart must continue the pace, not start a fresh burst).
#[test]
fn the_backlog_never_publishes_more_than_the_cap_in_any_window() {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-model-step-pace-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    let mut store = SqliteStore::open(&db).unwrap();
    store.begin_model_step_backfill(NOW).unwrap();
    // 12,000 rows: more than two full windows' worth.
    history_runs(&store, 6_000, NOW - DAY_MS);
    assert_eq!(store.model_step_backlog_len().unwrap(), 12_000);

    let mut published: Vec<(u64, u64)> = Vec::new();
    let mut clock = NOW;
    while store.model_step_backlog_len().unwrap() > 0 {
        if clock == NOW + 45_000 {
            drop(store);
            store = SqliteStore::open(&db).unwrap();
        }
        let report = store.publish_model_step_backlog(clock).unwrap();
        if report.published > 0 {
            published.push((clock, report.published));
        }
        clock += 250;
        assert!(clock < NOW + 600_000, "the backlog did not drain");
    }
    let total: u64 = published.iter().map(|(_, rows)| rows).sum();
    assert_eq!(total, 12_000);
    // Every 30 s window (both ends inclusive) that starts at a publish.
    let worst = published
        .iter()
        .map(|(start, _)| {
            published
                .iter()
                .filter(|(at, _)| *at >= *start && *at <= start + BACKFILL_WINDOW_MS)
                .map(|(_, rows)| rows)
                .sum::<u64>()
        })
        .max()
        .unwrap();
    assert!(
        worst <= BACKFILL_ROWS_PER_WINDOW,
        "{worst} rows published in one 30 s window"
    );
    // And it is a pace, not a trickle: 12,000 rows take about 12,000 * 6.25 ms = 75 s.
    let elapsed = published.last().unwrap().0 - NOW;
    assert!(
        (70_000..=80_000).contains(&elapsed),
        "drained in {elapsed} ms"
    );
    // Every published row is a step fact with an expiry, and the export cursor moved.
    let published_rows = rows(&store);
    assert_eq!(published_rows.len(), 12_000);
    assert!(published_rows
        .iter()
        .all(|(_, _, run_id, json)| run_id.is_empty() && json.contains("\"kind\":\"model_step\"")));
    let expiry_rows: i64 = store
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM export_fact_expiry", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(expiry_rows, 12_000);
    assert_eq!(store.high_water(), 12_000);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_backlog_row_whose_thirty_days_ended_while_waiting_is_dropped() {
    let store = SqliteStore::open_in_memory().unwrap();
    store.begin_model_step_backfill(NOW).unwrap();
    history_runs(&store, 1, NOW - DAY_MS);
    let later = NOW - DAY_MS + MODEL_STEP_FACT_RETENTION_MS + 5_000;
    let report = store.publish_model_step_backlog(later).unwrap();
    assert_eq!((report.published, report.expired), (0, 2));
    assert_eq!(store.model_step_backlog_len().unwrap(), 0);
    assert!(rows(&store).is_empty());
}
