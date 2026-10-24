use super::*;
use broca_core::{
    system_one_fact::UsageExportSegment, ExportIndex, RunIndex, RunIndexRow, RunIndexState,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{AttemptOutcome, ContentProgress},
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    FinishReason, MessageId, RunId, StepId, Usage,
};
use broca_wal::{RunConfig, RunOrigin, WalRecord};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

struct Rig {
    root: std::path::PathBuf,
    store: Arc<SqliteStore>,
    wal: Arc<FileWalStore>,
    hints: Arc<Hints>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn rig(name: &str) -> Rig {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-model-step-{name}-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let hints = Arc::new(Hints::default());
    let wal = Arc::new(FileWalStore::new(root.join("wal")).with_durable_observer(hints.observer()));
    let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    Rig {
        root,
        store,
        wal,
        hints,
    }
}

fn session(name: &str) -> BindIdentity {
    BindIdentity::new("/tmp/model-step-facts", "runner", name)
}

fn started(session: &BindIdentity, run_id: &str, ts_ms: u64) -> WalRecord {
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
        session: session.clone(),
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
            auth_selection: None,
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
    }
}

fn attempt(step: u64, end_ms: u64) -> WalRecord {
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
    }
}

fn step(id: u64) -> WalRecord {
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
            input_tokens: Some(5),
            ..Usage::default()
        },
        finish_reason: Some(FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn finished(ts_ms: u64) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        reason: RunFinishReason::Completed,
        ts_ms: Some(ts_ms),
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

/// Append a finished run with `steps` committed steps, the first finishing at `t`, and index it
/// as the coordinator would.
fn write_run(rig: &Rig, session: &BindIdentity, run_id: &str, t: u64, steps: u64) {
    let mut wal = rig.wal.open(session).unwrap();
    wal.append(1, &started(session, run_id, t - 100), true)
        .unwrap();
    for id in 1..=steps {
        wal.append(1, &attempt(id, t + id - 1), false).unwrap();
        wal.append(1, &step(id), true).unwrap();
    }
    wal.append(1, &finished(t + steps), true).unwrap();
    let row = RunIndexRow {
        withdrawals_owed: false,
        title: None,
        run_id: run_id.into(),
        session: session.clone(),
        submission_id: None,
        request_identity_digest: None,
        state: RunIndexState::Active,
        usage: None,
        final_step_finish_reason: None,
        retries_used: 0,
    };
    rig.store.index_run_active(&row).unwrap();
    rig.store
        .index_run_terminal(run_id, RunIndexState::Completed, None, None, 0)
        .unwrap();
}

fn step_ids(store: &SqliteStore) -> Vec<(String, String, u64)> {
    let mut ids: Vec<_> = store
        .read_export_page(None, 1000)
        .unwrap()
        .facts
        .into_iter()
        .filter_map(|fact| match fact.segment {
            UsageExportSegment::ModelStep(fact) => {
                Some((fact.session.session, fact.run_id, fact.step_id))
            }
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

fn walk_to_end(rig: &Rig, now: u64) -> Walk {
    let mut walk = Walk::begin(&rig.store, now).unwrap();
    while !walk.slice(&rig.store, &rig.wal, Duration::ZERO, now) {}
    walk
}

#[tokio::test]
async fn a_committed_step_reaches_the_export_on_the_pass_after_its_barrier() {
    let rig = rig("live");
    let stop = CancellationToken::new();
    let service = Service {
        store: rig.store.clone(),
        wal: rig.wal.clone(),
        hints: rig.hints.clone(),
        stop: stop.clone(),
    }
    .spawn();
    let s = session("live");
    let now = now_ms();
    let mut wal = rig.wal.open(&s).unwrap();
    wal.append(1, &started(&s, "run-live", now), true).unwrap();
    wal.append(1, &attempt(1, now + 1), false).unwrap();
    wal.append(1, &step(1), true).unwrap();
    broca_test_support::wait_until("the first step fact after its barrier", || async {
        if step_ids(&rig.store).is_empty() {
            Err("no step fact yet".to_string())
        } else {
            Ok(())
        }
    })
    .await;
    assert_eq!(
        step_ids(&rig.store),
        [("live".to_string(), "run-live".to_string(), 1)]
    );
    // A step that is written but not yet behind a barrier is not a fact.
    wal.append(1, &attempt(2, now + 2), false).unwrap();
    wal.append(1, &step(2), false).unwrap();
    // Absence cannot be awaited as an event: give the service a few passes (it runs every
    // 250 ms) to show it would wrongly write the undurable step, then check it did not.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(step_ids(&rig.store).len(), 1);
    wal.sync().unwrap();
    broca_test_support::wait_until("the second step fact after its barrier", || async {
        let count = step_ids(&rig.store).len();
        if count < 2 {
            Err(format!("{count} step facts"))
        } else {
            Ok(())
        }
    })
    .await;
    // Step facts are the only rows: writing them created no session (billing) fact for the
    // still-running run.
    assert!(rig
        .store
        .read_export_page(None, 1000)
        .unwrap()
        .facts
        .iter()
        .all(|fact| fact.segment.as_session().is_none()));
    stop.cancel();
    service.await.unwrap();
}

#[test]
fn a_step_made_durable_before_a_crash_is_written_once_by_the_next_start() {
    let rig = rig("crash");
    let s = session("crash");
    let now = now_ms();
    // The barrier happened and the process died before any projection ran.
    write_run(&rig, &s, "run-crash", now - DAY_MS, 2);
    assert!(step_ids(&rig.store).is_empty());

    let walk = walk_to_end(&rig, now);
    assert_eq!(walk.written, 2);
    assert_eq!(step_ids(&rig.store).len(), 2);
    // The next start walks again (and a late live hint arrives too): nothing is written twice.
    let walk = walk_to_end(&rig, now + 1);
    assert_eq!(walk.written, 0);
    project_session(&rig.store, &rig.wal, &s, now + 2, Some(u64::MAX)).unwrap();
    assert_eq!(step_ids(&rig.store).len(), 2);
}

#[test]
fn the_backfill_covers_thirty_days_resumes_after_a_restart_and_is_idempotent() {
    let rig = rig("backfill");
    let now = now_ms();
    let recent: Vec<_> = (0..4).map(|n| session(&format!("recent-{n}"))).collect();
    for (n, s) in recent.iter().enumerate() {
        write_run(
            &rig,
            s,
            &format!("run-{n}"),
            now - (n as u64 + 1) * DAY_MS,
            2,
        );
    }
    // A session whose only run ended 40 days ago yields nothing.
    let old = session("old");
    write_run(&rig, &old, "run-old", now - 40 * DAY_MS, 3);

    // The first start visits one session and is killed.
    let mut first = Walk::begin(&rig.store, now).unwrap();
    assert!(!first.slice(&rig.store, &rig.wal, Duration::ZERO, now));
    assert_eq!(first.visited, 1);
    drop(first);

    // The restart resumes after the stored cursor, then walks once more since the interrupted
    // walk began, and the result is every recent step exactly once.
    let later = now + 1000;
    walk_to_end(&rig, later);
    let ids = step_ids(&rig.store);
    assert_eq!(ids.len(), 8, "{ids:?}");
    assert!(ids.iter().all(|(session, _, _)| session != "old"));

    // Losing every checkpoint and walking again derives the same facts and writes none.
    let before = rig.store.read_export_page(None, 1000).unwrap();
    {
        let raw = rusqlite::Connection::open(rig.root.join("run-index.db")).unwrap();
        raw.execute("DELETE FROM model_step_projection", [])
            .unwrap();
        raw.execute("DELETE FROM model_step_walk", []).unwrap();
    }
    let walk = walk_to_end(&rig, later + 1000);
    assert_eq!(walk.written, 0);
    let after = rig.store.read_export_page(None, 1000).unwrap();
    let bytes = |page: &broca_core::ExportPage| {
        page.facts
            .iter()
            .map(|fact| {
                (
                    fact.export_seq,
                    serde_json::to_string(&fact.segment).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(bytes(&after), bytes(&before));
}

#[test]
fn the_sweep_runs_in_bounded_batches_until_nothing_is_due() {
    let rig = rig("sweep");
    let s = session("sweep");
    let now = now_ms();
    write_run(&rig, &s, "run-sweep", now - DAY_MS, 3);
    walk_to_end(&rig, now);
    assert_eq!(step_ids(&rig.store).len(), 3);
    assert_eq!(sweep_expired(&rig.store, now).unwrap(), 0);
    assert_eq!(
        sweep_expired(&rig.store, now - DAY_MS + MODEL_STEP_FACT_RETENTION_MS + 10).unwrap(),
        3
    );
    assert!(step_ids(&rig.store).is_empty());
    assert!(rig.store.missing_terminal_runs().unwrap().len() == 1);
}

/// The golden is one fact captured from the real writer, byte for byte. A consumer pins it, so
/// this decoder must read it and write back exactly the same bytes.
#[test]
fn the_captured_golden_fact_decodes_and_reencodes_byte_for_byte() {
    let golden = include_str!("../tests/golden/model_step_fact.json").trim_end_matches('\n');
    let decoded: UsageExportSegment = serde_json::from_str(golden).unwrap();
    let UsageExportSegment::ModelStep(fact) = &decoded else {
        panic!("the golden is not a model step fact")
    };
    assert_eq!(
        fact.fact_id,
        broca_core::model_step_fact::model_step_fact_id(
            &fact.lineage_id,
            &fact.run_id,
            fact.step_id
        )
    );
    assert_eq!(serde_json::to_string(&decoded).unwrap(), golden);
}
