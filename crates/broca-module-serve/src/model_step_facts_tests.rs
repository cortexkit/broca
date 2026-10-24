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
    while first.visited == 0 {
        assert!(!first.slice(&rig.store, &rig.wal, Duration::ZERO, now));
    }
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

/// A checkpoint too far behind the WAL's end for a bounded tail read is continued by streaming
/// the WAL from its first record: the steps after the checkpoint are written once, and those
/// before it are not written again.
#[test]
fn a_checkpoint_far_behind_a_large_wal_continues_by_streaming() {
    let rig = rig("far-behind");
    let s = session("far-behind");
    let now = now_ms();
    let t = now - DAY_MS;
    let mut wal = rig.wal.open(&s).unwrap();
    wal.append(1, &started(&s, "run-far", t), true).unwrap();
    wal.append(1, &attempt(1, t + 1), false).unwrap();
    wal.append(1, &step(1), true).unwrap();
    assert_eq!(
        project_session(&rig.store, &rig.wal, &s, now, None).unwrap(),
        Visit::Projected(broca_store::model_step_facts::StepProjectionReport {
            written: 1,
            through_seq: 3,
            ..Default::default()
        })
    );
    // More than the tail cap of records after the checkpoint.
    let text = "y".repeat(256 * 1024);
    for id in 2..=(TAIL_CAP / (256 * 1024) + 4) {
        wal.append(1, &attempt(id, t + id), false).unwrap();
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                tags: None,
                hooks: None,
                step_id: StepId(id),
                assistant_message: AssistantMessage {
                    message_id: MessageId(format!("m{id}")),
                    content: vec![ContentBlock::Text { text: text.clone() }],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::Stop),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            false,
        )
        .unwrap();
    }
    wal.sync().unwrap();
    let steps = TAIL_CAP / (256 * 1024) + 4;
    let Visit::Projected(report) = project_session(&rig.store, &rig.wal, &s, now, None).unwrap()
    else {
        panic!("not projected")
    };
    assert_eq!((report.written, report.already_present), (steps - 1, 0));
    let ids = step_ids(&rig.store);
    assert_eq!(
        ids.iter().map(|(_, _, step)| *step).collect::<Vec<_>>(),
        (1..=steps).collect::<Vec<_>>()
    );
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

/// The fixed lineage id of the golden fixture's WAL (a writer would mint a random one).
const GOLDEN_LINEAGE: [u8; 16] = [
    0x5e, 0x1f, 0x00, 0x51, 0x7a, 0x9c, 0x4d, 0x1e, 0x8b, 0x0a, 0x6f, 0x33, 0x2c, 0x90, 0x41, 0xd7,
];
/// The wall clock the golden projection runs at: one day after the fixture's step finished.
const GOLDEN_NOW: u64 = 1_790_086_400_000;
/// When the fixture's run started.
const GOLDEN_T0: u64 = 1_790_000_000_000;

/// A synthetic session holding one scoped run whose first step was retried once, written by the
/// real WAL writer. The WAL's lineage frame is then replaced by the frame the writer would have
/// written for [`GOLDEN_LINEAGE`], so the fact's id and bytes do not change between runs.
fn golden_wal(rig: &Rig) -> BindIdentity {
    let session = BindIdentity::new(
        "/synthetic/golden-project",
        "golden-harness",
        "golden-session",
    );
    let WalRecord::RunStarted {
        tags,
        session_class,
        retention,
        hooks,
        title,
        on_restart,
        keep_warm,
        marker_order_version,
        manifest,
        send_id,
        request_identity_digest,
        project_id,
        principal,
        run_id,
        session: run_session,
        input,
        origin,
        submission_id,
        mut config,
        ..
    } = started(&session, "run-golden", GOLDEN_T0)
    else {
        unreachable!()
    };
    config.model = ModelHandle::new("anthropic", "claude-synthetic-1");
    config.variant = Some("high".into());
    config.context_limit = Some(200_000);
    config.frozen_render_config = serde_json::json!({
        "family_id": "anthropic_messages",
        "cache": {"version": 1, "class": "markers", "syntax": "anthropic_cache_control",
                  "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20}
    });
    let scope = serde_json::from_value(serde_json::json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "ref": "golden-scope", "scope_epoch": 1,
        "agent_id": "agent-synthetic", "run_id": "agent-run-synthetic"
    }))
    .unwrap();
    let run_started = WalRecord::RunStarted {
        tags,
        session_class,
        retention,
        hooks,
        title,
        on_restart,
        keep_warm,
        marker_order_version,
        manifest,
        scope: Some(scope),
        send_id,
        request_identity_digest,
        project_id,
        principal,
        run_id,
        session: run_session,
        ts_ms: Some(GOLDEN_T0),
        config,
        input,
        origin,
        submission_id,
    };
    let attempt = |ordinal: u32, sent: u64, first_content: Option<u64>, end: u64, outcome| {
        let WalRecord::ModelAttemptFinished {
            content_type,
            response_body_cap,
            transport,
            websocket_request,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh,
            served_account_ref,
            ..
        } = attempt(1, end)
        else {
            unreachable!()
        };
        WalRecord::ModelAttemptFinished {
            step_id: StepId(1),
            attempt: ordinal,
            request_body_len: 48_213 + u64::from(ordinal),
            request_body_sha256: format!("{:064x}", 0x5eed_0000 + u64::from(ordinal)),
            http_status: Some(if ordinal == 0 { 529 } else { 200 }),
            content_type,
            response_body_len: 4_096,
            response_body_sha256: "c".repeat(64),
            response_body_cap,
            outcome,
            ts_ms: end,
            content_progress: Box::new(ContentProgress {
                first_content_at_ms: first_content,
                ..ContentProgress::default()
            }),
            sent_at_ms: Some(sent),
            transport,
            websocket_request,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh,
            served_account_ref,
        }
    };
    let step = WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(1),
        assistant_message: AssistantMessage {
            message_id: MessageId("m-golden".into()),
            content: vec![ContentBlock::Text {
                text: "synthetic answer".into(),
            }],
        }
        .into(),
        usage: Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(41_250),
            cache_write_tokens: Some(1_812),
            output_tokens: Some(640),
            reasoning_tokens: Some(128),
        },
        finish_reason: Some(FinishReason::Stop),
        retries_used: 1,
        service_tier_used: None,
        speed_used: None,
    };
    {
        let mut wal = rig.wal.open(&session).unwrap();
        wal.append(1, &run_started, true).unwrap();
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, false)
            .unwrap();
        wal.append(
            1,
            &attempt(
                0,
                GOLDEN_T0 + 200,
                None,
                GOLDEN_T0 + 1_400,
                AttemptOutcome::HttpError,
            ),
            false,
        )
        .unwrap();
        wal.append(
            1,
            &attempt(
                1,
                GOLDEN_T0 + 3_400,
                Some(GOLDEN_T0 + 5_150),
                GOLDEN_T0 + 18_900,
                AttemptOutcome::Complete,
            ),
            false,
        )
        .unwrap();
        wal.append(1, &step, true).unwrap();
    }
    // The writer follows every record with a copy of the lineage frame, so every copy of the
    // minted frame is replaced, leaving one consistent lineage the way the writer would have.
    let path = rig.wal.wal_path(&session);
    let minted = broca_wal::read_records_range(&path, u64::MAX, 0, None, None)
        .unwrap()
        .lineage_id
        .expect("a fresh WAL carries a lineage frame")
        .to_string()
        .replace('-', "");
    let minted: [u8; 16] = std::array::from_fn(|index| {
        u8::from_str_radix(&minted[index * 2..index * 2 + 2], 16).unwrap()
    });
    let from = broca_wal::lineage_frame_for_test(minted);
    let to = broca_wal::lineage_frame_for_test(GOLDEN_LINEAGE);
    let mut bytes = std::fs::read(&path).unwrap();
    let mut replaced = 0;
    let mut at = 0;
    while at + from.len() <= bytes.len() {
        if bytes[at..at + from.len()] == from {
            bytes[at..at + from.len()].copy_from_slice(&to);
            replaced += 1;
            at += from.len();
        } else {
            at += 1;
        }
    }
    assert!(
        replaced >= 2,
        "the lineage frame leads the file and follows each record"
    );
    std::fs::write(&path, bytes).unwrap();
    session
}

/// The golden fact is produced by the daemon's own projection (`project_session`, the call the
/// live hint and the walk both make) over a synthetic WAL, and stored bytes are compared whole.
/// Set `BROCA_WRITE_GOLDEN=1` to print the produced bytes (and rewrite the file) after a
/// deliberate change to the fact's shape.
#[test]
fn the_golden_fact_is_what_the_daemon_projection_writes() {
    let rig = rig("golden");
    let session = golden_wal(&rig);
    let visit = project_session(&rig.store, &rig.wal, &session, GOLDEN_NOW, None).unwrap();
    let Visit::Projected(report) = visit else {
        panic!("the fixture WAL was not projected: {visit:?}")
    };
    assert_eq!(report.written, 1);
    let stored: String = rusqlite::Connection::open(rig.root.join("run-index.db"))
        .unwrap()
        .query_row(
            "SELECT segment_json FROM export_facts
              WHERE json_extract(segment_json, '$.kind')='model_step'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    if std::env::var_os("BROCA_WRITE_GOLDEN").is_some() {
        println!("GOLDEN:{stored}");
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/golden/model_step_fact.json"
            ),
            format!("{stored}\n"),
        )
        .unwrap();
    }
    assert_eq!(
        stored,
        include_str!("../tests/golden/model_step_fact.json").trim_end_matches('\n')
    );
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
