use broca_session_store::record::{CallOutcome, Placement, Record};
use broca_wal::{FramedRecord, WalRecord};

use super::convert;

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

fn log(lines: &[&str]) -> Vec<FramedRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|e| panic!("fixture line {i}: {e}")),
        })
        .collect()
}

fn names(records: &[Record]) -> Vec<&'static str> {
    records.iter().map(Record::name).collect()
}

#[test]
fn a_tool_step_splits_into_transcript_and_ledger_records() {
    let run1 = RUN.replace("RUN", "run-1");
    let run2 = RUN.replace("RUN", "run-2");
    let old = log(&[
        &run1,
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"unknown"}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        r#"{"type":"turn_finished","step_id":1}"#,
        r#"{"type":"input_token_cap_frozen","limit":5}"#,
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
        &run2,
    ]);
    let converted = convert(&old, None).unwrap();
    assert_eq!(
        names(&converted.records),
        [
            "SetupStub",
            "RunStart",
            "Input",
            "EntryFacts",
            "StepStart",
            "Assistant",
            "StepEnd",
            "EntryFacts",
            "BatchStart",
            "ToolIntent",
            "ToolReply",
            "ToolOutcome",
            "EntryFacts",
            // The batch that places the turn's results records the facts of its calls.
            "TurnFacts",
            "BatchEnd",
            "Event",
            "RunEnd",
            // Today's storage billed the run when it ended.
            "Billed",
            "RunStart",
            "Input",
            "EntryFacts",
        ]
    );

    let intent = converted
        .records
        .iter()
        .find_map(|r| match r {
            Record::ToolIntent(intent) => Some(intent),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        intent.args, None,
        "arguments equal to the call's input aren't stored twice"
    );

    let outcome = converted
        .records
        .iter()
        .find_map(|r| match r {
            Record::ToolOutcome(outcome) => Some(outcome),
            _ => None,
        })
        .unwrap();
    assert_eq!(outcome.outcome, CallOutcome::Unknown);
    assert!(matches!(
        &converted.records[2],
        Record::Input(input) if input.placement == Placement::RunInput && input.run_id == "run-1"
    ));
    assert!(matches!(
        &converted.records[13],
        // The turn's assistant message is the run's second message. The log names no lineage,
        // so its call has no key and the facts list no call.
        Record::TurnFacts(turn) if turn.ordinal == 2 && turn.calls.is_empty()
    ));
    assert!(matches!(
        &converted.records[15],
        // The event names a run only when the old record did.
        Record::Event(event) if event.name == "input_token_cap_frozen"
            && event.run_id.is_none()
            && event.body == Some(serde_json::json!({"limit": 5}))
    ));
    assert!(matches!(
        &converted.records[16],
        Record::RunEnd(end) if end.run_id == "run-1"
    ));
    assert!(matches!(
        &converted.records[17],
        Record::Billed(billed) if billed.run_id == "run-1"
    ));

    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    // Tools, response format: two parts, each stored once though both runs named them.
    assert_eq!(converted.parts.len(), 2);
    assert_eq!(converted.runs, 2);
    assert_eq!(converted.dropped["turn_finished"].count, 1);
}

#[test]
fn unhandled_fields_wrong_types_and_missing_fields_are_reported_lost() {
    // A planned-call list under a name the converter doesn't take, a step id that isn't a
    // number, and a required field that is missing.
    let mut out = super::Converted::default();
    let mut object = serde_json::Map::new();
    object.insert("batch_id".into(), serde_json::json!("b1"));
    object.insert("planned".into(), serde_json::json!(["c1"]));
    object.insert("step_id".into(), serde_json::json!("one"));
    let mut f = super::Fields::new("tool_batch_started", object);
    assert_eq!(f.req_string("batch_id"), "b1");
    assert_eq!(f.req_u64("step_id"), 0);
    assert_eq!(f.req_u64("attempt"), 0);
    f.finish(&mut out);
    let lost: Vec<&str> = out.lost.keys().map(String::as_str).collect();
    assert_eq!(
        lost,
        [
            "tool_batch_started.attempt (missing)",
            "tool_batch_started.planned",
            "tool_batch_started.step_id (not an unsigned integer)",
        ]
    );
}

#[test]
fn changed_dispatch_arguments_are_kept() {
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{},"retries_used":0}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls -la"}}"#,
    ]);
    let converted = convert(&old, None).unwrap();
    let Some(Record::ToolIntent(intent)) = converted.records.last() else {
        panic!("last record is the intent");
    };
    assert_eq!(intent.args, Some(serde_json::json!({"cmd": "ls -la"})));
}

/// A call answered without a dispatch intent keeps the key today's projection gives it, made
/// from the log's lineage and the seq of its terminal record; a dispatched call's key stays on
/// its intent.
#[test]
fn a_call_answered_without_an_intent_keeps_todays_key_on_its_outcome() {
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{}},{"type":"tool_call","tool_call_id":"c2","tool_name":"nope","input":{}}]},"usage":{},"retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1","c2"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
        r#"{"type":"tool_no_dispatch","batch_id":"b1","tool_call_id":"c2","reason":{"kind":"validation_failed","detail":"no such tool"}}"#,
    ]);
    let converted = convert(&old, Some("lineage")).unwrap();
    let keys: Vec<(String, Option<String>)> = converted
        .records
        .iter()
        .filter_map(|record| match record {
            Record::ToolOutcome(outcome) => {
                Some((outcome.tool_call_id.0.clone(), outcome.call_key.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        keys,
        [
            ("c1".to_owned(), None),
            // The no-dispatch record is the log's seventh.
            ("c2".to_owned(), Some("lineage:7".to_owned())),
        ]
    );
}

/// A refusal-fallback schedule carries to the next run under the same model and is dropped
/// under another: the run start records what today's admission decided.
#[test]
fn a_run_start_records_the_refusal_schedule_admission_carried() {
    use broca_session_store::record::RunStart;
    let schedule = serde_json::json!({
        "source": {"provider_module_id": "p", "model_id": "m"},
        "fallback": {"provider_module_id": "p", "model_id": "fallback"},
        "phase": {"kind": "fallback", "remaining": 9},
        "re_refusals": 0,
    });
    let step = format!(
        r#"{{"type":"model_step_finished","step_id":1,"assistant_message":{{"message_id":"a","content":[{{"type":"text","text":"ok"}}]}},"usage":{{}},"retries_used":0,"refusal_fallback":{schedule}}}"#
    );
    let end = r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#;
    let other_model = RUN
        .replace("RUN", "r3")
        .replace(r#""model_id":"m""#, r#""model_id":"other""#);
    let old = log(&[
        &RUN.replace("RUN", "r1"),
        r#"{"type":"step_started","step_id":1}"#,
        &step,
        end,
        &RUN.replace("RUN", "r2"),
        end,
        &other_model,
    ]);
    let records = convert(&old, None).unwrap().records;
    let schedules: Vec<Option<serde_json::Value>> = records
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => {
                let start: &RunStart = start;
                Some(start.refusal_schedule.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(schedules, [None, Some(schedule), None]);
}

/// A tool wait's warms, read back from the new format, are priced to the same spend as today's
/// loop prices them from today's records after a restart: warms that reached the cache at their
/// recorded price, the run's own keep-warm only, this step only, and a failed warm that reported
/// usage but carries no reservation as unaffordable.
#[test]
fn a_tool_waits_warms_are_priced_as_todays_restart_prices_them() {
    use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick, WarmTiming, WarmWait};
    use broca_session_store::fold::operations::Operations;

    /// Prices a warm by its reported input tokens, so each warm counts its own amount.
    struct Priced;
    #[async_trait::async_trait]
    impl KeepWarmDriver for Priced {
        fn timing(&self, _: &broca_wal::RunConfig) -> Result<WarmTiming, String> {
            Err("not warming".into())
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            None
        }
        fn costs(
            &self,
            _: &broca_wal::RunConfig,
        ) -> Result<broca_core::keep_warm::WarmCosts, String> {
            Err("priced from usage".into())
        }
        fn recorded_cost(
            &self,
            _: &broca_wal::RunConfig,
            usage: &broca_protocol::usage::Usage,
        ) -> Result<f64, String> {
            Ok(usage.input_tokens.unwrap_or(0) as f64)
        }
        async fn warm(&self, _: &broca_wal::RunConfig, _: &broca_protocol::RunId) -> WarmTick {
            WarmTick::Skipped
        }
    }
    let warm = |run: &str, step: u64, trigger: &str, tokens: u64, extra: &str| {
        format!(
            r#"{{"type":"cache_warmed","trigger":"{trigger}","run_id":"{run}","step_id":{step},"request_body_sha256":"{d}","warm_body_sha256":"{d}","usage":{{"input_tokens":{tokens}}},"sent_at_ms":5,"ttl_ms":300000{extra}}}"#,
            d = "a".repeat(64),
        )
    };
    let failed = r#","envelope":{"sent":true,"response_bytes":0,"outcome":"eof_before_completion","http_status":500,"usage":{"input_tokens":9}}"#;
    let tool_step = r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#;
    let run = RUN.replace("RUN", "r1");
    let lines = [
        run.as_str(),
        r#"{"type":"step_started","step_id":1}"#,
        tool_step,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{}}"#,
    ];
    let priced = |extra_warms: &[String]| {
        let mut all: Vec<&str> = lines.to_vec();
        all.extend(extra_warms.iter().map(String::as_str));
        let old = log(&all);
        let config = match &old[0].record {
            broca_wal::WalRecord::RunStarted { config, .. } => (**config).clone(),
            _ => unreachable!("the log starts with its run"),
        };
        let driver: std::sync::Arc<dyn KeepWarmDriver> = std::sync::Arc::new(Priced);
        let run_id = broca_protocol::RunId("r1".into());
        let flags = (Default::default(), Default::default());
        let wait = WarmWait {
            driver: &driver,
            config: &config,
            run_id: &run_id,
            cancel: None,
            unsupported_logged: &flags.0,
            warm_missed: &flags.1,
        };
        let today =
            broca_core::keep_warm_wait::batch_spend(Some(&wait), broca_protocol::StepId(1), &old);
        let mut operations = Operations::default();
        for record in &convert(&old, None).unwrap().records {
            if record.stream() == broca_session_store::record::Stream::Ledger {
                operations.apply(record).unwrap();
            }
        }
        let new = broca_session_loop::run::warm_spend(driver.as_ref(), &config, &operations, 1);
        (today, new)
    };
    let counted = [
        warm("r1", 1, "keep_warm", 4, ""),
        warm("r1", 1, "keep_warm", 6, ""),
        // Not this wait's: another run's, another step's, or not the run's own keep-warm.
        warm("r0", 1, "keep_warm", 100, ""),
        warm("r1", 7, "keep_warm", 100, ""),
        warm("r1", 1, "source_keep_warm", 100, ""),
    ];
    let (today, new) = priced(&counted);
    assert_eq!(today, 10.0, "today's spend counts this wait's own warms");
    assert_eq!(new, today);
    // A failed warm that reported usage but can't be priced stops the wait.
    let mut with_failed = counted.to_vec();
    with_failed.push(warm("r1", 1, "keep_warm", 9, failed));
    let (today, new) = priced(&with_failed);
    assert_eq!(today, f64::INFINITY);
    assert_eq!(new, today);
}

/// A step the run's own model refused converts to a typed record and folds as today's replay
/// reads it: the refusal's schedule applies to the session, its billed usage counts toward the
/// run, and the step it names is the one a resume sends to the fallback model. Once the step
/// ends, nothing is pending.
#[test]
fn a_refused_step_folds_as_todays_replay_reads_it() {
    use broca_session_store::fold::operations::Operations;
    let schedule = |remaining: u64| {
        serde_json::json!({
            "source": {"provider_module_id": "p", "model_id": "m"},
            "fallback": {"provider_module_id": "p", "model_id": "fallback"},
            "phase": {"kind": "fallback", "remaining": remaining},
            "re_refusals": 0,
            "updated_at_ms": 7,
        })
    };
    let refused = format!(
        r#"{{"type":"model_step_refused","step_id":1,"usage":{{"input_tokens":50,"output_tokens":4}},"discarded_blocks":2,"schedule":{},"ts_ms":7}}"#,
        schedule(10)
    );
    let finished = format!(
        r#"{{"type":"model_step_finished","step_id":1,"assistant_message":{{"message_id":"a","content":[{{"type":"text","text":"ok"}}]}},"usage":{{"input_tokens":60,"output_tokens":5}},"retries_used":0,"route":{{"model":{{"provider_module_id":"p","model_id":"fallback"}},"reason":"refusal"}},"refusal_fallback":{}}}"#,
        schedule(9)
    );
    let run = RUN.replace("RUN", "r1");
    let started = r#"{"type":"step_started","step_id":1}"#;
    for (lines, pending) in [
        (vec![run.as_str(), started, refused.as_str()], Some(1)),
        (
            vec![run.as_str(), started, refused.as_str(), finished.as_str()],
            None,
        ),
    ] {
        let old = log(&lines);
        let records: Vec<broca_wal::WalRecord> =
            old.iter().map(|frame| frame.record.clone()).collect();
        let today = broca_core::replay::replay(&records).expect("today's replay reads it");
        let converted = convert(&old, None).unwrap();
        assert!(converted.lost.is_empty(), "{:?}", converted.lost);
        assert!(converted
            .records
            .iter()
            .any(|record| matches!(record, Record::StepRefused(refused) if refused.step_id == 1)));
        let mut operations = Operations::default();
        for record in &converted.records {
            if record.stream() == broca_session_store::record::Stream::Ledger {
                operations.apply(record).expect("the fold reads it");
            }
        }
        assert_eq!(
            operations.refusal_schedule,
            Some(serde_json::to_value(&today.refusal_fallback).unwrap())
        );
        let run = operations.run.expect("a run");
        assert_eq!(run.usage, today.usage, "the refused call's usage counts");
        assert_eq!(run.refused_step, pending);
        assert_eq!(
            run.refused_step.is_some(),
            today
                .refused_steps
                .contains(&broca_protocol::StepId(run.completed_steps + 1)),
            "the step a resume sends to the fallback"
        );
    }
}

/// A session the new storage creates is refused by today's WAL store at the same state root, for
/// reads and writes alike. The head the new storage's local backend writes, rooted at
/// `SESSION_STORAGE_DIR` and addressed by today's `session_addr`, is exactly where today's guard
/// looks: without this, a rollback to today's storage would create a second log for the session.
#[test]
fn todays_wal_store_refuses_a_session_the_new_storage_created() {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    use broca_session_store::head::SessionTriple;
    use broca_session_store::keys::DeploymentKey;
    use broca_session_store::session::Session;

    let state = std::env::temp_dir().join(format!("broca-storage-guard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&state);
    let bind = broca_protocol::BindIdentity::new(state.join("proj"), "runner", "guarded");
    let other = broca_protocol::BindIdentity::new(state.join("proj"), "runner", "other");
    let wal = broca_core::FileWalStore::new(state.join("wal"));
    // The control: before the new storage holds it, the session passes.
    wal.check_move(&bind, true).unwrap();
    let backend = LocalBackend::new(state.join(broca_wal::SESSION_STORAGE_DIR));
    futures::executor::block_on(Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(&bind)),
        SessionTriple {
            project_root: bind.project_root.to_string_lossy().into_owned(),
            harness: bind.harness.clone(),
            session: bind.session.clone(),
        },
        &DeploymentKey::new(1, &[0x5a; 32]),
        true,
    ))
    .unwrap();
    for write in [false, true] {
        match wal.check_move(&bind, write) {
            Err(broca_wal::WalError::NewerWriter { missing, .. }) => {
                assert_eq!(missing, [broca_wal::SESSION_STORAGE_FEATURE]);
            }
            other => panic!("write={write}: expected the session-storage refusal: {other:?}"),
        }
    }
    wal.check_move(&other, true).unwrap();
    std::fs::remove_dir_all(&state).unwrap();
}
