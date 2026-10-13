//! Compaction crash scenarios use the core trait directly, but keep the file WAL and run loop real.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::compaction_provider::{
    answer::StepAnswer,
    setup::{SetupAnswer, SetupRequest},
    status::StepStatus,
};
use broca_core::{
    CacheClock, CompactionPlane, CompactionUnavailable, FileWalStore, RunControl, RunRequest,
    StubReply, StubToolPlane,
};
use broca_protocol::content::{ContentBlock, ToolCall, ToolOutput};
use broca_protocol::{
    AssistantMessage, BindIdentity, FinishReason, MessageId, NormalizedEvent, ProviderControl,
    RunFinishReason, RunId, SubmissionId, ToolCallId, Usage,
};
use broca_wal::{
    CompactionDisposition, CompactionMessageKind, CompactionWaitExit, OpaqueJson, WalRecord,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::{
    cut_log, drive, wal_path, write_log, HarnessPlane, InFlight, KillMethod, KillPoint, KillReport,
    KillTrigger, LogImage, ObservedWalStore, Render, Resumed, RunnerHandle, Scenario,
    ScriptedProvider, Suite,
};

const PROVIDER: &str = "compactor";
const APPLIED_HEAD: &str = "durable compacted head";

fn request() -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[{"role":"user","content":[{"type":"text","text":"original user"}]}],
        "tools":[{"name":"edit","description":"edit","input_schema":{"type":"object"}}]
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("compaction-run".into()),
        BindIdentity::new("/crash-harness/compaction", "caller", "session"),
        SubmissionId("compaction-send".into()),
    );
    request.policy_allowed.insert("edit".into());
    request
}

fn control() -> RunControl {
    RunControl {
        manifest: Some(Arc::new(broca_wal::PlanManifest {
            plan: OpaqueJson::from_json_text(
                json!({"compaction_item":{"provider":PROVIDER,"preset":"summary","params":{}},"composition":{}})
                    .to_string(),
            )
            .unwrap(),
            plan_identity: "compaction-plan".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(vec![]),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: [("compaction-provider".into(), "v1".into())].into(),
            fetches: vec![],
            declarations: vec![],
        })),
        compaction_plane: Some(Arc::new(ScriptedCompaction::default())),
        ..Default::default()
    }
}

fn assistant(id: &str, content: Vec<ContentBlock>, finish: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: finish,
            retries_used: 0,
        }),
    ]
}

fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![assistant(
        "reply",
        vec![ContentBlock::Text {
            text: "done".into(),
        }],
        FinishReason::Stop,
    )]
}

fn tool_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        assistant(
            "tool-reply",
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("edit-1".into()),
                tool_name: "edit".into(),
                input: json!({}),
            })],
            FinishReason::ToolCalls,
        ),
        scripts().remove(0),
    ]
}

fn tools() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "edited".into(),
        })
    })
}

const SCENARIO: Scenario = Scenario {
    name: "compaction",
    request,
    control,
    scripts,
    render: Render::Canonical,
    plane: tools,
    expected: RunFinishReason::Completed,
};

const TOOL_SCENARIO: Scenario = Scenario {
    name: "compaction-tools",
    scripts: tool_scripts,
    ..SCENARIO
};

pub(crate) fn find(name: &str) -> Option<Scenario> {
    [SCENARIO, TOOL_SCENARIO]
        .into_iter()
        .find(|scenario| scenario.name == name)
}

fn replacement(version: u64, text: &str) -> Value {
    json!({"compaction_id":"summary","version":version,"range":{"from":0,"to":if version == 1 { 0 } else { 1 }},
        "replacement":[{"role":"user","content":[{"type":"text","text":text}]}]})
}

#[derive(Default)]
struct ScriptedCompaction {
    setups: AtomicUsize,
    statuses: Mutex<Vec<StepStatus>>,
    delayed: Option<String>,
    wait: Option<CancellationToken>,
    waiting: bool,
}

#[async_trait]
impl CompactionPlane for ScriptedCompaction {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        assert_eq!(provider, PROVIDER);
        assert_eq!(request.harness, bind.harness);
        self.setups.fetch_add(1, Ordering::SeqCst);
        Ok(
            serde_json::from_value(json!({"answer":"ready","request_id":request.request_id,
            "initial":replacement(1, "setup head"),"stability":[{"index":0,"rank":7}]}))
            .unwrap(),
        )
    }

    async fn step(
        &self,
        provider: &str,
        bind: &BindIdentity,
        status: StepStatus,
    ) -> Result<StepAnswer, CompactionUnavailable> {
        assert_eq!(provider, PROVIDER);
        assert_eq!(status.harness, bind.harness);
        self.statuses.lock().unwrap().push(status.clone());
        let answer = if self.waiting {
            // Trip only after entering the compaction call, so the loop first records its wait.
            if let Some(token) = &self.wait {
                token.cancel();
            }
            json!({"answer":"wait","request_id":status.request_id,"reason":"building summary","bound_ms":60000})
        } else if status.step_id == "1" {
            json!({"answer":"compaction_message","request_id":status.request_id,
                "compaction":replacement(2, APPLIED_HEAD)})
        } else if let Some(old_request) = &self.delayed {
            // Deliver the retained reply only on the later tool step, after the fresh apply.
            tokio::task::yield_now().await;
            json!({"answer":"compaction_message","request_id":old_request,
                "compaction":replacement(99, "stale delayed head")})
        } else {
            json!({"answer":"noop","request_id":status.request_id})
        };
        Ok(serde_json::from_value(answer).unwrap())
    }
}

async fn resume_with(
    scenario: Scenario,
    kill: &KillReport,
    compaction: Arc<ScriptedCompaction>,
) -> Resumed {
    let provider = ScriptedProvider::for_scenario(&scenario);
    let plane = HarnessPlane::for_scenario(&scenario);
    let store = FileWalStore::new(kill.state_root.join("wal"));
    let mut control = control();
    control.compaction_plane = Some(compaction);
    let outcome = drive(
        (scenario.request)(),
        control,
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    let log = LogImage::read(&wal_path(&kill.state_root, &request().session));
    Resumed {
        reason: outcome.reason,
        provider,
        plane,
        log,
        outcome,
    }
}

fn has_setup(records: &[WalRecord]) -> bool {
    records.iter().any(|record| {
        matches!(
            record,
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Setup,
                ..
            }
        )
    })
}

fn has_step_answer(records: &[WalRecord]) -> bool {
    records.iter().any(|record| {
        matches!(
            record,
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Step,
                ..
            } | WalRecord::CompactionAnswer {
                disposition: CompactionDisposition::Noop,
                ..
            }
        )
    })
}

#[tokio::test]
async fn compaction_every_record_boundary_and_real_kill_replay_identical_bytes() {
    let handle = RunnerHandle::new(SCENARIO).await;
    let reference = handle.reference.sent.get(&0).expect("reference model call");
    assert!(String::from_utf8_lossy(reference).contains(APPLIED_HEAD));
    assert!(
        !String::from_utf8_lossy(reference).contains("original user"),
        "step must replace raw history, not merely insert a head"
    );
    let mut suite = Suite::new("compaction");
    let mut unsynced = 0;
    let mut durable_answer_resumes = 0;
    let mut durable_answer_model_resumes = 0;
    let mut points = handle.record_boundaries(true);
    points.push(KillPoint::InFlight(InFlight::ProviderStep(1)));
    for point in points {
        let trigger = if matches!(point, KillPoint::InFlight(_)) {
            KillTrigger::ProcessKill
        } else {
            KillTrigger::Truncate
        };
        let kill = handle.kill_at(point, trigger).await;
        unsynced += usize::from(kill.after_sync == Some(false));
        let compaction = Arc::new(ScriptedCompaction::default());
        let resumed = resume_with(SCENARIO, &kill, compaction.clone()).await;
        handle.check(&kill, &resumed);
        let prefix = kill.durable.records();
        if has_setup(&prefix) {
            assert_eq!(
                compaction.setups.load(Ordering::SeqCst),
                0,
                "{}: Setup repeated",
                kill.label()
            );
        }
        if has_step_answer(&prefix) {
            durable_answer_resumes += 1;
            durable_answer_model_resumes += usize::from(resumed.provider.request_count() > 0);
            assert!(
                compaction.statuses.lock().unwrap().is_empty(),
                "{}: durable answer re-called",
                kill.label()
            );
        }
        for bytes in resumed.provider.sent().values() {
            assert_eq!(
                bytes,
                reference,
                "{}: compaction request bytes changed",
                kill.label()
            );
        }
        assert_eq!(
            resumed
                .log
                .records()
                .iter()
                .filter(|record| matches!(
                    record,
                    WalRecord::CompactionMessage {
                        kind: CompactionMessageKind::Setup,
                        ..
                    }
                ))
                .count(),
            1,
            "{}: Setup record count",
            kill.label()
        );
        suite.record([kill]);
    }
    assert!(unsynced > 0, "scenario must cut inside unsynced batches");
    assert!(
        durable_answer_resumes > 0,
        "scenario must resume durable applies"
    );
    assert!(
        durable_answer_model_resumes > 0,
        "durable applies must be rendered on resume, not just sealed"
    );
    let methods = suite.finish();
    assert_eq!(methods.get("Sigkill"), Some(&1));
    assert_eq!(
        methods.get("Truncation"),
        Some(&(2 * handle.reference.log.record_count() + 1))
    );
}

#[tokio::test]
async fn stale_delayed_answer_after_fresh_restart_apply_is_never_applied() {
    let handle = RunnerHandle::new(TOOL_SCENARIO).await;
    let records = handle.reference.log.records();
    let issued = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionRequest { .. }))
        .expect("issued compaction request")
        + 1;
    let WalRecord::CompactionRequest {
        request_id: old_request,
        ..
    } = &records[issued - 1]
    else {
        unreachable!();
    };
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: issued,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let compaction = Arc::new(ScriptedCompaction {
        delayed: Some(old_request.clone()),
        ..Default::default()
    });
    let resumed = resume_with(TOOL_SCENARIO, &kill, compaction.clone()).await;
    handle.check(&kill, &resumed);
    assert_eq!(compaction.setups.load(Ordering::SeqCst), 0);
    let statuses = compaction.statuses.lock().unwrap();
    assert_eq!(
        statuses.len(),
        2,
        "fresh apply must precede delayed reply delivery"
    );
    assert_ne!(statuses[0].request_id, *old_request);
    assert_ne!(statuses[1].request_id, statuses[0].request_id);
    let after = resumed.log.records();
    let applied = after.iter().position(|record| matches!(record,
        WalRecord::CompactionMessage { kind: CompactionMessageKind::Step, version: 2, request_id: Some(id), .. }
        if id == &statuses[0].request_id
    )).expect("fresh post-restart answer was durably applied");
    let rejected = after.iter().position(|record| matches!(record,
        WalRecord::CompactionAnswer { request_id, disposition: CompactionDisposition::SupersededRequest,
            rejected: Some(rejected), .. }
        if request_id == old_request && rejected.version == 99
    )).expect("delayed answer must be recorded as superseded");
    assert!(
        applied < rejected,
        "delayed answer arrived only after fresh apply"
    );
    assert!(
        !after
            .iter()
            .any(|record| matches!(record, WalRecord::CompactionMessage { version: 99, .. })),
        "stale version was applied"
    );
    assert_eq!(resumed.provider.sent(), handle.reference.sent);
    assert_eq!(
        resumed.provider.request_count(),
        2,
        "must observe both post-restart model requests"
    );
    assert!(resumed
        .provider
        .sent()
        .values()
        .all(|bytes| !String::from_utf8_lossy(bytes).contains("stale delayed head")));
}

fn truncate(handle: &RunnerHandle, log: &LogImage, records: usize) -> KillReport {
    let root = handle.fresh_root();
    let cut = cut_log(log, records, false);
    write_log(&wal_path(&root, &request().session), &cut);
    KillReport {
        scenario: SCENARIO.name,
        point: KillPoint::RecordBoundary {
            records,
            torn: false,
        },
        method: KillMethod::Truncation,
        after_sync: None,
        inside_resume_from: None,
        state_root: root,
        durable: LogImage::parse(cut),
    }
}

async fn wait_reference(handle: &RunnerHandle, exit: CompactionWaitExit) -> LogImage {
    let root = handle.fresh_root();
    let token = CancellationToken::new();
    let compaction = Arc::new(ScriptedCompaction {
        wait: Some(token.clone()),
        waiting: true,
        ..Default::default()
    });
    let mut control = control();
    control.compaction_plane = Some(compaction.clone());
    match exit {
        CompactionWaitExit::Cancel => control.cancel = Some(token),
        CompactionWaitExit::Stopped => control.stop_requested = Some(token),
        CompactionWaitExit::Exceeded => {
            let clock = CacheClock::new();
            // An expired cache bounds the wait at zero; the unknown window cannot fit safely.
            clock.note_sent(0, Some(0));
            control.compaction_cache_clock = Some(clock);
        }
        _ => panic!("unsupported reference exit {exit:?}"),
    }
    let model = ScriptedProvider::for_scenario(&SCENARIO);
    let outcome = drive(
        request(),
        control,
        &root,
        &ObservedWalStore::new(root.join("wal")),
        &model,
        &HarnessPlane::for_scenario(&SCENARIO),
    )
    .await;
    assert_eq!(outcome.reason, terminal_reason(exit));
    assert_eq!(model.request_count(), 0, "wait must not send to model");
    assert_eq!(compaction.setups.load(Ordering::SeqCst), 1);
    assert_eq!(compaction.statuses.lock().unwrap().len(), 1);
    let log = LogImage::read(&wal_path(&root, &request().session));
    let records = log.records();
    assert!(
        records.iter().any(|record| matches!(
            record,
            WalRecord::CompactionAnswer {
                disposition: CompactionDisposition::Wait,
                wait: Some(_),
                ..
            }
        )),
        "real loop must write the atomic wait entry"
    );
    assert!(
        records.iter().any(|record| matches!(record,
            WalRecord::CompactionWait { exited, .. } if *exited == exit
        )),
        "real loop must write the requested exit"
    );
    log
}

fn terminal_reason(exit: CompactionWaitExit) -> RunFinishReason {
    match exit {
        CompactionWaitExit::Cancel => RunFinishReason::Cancelled,
        CompactionWaitExit::Stopped | CompactionWaitExit::Interrupted => {
            RunFinishReason::Interrupted
        }
        CompactionWaitExit::Exceeded => RunFinishReason::Error,
        _ => panic!("not a terminal intent"),
    }
}

async fn check_terminal_resume(kill: &KillReport, exit: CompactionWaitExit) -> Resumed {
    let compaction = Arc::new(ScriptedCompaction::default());
    let resumed = resume_with(SCENARIO, kill, compaction.clone()).await;
    assert_eq!(
        resumed.reason,
        terminal_reason(exit),
        "{}: terminal intent lost",
        kill.label()
    );
    assert_eq!(
        compaction.setups.load(Ordering::SeqCst),
        0,
        "terminal recovery re-ran Setup"
    );
    assert!(
        compaction.statuses.lock().unwrap().is_empty(),
        "terminal recovery called compaction"
    );
    assert_eq!(
        resumed.provider.request_count(),
        0,
        "terminal recovery called model"
    );
    assert!(
        resumed.plane.dispatched().is_empty(),
        "terminal recovery dispatched tools"
    );
    let prefix = kill.durable.framed();
    let after = resumed.log.framed();
    assert_eq!(
        &after[..prefix.len()],
        &prefix,
        "terminal recovery rewrote durable records"
    );
    let records = resumed.log.records();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
            .count(),
        1,
        "repeated recovery must seal exactly once"
    );
    let Some(WalRecord::RunFinished { reason, error, .. }) = records.last() else {
        panic!("terminal must be the last record");
    };
    assert_eq!(*reason, terminal_reason(exit));
    if exit == CompactionWaitExit::Exceeded {
        assert_eq!(
            error
                .as_ref()
                .and_then(|error| error.provider_code.as_deref()),
            Some("compaction_wait_exceeded")
        );
    } else {
        assert!(error.is_none());
    }
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record,
                WalRecord::CompactionWait { exited, .. } if *exited == exit
            ))
            .count(),
        1,
        "repeated recovery must not duplicate the wait exit"
    );
    resumed
}

#[tokio::test]
async fn terminal_intent_cuts_and_second_crashes_reach_the_named_terminal() {
    let handle = RunnerHandle::new(SCENARIO).await;
    let stopped = wait_reference(&handle, CompactionWaitExit::Stopped).await;
    let entry = stopped
        .records()
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::CompactionAnswer {
                    disposition: CompactionDisposition::Wait,
                    ..
                }
            )
        })
        .unwrap()
        + 1;
    let open_wait = truncate(&handle, &stopped, entry);
    let interrupted = check_terminal_resume(&open_wait, CompactionWaitExit::Interrupted).await;
    let cancel = wait_reference(&handle, CompactionWaitExit::Cancel).await;
    let exceeded = wait_reference(&handle, CompactionWaitExit::Exceeded).await;
    for (exit, log) in [
        (CompactionWaitExit::Cancel, cancel),
        (CompactionWaitExit::Stopped, stopped),
        (CompactionWaitExit::Interrupted, interrupted.log.clone()),
        (CompactionWaitExit::Exceeded, exceeded),
    ] {
        let at_exit = log
            .records()
            .iter()
            .position(|record| {
                matches!(record,
                    WalRecord::CompactionWait { exited, .. } if *exited == exit
                )
            })
            .unwrap()
            + 1;
        let kill = truncate(&handle, &log, at_exit);
        let first = check_terminal_resume(&kill, exit).await;
        let total = first.log.record_count();
        assert!(total > at_exit, "recovery must append its terminal seal");
        // Early terminal recovery may append only the seal. Re-crash before it as well.
        for records in at_exit..=total {
            let mut second = truncate(&handle, &first.log, records);
            second.inside_resume_from = Some(at_exit);
            check_terminal_resume(&second, exit).await;
        }
    }
    // Re-crash every recovery append of an open wait, including its new interrupted exit.
    for records in entry + 1..=interrupted.log.record_count() {
        let second = truncate(&handle, &interrupted.log, records);
        check_terminal_resume(&second, CompactionWaitExit::Interrupted).await;
    }
}
