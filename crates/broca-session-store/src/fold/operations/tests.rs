use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::Message;
use broca_protocol::ids::ToolCallId;
use broca_protocol::request::ModelHandle;

use super::{FoldError, Operations, Resolution};
use crate::codec::Digest;
use crate::record::{
    BatchEnd, BatchStart, CallOutcome, Event, Pause, Record, RunEnd, RunStart, Steer, StepEnd,
    StepStart, ToolHeld, ToolIntent, ToolOutcome,
};

fn start() -> Record {
    Record::RunStart(Box::new(RunStart {
        run_id: "r".into(),
        model: serde_json::from_value::<ModelHandle>(
            serde_json::json!({"provider_module_id": "p", "model_id": "m"}),
        )
        .unwrap(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        render_config: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn step_start() -> Record {
    Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id: 1,
    })
}

fn step_end(calls: u32, stop_met: bool) -> Record {
    Record::StepEnd(StepEnd {
        run_id: "r".into(),
        step_id: 1,
        usage: None,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        call_count: calls,
        fallback: None,
        hooks: None,
        budget_finalize: None,
        total_usage: None,
        stop_met: stop_met.then_some(true),
        limiting_stop_met: None,
    })
}

fn batch(ids: &[&str]) -> Record {
    Record::BatchStart(BatchStart {
        run_id: "r".into(),
        batch_id: "b".into(),
        step_id: 1,
        planned: ids.iter().map(|id| ToolCallId((*id).into())).collect(),
    })
}

fn intent(id: &str) -> Record {
    Record::ToolIntent(ToolIntent {
        run_id: "r".into(),
        batch_id: "b".into(),
        tool_call_id: ToolCallId(id.into()),
        tool_name: None,
        module_id: None,
        args_digest: Digest([0; 32]),
        args: None,
        call_key: None,
        approval_digest: None,
        dispatched_at_ms: None,
        schema_pin: None,
        hooks: None,
    })
}

fn outcome(id: &str, outcome: CallOutcome) -> Record {
    Record::ToolOutcome(ToolOutcome {
        run_id: "r".into(),
        batch_id: "b".into(),
        tool_call_id: ToolCallId(id.into()),
        outcome,
        result_digest: Digest([0; 32]),
        is_error: false,
        reason: None,
        finished_at_ms: None,
        detail: None,
        hooks: None,
    })
}

fn fold(records: &[Record]) -> Result<Operations, FoldError> {
    let mut operations = Operations::default();
    for record in records {
        operations.apply(record)?;
    }
    Ok(operations)
}

fn resolution(records: &[Record]) -> Resolution {
    fold(records).unwrap().run.unwrap().resolution()
}

fn ids(ids: &[&str]) -> Vec<ToolCallId> {
    ids.iter().map(|id| ToolCallId((*id).into())).collect()
}

#[test]
fn a_call_with_an_intent_and_no_outcome_interrupts_the_run() {
    let records = [
        start(),
        step_start(),
        step_end(2, false),
        batch(&["a", "b"]),
        intent("a"),
        outcome("a", CallOutcome::Completed),
        intent("b"),
    ];
    assert_eq!(resolution(&records), Resolution::Interrupted(ids(&["b"])));
}

#[test]
fn a_call_never_sent_resumes_and_an_unknown_outcome_interrupts() {
    let never_sent = [start(), step_start(), step_end(1, false), batch(&["a"])];
    assert_eq!(resolution(&never_sent), Resolution::Resume);
    let unknown = [
        start(),
        step_start(),
        step_end(1, false),
        batch(&["a"]),
        intent("a"),
        outcome("a", CallOutcome::Unknown),
        Record::BatchEnd(BatchEnd {
            run_id: "r".into(),
            batch_id: "b".into(),
        }),
    ];
    assert_eq!(resolution(&unknown), Resolution::Interrupted(ids(&["a"])));
}

#[test]
fn a_held_call_waits_and_a_sequential_successor_is_never_sent() {
    let held = Record::ToolHeld(ToolHeld {
        batch_id: "b".into(),
        tool_call_id: ToolCallId("a".into()),
        call_key: "k".into(),
        module: "m".into(),
        reason: "late".into(),
        detail_digest: None,
        held_at_ms: 1,
        sequential: true,
    });
    let records = [
        start(),
        step_start(),
        step_end(2, false),
        batch(&["a", "b"]),
        intent("a"),
        held,
        intent("b"),
    ];
    let operations = fold(&records).unwrap();
    let run = operations.run.unwrap();
    assert_eq!(run.resolution(), Resolution::ProviderHeld(ids(&["a"])));
    assert!(run.indeterminate().is_empty(), "the successor never left");
}

#[test]
fn a_no_tool_step_ends_the_run_unless_a_steer_extends_it() {
    let ended = [start(), step_start(), step_end(0, true)];
    assert_eq!(
        resolution(&ended),
        Resolution::Idle(RunFinishReason::Completed)
    );
    let steer = Record::Steer(Steer {
        input: vec![Message::new(
            broca_protocol::content::Role::User,
            Vec::new(),
        )],
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
    });
    let extended = [start(), step_start(), step_end(0, false), steer];
    assert_eq!(resolution(&extended), Resolution::Resume);
}

#[test]
fn a_pause_is_reported_until_a_terminal_ends_the_run() {
    let pause = Record::Pause(Pause {
        run_id: "r".into(),
        reason: PauseReason::AuthRequired,
        ts_ms: None,
        error: None,
        account_limit: None,
    });
    let paused = [start(), pause.clone()];
    assert_eq!(
        resolution(&paused),
        Resolution::Paused(PauseReason::AuthRequired)
    );
    let ended = Record::RunEnd(RunEnd {
        run_id: "r".into(),
        reason: RunFinishReason::Cancelled,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    });
    assert_eq!(
        resolution(&[start(), pause, ended]),
        Resolution::Idle(RunFinishReason::Cancelled)
    );
}

/// The refusal-fallback schedule is session-scoped: a run starts with the schedule its run start
/// recorded, and a step that changes it replaces it.
#[test]
fn the_refusal_schedule_comes_from_the_run_start_and_the_steps_that_change_it() {
    let mut first = start();
    if let Record::RunStart(start) = &mut first {
        start.refusal_schedule = Some(serde_json::json!({"phase": "source"}));
    }
    let mut changed = step_end(0, false);
    if let Record::StepEnd(step) = &mut changed {
        step.fallback = Some(serde_json::json!({"refusal_fallback": {"phase": "fallback"}}));
    }
    let unchanged = step_end(0, false);
    let operations = fold(&[
        first.clone(),
        step_start(),
        changed,
        step_start(),
        unchanged,
    ])
    .unwrap();
    assert_eq!(
        operations.refusal_schedule,
        Some(serde_json::json!({"phase": "fallback"}))
    );
    // A new run without a recorded schedule starts with none.
    let operations = fold(&[first, start()]).unwrap();
    assert_eq!(operations.refusal_schedule, None);
}

/// An account-limit pause is cleared when the run continues with a step or ends.
#[test]
fn an_account_limit_pause_clears_when_the_run_continues() {
    let pause = Record::Pause(Pause {
        run_id: "r".into(),
        reason: PauseReason::AccountLimit,
        ts_ms: None,
        error: None,
        account_limit: Some(serde_json::json!({"resume_not_before_ms": 5})),
    });
    let paused = fold(&[start(), pause.clone()]).unwrap().run.unwrap();
    assert!(paused.account_limit_pause.is_some());
    let resumed = fold(&[start(), pause, step_start()]).unwrap().run.unwrap();
    assert_eq!(resumed.account_limit_pause, None);
    assert!(resumed.provider_call_started);
}

fn queued(id: &str, delivery: Option<&str>) -> Record {
    Record::QueuedSend(crate::record::QueuedSend {
        submission_id: id.into(),
        send_id: None,
        input: Vec::new(),
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: delivery.map(str::to_owned),
    })
}

/// Interrupts drain ahead of plain sends, each group in queued order, and a send leaves the
/// queue when a run consumes it or it is retracted.
#[test]
fn the_queue_drains_interrupts_first_and_drops_consumed_and_retracted_sends() {
    let mut consumes = start();
    if let Record::RunStart(start) = &mut consumes {
        start.submission_id = Some("plain-1".into());
    }
    let records = [
        queued("plain-1", None),
        queued("plain-2", None),
        queued("interrupt-1", Some("interrupt")),
        queued("plain-3", None),
        queued("interrupt-2", Some("interrupt")),
        consumes,
        Record::Retraction(crate::record::Retraction {
            submission_id: "plain-3".into(),
            failure: None,
        }),
    ];
    let order: Vec<String> = fold(&records)
        .unwrap()
        .queue
        .into_iter()
        .map(|pending| pending.submission_id)
        .collect();
    assert_eq!(order, ["interrupt-1", "interrupt-2", "plain-2"]);
}

/// An accepted view doesn't replace the applied one until a step applies it.
#[test]
fn an_accepted_view_waits_for_a_step_to_apply_it() {
    use super::{AppliedView, ViewSlot};
    let view = |version: u64| {
        Record::View(crate::record::View {
            version,
            checksum: format!("c{version}"),
            edits: serde_json::json!([]),
        })
    };
    let applied = Record::ViewApplied(crate::record::ViewApplied {
        step_id: 1,
        version: 1,
        checksum: "c1".into(),
    });
    let operations = fold(&[start(), view(1), step_start(), applied, view(2)]).unwrap();
    assert_eq!(
        operations.view_accepted,
        Some(ViewSlot {
            version: 2,
            checksum: "c2".into()
        })
    );
    assert_eq!(
        operations.view_applied,
        Some(AppliedView {
            step_id: 1,
            view: ViewSlot {
                version: 1,
                checksum: "c1".into()
            }
        })
    );
}

#[test]
fn records_it_does_not_fold_yet_refuse_by_name() {
    let event = Record::Event(Event {
        name: "recovery_cut".into(),
        run_id: None,
        body: None,
    });
    assert_eq!(
        fold(&[start(), event]).unwrap_err(),
        FoldError::NotFolded("recovery_cut".into())
    );
}
