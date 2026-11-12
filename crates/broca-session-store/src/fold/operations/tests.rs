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
        named_speaker: None,
        imported_ids: None,
        message_marks: None,
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
        mid: None,
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
        by_model: Vec::new(),
    })
}

fn batch(ids: &[&str]) -> Record {
    Record::BatchStart(BatchStart {
        run_id: "r".into(),
        batch_id: "b".into(),
        step_id: 1,
        planned: ids.iter().map(|id| ToolCallId((*id).into())).collect(),
        mid: None,
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
        key_derived: None,
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
        call_key: None,
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
fn session_class_adoption_is_first_label_only_and_checkpoint_compatible() {
    use crate::codec::Message as _;
    let labelled = |class: &str| {
        let Record::RunStart(mut start) = start() else {
            unreachable!()
        };
        start.session_class = Some(class.into());
        Record::RunStart(start)
    };
    let operations = fold(&[start(), labelled("head"), labelled("mason")]).unwrap();
    assert_eq!(
        operations.owner.adopted_session_class.as_deref(),
        Some("head")
    );
    assert!(operations
        .owner
        .first_start
        .as_ref()
        .unwrap()
        .session_class
        .is_none());
    let mut checkpoint = crate::checkpoint::Checkpoint {
        covers_through: 3,
        next_seq: 4,
        conversation: crate::fold::conversation::Conversation::new(false),
        operational: operations,
        transcript_base: None,
        ledger_base: None,
        activity_ms: None,
        media: None,
    };
    let decoded = crate::checkpoint::Checkpoint::decode(&checkpoint.encode()).unwrap();
    assert_eq!(
        decoded.operational.owner.adopted_session_class.as_deref(),
        Some("head")
    );
    assert!(
        crate::codec::Fields::new(&decoded.operational.owner.encode(), "Owner")
            .any(|field| field.unwrap().number == 7)
    );
    checkpoint.operational.owner.adopted_session_class = None;
    assert!(
        !crate::codec::Fields::new(&checkpoint.operational.owner.encode(), "Owner")
            .any(|field| field.unwrap().number == 7)
    );
    let decoded = crate::checkpoint::Checkpoint::decode(&checkpoint.encode()).unwrap();
    assert_eq!(decoded.operational.owner.adopted_session_class, None);
    let field = super::Owner::SCHEMA
        .iter()
        .find(|field| field.name == "adopted_session_class")
        .unwrap();
    assert_eq!(field.reset, "never");
    assert_eq!(field.bound, crate::fold::Bound::Fixed);
    assert_eq!(
        fold(&[labelled("head"), labelled("mason")])
            .unwrap()
            .owner
            .adopted_session_class,
        None
    );
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

/// A call answered without a dispatch intent takes its key from its outcome; a dispatched call
/// keeps its intent's key whatever its outcome carries.
#[test]
fn an_outcome_keys_only_a_call_without_an_intent() {
    let keyed = |id: &str, key: &str| {
        let mut record = outcome(id, CallOutcome::NotDispatched);
        if let Record::ToolOutcome(outcome) = &mut record {
            outcome.call_key = Some(key.into());
        }
        record
    };
    let mut dispatched = intent("a");
    if let Record::ToolIntent(intent) = &mut dispatched {
        intent.call_key = Some("intent-key".into());
    }
    let operations = fold(&[
        start(),
        step_start(),
        step_end(3, false),
        batch(&["a", "b", "c"]),
        dispatched,
        keyed("a", "outcome-key"),
        keyed("b", "g1.9"),
    ])
    .unwrap();
    let batch = operations.run.unwrap().open_batch.unwrap();
    assert_eq!(
        (0..3)
            .map(|position| batch.key(position))
            .collect::<Vec<_>>(),
        [Some("intent-key"), Some("g1.9"), None]
    );
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
        named_speaker: None,
        media: Vec::new(),
        input: vec![Message::new(
            broca_protocol::content::Role::User,
            Vec::new(),
        )],
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        message_marks: None,
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
        named_speaker: None,
        media: Vec::new(),
        submission_id: id.into(),
        send_id: None,
        input: Vec::new(),
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: delivery.map(str::to_owned),
        message_marks: None,
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
            built_through: None,
            new_signed: None,
            applied_by: None,
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

fn started_by(principal: Option<&str>, scope: Option<&str>) -> Record {
    let Record::RunStart(mut start) = start() else {
        unreachable!()
    };
    start.principal = principal.map(|module_id| serde_json::json!({ "reserved": module_id }));
    start.scope = scope.map(|owner| serde_json::json!({ "owner": owner }));
    Record::RunStart(start)
}

/// The owner summary keeps the first run's principal, the first two distinct principals in the
/// order they appeared, whether any run recorded none, and the latest scope; a run start without
/// a scope leaves the scope as it was.
#[test]
fn run_starts_fold_the_owner_summary() {
    let mut ops = Operations::default();
    for record in [
        started_by(Some("a"), Some("first")),
        started_by(Some("a"), None),
        started_by(Some("b"), Some("second")),
        started_by(None, None),
        started_by(Some("c"), None),
    ] {
        ops.apply(&record).unwrap();
    }
    let owner = &ops.owner;
    assert_eq!(owner.episodes, 5);
    assert_eq!(
        owner.first_principal,
        Some(serde_json::json!({ "reserved": "a" }))
    );
    assert!(owner.any_unrecorded_principal);
    assert_eq!(
        owner.principals,
        [
            serde_json::json!({ "reserved": "a" }),
            serde_json::json!({ "reserved": "b" })
        ]
    );
    assert_eq!(owner.scope, Some(serde_json::json!({ "owner": "second" })));
}

fn share(model: &str, variant: Option<&str>, input: Option<u64>) -> crate::record::ModelStepUsage {
    crate::record::ModelStepUsage {
        provider: "anthropic".into(),
        model: model.into(),
        variant: variant.map(str::to_owned),
        usage: broca_protocol::usage::Usage {
            input_tokens: input,
            ..Default::default()
        },
    }
}

/// The session's usage counts every model share a step records, in order of first use: a step
/// two models shared counts once under each and twice in the totals, a step with no counts is
/// unreported, and a sum stays absent until a step reports it.
#[test]
fn session_usage_counts_each_steps_model_shares() {
    let mut first = step_end(0, false);
    let mut second = step_end(0, false);
    let mut third = step_end(0, false);
    if let Record::StepEnd(step) = &mut first {
        step.run_id = "r1".into();
        step.by_model = vec![share("opus", Some("high"), Some(10))];
    }
    if let Record::StepEnd(step) = &mut second {
        step.run_id = "r2".into();
        step.by_model = vec![
            share("opus-4-8", None, Some(5)),
            share("opus", Some("high"), None),
        ];
    }
    if let Record::StepEnd(step) = &mut third {
        step.run_id = "r2".into();
        step.by_model = vec![share("opus", Some("high"), Some(1))];
    }
    let mut ops = Operations::default();
    for record in [first, second, third] {
        ops.apply(&record).unwrap();
    }
    let usage = ops.session_usage.expect("steps counted");
    assert_eq!(usage.since, "r1");
    assert_eq!(usage.totals.steps, 4);
    assert_eq!(usage.totals.unreported_steps, 1);
    assert_eq!(usage.totals.input_tokens, Some(16));
    assert_eq!(usage.totals.output_tokens, None);
    let models: Vec<_> = usage
        .by_model
        .iter()
        .map(|entry| {
            (
                entry.model.as_str(),
                entry.variant.as_deref(),
                entry.totals.steps,
                entry.totals.unreported_steps,
                entry.totals.input_tokens,
            )
        })
        .collect();
    assert_eq!(
        models,
        [
            ("opus", Some("high"), 3, 1, Some(11)),
            ("opus-4-8", None, 1, 0, Some(5))
        ]
    );
}

fn started(run_id: &str, seeded: bool) -> Record {
    let Record::RunStart(mut start) = start() else {
        unreachable!("start() is a run start")
    };
    start.run_id = run_id.into();
    start.seeded = seeded.then_some(true);
    Record::RunStart(start)
}

fn ended(run_id: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run_id.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

fn billed(run_id: &str) -> Record {
    Record::Billed(crate::record::Billed {
        run_id: run_id.into(),
    })
}

/// A run's end makes its billing owed and its `Billed` record settles it, so the owed set holds
/// only runs that ended and aren't billed yet, however many runs the session had.
#[test]
fn a_runs_end_owes_its_billing_until_it_is_billed() {
    let mut records = vec![started("r1", false)];
    assert!(
        fold(&records).unwrap().billing_owed.is_empty(),
        "owed before its end"
    );
    records.push(ended("r1"));
    assert_eq!(fold(&records).unwrap().billing_owed, ["r1"]);
    // A second run ends before the first is billed: both are owed, in the order they ended.
    records.extend([started("r2", false), ended("r2")]);
    assert_eq!(fold(&records).unwrap().billing_owed, ["r1", "r2"]);
    records.push(billed("r1"));
    assert_eq!(fold(&records).unwrap().billing_owed, ["r2"]);
    records.push(billed("r2"));
    for n in 3..20 {
        let run = format!("r{n}");
        records.extend([started(&run, false), ended(&run), billed(&run)]);
    }
    assert!(fold(&records).unwrap().billing_owed.is_empty());
    // A paused run owes nothing: only its end does.
    records.extend([
        started("paused", false),
        Record::Pause(Pause {
            run_id: "paused".into(),
            reason: PauseReason::Restart,
            ts_ms: None,
            error: None,
            account_limit: None,
        }),
    ]);
    assert!(fold(&records).unwrap().billing_owed.is_empty());
}

/// A run seeded from an import or fork never called a provider, so its end owes no billing, and
/// a run started after it is owed as any other.
#[test]
fn a_seeded_runs_end_owes_no_billing() {
    let operations = fold(&[
        started("imported", true),
        ended("imported"),
        started("next", false),
        ended("next"),
    ])
    .unwrap();
    assert_eq!(operations.billing_owed, ["next"]);
}

/// The owed set is part of the operational fold's declared schema, as open work that empties
/// when it is billed, so its growth is accounted for like every other fold field.
#[test]
fn the_owed_set_is_declared_as_open_work() {
    let field = Operations::SCHEMA
        .iter()
        .find(|field| field.name == "billing_owed")
        .expect("the owed set is declared");
    assert!(matches!(field.bound, crate::fold::Bound::OpenWork(_)));
    assert_eq!(field.reset, "billed");
}

/// The session's title is the newest one a send carried, on a run start or in a queued send's
/// or a steer's receipt, and a later send without one leaves it, as today's head reads the
/// newest titled record of its log.
#[test]
fn the_title_is_the_newest_one_any_send_carried() {
    let titled_start = |title: Option<&str>| {
        let mut record = start();
        if let Record::RunStart(start) = &mut record {
            start.title = title.map(str::to_owned);
        }
        record
    };
    let mut titled_queue = queued("q", None);
    if let Record::QueuedSend(send) = &mut titled_queue {
        send.receipt = Some(serde_json::json!({ "title": "queued", "retention": 60 }));
    }
    let steer = |receipt: Option<serde_json::Value>| {
        Record::Steer(Steer {
            named_speaker: None,
            media: Vec::new(),
            input: Vec::new(),
            written_at_ms: None,
            tags_v1: None,
            receipt,
            message_marks: None,
        })
    };
    let title = |records: &[Record]| fold(records).unwrap().title;
    assert_eq!(title(&[start()]), None);
    assert_eq!(
        title(&[titled_start(Some("first"))]).as_deref(),
        Some("first")
    );
    assert_eq!(
        title(&[titled_start(Some("first")), titled_start(None)]).as_deref(),
        Some("first"),
    );
    assert_eq!(
        title(&[titled_start(Some("first")), titled_queue.clone()]).as_deref(),
        Some("queued"),
    );
    assert_eq!(
        title(&[
            titled_start(Some("first")),
            steer(Some(serde_json::json!({ "title": "steered" }))),
        ])
        .as_deref(),
        Some("steered"),
    );
    assert_eq!(
        title(&[
            titled_start(Some("first")),
            steer(Some(serde_json::json!({ "send_id": "s" }))),
            steer(None),
        ])
        .as_deref(),
        Some("first"),
    );
}

fn refresh(generation: u64, identity: &str, selection: Option<serde_json::Value>) -> Record {
    Record::SessionRefresh(crate::record::SessionRefresh {
        generation,
        policy: "on_prefix_rebuild".into(),
        run_in_progress: false,
        applies_from_next_run: false,
        setup: Vec::new(),
        manifest: Some(serde_json::json!({"plan_identity": identity})),
        selection,
    })
}

#[test]
fn the_latest_refresh_gives_the_senders_manifest_and_the_latest_choice_gives_the_model() {
    let chosen = serde_json::json!({"provider": "p", "model": "m2"});
    let operations = fold(&[
        start(),
        refresh(1, "plan-2", Some(chosen.clone())),
        refresh(2, "plan-3", None),
    ])
    .unwrap();
    let plan = operations.plan.expect("the session took refreshes");
    assert_eq!(
        plan.accepted
            .as_ref()
            .and_then(|accepted| accepted.plan_identity()),
        Some("plan-3"),
        "the latest refresh's manifest, from its acceptance"
    );
    assert_eq!(
        plan.selection,
        Some(chosen),
        "a refresh choosing no model keeps the earlier choice"
    );
}
