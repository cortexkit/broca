//! Control-stream projection conformance (Phase 2, B2).
//!
//! `project_control` turns durable WAL records into the ordered control units a
//! subscriber sees, each addressed by a `(wal_seq, sub_index)` cursor. The properties
//! that matter:
//!  - DETERMINISM: same records → same `(Cursor, ControlUnit)` sequence, every time,
//!    independent of map iteration order or anything ambient.
//!  - the SHAPE: each record expands to the expected units in the expected order, and
//!    the cursors are monotonic + correctly sub-indexed.
//!  - CURSOR CONTINUITY: projecting `≤ H` then `> H` reconstructs the full stream with
//!    no gap and no duplicate at the boundary — the property a re-subscribing consumer
//!    depends on.
//!  - a RunPaused is the FINAL unit for a paused run (a subscriber sees the pause as its
//!    terminal-for-this-attach event).

use broca_core::{
    last_control_cursor_through, project_control, project_control_with_state,
    ControlProjectionState, Cursor,
};
use broca_protocol::{
    caller::{ControlUnit, PauseReason, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    error::{ErrorClass, ProviderError},
    request::{GenerationConfig, ModelHandle, SubmissionRequest, ToolChoice},
    BatchId, BindIdentity, BudgetKind, FinishReason, MessageId, RunId, StepId, SubmissionId,
    ToolCallId, Usage,
};
use broca_wal::{
    ApprovalDecision, AuthSelection, DispatchOutcome, FramedRecord, NoDispatchReason,
    QueuedAdmissionFailure, RunConfig, RunOrigin, WalRecord,
};

fn framed(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

fn run_config() -> RunConfig {
    RunConfig {
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: ModelHandle::new("stub", "m"),
        tools: vec![].into(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
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
    }
}

fn started(run: &str) -> WalRecord {
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
        ts_ms: None,
        run_id: RunId(run.into()),
        session: BindIdentity::new("/tmp/p", "h", "s"),
        config: Box::new(run_config()),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "q".into() }],
        }]
        .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

/// An assistant message with two tool calls in a FIXED source order.
fn assistant_two_calls() -> AssistantMessage {
    AssistantMessage {
        message_id: MessageId("m1".into()),
        content: vec![
            ContentBlock::Text {
                text: "calling".into(),
            },
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("c1".into()),
                tool_name: "alpha".into(),
                input: serde_json::json!({"x": 1}),
            }),
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("c2".into()),
                tool_name: "beta".into(),
                input: serde_json::json!({"y": 2}),
            }),
        ],
    }
}

fn step_with_two_calls(step: u64) -> WalRecord {
    WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: (assistant_two_calls()).into(),
        usage: Usage::default(),
        finish_reason: Some(FinishReason::ToolCalls),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn tool_result(id: &str, text: &str) -> WalRecord {
    tool_result_with_outcome(id, text, DispatchOutcome::Completed)
}

fn tool_result_with_outcome(id: &str, text: &str, outcome: DispatchOutcome) -> WalRecord {
    WalRecord::ToolResult {
        tags: None,
        hooks: None,
        batch_id: BatchId("b1".into()),
        result: ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }
        .into(),
        outcome,
        reason: None,
        withdraw_elicitation: None,
        provider_held: None,
    }
}

/// A representative single-turn-with-tools lineage record set.
fn sample_records() -> Vec<FramedRecord> {
    vec![
        framed(1, started("r1")),
        framed(2, WalRecord::StepStarted { step_id: StepId(1) }),
        framed(3, step_with_two_calls(1)),
        framed(
            4,
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("c1".into()), ToolCallId("c2".into())],
            },
        ),
        framed(5, tool_result("c1", "did-c1")),
        framed(6, tool_result("c2", "did-c2")),
        framed(
            7,
            WalRecord::ToolBatchFinished {
                batch_id: BatchId("b1".into()),
            },
        ),
        framed(8, WalRecord::TurnFinished { step_id: StepId(1) }),
        framed(
            9,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]
}

/// Determinism: projecting the SAME records many times yields an IDENTICAL sequence.
/// (A non-determinism from map iteration order would show up as a flaky ordering here.)
#[test]
fn run_started_projects_submission_id_on_live_and_replay_paths() {
    let submission_id = SubmissionId("sub-queued".into());
    let mut queued = started("queued");
    if let WalRecord::RunStarted {
        submission_id: record_submission_id,
        ..
    } = &mut queued
    {
        *record_submission_id = Some(submission_id.clone());
    } else {
        unreachable!("started helper always returns RunStarted");
    }
    let records = vec![framed(1, started("direct")), framed(2, queued)];
    let expected = vec![
        (
            Cursor::new(1, 0),
            ControlUnit::RunStarted {
                run_id: RunId("direct".into()),
                submission_id: None,
            },
        ),
        (
            Cursor::new(2, 0),
            ControlUnit::RunStarted {
                run_id: RunId("queued".into()),
                submission_id: Some(submission_id),
            },
        ),
    ];

    assert_eq!(project_control(&records), expected);

    let mut state = ControlProjectionState::default();
    let mut split_projection = project_control_with_state(&records[..1], &mut state);
    split_projection.extend(project_control_with_state(&records[1..], &mut state));
    assert_eq!(split_projection, expected);
}

#[test]
fn projection_is_deterministic() {
    let recs = sample_records();
    let first = project_control(&recs);
    for _ in 0..50 {
        assert_eq!(
            project_control(&recs),
            first,
            "projection must be deterministic"
        );
    }
}

/// The SHAPE: each record expands to the expected units, cursors are monotonic, and a
/// multi-unit record (the model step → assistant + 2 tool calls + step-finished) is
/// correctly sub-indexed 0..N within its seq.
#[test]
fn projection_shape_and_cursors() {
    let proj = project_control(&sample_records());

    // The model step at seq 3 expands to 4 units in order: AssistantMessage,
    // ToolCall(c1), ToolCall(c2), StepFinished — sub_index 0,1,2,3.
    let step_units: Vec<&(Cursor, ControlUnit)> =
        proj.iter().filter(|(c, _)| c.wal_seq == 3).collect();
    assert_eq!(step_units.len(), 4, "model step expands to 4 units");
    assert_eq!(step_units[0].0, Cursor::new(3, 0));
    assert!(matches!(
        step_units[0].1,
        ControlUnit::AssistantMessage { .. }
    ));
    assert!(matches!(
        &step_units[1].1,
        ControlUnit::ToolCall { call } if call.tool_call_id.0 == "c1" && call.tool_name == "alpha"
    ));
    assert!(matches!(
        &step_units[2].1,
        ControlUnit::ToolCall { call } if call.tool_call_id.0 == "c2" && call.tool_name == "beta"
    ));
    assert_eq!(step_units[3].0, Cursor::new(3, 3));
    assert!(matches!(step_units[3].1, ControlUnit::StepFinished { .. }));

    // The tool calls carry the REAL name+input from the assistant message, not empty
    // placeholders (the deterministic source is the message's content Vec).
    if let ControlUnit::ToolCall { call } = &step_units[1].1 {
        assert_eq!(call.input, serde_json::json!({"x": 1}));
    }

    // Cursors are globally monotonic (lineage order) and the stream begins/ends right.
    let cursors: Vec<Cursor> = proj.iter().map(|(c, _)| *c).collect();
    let mut sorted = cursors.clone();
    sorted.sort();
    assert_eq!(cursors, sorted, "cursors are emitted in monotonic order");
    assert!(matches!(
        proj.first().unwrap().1,
        ControlUnit::RunStarted { .. }
    ));
    assert!(matches!(
        proj.last().unwrap().1,
        ControlUnit::RunFinished { .. }
    ));
}

#[test]
fn run_terminal_carries_last_recorded_step_finish_reason() {
    let terminal_step = WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(1),
        assistant_message: (AssistantMessage {
            message_id: MessageId("m-length".into()),
            content: vec![ContentBlock::Text {
                text: "truncated".into(),
            }],
        })
        .into(),
        usage: Usage::default(),
        finish_reason: Some(FinishReason::Length),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    };
    let records = vec![
        framed(1, started("r1")),
        framed(2, terminal_step),
        framed(
            3,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ];

    assert!(matches!(
        project_control(&records).last().map(|(_, unit)| unit),
        Some(ControlUnit::RunFinished {
            final_step_finish_reason: Some(FinishReason::Length),
            ..
        })
    ));
}

#[test]
fn interrupted_projection_matches_replay_and_excludes_abandoned_call() {
    let mut records = vec![
        framed(1, started("r-interrupted")),
        framed(2, step_with_two_calls(1)),
        framed(
            3,
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("c1".into()), ToolCallId("c2".into())],
            },
        ),
        framed(
            4,
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("c1".into()),
                tool_name: Some("alpha".into()),
                args: serde_json::json!({"x": 1}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ),
        framed(
            5,
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("c2".into()),
                tool_name: Some("beta".into()),
                args: serde_json::json!({"y": 2}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ),
        framed(
            6,
            tool_result_with_outcome("c1", "outcome unknown", DispatchOutcome::Unknown),
        ),
        // Sequential execution abandoned c2 after c1 became ambiguous. Its synthetic Interrupted
        // result proves c2 did not run, so replay and projection must both exclude it.
        framed(
            7,
            tool_result_with_outcome("c2", "not run", DispatchOutcome::Interrupted),
        ),
        framed(
            8,
            WalRecord::ToolBatchFinished {
                batch_id: BatchId("b1".into()),
            },
        ),
        framed(9, WalRecord::TurnFinished { step_id: StepId(1) }),
    ];
    let replay_records: Vec<_> = records.iter().map(|framed| framed.record.clone()).collect();
    let replay_indeterminate = match broca_core::replay(&replay_records)
        .expect("well-formed interrupted lineage replays")
        .resolution
    {
        broca_core::ReplayResolution::Interrupted { indeterminate } => indeterminate,
        other => panic!("expected interrupted replay resolution, got {other:?}"),
    };
    assert_eq!(
        replay_indeterminate,
        vec![ToolCallId("c1".into())],
        "the abandoned c2 has a definite synthetic terminal and is not ambiguous"
    );

    records.push(framed(
        10,
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let projected_indeterminate = project_control(&records)
        .into_iter()
        .find_map(|(_, unit)| match unit {
            ControlUnit::RunFinished {
                indeterminate_tool_calls,
                ..
            } => Some(indeterminate_tool_calls),
            _ => None,
        })
        .expect("the durable terminal projects a RunFinished");
    assert_eq!(projected_indeterminate, Some(replay_indeterminate.clone()));

    let reconciled_records: Vec<_> = records.iter().map(|framed| framed.record.clone()).collect();
    let reconciled = broca_core::reconcile_run_state(&reconciled_records, "r-interrupted")
        .expect("the interrupted run is present in WAL reconciliation");
    assert_eq!(
        reconciled.indeterminate_tool_calls,
        Some(replay_indeterminate),
        "run.status WAL reconciliation uses the same pairing result"
    );
}

#[test]
fn clean_interrupted_terminal_projects_present_empty_set() {
    let projected = project_control(&[
        framed(1, started("r-clean-interrupt")),
        framed(
            2,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Interrupted,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]);
    let terminal = &projected.last().expect("projected terminal").1;
    assert!(matches!(
        terminal,
        ControlUnit::RunFinished {
            indeterminate_tool_calls: Some(calls),
            ..
        } if calls.is_empty()
    ));
    assert_eq!(
        serde_json::to_value(terminal).unwrap()["indeterminate_tool_calls"],
        serde_json::json!([]),
        "a present empty set must remain distinct on the wire from an absent field"
    );
}

#[test]
fn completed_terminal_omits_indeterminate_set() {
    let records = vec![
        framed(1, started("r-completed")),
        framed(
            2,
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("c1".into())],
            },
        ),
        framed(
            3,
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("c1".into()),
                tool_name: Some("alpha".into()),
                args: serde_json::json!({}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ),
        framed(
            4,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ];
    let terminal = project_control(&records)
        .into_iter()
        .find_map(|(_, unit)| matches!(unit, ControlUnit::RunFinished { .. }).then_some(unit))
        .expect("projected terminal");
    assert!(matches!(
        terminal,
        ControlUnit::RunFinished {
            indeterminate_tool_calls: None,
            ..
        }
    ));
    assert!(
        serde_json::to_value(terminal)
            .unwrap()
            .get("indeterminate_tool_calls")
            .is_none(),
        "completed terminals omit interrupted-only metadata"
    );
}

#[test]
fn run_terminal_usage_requires_a_completed_model_step() {
    let terminal = |usage| {
        framed(
            3,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage,
                error: None,
                retries_used: 0,
            },
        )
    };

    let no_step = project_control(&[framed(1, started("no-step")), terminal(Usage::default())]);
    assert!(matches!(
        no_step.last().map(|(_, unit)| unit),
        Some(ControlUnit::RunFinished { usage: None, .. })
    ));

    let zero_step = project_control(&[
        framed(1, started("zero-step")),
        framed(2, step_with_two_calls(1)),
        terminal(Usage::default()),
    ]);
    assert!(matches!(
        zero_step.last().map(|(_, unit)| unit),
        Some(ControlUnit::RunFinished {
            usage: Some(usage),
            ..
        }) if *usage == Usage::default()
    ));
}

#[test]
fn run_terminal_uses_recorded_reason_even_for_tool_call_message() {
    let mut records = sample_records();
    if let WalRecord::ModelStepFinished { finish_reason, .. } = &mut records[2].record {
        *finish_reason = Some(FinishReason::Stop);
    }

    assert!(matches!(
        project_control(&records).last().map(|(_, unit)| unit),
        Some(ControlUnit::RunFinished {
            final_step_finish_reason: Some(FinishReason::Stop),
            ..
        })
    ));
}

/// CURSOR CONTINUITY: split the records at a fsynced head H, project `≤ H` and `> H`
/// separately, and assert the concatenation EQUALS the full projection — no gap, no
/// duplicate at the boundary. This is the re-subscribe property: a consumer holding a
/// cursor at H resumes from `> H` and sees exactly the rest.
#[test]
fn cursor_continuity_across_a_split() {
    let recs = sample_records();
    let full = project_control(&recs);

    for split in 0..=recs.len() {
        let head = &recs[..split];
        let tail = &recs[split..];
        let mut state = ControlProjectionState::default();
        let mut merged = project_control_with_state(head, &mut state);
        merged.extend(project_control_with_state(tail, &mut state));
        assert_eq!(
            merged, full,
            "project(≤H) ⊕ project(>H) must equal project(full) at split {split} (no gap/dup)"
        );
    }
}

/// A RunPaused projects to a single `Paused` unit as the FINAL event for the run, so a
/// subscriber from `start` to a paused run sees the pause as its terminal-for-this-attach
/// event (the resumable analogue of RunFinished).
#[test]
fn paused_run_projects_pause_as_terminal() {
    let recs = vec![
        framed(1, started("r1")),
        framed(2, WalRecord::StepStarted { step_id: StepId(1) }),
        framed(
            3,
            WalRecord::RunPaused {
                run_id: RunId("r1".into()),
                ts_ms: None,
                reason: PauseReason::AuthRequired,
                error: Some(ProviderError {
                    provider_details: None,
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Auth,
                    message: "expired".into(),
                    status: Some(401),
                    retry_after_secs: None,
                    provider_code: None,
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                }),
            },
        ),
    ];
    let proj = project_control(&recs);
    match &proj.last().unwrap().1 {
        ControlUnit::Paused {
            run_id,
            reason,
            error,
        } => {
            assert_eq!(run_id.0, "r1");
            assert_eq!(*reason, PauseReason::AuthRequired);
            assert_eq!(error.as_ref().map(|e| e.message.as_str()), Some("expired"));
        }
        other => panic!("expected Paused as the terminal unit, got {other:?}"),
    }
    // No RunFinished was projected — a pause is not a terminal.
    assert!(
        !proj
            .iter()
            .any(|(_, u)| matches!(u, ControlUnit::RunFinished { .. })),
        "a paused run has no RunFinished in its projection"
    );
}

/// A typed terminal error is surfaced BEFORE the RunFinished, so a subscriber sees the
/// error detail then the terminal.
#[test]
fn error_terminal_surfaces_error_then_finished() {
    let recs = vec![
        framed(1, started("r1")),
        framed(
            2,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Error,
                usage: Usage::default(),
                error: Some(ProviderError {
                    provider_details: None,
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Permanent,
                    message: "boom".into(),
                    status: Some(500),
                    retry_after_secs: None,
                    provider_code: None,
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    affordable_max_tokens: None,
                }),
                retries_used: 0,
            },
        ),
    ];
    let proj = project_control(&recs);
    let tail: Vec<&ControlUnit> = proj.iter().rev().take(2).map(|(_, u)| u).collect();
    // rev: last is RunFinished, second-last is Error.
    assert!(matches!(tail[0], ControlUnit::RunFinished { .. }));
    assert!(matches!(tail[1], ControlUnit::Error { .. }));
}

#[test]
fn failed_queued_admission_projects_atomic_terminal() {
    let error = ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Permanent,
        message: "unknown model".into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some("queued_admission_failed".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    let projected = project_control(&[framed(
        7,
        WalRecord::Retracted {
            submission_id: SubmissionId("sub-b".into()),
            failure: Some(QueuedAdmissionFailure {
                run_id: RunId("b".into()),
                error: error.clone(),
            }),
        },
    )]);

    assert_eq!(projected.len(), 3);
    assert_eq!(projected[0].0, Cursor::new(7, 0));
    assert!(matches!(
        &projected[0].1,
        ControlUnit::RunStarted { run_id, submission_id }
            if run_id.0 == "b" && submission_id == &Some(SubmissionId("sub-b".into()))
    ));
    assert!(matches!(
        &projected[1].1,
        ControlUnit::Error { error: actual } if actual.as_inner() == &error
    ));
    assert!(matches!(
        &projected[2].1,
        ControlUnit::RunFinished {
            reason: RunFinishReason::Error,
            ..
        }
    ));
}

/// PER-RECORD == BATCH (the fan-out's "same function" guarantee): projecting each record
/// ONE AT A TIME and concatenating must yield the EXACT same `(Cursor, ControlUnit)`
/// sequence as projecting the whole slice at once. This is what makes the live control
/// stream (the projector projects one observed record at a time) identical to the attach
/// replay (which projects a batch of records) — the cursors must match unit-for-unit, or a
/// live unit and its replayed twin would carry different cursors and the seq≤H dedup would
/// break. `sub_index` is within-record 0-based so it MUST hold; this proves it.
#[test]
fn per_record_projection_equals_batch_projection() {
    let recs = sample_records();
    let batch = project_control(&recs);

    let mut per_record = Vec::new();
    let mut state = ControlProjectionState::default();
    for rec in &recs {
        per_record.extend(project_control_with_state(
            std::slice::from_ref(rec),
            &mut state,
        ));
    }
    assert_eq!(
        per_record, batch,
        "projecting records one-at-a-time (the live projector path) must equal projecting \
         them as a batch (the attach-replay path) — same cursors, same units, same order"
    );
}

/// COMPLETENESS (the gate of the projection-as-sole-control-source model): every
/// `ControlUnit` the run loop emits to its control stream MUST be `ControlProjectable`, so
/// it survives being sourced from the WAL projection (the fan-out ignores control on the
/// live `out` channel to avoid double-publishing). A control unit the loop emits that
/// isn't projectable would be SILENTLY LOST on both the live and resubscribe paths.
///
/// The 9 units below are exactly those the loop emits to `out` (traced in run.rs). The
/// `classify_control_unit` match is EXHAUSTIVE (no catch-all), so a NEW ControlUnit variant
/// is a COMPILE error until classified — this test then guarantees that if it's a unit the
/// loop emits, it was classified ControlProjectable (not silently DisplayLiveOnly /
/// NotEmittedByLoop). The two non-emitted variants (ApprovalRequest, DisplayReset) are
/// asserted to be classified as NOT ControlProjectable, documenting the boundary.
#[test]
fn every_loop_emitted_control_unit_is_projectable() {
    use broca_core::{classify_control_unit, ControlClass};
    use broca_protocol::approval::ApprovalRequest;

    // The exact set the loop emits to `out` (run.rs emit sites).
    let loop_emitted: Vec<ControlUnit> = vec![
        ControlUnit::RunStarted {
            run_id: RunId("r".into()),
            submission_id: None,
        },
        ControlUnit::StepStarted { step_id: StepId(1) },
        ControlUnit::BudgetFired {
            run_id: RunId("r".into()),
            kind: BudgetKind::Nudge,
            threshold: 2,
            ordinal: 3,
        },
        ControlUnit::AssistantMessage {
            message: assistant_two_calls().into(),
        },
        ControlUnit::ToolCall {
            call: ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("c".into()),
                tool_name: "t".into(),
                input: serde_json::Value::Null,
            },
        },
        ControlUnit::ToolResult {
            result: ToolResult {
                tool_call_id: ToolCallId("c".into()),
                output: ToolOutput::Text { text: "r".into() },
                is_error: false,
            },
        },
        ControlUnit::StepFinished {
            step_id: StepId(1),
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
        },
        ControlUnit::Error {
            error: ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Permanent,
                message: "e".into(),
                status: None,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }
            .into(),
        },
        ControlUnit::Paused {
            run_id: RunId("r".into()),
            reason: PauseReason::AuthRequired,
            error: None,
        },
        ControlUnit::RunFinished {
            run_id: None,
            reason: RunFinishReason::Completed,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            retries_used: 0,
        },
    ];
    for unit in &loop_emitted {
        assert_eq!(
            classify_control_unit(unit),
            ControlClass::ControlProjectable,
            "control unit {unit:?} is emitted by the loop but is NOT projectable — it would \
             be silently lost from the control stream"
        );
    }

    // The boundary: the two variants the loop never emits to the control stream are
    // classified as NOT ControlProjectable (so they're deliberately outside the contract).
    assert_eq!(
        classify_control_unit(&ControlUnit::DisplayReset),
        ControlClass::DisplayLiveOnly
    );
    assert_eq!(
        classify_control_unit(&ControlUnit::ApprovalRequest {
            request: ApprovalRequest {
                tool_call_id: ToolCallId("c".into()),
                tool_name: "t".into(),
                input: serde_json::Value::Null,
            },
        }),
        ControlClass::NotEmittedByLoop
    );
}

#[test]
fn budget_firing_projects_to_control_stream() {
    let projected = project_control(&[framed(
        7,
        WalRecord::BudgetFired {
            tags: None,
            run_id: RunId("r-budget".into()),
            kind: BudgetKind::Finalize,
            threshold: 30,
            ordinal: 8,
            caller_text: None,
        },
    )]);
    assert_eq!(
        projected,
        vec![(
            Cursor::new(7, 0),
            ControlUnit::BudgetFired {
                run_id: RunId("r-budget".into()),
                kind: BudgetKind::Finalize,
                threshold: 30,
                ordinal: 8,
            },
        )]
    );
}

/// One instance of every `WalRecord` variant handled by the exhaustive projection match.
/// Multi-unit records deliberately include two calls and a typed terminal error.
fn all_projection_record_variants() -> Vec<FramedRecord> {
    let queued_request = SubmissionRequest {
        directives: None,
        on_restart: None,
        keep_warm: None,
        scope: Default::default(),
        project_id: None,
        principal: None,
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "queued".into(),
            }],
        }],
        tools: Some(vec![]),
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        auth: None,
        stop_when: vec![],
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen: Default::default(),
        structured_output: None,
        client_message_id: None,
    };
    let records = vec![
        started("r-all"),
        WalRecord::StepStarted { step_id: StepId(1) },
        step_with_two_calls(1),
        WalRecord::ToolBatchStarted {
            batch_id: BatchId("b-all".into()),
            step_id: StepId(1),
            planned_calls: vec![ToolCallId("c1".into()), ToolCallId("c2".into())],
        },
        WalRecord::ToolDispatchIntent {
            hooks: None,
            batch_id: BatchId("b-all".into()),
            tool_call_id: ToolCallId("c1".into()),
            tool_name: Some("alpha".into()),
            args: serde_json::json!({"x": 1}).into(),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        },
        WalRecord::ToolNoDispatch {
            tags: None,
            hooks: None,
            batch_id: BatchId("b-all".into()),
            tool_call_id: ToolCallId("c2".into()),
            reason: NoDispatchReason::Denied {
                reason: "blocked".into(),
            },
        },
        tool_result("c1", "done"),
        WalRecord::ToolBatchFinished {
            batch_id: BatchId("b-all".into()),
        },
        WalRecord::TurnFinished { step_id: StepId(1) },
        WalRecord::BudgetFired {
            tags: None,
            run_id: RunId("r-all".into()),
            kind: BudgetKind::Finalize,
            threshold: 2,
            ordinal: 4,
            caller_text: None,
        },
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Permanent,
                message: "terminal".into(),
                status: Some(500),
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            retries_used: 0,
        },
        WalRecord::RunPaused {
            run_id: RunId("r-all".into()),
            ts_ms: None,
            reason: PauseReason::AuthRequired,
            error: None,
        },
        WalRecord::RunConfigRefrozen {
            renderer_version: 2,
            ts_ms: None,
            frozen_render_config: serde_json::json!({"wire": "config"}),
        },
        WalRecord::RunAuthRefrozen {
            ts_ms: None,
            auth_selection: AuthSelection {
                credential_id: "chatgpt:openai".into(),
                account_ref: Some("account-all".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            },
        },
        WalRecord::Queued {
            retention: None,
            ts_ms: None,
            hooks: None,
            title: None,
            keep_warm: None,
            conversation_key: None,
            send_id: None,
            delivery: None,
            submission_id: SubmissionId("sub-all".into()),
            request: Box::new(queued_request),
            request_identity_digest: None,
            principal: None,
        },
        WalRecord::Retracted {
            submission_id: SubmissionId("sub-all".into()),
            failure: None,
        },
        WalRecord::ApprovalRequested {
            tool_call_id: ToolCallId("c1".into()),
        },
        WalRecord::ApprovalResolved {
            tool_call_id: ToolCallId("c1".into()),
            decision: ApprovalDecision::Allow,
        },
    ];

    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| framed(index as u64 + 1, record))
        .collect()
}

#[test]
fn cursor_only_projection_matches_full_projection_for_every_record_prefix() {
    let records = all_projection_record_variants();
    let projected = project_control(&records);
    let expected_last = projected.last().map(|(cursor, _)| *cursor);
    assert_eq!(last_control_cursor_through(&records, None), expected_last);

    let max_seq = records.last().expect("fixture is non-empty").seq;
    for through_seq in 0..=max_seq + 1 {
        let expected = projected
            .iter()
            .filter(|(cursor, _)| cursor.wal_seq <= through_seq)
            .map(|(cursor, _)| *cursor)
            .next_back();
        assert_eq!(
            last_control_cursor_through(&records, Some(through_seq)),
            expected,
            "cursor-only projection diverged through WAL sequence {through_seq}"
        );
    }
}

/// A steered message carries no control unit (the stream carries no caller input), so the
/// control stream of a WAL with one is the stream without it, cursor for cursor, and the last
/// cursor through the steer is the one before it.
#[test]
fn a_steered_record_projects_no_control_unit() {
    let steered = WalRecord::Steered {
        tags: None,
        retention: None,
        ts_ms: None,
        title: None,
        send_id: Some("steer-1".into()),
        request_identity_digest: None,
        delivery: Some(broca_protocol::Delivery::Steer),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "write up now".into(),
            }],
        )]
        .into(),
        hooks: None,
    };
    let with = vec![
        framed(1, WalRecord::StepStarted { step_id: StepId(1) }),
        framed(2, steered),
        framed(3, WalRecord::StepStarted { step_id: StepId(2) }),
    ];
    let without: Vec<FramedRecord> = with
        .iter()
        .filter(|frame| frame.seq != 2)
        .cloned()
        .collect();
    assert_eq!(project_control(&with), project_control(&without));
    assert_eq!(project_control(&with).len(), 2);
}
