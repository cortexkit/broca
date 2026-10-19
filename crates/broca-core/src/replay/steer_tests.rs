//! Where replay places a steered message: at the first step boundary after its record, never
//! between a tool call and its result, and exactly once.

use std::sync::Arc;

use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{ModelHandle, StopCondition},
    BatchId, BindIdentity, Delivery, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{DispatchOutcome, FramedRecord, RunConfig, RunOrigin, WalRecord};

use super::{latest_projection_boundary, replay, ReplayResolution};

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn run_started(stop_when: Vec<StopCondition>) -> WalRecord {
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
        run_id: RunId("r1".into()),
        session: BindIdentity::new("/tmp/project", "runner", "session"),
        config: Box::new(RunConfig {
            render_tags: false,
            model: ModelHandle::new("stub", "m"),
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
            stop_when,
            context_management: None,
        }),
        input: vec![user("task")].into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

fn steered(text: &str) -> WalRecord {
    WalRecord::Steered {
        tags: None,
        retention: None,
        ts_ms: None,
        title: None,
        send_id: Some(format!("steer-{text}")),
        request_identity_digest: None,
        delivery: Some(Delivery::Steer),
        input: Arc::new(vec![user(text)]),
        hooks: None,
    }
}

fn step_started(step: u64) -> WalRecord {
    WalRecord::StepStarted {
        step_id: StepId(step),
    }
}

fn text_step(step: u64, text: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: Arc::new(AssistantMessage {
            message_id: MessageId(format!("m{step}")),
            content: vec![ContentBlock::Text { text: text.into() }],
        }),
        usage: Usage::default(),
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn call_step(step: u64, call: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: Arc::new(AssistantMessage {
            message_id: MessageId(format!("m{step}")),
            content: vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(call.into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            })],
        }),
        usage: Usage::default(),
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

/// A tool turn for `call` in step `step`, split so a test can put a record between the dispatch
/// intent and the result: (batch start + intent, result + batch end + turn end).
fn tool_turn(step: u64, call: &str) -> (Vec<WalRecord>, Vec<WalRecord>) {
    let batch_id = BatchId(format!("r1-b{step}"));
    let id = ToolCallId(call.into());
    (
        vec![
            WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(step),
                planned_calls: vec![id.clone()],
            },
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: batch_id.clone(),
                tool_call_id: id.clone(),
                tool_name: Some("work".into()),
                args: Arc::new(serde_json::json!({})),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ],
        vec![
            WalRecord::ToolResult {
                tags: None,
                hooks: None,
                batch_id: batch_id.clone(),
                result: Arc::new(ToolResult {
                    tool_call_id: id,
                    output: ToolOutput::Text {
                        text: "worked".into(),
                    },
                    is_error: false,
                }),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            WalRecord::ToolBatchFinished { batch_id },
            WalRecord::TurnFinished {
                step_id: StepId(step),
            },
        ],
    )
}

fn run_finished(reason: RunFinishReason) -> WalRecord {
    WalRecord::RunFinished {
        ts_ms: None,
        reason,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

/// The prompt as `(role, first text or "call"/"result")`, which is all these tests compare.
fn shape(prompt: &[Message]) -> Vec<(Role, String)> {
    prompt
        .iter()
        .map(|message| {
            let label = match message.content.first() {
                Some(ContentBlock::Text { text }) => text.clone(),
                Some(ContentBlock::ToolCall(_)) => "call".into(),
                Some(ContentBlock::ToolResult(_)) => "result".into(),
                _ => String::new(),
            };
            (message.role, label)
        })
        .collect()
}

fn records_with_steer_during_the_tool_call() -> Vec<WalRecord> {
    let (before, after) = tool_turn(1, "c1");
    let mut records = vec![run_started(vec![]), step_started(1), call_step(1, "c1")];
    records.extend(before);
    records.push(steered("hurry"));
    records.extend(after);
    records
}

/// A steer written while a tool call runs (after its dispatch intent, before its result) is
/// held until the next step boundary. Until then it is not in the prompt at all: it is handed
/// to a resume as pending, never placed between the call and its result.
#[test]
fn a_steer_during_a_tool_call_is_pending_until_the_next_step() {
    let state = replay(&records_with_steer_during_the_tool_call()).unwrap();
    assert_eq!(
        shape(&state.prompt),
        vec![
            (Role::User, "task".into()),
            (Role::Assistant, "call".into()),
            (Role::Tool, "result".into()),
        ]
    );
    assert_eq!(
        shape(&state.pending_steers),
        vec![(Role::User, "hurry".into())]
    );
    assert_eq!(state.resolution, ReplayResolution::Resume { pending: None });
}

/// At the next StepStarted the steer is placed after the tool result, so the step that follows
/// renders it as the last user message and nothing earlier in the prompt changed.
#[test]
fn a_steer_during_a_tool_call_lands_after_the_result_at_the_next_step() {
    let mut records = records_with_steer_during_the_tool_call();
    records.push(step_started(2));
    records.push(text_step(2, "written up"));
    records.push(run_finished(RunFinishReason::Completed));
    let state = replay(&records).unwrap();
    assert_eq!(
        shape(&state.prompt),
        vec![
            (Role::User, "task".into()),
            (Role::Assistant, "call".into()),
            (Role::Tool, "result".into()),
            (Role::User, "hurry".into()),
            (Role::Assistant, "written up".into()),
        ]
    );
    assert!(state.pending_steers.is_empty());
}

/// A steer written between two model steps (after the first step's turn barrier) is placed
/// right before the second step, after everything the first turn committed.
#[test]
fn a_steer_between_two_steps_is_placed_before_the_second_step() {
    let (before, after) = tool_turn(1, "c1");
    let mut records = vec![run_started(vec![]), step_started(1), call_step(1, "c1")];
    records.extend(before);
    records.extend(after);
    records.push(steered("new fact"));
    records.push(step_started(2));
    records.push(text_step(2, "answer"));
    records.push(run_finished(RunFinishReason::Completed));
    let state = replay(&records).unwrap();
    assert_eq!(
        shape(&state.prompt)[3..],
        [
            (Role::User, "new fact".into()),
            (Role::Assistant, "answer".into()),
        ]
    );
}

/// A steer written during a model step never lands ahead of that step's answer: the step did
/// not see it. With no further step before the terminal, it is placed once, at the close.
#[test]
fn a_steer_during_the_last_step_lands_after_it_at_the_close() {
    let records = vec![
        run_started(vec![]),
        step_started(1),
        steered("too late"),
        text_step(1, "answer"),
        run_finished(RunFinishReason::Cancelled),
    ];
    let state = replay(&records).unwrap();
    assert_eq!(
        shape(&state.prompt),
        vec![
            (Role::User, "task".into()),
            (Role::Assistant, "answer".into()),
            (Role::User, "too late".into()),
        ]
    );
    assert_eq!(
        state.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Cancelled
        }
    );
}

/// A final answer with a steer pending does not end the episode: the live loop runs one more
/// step to render the steer, so without a terminal the episode is still running (a resume
/// continues it, and a crash sweep seals it) rather than naturally finished.
#[test]
fn a_steer_pending_after_a_final_answer_keeps_the_episode_running() {
    let records = vec![
        run_started(vec![StopCondition::LoopFinished]),
        step_started(1),
        steered("one more thing"),
        text_step(1, "answer"),
    ];
    let state = replay(&records).unwrap();
    assert_eq!(state.resolution, ReplayResolution::Resume { pending: None });
    assert_eq!(
        shape(&state.pending_steers),
        vec![(Role::User, "one more thing".into())]
    );

    // Once the extra step has started, a crash there still resumes, not finishes.
    let mut continued = records.clone();
    continued.push(step_started(2));
    let state = replay(&continued).unwrap();
    assert_eq!(state.resolution, ReplayResolution::Resume { pending: None });
    assert!(state.pending_steers.is_empty());
    assert_eq!(shape(&state.prompt).last().unwrap().1, "one more thing");
}

/// A step a stop condition ended is not extended by a steer: the limit wins, the steer stays
/// undelivered and is placed at the close.
#[test]
fn a_steer_never_extends_a_step_a_limit_ended() {
    let records = vec![
        run_started(vec![
            StopCondition::StepCount { n: 1 },
            StopCondition::LoopFinished,
        ]),
        step_started(1),
        steered("more"),
        text_step(1, "answer"),
    ];
    let state = replay(&records).unwrap();
    assert_eq!(
        state.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::MaxSteps
        }
    );
}

#[test]
fn terminal_status_tracks_only_steers_not_rendered_in_the_episode() {
    for reason in [
        RunFinishReason::Completed,
        RunFinishReason::MaxSteps,
        RunFinishReason::Cancelled,
        RunFinishReason::Interrupted,
        RunFinishReason::Error,
        RunFinishReason::TransformUnavailable,
    ] {
        let mut records = vec![
            run_started(vec![]),
            step_started(1),
            steered("first"),
            steered("second"),
            text_step(1, "answer"),
            run_finished(reason),
        ];
        assert_eq!(
            super::reconcile_run_state(&records, "r1")
                .unwrap()
                .undelivered_steers,
            Some(vec!["steer-first".into(), "steer-second".into()])
        );
        records.insert(5, step_started(2));
        assert_eq!(
            super::reconcile_run_state(&records, "r1")
                .unwrap()
                .undelivered_steers,
            Some(vec![])
        );
        records.push(run_started(vec![]));
        records.push(run_finished(reason));
        assert_eq!(
            super::reconcile_run_state(&records, "r1")
                .unwrap()
                .undelivered_steers,
            Some(vec![])
        );
    }
}

/// No materialization boundary is offered while a steer is pending across it: the store would
/// fold the turn without the steer, and a tail replay after it would never see the record.
#[test]
fn no_projection_boundary_is_offered_with_a_steer_pending_across_it() {
    let framed = |records: Vec<WalRecord>| -> Vec<FramedRecord> {
        records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    };
    let records = records_with_steer_during_the_tool_call();
    let turn_finished_seq = records.len() as u64;
    let unsteered: Vec<WalRecord> = records
        .iter()
        .filter(|record| !matches!(record, WalRecord::Steered { .. }))
        .cloned()
        .collect();
    assert_eq!(
        latest_projection_boundary(&framed(unsteered.clone()), 0),
        Some(turn_finished_seq - 1),
        "without the steer the turn barrier is a boundary"
    );
    assert_eq!(
        latest_projection_boundary(&framed(records.clone()), 0),
        None
    );

    let mut placed = records;
    placed.push(step_started(2));
    placed.push(text_step(2, "answer"));
    placed.push(run_finished(RunFinishReason::Completed));
    let boundary = latest_projection_boundary(&framed(placed), 0);
    assert!(
        boundary.is_some_and(|seq| seq > turn_finished_seq),
        "once placed, a later boundary is offered: {boundary:?}"
    );
}

/// A resumed replay of a steered WAL reads the record as written: the input is rendered
/// verbatim, and the reserved `hooks` field is carried but never acted on.
#[test]
fn replay_reads_a_steer_as_written() {
    let mut records = records_with_steer_during_the_tool_call();
    let Some(WalRecord::Steered { hooks, .. }) = records
        .iter_mut()
        .find(|record| matches!(record, WalRecord::Steered { .. }))
    else {
        unreachable!("the fixture has a steer");
    };
    *hooks = None;
    records.push(step_started(2));
    let state = replay(&records).unwrap();
    assert_eq!(state.prompt[3].role, Role::User);
    assert_eq!(state.prompt[3].content, user("hurry").content);
    assert_eq!(state.prompt[3].cache_prefix_blocks, None);
}
