//! TURN-granular replay state machine tests.
//!
//! Two layers:
//!  1. Per-ROW classification tests (hand-built record vectors) — every row of the
//!     replay classification table is a test.
//!  2. The table-driven CRASH-CUT harness — drive a real run, truncate the on-disk
//!     WAL at EVERY record boundary, replay, assert the invariants (no duplicate
//!     side effect, provider-valid prompt, byte-identical rendered prefix, stop the
//!     run on an ambiguous cut). This is a durability exit gate.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    canonical_request_bytes, replay as replay_raw, run_import, run_loop, run_loop_append_episode,
    run_loop_on_writer_admitted, run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore,
    ImportOrigin, ImportRequest, ReplayResolution, ReplayState, RunControl, RunError, RunRequest,
    StartMode, StubProvider, StubReply, StubToolPlane, WalHandle, WalStore,
};

/// Replay-or-panic for the tests: replay() is fallible now (a corrupt lineage), but
/// every fixture here builds a well-formed WAL, so a replay error is a test bug —
/// unwrap it. (The dedicated corruption-guard test calls `replay_raw` directly to
/// assert the Err.)
fn replay(records: &[broca_wal::WalRecord]) -> ReplayState {
    replay_raw(records).expect("well-formed test WAL replays without a corruption error")
}
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, BudgetNudge, CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool,
        ToolChoice,
    },
    AttemptOutcome, BatchId, BindIdentity, FinishReason, GenerationConfig, MessageId, RunId,
    StepId, ToolCallId, Usage,
};
use broca_wal::{DispatchOutcome, FileWal, NoDispatchReason, RunConfig, Wal, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

// ───────────────────────── record builders ─────────────────────────

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
        frozen_render_config: serde_json::json!({"render_config": "stateless"}),
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

/// The ONE atomic run-start record (config + input).
fn start(run: &str, text: &str) -> WalRecord {
    WalRecord::RunStarted {
        written_at_ms: None,
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
            content: vec![ContentBlock::Text { text: text.into() }],
        }]
        .into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None,
    }
}

fn assistant_tool_call(id: &str, name: &str) -> AssistantMessage {
    AssistantMessage {
        message_id: MessageId("m".into()),
        content: vec![ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(id.into()),
            tool_name: name.into(),
            input: json!({}),
        })],
    }
}

fn step_with_call(step: u64, id: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: (assistant_tool_call(id, "edit")).into(),
        usage: Usage {
            input_tokens: Some(10),
            output_tokens: Some(5),
            ..Default::default()
        },
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn step_text(step: u64, text: &str) -> WalRecord {
    step_text_with_usage(step, text, usage_with_cache(7, 0, 3))
}

fn usage_with_cache(input_tokens: u64, cached_input_tokens: u64, output_tokens: u64) -> Usage {
    Usage {
        input_tokens: Some(input_tokens),
        cached_input_tokens: Some(cached_input_tokens),
        output_tokens: Some(output_tokens),
        ..Default::default()
    }
}

fn step_text_with_usage(step: u64, text: &str, usage: Usage) -> WalRecord {
    WalRecord::ModelStepFinished {
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: (AssistantMessage {
            message_id: MessageId("m".into()),
            content: vec![ContentBlock::Text { text: text.into() }],
        })
        .into(),
        usage,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn batch_started(b: &str, step: u64, ids: &[&str]) -> WalRecord {
    WalRecord::ToolBatchStarted {
        batch_id: BatchId(b.into()),
        step_id: StepId(step),
        planned_calls: ids.iter().map(|i| ToolCallId((*i).into())).collect(),
    }
}

fn intent(b: &str, id: &str) -> WalRecord {
    WalRecord::ToolDispatchIntent {
        dispatched_at_ms: None,
        hooks: None,
        batch_id: BatchId(b.into()),
        tool_call_id: ToolCallId(id.into()),
        tool_name: Some("edit".into()),
        args: json!({}).into(),
        module_id: None,
        schema_pin: None,
        approval_digest: None,
        call_key: None,
    }
}

fn result(b: &str, id: &str, outcome: DispatchOutcome) -> WalRecord {
    WalRecord::ToolResult {
        finished_at_ms: None,
        tags: None,
        hooks: None,
        batch_id: BatchId(b.into()),
        result: broca_protocol::content::ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: "ok".into() },
            is_error: false,
        }
        .into(),
        outcome,
        reason: None,
        withdraw_elicitation: None,
        provider_held: None,
    }
}

fn no_dispatch(b: &str, id: &str) -> WalRecord {
    WalRecord::ToolNoDispatch {
        dispatched_at_ms: None,
        tags: None,
        hooks: None,
        batch_id: BatchId(b.into()),
        tool_call_id: ToolCallId(id.into()),
        reason: NoDispatchReason::Denied {
            reason: "nope".into(),
        },
    }
}

fn batch_finished(b: &str) -> WalRecord {
    WalRecord::ToolBatchFinished {
        batch_id: BatchId(b.into()),
    }
}

fn count_tool_results(prompt: &[Message]) -> usize {
    prompt
        .iter()
        .filter(|m| m.role == Role::Tool)
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolResult(_)))
        .count()
}

/// Replay stamps each assistant turn with the episode's producing model (from the
/// frozen `RunConfig.model`), NOT `None` — so a later cross-model continuation can detect
/// the boundary. This pins the stamping is non-vacuous (the merge-invariant test proves
/// full==merged, but both could be None-but-equal without this).
#[test]
fn replay_stamps_assistant_origin_with_episode_model() {
    // A one-episode lineage: user → text terminal turn (a no-tools terminal commits the
    // assistant immediately). run_config().model is stub/m.
    let wal = vec![start("r", "hi"), step_text(1, "done")];
    let prompt = replay(&wal).prompt;
    let assistant = prompt
        .iter()
        .find(|m| m.role == Role::Assistant)
        .expect("an assistant turn");
    assert_eq!(
        assistant.origin,
        Some(ModelHandle::new("stub", "m")),
        "replay must stamp the assistant turn with the episode's frozen model, not None"
    );
    // User input carries no origin.
    let user = prompt.iter().find(|m| m.role == Role::User).unwrap();
    assert_eq!(
        user.origin, None,
        "user input has no producing-model origin"
    );
}

#[test]
fn replay_resets_last_step_usage_when_new_episode_starts() {
    let prior_last_step = usage_with_cache(10, 7000, 3);
    let recs = vec![
        start("r1", "hi"),
        step_text_with_usage(1, "done", prior_last_step),
        WalRecord::TurnFinished { step_id: StepId(1) },
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: prior_last_step,
            error: None,
            retries_used: 0,
        },
        start("r2", "follow-up"),
    ];
    let st = replay(&recs);
    assert_eq!(
        st.last_step_usage,
        None,
        "a new RunStarted opens a fresh episode, so replay clears the prior episode's last-step usage until the new episode completes a step"
    );
}

/// Assert the reconstructed prompt is PROVIDER-VALID: every assistant tool_use has
/// a matching tool_result in the immediately-following tool message.
fn assert_provider_valid(prompt: &[Message]) {
    for (i, m) in prompt.iter().enumerate() {
        if m.role != Role::Assistant {
            continue;
        }
        let tool_use_ids: HashSet<String> = m
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolCall(c) => Some(c.tool_call_id.0.clone()),
                _ => None,
            })
            .collect();
        if tool_use_ids.is_empty() {
            continue;
        }
        let next = prompt
            .get(i + 1)
            .expect("a tool_use turn must be followed by results");
        assert_eq!(
            next.role,
            Role::Tool,
            "tool_use must be followed by a tool message"
        );
        let result_ids: HashSet<String> = next
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolResult(r) => Some(r.tool_call_id.0.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            tool_use_ids, result_ids,
            "every tool_use has a matching tool_result"
        );
    }
}

// ───────────────────────── per-row classification ─────────────────────────

#[test]
fn row_completed_turn_commits_to_prompt() {
    // intent ∧ result(Completed) → include; turn committed; resume re-runs model.
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        result("b1", "t1", DispatchOutcome::Completed),
        batch_finished("b1"),
        WalRecord::TurnFinished { step_id: StepId(1) },
    ];
    let st = replay(&recs);
    assert_eq!(st.completed_steps, 1);
    assert_provider_valid(&st.prompt);
    assert_eq!(
        count_tool_results(&st.prompt),
        1,
        "the completed turn is committed"
    );
    // Not finished, batch complete → clean resume (re-run the model).
    assert!(matches!(st.resolution, ReplayResolution::Resume { .. }));
}

#[test]
fn row_intent_without_result_is_indeterminate_interrupt() {
    // intent ∧ ¬terminal → INDETERMINATE → interrupt (C4). The turn is NOT
    // committed (incomplete batch).
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        // crash before result
    ];
    let st = replay(&recs);
    match st.resolution {
        ReplayResolution::Interrupted { indeterminate } => {
            assert_eq!(indeterminate, vec![ToolCallId("t1".into())]);
        }
        other => panic!("expected Interrupted, got {other:?}"),
    }
    assert_eq!(
        count_tool_results(&st.prompt),
        0,
        "incomplete turn NOT committed"
    );
}

#[test]
fn paused_episode_with_open_intent_fails_closed_to_interrupted() {
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        WalRecord::RunPaused {
            ts_ms: None,
            run_id: RunId("r".into()),
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        },
    ];

    // This assertion is non-vacuous: without classify's guarded pause arm, the synthetic history
    // resolves to AuthRequired and could re-run a tool whose durable intent has unknown effects.
    match replay(&recs).resolution {
        ReplayResolution::Interrupted { indeterminate } => {
            assert_eq!(indeterminate, vec![ToolCallId("t1".into())]);
        }
        other => panic!("expected fail-closed Interrupted, got {other:?}"),
    }
}

#[test]
fn row_unknown_outcome_is_indeterminate_interrupt() {
    // result tagged Unknown → INDETERMINATE → interrupt (the ambiguous side effect).
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        result("b1", "t1", DispatchOutcome::Unknown),
        batch_finished("b1"),
    ];
    let st = replay(&recs);
    assert!(
        matches!(st.resolution, ReplayResolution::Interrupted { .. }),
        "Unknown is the ambiguous bucket → interrupt"
    );
}

#[test]
fn run_finished_after_indeterminate_intent_seals_without_committing_partial_turn() {
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ];

    let st = replay(&recs);
    assert!(matches!(
        st.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));
    assert_eq!(
        st.prompt.len(),
        1,
        "the assistant tool call remains outside the committed prompt"
    );
    assert_eq!(
        count_tool_results(&st.prompt),
        0,
        "sealing an ambiguous batch must not fabricate a tool result"
    );
}

#[test]
fn row_no_dispatch_terminal_includes_result_never_redispatch() {
    // no_dispatch terminal → include the synth result, never re-dispatch (C2).
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        no_dispatch("b1", "t1"),
        batch_finished("b1"),
        WalRecord::TurnFinished { step_id: StepId(1) },
    ];
    let st = replay(&recs);
    assert_provider_valid(&st.prompt);
    assert_eq!(
        count_tool_results(&st.prompt),
        1,
        "no_dispatch result is included"
    );
    // The batch COMPLETED (no_dispatch is a terminal) → the turn commits → no
    // pending turn, nothing re-dispatched.
    assert_eq!(
        st.resolution,
        ReplayResolution::Resume { pending: None },
        "a completed no_dispatch batch commits; nothing pending/re-dispatched"
    );
}

#[test]
fn row_hard_stop_terminal_includes_result_never_redispatch() {
    // A hard-stop MaxSteps/Interrupted terminal → include the result, never re-dispatch.
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        result("b1", "t1", DispatchOutcome::MaxSteps),
        batch_finished("b1"),
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::MaxSteps,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ];
    let st = replay(&recs);
    assert_provider_valid(&st.prompt);
    assert_eq!(count_tool_results(&st.prompt), 1);
    assert!(matches!(
        st.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::MaxSteps
        }
    ));
}

#[test]
fn hard_stop_batch_without_run_finished_finishes_not_resumes() {
    // CRASH-CUT between the hard-stop repair batch and the RunFinished record: the tool
    // turn is durably committed (all MaxSteps terminals) but the trailing RunFinished was
    // lost. Replay MUST finish the lineage (it hit the step limit), NOT re-run the model
    // past the limit. Before the fix this resolved Resume{None} → a divergent extra turn.
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        result("b1", "t1", DispatchOutcome::MaxSteps),
        batch_finished("b1"),
        // NO RunFinished — the crash cut here.
    ];
    let st = replay(&recs);
    assert_provider_valid(&st.prompt);
    assert_eq!(
        count_tool_results(&st.prompt),
        1,
        "the repair turn is committed"
    );
    assert!(
        matches!(
            st.resolution,
            ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::MaxSteps
            }
        ),
        "a committed hard-stop batch finishes the lineage even without RunFinished, got {:?}",
        st.resolution
    );
}

#[test]
fn row_intent_missing_call_is_redispatchable() {
    // ¬intent ∧ ¬terminal (a planned call that never left) → re-dispatchable.
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1", "t2"]),
        intent("b1", "t1"),
        result("b1", "t1", DispatchOutcome::Completed),
        // t2 was planned but has NO intent + NO terminal (crash before its intent)
    ];
    let st = replay(&recs);
    match st.resolution {
        ReplayResolution::Resume { pending: Some(p) } => {
            assert_eq!(
                p.redispatch,
                vec![ToolCallId("t2".into())],
                "the never-sent call is re-dispatchable"
            );
            assert_eq!(
                p.existing.len(),
                1,
                "t1's result is reconstructed as existing"
            );
            assert!(p.batch_id.is_some(), "the batch was durably started");
        }
        other => panic!("expected Resume with pending, got {other:?}"),
    }
}

#[test]
fn row_model_step_no_batch_redispatches_assistant_calls() {
    // ModelStepFinished (tool turn) with NO following batch + not finished → that
    // model step's calls were never dispatched → re-dispatch them.
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        // crash between model_step_finished and batch_started
    ];
    let st = replay(&recs);
    match st.resolution {
        ReplayResolution::Resume { pending: Some(p) } => {
            assert_eq!(p.redispatch, vec![ToolCallId("t1".into())]);
            assert!(p.batch_id.is_none(), "no batch was started");
            assert!(p.existing.is_empty(), "nothing terminated yet");
        }
        other => panic!("expected Resume with pending, got {other:?}"),
    }
    assert_eq!(
        count_tool_results(&st.prompt),
        0,
        "uncommitted tool turn not in prompt"
    );
}

#[test]
fn row_run_finished_is_terminal() {
    let recs = vec![
        start("r", "hi"),
        step_text(1, "done"),
        WalRecord::TurnFinished { step_id: StepId(1) },
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ];
    let st = replay(&recs);
    assert_eq!(
        st.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Completed
        }
    );
    // The text turn is committed.
    assert!(st.prompt.iter().any(|m| m.role == Role::Assistant));
}

#[test]
fn dangling_tool_use_turn_is_never_committed() {
    // The C1 invariant: an assistant tool_use whose batch is incomplete must NOT
    // appear in the reconstructed prompt (it'd be provider-invalid).
    let recs = vec![
        start("r", "hi"),
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
        // no result, no batch_finished
    ];
    let st = replay(&recs);
    assert_provider_valid(&st.prompt); // vacuously: the tool turn isn't in prompt
    assert!(
        !st.prompt.iter().any(|m| m.role == Role::Assistant),
        "the incomplete tool turn is held out of the prompt"
    );
}

// ───────────────────────── crash-cut harness (exit gate) ─────────────────────────

/// Stub provider script for a 2-step run: a tool call, then a text reply.
fn tool_then_text() -> Vec<Vec<NormalizedEvent>> {
    vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: assistant_tool_call("toolu_1", "edit"),
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(100),
                    output_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m2".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(50),
                    output_tokens: Some(4),
                    ..Default::default()
                },
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ],
    ]
}

fn demo_request(run: &str, dir: &std::path::Path) -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: BindIdentity::new(dir, "crashcut", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "edit the file".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

/// Render a prompt prefix the way the loop would, for byte-identical comparison.
fn render_prompt(prompt: &[Message]) -> Vec<u8> {
    canonical_request_bytes(&CallOptions {
        declared_markers: Vec::new(),
        ephemeral_session: false,
        prompt: prompt.to_vec(),
        tools: vec![].into(),
        tool_choice: ToolChoice::Auto.into(),
        response_format: Default::default(),
        generation: GenerationConfig::default().into(),
        provider_options: serde_json::Value::Null.into(),
        step_context: None,
        target_model: None,
        marker_order_version: None,
        render_episodes: Default::default(),
    })
}

/// End-to-end RESUME EXECUTION: seed a WAL with a partial batch (t1 done, t2 never
/// sent), then run run_loop and assert it COMPLETES the turn by dispatching ONLY t2
/// (t1 never re-runs), reuses the durable model step (no regenerate), and finishes
/// cleanly.
#[tokio::test]
async fn resume_completes_partial_batch_redispatching_only_missing_call() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let session = BindIdentity::new(&dir, "resume", "s1");
    let wal_store = FileWalStore::new(dir.join("wal"));
    let wal_path = wal_store.wal_path(&session);

    // Assistant message with TWO tool calls.
    let assistant = AssistantMessage {
        message_id: MessageId("m1".into()),
        content: vec![
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("t1".into()),
                tool_name: "edit".into(),
                input: json!({"f": "a"}),
            }),
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("t2".into()),
                tool_name: "edit".into(),
                input: json!({"f": "b"}),
            }),
        ],
    };

    // Seed the "crashed" WAL: t1 dispatched+done, t2 planned but never sent.
    {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let recs = vec![
            WalRecord::RunStarted {
                written_at_ms: None,
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
                run_id: RunId("r".into()),
                session: session.clone(),
                ts_ms: None,
                // The frozen run_config is now AUTHORITATIVE on resume — it must
                // carry the `edit` tool (a real run would have frozen it), since the
                // resumed manifest comes from here, not `req`.
                config: Box::new(RunConfig {
                    tools: vec![Tool {
                        name: "edit".into(),
                        description: "edit".into(),
                        input_schema: json!({"type": "object"}),
                        module: None,
                        execution_mode: ExecutionMode::Parallel,
                        reply: None,
                    }]
                    .into(),
                    ..run_config()
                }),
                input: vec![Message {
                    cache_prefix_blocks: None,
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                    role: Role::User,
                    content: vec![ContentBlock::Text { text: "go".into() }],
                }]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: assistant.clone().into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("r-b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("t1".into()), ToolCallId("t2".into())],
            },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: BatchId("r-b1".into()),
                tool_call_id: ToolCallId("t1".into()),
                tool_name: Some("edit".into()),
                args: json!({"f": "a"}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: BatchId("r-b1".into()),
                result: ToolResultProto("t1", "did-a").into(),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            // crash: t2 has NO intent, NO result.
        ];
        for r in &recs {
            wal.append(1, r, true).unwrap();
        }
    }

    // Resume: the provider only supplies STEP 2 (the text reply) — step 1 must NOT
    // be regenerated. The plane tracks executions.
    let provider = StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m2".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text { text: "ran".into() })
    });
    let lease = FileLeaseStore::new(dir.join("lease"));
    let req = RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("r".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "go".into() }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(req, &provider, &plane, &lease, &AllowAll, &wal_store, &tx)
        .await
        .unwrap();
    drop(tx);
    collector.await.unwrap();

    // t2 ran exactly once; t1 NEVER re-ran (its result was reconstructed).
    assert_eq!(
        plane.exec_count(&ToolCallId("t2".into())),
        1,
        "the missing call t2 ran once"
    );
    assert_eq!(
        plane.exec_count(&ToolCallId("t1".into())),
        0,
        "the durable call t1 NEVER re-ran"
    );
    assert_eq!(
        summary.reason,
        RunFinishReason::Completed,
        "the resumed run finished cleanly"
    );

    // The final WAL replays to a provider-valid, complete conversation.
    let final_recs: Vec<WalRecord> = FileWal::open(&wal_path)
        .unwrap()
        .replay()
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let st = replay(&final_recs);
    assert_provider_valid(&st.prompt);
    assert!(
        matches!(st.resolution, ReplayResolution::LineageIdle { .. }),
        "WAL shows a finished episode (lineage idle)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Build a ToolResult for seeding (helper to keep the WAL seed terse).
#[allow(non_snake_case)]
fn ToolResultProto(id: &str, text: &str) -> broca_protocol::content::ToolResult {
    broca_protocol::content::ToolResult {
        tool_call_id: ToolCallId(id.into()),
        output: ToolOutput::Text { text: text.into() },
        is_error: false,
    }
}

#[tokio::test]
async fn crash_cut_at_every_record_boundary() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-crashcut-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 1. Drive a full clean run → the complete WAL + the canonical full record list.
    let full_records = {
        let lease = FileLeaseStore::new(dir.join("lease-full"));
        let wal = FileWalStore::new(dir.join("wal-full"));
        let req = demo_request("full", &dir);
        let wal_path = wal.wal_path(&req.session);
        let plane = StubToolPlane::new().with_tool("edit", |_| {
            StubReply::Output(ToolOutput::Text {
                text: "edited".into(),
            })
        });
        let provider = StubProvider::new(tool_then_text());
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(req, &provider, &plane, &lease, &AllowAll, &wal, &tx)
            .await
            .unwrap();
        drop(tx);
        collector.await.unwrap();
        FileWal::open(&wal_path).unwrap().replay().unwrap()
    };
    assert!(full_records.len() >= 8, "a non-trivial WAL to cut");

    // The reference prompt: replay of the COMPLETE WAL.
    let full_state = replay(
        &full_records
            .iter()
            .map(|f| f.record.clone())
            .collect::<Vec<_>>(),
    );
    let reference_prefix = render_prompt(&full_state.prompt);

    // 2. CUT at every record boundary: replay the first N records, assert invariants.
    for n in 1..=full_records.len() {
        let prefix: Vec<WalRecord> = full_records
            .iter()
            .take(n)
            .map(|f| f.record.clone())
            .collect();
        let st = replay(&prefix);
        let cut_label = format!("cut after {n}/{} records", full_records.len());

        // (b) provider-valid prompt at EVERY cut (never a dangling tool_use).
        assert_provider_valid(&st.prompt);

        // (c) byte-identical rendered prefix: any COMMITTED turn must match the
        // reference's same-length prefix exactly. The full cut equals the reference.
        if n == full_records.len() {
            assert_eq!(
                render_prompt(&st.prompt),
                reference_prefix,
                "{cut_label}: full replay == reference"
            );
        }

        // (d) interrupt-to-doctor whenever the cut leaves an intent without a
        // terminal (the INDETERMINATE window between barrier 1 and barrier 2).
        let cut_in_indeterminate = matches!(
            full_records.get(n - 1).map(|f| &f.record),
            Some(WalRecord::ToolDispatchIntent { .. })
        );
        if cut_in_indeterminate {
            assert!(
                matches!(st.resolution, ReplayResolution::Interrupted { .. }),
                "{cut_label}: a cut on an intent (no result yet) MUST interrupt"
            );
        }

        // (a) NO DUPLICATE SIDE EFFECT (replay-level guarantee): replay must NEVER
        // ask to re-dispatch a call that already has a durable terminal — doing so
        // would re-run a side effect that may have happened. Collect every
        // terminated call id in the cut, assert redispatch is disjoint from it.
        if let ReplayResolution::Resume { pending: Some(p) } = &st.resolution {
            let terminated: HashSet<String> = prefix
                .iter()
                .filter_map(|r| match r {
                    WalRecord::ToolResult { result, .. } => Some(result.tool_call_id.0.clone()),
                    WalRecord::ToolNoDispatch { tool_call_id, .. } => Some(tool_call_id.0.clone()),
                    _ => None,
                })
                .collect();
            for id in &p.redispatch {
                assert!(
                    !terminated.contains(&id.0),
                    "{cut_label}: re-dispatch of already-terminated {} = duplicate side effect",
                    id.0
                );
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────── record types from a newer build ─────────────────────────

/// A record type this build has no variant for, as a newer build would write it. The field
/// names deliberately overlap known records (`step_id`, `usage`, `reason`, `tool_call_id`),
/// so a reader that interpreted it as anything would change the session.
const NEWER_BUILD_RECORD: &[u8] = br#"{"type":"attempt_digest_from_a_newer_build","step_id":1,"attempt":1,"status":200,"reason":"completed","tool_call_id":"toolu_1","usage":{"input_tokens":999,"output_tokens":999}}"#;

/// Frame `records` into a WAL file through the real framing, optionally with the newer-build
/// record inserted before index `insert_at`, and read it back through the real reader.
fn write_and_read_wal(
    path: &std::path::Path,
    records: &[WalRecord],
    insert_at: Option<usize>,
) -> Vec<broca_wal::FramedRecord> {
    let mut bytes = Vec::new();
    let mut seq = 1;
    for (index, record) in records.iter().enumerate() {
        if insert_at == Some(index) {
            bytes.extend(broca_wal::framing::encode_payload(seq, 1, NEWER_BUILD_RECORD).unwrap());
            seq += 1;
        }
        bytes.extend(broca_wal::framing::encode(seq, 1, record).unwrap());
        seq += 1;
    }
    std::fs::write(path, bytes).unwrap();
    broca_wal::replay_file(path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap()
}

/// A WAL carrying a record type from a newer build replays to EXACTLY the session the same
/// WAL has without it: the same replay state, the same admission checkpoint, the same
/// control units, and a byte-identical rendered prompt. Checked with the record inserted at
/// every interior position of a real run's WAL (tool call, dispatch, result, text reply).
#[tokio::test]
async fn a_record_type_from_a_newer_build_changes_nothing_replay_reads() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-unknown-record-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // A real run's WAL, written by the run loop.
    let records: Vec<WalRecord> = {
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let req = demo_request("real", &dir);
        let wal_path = wal.wal_path(&req.session);
        let plane = StubToolPlane::new().with_tool("edit", |_| {
            StubReply::Output(ToolOutput::Text {
                text: "edited".into(),
            })
        });
        let provider = StubProvider::new(tool_then_text());
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(req, &provider, &plane, &lease, &AllowAll, &wal, &tx)
            .await
            .unwrap();
        drop(tx);
        collector.await.unwrap();
        FileWal::open(&wal_path)
            .unwrap()
            .replay()
            .unwrap()
            .into_iter()
            .map(|framed| framed.record)
            .collect()
    };
    assert!(records.len() >= 8, "a non-trivial WAL to insert into");

    let plain = write_and_read_wal(&dir.join("plain.wal"), &records, None);
    let plain_records: Vec<WalRecord> = plain.iter().map(|f| f.record.clone()).collect();
    assert_eq!(
        plain_records, records,
        "re-framing the run's records is lossless"
    );
    let plain_state = replay(&plain_records);
    let plain_prompt = render_prompt(&plain_state.prompt);
    let plain_checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&plain)
        .expect("admission fold of the plain WAL");
    let plain_units: Vec<_> = broca_core::project_control(&plain)
        .into_iter()
        .map(|(_, unit)| unit)
        .collect();
    assert!(!plain_units.is_empty());

    for insert_at in 1..records.len() {
        let label = format!(
            "newer-build record before record {insert_at}/{}",
            records.len()
        );
        let with = write_and_read_wal(&dir.join("with.wal"), &records, Some(insert_at));
        assert_eq!(with.len(), records.len() + 1, "{label}: the frame decodes");
        assert_eq!(with[insert_at].record, WalRecord::Unknown, "{label}");
        let with_records: Vec<WalRecord> = with.iter().map(|f| f.record.clone()).collect();

        let mut state = replay(&with_records);
        assert_eq!(
            render_prompt(&state.prompt),
            plain_prompt,
            "{label}: rendered prompt bytes"
        );
        // Each committed tool turn names its step record by index among the records replayed,
        // which the extra record moves by one when it comes before the step; the turn itself is
        // the same, and names the same step.
        for (turn, plain_turn) in state
            .committed_tool_turns
            .iter_mut()
            .zip(&plain_state.committed_tool_turns)
        {
            assert_eq!(turn.0, plain_turn.0, "{label}: the turn's prompt position");
            assert_eq!(
                with_records[turn.1], records[plain_turn.1],
                "{label}: the turn names the same step record"
            );
            turn.1 = plain_turn.1;
        }
        assert_eq!(
            format!("{state:?}"),
            format!("{plain_state:?}"),
            "{label}: replay state"
        );

        // The checkpoint's `at_seq` follows the WAL head, which the extra frame moves by one.
        let mut checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&with)
            .expect("admission fold of the WAL with the newer-build record");
        assert_eq!(checkpoint.at_seq, plain_checkpoint.at_seq + 1, "{label}");
        checkpoint.at_seq = plain_checkpoint.at_seq;
        assert_eq!(
            format!("{checkpoint:?}"),
            format!("{plain_checkpoint:?}"),
            "{label}: admission checkpoint"
        );

        let projected = broca_core::project_control(&with);
        let unknown_seq = with[insert_at].seq;
        assert!(
            projected
                .iter()
                .all(|(cursor, _)| cursor.wal_seq != unknown_seq),
            "{label}: the newer-build record emits no control unit"
        );
        let units: Vec<_> = projected.into_iter().map(|(_, unit)| unit).collect();
        assert_eq!(units, plain_units, "{label}: control units");

        // A crash right after the newer-build record: the WAL ends on it. The full WAL above
        // ends on a terminal that would mask a record read as one, so the cut is what shows
        // whether resume sees the same session as a crash one record earlier.
        let cut = replay(&with_records[..=insert_at]);
        let cut_plain = replay(&plain_records[..insert_at]);
        assert_eq!(
            render_prompt(&cut.prompt),
            render_prompt(&cut_plain.prompt),
            "{label}: rendered prompt of the cut WAL"
        );
        assert_eq!(
            format!("{cut:?}"),
            format!("{cut_plain:?}"),
            "{label}: replay state of the cut WAL"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// A provider attempt record as the live loop writes it.
fn attempt_record(step_id: StepId, attempt: u32, outcome: AttemptOutcome) -> WalRecord {
    WalRecord::ModelAttemptFinished {
        content_progress: Default::default(),
        step_id,
        attempt,
        request_body_len: 999,
        request_body_sha256: "ab".repeat(32),
        http_status: Some(503),
        content_type: Some("text/event-stream".into()),
        response_body_len: 99,
        response_body_sha256: "cd".repeat(32),
        response_body_cap: None,
        outcome,
        ts_ms: 1,
        sent_at_ms: None,
        transport: None,
        websocket_request: None,
        websocket_close_code: None,
        input_transformations: None,
        input_transformations_unmeasured: false,
        credential_refresh: None,
        served_account_ref: None,
    }
}

/// Frame `records` through the real framing into `path` and read them back.
fn frame_and_read(path: &std::path::Path, records: &[WalRecord]) -> Vec<broca_wal::FramedRecord> {
    let mut bytes = Vec::new();
    for (index, record) in records.iter().enumerate() {
        bytes.extend(broca_wal::framing::encode(index as u64 + 1, 1, record).unwrap());
    }
    std::fs::write(path, bytes).unwrap();
    broca_wal::replay_file(path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap()
}

/// Provider attempt records are known to this build but carry no session semantics: a real
/// run's WAL with them replays to EXACTLY the session the same WAL has without them -- the
/// same replay state, admission checkpoint and control units, and a byte-identical rendered
/// prompt. Checked where the live loop writes them (before each `ModelStepFinished`, failed
/// attempts included) and with one inserted at every interior position, including as the last
/// record of a crash-cut WAL.
#[tokio::test]
async fn provider_attempt_records_change_nothing_replay_reads() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-attempt-record-replay-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let records: Vec<WalRecord> = {
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let req = demo_request("real", &dir);
        let wal_path = wal.wal_path(&req.session);
        let plane = StubToolPlane::new().with_tool("edit", |_| {
            StubReply::Output(ToolOutput::Text {
                text: "edited".into(),
            })
        });
        let provider = StubProvider::new(tool_then_text());
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(req, &provider, &plane, &lease, &AllowAll, &wal, &tx)
            .await
            .unwrap();
        drop(tx);
        collector.await.unwrap();
        FileWal::open(&wal_path)
            .unwrap()
            .replay()
            .unwrap()
            .into_iter()
            .map(|framed| framed.record)
            .collect()
    };
    assert!(records.len() >= 8, "a non-trivial WAL to insert into");

    let plain = frame_and_read(&dir.join("plain.wal"), &records);
    let plain_state = replay(&records);
    let plain_prompt = render_prompt(&plain_state.prompt);
    let plain_checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&plain).unwrap();
    let plain_units: Vec<_> = broca_core::project_control(&plain)
        .into_iter()
        .map(|(_, unit)| unit)
        .collect();

    let compare = |label: &str, with_records: &[WalRecord], extra: u64| {
        let with = frame_and_read(&dir.join("with.wal"), with_records);
        let mut state = replay(with_records);
        assert_eq!(
            render_prompt(&state.prompt),
            plain_prompt,
            "{label}: rendered prompt bytes"
        );
        // Each committed tool turn names its step record by index among the records replayed,
        // which inserted records move; the turn itself is the same, and names the same step.
        for (turn, plain_turn) in state
            .committed_tool_turns
            .iter_mut()
            .zip(&plain_state.committed_tool_turns)
        {
            assert_eq!(turn.0, plain_turn.0, "{label}: the turn's prompt position");
            assert_eq!(
                with_records[turn.1], records[plain_turn.1],
                "{label}: the turn names the same step record"
            );
            turn.1 = plain_turn.1;
        }
        // The output reservation is the one reader of attempt records: it takes an attempt's
        // rendered body length as the measurement its next estimate scales from. It is asserted
        // on its own below; everything else replay reads must not change.
        let reservation = state.output_reservation.clone();
        state.output_reservation = plain_state.output_reservation.clone();
        assert_eq!(
            format!("{state:?}"),
            format!("{plain_state:?}"),
            "{label}: replay state"
        );
        let mut checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&with).unwrap();
        assert_eq!(
            checkpoint.at_seq,
            plain_checkpoint.at_seq + extra,
            "{label}"
        );
        checkpoint.at_seq = plain_checkpoint.at_seq;
        // The checkpoint carries the reservation fold replay builds from the same log, so a
        // resume from it clamps as a full replay would. That fold is the one part allowed to
        // differ from the plain log's checkpoint.
        assert_eq!(
            format!("{:?}", checkpoint.output_reservation),
            format!("{:?}", Some(&reservation)),
            "{label}: the checkpoint's reservation fold is replay's"
        );
        checkpoint.output_reservation = plain_checkpoint.output_reservation.clone();
        assert_eq!(
            format!("{checkpoint:?}"),
            format!("{plain_checkpoint:?}"),
            "{label}: admission checkpoint"
        );
        let units: Vec<_> = broca_core::project_control(&with)
            .into_iter()
            .map(|(_, unit)| unit)
            .collect();
        assert_eq!(units, plain_units, "{label}: control units");
    };

    // Where the live loop writes them: two failed attempts and the successful one before every
    // ModelStepFinished, carrying that step's id.
    let mut live_shaped = Vec::new();
    let mut inserted = 0;
    for record in &records {
        if let WalRecord::ModelStepFinished { step_id, .. } = record {
            live_shaped.push(attempt_record(*step_id, 0, AttemptOutcome::HttpError));
            live_shaped.push(attempt_record(*step_id, 1, AttemptOutcome::ChunkError));
            live_shaped.push(attempt_record(*step_id, 2, AttemptOutcome::Complete));
            inserted += 3;
        }
        live_shaped.push(record.clone());
    }
    assert!(inserted >= 6, "both model steps carry attempt records");
    compare("live-shaped attempt records", &live_shaped, inserted);
    // With an attempt record before each finished step, as the live loop writes them, the
    // output reservation has a measurement for the run's model; without them it has none.
    let model = records
        .iter()
        .find_map(|r| match r {
            WalRecord::RunStarted { config, .. } => Some(config.model.clone()),
            _ => None,
        })
        .expect("the run's RunStarted");
    assert!(replay(&live_shaped)
        .output_reservation
        .has_measurement(&model));
    assert!(!plain_state.output_reservation.has_measurement(&model));

    for insert_at in 1..records.len() {
        let label = format!("attempt record before record {insert_at}/{}", records.len());
        let mut with_records = records.clone();
        with_records.insert(
            insert_at,
            attempt_record(StepId(1), 0, AttemptOutcome::Cancelled),
        );
        compare(&label, &with_records, 1);

        // A crash right after the attempt record: the WAL ends on it. The output reservation
        // holds that attempt's body length for the step it belongs to, which is the one
        // difference allowed.
        let mut cut = replay(&with_records[..=insert_at]);
        let cut_plain = replay(&records[..insert_at]);
        assert_eq!(
            render_prompt(&cut.prompt),
            render_prompt(&cut_plain.prompt),
            "{label}: rendered prompt of the cut WAL"
        );
        cut.output_reservation = cut_plain.output_reservation.clone();
        assert_eq!(
            format!("{cut:?}"),
            format!("{cut_plain:?}"),
            "{label}: replay state of the cut WAL"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────── append-new-episode ─────────────────────────

/// A text-only request that runs one model step and ends `Completed`.
fn text_request(run: &str, session: &broca_protocol::BindIdentity, prompt: &str) -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: prompt.into(),
            }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn text_request_with_system(
    run: &str,
    session: &broca_protocol::BindIdentity,
    system: &str,
    prompt: &str,
) -> RunRequest {
    let mut req = text_request(run, session, prompt);
    req.input.insert(
        0,
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: system.into(),
            }],
        },
    );
    req
}

fn count_system_messages(prompt: &[Message]) -> usize {
    prompt
        .iter()
        .filter(|message| message.role == Role::System)
        .count()
}

/// One scripted text-reply step.
fn text_reply(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

#[tokio::test]
async fn later_episode_copies_first_marker_order_including_absence() {
    for version in [None, Some(1)] {
        let dir = broca_test_support::scratch_root()
            .join(format!("marker-copy-{version:?}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let session = BindIdentity::new(dir.join("proj"), "h", "s");
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let mut seed = start("r1", "first question");
        if let WalRecord::RunStarted {
            session: bind,
            marker_order_version,
            ..
        } = &mut seed
        {
            *bind = session.clone();
            *marker_order_version = version;
        }
        let seed_bytes = serde_json::to_vec(&seed).unwrap();
        let mut writer = wal.open(&session).unwrap();
        writer.append(1, &seed, true).unwrap();
        writer
            .append(
                1,
                &WalRecord::RunFinished {
                    interrupt_reason: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    retries_used: 0,
                    error: None,
                    ts_ms: None,
                },
                true,
            )
            .unwrap();
        writer.sync().unwrap();
        drop(writer);
        let provider = StubProvider::new(vec![text_reply("m2", "answer")]);
        drive(
            text_request("r2", &session, "second question"),
            &provider,
            &StubToolPlane::new(),
            &lease,
            &wal,
            true,
        )
        .await;
        let records = WalStore::read_records(&wal, &session).unwrap();
        assert_eq!(serde_json::to_vec(&records[0].record).unwrap(), seed_bytes);
        let versions: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted {
                    marker_order_version,
                    ..
                } => Some(*marker_order_version),
                _ => None,
            })
            .collect();
        assert_eq!(versions, vec![version, version]);
        assert_eq!(provider.calls()[0].marker_order_version, version);
        assert_eq!(
            replay(&records.into_iter().map(|f| f.record).collect::<Vec<_>>()).marker_order_version,
            version
        );
    }
}

#[tokio::test]
async fn resumed_episode_keeps_frozen_marker_order_and_request() {
    for version in [None, Some(1)] {
        let dir = broca_test_support::scratch_root()
            .join(format!("marker-resume-{version:?}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let session = BindIdentity::new(dir.join("proj"), "h", "s");
        let lease = FileLeaseStore::new(dir.join("lease"));
        let wal = FileWalStore::new(dir.join("wal"));
        let mut seed = start("r1", "question");
        if let WalRecord::RunStarted {
            session: bind,
            marker_order_version,
            ..
        } = &mut seed
        {
            *bind = session.clone();
            *marker_order_version = version;
        }
        let mut writer = wal.open(&session).unwrap();
        writer.append(1, &seed, true).unwrap();
        writer.sync().unwrap();
        drop(writer);
        let provider = StubProvider::new(vec![text_reply("m1", "answer")]);
        drive(
            text_request("ignored", &session, "ignored"),
            &provider,
            &StubToolPlane::new(),
            &lease,
            &wal,
            false,
        )
        .await;
        let calls = provider.calls();
        assert_eq!(calls[0].marker_order_version, version);
        assert_eq!(
            canonical_request_bytes(&calls[0]),
            render_prompt(&[Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "question".into()
                }]
            )])
        );
        let records = WalStore::read_records(&wal, &session).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
                .count(),
            1
        );
    }
}

#[derive(Clone, Default)]
struct RecordedRequests(Arc<Mutex<Vec<Vec<u8>>>>);

impl RecordedRequests {
    fn push(&self, bytes: Vec<u8>) {
        self.0.lock().unwrap().push(bytes);
    }

    fn snapshot(&self) -> Vec<Vec<u8>> {
        self.0.lock().unwrap().clone()
    }
}

struct RecordingProvider {
    inner: StubProvider,
    recorded: RecordedRequests,
}

impl RecordingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> (Self, RecordedRequests) {
        let recorded = RecordedRequests::default();
        (
            Self {
                inner: StubProvider::new(scripts),
                recorded: recorded.clone(),
            },
            recorded,
        )
    }
}

#[async_trait]
impl broca_core::provider::Provider for RecordingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.recorded.push(canonical_request_bytes(opts));
        broca_core::provider::Provider::step(&self.inner, step_id, opts).await
    }
}

async fn drive<P>(
    req: RunRequest,
    provider: &P,
    plane: &StubToolPlane,
    lease: &FileLeaseStore,
    wal: &FileWalStore,
    append: bool,
) -> broca_core::RunSummary
where
    P: broca_core::provider::Provider,
{
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = if append {
        run_loop_append_episode(req, provider, plane, lease, &AllowAll, wal, None, &tx).await
    } else {
        run_loop(req, provider, plane, lease, &AllowAll, wal, &tx).await
    }
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    summary
}

async fn drive_with_admission(
    req: RunRequest,
    provider: &StubProvider,
    plane: &StubToolPlane,
    wal: &FileWalStore,
) -> (
    Result<broca_core::RunSummary, RunError>,
    Result<RunId, RunError>,
) {
    let writer =
        WalHandle::spawn(wal.open(&req.session).expect("open WAL"), 1).expect("spawn writer");
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let (admit_tx, admit_rx) = tokio::sync::oneshot::channel();
    let result = run_loop_on_writer_admitted(
        req,
        &writer,
        StartMode::FreshOrResume,
        provider,
        plane,
        &AllowAll,
        wal,
        None,
        RunControl::default(),
        &tx,
        Some(admit_tx),
    )
    .await;
    drop(tx);
    collector.await.unwrap();
    let _ = writer.shutdown().await;
    let admitted = admit_rx.await.expect("admission ack fired");
    (result, admitted)
}

/// Append a second turn (episode) to a finished lineage: the WAL becomes a 2-episode
/// lineage, the second episode's rendered prompt carries the FIRST episode's
/// conversation as prefix, and the combined WAL replays coherently.
#[tokio::test]
async fn append_episode_continues_a_finished_lineage() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-append-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // Episode 1: a fresh single-turn run.
    let p1 = StubProvider::new(vec![text_reply("m1", "first answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "first question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);

    // Episode 2: APPEND a new turn. The provider must SEE the prior conversation in
    // its rendered prompt — assert it by capturing what the loop sends.
    let p2 = StubProvider::new(vec![text_reply("m2", "second answer")]);
    let s2 = drive(
        text_request("r2", &session, "second question"),
        &p2,
        &plane,
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(s2.reason, RunFinishReason::Completed);

    // The WAL is now a TWO-episode lineage: two RunStarted records.
    let recs: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let run_starts = recs
        .iter()
        .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(run_starts, 2, "the lineage has two episodes");

    // Replay the combined WAL: it must be a coherent lineage ending idle, whose prompt
    // contains BOTH turns' user messages AND both assistant answers (prefix carried).
    let st = replay(&recs);
    assert!(matches!(
        st.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    let text: String = st
        .prompt
        .iter()
        .flat_map(|m| m.content.iter())
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(
        text.contains("first question")
            && text.contains("first answer")
            && text.contains("second question")
            && text.contains("second answer"),
        "the 2-episode prompt carries both turns: {text}"
    );
    // The recovered run_id is the LAST episode's.
    assert_eq!(st.run_id, Some(RunId("r2".into())));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_seals_an_inferred_natural_end_before_starting_the_next_episode() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-natural-seal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let mut file = wal.open(&session).expect("open WAL for crash cut");
    let crash_cut = [
        WalRecord::RunStarted {
            written_at_ms: None,
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
            session: session.clone(),
            config: Box::new(run_config()),
            input: text_request("r1", &session, "first question").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::StepStarted { step_id: StepId(1) },
        step_text(1, "first answer"),
    ];
    for record in crash_cut {
        file.append(1, &record, true).expect("seed crash cut");
    }
    drop(file);

    let (provider, requests) = RecordingProvider::new(vec![text_reply("m2", "second answer")]);
    let summary = drive(
        text_request("r2", &session, "second question"),
        &provider,
        &StubToolPlane::new(),
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        requests.snapshot().len(),
        1,
        "repairing r1 must not re-run its model step"
    );

    let frames = wal.read_records(&session).expect("read repaired WAL");
    let seal = frames
        .iter()
        .position(|frame| {
            matches!(
                frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        })
        .expect("r1 receives a durable seal");
    let next_start = frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
            )
        })
        .expect("r2 starts");
    assert!(
        seal < next_start,
        "RunFinished(r1) must precede RunStarted(r2)"
    );
    broca_core::AdmissionCheckpointV1::from_frames(&frames)
        .expect("the repaired lineage folds into an admission checkpoint");
    replay_raw(
        &frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("the repaired lineage replays");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_seals_an_inferred_hard_stop_before_starting_the_next_episode() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-append-hard-stop-seal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let mut file = wal.open(&session).expect("open WAL for hard-stop cut");
    let crash_cut = [
        WalRecord::RunStarted {
            written_at_ms: None,
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
            session: session.clone(),
            config: Box::new(run_config()),
            input: text_request("r1", &session, "first question").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::StepStarted { step_id: StepId(1) },
        step_with_call(1, "t1"),
        batch_started("b1", 1, &["t1"]),
        result("b1", "t1", DispatchOutcome::MaxSteps),
        batch_finished("b1"),
    ];
    for record in crash_cut {
        file.append(1, &record, true).expect("seed hard-stop cut");
    }
    drop(file);

    let (provider, requests) = RecordingProvider::new(vec![text_reply("m2", "second answer")]);
    let summary = drive(
        text_request("r2", &session, "second question"),
        &provider,
        &StubToolPlane::new(),
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        requests.snapshot().len(),
        1,
        "repairing the hard stop must not re-run r1"
    );

    let frames = wal.read_records(&session).expect("read repaired WAL");
    let seal = frames
        .iter()
        .position(|frame| {
            matches!(
                frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::MaxSteps,
                    ..
                }
            )
        })
        .expect("the hard stop receives its durable seal");
    let next_start = frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
            )
        })
        .expect("r2 starts");
    assert!(
        seal < next_start,
        "RunFinished(r1) must precede RunStarted(r2)"
    );
    broca_core::AdmissionCheckpointV1::from_frames(&frames)
        .expect("the hard-stop lineage folds into an admission checkpoint");
    replay_raw(
        &frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("the hard-stop lineage replays");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Appending over an empty WAL degenerates to a fresh run (no prior conversation),
/// not an error.
#[tokio::test]
async fn append_over_empty_wal_is_a_fresh_run() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let p = StubProvider::new(vec![text_reply("m1", "hello")]);
    let plane = StubToolPlane::new();
    let s = drive(
        text_request("r1", &session, "hi"),
        &p,
        &plane,
        &lease,
        &wal,
        true, // append over an EMPTY wal
    )
    .await;
    assert_eq!(
        s.reason,
        RunFinishReason::Completed,
        "append over an empty WAL runs fresh and completes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A plain send (NO explicit append) to an idle lineage with prior episodes, carrying a
/// FRESH run_id, AUTO-APPENDS a new episode — it must NOT no-op and silently drop the new
/// turn. This is the ergonomic default: a client just sends the next turn, no flag. The
/// distinguishing signal is run_id identity (a new turn always mints a fresh id).
#[tokio::test]
async fn plain_send_with_a_fresh_run_id_auto_appends_a_new_episode() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-autoappend-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // Episode 1: a fresh single-turn run (run_id r1).
    let p1 = StubProvider::new(vec![text_reply("m1", "first answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "first question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);

    // Turn 2: a PLAIN send (append=false, the default) with a FRESH run_id r2. This is the
    // exact path a client next-turn send takes — no explicit append flag.
    let p2 = StubProvider::new(vec![text_reply("m2", "second answer")]);
    let s2 = drive(
        text_request("r2", &session, "second question"),
        &p2,
        &plane,
        &lease,
        &wal,
        false, // NO explicit append — the run_id-identity promotion must append
    )
    .await;
    assert_eq!(
        s2.reason,
        RunFinishReason::Completed,
        "the plain next-turn send ran (not a no-op that dropped the input)"
    );

    // The WAL is now a TWO-episode lineage, and the prompt carried BOTH turns — proving the
    // send appended rather than no-op'd.
    let recs: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let run_starts = recs
        .iter()
        .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(
        run_starts, 2,
        "the plain fresh-run_id send appended a 2nd episode"
    );
    let st = replay(&recs);
    let text: String = st
        .prompt
        .iter()
        .flat_map(|m| m.content.iter())
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(
        text.contains("first question") && text.contains("second question"),
        "the auto-appended episode carries the prior conversation: {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The BOUNDARY the auto-append must never cross: a crash-recovery re-drive of a COMPLETED
/// run — the SAME run_id, input still populated, over a complete (LineageIdle) WAL — must
/// be an idempotent NO-OP, never a duplicate episode. This is the exact case the LineageIdle
/// no-op exists for; it guards the new fresh-run_id-appends branch's edge (only a DIFFERENT
/// run_id appends).
#[tokio::test]
async fn crash_redrive_same_run_id_with_input_is_a_noop_not_a_duplicate_episode() {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-redrive-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // Episode 1: a fresh run (run_id r1) that completes.
    let p1 = StubProvider::new(vec![text_reply("m1", "answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);
    let records_before = WalStore::read_records(&wal, &session).unwrap();
    let run_starts_before = records_before
        .iter()
        .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(run_starts_before, 1);
    let record_count_before = records_before.len();

    // RE-DRIVE the SAME run_id r1 with the SAME input over the complete WAL (a crash
    // that lost the reply between RunFinished-durable and the caller learning the outcome →
    // the caller re-issues the identical request). This MUST be an idempotent no-op: the
    // run_id matches the last episode's, so it is a re-drive, NOT a new turn.
    let p2 = StubProvider::new(vec![text_reply("m-should-not-run", "DUPLICATE")]);
    let s2 = drive(
        text_request("r1", &session, "question"),
        &p2,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(
        s2.reason,
        RunFinishReason::Completed,
        "the re-drive resolves the durable terminal (idempotent)"
    );

    // NO second episode was written: still exactly one RunStarted.
    let records_after = WalStore::read_records(&wal, &session).unwrap();
    let run_starts_after = records_after
        .iter()
        .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(
        run_starts_after, 1,
        "a same-run_id re-drive is a no-op, NOT a duplicate episode"
    );
    assert_eq!(
        records_after.len(),
        record_count_before,
        "an idempotent same-run_id retry appends no new durable records"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reusing a same-run-id idle lineage send for DIFFERENT input must fail LOUD before
/// admission. Without this guard the loop would silently no-op and drop the new prompt.
#[tokio::test]
async fn crash_redrive_same_run_id_with_different_input_fails_loud_and_writes_nothing() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-redrive-mismatch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let p1 = StubProvider::new(vec![text_reply("m1", "answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);

    let records_before = WalStore::read_records(&wal, &session).unwrap();
    let run_starts_before = records_before
        .iter()
        .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
        .count();
    let record_count_before = records_before.len();

    let p2 = StubProvider::new(vec![text_reply("m-should-not-run", "DUPLICATE")]);
    let (result, admitted) = drive_with_admission(
        text_request("r1", &session, "different question"),
        &p2,
        &plane,
        &wal,
    )
    .await;
    assert!(
        matches!(result, Err(RunError::SendIdReuse { .. })),
        "reusing the same run_id/send_id for different input must fail loud"
    );
    assert!(
        matches!(admitted, Err(RunError::SendIdReuse { .. })),
        "the admission ack must surface the typed pre-admission error"
    );

    let records_after = WalStore::read_records(&wal, &session).unwrap();
    let run_starts_after = records_after
        .iter()
        .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(
        run_starts_after, run_starts_before,
        "no new episode opened on mismatch"
    );
    assert_eq!(
        records_after.len(),
        record_count_before,
        "the mismatch path writes no RunStarted and no extra durable records"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The internal empty-input re-drive shape (resume tooling) stays the existing idle no-op.
#[tokio::test]
async fn crash_redrive_same_run_id_with_empty_input_is_still_a_noop() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-redrive-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let p1 = StubProvider::new(vec![text_reply("m1", "answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);

    let records_before = WalStore::read_records(&wal, &session).unwrap();
    let record_count_before = records_before.len();

    let mut empty_redrive = text_request("r1", &session, "unused");
    empty_redrive.input.clear();
    let p2 = StubProvider::new(vec![text_reply("m-should-not-run", "DUPLICATE")]);
    let s2 = drive(empty_redrive, &p2, &plane, &lease, &wal, false).await;
    assert_eq!(
        s2.reason,
        RunFinishReason::Completed,
        "an empty-input same-run_id re-drive stays the existing no-op"
    );

    let records_after = WalStore::read_records(&wal, &session).unwrap();
    assert_eq!(
        records_after.len(),
        record_count_before,
        "the empty-input re-drive appends no new durable records"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Resuming after a crash that spans two turns still gives exactly-once tool
/// execution. The session WAL holds two turns (episodes): turn 1 fully completes its
/// tool call; turn 2 records the INTENT to call its tool but crashes before the result
/// is written. On resume the loop must run only turn 2's missing call exactly once and
/// must NEVER re-run turn 1's already-completed tool. (Only running the real loop — not
/// re-reading the log — can prove the tool dispatch count, which is why this is a
/// separate execution test.)
#[tokio::test]
async fn cross_episode_resume_is_exactly_once() {
    let dir = broca_test_support::scratch_root().join(format!("broca-xep-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal_store = FileWalStore::new(dir.join("wal"));
    let wal_path = wal_store.wal_path(&session);

    // A tool request whose model calls `edit` once, then answers.
    let req1 = RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("r1".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "turn-1".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };

    // Episode 1: the model calls `e1` then answers. Drive it fully.
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text { text: "ran".into() })
    });
    let p1 = StubProvider::new(vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: assistant_tool_call("e1", "edit"),
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        text_reply("m1b", "turn-1 done"),
    ]);
    {
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(req1, &p1, &plane, &lease, &AllowAll, &wal_store, &tx)
            .await
            .unwrap();
        drop(tx);
        collector.await.unwrap();
    }
    assert_eq!(
        plane.exec_count(&ToolCallId("e1".into())),
        1,
        "episode 1's tool ran once during its live turn"
    );

    // Seed episode 2 as a CRASH mid-batch: a new RunStarted, a model step with one tool
    // call `e2`, the batch opened + the intent durable, but NO result (crash).
    {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let recs = vec![
            WalRecord::RunStarted {
                written_at_ms: None,
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
                run_id: RunId("r2".into()),
                session: session.clone(),
                ts_ms: None,
                config: Box::new(RunConfig {
                    tools: vec![Tool {
                        name: "edit".into(),
                        description: "edit".into(),
                        input_schema: json!({"type": "object"}),
                        module: None,
                        execution_mode: ExecutionMode::Parallel,
                        reply: None,
                    }]
                    .into(),
                    ..run_config()
                }),
                input: vec![Message {
                    cache_prefix_blocks: None,
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: "turn-2".into(),
                    }],
                }]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: assistant_tool_call("e2", "edit").into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("r2-b1".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("e2".into())],
            },
            // The crash point: e2 appears only in planned_calls — no intent record and
            // no result record — so on resume the loop dispatches e2 exactly once.
        ];
        for r in &recs {
            wal.append(1, r, true).unwrap();
        }
    }

    // RESUME the lineage (the last episode is unfinished): the provider supplies only
    // episode 2's step 2 (the text reply after the tool completes).
    let p2 = StubProvider::new(vec![text_reply("m2b", "turn-2 done")]);
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(
        req2(&session),
        &p2,
        &plane,
        &lease,
        &AllowAll,
        &wal_store,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(
        summary.reason,
        RunFinishReason::Completed,
        "the resumed episode finishes"
    );
    assert_eq!(
        plane.exec_count(&ToolCallId("e2".into())),
        1,
        "episode 2's missing call ran exactly once on resume"
    );
    assert_eq!(
        plane.exec_count(&ToolCallId("e1".into())),
        1,
        "episode 1's tool NEVER re-ran across the episode boundary (still 1)"
    );

    // The final WAL is a coherent 2-episode lineage ending idle.
    let final_recs: Vec<WalRecord> = WalStore::read_records(&wal_store, &session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let st = replay(&final_recs);
    assert!(matches!(
        st.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    assert_provider_valid(&st.prompt);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_episode_strips_a_byte_identical_system_resend_before_persist() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-system-dedup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let first = drive(
        text_request_with_system("r1", &session, "system rules", "first question"),
        &StubProvider::new(vec![text_reply("m1", "first answer")]),
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(first.reason, RunFinishReason::Completed);

    let second = drive(
        text_request_with_system("r2", &session, "system rules", "second question"),
        &StubProvider::new(vec![text_reply("m2", "second answer")]),
        &plane,
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(second.reason, RunFinishReason::Completed);

    let records: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    let starts: Vec<&WalRecord> = records
        .iter()
        .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
        .collect();
    assert_eq!(starts.len(), 2, "the lineage still contains two episodes");
    let WalRecord::RunStarted { input, .. } = starts[1] else {
        unreachable!("filtered as RunStarted")
    };
    assert!(
        input.iter().all(|message| message.role != Role::System),
        "the second episode persisted without the duplicate system message"
    );

    let state = replay(&records);
    assert_eq!(
        count_system_messages(&state.prompt),
        1,
        "the replayed lineage contains exactly one system prompt"
    );
    assert_eq!(state.run_id, Some(RunId("r2".into())));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_episode_resume_keeps_request_bytes_identical_after_system_dedup() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-system-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let first = drive(
        text_request_with_system("r1", &session, "system rules", "first question"),
        &StubProvider::new(vec![text_reply("m1", "first answer")]),
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(first.reason, RunFinishReason::Completed);

    let (append_provider, live_recorded) =
        RecordingProvider::new(vec![text_reply("m2", "second answer")]);
    let second = drive(
        text_request_with_system("r2", &session, "system rules", "second question"),
        &append_provider,
        &plane,
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(second.reason, RunFinishReason::Completed);
    let live_bytes = live_recorded.snapshot();
    assert_eq!(live_bytes.len(), 1, "the live append recorded one request");

    let records: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    let second_start = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunStarted { run_id, .. } if run_id == &RunId("r2".into())))
        .expect("the appended episode wrote RunStarted");
    let crash_records = records[..=second_start].to_vec();

    let resume_dir = dir.join("resume");
    std::fs::create_dir_all(&resume_dir).unwrap();
    let resume_lease = FileLeaseStore::new(resume_dir.join("lease"));
    let resume_wal = FileWalStore::new(resume_dir.join("wal"));
    let resume_path = resume_wal.wal_path(&session);
    {
        let mut wal_file = FileWal::open(&resume_path)
            .unwrap()
            .with_feature_floor(resume_wal.feature_floor_path());
        for record in &crash_records {
            wal_file.append(1, record, true).unwrap();
        }
    }

    let (resume_provider, resumed_recorded) =
        RecordingProvider::new(vec![text_reply("m2-resume", "second answer")]);
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(
        text_request_with_system("r2", &session, "system rules", "second question"),
        &resume_provider,
        &plane,
        &resume_lease,
        &AllowAll,
        &resume_wal,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        resumed_recorded.snapshot(),
        live_bytes,
        "resume sends byte-identical request bytes because the stripped system input is durable"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_episode_with_different_system_fails_before_admission_and_persist() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-append-system-mismatch-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let first = drive(
        text_request_with_system("r1", &session, "system rules", "first question"),
        &StubProvider::new(vec![text_reply("m1", "first answer")]),
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(first.reason, RunFinishReason::Completed);

    let writer = WalHandle::spawn(wal.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let before_count = WalStore::read_records(&wal, &session).unwrap().len();
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let (admit_tx, admit_rx) = tokio::sync::oneshot::channel();
    let result = run_loop_on_writer_admitted(
        text_request_with_system("r2", &session, "changed rules", "second question"),
        &writer,
        StartMode::AppendEpisode,
        &StubProvider::new(vec![text_reply("m2", "should not run")]),
        &plane,
        &AllowAll,
        &wal,
        None,
        RunControl::default(),
        &tx,
        Some(admit_tx),
    )
    .await;
    drop(tx);
    collector.await.unwrap();
    let _ = writer.shutdown().await;
    let admitted = admit_rx.await.expect("admission ack fired");

    assert!(matches!(result, Err(RunError::SystemPromptChanged)));
    assert!(matches!(admitted, Err(RunError::SystemPromptChanged)));
    assert_eq!(
        WalStore::read_records(&wal, &session).unwrap().len(),
        before_count,
        "the rejected append writes no new RunStarted"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_episode_cannot_introduce_a_system_prompt_mid_lineage() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-append-system-introduce-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let first = drive(
        text_request("r1", &session, "first question"),
        &StubProvider::new(vec![text_reply("m1", "first answer")]),
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(first.reason, RunFinishReason::Completed);

    let writer = WalHandle::spawn(wal.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let before_count = WalStore::read_records(&wal, &session).unwrap().len();
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let (admit_tx, admit_rx) = tokio::sync::oneshot::channel();
    let result = run_loop_on_writer_admitted(
        text_request_with_system("r2", &session, "new rules", "second question"),
        &writer,
        StartMode::AppendEpisode,
        &StubProvider::new(vec![text_reply("m2", "should not run")]),
        &plane,
        &AllowAll,
        &wal,
        None,
        RunControl::default(),
        &tx,
        Some(admit_tx),
    )
    .await;
    drop(tx);
    collector.await.unwrap();
    let _ = writer.shutdown().await;
    let admitted = admit_rx.await.expect("admission ack fired");

    assert!(matches!(result, Err(RunError::SystemPromptChanged)));
    assert!(matches!(admitted, Err(RunError::SystemPromptChanged)));
    assert_eq!(
        WalStore::read_records(&wal, &session).unwrap().len(),
        before_count,
        "a mid-lineage system introduction writes no durable records"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The resume request for episode 2 (same session; run_id is reused from the WAL on
/// resume, so this id is only the caller's handle).
fn req2(session: &BindIdentity) -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("r2".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "turn-2".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

#[tokio::test]
async fn deterministic_send_id_reuse_rejects_changed_budget() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-budget-reuse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let mut first = text_request("r-budget-reuse", &session, "question");
    first.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 20,
            text: "first budget".into(),
        }],
        finalize_at_tool_calls: 30,
        finalize_text: "first finalize".into(),
    });
    drive(
        first,
        &StubProvider::new(vec![text_reply("m1", "answer")]),
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;

    let mut reused = text_request("r-budget-reuse", &session, "question");
    reused.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 20,
            text: "different budget".into(),
        }],
        finalize_at_tool_calls: 30,
        finalize_text: "first finalize".into(),
    });
    let (tx, mut rx) = mpsc::channel(32);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop(
        reused,
        &StubProvider::new(vec![text_reply("unused", "unused")]),
        &plane,
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();
    assert!(matches!(result, Err(RunError::SendIdReuse { .. })));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn recreated_retry_reports_omitted_vs_explicit_tools_difference() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-tools-reuse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let plane = StubToolPlane::new();

    let first = text_request("r-tools-reuse", &session, "question");
    let (tx, mut rx) = mpsc::channel(32);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_control(
        first,
        &StubProvider::new(vec![text_reply("m1", "answer")]),
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            tools_were_omitted: true,
            ..Default::default()
        },
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    let retry = text_request("r-tools-reuse", &session, "question");
    let (tx, mut rx) = mpsc::channel(32);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop_with_control(
        retry,
        &StubProvider::new(vec![text_reply("unused", "unused")]),
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl::default(),
        &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();
    assert!(matches!(
        result,
        Err(RunError::SendIdReuse { differing: "tools" })
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn post_pause_step_boundary_clears_auth_classification_at_every_cut() {
    let paused = WalRecord::RunPaused {
        ts_ms: None,
        run_id: RunId("r".into()),
        reason: broca_protocol::PauseReason::AuthRequired,
        error: None,
    };

    let no_tool = vec![
        start("r", "hi"),
        paused.clone(),
        WalRecord::StepStarted { step_id: StepId(1) },
        step_text(1, "done"),
    ];
    for cut in 3..=no_tool.len() {
        assert!(
            !matches!(
                replay(&no_tool[..cut]).resolution,
                ReplayResolution::AuthRequired { .. }
            ),
            "post-pause cut {cut} must not retain the stale pause"
        );
    }
    assert!(matches!(
        replay(&no_tool).resolution,
        ReplayResolution::LineageIdle { .. }
    ));

    let tool_turn = vec![
        start("r", "hi"),
        paused,
        WalRecord::StepStarted { step_id: StepId(1) },
        step_with_call(1, "c1"),
        batch_started("b1", 1, &["c1"]),
        intent("b1", "c1"),
        result("b1", "c1", DispatchOutcome::Completed),
        batch_finished("b1"),
        WalRecord::TurnFinished { step_id: StepId(1) },
    ];
    for cut in 3..=tool_turn.len() {
        let state = replay(&tool_turn[..cut]);
        assert!(
            !matches!(state.resolution, ReplayResolution::AuthRequired { .. }),
            "post-pause tool cut {cut} must not retain the stale pause"
        );
        if cut == 6 {
            assert!(matches!(
                state.resolution,
                ReplayResolution::Interrupted { .. }
            ));
        }
    }
}

fn budgeted_start(run: &str) -> WalRecord {
    let mut config = run_config();
    config.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 1,
            text: "nudge".into(),
        }],
        finalize_at_tool_calls: 2,
        finalize_text: "finalize".into(),
    });
    match start(run, "hi") {
        WalRecord::RunStarted {
            session,
            input,
            origin,
            submission_id,
            ..
        } => WalRecord::RunStarted {
            written_at_ms: None,
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
            session,
            config: Box::new(config),
            input,
            origin,
            submission_id,
        },
        _ => unreachable!(),
    }
}

fn budget_fired(
    run: &str,
    kind: broca_protocol::request::BudgetKind,
    threshold: u64,
    ordinal: u64,
) -> WalRecord {
    WalRecord::BudgetFired {
        tags: None,
        run_id: RunId(run.into()),
        kind,
        threshold,
        ordinal,
        caller_text: None,
    }
}

#[test]
fn budget_fired_semantic_validation_rejects_wrong_early_reordered_and_duplicate_records() {
    use broca_protocol::request::BudgetKind;

    let early = vec![
        budgeted_start("r"),
        budget_fired("r", BudgetKind::Nudge, 1, 1),
    ];
    assert!(replay_raw(&early)
        .unwrap_err()
        .to_string()
        .contains("early"));

    let mut one_call = vec![
        budgeted_start("r"),
        step_with_call(1, "c1"),
        batch_started("b1", 1, &["c1"]),
        no_dispatch("b1", "c1"),
        batch_finished("b1"),
    ];
    let mut wrong_run = one_call.clone();
    wrong_run.push(budget_fired("other", BudgetKind::Nudge, 1, 3));
    assert!(replay_raw(&wrong_run)
        .unwrap_err()
        .to_string()
        .contains("run_id"));

    one_call.push(budget_fired("r", BudgetKind::Nudge, 1, 3));
    let mut duplicate = one_call.clone();
    duplicate.push(budget_fired("r", BudgetKind::Nudge, 1, 4));
    assert!(replay_raw(&duplicate)
        .unwrap_err()
        .to_string()
        .contains("duplicate"));

    let mut reordered = vec![
        budgeted_start("r"),
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![
                    ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("c1".into()),
                        tool_name: "edit".into(),
                        input: json!({}),
                    }),
                    ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("c2".into()),
                        tool_name: "edit".into(),
                        input: json!({}),
                    }),
                ],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
    ];
    reordered.push(budget_fired("r", BudgetKind::Finalize, 2, 1));
    assert!(replay_raw(&reordered)
        .unwrap_err()
        .to_string()
        .contains("expected order"));
}

#[test]
fn replay_rejects_post_call_and_duplicate_refreeze() {
    let refreeze = WalRecord::RunConfigRefrozen {
        ts_ms: None,
        renderer_version: 2,
        frozen_render_config: json!({"v": 2}),
    };
    let post_call = vec![
        start("r", "hi"),
        WalRecord::StepStarted { step_id: StepId(1) },
        refreeze.clone(),
    ];
    assert!(replay_raw(&post_call)
        .unwrap_err()
        .to_string()
        .contains("after a provider call"));

    let duplicate = vec![start("r", "hi"), refreeze.clone(), refreeze];
    assert!(replay_raw(&duplicate)
        .unwrap_err()
        .to_string()
        .contains("more than once"));
}

#[tokio::test]
async fn imported_lineage_auto_appends_on_the_next_plain_send() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-import-continue-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let wal = FileWalStore::new(dir.join("wal"));
    let writer =
        WalHandle::spawn(wal.open(&session).expect("open import WAL"), 1).expect("spawn writer");
    let imported = vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "imported question".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "imported answer".into(),
            }],
        ),
    ];
    run_import(
        ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("run-import".into()),
            session: session.clone(),
            messages: imported.clone(),
            origin: ImportOrigin {
                source_ref: "cc:session-1".into(),
            },
        },
        &writer,
        &wal,
    )
    .await
    .expect("import");
    let _ = writer.shutdown().await;

    let lease = FileLeaseStore::new(dir.join("lease"));
    let provider = StubProvider::new(vec![text_reply("m-next", "continued answer")]);
    let summary = drive(
        text_request("run-next", &session, "next question"),
        &provider,
        &StubToolPlane::new(),
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let calls = provider.calls();
    assert_eq!(calls.len(), 1);
    let rendered_text = calls[0]
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rendered_text,
        vec!["imported question", "imported answer", "next question"]
    );

    let records: Vec<_> = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count(),
        2
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn first_send_after_import_can_introduce_its_harness_system_prompt() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-import-system-continue-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let wal = FileWalStore::new(dir.join("wal"));
    let writer =
        WalHandle::spawn(wal.open(&session).expect("open import WAL"), 1).expect("spawn writer");
    run_import(
        ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("run-import-system".into()),
            session: session.clone(),
            messages: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "imported question".into(),
                }],
            )],
            origin: ImportOrigin {
                source_ref: "cc:system-session".into(),
            },
        },
        &writer,
        &wal,
    )
    .await
    .expect("import");
    let _ = writer.shutdown().await;

    let lease = FileLeaseStore::new(dir.join("lease"));
    let provider = StubProvider::new(vec![text_reply("m-system", "continued answer")]);
    let summary = drive(
        text_request_with_system(
            "run-next-system",
            &session,
            "new harness rules",
            "next question",
        ),
        &provider,
        &StubToolPlane::new(),
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let calls = provider.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(count_system_messages(&calls[0].prompt), 1);
    assert_eq!(
        calls[0]
            .prompt
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec!["imported question", "new harness rules", "next question"]
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// A caller recovering from an interrupted run appends a NEW episode to the same session, and
/// this proves the engine actually supports that end to end -- not just that replay classifies
/// the sealed interrupt as idle.
///
/// The distinction matters because the two are easy to conflate: `classify` returning
/// `LineageIdle{Interrupted}` says the lineage is APPENDABLE, but appendable is a claim about
/// replay, and a caller needs the append to run, carry the prior conversation, and leave a
/// coherent two-episode lineage. Existing coverage appends after a COMPLETED episode; nothing
/// exercised the path after a sealed interrupt, and it has never occurred in production -- 277
/// interrupted runs, none followed by another run in the same session.
///
/// The recovery is a FRESH send_id, not the original. Re-sending the original is a documented
/// no-op, because an interrupted run is terminal in the run index.
#[tokio::test]
async fn a_new_episode_appends_onto_a_lineage_sealed_by_an_interrupt() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-interrupt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    // Episode 1: a run that reached a durable terminal, then a seal recording that a later
    // attempt was cut mid-flight. This is the shape a supervisor restart leaves behind.
    let p1 = StubProvider::new(vec![text_reply("m1", "first answer")]);
    let plane = StubToolPlane::new();
    let s1 = drive(
        text_request("r1", &session, "first question"),
        &p1,
        &plane,
        &lease,
        &wal,
        false,
    )
    .await;
    assert_eq!(s1.reason, RunFinishReason::Completed);

    {
        let mut w = FileWal::open(wal.wal_path(&session)).unwrap();
        w.append(
            1,
            &WalRecord::RunStarted {
                written_at_ms: None,
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
                run_id: RunId("r2-cut".into()),
                session: session.clone(),
                config: Box::new(run_config()),
                input: vec![Message {
                    cache_prefix_blocks: None,
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: "interrupted question".into(),
                    }],
                }]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .unwrap();
        w.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Interrupted,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
    }

    let sealed: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    assert!(
        matches!(
            replay(&sealed).resolution,
            ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Interrupted
            }
        ),
        "precondition: the lineage is sealed by an interrupt"
    );

    // The recovery: same session, FRESH send_id, appended as a new episode.
    let p3 = StubProvider::new(vec![text_reply("m3", "recovered answer")]);
    let s3 = drive(
        text_request("r3", &session, "retry question"),
        &p3,
        &plane,
        &lease,
        &wal,
        true,
    )
    .await;
    assert_eq!(
        s3.reason,
        RunFinishReason::Completed,
        "a fresh episode must run to completion over a lineage sealed by an interrupt"
    );

    let recs: Vec<WalRecord> = WalStore::read_records(&wal, &session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let st = replay(&recs);
    let text: String = st
        .prompt
        .iter()
        .flat_map(|m| m.content.iter())
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(
        text.contains("first question") && text.contains("first answer"),
        "the recovery turn carries the pre-interrupt conversation as its prefix, which is the \
         whole reason to reuse the session rather than start a new one: {text}"
    );
    assert!(
        text.contains("retry question") && text.contains("recovered answer"),
        "and the recovery turn itself is in the lineage: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────── content-filter fallback: refused steps and the schedule ───────────────

const FALLBACK_SOURCE: &str = "claude-opus-5-5";
const FALLBACK_MODEL: &str = "claude-opus-4-8";

fn fallback_handle() -> ModelHandle {
    ModelHandle::new("anthropic", FALLBACK_MODEL)
}

fn source_handle() -> ModelHandle {
    ModelHandle::new("anthropic", FALLBACK_SOURCE)
}

/// A run start on the Anthropic source model, admitted at `ts_ms`.
fn source_start(run: &str, model: &str, ts_ms: Option<u64>) -> WalRecord {
    let mut record = start(run, "hi");
    if let WalRecord::RunStarted {
        config, ts_ms: at, ..
    } = &mut record
    {
        config.model = ModelHandle::new("anthropic", model);
        *at = ts_ms;
    }
    record
}

fn schedule(phase: serde_json::Value, re_refusals: u8, at: u64) -> serde_json::Value {
    json!({"source": source_handle(), "fallback": fallback_handle(), "phase": phase,
        "re_refusals": re_refusals, "updated_at_ms": at})
}

fn refused(step: u64, input_tokens: u64, schedule: serde_json::Value) -> WalRecord {
    serde_json::from_value(json!({"type":"model_step_refused","step_id":step,
        "usage":{"input_tokens":input_tokens,"output_tokens":2},"discarded_blocks":1,
        "schedule":schedule}))
    .unwrap()
}

/// A committed step with the fallback members `extra` merged in.
fn fallback_step(step: u64, content: serde_json::Value, extra: serde_json::Value) -> WalRecord {
    let mut value = json!({"type":"model_step_finished","step_id":step,
        "assistant_message":{"message_id":format!("m{step}"),"content":content},
        "usage":{"input_tokens":5,"output_tokens":3},"retries_used":0});
    value
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(value).unwrap()
}

fn routed(reason: &str) -> serde_json::Value {
    json!({"model": fallback_handle(), "reason": reason})
}

fn finished(usage: Usage) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage,
        error: None,
        retries_used: 0,
    }
}

/// The records of a plain source refusal of step 1, re-sent to the fallback model: the run
/// start, the step, the refused attempt, the refusal (synced before the re-send), the
/// fallback's attempt, and the fallback's committed answer.
fn plain_refusal_records() -> Vec<WalRecord> {
    vec![
        source_start("r", FALLBACK_SOURCE, Some(1_000)),
        WalRecord::StepStarted { step_id: StepId(1) },
        attempt_record(StepId(1), 0, AttemptOutcome::Complete),
        refused(
            1,
            100,
            schedule(json!({"kind":"fallback","remaining":10}), 0, 2_000),
        ),
        attempt_record(StepId(1), 1, AttemptOutcome::Complete),
        fallback_step(
            1,
            json!([{"type":"text","text":"fallback answer"}]),
            json!({"route": routed("refusal"),
                "refusal_fallback": schedule(json!({"kind":"fallback","remaining":9}), 0, 3_000)}),
        ),
    ]
}

fn assistant_messages(prompt: &[Message]) -> Vec<&Message> {
    prompt
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .collect()
}

/// A refused-then-re-sent step replays as the fallback's answer alone, stamped with the
/// fallback model; the refused call adds its usage and leaves the schedule it armed.
#[test]
fn a_refused_step_replays_as_the_fallback_answer_with_both_calls_billed() {
    let mut records = plain_refusal_records();
    records.push(finished(Usage::default()));
    let state = replay(&records);
    let answers = assistant_messages(&state.prompt);
    assert_eq!(answers.len(), 1, "{:?}", state.prompt);
    assert_eq!(
        answers[0].content,
        vec![ContentBlock::Text {
            text: "fallback answer".into()
        }]
    );
    assert_eq!(answers[0].origin, Some(fallback_handle()));
    assert_eq!(state.completed_steps, 1);
    assert_eq!(state.usage.input_tokens, Some(105));
    assert_eq!(state.usage.output_tokens, Some(5));
    assert_eq!(
        state.refused_steps,
        [StepId(1)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );
    let schedule = state
        .refusal_fallback
        .expect("the schedule survives replay");
    assert_eq!(
        schedule.phase,
        broca_wal::RefusalFallbackPhase::Fallback { remaining: 9 }
    );
    assert_eq!(
        broca_core::refusal_fallback::route(Some(&schedule), true),
        broca_core::refusal_fallback::StepRoute::Fallback
    );

    // `by_model`: the refused call under the source, the answer under the fallback.
    let mut usage = broca_core::replay::RunUsageProjection::new(RunId("r".into()));
    for record in &records {
        usage.observe(record);
    }
    let rows = serde_json::to_value(&usage.usage).unwrap();
    let by_model = rows["by_model"].as_array().unwrap();
    let row = |model: &str| {
        by_model
            .iter()
            .find(|row| row["model"] == model)
            .unwrap_or_else(|| panic!("no {model} row in {rows}"))
            .clone()
    };
    assert_eq!(by_model.len(), 2, "{rows}");
    assert_eq!(row(FALLBACK_SOURCE)["steps"], 1);
    assert_eq!(row(FALLBACK_SOURCE)["input_tokens"], 100);
    assert_eq!(row(FALLBACK_MODEL)["steps"], 1);
    assert_eq!(row(FALLBACK_MODEL)["input_tokens"], 5);
}

/// Crash cuts at each boundary of a plain refusal. Before the refusal is durable the step is
/// redone on the source with no schedule; after it, the step is redone with the schedule
/// routing it to the fallback; a lost fallback answer reads the same as the cut after the
/// refusal; a durable one is the fallback's message.
#[test]
fn refusal_crash_cuts_replay_to_the_expected_route_and_state() {
    let records = plain_refusal_records();
    // Before the refusal record is durable.
    let before = replay(&records[..3]);
    assert_eq!(
        before.resolution,
        ReplayResolution::Resume { pending: None }
    );
    assert_eq!(before.refusal_fallback, None);
    assert!(before.refused_steps.is_empty());
    assert_eq!(before.usage, Usage::default());
    assert!(assistant_messages(&before.prompt).is_empty());
    assert_eq!(
        broca_core::refusal_fallback::route(before.refusal_fallback.as_ref(), true),
        broca_core::refusal_fallback::StepRoute::Source
    );

    // After the refusal, with or without the fallback's attempt, and with the fallback's
    // answer queued but never synced: the same state.
    for cut in [4, 5] {
        let after = replay(&records[..cut]);
        assert_eq!(after.resolution, ReplayResolution::Resume { pending: None });
        assert_eq!(after.completed_steps, 0);
        assert!(assistant_messages(&after.prompt).is_empty());
        assert_eq!(after.usage.input_tokens, Some(100));
        assert!(after.refused_steps.contains(&StepId(1)));
        assert_eq!(
            broca_core::refusal_fallback::route(after.refusal_fallback.as_ref(), true),
            broca_core::refusal_fallback::StepRoute::Fallback
        );
    }

    // The fallback's answer is durable but the run's terminal is not.
    let answered = replay(&records);
    assert!(matches!(
        answered.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    assert_eq!(
        assistant_messages(&answered.prompt)[0].origin,
        Some(fallback_handle())
    );
}

/// A refusal after a completed tool call commits the step as a tool step and arms the
/// schedule; a cut after the tool-plan barrier keeps both, the call is not re-sent to the
/// model, and the next step routes to the fallback.
#[test]
fn a_refusal_after_tool_use_survives_a_cut_after_the_tool_plan_barrier() {
    let records = vec![
        source_start("r", FALLBACK_SOURCE, Some(1_000)),
        WalRecord::StepStarted { step_id: StepId(1) },
        fallback_step(
            1,
            json!([{"type":"tool_call","tool_call_id":"t1","tool_name":"edit","input":{}}]),
            json!({"finish_reason":"tool_calls","refusal_after_tool_use":true,
                "refusal_fallback": schedule(json!({"kind":"fallback","remaining":10}), 0, 2_000)}),
        ),
        batch_started("b1", 1, &["t1"]),
        intent("b1", "t1"),
    ];
    let state = replay(&records);
    assert!(
        matches!(state.resolution, ReplayResolution::Interrupted { ref indeterminate } if indeterminate == &[ToolCallId("t1".into())]),
        "{:?}",
        state.resolution
    );
    assert_eq!(
        state.open_tool_turn.as_ref().unwrap().0.origin,
        Some(source_handle())
    );
    assert!(state.refused_steps.is_empty());
    assert_eq!(
        broca_core::refusal_fallback::route(state.refusal_fallback.as_ref(), true),
        broca_core::refusal_fallback::StepRoute::Fallback
    );

    // The batch commits and step 2 goes to the fallback by the schedule.
    let mut records = records;
    records.extend([
        result("b1", "t1", DispatchOutcome::Completed),
        batch_finished("b1"),
        WalRecord::TurnFinished { step_id: StepId(1) },
        WalRecord::StepStarted { step_id: StepId(2) },
        fallback_step(
            2,
            json!([{"type":"text","text":"done"}]),
            json!({"route": routed("schedule"),
                "refusal_fallback": schedule(json!({"kind":"fallback","remaining":9}), 0, 3_000)}),
        ),
    ]);
    let state = replay(&records);
    let answers = assistant_messages(&state.prompt);
    assert_eq!(answers.len(), 2);
    assert_eq!(answers[0].origin, Some(source_handle()));
    assert_eq!(answers[1].origin, Some(fallback_handle()));
}

/// A step is re-sent at most once: a second refusal record for it is corruption.
#[test]
fn a_second_refusal_of_the_same_step_is_refused_as_corrupt() {
    let mut records = plain_refusal_records();
    records.truncate(4);
    records.push(refused(
        1,
        100,
        schedule(json!({"kind":"fallback","remaining":10}), 0, 2_500),
    ));
    match replay_raw(&records) {
        Err(error) => assert!(
            error.to_string().contains("second model_step_refused"),
            "{error}"
        ),
        Ok(state) => panic!("a duplicate refusal replayed: {:?}", state.resolution),
    }
}

/// The schedule carries into the next episode of the same source model, and is cleared by
/// an episode under another model or one admitted more than a day after it last changed.
#[test]
fn the_schedule_carries_across_episodes_until_a_model_change_or_a_day_idle() {
    let mut records = plain_refusal_records();
    records.push(finished(Usage::default()));
    let next = |model: &str, ts: u64| {
        let mut records = records.clone();
        records.push(source_start("r2", model, Some(ts)));
        replay(&records).refusal_fallback
    };
    const DAY: u64 = 24 * 60 * 60 * 1000;
    assert!(next(FALLBACK_SOURCE, 3_000 + DAY).is_some());
    assert_eq!(next(FALLBACK_SOURCE, 3_001 + DAY), None);
    assert_eq!(next("claude-fable-5-1", 4_000), None);
}

/// A step answered by an Anthropic server-side fallback keeps the episode's model as its
/// origin (its served model is attribution, not identity) and counts under the served model.
#[test]
fn a_server_side_fallback_step_keeps_its_origin_and_counts_under_the_served_model() {
    let records = vec![
        source_start("r", FALLBACK_SOURCE, None),
        WalRecord::StepStarted { step_id: StepId(1) },
        fallback_step(
            1,
            json!([{"type":"provider_fallback","from_model":FALLBACK_SOURCE,"to_model":FALLBACK_MODEL},
                {"type":"text","text":"ok"}]),
            json!({"served_model": FALLBACK_MODEL}),
        ),
    ];
    let state = replay(&records);
    let answers = assistant_messages(&state.prompt);
    assert_eq!(answers[0].origin, Some(source_handle()));
    assert!(matches!(
        answers[0].content[0],
        ContentBlock::ProviderFallback { .. }
    ));
    let mut usage = broca_core::replay::RunUsageProjection::new(RunId("r".into()));
    for record in &records {
        usage.observe(record);
    }
    let rows = serde_json::to_value(&usage.usage).unwrap();
    assert_eq!(rows["by_model"][0]["model"], FALLBACK_MODEL, "{rows}");
    assert_eq!(rows["by_model"].as_array().unwrap().len(), 1);
}
