//! Resume paths through the approval gate that seal or re-gate a batch holding Prepared calls:
//! duplicate ids answered from the WAL, a resume whose input budget is already spent, and a
//! resume of a step whose provider attempt was cut for runaway tool arguments.

use super::*;
use broca_protocol::request::{BudgetConfig, OnRestart};
use std::collections::VecDeque;

fn scope() -> broca_protocol::SessionScope {
    serde_json::from_value(
        json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
    )
    .unwrap()
}

/// A model step answering with `calls`, preceded by `extra` provider events.
fn tool_turn(
    calls: &[&str],
    input_tokens: u64,
    extra: Vec<NormalizedEvent>,
) -> Vec<NormalizedEvent> {
    let mut events = extra;
    events.extend([
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("m".into()),
                content: calls
                    .iter()
                    .map(|id| ContentBlock::ToolCall(call(id)))
                    .collect(),
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(input_tokens),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]);
    events
}

fn text_turn() -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("done".into()),
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
    ]
}

/// A rig whose approval hook files a card that is never answered, and whose stop request
/// fires once it is filed, so a resume-enabled run pauses with the call Prepared.
fn pausing_rig() -> (Rig, RunRequest) {
    let mut rig = Rig::new("pass", "none", false, true);
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("mutate"), "pass"),
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    request.on_restart = Some(OnRestart::Resume);
    request.scope = Some(scope());
    (rig, request)
}

async fn run(rig: &Rig, request: RunRequest, model: &crate::StubProvider) -> RunSummary {
    let (out, _rx) = mpsc::channel(1000);
    tokio::time::timeout(
        HANG_GUARD,
        run_loop_on_writer(
            request,
            &rig.writer,
            StartMode::FreshOrResume,
            model,
            &rig.tools,
            &crate::AllowAll,
            rig.memory_store(),
            None,
            rig.control.clone(),
            &out,
        ),
    )
    .await
    .expect("scripted run must end")
    .unwrap()
}

fn result_for<'a>(frames: &'a [broca_wal::FramedRecord], id: &str) -> Vec<&'a WalRecord> {
    frames
        .iter()
        .map(|frame| &frame.record)
        .filter(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == id))
        .collect()
}

/// A provider that repeats an id makes every later instance a duplicate, even when the first
/// instance's result is restored from the WAL on resume. The duplicate gets the synthetic
/// refusal the live batch gave it and is never gated or sent.
#[tokio::test]
async fn resumed_duplicate_of_a_restored_call_is_refused_not_dispatched() {
    let rig = Rig::new("pass", "approve", false, false);
    let restored = ToolResult {
        tool_call_id: ToolCallId("c".into()),
        output: ToolOutput::Text { text: "ran".into() },
        is_error: false,
    };
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: [(ToolCallId("c".into()), VecDeque::from([restored]))]
            .into_iter()
            .collect(),
        prepared: Vec::new(),
        ..Default::default()
    };
    let result = rig
        .batch(
            &crate::AllowAll,
            vec![call("c"), call("c")],
            Some(resume),
            false,
        )
        .await;
    assert!(
        rig.tools.sent.lock().unwrap().is_empty(),
        "the duplicate instance was dispatched on resume"
    );
    assert_eq!(result.results.len(), 2);
    assert_eq!(
        result.results[1].output,
        ToolOutput::error("duplicate tool_call_id from provider")
    );
    assert!(rig.questions.keys.lock().unwrap().is_empty());
    rig.finish().await;
}

/// A resume that finds the input budget already spent seals the episode. A call whose
/// approval card is still open gets the withdrawing terminal, so the card is closed rather
/// than left live for a call that will never run.
#[tokio::test]
async fn budget_exhausted_resume_withdraws_the_open_approval_card() {
    let (mut rig, mut request) = pausing_rig();
    let model = crate::StubProvider::new(vec![tool_turn(&["c"], 100, Vec::new())]);
    let paused = run(&rig, request.clone(), &model).await;
    assert_eq!(paused.paused, Some(broca_protocol::PauseReason::Restart));
    assert!(rig.frames().iter().any(|frame| matches!(
        &frame.record,
        WalRecord::AwaitingApproval { pending, .. }
            if pending.iter().any(|entry| entry.elicitation_id.is_some())
    )));

    rig.control.stop_requested = None;
    request.budget = Some(BudgetConfig {
        max_input_tokens: Some(10),
        ..BudgetConfig::default()
    });
    let sealed = run(&rig, request, &model).await;
    assert_eq!(sealed.reason, RunFinishReason::BudgetExhausted);
    let frames = rig.frames();
    let terminals = result_for(&frames, "c");
    assert_eq!(terminals.len(), 1, "one terminal for the Prepared call");
    assert!(
        matches!(
            terminals[0],
            WalRecord::ToolResult {
                withdraw_elicitation: Some(_),
                outcome: DispatchOutcome::Interrupted,
                ..
            }
        ),
        "the sealed call must withdraw its card: {:?}",
        terminals[0]
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}

fn args_cut_attempt() -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage {
            cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: ToolCallId("bad".into()),
                tool_name: "edit".into(),
                json_bytes: 2,
                whitespace_bytes: 4097,
                omitted_tool_calls: 0,
            },
        }),
        NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt: broca_protocol::ModelAttempt {
                content_progress: Default::default(),
                attempt: 0,
                request_body_len: 1,
                request_body_sha256: "aa".into(),
                http_status: Some(200),
                content_type: None,
                response_body_len: 0,
                response_body_sha256: "bb".into(),
                response_body_cap: None,
                outcome: broca_protocol::provider::AttemptOutcome::ToolArgsTrailingGarbage,
                ts_ms: 0,
                sent_at_ms: None,
                transport: None,
                websocket_request: None,
                websocket_close_code: None,
                input_transformations: None,
                input_transformations_unmeasured: false,
                credential_refresh: None,
                served_account_ref: None,
                account_observation: Default::default(),
            },
        }),
    ]
}

/// A step whose provider attempt was cut rejects only the cut call. Its well-formed sibling
/// was gated and Prepared before a restart; resuming that step keeps the sibling in the
/// approval gate, where the answer arrives and the call runs, instead of reporting it to the
/// model as a malformed call and leaving its card open.
#[tokio::test]
async fn args_cut_resume_keeps_a_prepared_sibling_in_the_approval_gate() {
    let (mut rig, request) = pausing_rig();
    let model = crate::StubProvider::new(vec![
        tool_turn(&["bad", "c"], 1, args_cut_attempt()),
        text_turn(),
    ]);
    let paused = run(&rig, request.clone(), &model).await;
    assert_eq!(paused.paused, Some(broca_protocol::PauseReason::Restart));
    let frames = rig.frames();
    assert!(frames.iter().any(|frame| matches!(
        &frame.record,
        WalRecord::ModelAttemptFinished { outcome, .. } if outcome.as_str() == "tool_args_trailing_garbage"
    )));
    assert!(frames.iter().any(|frame| matches!(
        &frame.record,
        WalRecord::ToolCallDeferred { call_id, .. } if call_id.0 == "c"
    )));

    rig.control.stop_requested = None;
    *rig.questions.answer.lock().unwrap() = "approve";
    let resumed = run(&rig, request, &model).await;
    assert_eq!(resumed.reason, RunFinishReason::Completed);
    let frames = rig.frames();
    let terminals = result_for(&frames, "c");
    assert_eq!(terminals.len(), 1, "one terminal for the Prepared sibling");
    assert!(
        !matches!(
            terminals[0],
            WalRecord::ToolResult { result, .. }
                if matches!(&result.output, ToolOutput::Error { message, .. } if message.starts_with("tool_args_trailing_garbage"))
        ),
        "the Prepared sibling was reported as a malformed call: {:?}",
        terminals[0]
    );
    assert_eq!(
        rig.tools.sent.lock().unwrap().len(),
        1,
        "the approved sibling runs exactly once"
    );
    assert_eq!(
        result_for(&frames, "bad").len(),
        1,
        "the cut call keeps one rejection"
    );
    rig.finish().await;
}
