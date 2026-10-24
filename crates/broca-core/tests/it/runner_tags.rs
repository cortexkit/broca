//! Runner tags through a real run under the computed rules (`runner-tags/v2`). No record stores
//! a number: the transcript's counted parts are numbered in the order a request renders them,
//! and only a run frozen with `render_tags` numbers and shows them. Replaying the WAL rebuilds
//! each request the run sent, with the same rendered numbers, byte for byte.

use std::collections::HashSet;
use std::sync::Arc;

use broca_core::{
    replay, run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore, RunControl, RunRequest,
    StubProvider, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::Tool,
    tags::render_tags_into,
    BindIdentity, DisplayEvent, FinishReason, MessageId, RunId, StepId, SubmissionId, ToolCallId,
    Usage,
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

fn step(content: Vec<ContentBlock>, reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::StreamStart { warnings: vec![] }),
        NormalizedEvent::Display(DisplayEvent::TextDelta {
            mid: None,
            part: Some(0),
            message_id: MessageId("m".into()),
            tool_call_id: None,
            delta: "x".into(),
        }),
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id: StepId(0),
            usage: Usage::default(),
            finish_reason: reason,
            service_tier_used: None,
            speed_used: None,
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: reason,
            retries_used: 0,
        }),
    ]
}

fn lookup() -> Tool {
    Tool {
        name: "lookup".into(),
        description: "look something up".into(),
        input_schema: json!({"type":"object"}),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

/// A session's frozen plan manifest, with the plan's `render_tags` set as given.
fn manifest(render_tags: bool) -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"step_transform_items":[],"render_tags":render_tags}).to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: Vec::new(),
        tools: vec![lookup()].into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: Vec::new(),
    }
}

fn request(name: &str) -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"m"},
        "input":[{"role":"user","content":[{"type":"text","text":"find the tags"}]}],
        "tools":[lookup()],
        "policy_allowed":["lookup"],
        "stop_when":[{"type":"step_count","n":4}],
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId(format!("run-{name}")),
        BindIdentity::new("/tmp/tags", "runner", name),
        SubmissionId(format!("send-{name}")),
    );
    request.submission_id = None;
    request.policy_allowed = HashSet::from(["lookup".to_owned()]);
    request
}

struct Run {
    calls: Vec<broca_protocol::CallOptions>,
    records: Vec<FramedRecord>,
}

async fn run(name: &str, render_tags: bool) -> Run {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-runner-tags-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let leases = FileLeaseStore::new(root.join("leases"));
    let wal = FileWalStore::new(root.join("wal"));
    let call = ToolCall {
        tool_call_id: ToolCallId("call-1".into()),
        tool_name: "lookup".into(),
        input: json!({"q": "tags"}),
        thought_signature: None,
    };
    let provider = StubProvider::new(vec![
        step(
            vec![
                ContentBlock::Text {
                    text: "looking".into(),
                },
                ContentBlock::ToolCall(call),
            ],
            FinishReason::ToolCalls,
        ),
        step(
            vec![ContentBlock::Text {
                text: "done".into(),
            }],
            FinishReason::Stop,
        ),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "two hits");
    let control = RunControl {
        manifest: Some(Arc::new(manifest(render_tags))),
        ..RunControl::default()
    };
    let request = request(name);
    let session = request.session.clone();
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        request, &provider, &plane, &leases, &AllowAll, &wal, None, control, &tx,
    )
    .await
    .expect("the run completes");
    drop(tx);
    drain.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let records = wal.read_records(&session).unwrap();
    let _ = std::fs::remove_dir_all(&root);
    Run {
        calls: provider.calls(),
        records,
    }
}

/// The payload of one part as a provider renders it: a text's text, a tool call's arguments as
/// compact JSON, and a text tool result's text. This is where a rendered tag shows. It is not
/// `broca_protocol::tags::part_text`, which gives `session.resolve`'s complete stored encoding
/// of every non-text part (ids, kind and flags included).
fn payload(block: &ContentBlock) -> String {
    match block {
        ContentBlock::ToolCall(call) => call.input.to_string(),
        ContentBlock::ToolResult(result) => match &result.output {
            ToolOutput::Text { text } => text.clone(),
            other => panic!("the scripted tool answers with text, not {other:?}"),
        },
        other => broca_protocol::tags::part_text(other),
    }
}

fn texts(messages: &[Message]) -> Vec<(Role, Vec<String>)> {
    messages
        .iter()
        .filter(|message| message.role != Role::System)
        .map(|message| (message.role, message.content.iter().map(payload).collect()))
        .collect()
}

fn wal_tags(records: &[FramedRecord]) -> Vec<u64> {
    records
        .iter()
        .flat_map(|frame| frame.record.runner_tags().to_vec())
        .flat_map(|tags| tags.parts)
        .collect()
}

/// The prompt a replay of the WAL up to (not including) the `nth` `StepStarted` rebuilds.
fn replayed_prompt(records: &[FramedRecord], nth: usize) -> Vec<Message> {
    let end = records
        .iter()
        .enumerate()
        .filter(|(_, frame)| matches!(frame.record, WalRecord::StepStarted { .. }))
        .nth(nth)
        .map(|(index, _)| index)
        .expect("the step started");
    let records: Vec<WalRecord> = records[..end]
        .iter()
        .map(|frame| frame.record.clone())
        .collect();
    let state = replay(&records).unwrap();
    let mut prompt = state.prompt;
    prompt.extend(state.pending_steers);
    prompt
}

/// The numbers the replayed transcript shows, part by part, in transcript order.
fn transcript_numbers(records: &[FramedRecord]) -> Vec<Vec<Option<u64>>> {
    let records: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    replay(&records)
        .unwrap()
        .prompt
        .iter()
        .map(|message| {
            message
                .part_tags()
                .expect("every part of a run under the computed rules has a row")
                .parts
                .clone()
        })
        .collect()
}

#[tokio::test]
async fn a_tag_rendering_run_numbers_counted_parts_in_rendered_order_and_replay_rebuilds_the_same_request(
) {
    let run = run("render-on", true).await;
    // Nothing is stored: the numbers are computed from the transcript.
    assert!(wal_tags(&run.records).is_empty());
    assert!(run.records.iter().any(|frame| matches!(
        &frame.record,
        WalRecord::RunStarted { config, .. }
            if config.render_tags && config.computed_tags.is_some()
    )));
    assert!(run.records.iter().any(|frame| frame
        .record
        .required_features()
        .contains(&broca_wal::framing::RUNNER_TAGS_V2_FEATURE)));
    // The prompt, the model's text (its call not counted), the result, the final answer.
    assert_eq!(
        transcript_numbers(&run.records),
        vec![
            vec![Some(1)],
            vec![Some(2), None],
            vec![Some(3)],
            vec![Some(4)],
        ]
    );

    assert_eq!(run.calls.len(), 2);
    assert_eq!(
        texts(&run.calls[0].prompt),
        vec![(Role::User, vec!["§1§ find the tags".to_owned()])]
    );
    // The model's own text shows its number on every family; the call never does.
    assert_eq!(
        texts(&run.calls[1].prompt),
        vec![
            (Role::User, vec!["§1§ find the tags".to_owned()]),
            (
                Role::Assistant,
                vec!["§2§ looking".to_owned(), "{\"q\":\"tags\"}".to_owned()]
            ),
            (Role::Tool, vec!["§3§ two hits".to_owned()]),
        ]
    );

    // A resume renders from the WAL: the same numbers, rendered the same way, give the same
    // request the live run sent, so the provider's cached prefix still matches.
    for (nth, sent) in run.calls.iter().enumerate() {
        let mut rebuilt = replayed_prompt(&run.records, nth);
        render_tags_into(&mut rebuilt, Default::default());
        assert_eq!(texts(&rebuilt), texts(&sent.prompt), "request {nth}");
        assert_eq!(
            broca_core::canonical_request_bytes(&broca_protocol::CallOptions::new(rebuilt)),
            broca_core::canonical_request_bytes(&broca_protocol::CallOptions::new(
                sent.prompt.clone()
            )),
            "request {nth}"
        );
    }
}

#[tokio::test]
async fn a_run_without_render_tags_numbers_nothing_and_sends_the_text_unchanged() {
    let run = run("render-off", false).await;
    assert!(wal_tags(&run.records).is_empty());
    assert!(run.records.iter().all(|frame| !matches!(
        &frame.record,
        WalRecord::RunStarted { config, .. } if config.render_tags
    )));
    // Every part keeps a row and a key, and none shows a number.
    assert_eq!(
        transcript_numbers(&run.records),
        vec![vec![None], vec![None, None], vec![None], vec![None]]
    );
    assert_eq!(
        texts(&run.calls[1].prompt),
        vec![
            (Role::User, vec!["find the tags".to_owned()]),
            (
                Role::Assistant,
                vec!["looking".to_owned(), "{\"q\":\"tags\"}".to_owned()]
            ),
            (Role::Tool, vec!["two hits".to_owned()]),
        ]
    );
}
