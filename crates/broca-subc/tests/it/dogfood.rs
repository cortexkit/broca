//! End-to-end wire test: the loop drives a real tool call through the REAL subc
//! daemon and the fake-aft-stub tool provider — runner → SubcToolPlane → subc →
//! AFT-stub → back. The in-process stub tests (broca-core) prove the LOOP; this
//! proves the WIRE.
//!
//! Brings up the real `subc-core` daemon + `fake-aft-stub` (built from the
//! subconscious checkout) in a per-test temp rig. Binary paths come from
//! SUBC_CORE_BIN / FAKE_AFT_STUB_BIN, else the sibling checkout's target/debug.
//! If the binaries aren't present the test SKIPS with a clear message (it cannot
//! run without them) rather than silently passing.

use crate::common;

use broca_core::{
    run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest, StubProvider,
};
use broca_protocol::{
    caller::ControlUnit,
    content::{AssistantMessage, ContentBlock, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, Tool, ToolChoice},
    BindIdentity, DisplayEvent, FinishReason, GenerationConfig, MessageId, RunId, ToolCallId,
    Usage,
};
use broca_subc::SubcToolPlane;
use serde_json::json;
use tokio::sync::mpsc;

use common::{wait_for_aft, Rig};

// ───────────────────────── model script ─────────────────────────

fn tool_step(calls: &[(&str, &str, serde_json::Value)]) -> Vec<NormalizedEvent> {
    let content = calls
        .iter()
        .map(|(id, name, input)| {
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId((*id).into()),
                tool_name: (*name).into(),
                input: input.clone(),
            })
        })
        .collect();
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_step(text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Display(DisplayEvent::TextDelta {
            message_id: MessageId("m2".into()),
            tool_call_id: None,
            delta: text.into(),
        }),
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m2".into()),
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

fn request(session: &str, project_root: &std::path::Path, tools: Vec<Tool>) -> RunRequest {
    let policy_allowed = tools.iter().map(|t| t.name.clone()).collect();
    RunRequest {
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
        run_id: RunId(format!("dogfood-{session}")),
        session: BindIdentity::new(project_root, "dogfood", session),
        model: ModelHandle::new("aft-stub", "stub-model"),
        input: vec![],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

fn tool(name: &str, mode: ExecutionMode) -> Tool {
    Tool {
        name: name.into(),
        description: "t".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: mode,
    }
}

async fn collect_results(events: Vec<OutputEvent>) -> Vec<ToolResultText> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolResult { result }) => Some(ToolResultText {
                is_error: result.is_error,
                text: match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Error { message } => message.clone(),
                    _ => String::new(),
                },
            }),
            _ => None,
        })
        .collect()
}

struct ToolResultText {
    is_error: bool,
    text: String,
}

// ───────────────────────── the exit-gate tests ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_dispatches_tool_call_through_real_subc() {
    let Some(rig) = Rig::start(
        "main",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "get_time"),
            ("FAKE_AFT_TOOLCALL_RESULT", "MADRID-12:00"),
        ],
    )
    .await
    else {
        return; // skipped (binaries absent)
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "get_time").await;

    let lease_dir = rig.dir.join("leases");
    let store = FileLeaseStore::new(&lease_dir);
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "get_time", json!({"timezone": "Europe/Madrid"}))]),
        text_step("It is noon in Madrid."),
    ]);

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(
        request(
            "s-main",
            &rig.dir,
            vec![tool("get_time", ExecutionMode::Parallel)],
        ),
        &provider,
        &plane,
        &store,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("run ok");
    drop(tx);
    let events = collector.await.unwrap();

    assert_eq!(summary.steps, 2, "tool step + text reply");
    let results = collect_results(events).await;
    assert_eq!(results.len(), 1, "exactly one tool result");
    assert!(
        !results[0].is_error,
        "real stub returned a non-error result; got is_error with text: {:?}",
        results[0].text
    );
    assert_eq!(
        results[0].text, "MADRID-12:00",
        "the deterministic fake-aft result flowed loop->subc->stub->loop"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_provider_serializes_batch_without_over_window_error() {
    // FAKE_AFT_CONCURRENCY=serial → subc per-route window = 1. The loop fires a
    // 2-call batch concurrently; the plane's per-route semaphore (sized to the
    // provider's declared concurrency) must SERIALIZE rather than over-window
    // ERROR. Both calls must come back as successful results.
    let Some(rig) = Rig::start(
        "serial",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "echo"),
            ("FAKE_AFT_CONCURRENCY", "serial"),
            ("FAKE_AFT_TOOLCALL_RESULT", "OK"),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("call_a", "echo", json!({"v": "a"})),
            ("call_b", "echo", json!({"v": "b"})),
        ]),
        text_step("done"),
    ]);

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(
        request(
            "s-serial",
            &rig.dir,
            vec![tool("echo", ExecutionMode::Parallel)],
        ),
        &provider,
        &plane,
        &store,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("run ok");
    drop(tx);
    let events = collector.await.unwrap();

    assert_eq!(summary.steps, 2);
    let results = collect_results(events).await;
    assert_eq!(results.len(), 2, "both calls produced results");
    assert!(
        results.iter().all(|r| !r.is_error && r.text == "OK"),
        "Serial provider serialized — no over-window error: {:?}",
        results
            .iter()
            .map(|r| (r.is_error, &r.text))
            .collect::<Vec<_>>()
    );
}

// Keep a direct ref to subc_protocol so the manifest match above resolves.
use subc_protocol as _;
