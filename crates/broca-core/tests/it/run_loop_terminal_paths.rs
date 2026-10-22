//! A run's end and its tool dispatch agree across the loop's paths: a satisfied stop
//! condition on a tool-free answer keeps a `final_text` structured result, and the caller's
//! immediate cancellation keeps the plain dispatcher from sending calls it has not sent yet.

use std::sync::atomic::{AtomicUsize, Ordering};

use broca_core::{
    run_loop_with_control, AllowAll, ApprovalPolicy, BatchCtx, DispatchResult, FileLeaseStore,
    FileWalStore, OutputEvent, Provider, RunControl, RunRequest, StubProvider, ToolManifest,
    ToolPlane, ToolPlaneError, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    ApprovalResponse, BindIdentity, FinishReason, MessageId, RunId, StepId, SubmissionId,
    ToolCallId, ToolResult, Usage,
};
use broca_wal::{DispatchOutcome, FramedRecord, WalRecord};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn request(name: &str, extra: Value) -> RunRequest {
    let mut submission = json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "go"}]}],
    });
    for (key, value) in extra.as_object().unwrap() {
        submission[key] = value.clone();
    }
    let mut request = RunRequest::from_submission(
        serde_json::from_value(submission).unwrap(),
        RunId(format!("run-{name}")),
        BindIdentity::new("/tmp/terminal-paths", "test", name),
        SubmissionId(format!("send-{name}")),
    );
    request.submission_id = None;
    request.policy_allowed = request.tools.iter().map(|tool| tool.name.clone()).collect();
    request
}

fn step(content: Vec<ContentBlock>, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason,
            retries_used: 0,
        }),
    ]
}

fn call(id: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: "lookup".into(),
        input: json!({}),
    })
}

/// Run `request` to its end on a fresh WAL and return the summary's reason and the WAL.
async fn drive<P: Provider, T: ToolPlane, A: ApprovalPolicy>(
    request: RunRequest,
    provider: &P,
    plane: &T,
    approval: &A,
    control: RunControl,
) -> (RunFinishReason, Vec<FramedRecord>) {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-terminal-paths-{}-{}",
        std::process::id(),
        request.run_id.0
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let wal = FileWalStore::new(dir.join("wal"));
    let leases = FileLeaseStore::new(dir.join("leases"));
    let session = request.session.clone();
    let (out, mut events) = mpsc::channel::<OutputEvent>(4096);
    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        request, provider, plane, &leases, approval, &wal, None, control, &out,
    )
    .await
    .expect("the run ends with a terminal");
    drop(out);
    drain.await.unwrap();
    let records = wal.read_records(&session).unwrap();
    let _ = std::fs::remove_dir_all(dir);
    (summary.reason, records)
}

/// A stub model whose frozen render config admits a `final_text` structured-output run.
struct FinalTextModel(StubProvider);

#[async_trait::async_trait]
impl Provider for FinalTextModel {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.0.step(step_id, opts).await
    }
    fn render_config_blob(&self) -> Value {
        json!({"render_config": "stateless", "structured_output": {
            "mode": "final_text", "name": "result",
            "schema_jcs": r#"{"properties":{"answer":{"type":"string"}},"type":"object"}"#,
            "enforcement": "best_effort", "delivery": "unconstrained"}})
    }
}

/// `loop_finished` recognizes a tool-free answer; a run it ends is `completed` with the same
/// final text as a run that simply answered. Default controls emit live control payloads,
/// which is the path where the stop is checked before the tool-free branch.
#[tokio::test]
async fn loop_finished_stop_keeps_the_final_text_structured_result() {
    let model = FinalTextModel(StubProvider::new(vec![step(
        vec![ContentBlock::Text {
            text: r#"{"answer":"x"}"#.into(),
        }],
        FinishReason::Stop,
    )]));
    let (reason, records) = drive(
        request(
            "loop-finished-final-text",
            json!({"stop_when": [{"type": "loop_finished"}]}),
        ),
        &model,
        &broca_core::StubToolPlane::new(),
        &AllowAll,
        RunControl::default(),
    )
    .await;
    assert_eq!(reason, RunFinishReason::Completed);
    let result = records
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::StructuredOutputResult { result, .. } => Some(result),
            _ => None,
        })
        .expect("a completed structured-output run records its result");
    assert_eq!(
        result.validation,
        broca_protocol::structured_output::validation::PASSED,
        "the stopped run's final text is its result: {result:?}"
    );
    assert_eq!(result.value, Some(json!({"answer": "x"})));
}

/// A tool plane that counts sends and trips the run's cancel token while serving the first.
struct CancellingPlane {
    cancel: CancellationToken,
    sent: AtomicUsize,
}

#[async_trait::async_trait]
impl ToolPlane for CancellingPlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.cancel.cancel();
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text { text: "ok".into() },
            is_error: false,
        })
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}

/// An approval policy that allows every call and trips the run's cancel token while deciding,
/// so the cancellation lands after the model step and before any call is sent.
struct CancellingApproval(CancellationToken);

#[async_trait::async_trait]
impl ApprovalPolicy for CancellingApproval {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        self.0.cancel();
        ApprovalResponse::Allow {
            tool_call_id: call.tool_call_id.clone(),
        }
    }
}

fn the_never_sent_result(records: &[FramedRecord], id: &str) -> bool {
    records.iter().any(|frame| {
        matches!(&frame.record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Interrupted, .. }
            if result.tool_call_id.0 == id)
    })
}

fn tools(mode: &str) -> Value {
    json!({"tools": [{"name": "lookup", "description": "lookup",
        "input_schema": {"type": "object"}, "execution_mode": mode}]})
}

/// A caller cancel while a sequential call runs sends no later call of the batch: the unsent
/// call gets the definite not-dispatched terminal and the run ends `cancelled`.
#[tokio::test]
async fn immediate_cancel_stops_later_sequential_calls() {
    let cancel = CancellationToken::new();
    let plane = CancellingPlane {
        cancel: cancel.clone(),
        sent: AtomicUsize::new(0),
    };
    let model = StubProvider::new(vec![step(
        vec![call("c1"), call("c2")],
        FinishReason::ToolCalls,
    )]);
    let (reason, records) = drive(
        request("cancel-sequential", tools("sequential")),
        &model,
        &plane,
        &AllowAll,
        RunControl {
            cancel: Some(cancel),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        plane.sent.load(Ordering::SeqCst),
        1,
        "c2 was sent after the cancel"
    );
    assert_eq!(reason, RunFinishReason::Cancelled);
    assert!(the_never_sent_result(&records, "c2"));
}

/// A caller cancel that lands after the model step but before the batch sends anything leaves
/// every call of a parallel batch unsent.
#[tokio::test]
async fn immediate_cancel_before_send_sends_no_parallel_call() {
    let cancel = CancellationToken::new();
    let plane = CancellingPlane {
        cancel: cancel.clone(),
        sent: AtomicUsize::new(0),
    };
    let model = StubProvider::new(vec![step(
        vec![call("p1"), call("p2")],
        FinishReason::ToolCalls,
    )]);
    let (reason, records) = drive(
        request("cancel-parallel", tools("parallel")),
        &model,
        &plane,
        &CancellingApproval(cancel.clone()),
        RunControl {
            cancel: Some(cancel),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        plane.sent.load(Ordering::SeqCst),
        0,
        "a call was sent after the cancel"
    );
    assert_eq!(reason, RunFinishReason::Cancelled);
    assert!(the_never_sent_result(&records, "p1"));
    assert!(the_never_sent_result(&records, "p2"));
    // Nothing was in flight, so nothing is reported as possibly applied.
    assert!(!records.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Unknown,
            ..
        }
    )));
}
