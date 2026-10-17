//! A run asked to stop finishes the work it has in flight and starts nothing new.
//!
//! The module trips `RunControl::stop_requested` when it begins to shut down. From then on the
//! loop starts no model step, sends no tool call that has not left yet, and renders no steered
//! message; it ends `Interrupted` at the first point where it would have started something. A
//! model response already streaming is read to its end, and a tool call already sent runs to its
//! result. A stop is never a caller's cancel, so no path here may write `Cancelled`, and no call
//! may be left `outcome_unknown`.
//!
//! Each test trips the stop from inside the work it means to interrupt (the provider's stream or
//! the tool plane's dispatch), so where the stop lands is fixed by construction, not by timing.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::resume::NOT_DISPATCHED_TEXT;
use broca_core::{
    run_loop_with_control, AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore,
    Provider, RunControl, RunRequest, RunSummary, SteerInbox, ToolManifest, ToolPlane,
    ToolPlaneError, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, CallOptions, FinishReason, GenerationConfig, MessageId, RunId, StepId,
    ToolCallId, ToolResult, Usage,
};
use broca_wal::{DispatchOutcome, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn assistant_step(content: Vec<ContentBlock>, finish: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::StreamStart { warnings: vec![] }),
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
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

fn edits_step(ids: &[&str]) -> Vec<NormalizedEvent> {
    assistant_step(
        ids.iter()
            .map(|id| {
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId((*id).into()),
                    tool_name: "edit".into(),
                    input: json!({ "f": id }),
                })
            })
            .collect(),
        FinishReason::ToolCalls,
    )
}

fn text_step(text: &str) -> Vec<NormalizedEvent> {
    assistant_step(
        vec![ContentBlock::Text { text: text.into() }],
        FinishReason::Stop,
    )
}

/// Where in a provider's stream the stop is tripped.
#[derive(Clone, Copy)]
enum StreamStop {
    Never,
    /// During the first step, after its stream has started and before the assistant message.
    MidFirstStep,
}

/// A scripted provider that counts its steps and can trip the stop in the middle of a stream.
/// With a steer inbox, it steers one message into the run during the first step's stream, the
/// way a caller's `session.send` with `delivery: steer` reaches a running turn.
struct StoppingProvider {
    scripts: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    steps: AtomicUsize,
    stop: CancellationToken,
    at: StreamStop,
    steer: Option<Arc<SteerInbox>>,
}

impl StoppingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>, stop: &CancellationToken, at: StreamStop) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            steps: AtomicUsize::new(0),
            stop: stop.clone(),
            at,
            steer: None,
        }
    }

    fn steering(mut self, inbox: &Arc<SteerInbox>) -> Self {
        self.steer = Some(Arc::clone(inbox));
        self
    }

    fn steps(&self) -> usize {
        self.steps.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Provider for StoppingProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let step = self.steps.fetch_add(1, Ordering::SeqCst);
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let stop =
            (step == 0 && matches!(self.at, StreamStop::MidFirstStep)).then(|| self.stop.clone());
        let steer = if step == 0 { self.steer.clone() } else { None };
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for (index, event) in script.into_iter().enumerate() {
                if index == 1 {
                    if let Some(inbox) = &steer {
                        assert!(inbox.deliver(
                            vec![Message::new(
                                Role::User,
                                vec![ContentBlock::Text {
                                    text: "also this".into(),
                                }],
                            )],
                            || {},
                        ));
                    }
                    if let Some(stop) = &stop {
                        stop.cancel();
                        // Leave the loop parked on an unfinished stream for a moment, so the
                        // rest of this step is visibly read after the stop arrived.
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

/// A tool plane that records every call it is sent and, when asked, trips the stop while the
/// first call it receives is running. The trip happens before the call yields, so in a parallel
/// batch the stop is already requested when the next call of the batch would be sent.
struct StoppingPlane {
    stop: CancellationToken,
    trip_on_dispatch: bool,
    dispatched: Mutex<Vec<String>>,
    batches_started: AtomicUsize,
}

impl StoppingPlane {
    fn new(stop: &CancellationToken, trip_on_dispatch: bool) -> Self {
        Self {
            stop: stop.clone(),
            trip_on_dispatch,
            dispatched: Mutex::new(Vec::new()),
            batches_started: AtomicUsize::new(0),
        }
    }

    fn dispatched(&self) -> Vec<String> {
        self.dispatched.lock().unwrap().clone()
    }
}

#[async_trait]
impl ToolPlane for StoppingPlane {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.batches_started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.dispatched
            .lock()
            .unwrap()
            .push(call.tool_call_id.0.clone());
        if self.trip_on_dispatch {
            self.stop.cancel();
        }
        // The call is running when the stop arrives; let the rest of the batch be polled before
        // it returns.
        tokio::task::yield_now().await;
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: format!("edited-{}", call.tool_call_id.0),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}

fn request(name: &str, mode: ExecutionMode) -> RunRequest {
    let tools = vec![Tool {
        name: "edit".into(),
        description: "edit a file".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: mode,
    }];
    let policy_allowed: HashSet<String> = ["edit".to_string()].into_iter().collect();
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
        run_id: RunId(format!("run-{name}")),
        session: BindIdentity::new("/tmp/graceful-stop", "test", name),
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "edit".into(),
            }],
        )],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 10 }],
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

async fn drive<P: Provider, T: ToolPlane>(
    request: RunRequest,
    provider: &P,
    plane: &T,
    control: RunControl,
) -> (RunSummary, Vec<WalRecord>) {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-graceful-stop-{}-{}",
        std::process::id(),
        request.run_id.0
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let session = request.session.clone();
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        request, provider, plane, &lease, &AllowAll, &wal, None, control, &tx,
    )
    .await
    .expect("the run ends with a summary");
    drop(tx);
    drain.await.unwrap();
    let records = wal
        .read_records(&session)
        .expect("read the run's WAL")
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    let _ = std::fs::remove_dir_all(&dir);
    (summary, records)
}

fn control(stop: &CancellationToken) -> RunControl {
    RunControl {
        cancel: Some(CancellationToken::new()),
        stop_requested: Some(stop.clone()),
        ..Default::default()
    }
}

fn steps_started(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|r| matches!(r, WalRecord::StepStarted { .. }))
        .count()
}

fn intents(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolDispatchIntent { tool_call_id, .. } => Some(tool_call_id.0.clone()),
            _ => None,
        })
        .collect()
}

/// Every tool result: its call id, outcome, and text.
fn tool_results(records: &[WalRecord]) -> Vec<(String, DispatchOutcome, String)> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolResult {
                result, outcome, ..
            } => {
                let text = match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Error { message } => message.clone(),
                    other => format!("{other:?}"),
                };
                Some((result.tool_call_id.0.clone(), *outcome, text))
            }
            _ => None,
        })
        .collect()
}

fn terminals(records: &[WalRecord]) -> Vec<RunFinishReason> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect()
}

/// What every stopped run must show: exactly one terminal, `Interrupted` and last; never
/// `Cancelled`; no call left `outcome_unknown`; and the returned summary agrees.
fn assert_stopped_cleanly(summary: &RunSummary, records: &[WalRecord]) {
    assert_eq!(
        terminals(records),
        vec![RunFinishReason::Interrupted],
        "a stopped run has one Interrupted terminal and never a Cancelled one"
    );
    assert!(
        matches!(
            records.last(),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            })
        ),
        "the terminal is the last record"
    );
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert!(
        tool_results(records)
            .iter()
            .all(|(_, outcome, _)| *outcome != DispatchOutcome::Unknown),
        "a stop leaves no call outcome_unknown: {:?}",
        tool_results(records)
    );
}

/// The stop arrives while the step's only tool call is running. The call finishes and its
/// result is recorded `completed`; the run then starts no next step and ends Interrupted.
#[tokio::test]
async fn a_stop_during_a_tool_call_lets_it_finish_and_starts_no_next_step() {
    let stop = CancellationToken::new();
    let provider = StoppingProvider::new(
        vec![edits_step(&["t1"]), text_step("done")],
        &stop,
        StreamStop::Never,
    );
    let plane = StoppingPlane::new(&stop, true);
    let (summary, records) = drive(
        request("stop-mid-tool", ExecutionMode::Parallel),
        &provider,
        &plane,
        control(&stop),
    )
    .await;

    assert_eq!(plane.dispatched(), vec!["t1"]);
    assert_eq!(
        tool_results(&records),
        vec![(
            "t1".to_string(),
            DispatchOutcome::Completed,
            "edited-t1".to_string()
        )]
    );
    assert_eq!(steps_started(&records), 1, "no step starts after the stop");
    assert_eq!(provider.steps(), 1, "no model call starts after the stop");
    assert!(records
        .iter()
        .any(|r| matches!(r, WalRecord::TurnFinished { .. })));
    assert_stopped_cleanly(&summary, &records);
}

/// The stop arrives while the model's response is streaming. The stream is read to its end and
/// the step is recorded; its tool calls are never sent, each closed as not dispatched, and the
/// run ends Interrupted.
#[tokio::test]
async fn a_stop_during_a_model_stream_reads_it_to_the_end_and_sends_none_of_its_calls() {
    let stop = CancellationToken::new();
    let provider = StoppingProvider::new(
        vec![edits_step(&["t1", "t2"]), text_step("done")],
        &stop,
        StreamStop::MidFirstStep,
    );
    let plane = StoppingPlane::new(&stop, false);
    let (summary, records) = drive(
        request("stop-mid-stream", ExecutionMode::Parallel),
        &provider,
        &plane,
        control(&stop),
    )
    .await;

    let finished: Vec<usize> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => Some(assistant_message.tool_calls().count()),
            _ => None,
        })
        .collect();
    assert_eq!(
        finished,
        vec![2],
        "the step in flight is read to its end and recorded with both calls"
    );
    assert!(plane.dispatched().is_empty(), "no call of the step is sent");
    assert_eq!(
        plane.batches_started.load(Ordering::SeqCst),
        0,
        "the tool plane is never told a batch started"
    );
    assert!(
        intents(&records).is_empty(),
        "no dispatch intent is written for a call the stop keeps back"
    );
    assert_eq!(
        tool_results(&records),
        vec![
            (
                "t1".to_string(),
                DispatchOutcome::Interrupted,
                NOT_DISPATCHED_TEXT.to_string()
            ),
            (
                "t2".to_string(),
                DispatchOutcome::Interrupted,
                NOT_DISPATCHED_TEXT.to_string()
            ),
        ]
    );
    assert!(records
        .iter()
        .any(|r| matches!(r, WalRecord::ToolBatchFinished { .. })));
    assert_eq!(steps_started(&records), 1);
    assert_eq!(provider.steps(), 1);
    assert_stopped_cleanly(&summary, &records);
}

/// A parallel batch whose first call trips the stop while it runs: that call finishes, and no
/// other call of the batch is sent. Each unsent call is closed as not dispatched.
#[tokio::test]
async fn a_stop_after_the_first_parallel_dispatch_sends_no_other_call_of_the_batch() {
    stop_inside_a_batch("stop-parallel", ExecutionMode::Parallel).await;
}

/// The same for a batch the manifest makes sequential.
#[tokio::test]
async fn a_stop_after_the_first_sequential_dispatch_sends_no_later_call_of_the_batch() {
    stop_inside_a_batch("stop-sequential", ExecutionMode::Sequential).await;
}

async fn stop_inside_a_batch(name: &str, mode: ExecutionMode) {
    let stop = CancellationToken::new();
    let provider = StoppingProvider::new(
        vec![edits_step(&["t1", "t2", "t3"]), text_step("done")],
        &stop,
        StreamStop::Never,
    );
    let plane = StoppingPlane::new(&stop, true);
    let (summary, records) = drive(request(name, mode), &provider, &plane, control(&stop)).await;

    assert_eq!(
        plane.dispatched(),
        vec!["t1"],
        "only the call sent before the stop reaches the tool plane"
    );
    // The batch's intents were all durable before the first call was sent; the two calls kept
    // back still get a definite terminal, so neither is ever sent later.
    assert_eq!(intents(&records), vec!["t1", "t2", "t3"]);
    assert_eq!(
        tool_results(&records),
        vec![
            (
                "t1".to_string(),
                DispatchOutcome::Completed,
                "edited-t1".to_string()
            ),
            (
                "t2".to_string(),
                DispatchOutcome::Interrupted,
                NOT_DISPATCHED_TEXT.to_string()
            ),
            (
                "t3".to_string(),
                DispatchOutcome::Interrupted,
                NOT_DISPATCHED_TEXT.to_string()
            ),
        ]
    );
    assert_eq!(steps_started(&records), 1);
    assert_eq!(provider.steps(), 1);
    assert_stopped_cleanly(&summary, &records);
}

/// A final answer arrives while a steered message waits for the next step, and the stop was
/// requested during that answer's stream. The steer would normally extend the turn by one step;
/// under the stop it does not, and the run ends Interrupted instead of rendering it.
#[tokio::test]
async fn a_stop_keeps_a_waiting_steer_from_starting_a_new_step() {
    let stop = CancellationToken::new();
    let inbox = SteerInbox::new();
    let provider = StoppingProvider::new(
        vec![text_step("first answer"), text_step("steered answer")],
        &stop,
        StreamStop::MidFirstStep,
    )
    .steering(&inbox);
    let plane = StoppingPlane::new(&stop, false);
    let mut control = control(&stop);
    control.steer = Some(Arc::clone(&inbox));
    let (summary, records) = drive(
        request("stop-steer", ExecutionMode::Parallel),
        &provider,
        &plane,
        control,
    )
    .await;

    assert_eq!(steps_started(&records), 1, "the steer starts no step");
    assert_eq!(provider.steps(), 1);
    assert!(inbox.is_closed(), "the run takes no further steer");
    assert_stopped_cleanly(&summary, &records);
}

/// Without a stop the same steered run does extend by one step, so the test above is measuring
/// the stop and not a run that would have ended anyway.
#[tokio::test]
async fn without_a_stop_a_waiting_steer_extends_the_run() {
    let stop = CancellationToken::new();
    let inbox = SteerInbox::new();
    let provider = StoppingProvider::new(
        vec![text_step("first answer"), text_step("steered answer")],
        &stop,
        StreamStop::Never,
    )
    .steering(&inbox);
    let plane = StoppingPlane::new(&stop, false);
    let mut control = control(&stop);
    control.steer = Some(Arc::clone(&inbox));
    let (summary, records) = drive(
        request("no-stop-steer", ExecutionMode::Parallel),
        &provider,
        &plane,
        control,
    )
    .await;

    assert_eq!(steps_started(&records), 2);
    assert_eq!(summary.reason, RunFinishReason::Completed);
}
