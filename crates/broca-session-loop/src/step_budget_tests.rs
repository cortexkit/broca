//! The memory a model step allocates, measured with the counting allocator on a synthetic
//! session shaped like production ones (`broca_session_store::synthetic`), at two window sizes.
//!
//! The budget is the one `.cortexkit/internal-docs/designs/session-storage/27-memory-shape.md`
//! sets: a step allocates at most `2 x request bytes` plus a constant per new message, and the
//! count of its allocations doesn't grow with the window. Each limit below is today's measure
//! plus 10%, with today's measure and the design's target beside it. A change that lowers a
//! step's cost lowers its limit; none may raise one.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::request::CallOptions;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, StepId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::session::Session;
use broca_session_store::synthetic::{window_bytes, write, Shape};
use broca_test_support::alloc::{installed, Counting, Counts, Group};
use tokio::sync::mpsc;

use crate::parts::MemoryParts;
use crate::run::Seams;
use crate::writer::Writer;

/// The two window sizes a step is measured at.
const SMALL: usize = 1_000;
const LARGE: usize = 2_000;

// The provider here renders nothing, so these measure the loop's own share of a step: a live
// provider's rendering of the request adds its own on top.
/// Bytes a step allocates per byte of its request, at the smaller window. Today: 3.60. Target:
/// 2, plus a constant per message the step adds.
const ALLOCATED_PER_REQUEST: f64 = 3.96;
/// Bytes a step allocates beyond the smaller window's step, per byte its request grew. Today:
/// 3.58. Target: 2.
const ALLOCATED_PER_REQUEST_BYTE: f64 = 3.94;
/// Allocations a step makes beyond the smaller window's step, per window message added. Today:
/// 32.5. Target: 0: the count doesn't grow with the window.
const ALLOCATIONS_PER_WINDOW_MESSAGE: f64 = 35.8;

fn bind() -> BindIdentity {
    BindIdentity::new("/synthetic", "budget", "s1")
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "budget".into(),
        session: "s1".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32]).creating(Cipher::XChaCha20Poly1305)
}

fn read_step(call: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{call}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(call.into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({"path": format!("src/{call}.rs")}),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn answer_step() -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m-answer".into()),
                content: vec![ContentBlock::Text {
                    text: "Done.".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(10),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A scripted provider that notes, at each call, what the counting group had done so far and
/// the call's options. It copies nothing of the request, so it adds no allocation that grows
/// with the window.
struct Marking {
    scripts: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    counting: Counting,
    marks: Mutex<Vec<(Counts, Arc<CallOptions>)>>,
}

#[async_trait]
impl broca_core::provider::Provider for Marking {
    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        _runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let counts = self.counting.read();
        self.marks.lock().unwrap().push((counts, Arc::clone(&opts)));
        self.step(step_id, &opts).await
    }

    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(8);
        for event in script {
            tx.try_send(event).unwrap();
        }
        rx
    }
}

/// One step as measured: bytes and allocations from one model call to the next, and the
/// request's bytes as compact JSON.
#[derive(Debug, Clone, Copy)]
struct Step {
    allocated: u64,
    allocations: u64,
    request: usize,
    window: usize,
}

/// Runs four tool steps and an answer on a synthetic session of `messages` window messages and
/// measures its third step, from the third model call to the fourth: a step in the middle of a
/// run, past the run's start.
fn a_step_at(messages: usize) -> Step {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let addr = SessionAddr("5e55105e55105e55".into());
    futures::executor::block_on(async {
        let mut session = Session::create(backend, addr.clone(), triple(), &deployment(), false)
            .await
            .unwrap();
        write(&mut session, &Shape::production(messages))
            .await
            .unwrap();
    });
    let group = Group::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_start(group.joiner())
        .build()
        .unwrap();
    let mut scripts: VecDeque<_> = (0..4).map(|n| read_step(&format!("c{n}"))).collect();
    scripts.push_back(answer_step());
    let provider = Marking {
        scripts: Mutex::new(scripts),
        counting: group.start(),
        marks: Mutex::new(Vec::with_capacity(8)),
    };
    let window = runtime.block_on(async {
        let mut session = Session::load(backend, addr, &triple(), &deployment())
            .await
            .unwrap();
        let window = window_bytes(&session.conversation);
        let mut parts = MemoryParts::default();
        let request: SubmissionRequest = serde_json::from_value(serde_json::json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [{"role": "user", "content": [{"type": "text", "text": "one more task"}]}],
            "stop_when": [{"type": "step_count", "n": 50}],
        }))
        .unwrap();
        let mut request = broca_core::RunRequest::from_submission(
            request,
            RunId("measured".into()),
            bind(),
            broca_protocol::SubmissionId(String::new()),
        );
        let control = broca_core::RunControl {
            cancel: Some(Default::default()),
            ..Default::default()
        };
        let now = || 1_800_000_000_000;
        let admission = crate::admit::admission(
            &mut request,
            &control,
            &provider,
            Writer::operations(&session),
            Some(now()),
        )
        .unwrap();
        let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
        let seams = Seams {
            provider: &provider,
            tools: &tools,
            bind: bind(),
            out: None,
            cancel: Default::default(),
            stop: Default::default(),
            held: None,
            approval: &broca_core::capability::AllowAll,
            hook_plane: None,
            now_ms: &now,
            session_address: None,
            compaction_plane: None,
            compaction_session: None,
            compaction_changes: false,
            keep_warm: None,
            account_reports: None,
            max_same_account_wait: Default::default(),
            auth_selection: None,
            step_progress: None,
            model_call_ready: None,
            pause_for_auth_at_start: None,
        };
        crate::run::run(&mut session, &mut parts, admission, &seams)
            .await
            .unwrap();
        window
    });
    let marks = provider.marks.lock().unwrap();
    assert_eq!(marks.len(), 5, "four tool steps and an answer");
    let ((from, _), (to, request)) = (&marks[2], &marks[3]);
    Step {
        allocated: to.allocated - from.allocated,
        allocations: to.allocations - from.allocations,
        request: serde_json::to_vec(&**request).unwrap().len(),
        window,
    }
}

/// A step's allocations grow with its window no more than its request's bytes explain, and the
/// number of its allocations no more than its budget.
#[test]
fn a_step_allocates_no_more_than_its_budget_as_the_window_grows() {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let small = a_step_at(SMALL);
    let large = a_step_at(LARGE);
    let per_request = small.allocated as f64 / small.request as f64;
    let per_request_byte = (large.allocated as f64 - small.allocated as f64)
        / (large.request as f64 - small.request as f64);
    let per_message =
        (large.allocations as f64 - small.allocations as f64) / (LARGE - SMALL) as f64;
    let table = format!(
        "\n  {SMALL} messages: W {} bytes, request {} bytes, a step allocated {} bytes ({:.1} x its \
         request) in {} allocations\n  {LARGE} messages: W {} bytes, request {} bytes, a step \
         allocated {} bytes ({:.1} x its request) in {} allocations\n  growth: {per_request_byte:.2} \
         bytes per request byte (limit {ALLOCATED_PER_REQUEST_BYTE:.2}), {per_message:.2} \
         allocations per window message (limit {ALLOCATIONS_PER_WINDOW_MESSAGE:.2})",
        small.window,
        small.request,
        small.allocated,
        small.allocated as f64 / small.request as f64,
        small.allocations,
        large.window,
        large.request,
        large.allocated,
        large.allocated as f64 / large.request as f64,
        large.allocations,
    );
    eprintln!("{table}");
    assert!(
        per_request <= ALLOCATED_PER_REQUEST,
        "a step allocated {per_request:.2} bytes per byte of its request, over the limit of \
         {ALLOCATED_PER_REQUEST:.2}{table}"
    );
    assert!(
        per_request_byte <= ALLOCATED_PER_REQUEST_BYTE,
        "a step's allocated bytes grew with its window past its budget{table}"
    );
    assert!(
        per_message <= ALLOCATIONS_PER_WINDOW_MESSAGE,
        "a step's allocations grew with its window past its budget{table}"
    );
}
