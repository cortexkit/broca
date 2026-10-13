//! A scripted scenario, and the provider and tool plane the harness runs it with.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
use broca_core::{
    provider::Provider, run_loop_with_control, AllowAll, FileLeaseStore, OutputEvent, RunControl,
    RunRequest, StubToolPlane, ToolManifest, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{ContentBlock, Message, Role, ToolCall},
    provider::NormalizedEvent,
    request::CallOptions,
    StepId, ToolCallId,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// How the scripted provider renders a request into the bytes it records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Render {
    /// The shared canonical request bytes.
    Canonical,
    /// The canonical bytes plus a token built from the run id and step index, the way the
    /// Antigravity family embeds a request id. A resume reproduces these bytes only if it
    /// recovers the same run id and step numbering from the log.
    IdentityEmbedding,
}

/// One scripted run: the request, its controls, the provider's step scripts and the tools.
///
/// Every part is a plain function so a spawned runner process can rebuild the same scenario
/// from its name alone.
#[derive(Clone, Copy)]
pub struct Scenario {
    pub name: &'static str,
    /// The request the first run and every resume send. Its session identity must not depend
    /// on the state root: each kill gets a fresh root holding a copy of the same log.
    pub request: fn() -> RunRequest,
    pub control: fn() -> RunControl,
    /// Step scripts, indexed by step number minus one, so a resume that already holds N
    /// durable steps is served step N+1.
    pub scripts: fn() -> Vec<Vec<NormalizedEvent>>,
    pub render: Render,
    pub plane: fn() -> StubToolPlane,
    /// How the uninterrupted run ends.
    pub expected: RunFinishReason,
}

/// Where the runner blocks forever so a process kill can land while it is inside a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InFlight {
    /// Inside the tool plane's dispatch of this call: its intent is durable, its result is not.
    ToolDispatch(ToolCallId),
    /// Inside the provider call for this step (1-based).
    ProviderStep(u64),
}

impl InFlight {
    pub(crate) fn encode(&self) -> String {
        match self {
            InFlight::ToolDispatch(id) => format!("tool:{}", id.0),
            InFlight::ProviderStep(step) => format!("step:{step}"),
        }
    }

    pub(crate) fn decode(text: &str) -> Option<Self> {
        match text.split_once(':')? {
            ("tool", id) => Some(InFlight::ToolDispatch(ToolCallId(id.into()))),
            ("step", step) => step.parse().ok().map(InFlight::ProviderStep),
            _ => None,
        }
    }
}

/// Written by a runner that reached its in-flight point, just before it blocks.
pub(crate) fn reached_marker(state_root: &Path) -> PathBuf {
    state_root.join("in-flight-point-reached")
}

/// Block this task forever after announcing that the in-flight point was reached.
async fn announce_and_block(marker: &Path) -> ! {
    std::fs::write(marker, b"reached").expect("write the in-flight marker");
    std::future::pending::<()>().await;
    unreachable!("a pending future never completes")
}

/// The scripted provider. Records the bytes of every request and counts every call, and
/// refuses a prompt that pairs a tool call with no result (or a result with no call), the way
/// a real provider rejects one.
#[derive(Clone)]
pub struct ScriptedProvider {
    scripts: Arc<Vec<Vec<NormalizedEvent>>>,
    render: Render,
    sent: Arc<Mutex<BTreeMap<usize, Vec<u8>>>>,
    requests: Arc<AtomicUsize>,
    rejected: Arc<Mutex<Vec<String>>>,
    block_at: Option<(u64, PathBuf)>,
}

impl ScriptedProvider {
    pub fn new(scripts: Vec<Vec<NormalizedEvent>>, render: Render) -> Self {
        Self {
            scripts: Arc::new(scripts),
            render,
            sent: Arc::default(),
            requests: Arc::default(),
            rejected: Arc::default(),
            block_at: None,
        }
    }

    pub fn for_scenario(scenario: &Scenario) -> Self {
        Self::new((scenario.scripts)(), scenario.render)
    }

    pub(crate) fn blocking_at(mut self, step: u64, marker: PathBuf) -> Self {
        self.block_at = Some((step, marker));
        self
    }

    /// The request bytes of every step this provider was asked for, by 0-based step index.
    pub fn sent(&self) -> BTreeMap<usize, Vec<u8>> {
        self.sent.lock().unwrap().clone()
    }

    /// How many provider calls were made, refused ones included.
    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// Why each refused request was refused.
    pub fn rejected(&self) -> Vec<String> {
        self.rejected.lock().unwrap().clone()
    }
}

/// Whether `prompt` is one a provider accepts: every assistant tool call is answered by a tool
/// message right after it holding exactly one result per call, and no result appears without
/// its call.
pub fn check_tool_pairing(prompt: &[Message]) -> Result<(), String> {
    let mut index = 0;
    while index < prompt.len() {
        let message = &prompt[index];
        let calls: Vec<&ToolCall> = message
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        if message.role == Role::Tool {
            return Err(format!(
                "message {index} holds tool results with no tool call before it"
            ));
        }
        if calls.is_empty() {
            index += 1;
            continue;
        }
        let Some(answer) = prompt.get(index + 1).filter(|next| next.role == Role::Tool) else {
            return Err(format!(
                "message {index} makes tool calls that no tool message answers"
            ));
        };
        let mut called: Vec<&str> = calls
            .iter()
            .map(|call| call.tool_call_id.0.as_str())
            .collect();
        let mut answered: Vec<&str> = answer
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
                _ => None,
            })
            .collect();
        called.sort_unstable();
        answered.sort_unstable();
        if called != answered {
            return Err(format!(
                "message {index} calls {called:?} but the tool message answers {answered:?}"
            ));
        }
        index += 2;
    }
    Ok(())
}

#[async_trait]
impl Provider for ScriptedProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(64);
        if let Err(why) = check_tool_pairing(&opts.prompt) {
            // Refused: the stream ends with no assistant message, which the loop reports as a
            // provider failure. The harness also fails any cut that records a refusal.
            self.rejected.lock().unwrap().push(why);
            return rx;
        }
        let idx = (step_id.0 - 1) as usize;
        let bytes = self.render_request(opts);
        self.sent.lock().unwrap().insert(idx, bytes);
        if let Some((step, marker)) = &self.block_at {
            if *step == step_id.0 {
                announce_and_block(marker).await;
            }
        }
        let script = self.scripts.get(idx).cloned().unwrap_or_default();
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        let mut bytes = broca_core::render::canonical_request_bytes(opts);
        if self.render == Render::IdentityEmbedding {
            let step = opts
                .step_context
                .as_ref()
                .expect("the loop populates step_context on every request");
            bytes.extend_from_slice(
                format!("\nidentity:{}:{}", step.run_id, step.step_index).as_bytes(),
            );
        }
        bytes
    }
}

/// The scenario's stub tool plane, counting every dispatch it is asked for, before the stub's
/// own repeated-id fence can hide a second one.
#[derive(Clone)]
pub struct HarnessPlane {
    inner: Arc<StubToolPlane>,
    dispatched: Arc<Mutex<Vec<ToolCallId>>>,
    block_at: Option<(ToolCallId, PathBuf)>,
    /// Tripped when a call is dispatched, standing in for the module's stop request arriving
    /// while that call runs.
    stop_on_dispatch: Option<CancellationToken>,
}

impl HarnessPlane {
    pub fn new(inner: StubToolPlane) -> Self {
        Self {
            inner: Arc::new(inner),
            dispatched: Arc::default(),
            block_at: None,
            stop_on_dispatch: None,
        }
    }

    pub fn for_scenario(scenario: &Scenario) -> Self {
        Self::new((scenario.plane)())
    }

    pub(crate) fn blocking_at(mut self, call: ToolCallId, marker: PathBuf) -> Self {
        self.block_at = Some((call, marker));
        self
    }

    /// Trip `stop` as each call is dispatched, before the call returns.
    pub(crate) fn stopping_on_dispatch(mut self, stop: CancellationToken) -> Self {
        self.stop_on_dispatch = Some(stop);
        self
    }

    /// Every dispatch this plane was asked for, in order.
    pub fn dispatched(&self) -> Vec<ToolCallId> {
        self.dispatched.lock().unwrap().clone()
    }

    pub fn dispatch_count(&self, id: &ToolCallId) -> usize {
        self.dispatched
            .lock()
            .unwrap()
            .iter()
            .filter(|dispatched| *dispatched == id)
            .count()
    }
}

#[async_trait]
impl ToolPlane for HarnessPlane {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.dispatched
            .lock()
            .unwrap()
            .push(call.tool_call_id.clone());
        if let Some((id, marker)) = &self.block_at {
            if *id == call.tool_call_id {
                announce_and_block(marker).await;
            }
        }
        if let Some(stop) = &self.stop_on_dispatch {
            stop.cancel();
        }
        self.inner.dispatch(ctx, call).await
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }
}

/// What one run of the real loop returned.
pub struct RunOutcome {
    pub reason: RunFinishReason,
    pub events: Vec<OutputEvent>,
}

/// Run `request` through the real run loop against the log under `state_root`: a fresh start
/// on an empty log, a resume on one that holds an unfinished episode.
pub async fn drive<W: WalStore>(
    request: RunRequest,
    control: RunControl,
    state_root: &Path,
    wal_store: &W,
    provider: &ScriptedProvider,
    plane: &HarnessPlane,
) -> RunOutcome {
    let lease = FileLeaseStore::new(state_root.join("lease"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop_with_control(
        request, provider, plane, &lease, &AllowAll, wal_store, None, control, &tx,
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "the run loop failed under {}: {error}",
            state_root.display()
        )
    });
    drop(tx);
    RunOutcome {
        reason: summary.reason,
        events: collector.await.expect("event collector"),
    }
}
