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
    /// The canonical bytes, from a provider that binds signed reasoning to the bytes before it
    /// (as Anthropic's thinking signatures are), so compaction views strip or defer it.
    CanonicalPrefixBound,
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

    fn reasoning_is_prefix_bound(&self) -> bool {
        self.render == Render::CanonicalPrefixBound
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
    #[cfg(test)]
    let control = {
        let mut control = control;
        crate::compaction::schedule_views(&mut control, &request, state_root);
        control
    };
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

#[cfg(all(test, unix))]
mod lease_tests {
    use super::*;
    use std::fs::{self, Metadata, TryLockError};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::AtomicBool;

    use broca_core::lease::WalLeaseDomain;
    use broca_core::{FileWalStore, LeaseError, WalHandle};
    use broca_protocol::BindIdentity;
    use broca_test_support::wait_until_sync;
    use broca_wal::fs_ops::{with_fs_ops, FileOps, FsOps, OpenOptions, ReadDir, RealFs};
    use broca_wal::{FramedRecord, Wal, WalError, WalLineageId, WalRecord};

    use crate::power_loss::{Family, RecordingFs, State};

    const CHILD: &str = "scenario::lease_tests::lease_process_child";
    const LIMIT: usize = 100_000;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "broca-lease-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn identity(root: &Path) -> BindIdentity {
        BindIdentity::new(root.join("project"), "crash-harness", "lease-tenure")
    }

    fn record(step: u64) -> WalRecord {
        WalRecord::StepStarted {
            step_id: StepId(step),
        }
    }

    fn seed(root: &Path, session: &BindIdentity) {
        let store = FileWalStore::new(root.join("state/wal"));
        store
            .open(session)
            .unwrap()
            .append(1, &record(1), true)
            .unwrap();
        // Recovery must truncate a real torn tail while retaining the acquired tenure.
        fs::OpenOptions::new()
            .append(true)
            .open(store.wal_path(session))
            .unwrap()
            .write_all(b"torn")
            .unwrap();
    }

    #[derive(Default)]
    struct Hooks {
        pause: Option<(String, PathBuf)>,
        trace: Mutex<Vec<&'static str>>,
        closed: AtomicBool,
        syncs: AtomicUsize,
        preencoded: AtomicUsize,
    }

    impl Hooks {
        fn hit(&self, phase: &'static str) {
            self.trace.lock().unwrap().push(phase);
            if let Some((target, marker)) = &self.pause {
                if target == phase {
                    fs::write(marker, phase).unwrap();
                    if matches!(phase, "released" | "takeover-live") {
                        let resume = marker.with_extension("resume");
                        wait_until_sync(&format!("lease {phase} resume marker"), || {
                            if resume.exists() {
                                Ok(())
                            } else {
                                Err(format!("missing resume marker {}", resume.display()))
                            }
                        });
                        return;
                    }
                    loop {
                        std::thread::park();
                    }
                }
            }
        }

        fn append(&self, record: &WalRecord, before: bool) {
            self.hit(if before {
                "append-before"
            } else {
                "append-after"
            });
            if matches!(record, WalRecord::StepStarted { step_id: StepId(3) }) {
                self.hit(if before {
                    "drain-append-before"
                } else {
                    "drain-append-after"
                });
            }
        }

        fn assert_wal_closed(&self) {
            assert!(self.closed.load(Ordering::Acquire));
            let trace = self.trace.lock().unwrap();
            let closed = trace
                .iter()
                .position(|phase| *phase == "close-after")
                .unwrap();
            assert!(trace[closed + 1..]
                .iter()
                .all(|phase| !phase.contains("append")));
        }
    }

    // The override is thread-local: install it in the synchronous store and WAL calls,
    // not around the async opener whose recovery and writer run on other threads.
    struct LeaseStore {
        inner: FileWalStore,
        fs: Arc<dyn FsOps>,
        hooks: Arc<Hooks>,
    }

    impl LeaseStore {
        fn new(state: &Path, fs: Arc<dyn FsOps>, hooks: Arc<Hooks>) -> Self {
            Self {
                inner: FileWalStore::new(state.join("wal")),
                fs,
                hooks,
            }
        }
    }

    impl WalStore for LeaseStore {
        fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            with_fs_ops(self.fs.clone(), || self.inner.open(session))
        }

        fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
            with_fs_ops(self.fs.clone(), || self.inner.read_records(session))
        }

        fn open_owned_writer(
            &self,
            session: &BindIdentity,
            retention: Option<u64>,
            class: Option<&str>,
            lease: Box<dyn broca_core::LeaseHandle>,
        ) -> Result<(Box<dyn Wal + Send>, Box<dyn broca_core::LeaseHandle>), WalError> {
            self.hooks.hit("open-before");
            let (wal, lease) = with_fs_ops(self.fs.clone(), || {
                self.inner
                    .open_owned_writer(session, retention, class, lease)
            })?;
            self.hooks.hit("open-after");
            Ok((
                Box::new(LeaseWal {
                    inner: Some(wal),
                    fs: self.fs.clone(),
                    hooks: self.hooks.clone(),
                }),
                lease,
            ))
        }
    }

    struct LeaseWal {
        inner: Option<Box<dyn Wal + Send>>,
        fs: Arc<dyn FsOps>,
        hooks: Arc<Hooks>,
    }

    impl LeaseWal {
        fn inner(&self) -> &dyn Wal {
            self.inner.as_deref().unwrap()
        }
    }

    impl Drop for LeaseWal {
        fn drop(&mut self) {
            self.hooks.hit("close-before");
            with_fs_ops(self.fs.clone(), || drop(self.inner.take()));
            self.hooks.closed.store(true, Ordering::Release);
            self.hooks.hit("close-after");
        }
    }

    async fn open(root: &Path, session: &BindIdentity, store: Arc<LeaseStore>) -> WalHandle {
        let tenure = WalLeaseDomain::new(root.join("leases"))
            .acquire(session)
            .unwrap();
        store.hooks.hit("acquired");
        let handle = WalHandle::open_and_spawn_owned(store.clone(), session.clone(), tenure, None)
            .await
            .unwrap();
        store.hooks.hit("open-published");
        handle
    }

    async fn close(root: &Path, session: &BindIdentity, handle: WalHandle, hooks: &Hooks) {
        let mut shutdown = Box::pin(handle.shutdown_into_tenure());
        if let Some((phase, marker)) = &hooks.pause {
            if phase.starts_with("drain-append") {
                use std::future::Future;
                std::future::poll_fn(|cx| {
                    assert!(shutdown.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                fs::write(
                    marker.with_extension("shutdown"),
                    b"waiting for blocked append",
                )
                .unwrap();
            }
        }
        let (result, tenure) = shutdown.await;
        result.unwrap();
        let tenure = tenure.expect("owned close returns the held tenure");
        hooks.assert_wal_closed();
        let domain = WalLeaseDomain::new(root.join("leases"));
        assert!(matches!(
            domain.acquire(session),
            Err(LeaseError::Held { .. })
        ));
        let path = crate::wal::wal_path(&root.join("state"), session);
        let before = fs::read(&path).unwrap();
        hooks.hit("closed-tenure");
        drop(tenure);
        // Consuming shutdown joined the writer. A new tenure must not resurrect its queue.
        let replacement = domain
            .acquire(session)
            .expect("closed tenure releases the lease");
        assert_eq!(fs::read(&path).unwrap(), before, "append after release");
        hooks.assert_wal_closed();
        drop(replacement);
        hooks.hit("released");
    }

    fn assert_prefix(store: &FileWalStore, session: &BindIdentity, minimum: usize) -> usize {
        let frames = store.read_records(session).unwrap();
        assert_prefix_frames(&frames, minimum);
        // A repaired tail must be strictly decodable, not merely accepted by recovery again.
        let bytes = fs::read(store.wal_path(session)).unwrap();
        let mut offset = 0;
        while offset < bytes.len() {
            use broca_wal::framing::DecodeStep;
            offset += match broca_wal::framing::decode_next(&bytes[offset..]) {
                DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                    consumed
                }
                _ => panic!("recovery left a non-frame at {offset}"),
            };
        }
        frames.len()
    }

    fn assert_prefix_frames(frames: &[FramedRecord], minimum: usize) {
        assert!(
            (minimum..=3).contains(&frames.len()),
            "minimum {minimum}, frames {frames:?}"
        );
        for (index, frame) in frames.iter().enumerate() {
            assert_eq!(frame.seq, index as u64 + 1);
            assert_eq!(
                serde_json::to_value(&frame.record).unwrap(),
                serde_json::to_value(record(index as u64 + 1)).unwrap()
            );
        }
    }

    fn child_command(root: &Path, mode: &str, phase: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", CHILD, "--nocapture"])
            .env("BROCA_LEASE_CHILD_ROOT", root)
            .env("BROCA_LEASE_CHILD_MODE", mode)
            .env("BROCA_LEASE_CHILD_PHASE", phase);
        command
    }

    struct Process(Child);

    impl Process {
        fn spawn(root: &Path, mode: &str, phase: &str) -> Self {
            Self(
                child_command(root, mode, phase)
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap(),
            )
        }

        fn at(&mut self, marker: &Path) {
            let child = self.0.id();
            wait_until_sync(
                &format!("lease child {child} marker {}", marker.display()),
                || match self
                    .0
                    .try_wait()
                    .map_err(|error| format!("cannot observe lease child {child}: {error}"))?
                {
                    Some(status) => Err(format!(
                        "lease child {child} exited with {status} before marker {}",
                        marker.display()
                    )),
                    None if marker.exists() => Ok(()),
                    None => Err(format!(
                        "lease child {child} is still running; missing marker {}",
                        marker.display()
                    )),
                },
            );
        }

        fn kill(&mut self) {
            use std::os::unix::process::ExitStatusExt;
            self.0.kill().unwrap();
            assert_eq!(self.0.wait().unwrap().signal(), Some(9));
        }

        fn success(&mut self, description: &str) {
            let child = self.0.id();
            let status = wait_until_sync(description, || {
                self.0
                    .try_wait()
                    .map_err(|error| format!("cannot observe lease child {child}: {error}"))?
                    .ok_or_else(|| format!("lease child {child} is still running"))
            });
            let mut output = String::new();
            if let Some(mut stdout) = self.0.stdout.take() {
                stdout.read_to_string(&mut output).unwrap();
            }
            if let Some(mut stderr) = self.0.stderr.take() {
                stderr.read_to_string(&mut output).unwrap();
            }
            assert!(status.success(), "lease subprocess failed: {output}");
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn probe(root: &Path, held: bool) {
        let mut process = Process(
            child_command(root, if held { "held" } else { "takeover" }, "")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        process.success(if held {
            "lease contender reports Held"
        } else {
            "lease successor acquires, appends and closes"
        });
    }

    #[test]
    fn lease_process_child() {
        let Some(root) = std::env::var_os("BROCA_LEASE_CHILD_ROOT").map(PathBuf::from) else {
            return;
        };
        let mode = std::env::var("BROCA_LEASE_CHILD_MODE").unwrap();
        let session = identity(&root);
        let domain = WalLeaseDomain::new(root.join("leases"));
        if mode == "held" {
            assert!(matches!(
                domain.acquire(&session),
                Err(LeaseError::Held { .. })
            ));
            return;
        }
        let phase = std::env::var("BROCA_LEASE_CHILD_PHASE").unwrap();
        let hooks = Arc::new(Hooks {
            pause: if mode == "owner" {
                Some((phase, root.join("paused")))
            } else if mode == "takeover-hold" {
                Some(("takeover-live".into(), root.join("successor-paused")))
            } else {
                None
            },
            ..Hooks::default()
        });
        let store = Arc::new(LeaseStore::new(
            &root.join("state"),
            Arc::new(RecoveryFs(hooks.clone())),
            hooks.clone(),
        ));
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let handle = open(&root, &session, store.clone()).await;
            if mode.starts_with("takeover") {
                let count = assert_prefix(&store.inner, &session, 1);
                assert_eq!(
                    handle.record(record(99)).into_result().unwrap(),
                    count as u64 + 1
                );
                assert_eq!(handle.sync().await.unwrap(), count as u64 + 1);
                // The successor is itself a live owner, so stale backend state cannot admit a third holder.
                probe(&root, true);
                hooks.hit("takeover-live");
            } else {
                assert_eq!(handle.record(record(2)).into_result().unwrap(), 2);
                assert_eq!(handle.sync().await.unwrap(), 2);
                hooks.hit("ack");
                assert_eq!(handle.record(record(3)).into_result().unwrap(), 3);
            }
            close(&root, &session, handle, &hooks).await;
        });
    }

    #[test]
    fn lease_crash_cuts_reacquire_after_sigkill_without_dual_holders() {
        let phases = [
            "acquired",
            "open-before",
            "truncate-descriptor-open",
            "append-descriptor-open",
            "truncate-before",
            "truncate-after",
            "open-after",
            "recovery-sync-before",
            "recovery-sync-after",
            "open-published",
            "append-before",
            "append-after",
            "sync-before",
            "sync-after",
            "ack",
            "drain-append-before",
            "drain-append-after",
            "final-sync-before",
            "final-sync-after",
            "close-before",
            "close-after",
            "closed-tenure",
        ];
        for phase in phases {
            let root = Scratch::new(phase);
            let session = identity(&root.0);
            seed(&root.0, &session);
            let mut owner = Process::spawn(&root.0, "owner", phase);
            owner.at(&root.0.join("paused"));
            if phase.starts_with("drain-append") {
                owner.at(&root.0.join("paused.shutdown"));
            }
            // Marker synchronization, not a timing window: the old owner remains paused throughout the probe.
            probe(&root.0, true);
            owner.kill();
            probe(&root.0, false);
            let store = FileWalStore::new(root.0.join("state/wal"));
            let frames = store.read_records(&session).unwrap();
            let minimum = match phase {
                "final-sync-after" | "close-before" | "close-after" | "closed-tenure" => 3,
                "sync-after"
                | "ack"
                | "drain-append-before"
                | "drain-append-after"
                | "final-sync-before" => 2,
                _ => 1,
            };
            assert_prefix_frames(&frames[..frames.len() - 1], minimum);
            assert_eq!(frames.last().unwrap().seq, frames.len() as u64);
            assert_eq!(
                serde_json::to_value(&frames.last().unwrap().record).unwrap(),
                serde_json::to_value(record(99)).unwrap()
            );
            eprintln!(
                "lease crash cut: {phase}; successor appended seq {}",
                frames.last().unwrap().seq
            );
        }
    }

    #[test]
    fn lease_released_process_has_no_writable_queue_after_takeover() {
        let root = Scratch::new("released");
        let session = identity(&root.0);
        seed(&root.0, &session);
        let mut old = Process::spawn(&root.0, "owner", "released");
        old.at(&root.0.join("paused"));
        let store = FileWalStore::new(root.0.join("state/wal"));
        assert_eq!(assert_prefix(&store, &session, 3), 3);
        // The old process is alive, but its descriptor and queued append capability are gone.
        let mut successor = Process::spawn(&root.0, "takeover-hold", "");
        successor.at(&root.0.join("successor-paused"));
        let before = fs::read(store.wal_path(&session)).unwrap();
        fs::write(root.0.join("paused.resume"), b"resume").unwrap();
        old.success("released lease predecessor exits after resuming");
        assert_eq!(
            fs::read(store.wal_path(&session)).unwrap(),
            before,
            "old owner appended after release"
        );
        probe(&root.0, true);
        fs::write(root.0.join("successor-paused.resume"), b"resume").unwrap();
        successor.success("lease successor closes after resuming");
        assert_eq!(fs::read(store.wal_path(&session)).unwrap(), before);
    }

    fn operation_count(recording: &RecordingFs) -> usize {
        recording
            .enumerate(LIMIT)
            .unwrap()
            .iter()
            .map(|state| state.cut)
            .max()
            .unwrap()
    }

    fn remember(states: &mut BTreeMap<State, usize>, state: State, minimum: usize) {
        states
            .entry(state)
            .and_modify(|old| *old = (*old).max(minimum))
            .or_insert(minimum);
    }

    #[test]
    fn lease_h1_power_loss_cuts_and_second_recovery_preserve_prefix_and_release_order() {
        let root = Scratch::new("h1");
        let session = identity(&root.0);
        seed(&root.0, &session);
        // Only WAL filesystem operations are modelled. Real lease files stay outside the
        // recorded subtree; process-death tests exercise the backend's surviving lock state.
        let recording = Arc::new(RecordingFs::new(&root.0.join("state")).unwrap());
        let hooks = Arc::new(Hooks::default());
        let store = Arc::new(LeaseStore::new(
            &root.0.join("state"),
            recording.clone(),
            hooks.clone(),
        ));
        let relative_wal = store
            .inner
            .wal_path(&session)
            .strip_prefix(root.0.join("state"))
            .unwrap()
            .to_path_buf();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (synced_cut, synced_len) = runtime.block_on(async {
            let handle = open(&root.0, &session, store.clone()).await;
            assert_eq!(handle.record(record(2)).into_result().unwrap(), 2);
            assert_eq!(handle.sync().await.unwrap(), 2);
            let synced_cut = operation_count(&recording);
            let synced_len = fs::metadata(store.inner.wal_path(&session)).unwrap().len() as usize;
            // Leave an unsynced append for shutdown's final durability barrier.
            assert_eq!(handle.record(record(3)).into_result().unwrap(), 3);
            close(&root.0, &session, handle, &hooks).await;
            (synced_cut, synced_len)
        });
        assert_eq!(
            hooks.preencoded.load(Ordering::Relaxed),
            2,
            "the adapter must preserve production's preencoded path"
        );
        let final_cut = operation_count(&recording);
        assert!(final_cut > synced_cut);
        let first = recording.enumerate(LIMIT).unwrap();
        let complete_len = fs::metadata(store.inner.wal_path(&session)).unwrap().len() as usize;
        assert!(first.iter().any(|state| state.family == Family::SyncedOnly));
        assert!(first.iter().any(|state| state.family == Family::PartialPersistence
            && state.state.files.get(&relative_wal).is_some_and(|bytes|
                bytes.len() > synced_len && bytes.len() < complete_len)),
            "power-loss states must include a partial append, not only the initial torn-tail repair");
        let first_count = first.len();
        let mut unique = BTreeMap::new();
        for state in first {
            let minimum = if state.cut >= final_cut {
                3
            } else if state.cut >= synced_cut {
                2
            } else {
                1
            };
            remember(&mut unique, state.state, minimum);
        }

        // Equivalent byte images need only one recovery, but inherit the strongest
        // acknowledgement requirement of every operation cut that produced them.
        let first_unique = unique.len();
        let mut second_unique = BTreeMap::new();
        let mut second_count = 0;
        let mut interrupted_repairs = 0;
        for (state, minimum) in unique {
            let old_len = state.files[&relative_wal].len();
            let recovery = Scratch::new("h1-recovery");
            state.install(&recovery.0.join("state")).unwrap();
            let recording = Arc::new(RecordingFs::new(&recovery.0.join("state")).unwrap());
            let hooks = Arc::new(Hooks::default());
            let store = Arc::new(LeaseStore::new(
                &recovery.0.join("state"),
                recording.clone(),
                hooks.clone(),
            ));
            runtime.block_on(async {
                let handle = open(&recovery.0, &session, store.clone()).await;
                assert_prefix(&store.inner, &session, minimum);
                close(&recovery.0, &session, handle, &hooks).await;
            });
            let second = recording.enumerate(LIMIT).unwrap();
            let repaired_len = fs::metadata(store.inner.wal_path(&session)).unwrap().len() as usize;
            if repaired_len < old_len {
                let repair_cut = second
                    .iter()
                    .filter(|state| state.state.files[&relative_wal].len() == repaired_len)
                    .map(|state| state.cut)
                    .min()
                    .expect("recorded recovery truncates the torn input");
                assert!(repair_cut > 0);
                assert!(
                    second.iter().any(|state| state.cut == repair_cut
                        && state.state.files[&relative_wal].len() == old_len),
                    "the second loss must explore losing the unsynced recovery truncation"
                );
                interrupted_repairs += 1;
            }
            second_count += second.len();
            assert!(
                second_count <= LIMIT,
                "second-loss exploration exceeded its budget"
            );
            for state in second {
                remember(&mut second_unique, state.state, minimum);
            }
        }
        assert!(
            interrupted_repairs > 0,
            "second-loss exploration must interrupt actual torn-tail repairs"
        );
        let final_unique = second_unique.len();
        for (state, minimum) in second_unique {
            let recovery = Scratch::new("h1-final");
            state.install(&recovery.0.join("state")).unwrap();
            let hooks = Arc::new(Hooks::default());
            let store = Arc::new(LeaseStore::new(
                &recovery.0.join("state"),
                Arc::new(RealFs),
                hooks.clone(),
            ));
            runtime.block_on(async {
                let handle = open(&recovery.0, &session, store.clone()).await;
                let count = assert_prefix(&store.inner, &session, minimum);
                assert_eq!(
                    handle.record(record(99)).into_result().unwrap(),
                    count as u64 + 1
                );
                assert_eq!(handle.sync().await.unwrap(), count as u64 + 1);
                close(&recovery.0, &session, handle, &hooks).await;
                let frames = store.inner.read_records(&session).unwrap();
                assert_eq!(frames.len(), count + 1);
                assert_eq!(frames.last().unwrap().seq, count as u64 + 1);
                assert_eq!(
                    serde_json::to_value(&frames.last().unwrap().record).unwrap(),
                    serde_json::to_value(record(99)).unwrap()
                );
            });
        }
        eprintln!("lease H1: {first_count} first cuts ({first_unique} distinct images), {second_count} recovery cuts ({final_unique} distinct images), {interrupted_repairs} interrupted repairs; WAL persistence checked with real ownership lifecycle checkpoints");
    }

    struct RecoveryFs(Arc<Hooks>);
    struct RecoveryFile {
        inner: Box<dyn FileOps>,
        hooks: Arc<Hooks>,
    }

    impl Read for RecoveryFile {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.inner.read(bytes)
        }
    }
    impl Write for RecoveryFile {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.inner.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    impl Seek for RecoveryFile {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.inner.seek(pos)
        }
    }
    impl FileOps for RecoveryFile {
        fn metadata(&self) -> io::Result<Metadata> {
            self.inner.metadata()
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.hooks.hit("truncate-before");
            self.inner.set_len(len)?;
            self.hooks.hit("truncate-after");
            Ok(())
        }
        fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all()
        }
        fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
            Ok(Box::new(Self {
                inner: self.inner.try_clone()?,
                hooks: self.hooks.clone(),
            }))
        }
        fn lock(&self) -> io::Result<()> {
            self.inner.lock()
        }
        fn unlock(&self) -> io::Result<()> {
            self.inner.unlock()
        }
        fn try_lock(&self) -> Result<(), TryLockError> {
            self.inner.try_lock()
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(bytes, offset)
        }
    }

    impl FsOps for RecoveryFs {
        fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
            let inner = RealFs.open(path, options)?;
            if path.extension().is_some_and(|ext| ext == "wal") && (options.write || options.append)
            {
                self.0.hit(if options.append {
                    "append-descriptor-open"
                } else {
                    "truncate-descriptor-open"
                });
                Ok(Box::new(RecoveryFile {
                    inner,
                    hooks: self.0.clone(),
                }))
            } else {
                Ok(inner)
            }
        }
        fn metadata(&self, path: &Path) -> io::Result<Metadata> {
            RealFs.metadata(path)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            RealFs.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            RealFs.remove_file(path)
        }
        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.sync_dir(path)
        }
        fn create_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir(path)
        }
        fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
            RealFs.hard_link(from, to)
        }
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
            RealFs.canonicalize(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
            RealFs.read_dir(path)
        }
        fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
            RealFs.validate_if_exists(path)
        }
    }

    impl Wal for LeaseWal {
        fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
            self.hooks.append(record, true);
            let result = with_fs_ops(self.fs.clone(), || {
                self.inner.as_mut().unwrap().append(fence, record, sync)
            });
            self.hooks.append(record, false);
            result
        }

        fn append_preencoded(
            &mut self,
            fence: u64,
            record: &WalRecord,
            sync: bool,
            frame: Vec<u8>,
        ) -> Result<u64, WalError> {
            self.hooks.preencoded.fetch_add(1, Ordering::Relaxed);
            self.hooks.append(record, true);
            let result = with_fs_ops(self.fs.clone(), || {
                self.inner
                    .as_mut()
                    .unwrap()
                    .append_preencoded(fence, record, sync, frame)
            });
            self.hooks.append(record, false);
            result
        }

        fn append_budget(&self) -> Option<(u64, u64, u64)> {
            self.inner().append_budget()
        }
        fn recovery_byte_cap(&self) -> Option<u64> {
            self.inner().recovery_byte_cap()
        }
        fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
            self.inner().append_budget_scope()
        }
        fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
            with_fs_ops(self.fs.clone(), || self.inner().replay())
        }
        fn tail_after_seq(&self, seq: u64) -> Result<Vec<FramedRecord>, WalError> {
            with_fs_ops(self.fs.clone(), || self.inner().tail_after_seq(seq))
        }
        fn last_seq(&self) -> u64 {
            self.inner().last_seq()
        }
        fn lineage_id(&self) -> Option<WalLineageId> {
            self.inner().lineage_id()
        }

        fn sync(&self) -> Result<(), WalError> {
            let (before, after) = match self.hooks.syncs.fetch_add(1, Ordering::Relaxed) {
                0 => ("recovery-sync-before", "recovery-sync-after"),
                1 => ("sync-before", "sync-after"),
                _ => ("final-sync-before", "final-sync-after"),
            };
            self.hooks.hit(before);
            with_fs_ops(self.fs.clone(), || self.inner().sync())?;
            self.hooks.hit(after);
            Ok(())
        }
    }
}
