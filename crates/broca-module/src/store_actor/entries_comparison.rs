//! `session.history` and the `compaction-runner/v1` route on a session kept in the new storage,
//! through the module's registry, against the same sends on today's storage.
//!
//! Each scenario drives the same sends through a module on today's storage and a module that
//! starts the session in the new storage, then compares the replies as JSON with only what each
//! storage keeps for itself taken out (see [`masked`]). The store actor doesn't put a planned
//! session on the current compaction contract yet, so the compaction scenarios' executor does it
//! for a store run whose plan names a compaction provider, as today's actor does for today's.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::compaction_v2::protocol::read::{Detail, ReadPage, ReadRequest, Select};
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore, FileWalStore,
    OutputEvent, RunError, RunSummary, StartMode, StubToolPlane,
};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use broca_session_loop::run::Seams;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use super::tests::{auth_failure, request, text_step, Gated};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use crate::registry::LineageRegistry;

/// A compaction provider that is always ready and never hands over a view.
struct ReadyProvider;

#[async_trait]
impl broca_core::CompactionPlane for ReadyProvider {
    async fn provider_capabilities(&self, _: &str) -> broca_core::ProviderCapabilities {
        broca_core::ProviderCapabilities::Declared(vec![])
    }

    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => json!({"answer": "nothing"}),
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// One executor for both storages, as the module's is: today's loop for a session on today's
/// storage, the new loop for one in the new storage, over one scripted provider script each.
/// With `compaction`, both have a ready compaction provider, and a run whose plan names one
/// starts under the current compaction contract with a context limit. With `hooks`, both have a
/// `pre_user` provider.
struct Loops<P = Gated> {
    today: Gated,
    wal: Arc<FileWalStore>,
    store: P,
    compaction: Option<Arc<ReadyProvider>>,
    hooks: Option<Arc<dyn broca_core::HookPlane>>,
}

impl<P> Loops<P> {
    fn compacts(&self, control: &broca_core::RunControl) -> bool {
        self.compaction.is_some()
            && control.manifest.as_deref().is_some_and(|manifest| {
                matches!(broca_core::compaction::item(manifest), Ok(Some(_)))
            })
    }
}

#[async_trait]
impl<P: broca_core::provider::Provider + 'static> RunExecutor for Loops<P> {
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.hooks.clone()
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.compaction
            .clone()
            .map(|plane| plane as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        if self.compacts(&job.control) {
            job.control.admitted_context_limit = Some(200_000);
        }
        let mode = match job.kind {
            RunKind::FreshOrResume | RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
        };
        let tools = StubToolPlane::new().with_canned_text("read", "file contents");
        Box::pin(run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &self.today,
            &tools,
            &AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        ))
        .await
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        if self.compacts(&control) {
            control.new_log_compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
            control.admitted_context_limit = Some(200_000);
        }
        let result = async {
            let tools = StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || 1;
            let seams = Seams {
                provider: &self.store,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
                // As the module's store executor hands the run the plane its control names.
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: None,
                // As the module's store executor runs a store session's runs.
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            match mode {
                StoreRunMode::Fresh => {
                    use broca_session_loop::writer::Writer;
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.store,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await,
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// A module over a scratch folder, which it removes when dropped.
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    gate: Option<Arc<Notify>>,
    /// The scripted provider this side's runs call, which keeps the requests they sent.
    provider: broca_core::provider::StubProvider,
    /// The caller whose sends reach the side's session.
    caller: super::tests::Caller,
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

const IDLE: Duration = Duration::from_millis(100);

/// A module whose sessions start in the new storage when `store`, else on today's storage,
/// over the scripted provider `steps`. With `gated`, every model step waits for the gate. With
/// `compaction`, the module has a ready compaction provider.
async fn side(
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    compaction: bool,
) -> Side {
    side_idling(store, steps, gated, compaction, IDLE).await
}

/// [`side`], whose sessions close after `idle` without a command.
async fn side_idling(
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    compaction: bool,
    idle: Duration,
) -> Side {
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]);
    side_in(store, steps, gated, compaction, idle, deployment).await
}

/// [`side_idling`], whose new storage creates sessions under `deployment`.
async fn side_in(
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    compaction: bool,
    idle: Duration,
    deployment: broca_session_store::keys::DeploymentKey,
) -> Side {
    let build = Build {
        runtime: true,
        new_sessions: store,
        gated,
        compaction,
        hooks: false,
        idle,
    };
    side_with(build, steps, deployment).await
}

/// How a side's module is built.
struct Build {
    /// Whether the module's opener serves the new storage.
    runtime: bool,
    /// Whether the registry starts every session it can in the new storage, not only heads.
    new_sessions: bool,
    /// Whether every model step waits for the side's gate.
    gated: bool,
    /// Whether the executor offers a ready compaction provider.
    compaction: bool,
    /// Whether the executor offers a `pre_user` provider that passes every message unchanged.
    hooks: bool,
    /// How long a session waits idle before its actor closes.
    idle: Duration,
}

/// A `pre_user` provider that passes every message it is asked about unchanged.
struct Passes;

#[async_trait]
impl broca_core::HookPlane for Passes {
    async fn hook(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::hooks::HookCall,
    ) -> Result<broca_core::hooks::HookAnswer, broca_core::HookUnavailable> {
        Ok(broca_core::hooks::HookAnswer::Pass)
    }
}

/// A side built as `build` says, over the scripted provider `steps`, whose new storage creates
/// sessions under `deployment`.
async fn side_with(
    build: Build,
    steps: Vec<Vec<NormalizedEvent>>,
    deployment: broca_session_store::keys::DeploymentKey,
) -> Side {
    side_with_events(build, steps, deployment, None).await
}

async fn side_with_events(
    build: Build,
    steps: Vec<Vec<NormalizedEvent>>,
    deployment: broca_session_store::keys::DeploymentKey,
    step_events: Option<Arc<crate::step_events::StepEvents>>,
) -> Side {
    let Build {
        runtime,
        new_sessions,
        gated,
        compaction,
        hooks,
        idle,
    } = build;
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-entries-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let gate = gated.then(|| Arc::new(Notify::new()));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let provider = broca_core::provider::StubProvider::new(steps.clone());
    let scripted = || Gated {
        render_config: None,
        notes_sends: false,
        hold: None,
        inner: provider.clone(),
        gate: gate.clone(),
    };
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        today: scripted(),
        wal: Arc::clone(&wal),
        store: scripted(),
        compaction: compaction.then(|| Arc::new(ReadyProvider)),
        hooks: hooks.then(|| Arc::new(Passes) as Arc<dyn broca_core::HookPlane>),
    });
    let mut opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        Arc::clone(&executor),
    )
    .with_idle_ttl(idle);
    if let Some(events) = step_events {
        opener = opener.with_step_events(events);
    }
    if runtime {
        let runtime = crate::store_open::StoreRuntime::open(&dir, deployment, executor, idle)
            .await
            .unwrap();
        opener = opener.with_store_runtime(Arc::new(runtime));
    }
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
    let registry = if new_sessions {
        registry.with_store_new_sessions()
    } else {
        registry
    };
    let key = BindIdentity::new(dir.join("project"), "store-entries", "s1");
    Side {
        dir,
        registry,
        key,
        gate,
        provider,
        caller: Default::default(),
    }
}

impl Side {
    /// Sends `request` as the side's caller sends it.
    async fn send_request(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        plan: Option<crate::actor::SendPlan>,
    ) -> SendOutcome {
        self.send_labelled(run_id, request, plan, None).await
    }

    /// [`Side::send_request`], naming the session label `class` when one is given.
    async fn send_labelled(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        plan: Option<crate::actor::SendPlan>,
        class: Option<&str>,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &SendKey::Minted, request),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                None,
                class.map(str::to_owned),
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        self.send_request(run_id, request(text), None).await
    }

    /// Sends `request` on the session's plan with the ordered delivery `order`, as the side's
    /// caller sends it.
    async fn send_ordered(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        order: Value,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &SendKey::Minted, request),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                Some(planned("plan-1", None)),
                None,
                None,
                None,
                None,
                false,
                Some(serde_json::from_value(order).expect("an order decodes")),
            )
            .await
            .expect("the session opens")
    }

    /// Waits until `run_id` is the session's latest run and it no longer runs, letting every
    /// gated model step through meanwhile.
    async fn settled(&self, run_id: &str) {
        use crate::serve::{ServeHandler, SessionReadRequest};
        let mut last = None;
        for _ in 0..2000 {
            if let Some(gate) = &self.gate {
                gate.notify_one();
            }
            let read = self
                .registry
                .read(SessionReadRequest::new(self.key.clone(), None, Some(1)))
                .await
                .expect("the session reads");
            if read.lineage_state.last_run_id.as_deref() == Some(run_id)
                && read.lineage_state.state != "active"
            {
                return;
            }
            last = Some(read.lineage_state);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {run_id} never settled: {last:?}");
    }

    async fn history(&self, request: Value) -> Result<Value, crate::history::HistoryError> {
        let request = serde_json::from_value(request).expect("a history request");
        crate::history::read(&self.registry, &self.key, &request).await
    }

    async fn compaction(
        &self,
        caller: Option<&str>,
        method: &str,
        params: Value,
    ) -> Result<Value, Value> {
        self.registry
            .route_compaction_v2(
                &self.key,
                caller.map(str::to_owned),
                method.into(),
                serde_json::value::to_raw_value(&params).unwrap(),
            )
            .await
            .map(|reply| serde_json::from_str(reply.get()).unwrap())
            .map_err(|refusal| serde_json::to_value(refusal).unwrap())
    }

    fn holds(&self) -> bool {
        broca_wal::session_storage_class(&self.dir, &broca_core::session_addr(&self.key))
            .unwrap()
            .is_some()
    }

    /// Asserts the session's latest run is `run_id` and completed.
    async fn assert_completed(&self, run_id: &str) {
        use crate::serve::{ServeHandler, SessionReadRequest};
        let read = self
            .registry
            .read(SessionReadRequest::new(self.key.clone(), None, Some(1)))
            .await
            .expect("the session reads");
        let state = serde_json::to_value(&read.lineage_state).unwrap();
        assert_eq!(state["last_run_id"], json!(run_id), "{state}");
        assert_eq!(state["state"], json!("completed"), "{state}");
    }
}

/// A `session.history` reply with what each storage keeps for itself taken out: the identity
/// the cursors bind to (`incarnation`), the history version each storage counts for itself, each
/// message's id (each storage mints its own), and wall-clock times, kept as whether present. A
/// cursor is kept as whether present: it encodes the identity and version. A refusal is kept by
/// its code, or, for a request the reader couldn't serve, by its error.
fn masked(reply: &Result<Value, crate::history::HistoryError>) -> Value {
    use crate::history::HistoryError;
    let mut value = match reply {
        Ok(value) => value.clone(),
        Err(HistoryError::Refused { code, .. }) => return json!({ "refused": code }),
        Err(HistoryError::Unrepresentable { ordinal, field }) => {
            return json!({ "unrepresentable": [ordinal, field] })
        }
        Err(HistoryError::Serve(error)) => return json!({ "error": format!("{error:?}") }),
    };
    let page = value.as_object_mut().unwrap();
    page.remove("incarnation");
    page.remove("history_version");
    for cursor in ["older", "newer"] {
        if let Some(at) = page.get_mut(cursor) {
            *at = json!(!at.is_null());
        }
    }
    for message in page["messages"].as_array_mut().unwrap() {
        let message = message.as_object_mut().unwrap();
        message.remove("mid");
        for time in ["written_at", "created_at", "completed_at"] {
            if let Some(at) = message.get_mut(time) {
                *at = json!(!at.is_null());
            }
        }
        // A call key names a position in each storage's own log: kept as whether present.
        for call in message
            .get_mut("tool_calls")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
        {
            let present = !call["call_key"].is_null();
            call["call_key"] = json!(present);
        }
        if let Some(receipt) = message
            .get_mut("delivered_as")
            .and_then(Value::as_object_mut)
        {
            let present = receipt.get("call_key").is_some_and(|key| !key.is_null());
            receipt.insert("call_key".into(), json!(present));
        }
    }
    value
}

/// The id of the message at `ordinal` in a history reply.
fn mid_at(reply: &Value, ordinal: u64) -> String {
    reply["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["ordinal"] == json!(ordinal))
        .and_then(|message| message["mid"].as_str())
        .unwrap_or_else(|| panic!("no message at {ordinal}"))
        .to_owned()
}

/// Compares every raw `session.history` read worth telling apart on both sides: the newest
/// page whole and in short pages, each side following its own cursors, seeks by ordinal and by
/// each side's own message ids, and the refusals of a position the session doesn't hold.
async fn compare_history(today: &Side, store: &Side, label: &str) {
    assert!(store.holds() && !today.holds(), "{label}: the storages");
    let both = |request: Value| async move {
        (
            today.history(request.clone()).await,
            store.history(request).await,
        )
    };
    for request in [
        json!({}),
        json!({"limit": 1}),
        json!({"limit": 2}),
        json!({"from_ordinal": 1, "limit": 2}),
        json!({"from_ordinal": 2}),
        json!({"before_ordinal": 3, "limit": 1}),
        json!({"from_ordinal": 99}),
        json!({"before_ordinal": 99}),
        json!({"from_mid": "no-such-message"}),
        json!({"cursor": "not-a-cursor"}),
        json!({"limit": 1, "max_bytes": 1}),
    ] {
        let (old, new) = both(request.clone()).await;
        alike!(masked(&new), masked(&old), "{label}: {request}");
    }
    // Each side follows its own cursors back from the newest message, and forward again.
    let (mut old, mut new) = both(json!({"limit": 1})).await;
    for step in 0..8 {
        let (Ok(old_page), Ok(new_page)) = (&old, &new) else {
            panic!("{label}: a page failed at step {step}");
        };
        let (Some(older_old), Some(older_new)) = (
            old_page["older"].as_str().map(str::to_owned),
            new_page["older"].as_str().map(str::to_owned),
        ) else {
            alike!(masked(&new), masked(&old), "{label}: the oldest page");
            break;
        };
        old = today.history(json!({"cursor": older_old})).await;
        new = store.history(json!({"cursor": older_new})).await;
        alike!(masked(&new), masked(&old), "{label}: older page {step}");
    }
    if let (Ok(old_page), Ok(new_page)) = (&old, &new) {
        if let (Some(newer_old), Some(newer_new)) =
            (old_page["newer"].as_str(), new_page["newer"].as_str())
        {
            let old = today.history(json!({"cursor": newer_old})).await;
            let new = store.history(json!({"cursor": newer_new})).await;
            alike!(masked(&new), masked(&old), "{label}: a newer page");
        }
    }
    // A seek by message id, each side naming its own id of the same message.
    let (Ok(old_all), Ok(new_all)) = both(json!({"from_ordinal": 1})).await else {
        panic!("{label}: the whole history reads");
    };
    let last = old_all["messages"].as_array().unwrap().len() as u64;
    for ordinal in [1, last.div_ceil(2).max(1), last] {
        for seek in ["from_mid", "before_mid"] {
            let old = today
                .history(json!({ seek: mid_at(&old_all, ordinal), "limit": 2 }))
                .await;
            let new = store
                .history(json!({ seek: mid_at(&new_all, ordinal), "limit": 2 }))
                .await;
            alike!(
                masked(&new),
                masked(&old),
                "{label}: {seek} at {ordinal} (the store's id {})",
                mid_at(&new_all, ordinal)
            );
        }
    }
    // The display form, newest page and from the first message, compared alike.
    for params in [
        json!({"form": "display"}),
        json!({"form": "display", "from_ordinal": 1, "limit": 3}),
    ] {
        let old = today.history(params.clone()).await;
        let new = store.history(params.clone()).await;
        assert!(old.is_ok(), "{label}: today's display history {params}");
        alike!(
            masked_display(&new),
            masked_display(&old),
            "{label}: display {params}"
        );
    }
}

/// A display reply as compared: [`masked`], and every member that names a position in each
/// storage's own log or its own identity, at any depth, kept as whether present: message ids,
/// call keys, output-reference set ids and the digests computed over them, and times.
fn masked_display(reply: &Result<Value, crate::history::HistoryError>) -> Value {
    fn mask(value: &mut Value) {
        const OWN: [&str; 9] = [
            "mid",
            "call_key",
            "set_id",
            "digest",
            "written_at",
            "created_at",
            "completed_at",
            "started_at",
            "finished_at",
        ];
        match value {
            Value::Object(object) => {
                for (key, member) in object.iter_mut() {
                    if OWN.contains(&key.as_str()) {
                        *member = json!(!member.is_null());
                    } else {
                        mask(member);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(mask),
            _ => {}
        }
    }
    let mut value = masked(reply);
    mask(&mut value);
    value
}

/// The compaction route on a session off the compaction contract: every method is refused
/// alike, before its parameters are read.
async fn compare_compaction_refusals(today: &Side, store: &Side, label: &str) {
    for (caller, method, params) in [
        (Some("ck-mc"), "bind", json!({})),
        (None, "bind", json!({})),
        (
            Some("ck-mc"),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        ),
        (Some("ck-mc"), "session.applied_view", json!({})),
        (Some("ck-mc"), "session.apply_view", json!({"view": {}})),
        (Some("ck-mc"), "session.unknown", json!({})),
    ] {
        let old = today.compaction(caller, method, params.clone()).await;
        let new = store.compaction(caller, method, params).await;
        alike!(new, old, "{label}: {method} from {caller:?}");
        assert_eq!(
            old.unwrap_err()["detail"]["reason"],
            json!("session_not_held"),
            "{label}: {method}"
        );
    }
}

/// Both sides of a scenario over the same scripted provider steps.
async fn both(steps: Vec<Vec<NormalizedEvent>>, gated: bool) -> (Side, Side) {
    (
        side(false, steps.clone(), gated, false).await,
        side(true, steps, gated, false).await,
    )
}

/// A step that calls the `read` tool, whose canned result the next step reads.
fn tool_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![
                    ContentBlock::Text {
                        text: "let me look".into(),
                    },
                    ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                        thought_signature: None,
                        tool_call_id: broca_protocol::ToolCallId("c1".into()),
                        tool_name: "read".into(),
                        input: json!({"path": "a"}).into(),
                    }),
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A step the provider refuses for good.
fn failing_step() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(json!({
            "class": "permanent",
            "message": "the request is malformed",
            "status": 400
        }))
        .unwrap(),
        retries_used: 0,
    })]
}

/// Finished runs read alike from either storage, a tool turn's calls included: each call's key
/// (present on both), the module it was dispatched to, and that it isn't indeterminate.
#[tokio::test]
async fn finished_runs_read_as_the_same_history() {
    let (today, store) = both(
        vec![
            text_step("a1", "hello"),
            tool_step("a2"),
            text_step("a3", "read it"),
        ],
        false,
    )
    .await;
    for side in [&today, &store] {
        assert!(matches!(
            side.send("r1", "first").await,
            SendOutcome::Active { .. }
        ));
        side.settled("r1").await;
        // The actor may not have seen r1 end yet, and then queues r2 behind it.
        assert!(matches!(
            side.send("r2", "second").await,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ));
        side.settled("r2").await;
    }
    // The comparison sees a served call: keyed, dispatched, determinate.
    let read = crate::serve::ServeHandler::read(
        &store.registry,
        crate::serve::SessionReadRequest::new(store.key.clone(), Some(1), None),
    )
    .await
    .unwrap();
    let read = serde_json::to_value(&read).unwrap();
    let calls: Vec<&Value> = read["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
        .collect();
    assert_eq!(calls.len(), 1, "{read}");
    assert!(calls[0]["call_key"].is_string(), "{read}");
    assert_eq!(calls[0]["indeterminate"], json!(false), "{read}");
    compare_history(&today, &store, "finished").await;
    compare_compaction_refusals(&today, &store, "finished").await;
}

/// A run the provider failed, and a run paused for sign-in, read alike.
#[tokio::test]
async fn failed_and_paused_runs_read_as_the_same_history() {
    let (today, store) = both(
        vec![text_step("a1", "hello"), failing_step(), auth_failure()],
        false,
    )
    .await;
    for side in [&today, &store] {
        side.send("r1", "first").await;
        side.settled("r1").await;
        side.send("r2", "second").await;
        side.settled("r2").await;
        side.send("r3", "third").await;
        side.settled("r3").await;
    }
    compare_history(&today, &store, "failed and paused").await;
    compare_compaction_refusals(&today, &store, "failed and paused").await;
}

/// A run cancelled while its model step was in flight, then a send queued behind a running run
/// and drained after it, read alike. Every step answers the same text: a model step cut by the
/// cancel may or may not have taken its scripted answer, and that isn't what's compared.
#[tokio::test]
async fn cancelled_and_queued_runs_read_as_the_same_history() {
    let (today, store) = both(
        vec![
            text_step("a1", "hello"),
            text_step("a2", "hello"),
            text_step("a3", "hello"),
            text_step("a4", "hello"),
        ],
        true,
    )
    .await;
    for side in [&today, &store] {
        assert!(matches!(
            side.send("r1", "first").await,
            SendOutcome::Active { .. }
        ));
        side.registry
            .route_cancel(&side.key, RunId("r1".into()))
            .await
            .expect("the cancel is routed");
        side.settled("r1").await;
        // Settling may leave one release unused; take it back, so r2's first step waits until
        // r3 is queued behind it.
        if let Some(gate) = &side.gate {
            let _ = tokio::time::timeout(Duration::from_millis(20), gate.notified()).await;
        }
        assert!(matches!(
            side.send("r2", "second").await,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ));
        assert!(
            matches!(side.send("r3", "queued").await, SendOutcome::Pending { .. }),
            "a send while r2 runs is queued"
        );
        side.settled("r3").await;
    }
    compare_history(&today, &store, "cancelled and queued").await;
    compare_compaction_refusals(&today, &store, "cancelled and queued").await;
}

/// A plan naming the compaction provider `ck-mc`, and, when `renders`, asking its runs to show
/// part numbers to the model (the plan's `render_tags`).
fn compacting_manifest(renders: bool) -> broca_wal::PlanManifest {
    let mut plan = json!({"compaction_item": {"provider": "ck-mc", "preset": "default"}});
    if renders {
        plan["render_tags"] = json!(true);
    }
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![serde_json::from_value(json!({
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }))
        .unwrap()]),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }
}

fn planned(identity: &str, manifest: Option<broca_wal::PlanManifest>) -> crate::actor::SendPlan {
    crate::actor::SendPlan {
        identity: identity.into(),
        // Only a send creating the session carries the manifest its plan fetched; a later send
        // names the plan, and runs under the manifest the session froze.
        manifest: manifest.map(Arc::new),
        model_check: None,
    }
}

/// A send carrying no tools of its own, as a planned session's sends do.
fn untooled(text: &str) -> SubmissionRequest {
    let mut request = request(text);
    request.tools = None;
    request.policy_allowed = Default::default();
    request
}

/// The same session on each storage, on the current compaction contract, whose plan names
/// `ck-mc` and, when `renders`, renders part numbers: two runs, the second with a tool turn, and
/// replies carrying part-number notation.
async fn contract_sessions(renders: bool) -> (Side, Side) {
    contract_sessions_with(renders, Vec::new(), IDLE).await
}

/// [`contract_sessions`], with `more` steps scripted after its two runs' and sessions that
/// close after `idle` without a command.
async fn contract_sessions_with(
    renders: bool,
    more: Vec<Vec<NormalizedEvent>>,
    idle: Duration,
) -> (Side, Side) {
    contract_sessions_in(renders, more, idle, || {
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32])
    })
    .await
}

/// [`contract_sessions_with`], whose new storage creates sessions under the key `deployment`
/// makes.
async fn contract_sessions_in(
    renders: bool,
    more: Vec<Vec<NormalizedEvent>>,
    idle: Duration,
    deployment: impl Fn() -> broca_session_store::keys::DeploymentKey,
) -> (Side, Side) {
    let mut steps = vec![
        text_step("a1", "§2§ Done."),
        tool_step("a2"),
        text_step("a3", "§5§ §6§ It says file contents."),
    ];
    steps.extend(more);
    let today = side_in(false, steps.clone(), false, true, idle, deployment()).await;
    let store = side_in(true, steps, false, true, idle, deployment()).await;
    for side in [&today, &store] {
        assert!(matches!(
            side.send_request(
                "r1",
                untooled("first"),
                Some(planned("plan-1", Some(compacting_manifest(renders))))
            )
            .await,
            SendOutcome::Active { .. }
        ));
        side.settled("r1").await;
        side.assert_completed("r1").await;
        assert!(matches!(
            side.send_request("r2", untooled("read a"), Some(planned("plan-1", None)))
                .await,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ));
        side.settled("r2").await;
        side.assert_completed("r2").await;
    }
    let view = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::store_entries::on_contract(&view),
        "the store session is on the compaction contract"
    );
    (today, store)
}

/// A compaction reply with what each storage keeps for itself taken out: message ids (each
/// storage mints its own), kept as which entry they name, and wall-clock times, kept as whether
/// present.
fn compaction_masked(mut reply: Result<Value, Value>) -> Result<Value, Value> {
    fn mask(value: &mut Value) {
        match value {
            Value::Object(members) => {
                for (key, member) in members.iter_mut() {
                    match key.as_str() {
                        "mid" => *member = json!("<mid>"),
                        "written_at" | "completed_at" => *member = json!(!member.is_null()),
                        _ => mask(member),
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(mask),
            _ => {}
        }
    }
    match &mut reply {
        Ok(value) | Err(value) => mask(value),
    }
    reply
}

/// Each entry's id, by its ordinal, as `session.entries` serves it.
async fn entry_mids(side: &Side) -> std::collections::BTreeMap<u64, String> {
    let page = side
        .compaction(
            Some("ck-mc"),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "compact"}),
        )
        .await
        .unwrap();
    page["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["ordinal"].as_u64().unwrap(),
                entry["mid"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

/// The compaction route admits only the provider the plan in force froze, and serves it the same
/// entries and applied view on either storage: the same refusals, every `after` page however it
/// is cut, reads by ids and by part numbers, and compact detail.
#[tokio::test]
async fn the_compaction_route_serves_the_frozen_provider_alike() {
    let (today, store) = contract_sessions(false).await;
    let both = |caller: Option<&'static str>, method: &'static str, params: Value| {
        let (today, store) = (&today, &store);
        async move {
            (
                compaction_masked(today.compaction(caller, method, params.clone()).await),
                compaction_masked(store.compaction(caller, method, params).await),
            )
        }
    };
    for (caller, method, params) in [
        (Some("ck-mc"), "bind", json!({})),
        (Some("another"), "bind", json!({})),
        (None, "bind", json!({})),
        (Some("another"), "session.entries", json!({})),
        (Some("another"), "session.applied_view", json!({})),
        (Some("another"), "session.unknown", json!({})),
        (Some("ck-mc"), "session.applied_view", json!({})),
        (Some("ck-mc"), "session.unknown", json!({})),
        (
            Some("ck-mc"),
            "session.entries",
            json!({"select": {"by": "after", "limit": 0}, "detail": "full"}),
        ),
    ] {
        let (old, new) = both(caller, method, params.clone()).await;
        alike!(new, old, "{method} from {caller:?}: {params}");
    }
    assert_eq!(
        store.compaction(Some("ck-mc"), "bind", json!({})).await,
        Ok(json!({}))
    );
    assert_eq!(
        store
            .compaction(Some("another"), "bind", json!({}))
            .await
            .unwrap_err()["detail"]["reason"],
        json!("provider_mismatch")
    );
    let today_mids = entry_mids(&today).await;
    let store_mids = entry_mids(&store).await;
    let len = today_mids.len() as u64;
    assert!(len >= 6, "{len} entries");
    assert_eq!(store_mids.len() as u64, len);
    let mut requests = Vec::new();
    for after in (0..=len + 1).map(Some).chain([None]) {
        for limit in [Some(1), Some(2), Some(len), None] {
            for detail in ["full", "compact"] {
                let mut select = json!({"by": "after"});
                if let Some(after) = after {
                    select["after_ordinal"] = json!(after);
                }
                if let Some(limit) = limit {
                    select["limit"] = json!(limit);
                }
                requests.push(json!({"select": select, "detail": detail}));
            }
        }
    }
    requests.push(json!({"select": {"by": "after"}, "detail": "full", "max_bytes": 600}));
    requests.push(json!({"select": {"by": "tags", "tags": [1, 3, 999]}, "detail": "compact"}));
    for request in requests {
        let (old, new) = both(Some("ck-mc"), "session.entries", request.clone()).await;
        alike!(new, old, "session.entries {request}");
    }
    // A read by ids names each storage's own ids of the same messages.
    let by_ids = |mids: &std::collections::BTreeMap<u64, String>| {
        json!({"select": {"by": "mids", "mids": [mids[&len], "nobody", mids[&1], mids[&3]]},
               "detail": "full"})
    };
    let old = compaction_masked(
        today
            .compaction(Some("ck-mc"), "session.entries", by_ids(&today_mids))
            .await,
    );
    let new = compaction_masked(
        store
            .compaction(Some("ck-mc"), "session.entries", by_ids(&store_mids))
            .await,
    );
    alike!(new, old, "session.entries by ids");
    assert_eq!(new.unwrap()["entries"].as_array().unwrap().len(), 3);
    // Both storages refuse a malformed view (here an empty object) before reading the session.
    let params = json!({"view": {}});
    let old = today
        .compaction(Some("ck-mc"), "session.apply_view", params.clone())
        .await;
    let new = store
        .compaction(Some("ck-mc"), "session.apply_view", params)
        .await;
    alike!(new, old);
    assert_eq!(new.unwrap_err()["code"], json!("invalid_params"));
}

/// `session.entries` pages the session's entries exactly as today's paging pages the whole
/// transcript (`views::page` over every entry, with the newest entry as the head). An `after`
/// read loads only the entries it needs, so this is what proves the bounded load serves the same
/// page as paging everything would.
#[tokio::test]
async fn session_entries_pages_as_todays_paging_of_the_whole_transcript() {
    let (_today, store) = contract_sessions(false).await;
    let session = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    let all = crate::store_entries::all_entries(&session).await.unwrap();
    assert!(all.len() >= 6, "{} entries", all.len());
    let runtime = store.registry.store_runtime().unwrap();
    let parts = runtime.parts_for(broca_wal::StorageClass::Ordinary, session.head().cipher());
    let admission = broca_session_loop::run::admission_of_start(
        session
            .operations
            .run
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap(),
        parts,
    )
    .unwrap();
    let counts = std::cell::RefCell::new(broca_core::compaction_v2::tokens::Counts::default());
    counts.borrow_mut().configure(
        &broca_session_loop::compaction::frozen(&admission),
        admission.manifest.as_deref(),
    );
    let today = |request: &ReadRequest| {
        let mut head = broca_core::compaction_v2::protocol::read::Head::new();
        head.newest = broca_core::compaction_v2::views::newest(&all);
        // Derive metadata before selecting and compacting, so the byte caps still compare the
        // bounded route with paging the whole transcript, including its new count metadata.
        serde_json::to_value(
            counts
                .borrow_mut()
                .finish_page(ReadPage::new(all.clone(), head), request),
        )
        .unwrap()
    };
    let len = all.len() as u64;
    let tags: Vec<u64> = all
        .iter()
        .flat_map(|entry| entry.parts.iter().filter_map(|part| part.tag))
        .collect();
    assert!(!tags.is_empty(), "the entries carry part numbers");
    let mut requests = Vec::new();
    for after in (0..=len + 1).map(Some).chain([None]) {
        for limit in [Some(1), Some(2), Some(len), None] {
            for detail in [Detail::Full, Detail::Compact] {
                requests.push(ReadRequest::new(
                    Select::After {
                        after_ordinal: after,
                        limit,
                    },
                    detail,
                ));
            }
        }
    }
    let mut capped = ReadRequest::new(
        Select::After {
            after_ordinal: None,
            limit: None,
        },
        Detail::Full,
    );
    capped.max_bytes = Some(600);
    requests.push(capped);
    requests.push(ReadRequest::new(
        Select::Mids {
            mids: vec![
                all[len as usize - 1].mid.clone(),
                "nobody".into(),
                all[0].mid.clone(),
            ],
        },
        Detail::Full,
    ));
    requests.push(ReadRequest::new(
        Select::Tags {
            tags: vec![tags[0], tags[tags.len() - 1], 999],
        },
        Detail::Compact,
    ));
    // Reads by ids and by part numbers find each named entry instead of reading every entry, so
    // every id and every number is read alone, and all of them together, against the same page
    // over the whole transcript: a tool turn's results are numbered with its call.
    for entry in &all {
        requests.push(ReadRequest::new(
            Select::Mids {
                mids: vec![entry.mid.clone()],
            },
            Detail::Full,
        ));
    }
    requests.push(ReadRequest::new(
        Select::Mids {
            mids: all.iter().rev().map(|entry| entry.mid.clone()).collect(),
        },
        Detail::Full,
    ));
    let highest = tags.iter().copied().max().unwrap();
    for tag in 0..=highest + 1 {
        requests.push(ReadRequest::new(
            Select::Tags { tags: vec![tag] },
            Detail::Full,
        ));
    }
    requests.push(ReadRequest::new(
        Select::Tags { tags: tags.clone() },
        Detail::Full,
    ));
    for request in &requests {
        let served = store
            .compaction(
                Some("ck-mc"),
                "session.entries",
                serde_json::to_value(request).unwrap(),
            )
            .await
            .unwrap_or_else(|refusal| panic!("{refusal}"));
        assert_eq!(
            served,
            today(request),
            "{}",
            serde_json::to_value(request).unwrap()
        );
        let _: ReadPage = serde_json::from_value(served).unwrap();
    }
}

/// A store session's reads clean part-number notation out of a reply wherever its entry facts
/// say its run cleans replies (`EntryFacts::clean_reply`): under a run whose plan renders part
/// numbers, and under one whose plan only names a compaction provider, whose compaction view
/// shows the model numbers too. Today's storage cleans only under the first: its read follows
/// the run's own `render_tags`, so its pages under the second keep the tags, which the new
/// storage stopped serving (`store_exposure`). The stored message always keeps the text the
/// model wrote.
#[tokio::test]
async fn reads_clean_replies_where_their_facts_say_their_run_cleans_them() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let raw = ["§2§ Done.", "let me look", "§5§ §6§ It says file contents."];
    let cleaned: Vec<String> = raw
        .iter()
        .map(|text| broca_protocol::tags::exposed_model_text(true, text).into_owned())
        .collect();
    assert_eq!(cleaned[0], "Done.");
    let texts = |messages: &[Value]| -> Vec<String> {
        messages
            .iter()
            .filter(|message| message["message"]["role"] == "assistant")
            .flat_map(|message| message["message"]["content"].as_array().unwrap().clone())
            .filter_map(|block| block["text"].as_str().map(str::to_owned))
            .collect()
    };
    for (renders, today_expected) in [
        (true, cleaned.clone()),
        (false, raw.map(str::to_owned).to_vec()),
    ] {
        let (today, store) = contract_sessions(renders).await;
        let old = today.history(json!({"from_ordinal": 1})).await.unwrap();
        assert_eq!(
            texts(old["messages"].as_array().unwrap()),
            today_expected,
            "today's history, renders {renders}"
        );
        let new = store.history(json!({"from_ordinal": 1})).await.unwrap();
        assert_eq!(
            texts(new["messages"].as_array().unwrap()),
            cleaned,
            "the store's history, renders {renders}"
        );
        let read = store
            .registry
            .read(SessionReadRequest::new(store.key.clone(), Some(1), None))
            .await
            .unwrap();
        let read = serde_json::to_value(&read).unwrap();
        assert_eq!(
            texts(read["messages"].as_array().unwrap()),
            cleaned,
            "the store's session.read, renders {renders}"
        );
        // The stored replies keep the model's own text.
        let session = store
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&store.key)
            .await
            .unwrap()
            .unwrap();
        let stored: Vec<String> = session
            .conversation
            .messages
            .iter()
            .filter(|message| message.role == broca_protocol::Role::Assistant)
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(stored, raw, "renders {renders}");
    }
}

/// The text parts of each assistant message of `messages`, in order, in whichever form holds
/// them: a read entry's `message.content`, a display entry's `row.parts` or a compaction entry's
/// `parts`.
fn assistant_texts(messages: &[Value]) -> Vec<Vec<String>> {
    messages
        .iter()
        .map(|message| match &message["row"] {
            Value::Object(_) => &message["row"],
            _ => message,
        })
        .filter(|message| {
            message["message"]["role"] == "assistant" || message["role"] == "assistant"
        })
        .map(|message| {
            let parts = message["message"]["content"]
                .as_array()
                .or_else(|| message["parts"].as_array())
                .cloned()
                .unwrap_or_default();
            parts
                .iter()
                .filter(|part| part["type"] == "text" || part["kind"] == "text")
                .filter_map(|part| part["text"].as_str().map(str::to_owned))
                .collect()
        })
        .collect()
}

/// The replies of [`served_session`]'s three runs as the model wrote them: one starting with its
/// own part number, one tool-calling step, one starting with two numbers, and one naming
/// numbers mid-text.
const WRITTEN: [&str; 4] = [
    "§2§ Done.",
    "let me look",
    "§5§ §6§ It says file contents.",
    "As §3§ shows, the file §4§ holds it.",
];

/// A store session on the current compaction contract whose plan names `ck-mc` without asking
/// for rendered numbers, after three runs replying [`WRITTEN`], with a subscriber attached live
/// before the third run and the deliveries it received.
async fn served_session() -> (Side, Vec<crate::Delivery>) {
    use crate::serve::{ServeHandler, SubscribeFrom};
    let (_today, store) = contract_sessions_with(
        false,
        vec![text_step("a4", WRITTEN[3])],
        Duration::from_secs(600),
    )
    .await;
    let (sink, mut deliveries) = mpsc::channel(4096);
    store
        .registry
        .subscribe(&store.key, SubscribeFrom::Live, sink)
        .await
        .unwrap();
    assert!(matches!(
        store
            .send_request("r3", untooled("and"), Some(planned("plan-1", None)))
            .await,
        SendOutcome::Active { .. } | SendOutcome::Pending { .. }
    ));
    store.settled("r3").await;
    store.assert_completed("r3").await;
    let mut live = Vec::new();
    while let Ok(Some(delivery)) =
        tokio::time::timeout(Duration::from_millis(300), deliveries.recv()).await
    {
        live.push(delivery);
    }
    (store, live)
}

/// The `assistant_message` control units of `deliveries`, each as its cursor and unit serialize.
fn assistant_units(deliveries: &[crate::Delivery]) -> Vec<Value> {
    deliveries
        .iter()
        .filter_map(|delivery| match delivery {
            crate::Delivery::Control(event)
                if matches!(
                    event.1,
                    broca_protocol::caller::ControlUnit::AssistantMessage { .. }
                ) =>
            {
                Some(json!({
                    "cursor": broca_wire::CursorWire::from(event.0),
                    "unit": event.1,
                }))
            }
            _ => None,
        })
        .collect()
}

/// Every llm-runner exposure of a store session's replies serves a reply whose entry facts say
/// its run cleans replies cleaned of part-number notation, and the same cleaned text on each:
/// `session.read` in every media mode, `session.history` raw and display, and `run.result`,
/// each equal to the text the compaction entry for that message carries. A served digest is the
/// cleaned body's. The stored message keeps what the model wrote, and the next request renders
/// it with exactly one number in front. The control lane's `assistant_message` unit carries the
/// stored text, live and replayed alike.
#[tokio::test]
async fn every_exposure_serves_a_cleaned_reply_and_the_store_keeps_the_written_one() {
    use crate::serve::{ServeHandler, SessionReadRequest, SubscribeFrom};
    use cortexkit_role_llm_runner::read::MediaMode;
    let (store, live) = served_session().await;
    let cleaned: Vec<String> = WRITTEN
        .iter()
        .map(|text| broca_protocol::tags::exposed_model_text(true, text).into_owned())
        .collect();
    assert_eq!(cleaned[0], "Done.");
    assert_eq!(cleaned[2], "It says file contents.");
    assert!(!cleaned[3].contains('§'), "{}", cleaned[3]);
    let expected: Vec<Vec<String>> = cleaned.iter().map(|text| vec![text.clone()]).collect();

    // What the store keeps: the text the model wrote, under a run whose facts say it cleans.
    let session = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    let entries = session.entries(1, 64).await.unwrap();
    let replies: Vec<_> = entries
        .iter()
        .filter(|entry| entry.message.role == broca_protocol::Role::Assistant)
        .collect();
    let stored: Vec<Vec<String>> = replies
        .iter()
        .map(|entry| {
            entry
                .message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect()
        })
        .collect();
    assert_eq!(
        stored,
        WRITTEN.map(|text| vec![text.to_owned()]).to_vec(),
        "the store keeps the written replies"
    );
    assert!(replies
        .iter()
        .all(|entry| entry.state.facts.as_ref().unwrap().clean_reply));

    // `session.read`, in every media mode, and its digests.
    for media in [
        MediaMode::Inline,
        MediaMode::Blocks,
        MediaMode::MediaRefs,
        MediaMode::Descriptors,
    ] {
        let mut request = SessionReadRequest::new(store.key.clone(), Some(1), None);
        request.media = media;
        let read = store.registry.read(request).await.unwrap();
        let read = serde_json::to_value(&read).unwrap();
        let messages = read["messages"].as_array().unwrap();
        if media == MediaMode::Descriptors {
            let digests: Vec<&Value> = messages
                .iter()
                .filter(|message| message["message"]["role"] == "assistant")
                .map(|message| &message["digest"])
                .collect();
            for (entry, digest) in replies.iter().zip(digests) {
                let mut written = broca_protocol::Message::clone(&entry.message);
                written.ordinal = Some(entry.ordinal - 1);
                let served = crate::store_exposure::served_message(&written, true).into_owned();
                let identity = |message: &broca_protocol::Message| {
                    json!(broca_protocol::caller_message::message_content_identity(message).digest)
                };
                assert_eq!(digest, &identity(&served), "message {}", entry.ordinal);
                if served.content != written.content {
                    assert_ne!(digest, &identity(&written), "message {}", entry.ordinal);
                }
            }
        } else {
            assert_eq!(
                assistant_texts(messages),
                expected,
                "session.read {media:?}"
            );
        }
    }

    // `session.history`, raw and display.
    for form in ["raw", "display"] {
        let history = store
            .history(json!({"form": form, "from_ordinal": 1}))
            .await
            .unwrap();
        assert_eq!(
            assistant_texts(history["messages"].as_array().unwrap()),
            expected,
            "session.history {form}: {history}"
        );
    }

    // `run.result`: each run's final reply.
    for (run, at) in [("r1", 0), ("r2", 2), ("r3", 3)] {
        let result = store
            .registry
            .run_result(&store.key, RunId(run.into()))
            .await
            .unwrap();
        assert_eq!(
            result.final_message.map(|message| message.text),
            Some(cleaned[at].clone()),
            "run.result {run}"
        );
    }

    // The compaction entries carry the same text, message by message.
    let page = store
        .compaction(
            Some("ck-mc"),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await
        .unwrap();
    let by_mid = |messages: &[Value]| -> std::collections::BTreeMap<String, Vec<String>> {
        messages
            .iter()
            .filter_map(|message| {
                let texts = assistant_texts(std::slice::from_ref(message)).pop()?;
                Some((message["mid"].as_str()?.to_owned(), texts))
            })
            .collect()
    };
    let entries = by_mid(page["entries"].as_array().unwrap());
    let read = store
        .registry
        .read(SessionReadRequest::new(store.key.clone(), Some(1), None))
        .await
        .unwrap();
    let read = by_mid(
        serde_json::to_value(&read).unwrap()["messages"]
            .as_array()
            .unwrap(),
    );
    assert_eq!(read.len(), WRITTEN.len());
    for (mid, texts) in &read {
        assert_eq!(Some(texts), entries.get(mid), "session.entries {mid}");
    }

    // The next request renders the first reply with exactly one number in front. r2's first
    // step is the provider's second request.
    let prompt = serde_json::to_value(&store.provider.calls()[1].prompt).unwrap();
    let mut shown = Vec::new();
    let mut stack = vec![&prompt];
    while let Some(value) = stack.pop() {
        match value {
            Value::String(text) if text.contains("Done.") => shown.push(text.clone()),
            Value::Array(items) => stack.extend(items),
            Value::Object(members) => stack.extend(members.values()),
            _ => {}
        }
    }
    assert!(
        !shown.is_empty(),
        "the request shows the first reply: {prompt}"
    );
    for text in &shown {
        let number = text
            .strip_prefix('§')
            .and_then(|rest| rest.strip_suffix("§ Done."))
            .unwrap_or_else(|| panic!("one number in front of the reply: {text:?}"));
        assert!(
            !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()),
            "one number in front of the reply: {text:?}"
        );
    }

    // The control lane: a subscriber replaying the third run gets the units the live one got,
    // byte for byte, carrying the stored text.
    let live = assistant_units(&live);
    assert_eq!(live.len(), 1, "{live:?}");
    assert_eq!(
        live[0]["unit"]["message"]["content"][0]["text"],
        json!(WRITTEN[3])
    );
    let (sink, mut deliveries) = mpsc::channel(4096);
    store
        .registry
        .subscribe(&store.key, SubscribeFrom::Start, sink)
        .await
        .unwrap();
    let mut replayed = Vec::new();
    while let Ok(Some(delivery)) =
        tokio::time::timeout(Duration::from_millis(300), deliveries.recv()).await
    {
        replayed.push(delivery);
    }
    let replayed = assistant_units(&replayed);
    assert_eq!(
        replayed.last(),
        live.last(),
        "the replayed unit is the live one"
    );
    assert_eq!(
        serde_json::to_string(&replayed.last()).unwrap(),
        serde_json::to_string(&live.last()).unwrap()
    );
}

/// A reply whose entry facts say its run doesn't clean replies (a session no compaction
/// provider runs) is served byte for byte as stored, tags and all, on every exposure.
#[tokio::test]
async fn a_reply_its_run_doesnt_clean_is_served_as_stored() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let store = side(true, vec![text_step("a1", WRITTEN[0])], false, false).await;
    assert!(matches!(
        store.send_request("r1", untooled("first"), None).await,
        SendOutcome::Active { .. }
    ));
    store.settled("r1").await;
    store.assert_completed("r1").await;
    let written = vec![vec![WRITTEN[0].to_owned()]];
    let read = store
        .registry
        .read(SessionReadRequest::new(store.key.clone(), Some(1), None))
        .await
        .unwrap();
    let read = serde_json::to_value(&read).unwrap();
    assert_eq!(
        assistant_texts(read["messages"].as_array().unwrap()),
        written
    );
    for form in ["raw", "display"] {
        let history = store
            .history(json!({"form": form, "from_ordinal": 1}))
            .await
            .unwrap();
        assert_eq!(
            assistant_texts(history["messages"].as_array().unwrap()),
            written,
            "session.history {form}"
        );
    }
    let result = store
        .registry
        .run_result(&store.key, RunId("r1".into()))
        .await
        .unwrap();
    assert_eq!(
        result.final_message.map(|message| message.text),
        Some(WRITTEN[0].to_owned())
    );
}

/// The input folder of [`dump_head_import_entries`]: `HEAD_IMPORT_EXPORT_DIR`, else Thalamus's
/// first published exports.
fn head_import_input() -> std::path::PathBuf {
    std::env::var_os("HEAD_IMPORT_EXPORT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../ck-session-import/tests/fixtures/thalamus-export-v1")
        })
}

/// Checks every file `SHA256SUMS` in `folder` lists against its digest, and refuses on any
/// mismatch or missing file, before anything else in the folder is read.
fn check_sums(folder: &std::path::Path) {
    use sha2::Digest as _;
    let sums = std::fs::read_to_string(folder.join("SHA256SUMS"))
        .unwrap_or_else(|e| panic!("{}: no SHA256SUMS: {e}", folder.display()));
    for line in sums.lines().filter(|line| !line.trim().is_empty()) {
        let (digest, name) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("a SHA256SUMS line without a name: {line}"));
        let bytes = std::fs::read(folder.join(name))
            .unwrap_or_else(|e| panic!("{name}, listed in SHA256SUMS: {e}"));
        let actual = format!("{:x}", sha2::Sha256::digest(&bytes));
        assert_eq!(
            actual, digest,
            "{name} doesn't match SHA256SUMS: refusing the folder"
        );
    }
}

/// One row of the comparison of Thalamus's entries with broca's: a source message id, the
/// ordinal and part ids each side gives it.
#[derive(Default)]
struct Compared {
    thalamus_ordinal: Option<u64>,
    broca_ordinal: Option<u64>,
    thalamus_parts: Option<Vec<String>>,
    broca_parts: Option<Vec<String>>,
}

/// The ordinal and part ids of each entry of `session.entries` answers, by message id, in order.
fn entries_by_mid(pages: &[Value]) -> Vec<(String, u64, Vec<String>)> {
    pages
        .iter()
        .flat_map(|page| page["entries"].as_array().cloned().unwrap_or_default())
        .map(|entry| {
            (
                entry["mid"].as_str().unwrap_or_default().to_owned(),
                entry["ordinal"].as_u64().unwrap_or_default(),
                entry["parts"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|part| part["id"].as_str().unwrap_or("?").to_owned())
                    .collect(),
            )
        })
        .collect()
}

/// A plain table of every source message id: Thalamus's entry ordinal and broca's, the part ids
/// on each side, then any entry broca adds (one whose id no source row has).
fn comparison(
    reply: &broca_wire::head_import::FileImportReply,
    thalamus: &[(String, u64, Vec<String>)],
    broca: &[(String, u64, Vec<String>)],
    broca_refused: Option<&str>,
) -> String {
    let mut rows: Vec<(String, Compared)> = Vec::new();
    fn row(rows: &mut Vec<(String, Compared)>, mid: &str) -> usize {
        match rows.iter().position(|(known, _)| known == mid) {
            Some(at) => at,
            None => {
                rows.push((mid.to_owned(), Compared::default()));
                rows.len() - 1
            }
        }
    }
    // A row for a message broca added names no source message: it is listed with the
    // entries broca adds.
    for moved in reply.moves.rows.iter().filter(|moved| !moved.added) {
        let at = row(&mut rows, &moved.mid);
        rows[at].1.broca_ordinal = moved.entry;
    }
    for (mid, ordinal, parts) in thalamus {
        let at = row(&mut rows, mid);
        rows[at].1.thalamus_ordinal = Some(*ordinal);
        rows[at].1.thalamus_parts = Some(parts.clone());
    }
    let sources = rows.len();
    let mut added = Vec::new();
    for (mid, ordinal, parts) in broca {
        match rows[..sources].iter().position(|(known, _)| known == mid) {
            Some(at) => {
                rows[at].1.broca_ordinal = Some(*ordinal);
                rows[at].1.broca_parts = Some(parts.clone());
            }
            None => added.push((mid.clone(), *ordinal, parts.clone())),
        }
    }
    let show = |value: &Option<u64>| value.map_or("-".to_owned(), |value| value.to_string());
    let parts = |value: &Option<Vec<String>>| match value {
        None => "-".to_owned(),
        Some(parts) if parts.is_empty() => "(none)".to_owned(),
        Some(parts) => parts.join(" "),
    };
    let mut out = String::new();
    if let Some(refused) = broca_refused {
        out.push_str(&format!(
            "broca's compaction route refused session.entries: {refused}. broca's columns \
             come from the import reply's move list and the `entries-unadmitted-<n>.json` \
             pages: not route bytes, the page the route serves once the session's plan names \
             the compaction provider.\n\n"
        ));
    }
    out.push_str(
        "| source mid | thalamus ordinal | broca ordinal | thalamus part ids | broca part ids |\n\
         |---|---|---|---|---|\n",
    );
    for (mid, compared) in &rows {
        out.push_str(&format!(
            "| {mid} | {} | {} | {} | {} |\n",
            show(&compared.thalamus_ordinal),
            show(&compared.broca_ordinal),
            parts(&compared.thalamus_parts),
            parts(&compared.broca_parts),
        ));
    }
    out.push_str("\nEntries broca adds (no source mid):\n\n");
    if added.is_empty() {
        out.push_str("(none)\n");
    } else {
        out.push_str(
            "| broca mid | broca ordinal | broca part ids | move list row |\n|---|---|---|---|\n",
        );
        for (mid, ordinal, part_ids) in &added {
            let listed = reply
                .moves
                .rows
                .iter()
                .any(|moved| moved.added && &moved.mid == mid && moved.entry == Some(*ordinal));
            out.push_str(&format!(
                "| {mid} | {ordinal} | {} | {} |\n",
                parts(&Some(part_ids.clone())),
                if listed { "added" } else { "none" }
            ));
        }
    }
    out
}

/// For each host folder (`opencode2`, `pi`) of a Thalamus export folder, imports the export
/// through the module's real file import into a session in the new storage, then reads every
/// page of `session.entries` through the module's `compaction-runner/v1` route, as the
/// compaction provider `magic-context` would. Writes, under `HEAD_IMPORT_OUT_DIR/<host>/`:
/// - `import-reply.json`, the import's reply, move list included;
/// - `entries-<n>.json`, each page exactly as the route returns it, or, when the route refuses,
///   `entries-1.refused.json` with its refusal and `entries-unadmitted-<n>.json`, the pages the
///   route's own page function builds, which it serves once the session's plan names the
///   compaction provider;
/// - `comparison.md`, when the host folder holds Thalamus's own `entries-1.json`;
/// - `README.md`, saying what each file is, and `SHA256SUMS` over them all.
///
/// The input folder's `SHA256SUMS` is checked before anything else is read. Run by hand:
///
/// ```text
/// HEAD_IMPORT_EXPORT_DIR=<export folder> HEAD_IMPORT_OUT_DIR=<output folder> \
///   cargo nextest run -p broca-module --run-ignored only \
///   -E 'test(=store_actor::entries_comparison::dump_head_import_entries)'
/// ```
#[tokio::test]
#[ignore = "writes files: run by hand with HEAD_IMPORT_EXPORT_DIR and HEAD_IMPORT_OUT_DIR"]
async fn dump_head_import_entries() {
    use broca_wire::head_import::{FileImportParams, ImportHarness, ImportOrigin, ImportSource};
    use sha2::Digest as _;
    let input = head_import_input();
    let out = std::path::PathBuf::from(
        std::env::var_os("HEAD_IMPORT_OUT_DIR").expect("HEAD_IMPORT_OUT_DIR names the output"),
    );
    check_sums(&input);
    for (host, journal, harness) in [
        ("opencode2", "export.json", ImportHarness::OpenCode2),
        ("pi", "export.jsonl", ImportHarness::Pi),
    ] {
        let folder = input.join(host);
        if !folder.exists() {
            continue;
        }
        let export = std::fs::read(folder.join(journal)).unwrap();
        // The session id the export names: OpenCode's `info.id`, Pi's header line's `id`.
        let session_id = match harness {
            ImportHarness::OpenCode2 => serde_json::from_slice::<Value>(&export).unwrap()["info"]
                ["id"]
                .as_str()
                .unwrap()
                .to_owned(),
            _ => serde_json::from_str::<Value>(
                std::str::from_utf8(&export)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap(),
            )
            .unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned(),
        };
        let thalamus_reply: Value =
            serde_json::from_slice(&std::fs::read(folder.join("reply.json")).unwrap()).unwrap();
        let side = side(true, Vec::new(), false, false).await;
        // The export and its side file, in a folder the import may read and then delete.
        let source = side.dir.join("import-source");
        std::fs::create_dir_all(&source).unwrap();
        let path = source.join(format!("head.{}", journal.rsplit('.').next().unwrap()));
        std::fs::write(&path, &export).unwrap();
        if let Ok(sends) = std::fs::read(folder.join("export.sends.json")) {
            let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
            std::fs::write(source.join(format!("{stem}.sends.json")), sends).unwrap();
        }
        let command = crate::store_import::StoreCommand::Import(Box::new(
            crate::store_import::ImportCommand {
                params: FileImportParams {
                    import_id: format!("imp-{host}"),
                    source: ImportSource {
                        harness,
                        path: path.display().to_string(),
                        session: session_id.clone(),
                        sha256: format!("{:x}", sha2::Sha256::digest(&export)),
                    },
                    origin: ImportOrigin {
                        kind: "import".into(),
                        source_ref: format!("thalamus:{session_id}"),
                    },
                    tag_floor: thalamus_reply["tag_floor"].as_u64(),
                    retention: None,
                    moves_from: None,
                },
                principal: broca_protocol::BindPrincipal::Reserved {
                    module_id: "prefrontal-core".into(),
                },
                // The import refuses a route without a scope; Prefrontal sends one on the scope
                // of the agent whose head it moves.
                scope: Some(broca_protocol::SessionScope {
                    owner: broca_protocol::BindPrincipal::Reserved {
                        module_id: "prefrontal-core".into(),
                    },
                    scope_ref: "agent:a1".into(),
                    scope_epoch: 1,
                    flow_id: None,
                    sender: None,
                    agent_id: None,
                    run_id: None,
                    agent_id_frozen: false,
                }),
                roots: Arc::new(crate::store_import::ImportRoots::new(
                    vec![source.clone()],
                    Some(source.clone()),
                )),
                now_ms: 9_000,
            },
        ));
        let reply = match side
            .registry
            .route_store_command(&side.key, command)
            .await
            .unwrap()
        {
            crate::store_import::StoreOutcome::Imported(reply) => reply,
            other => panic!("{host}: the import was refused: {other:?}"),
        };
        let written = out.join(host);
        let _ = std::fs::remove_dir_all(&written);
        std::fs::create_dir_all(&written).unwrap();
        let mut files = vec![(
            "import-reply.json".to_owned(),
            serde_json::to_vec_pretty(&reply).unwrap(),
        )];
        // Every page, from the first entry on, as Thalamus's own recording reads them.
        let mut pages = Vec::new();
        let mut refused = None;
        let mut after: Option<u64> = None;
        for page in 1.. {
            let mut params = json!({"select": {"by": "after"}, "detail": "full"});
            if let Some(after) = after {
                params["select"]["after_ordinal"] = json!(after);
            }
            match side
                .registry
                .route_compaction_v2(
                    &side.key,
                    Some("magic-context".into()),
                    "session.entries".into(),
                    serde_json::value::to_raw_value(&params).unwrap(),
                )
                .await
            {
                Ok(served) => {
                    files.push((
                        format!("entries-{page}.json"),
                        served.get().as_bytes().to_vec(),
                    ));
                    let served: Value = serde_json::from_str(served.get()).unwrap();
                    let next = served["next_after"].as_u64();
                    pages.push(served);
                    match next {
                        Some(next) => after = Some(next),
                        None => break,
                    }
                }
                Err(refusal) => {
                    let refusal = serde_json::to_value(&refusal).unwrap();
                    refused = Some(format!(
                        "{} ({})",
                        refusal["code"].as_str().unwrap_or("?"),
                        refusal["detail"]["reason"].as_str().unwrap_or("no reason")
                    ));
                    files.push((
                        format!("entries-{page}.refused.json"),
                        serde_json::to_vec(&refusal).unwrap(),
                    ));
                    break;
                }
            }
        }
        if refused.is_some() {
            // The pages the route's own page function builds for the session, which the route
            // serves once the session's plan names the compaction provider.
            let session = side
                .registry
                .store_runtime()
                .unwrap()
                .current_view(&side.key)
                .await
                .unwrap()
                .expect("the import created the session");
            let mut after: Option<u64> = None;
            for page in 1.. {
                let request: ReadRequest = serde_json::from_value(json!({
                    "select": {"by": "after", "after_ordinal": after},
                    "detail": "full",
                }))
                .unwrap();
                let built = crate::store_entries::entries_page(&session, &request)
                    .await
                    .unwrap();
                let bytes = serde_json::to_vec(&built).unwrap();
                files.push((format!("entries-unadmitted-{page}.json"), bytes.clone()));
                let built: Value = serde_json::from_slice(&bytes).unwrap();
                let next = built["next_after"].as_u64();
                pages.push(built);
                match next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        let mut readme = format!(
            "What broca serves for Thalamus's `{host}` export after importing it into a session \
             in the new storage, written by `dump_head_import_entries` in \
             `crates/broca-module/src/store_actor/entries_comparison.rs`.\n\n\
             - `import-reply.json`: the `session.import` reply, move list included.\n"
        );
        match &refused {
            None => readme.push_str(
                "- `entries-<n>.json`: each `session.entries` page exactly as the \
                 `compaction-runner/v1` route returned it to `magic-context`, from the first \
                 entry on in full detail, following `next_after`.\n",
            ),
            Some(refused) => readme.push_str(&format!(
                "- `entries-1.refused.json`: the `compaction-runner/v1` route's refusal of \
                 `session.entries` to `magic-context`, exactly as returned: {refused}. This is \
                 expected: a moved head gets its plan, naming its compaction provider, from \
                 core's adopt after the import, and until then the route has no compaction \
                 provider to admit.\n\
                 - `entries-unadmitted-<n>.json`: not route bytes: the page the route serves \
                 once the session's plan names the compaction provider, built by the route's \
                 own page function (`store_entries::entries_page`), from the first entry on in \
                 full detail, following `next_after`.\n"
            )),
        }
        readme.push_str(
            "- `comparison.md`, when the export folder holds Thalamus's own `entries-<n>.json`: \
             one row per source message id with each side's entry ordinal and part ids, then \
             any entry broca adds.\n\
             - `SHA256SUMS`: every file above.\n",
        );
        files.push(("README.md".into(), readme.into_bytes()));
        if let Ok(theirs) = std::fs::read(folder.join("entries-1.json")) {
            let mut thalamus = vec![serde_json::from_slice::<Value>(&theirs).unwrap()];
            for page in 2.. {
                match std::fs::read(folder.join(format!("entries-{page}.json"))) {
                    Ok(bytes) => thalamus.push(serde_json::from_slice(&bytes).unwrap()),
                    Err(_) => break,
                }
            }
            files.push((
                "comparison.md".into(),
                comparison(
                    &reply,
                    &entries_by_mid(&thalamus),
                    &entries_by_mid(&pages),
                    refused.as_deref(),
                )
                .into_bytes(),
            ));
        }
        let mut sums = String::new();
        for (name, bytes) in &files {
            std::fs::write(written.join(name), bytes).unwrap();
            sums.push_str(&format!("{:x}  {name}\n", sha2::Sha256::digest(bytes)));
        }
        std::fs::write(written.join("SHA256SUMS"), sums).unwrap();
    }
}

/// A view the compaction provider applies while the session is idle between runs is accepted
/// alike by both storages and answered alike on a retry, a view naming a message the session
/// doesn't hold is refused alike, and the next run's first request renders the accepted view
/// alike. The store session stays open from the apply to that run, so the view reaches it
/// through the compaction state the actor keeps, not a reload.
#[tokio::test]
async fn an_idle_apply_view_reaches_the_next_runs_first_step_as_today() {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    let (today, store) = contract_sessions_with(
        false,
        vec![text_step("a4", "after the view")],
        Duration::from_secs(60),
    )
    .await;
    let drop = |version: u64, mid: String| {
        View::seal(
            version,
            vec![Edit::new(version, EditOp::Drop { mid, part: None })],
        )
    };
    let mut answers = Vec::new();
    for side in [&today, &store] {
        let apply = |view: View| {
            side.compaction(Some("ck-mc"), "session.apply_view", json!({ "view": view }))
        };
        // The first run's reply, entry 2.
        let reply = entry_mids(side).await[&2].clone();
        let first = apply(drop(1, reply.clone())).await;
        let retry = apply(drop(1, reply)).await;
        let unknown = apply(drop(2, "no-such-message".into())).await;
        answers.push((first, retry, unknown));
    }
    alike!(answers[1], answers[0], "the store answers as today's");
    assert_eq!(answers[0].0, Ok(json!({"answer": "accepted"})));
    for refused in [&answers[0].1, &answers[0].2] {
        assert!(
            matches!(refused, Ok(answer) if answer["answer"] == "refused"),
            "{refused:?}"
        );
    }
    for side in [&today, &store] {
        assert!(matches!(
            side.send_request("r3", untooled("again"), Some(planned("plan-1", None)))
                .await,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ));
        side.settled("r3").await;
        side.assert_completed("r3").await;
    }
    // r3's first step is the provider's fourth request.
    let prompt = |side: &Side| serde_json::to_value(&side.provider.calls()[3].prompt).unwrap();
    alike!(prompt(&store), prompt(&today));
    assert!(
        !prompt(&store).to_string().contains("Done."),
        "the view dropped the first reply: {}",
        prompt(&store)
    );
    let session = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    let applied: Vec<u64> = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            broca_session_store::record::Record::ViewApplied(applied) => Some(applied.version),
            _ => None,
        })
        .collect();
    assert_eq!(applied, [1], "r3's first step applied the view");
}

/// One of core's sends to a new head, as core serializes it for transport: its `session.send`
/// params. Core's runner-head sample (prefrontal-core's
/// `write_broca_head_first_send_sample`, at core commit 83d9e536) gave them, with synthetic
/// names: the creating send, carrying the head's fetch plan and `session_class: "head"`, and the
/// next send, carrying neither. Core retries the creating send byte for byte. The creating send
/// here leaves out the sample's `plan.cache`: core adds it only for a runner whose describe
/// declares `cache_policy`, which broca's doesn't.
fn core_head_send(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/core-head-send")
        .join(name);
    let body: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(body["method"], "session.send");
    body["params"].clone()
}

/// The request the serve layer hands the registry for core's send `params` on core's head
/// route, which broca's wire decoder must take: the prompt as the user's message, marked a
/// notice, sent by core over its head scope. A send creating the session runs the model its
/// plan selects; a later one names none, and inherits the session's.
fn core_head_request(params: &Value, creating: bool) -> SubmissionRequest {
    let decoded: broca_wire::SendParams =
        serde_json::from_value(params.clone()).expect("broca decodes core's send");
    let model = if creating {
        let model = &params["plan"]["model"];
        json!({"provider_module_id": model["provider"], "model_id": model["model"]})
    } else {
        json!({"provider_module_id": "", "model_id": ""})
    };
    let mut sent: SubmissionRequest = serde_json::from_value(json!({
        "model": model,
        "input": [{"role": "user", "content": [{"type": "text", "text": decoded.prompt}]}],
    }))
    .unwrap();
    if creating {
        sent.variant = params["plan"]["model"]["variant"]
            .as_str()
            .map(str::to_owned);
    }
    sent.inherit_frozen = [broca_protocol::request::InheritedParam::Model].into();
    sent.message_marks = [("notice".to_owned(), params["notice"].clone())]
        .into_iter()
        .collect();
    let core = broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    };
    sent.principal = Some(core.clone());
    // The head stamp core's sample route carries.
    sent.scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: core,
            agent_id_frozen: false,
            scope_ref: "d6f57d11448da25d6bba5edb0d2fd941".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    sent
}

/// What admitting core's creating send fetches, as broca's plan admission freezes it: the plan
/// as sent, which broca's plan reader must take, under its identity, with stand-in tools and
/// system text in place of what the plan's providers would answer, and the `pre_user`
/// subscription Magic Context declares for its head transform.
fn core_head_plan(params: &Value) -> crate::actor::SendPlan {
    let plan = &params["plan"];
    let parsed = broca_fetch_plan::parse_plan_with(
        plan,
        broca_fetch_plan::CompactionSupport::Supported,
        broca_fetch_plan::OutputRefsSupport::Unsupported,
    )
    .expect("broca's plan reader takes core's plan");
    let subscriptions = plan["step_transform_items"][0]["subscriptions"].clone();
    let manifest = broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: parsed.identity.clone(),
        composition_digest: parsed.composition_digest.clone(),
        tools: Arc::new(vec![serde_json::from_value(json!({
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }))
        .unwrap()]),
        system_text: "You are the head of a synthetic agent.".into(),
        system_digest: "e".repeat(64),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![broca_wal::FrozenDeclaration {
            provider: "magic-context".into(),
            answer: broca_wal::OpaqueJson::from_json_text(
                json!({ "subscriptions": subscriptions }).to_string(),
            )
            .unwrap(),
        }],
    };
    planned(&parsed.identity, Some(manifest))
}

/// Sends core's `request` to `side` as the serve layer routes a keyed `session.send`, under
/// `send_id`, with `plan` and the label `class`.
async fn send_keyed(
    side: &Side,
    send_id: &str,
    request: SubmissionRequest,
    plan: Option<crate::actor::SendPlan>,
    class: Option<&str>,
) -> SendOutcome {
    side.registry
        .route_submission_with_order(
            &side.key,
            RunId(format!("run-{send_id}")),
            request,
            RunKind::FreshOrResume,
            SendKey::SendId(send_id.into()),
            false,
            broca_protocol::Delivery::Queue,
            plan,
            None,
            None,
            None,
            class.map(str::to_owned),
            false,
            None,
        )
        .await
        .expect("the session opens")
}

/// Core's reader-bearing creating send starts a head in broca-session-store's durable root,
/// even when the module creates no other session there. Legacy WAL storage refuses that plan
/// without writing; the equivalent plan omitting readers still starts in its durable folder.
/// Both admitted heads use compaction-provider/v2. Core's keyed retry writes nothing, and its
/// next send runs as the next turn in the same storage. A view the synthetic compaction
/// provider submits while the head is idle is accepted on both storages and applies at the
/// next turn's first step; the two histories read the same.
#[tokio::test]
async fn cores_creating_send_starts_a_head_in_the_new_storage_as_today() {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    use broca_wal::StorageClass;
    let first = core_head_send("first-send.json");
    let second = core_head_send("second-send.json");
    let first_id = first["send_id"].as_str().unwrap().to_owned();
    let second_id = second["send_id"].as_str().unwrap().to_owned();
    let class = first["session_class"].as_str();
    assert_eq!(class, Some(crate::registry::HEAD_SESSION_CLASS));
    let steps = vec![
        text_step("a1", "Awake."),
        text_step("a2", "Continuing."),
        text_step("a3", "after the view"),
    ];
    let side = |runtime: bool| {
        let steps = steps.clone();
        async move {
            let build = Build {
                runtime,
                new_sessions: false,
                gated: false,
                compaction: true,
                hooks: true,
                idle: Duration::from_secs(60),
            };
            let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]);
            let mut side = side_with(build, steps, deployment).await;
            side.key = BindIdentity::new(
                side.dir.join("project"),
                "broca",
                "alfonso:head:agent_0123456789abcdef",
            );
            side
        }
    };
    let today = side(false).await;
    let store = side(true).await;
    let refused = send_keyed(
        &today,
        &first_id,
        core_head_request(&first, true),
        Some(core_head_plan(&first)),
        class,
    )
    .await;
    assert!(
        matches!(
            refused,
            SendOutcome::DeliveryRefused {
                code: "invalid_params",
                field: "plan.readers",
                ..
            }
        ),
        "{refused:?}"
    );
    assert!(today.registry.slot_state(&today.key).is_none());
    assert!(
        !StorageClass::ALL.into_iter().any(|class| {
            FileWalStore::new(today.dir.join("wal"))
                .class_wal_path(class, &today.key)
                .exists()
        }),
        "the refused reader plan created no legacy WAL"
    );
    let mut legacy_first = first.clone();
    assert!(legacy_first["plan"]
        .as_object_mut()
        .unwrap()
        .remove("readers")
        .is_some());
    let mut replies = Vec::new();
    for (side, first) in [(&today, &legacy_first), (&store, &first)] {
        let reply = send_keyed(
            side,
            &first_id,
            core_head_request(first, true),
            Some(core_head_plan(first)),
            class,
        )
        .await;
        assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
        side.settled(&format!("run-{first_id}")).await;
        side.assert_completed(&format!("run-{first_id}")).await;
        replies.push(reply);
    }
    alike!(replies[1], replies[0]);
    let address = |side: &Side| broca_core::session_addr(&side.key);
    let wal_folder = |side: &Side| {
        let wal = FileWalStore::new(side.dir.join("wal"));
        StorageClass::ALL
            .into_iter()
            .find(|class| wal.class_wal_path(*class, &side.key).is_file())
    };
    assert_eq!(
        broca_wal::session_storage_class(&store.dir, &address(&store)).unwrap(),
        Some(StorageClass::Durable)
    );
    assert_eq!(wal_folder(&store), None);
    assert_eq!(
        broca_wal::session_storage_class(&today.dir, &address(&today)).unwrap(),
        None
    );
    assert_eq!(wal_folder(&today), Some(StorageClass::Durable));
    let current = || async {
        store
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&store.key)
            .await
            .unwrap()
            .unwrap()
    };
    let view = current().await;
    assert!(crate::store_entries::on_contract(&view));

    // Core's retry of the creating send, byte for byte, once the session holds its plan: the
    // serve layer then fetches nothing and hands over the plan's identity alone.
    let written = current().await.open_log_records().await.unwrap().len();
    let mut retries = Vec::new();
    for (side, first) in [(&today, &legacy_first), (&store, &first)] {
        let identity = core_head_plan(first).identity;
        retries.push(
            send_keyed(
                side,
                &first_id,
                core_head_request(first, false),
                Some(planned(&identity, None)),
                class,
            )
            .await,
        );
    }
    alike!(
        retries[1],
        retries[0],
        "the store answers the retry as today's"
    );
    assert!(
        !matches!(
            retries[1],
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "the retry started nothing: {:?}",
        retries[1]
    );
    assert_eq!(
        current().await.open_log_records().await.unwrap().len(),
        written,
        "the retry wrote nothing"
    );

    for side in [&today, &store] {
        let next = send_keyed(
            side,
            &second_id,
            core_head_request(&second, false),
            None,
            None,
        )
        .await;
        assert!(
            matches!(
                next,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{next:?}"
        );
        side.settled(&format!("run-{second_id}")).await;
        side.assert_completed(&format!("run-{second_id}")).await;
    }
    assert_eq!(
        wal_folder(&store),
        None,
        "the next turn ran in the new storage"
    );

    // Magic Context drops the first turn's reply, entry 2, while the head is idle; the turn
    // after applies the view at its first step.
    let mut answers = Vec::new();
    for side in [&today, &store] {
        let page = side
            .compaction(
                Some("magic-context"),
                "session.entries",
                json!({"select": {"by": "after"}, "detail": "compact"}),
            )
            .await
            .unwrap();
        let reply = page["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["ordinal"] == 2)
            .expect("the first turn's reply")["mid"]
            .as_str()
            .unwrap()
            .to_owned();
        let view = View::seal(
            1,
            vec![Edit::new(
                1,
                EditOp::Drop {
                    mid: reply,
                    part: None,
                },
            )],
        );
        answers.push(
            side.compaction(
                Some("magic-context"),
                "session.apply_view",
                json!({ "view": view }),
            )
            .await,
        );
    }
    assert_eq!(answers[0], Ok(json!({"answer": "accepted"})));
    alike!(answers[1], answers[0], "the store answers as today's");
    let mut third = second.clone();
    third["send_id"] = json!("send_sample_third_0123456789abcdef");
    third["prompt"] = json!("Carry on with the synthetic project.");
    for side in [&today, &store] {
        let next = send_keyed(
            side,
            "send_sample_third_0123456789abcdef",
            core_head_request(&third, false),
            None,
            None,
        )
        .await;
        assert!(
            matches!(
                next,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{next:?}"
        );
        side.settled("run-send_sample_third_0123456789abcdef").await;
        side.assert_completed("run-send_sample_third_0123456789abcdef")
            .await;
    }
    // The third turn's first step is the provider's third request.
    let prompt = |side: &Side| serde_json::to_value(&side.provider.calls()[2].prompt).unwrap();
    alike!(prompt(&store), prompt(&today));
    assert!(
        !prompt(&store).to_string().contains("Awake."),
        "the view dropped the first reply: {}",
        prompt(&store)
    );
    let applied: Vec<u64> = current()
        .await
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            broca_session_store::record::Record::ViewApplied(applied) => Some(applied.version),
            _ => None,
        })
        .collect();
    assert_eq!(applied, [1], "the third turn's first step applied the view");
    compare_history(&today, &store, "a head's three turns").await;
}

/// A view the compaction provider applies with its build metadata (the entry it was built
/// through and its policy for newly signed blocks, both covered by its checksum) is recorded
/// alike by both storages, with the provider that applied it. Once the next run's first step
/// applies it, that step's request renders it alike, and `session.applied_view` serves the same
/// view and names the same provider.
mod a_view_keeps_its_build_metadata_and_applier_alike {
    use broca_session_store::keys::Cipher;

    #[tokio::test]
    async fn encrypted() {
        super::a_view_keeps_its_build_metadata_and_applier_alike(Cipher::XChaCha20Poly1305).await;
    }

    #[tokio::test]
    async fn plain() {
        super::a_view_keeps_its_build_metadata_and_applier_alike(Cipher::None).await;
    }
}

async fn a_view_keeps_its_build_metadata_and_applier_alike(
    cipher: broca_session_store::keys::Cipher,
) {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    use broca_core::WalStore as _;
    use std::collections::BTreeMap;
    let (today, store) = contract_sessions_in(
        false,
        vec![text_step("a4", "after the view")],
        Duration::from_secs(60),
        || broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
    )
    .await;
    // Each storage mints its own message ids, so what names one is compared by the ordinal of
    // the entry it names; the checksum covers the ids, so it is kept as whether present.
    fn by_ordinal(value: &mut Value, mids: &BTreeMap<u64, String>) {
        match value {
            Value::String(text) => {
                if let Some((ordinal, _)) = mids.iter().find(|(_, mid)| *mid == text) {
                    *value = json!(format!("entry {ordinal}"));
                }
            }
            Value::Object(members) => {
                for (key, member) in members.iter_mut() {
                    if key == "checksum" {
                        *member = json!(member.is_string());
                    } else {
                        by_ordinal(member, mids);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| by_ordinal(item, mids)),
            _ => {}
        }
    }
    let mut ids = Vec::new();
    for side in [&today, &store] {
        let mids = entry_mids(side).await;
        // The view drops the first run's reply, entry 2, and was built through the newest entry.
        let newest = mids.values().next_back().unwrap().clone();
        let view = View::seal_with_policy(
            1,
            vec![Edit::new(
                1,
                EditOp::Drop {
                    mid: mids[&2].clone(),
                    part: None,
                },
            )],
            Some(newest),
            Some("strip".into()),
        );
        assert_eq!(
            side.compaction(Some("ck-mc"), "session.apply_view", json!({ "view": view }))
                .await,
            Ok(json!({"answer": "accepted"}))
        );
        ids.push(mids);
    }
    let old: Vec<Value> = FileWalStore::new(today.dir.join("wal"))
        .read_records(&today.key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            broca_wal::WalRecord::CompactionViewV2(view) => Some(json!({
                "version": view.version,
                "built_through": view.built_through,
                "new_signed": view.new_signed,
                "applied_by": view.applied_by,
            })),
            _ => None,
        })
        .collect();
    let session = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    let new: Vec<Value> = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            broca_session_store::record::Record::View(view) => Some(json!({
                "version": view.version,
                "built_through": view.built_through,
                "new_signed": view.new_signed,
                "applied_by": view.applied_by,
            })),
            _ => None,
        })
        .collect();
    let [mut old, mut new] = [Value::from(old), Value::from(new)];
    by_ordinal(&mut old, &ids[0]);
    by_ordinal(&mut new, &ids[1]);
    alike!(new, old, "both logs record the view alike");
    let through = format!("entry {}", ids[0].keys().next_back().unwrap());
    assert_eq!(
        old,
        json!([{"version": 1, "built_through": through, "new_signed": "strip",
                "applied_by": "ck-mc"}])
    );
    for side in [&today, &store] {
        assert!(matches!(
            side.send_request("r3", untooled("again"), Some(planned("plan-1", None)))
                .await,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ));
        side.settled("r3").await;
        side.assert_completed("r3").await;
    }
    // r3's first step is the provider's fourth request.
    let prompt = |side: &Side| serde_json::to_value(&side.provider.calls()[3].prompt).unwrap();
    alike!(prompt(&store), prompt(&today));
    let mut served = Vec::new();
    for (side, mids) in [&today, &store].into_iter().zip(&ids) {
        let mut reply = side
            .compaction(Some("ck-mc"), "session.applied_view", json!({}))
            .await
            .unwrap();
        by_ordinal(&mut reply, mids);
        served.push(reply);
    }
    alike!(
        served[1],
        served[0],
        "the store serves the applied view as today's"
    );
    assert_eq!(served[0]["applied_by"], json!("ck-mc"), "{}", served[0]);
    assert_eq!(
        served[0]["view"]["built_through"],
        json!(through),
        "{}",
        served[0]
    );
    assert_eq!(
        served[0]["view"]["new_signed"],
        json!("strip"),
        "{}",
        served[0]
    );
}

/// The template the plan gives the session's owner: a pointer at one part of the session.
fn pointer() -> cortexkit_role_llm_runner::delivery_order::Template {
    serde_json::from_value(json!({
        "id": "pointer",
        "text": "see {part}",
        "slots": {"part": {"type": "part_tag"}},
    }))
    .unwrap()
}

/// A send from the session's owner, which the plan names as a sender with [`pointer`].
fn owned(text: &str) -> SubmissionRequest {
    let mut request = untooled(text);
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    });
    request
}

/// The first part of the entry at `ordinal` that shows a number, as `session.entries` serves
/// it: the entry's message id, the part's id and its number.
async fn numbered_part(side: &Side, ordinal: u64) -> (String, String, u64) {
    let page = side
        .compaction(
            Some("ck-mc"),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await
        .unwrap();
    let entry = page["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["ordinal"] == ordinal)
        .unwrap_or_else(|| panic!("no entry {ordinal}: {page}"))
        .clone();
    let part = entry["parts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|part| part["tag"].is_u64())
        .unwrap_or_else(|| panic!("entry {ordinal} shows no number: {entry}"));
    (
        entry["mid"].as_str().unwrap().to_owned(),
        part["id"].as_str().unwrap().to_owned(),
        part["tag"].as_u64().unwrap(),
    )
}

/// A template send whose `part_tag` slot names a part of the session renders the number the
/// session's next request shows for it, on a session whose plan names a compaction provider and
/// renders part numbers: the same number, the same run input and request on either storage,
/// each naming the part by its own id. Once the provider's newest accepted view drops that
/// part's message, the next request no longer shows it, and the same send ends
/// `template_part_gone` alike, before any step applied the view. The store session stays open
/// throughout, so its numbers come from the compaction state its actor keeps.
mod a_part_tag_template_renders_the_number_the_next_request_shows_alike {
    use broca_session_store::keys::Cipher;

    #[tokio::test]
    async fn encrypted() {
        super::a_part_tag_template_renders_the_number_the_next_request_shows_alike(
            Cipher::XChaCha20Poly1305,
        )
        .await;
    }

    #[tokio::test]
    async fn plain() {
        super::a_part_tag_template_renders_the_number_the_next_request_shows_alike(Cipher::None)
            .await;
    }
}

async fn a_part_tag_template_renders_the_number_the_next_request_shows_alike(
    cipher: broca_session_store::keys::Cipher,
) {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    let template = pointer();
    let mut manifest = compacting_manifest(true);
    manifest.plan = broca_wal::OpaqueJson::from_json_text(
        json!({
            "compaction_item": {"provider": "ck-mc", "preset": "default"},
            "render_tags": true,
            "senders": [{"module": "prefrontal-core", "templates": [template]}],
        })
        .to_string(),
    )
    .unwrap();
    let steps = vec![
        text_step("a1", "§2§ Done."),
        tool_step("a2"),
        text_step("a3", "§5§ §6§ It says file contents."),
        text_step("a4", "seen"),
    ];
    let deployment = || broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let today = side_in(
        false,
        steps.clone(),
        false,
        true,
        Duration::from_secs(60),
        deployment(),
    )
    .await;
    let store = side_in(
        true,
        steps,
        false,
        true,
        Duration::from_secs(60),
        deployment(),
    )
    .await;
    for side in [&today, &store] {
        let first = side
            .send_request(
                "r1",
                owned("first"),
                Some(planned("plan-1", Some(manifest.clone()))),
            )
            .await;
        assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
        side.settled("r1").await;
        side.assert_completed("r1").await;
        let second = side
            .send_request("r2", owned("read a"), Some(planned("plan-1", None)))
            .await;
        assert!(
            matches!(
                second,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{second:?}"
        );
        side.settled("r2").await;
        side.assert_completed("r2").await;
    }
    let digest = cortexkit_role_llm_runner::delivery_order::template_digest(&template);
    let order = |mid: &str, part: &str| {
        json!({
            "modes": ["idle_start"],
            "template": {"id": "pointer", "digest": digest, "slots": {"part": {"mid": mid, "part": part}}},
        })
    };
    let mut named = Vec::new();
    for side in [&today, &store] {
        // The first run's reply, entry 2.
        let (mid, part, number) = numbered_part(side, 2).await;
        let mut request = owned("");
        request.input = Vec::new();
        let outcome = side.send_ordered("t1", request, order(&mid, &part)).await;
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        side.settled("t1").await;
        side.assert_completed("t1").await;
        named.push((mid, part, number));
    }
    alike!(
        named[1].2,
        named[0].2,
        "both storages number the part alike"
    );
    // t1's first step is the provider's fourth request; its input is the rendered template.
    let prompt = |side: &Side| serde_json::to_value(&side.provider.calls()[3].prompt).unwrap();
    alike!(prompt(&store), prompt(&today));
    let input = prompt(&store)
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .cloned()
        .unwrap();
    assert!(
        input.to_string().contains(&format!("see {}", named[0].2))
            || input.to_string().contains(&format!("§{}§", named[0].2)),
        "the run's input names part {}: {input}",
        named[0].2
    );
    let mut gone = Vec::new();
    for (side, (mid, part, _)) in [&today, &store].into_iter().zip(&named) {
        let view = View::seal(
            1,
            vec![Edit::new(
                1,
                EditOp::Drop {
                    mid: mid.clone(),
                    part: None,
                },
            )],
        );
        let applied = side
            .compaction(Some("ck-mc"), "session.apply_view", json!({ "view": view }))
            .await;
        assert_eq!(applied, Ok(json!({"answer": "accepted"})));
        let mut request = owned("");
        request.input = Vec::new();
        gone.push(side.send_ordered("t2", request, order(mid, part)).await);
    }
    alike!(gone[1], gone[0], "the store answers as today's");
    assert_eq!(
        gone[0],
        SendOutcome::TemplatePartGone {
            slot: "part".into()
        }
    );
}

/// `session.tag_map` over the sessions these scenarios build.
mod tag_map_tests;

#[path = "entries_comparison/message_ids.rs"]
mod message_ids;

#[path = "entries_comparison/model_fallback.rs"]
mod model_fallback;
