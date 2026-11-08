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
/// starts under the current compaction contract with a context limit.
struct Loops {
    today: Gated,
    wal: Arc<FileWalStore>,
    store: Gated,
    compaction: Option<Arc<ReadyProvider>>,
}

impl Loops {
    fn compacts(&self, control: &broca_core::RunControl) -> bool {
        self.compaction.is_some()
            && control.manifest.as_deref().is_some_and(|manifest| {
                matches!(broca_core::compaction::item(manifest), Ok(Some(_)))
            })
    }
}

#[async_trait]
impl RunExecutor for Loops {
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
                hook_plane: None,
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
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-entries-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let gate = gated.then(|| Arc::new(Notify::new()));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let provider = broca_core::provider::StubProvider::new(steps.clone());
    let scripted = || Gated {
        inner: provider.clone(),
        gate: gate.clone(),
    };
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        today: scripted(),
        wal: Arc::clone(&wal),
        store: scripted(),
        compaction: compaction.then(|| Arc::new(ReadyProvider)),
    });
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor),
        idle,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(idle)
            .with_store_runtime(Arc::new(runtime)),
        ),
        wal,
        None,
    );
    let registry = if store {
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
    }
}

impl Side {
    async fn send_request(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        plan: Option<crate::actor::SendPlan>,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        self.send_request(run_id, request(text), None).await
    }

    /// Waits until `run_id` is the session's latest run and it no longer runs, letting every
    /// gated model step through meanwhile.
    async fn settled(&self, run_id: &str) {
        use crate::serve::{ServeHandler, SessionReadRequest};
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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {run_id} never settled");
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
        broca_wal::session_storage_head(&self.dir, &broca_core::session_addr(&self.key)).exists()
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
        assert_eq!(masked(&new), masked(&old), "{label}: {request}");
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
            assert_eq!(masked(&new), masked(&old), "{label}: the oldest page");
            break;
        };
        old = today.history(json!({"cursor": older_old})).await;
        new = store.history(json!({"cursor": older_new})).await;
        assert_eq!(masked(&new), masked(&old), "{label}: older page {step}");
    }
    if let (Ok(old_page), Ok(new_page)) = (&old, &new) {
        if let (Some(newer_old), Some(newer_new)) =
            (old_page["newer"].as_str(), new_page["newer"].as_str())
        {
            let old = today.history(json!({"cursor": newer_old})).await;
            let new = store.history(json!({"cursor": newer_new})).await;
            assert_eq!(masked(&new), masked(&old), "{label}: a newer page");
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
            assert_eq!(
                masked(&new),
                masked(&old),
                "{label}: {seek} at {ordinal} (the store's id {})",
                mid_at(&new_all, ordinal)
            );
        }
    }
    // The display form needs facts the new storage doesn't serve yet: refused by name.
    let display = store.history(json!({"form": "display"})).await;
    assert_eq!(
        masked(&display),
        json!({"refused": crate::store_entries::NOT_SERVED_ON_NEW_STORAGE}),
        "{label}"
    );
    assert!(today.history(json!({"form": "display"})).await.is_ok());
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
        assert_eq!(new, old, "{label}: {method} from {caller:?}");
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
                        input: json!({"path": "a"}),
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
    let mut steps = vec![
        text_step("a1", "§2§ Done."),
        tool_step("a2"),
        text_step("a3", "§5§ §6§ It says file contents."),
    ];
    steps.extend(more);
    let today = side_idling(false, steps.clone(), false, true, idle).await;
    let store = side_idling(true, steps, false, true, idle).await;
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
        assert_eq!(new, old, "{method} from {caller:?}: {params}");
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
        assert_eq!(new, old, "session.entries {request}");
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
    assert_eq!(new, old, "session.entries by ids");
    assert_eq!(new.unwrap()["entries"].as_array().unwrap().len(), 3);
    // Both storages refuse a malformed view (here an empty object) before reading the session.
    let params = json!({"view": {}});
    let old = today
        .compaction(Some("ck-mc"), "session.apply_view", params.clone())
        .await;
    let new = store
        .compaction(Some("ck-mc"), "session.apply_view", params)
        .await;
    assert_eq!(new, old);
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
    let today = |request: &ReadRequest| {
        let mut head = broca_core::compaction_v2::protocol::read::Head::new();
        head.newest = broca_core::compaction_v2::views::newest(&all);
        serde_json::to_value(broca_core::compaction_v2::views::page(&all, request, head)).unwrap()
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

/// A store session's reads clean part-number notation out of a reply exactly where today's
/// reads do: under a run whose plan renders part numbers, and not under one whose plan only
/// names a compaction provider (today's read follows the run's own `render_tags`, not the
/// compaction view's numbering). The stored message always keeps the text the model wrote.
#[tokio::test]
async fn reads_clean_replies_where_todays_reads_clean_them() {
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
    for (renders, expected) in [
        (true, cleaned.clone()),
        (false, raw.map(str::to_owned).to_vec()),
    ] {
        let (today, store) = contract_sessions(renders).await;
        let old = today.history(json!({"from_ordinal": 1})).await.unwrap();
        assert_eq!(
            texts(old["messages"].as_array().unwrap()),
            expected,
            "today's history, renders {renders}"
        );
        let new = store.history(json!({"from_ordinal": 1})).await.unwrap();
        assert_eq!(
            texts(new["messages"].as_array().unwrap()),
            expected,
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
            expected,
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
    assert_eq!(answers[1], answers[0], "the store answers as today's");
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
    assert_eq!(prompt(&store), prompt(&today));
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
