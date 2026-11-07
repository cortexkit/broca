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
                // As the module's executor runs every store run (`Seams::compaction_changes`).
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
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
    let scripted = || Gated {
        inner: broca_core::provider::StubProvider::new(steps.clone()),
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
        IDLE,
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
            .with_idle_ttl(IDLE)
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
    let read = store.history(json!({"from_ordinal": 1})).await.unwrap();
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
    let steps = vec![
        text_step("a1", "§2§ Done."),
        tool_step("a2"),
        text_step("a3", "§5§ §6§ It says file contents."),
    ];
    let today = side(false, steps.clone(), false, true).await;
    let store = side(true, steps, false, true).await;
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
        .view(&store.key)
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
    // Accepting a pushed view is the session writer's, which the store actor doesn't take yet.
    let apply = store
        .compaction(Some("ck-mc"), "session.apply_view", json!({"view": {}}))
        .await
        .unwrap_err();
    assert_eq!(
        apply["code"],
        json!(crate::store_entries::NOT_SERVED_ON_NEW_STORAGE)
    );
    assert_eq!(apply["detail"]["method"], json!("session.apply_view"));
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
        .view(&store.key)
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
            .view(&store.key)
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
