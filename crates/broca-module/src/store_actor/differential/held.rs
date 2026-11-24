//! Provider-held status and restart continuation through the same scripted loops as the other
//! actor comparisons. Only storage-owned keys and wall-clock stamps are normalized.

use super::*;
use crate::executor::{StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode};
use crate::held_slots::{HeldRunResumer, HeldSlots};
use crate::serve::ServeHandler;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchResult, ProviderHeldReply, ToolPlane, ToolPlaneError,
};
use broca_protocol::{ToolCallId, ToolOutput, ToolResult};
use broca_session_loop::writer::Writer;
use broca_session_store::keys::{Cipher, DeploymentKey};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "held/intake.rs"]
mod intake;
mod provider_code;
mod sign_in;

#[tokio::test]
async fn run_awaits_held_call_is_active_on_all_surfaces() {
    for store in [true] {
        let side = Side::new(
            store,
            Cipher::None,
            vec![super::super::tests::read_step("m1")],
        )
        .await;
        side.send().await;
        side.idle().await;
        crate::store_actor::awaits_tests::prove(
            &side.registry,
            &side.key,
            if store { "held.after" } else { "held.before" },
            store.then_some(("active", "runner", Some("tool_held"))),
        )
        .await;
        assert!(side.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn run_awaits_terminal_errors_match_status_head_result_and_both_controls() {
    for (name, class, code, message, status, party) in [
        ("terminal-account", "transient", "account_limit_exhausted", "Account limited.", None, "caller"),
        ("terminal-retryable", "transient", "transient", "Try again.", None, "caller"),
        ("terminal-final", "permanent", "request_invalid", "400: {\"error\":{\"message\":\"The request is invalid.\"}}", Some(400), "nobody"),
        ("terminal-sign-in", "permanent", "auth_required", "Sign in.", None, "person"),
        ("message-runner", "transient", "response_deadline", "the ChatGPT backend ended the response at its 900 s limit; send a smaller turn or lower reasoning effort", None, "nobody"),
        ("message-withheld", "permanent", "request_invalid", "websocket error: response body omitted", Some(400), "nobody"),
    ] {
        for store in [true] {
            let error = serde_json::from_value(json!({"class":class, "provider_code":code, "message":message, "status":status})).unwrap();
            let side = Side::new(store, Cipher::None, vec![vec![NormalizedEvent::Control(
                broca_protocol::provider::ProviderControl::Error { error, retries_used: 0 }
            )]]).await;
            side.send().await;
            side.idle().await;
            let body = crate::store_actor::awaits_tests::prove(&side.registry, &side.key,
                &format!("{name}.{}", if store { "after" } else { "before" }),
                store.then_some(("error", party, None))).await;
            if store {
                match name {
                    "terminal-final" => assert_eq!(body["awaits"]["message"]["from"], "provider"),
                    "message-runner" => assert_eq!(body["awaits"]["message"]["from"], "runner"),
                    "message-withheld" => assert!(body["awaits"].get("message").is_none()),
                    _ => {}
                }
            }
            assert!(side.registry.stop_and_seal().await);
        }
    }
}

#[tokio::test]
async fn run_awaits_unknown_error_code_keeps_every_surface_readable() {
    for store in [true] {
        let error = serde_json::from_value(json!({"class":"permanent", "message":"synthetic error without a provider code", "status":400})).unwrap();
        let side = Side::new(
            store,
            Cipher::None,
            vec![vec![NormalizedEvent::Control(
                broca_protocol::provider::ProviderControl::Error {
                    error,
                    retries_used: 0,
                },
            )]],
        )
        .await;
        side.send().await;
        side.idle().await;
        let body = crate::store_actor::awaits_tests::prove(
            &side.registry,
            &side.key,
            if store {
                "terminal-unknown-code.after"
            } else {
                "terminal-unknown-code.before"
            },
            store.then_some(("error", "nobody", None)),
        )
        .await;
        assert_eq!(body["state"], "error");
        assert_eq!(
            body["error"],
            json!({"class":"permanent", "message":"synthetic error without a provider code", "status":400, "provider_code":"provider_error"})
        );
        assert!(side.registry.stop_and_seal().await);
    }
}

#[derive(Default)]
struct HoldingTools(AtomicUsize, std::sync::atomic::AtomicBool);

#[async_trait]
impl ToolPlane for HoldingTools {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[broca_protocol::ToolCall],
        _: &broca_core::ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    fn dispatch_target(&self, _: &BatchCtx, _: &ToolCallId) -> Option<String> {
        Some("tools".into())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &broca_protocol::ToolCall) -> DispatchResult {
        panic!("held calls require their durable key")
    }
    async fn dispatch_keyed(
        &self,
        _: &BatchCtx,
        call: &broca_protocol::ToolCall,
        key: DispatchKey<'_>,
    ) -> DispatchResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        key.held_recorder
            .unwrap()
            .sync_held(&ProviderHeldReply {
                tool_call_id: call.tool_call_id.clone(),
                call_key: key.call_key.into(),
                module: "tools".into(),
                reason: "running".into(),
                detail_digest: None,
            })
            .await
            .unwrap();
        std::future::pending().await
    }

    async fn dispatch_guarded(
        &self,
        ctx: &BatchCtx,
        call: &broca_protocol::ToolCall,
        key: Option<DispatchKey<'_>>,
        guard: &broca_core::tool_plane::ToolOpenGuard,
    ) -> DispatchResult {
        if self.1.load(Ordering::SeqCst) {
            // Stand in for a provider that returns its held final only after runner stop.
            self.0.fetch_add(1, Ordering::SeqCst);
            guard.signals().stopping().await;
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
        self.dispatch_keyed(ctx, call, key.unwrap()).await
    }
}

struct HeldExecutor {
    auth_selection: std::sync::Mutex<Option<broca_records::record::AuthSelection>>,
    provider: Gated,
    tools: Arc<HoldingTools>,
    slots: Arc<HeldSlots>,
    // Replace a scripted auth pause with another pause reason, without involving timers or
    // a process restart, so cancellation exercises the same paused actor on both storages.
    pause_as: std::sync::Mutex<Option<broca_protocol::PauseReason>>,
    /// How long ago a restart pause written through `pause_as` says it was written. The run
    /// loop records the time of a restart pause, so the fixture does too; `0` is now.
    pause_age_ms: std::sync::atomic::AtomicU64,
    refuse_restart: std::sync::atomic::AtomicBool,
    signed_out: std::sync::atomic::AtomicBool,
    sign_in_fixture: std::sync::atomic::AtomicBool,
}

impl HeldExecutor {
    /// The time a pause written through `pause_as` records: a restart pause's time, as the run
    /// loop records it, and none for the other reasons, which the fixture leaves as they were.
    fn pause_ts_ms(&self, reason: broca_protocol::PauseReason) -> Option<u64> {
        (reason == broca_protocol::PauseReason::Restart).then(|| {
            broca_core::clock::epoch_millis()
                .unwrap_or(0)
                .saturating_sub(self.pause_age_ms.load(Ordering::SeqCst))
        })
    }
}

#[async_trait]
impl RunExecutor for HeldExecutor {
    fn cached_account_email(
        &self,
        selection: &broca_records::record::AuthSelection,
    ) -> Option<String> {
        (selection.credential_id == "oauth:stub").then(|| "  default@example.invalid  ".into())
    }
    fn provider_held_lifecycle(
        &self,
    ) -> Option<Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>> {
        Some(self.slots.clone())
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
        if self.refuse_restart.load(Ordering::SeqCst)
            && mode == StoreRunMode::Resume
            && writer
                .operations()
                .run
                .as_ref()
                .is_some_and(|run| run.paused == Some(broca_protocol::PauseReason::Restart))
        {
            return StoreRunEnd {
                parts,
                result: Err(StoreRunError::Admission(RunError::Config(
                    "restart admission refused".into(),
                ))),
            };
        }
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
        control.pause_for_auth_at_start = self.sign_in_error();
        if self.sign_in_fixture.load(Ordering::SeqCst) {
            control.auth_selection = Some(
                self.auth_selection
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(sign_in::selection),
            );
        }
        let result = async {
            let admission = if mode == StoreRunMode::Fresh {
                Some(
                    broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?,
                )
            } else {
                None
            };
            let now = || broca_core::clock::epoch_millis().unwrap_or(0);
            let seams = broca_session_loop::run::Seams {
                person_steers: None,
                provider: &self.provider,
                tools: self.tools.as_ref(),
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: control.stop_requested.clone().unwrap_or_default(),
                held: Some(self.slots.as_ref()),
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: None,
                compaction_session: control.compaction_session,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: control.auth_selection,
                step_progress: control.step_progress,
                model_call_ready: None,
                pause_for_auth_at_start: control.pause_for_auth_at_start,
                account_continue: None,
            };
            let mut finished = match (admission, mode) {
                (Some(admission), _) => {
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                (_, StoreRunMode::Seal(cause)) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await,
            }
            .map_err(StoreRunError::Loop)?;
            let pause_as = *self.pause_as.lock().unwrap();
            if let Some(reason) = pause_as {
                if finished.paused == Some(broca_protocol::PauseReason::AuthRequired) {
                    Writer::append(
                        &mut writer,
                        &[broca_session_store::record::Record::Pause(
                            broca_session_store::record::Pause {
                                run_id: req.run_id.0.clone(),
                                reason,
                                ts_ms: self.pause_ts_ms(reason),
                                error: None,
                                account_limit: None,
                            },
                        )],
                        true,
                    )
                    .await
                    .map_err(broca_session_loop::LoopError::from)
                    .map_err(StoreRunError::Loop)?;
                    finished.paused = Some(reason);
                }
            }
            Ok(finished)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

struct Side {
    dir: std::path::PathBuf,
    key: BindIdentity,
    registry: LineageRegistry,
    executor: Arc<HeldExecutor>,
    store: bool,
    cipher: Cipher,
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Side {
    async fn at(
        dir: std::path::PathBuf,
        key: BindIdentity,
        store: bool,
        cipher: Cipher,
        steps: Vec<Vec<NormalizedEvent>>,
        tools: Arc<HoldingTools>,
        gate: Option<Arc<Notify>>,
    ) -> Self {
        let executor = Arc::new(HeldExecutor {
            provider: Gated {
                inner: broca_core::provider::StubProvider::new(steps),
                gate,
                render_config: None,
                notes_sends: false,
                hold: None,
            },
            tools,
            slots: HeldSlots::new(),
            pause_as: Default::default(),
            pause_age_ms: Default::default(),
            refuse_restart: false.into(),
            signed_out: false.into(),
            sign_in_fixture: false.into(),
            auth_selection: Default::default(),
        });
        let runtime = crate::store_open::StoreRuntime::open(
            &dir,
            DeploymentKey::new(1, &[7; 32]).creating(cipher),
            executor.clone(),
            IDLE,
        )
        .await
        .unwrap();
        let opener = crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor.clone(),
            std::sync::Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
        )
        .with_idle_ttl(IDLE);
        let registry = LineageRegistry::new(Arc::new(opener));
        Self {
            dir,
            key,
            registry,
            executor,
            store,
            cipher,
        }
    }
    async fn new(store: bool, cipher: Cipher, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        let dir = scratch_dir();
        let key = BindIdentity::new(dir.join("project"), "held-comparison", "s1");
        Self::at(dir, key, store, cipher, steps, Arc::default(), None).await
    }
    async fn send(&self) {
        assert!(matches!(
            self.registry
                .route_submission_with_order(
                    &self.key,
                    RunId("r1".into()),
                    request("hello"),
                    RunKind::FreshOrResume,
                    SendKey::Minted,
                    false,
                    broca_protocol::Delivery::Queue,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap(),
            SendOutcome::Active { .. }
        ));
    }
    async fn idle(&self) {
        broca_test_support::wait_until("held actor parks", || async {
            match self.registry.route_status(&self.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }
    async fn status(&self) -> Value {
        serde_json::to_value(
            self.registry
                .run_status(&self.key, RunId("r1".into()))
                .await
                .unwrap(),
        )
        .unwrap()
    }
    async fn result(&self) -> Value {
        serde_json::to_value(
            self.registry
                .run_result(&self.key, RunId("r1".into()))
                .await
                .unwrap(),
        )
        .unwrap()
    }
    async fn restarted(&self) -> Self {
        self.restarted_with_steps(vec![super::super::tests::text_step("m2", "done")])
            .await
    }
    async fn restarted_with_steps(&self, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        fn copy(from: &std::path::Path, to: &std::path::Path) {
            if !from.exists() {
                return;
            }
            std::fs::create_dir_all(to).unwrap();
            for entry in std::fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                let target = to.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    copy(&entry.path(), &target);
                } else {
                    std::fs::copy(entry.path(), target).unwrap();
                }
            }
        }
        let dir = scratch_dir();
        for folder in [broca_records::SESSION_STORAGE_DIR, "setup"] {
            copy(&self.dir.join(folder), &dir.join(folder));
        }
        Self::at(
            dir,
            self.key.clone(),
            self.store,
            self.cipher,
            steps,
            self.executor.tools.clone(),
            None,
        )
        .await
    }

    /// Seeds the crash boundary after the interrupt's queue append, before its cancellation
    /// finalizer can run. This side has no actor yet, so no writer can race the seeded record.
    async fn seed_queued_interrupt(&self) {
        use broca_session_loop::queue::{self, Queued, Receipt};
        use broca_session_store::backend::{LocalBackend, SessionAddr};
        use broca_session_store::record::Record;
        let request = request("stop and do this");
        let at = Queued {
            written_at_ms: Some(1),
            ts_ms: None,
            delivery: Some(broca_protocol::Delivery::Interrupt),
        };
        {
            let runtime = self.registry.store_runtime().unwrap();
            let class = runtime.class_of(&self.key).unwrap().unwrap();
            let backend = LocalBackend::new(class.store_root(&self.dir));
            let mut session = Session::load(
                backend,
                SessionAddr(broca_core::session_addr(&self.key)),
                &crate::store_open::triple(&self.key),
                runtime.deployment(),
            )
            .await
            .unwrap();
            assert_eq!(
                session.operations.run.as_ref().unwrap().paused,
                Some(broca_protocol::PauseReason::ToolHeld)
            );
            let mut parts = runtime.parts_for(class, self.cipher).clone();
            let queued = queue::queued_send(
                "sub-i1",
                None,
                &request,
                &Receipt::default(),
                at,
                None,
                &mut parts,
            )
            .unwrap();
            Writer::append(&mut session, &[Record::QueuedSend(queued)], true)
                .await
                .unwrap();
        }
    }

    async fn pending(&self) -> Vec<String> {
        {
            self.registry
                .store_runtime()
                .unwrap()
                .current_view(&self.key)
                .await
                .unwrap()
                .unwrap()
                .operations
                .queue
                .iter()
                .map(|pending| pending.submission_id.clone())
                .collect()
        }
    }
}

mod cancel;
#[path = "held/restart.rs"]
#[cfg(feature = "test-clock")]
mod restart;

fn comparable(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("awaits");
        if object.get("reason") == Some(&json!("tool_held"))
            && object.get("state") == Some(&json!("active"))
        {
            object.remove("reason");
        }
    }
    if let Some(held) = value.get_mut("held_calls").and_then(Value::as_array_mut) {
        for call in held {
            let object = call.as_object_mut().unwrap();
            for field in ["call_key", "held_at_ms"] {
                if let Some(value) = object.get_mut(field) {
                    *value = json!(!value.is_null());
                }
            }
        }
    }
    value
}

cipher_tests!(
    held_status_and_late_continuation_match_after_restart,
    a_stop_yielded_hold_survives_the_seal_and_restart,
    boot_sweep_seals_store_orphans_without_a_run_index,
    a_held_pause_keeps_a_queued_interrupt_parked_after_restart
);

/// A crash after an interrupt is queued but before it ends a held run leaves both parked on
/// open. Today's held-call recovery returns before its ordinary paused-run interrupt drain.
async fn a_held_pause_keeps_a_queued_interrupt_parked_after_restart(cipher: Cipher) {
    let mut initial = Vec::new();
    let mut restarted = Vec::new();
    let mut statuses = Vec::new();
    let mut results = Vec::new();
    let mut queues = Vec::new();
    for store in [true] {
        let side = Side::new(store, cipher, vec![super::super::tests::read_step("m1")]).await;
        side.send().await;
        side.idle().await;
        let recovered = side.restarted().await;
        recovered.seed_queued_interrupt().await;
        // The first command opens the actor and waits for its startup recovery to finish.
        assert!(matches!(
            recovered
                .registry
                .route_status(&recovered.key)
                .await
                .unwrap(),
            LineageStatus::Idle
        ));
        let status = recovered.status().await;
        assert_eq!(status["state"], "active");
        assert_eq!(status["held_calls"].as_array().unwrap().len(), 1);
        statuses.push(comparable(status));
        results.push(comparable(recovered.result().await));
        let queue = recovered.pending().await;
        assert_eq!(queue, ["sub-i1"]);
        queues.push(queue);
        assert_eq!(recovered.executor.tools.0.load(Ordering::SeqCst), 1);
        assert!(recovered.executor.provider.inner.calls().is_empty());
        initial.push(side);
        restarted.push(recovered);
    }
    broca_test_support::pin!("alike", statuses[0]);
    broca_test_support::pin!("alike", results[0]);
    broca_test_support::pin!("alike", queues[0]);
    for side in initial.iter().chain(&restarted) {
        assert!(side.registry.stop_and_seal().await);
    }
}

async fn held_status_and_late_continuation_match_after_restart(cipher: Cipher) {
    held_restart(cipher, false).await;
}

async fn a_stop_yielded_hold_survives_the_seal_and_restart(cipher: Cipher) {
    held_restart(cipher, true).await;
}

async fn held_restart(cipher: Cipher, yield_at_stop: bool) {
    let mut initial = Vec::new();
    for store in [true] {
        let side = Side::new(store, cipher, vec![super::super::tests::read_step("m1")]).await;
        side.executor.tools.1.store(yield_at_stop, Ordering::SeqCst);
        side.send().await;
        if yield_at_stop {
            broca_test_support::wait_until("tool waiting for stop", || async {
                if side.executor.tools.0.load(Ordering::SeqCst) == 1 {
                    Ok(())
                } else {
                    Err("not dispatched".into())
                }
            })
            .await;
            side.registry.begin_stop();
        }
        side.idle().await;
        initial.push(side);
    }
    let before = initial[0].status().await;
    broca_test_support::pin!("alike", comparable(before.clone()));
    assert_eq!(before["state"], "active");
    assert_eq!(before["held_calls"].as_array().unwrap().len(), 1);
    broca_test_support::pin!("alike", comparable(initial[0].result().await));
    let mut restarted = Vec::new();
    for side in &initial {
        if yield_at_stop {
            assert!(side.registry.stop_and_seal().await);
            if side.store {
                let runtime = side.registry.store_runtime().unwrap();
                let class = runtime.class_of(&side.key).unwrap().unwrap();
                let session = Session::load(
                    broca_session_store::backend::LocalBackend::new(class.store_root(&side.dir)),
                    broca_session_store::backend::SessionAddr(broca_core::session_addr(&side.key)),
                    &crate::store_open::triple(&side.key),
                    runtime.deployment(),
                )
                .await
                .unwrap();
                let run = session.operations.run.as_ref().unwrap();
                assert!(run.finished.is_none(), "shutdown must not end the held run");
                assert_eq!(run.paused, Some(broca_protocol::PauseReason::ToolHeld));
            }
        }
        restarted.push(side.restarted().await);
    }
    for side in &restarted {
        let sweep = side.registry.recover_store_crash_orphans().await.unwrap();
        assert!(sweep.failed.is_empty(), "{sweep:?}");
        side.registry.route_status(&side.key).await.unwrap();
        side.idle().await;
        assert_eq!(
            side.executor.tools.0.load(Ordering::SeqCst),
            1,
            "opening must not redispatch"
        );
    }
    broca_test_support::pin!("alike", comparable(restarted[0].status().await));
    for side in &restarted {
        let status = side.status().await;
        let key = status["held_calls"][0]["call_key"].as_str().unwrap();
        let synced = side.executor.slots.deposit(
            &side.key,
            &ToolCallId("c1".into()),
            key,
            ToolResult {
                tool_call_id: ToolCallId("c1".into()),
                output: ToolOutput::Text {
                    text: "late answer".into(),
                },
                is_error: false,
            },
        );
        side.registry
            .resume_for_late_result(&side.key)
            .await
            .unwrap();
        broca_test_support::wait_for("held result durable", synced, || {
            "late result not synced".into()
        })
        .await
        .unwrap();
        side.idle().await;
        assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
        assert_eq!(side.executor.provider.inner.calls().len(), 1);
    }
    broca_test_support::pin!("alike", restarted[0].status().await);
    let mut results = [restarted[0].result().await];
    for result in &mut results {
        result["final_message"]
            .as_object_mut()
            .unwrap()
            .remove("mid");
    }
    broca_test_support::pin!("alike", results[0]);
    assert_eq!(results[0]["state"], "completed");
    for side in initial.iter().chain(&restarted) {
        assert!(side.registry.stop_and_seal().await);
    }
}

async fn boot_sweep_seals_store_orphans_without_a_run_index(cipher: Cipher) {
    let mut restarted = Vec::new();
    let mut originals = Vec::new();
    for store in [true] {
        let dir = scratch_dir();
        let key = BindIdentity::new(dir.join("project"), "orphan-comparison", "s1");
        let original = Side::at(
            dir,
            key,
            store,
            cipher,
            vec![super::super::tests::text_step("m1", "never sent")],
            Arc::default(),
            Some(Arc::default()),
        )
        .await;
        original.send().await;
        let side = original.restarted().await;
        if store {
            // Neither side has a run index. Discovery must come from the session store's heads.
            let sweep = side.registry.recover_store_crash_orphans().await.unwrap();
            assert_eq!(sweep.inspected, 1, "{sweep:?}");
            assert_eq!(sweep.recovered, 1, "{sweep:?}");
            assert!(sweep.failed.is_empty(), "{sweep:?}");
            let again = side.registry.recover_store_crash_orphans().await.unwrap();
            assert_eq!(
                again.recovered, 0,
                "finished runs must stay closed: {again:?}"
            );
        }
        assert_eq!(
            side.executor.provider.inner.calls().len(),
            0,
            "sealing must not call the model"
        );
        assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 0);
        restarted.push(side);
        originals.push(original);
    }
    broca_test_support::pin!("alike", restarted[0].status().await);
    broca_test_support::pin!("alike", restarted[0].result().await);
    assert_eq!(restarted[0].status().await["state"], "interrupted");
    for side in originals.iter().chain(&restarted) {
        assert!(side.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn startup_read_is_served_while_background_recovery_is_pending() {
    use broca_records::session_storage::StorageClass;
    use broca_session_store::backend::{LocalBackend, SessionAddr};

    let dir = scratch_dir();
    let key = BindIdentity::new(dir.join("project"), "startup-recovery", "orphan");
    let original = Side::at(
        dir,
        key,
        true,
        Cipher::None,
        vec![super::super::tests::text_step("m1", "never sent")],
        Arc::default(),
        Some(Arc::default()),
    )
    .await;
    // Establish coverage as a previous successful startup would, before a run starts.
    assert_eq!(
        original
            .registry
            .recover_store_crash_orphans()
            .await
            .unwrap()
            .inspected,
        0
    );
    original.send().await;
    let restarted = original.restarted().await;
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(Cipher::None);
    let root = StorageClass::Ordinary.store_root(&restarted.dir);
    let mut idle = None;
    for n in 0..8 {
        let key = BindIdentity::new(
            restarted.dir.join("project"),
            "startup-recovery",
            format!("idle-{n}"),
        );
        drop(
            Session::create(
                LocalBackend::new(&root),
                SessionAddr(broca_core::session_addr(&key)),
                crate::store_open::triple(&key),
                &deployment,
                false,
            )
            .await
            .unwrap(),
        );
        idle = Some(key);
    }
    let started = std::time::Instant::now();
    let mut sweep = Box::pin(restarted.registry.recover_store_crash_orphans());
    assert!(
        futures::poll!(sweep.as_mut()).is_pending(),
        "the background pass yields to requests"
    );
    let runtime = restarted.registry.store_runtime().unwrap();
    assert!(
        runtime
            .read(
                &restarted.registry,
                &idle.unwrap(),
                crate::store_reads::StoreReadRequest::Head
            )
            .await
            .unwrap()
            .is_some(),
        "another session is readable while recovery is pending"
    );
    let sweep = sweep.await.unwrap();
    eprintln!(
        "synthetic startup: idle_sessions=8 inspected={} recovered={} recovery_ms={}",
        sweep.inspected,
        sweep.recovered,
        started.elapsed().as_millis()
    );
    assert!(sweep.failed.is_empty(), "{sweep:?}");
    assert_eq!(
        sweep.inspected, 1,
        "idle sessions must add no recovery work: {sweep:?}"
    );
    assert_eq!(
        sweep.recovered, 1,
        "an untouched orphan is recovered in the background"
    );
    assert_eq!(restarted.status().await["state"], "interrupted");
    assert_eq!(restarted.executor.provider.inner.calls().len(), 0);
    let again = restarted
        .registry
        .recover_store_crash_orphans()
        .await
        .unwrap();
    assert_eq!(
        again.inspected, 0,
        "the durable terminal retires its candidate: {again:?}"
    );
    assert!(restarted.registry.stop_and_seal().await);
    original
        .executor
        .provider
        .gate
        .as_ref()
        .unwrap()
        .notify_one();
    assert!(original.registry.stop_and_seal().await);
}

/// Valid in-memory sessions avoid making a work-bound assertion depend on thousands of
/// physical durability barriers. The production selector must neither list idle heads nor
/// load their logs, at either inventory size.
#[tokio::test]
async fn startup_recovery_work_ignores_idle_store_sessions() {
    use broca_records::session_storage::StorageClass;
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::record::{Record, RunStart};

    let mut work = Vec::new();
    for size in [64, 3000] {
        let backend = MemoryBackend::default();
        let deployment = DeploymentKey::new(1, &[7; 32]).creating(Cipher::None);
        let mut sessions = std::collections::HashMap::new();
        for n in 0..=size {
            let key = BindIdentity::new("/project", "startup-recovery", format!("session-{n}"));
            let addr = SessionAddr(broca_core::session_addr(&key));
            let mut session = Session::create(
                &backend,
                addr.clone(),
                crate::store_open::triple(&key),
                &deployment,
                false,
            )
            .await
            .unwrap();
            if n == size {
                session
                    .append(
                        &[Record::RunStart(Box::new(RunStart {
                            named_speaker: None,
                            kept_notice: None,
                            imported_ids: None,
                            render_identity: None,
                            message_marks: None,
                            run_id: "orphan".into(),
                            model: broca_protocol::ModelHandle::new("stub", "m"),
                            setup: Vec::new(),
                            renderer_version: 1,
                            submission_id: None,
                            send_id: None,
                            request_identity_digest: None,
                            ts_ms: None,
                            written_at_ms: None,
                            session_class: None,
                            retention_secs: None,
                            title: None,
                            context_limit: None,
                            service_tier: None,
                            route_decision_id: None,
                            compaction_plan_version: None,
                            auth_selection: None,
                            run_choices: None,
                            origin: None,
                            principal: None,
                            scope: None,
                            hooks: None,
                            keep_warm: None,
                            on_restart: None,
                            seeded: None,
                            refusal_schedule: None,
                        }))],
                        true,
                    )
                    .await
                    .unwrap();
            }
            sessions.insert(addr, key);
        }
        let orphan = BindIdentity::new("/project", "startup-recovery", format!("session-{size}"));
        let candidate = SessionAddr(broca_core::session_addr(&orphan));
        let mut listed_heads = 0;
        let addresses = crate::store_open::recovery::candidate_addresses(
            Some(vec![candidate]),
            StorageClass::Ordinary,
            || {
                listed_heads += sessions.len();
                Ok(sessions.keys().cloned().collect())
            },
        )
        .unwrap();
        let before = (backend.log_reads(), backend.object_reads());
        let mut inspected = 0;
        let mut unfinished = 0;
        for addr in addresses {
            inspected += 1;
            let key = &sessions[&addr];
            let view = Session::view(&backend, addr, &crate::store_open::triple(key), &deployment)
                .await
                .unwrap();
            if let Some(run) = view
                .operations
                .run
                .as_ref()
                .filter(|run| run.finished.is_none())
            {
                assert!(matches!(
                    crate::store_actor::recovery_mode(run),
                    Some(StoreRunMode::Seal(broca_core::seal::SealCause::CrashOrphan))
                ));
                unfinished += 1;
            }
        }
        let counts = (
            inspected,
            backend.log_reads() - before.0,
            backend.object_reads() - before.1,
            listed_heads,
        );
        eprintln!("startup work: idle_sessions={size} inspected={} log_reads={} object_reads={} listed_heads={}",
            counts.0, counts.1, counts.2, counts.3);
        assert_eq!(unfinished, 1);
        assert_eq!(
            (counts.0, counts.2, counts.3),
            (1, 0, 0),
            "only the candidate is inspected, without listing idle heads or loading objects"
        );
        assert!(counts.1 > 0, "the candidate's real log must be read");
        work.push(counts);
    }
    assert_eq!(
        work[0], work[1],
        "the work is the same at either inventory size"
    );
}

#[tokio::test]
async fn startup_read_recovers_its_own_orphan_without_global_sweep() {
    let original = Side::at(
        scratch_dir(),
        BindIdentity::new("/project", "startup-recovery", "read-orphan"),
        true,
        Cipher::None,
        vec![super::super::tests::text_step("m1", "never sent")],
        Arc::default(),
        Some(Arc::default()),
    )
    .await;
    original.send().await;
    let reboot = original.restarted().await;
    assert!(reboot.registry.slot_state(&reboot.key).is_none());
    let runtime = reboot.registry.store_runtime().unwrap();
    runtime
        .read(
            &reboot.registry,
            &reboot.key,
            crate::store_reads::StoreReadRequest::Head,
        )
        .await
        .unwrap()
        .unwrap();
    let view = runtime.current_view(&reboot.key).await.unwrap().unwrap();
    assert_eq!(
        view.operations.run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Interrupted)
    );
    assert_eq!(reboot.status().await["state"], "interrupted");
    assert_eq!(reboot.executor.provider.inner.calls().len(), 0);
    assert!(reboot.registry.stop_and_seal().await);
    original
        .executor
        .provider
        .gate
        .as_ref()
        .unwrap()
        .notify_one();
    assert!(original.registry.stop_and_seal().await);
}

#[tokio::test]
async fn a_parked_held_run_reads_without_opening_an_actor() {
    let original = Side::new(
        true,
        Cipher::None,
        vec![super::super::tests::read_step("m1")],
    )
    .await;
    original.send().await;
    original.idle().await;
    let expected = comparable(original.status().await);
    let reboot = original.restarted().await;
    assert!(reboot.registry.slot_state(&reboot.key).is_none());
    assert_eq!(comparable(reboot.status().await), expected);
    assert!(
        reboot.registry.slot_state(&reboot.key).is_none(),
        "a durable held pause stays read-only"
    );
    assert_eq!(reboot.executor.tools.0.load(Ordering::SeqCst), 1);
    assert!(reboot.registry.stop_and_seal().await);
    assert!(original.registry.stop_and_seal().await);
}

cipher_tests!(held_record_boundary_cuts_rejoin_without_redispatch);

async fn held_record_boundary_cuts_rejoin_without_redispatch(cipher: Cipher) {
    use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend};
    use broca_session_store::frame::{FrameRead, FrameStream};
    use broca_session_store::record::Record;
    let source = Side::new(
        true,
        cipher,
        vec![
            super::super::tests::read_step("m1"),
            super::super::tests::text_step("m2", "done"),
        ],
    )
    .await;
    source.send().await;
    source.idle().await;
    let status = source.status().await;
    let call_key = status["held_calls"][0]["call_key"].as_str().unwrap();
    let result = || ToolResult {
        tool_call_id: ToolCallId("c1".into()),
        output: ToolOutput::Text {
            text: "late answer".into(),
        },
        is_error: false,
    };
    let synced =
        source
            .executor
            .slots
            .deposit(&source.key, &ToolCallId("c1".into()), call_key, result());
    source
        .registry
        .resume_for_late_result(&source.key)
        .await
        .unwrap();
    broca_test_support::wait_for("source held terminal", synced, || "no terminal".into())
        .await
        .unwrap();
    source.idle().await;
    let runtime = source.registry.store_runtime().unwrap();
    let class = runtime.class_of(&source.key).unwrap().unwrap();
    let view = runtime.current_view(&source.key).await.unwrap().unwrap();
    let parts = runtime.parts_for(class, cipher).clone();
    let backend = LocalBackend::new(class.store_root(&source.dir));
    let addr = SessionAddr(broca_core::session_addr(&source.key));
    let (stored_head, version) = backend.read_stored_head(&addr).unwrap().unwrap();
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let (head, key) = broca_session_store::head::open(&stored_head, &deployment).unwrap();
    let frame_key = key.frame_key();
    let (log, end) = view.open_log();
    let bytes = backend.read_log(&addr, &log, 0..end).await.unwrap();
    let mut bind = head.session_id.0.to_vec();
    bind.extend_from_slice(&log.0);
    let mut offset = 0;
    let mut cuts = Vec::new();
    while offset < bytes.len() {
        let FrameRead::Frame { header, body, len } =
            broca_session_store::frame::open(&bytes[offset..], |_| Some(&frame_key), &bind)
                .unwrap()
        else {
            panic!("source log is complete")
        };
        offset += len;
        if header.stream == FrameStream::None {
            continue;
        }
        let record = Record::decode(u32::from(header.kind), &body).unwrap();
        if matches!(
            record,
            Record::ToolHeld(_) | Record::Pause(_) | Record::ToolReply(_) | Record::ToolOutcome(_)
        ) {
            cuts.push((offset, record));
        }
    }
    assert_eq!(cuts.len(), 4);
    for (cut, record) in cuts {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        backend
            .write_head(&addr, &stored_head, None, version)
            .await
            .unwrap();
        // Preserve the real batch framing. A cut between ToolReply and ToolOutcome rolls
        // the whole terminal append back, rather than inventing a transcript-only result.
        backend.set_log(&addr, &log, bytes[..cut].to_vec());
        let session = Session::load(backend, addr.clone(), &head.triple, &deployment)
            .await
            .unwrap();
        let needs_result = !session
            .records_from(0)
            .await
            .unwrap()
            .iter()
            .any(|(_, record)| matches!(record, Record::ToolOutcome(_)));
        let executor = Arc::new(HeldExecutor {
            provider: Gated {
                inner: broca_core::provider::StubProvider::new(vec![
                    super::super::tests::text_step("m2", "done"),
                ]),
                gate: None,
                render_config: None,
                notes_sends: false,
                hold: None,
            },
            tools: source.executor.tools.clone(),
            slots: HeldSlots::new(),
            pause_as: Default::default(),
            pause_age_ms: Default::default(),
            refuse_restart: false.into(),
            signed_out: false.into(),
            sign_in_fixture: false.into(),
            auth_selection: Default::default(),
        });
        let actor = super::super::StoreActor::new(
            source.key.clone(),
            session,
            executor.clone(),
            Box::new(parts.clone()),
        );
        let (commands, receiver) = mpsc::unbounded_channel();
        let served = tokio::spawn(actor.serve(receiver));
        if needs_result {
            let synced =
                executor
                    .slots
                    .deposit(&source.key, &ToolCallId("c1".into()), call_key, result());
            let (reply, receive) = tokio::sync::oneshot::channel();
            commands
                .send(crate::actor::LineageCmd::ResumeHeld { reply })
                .unwrap();
            receive.await.unwrap().unwrap();
            broca_test_support::wait_for("cut held terminal", synced, || {
                format!("cut at {record:?}")
            })
            .await
            .unwrap();
        }
        broca_test_support::wait_until("cut run completes", || async {
            let (reply, receive) = tokio::sync::oneshot::channel();
            commands
                .send(crate::actor::LineageCmd::Status { reply })
                .unwrap();
            match receive.await.unwrap() {
                LineageStatus::Idle => Ok(()),
                status => Err(format!("{status:?}, cut at {record:?}")),
            }
        })
        .await;
        drop(commands);
        let session = served.await.unwrap();
        let after = session.records_from(0).await.unwrap();
        assert_eq!(
            after
                .iter()
                .filter(|(_, record)| matches!(record, Record::ToolReply(_)))
                .count(),
            1,
            "a recorded result must never be appended again; cut at {record:?}"
        );
        assert_eq!(
            session.operations.run.as_ref().unwrap().finished,
            Some(broca_protocol::RunFinishReason::Completed),
            "cut at {record:?}"
        );
        assert_eq!(
            source.executor.tools.0.load(Ordering::SeqCst),
            1,
            "a held call must never be resent"
        );
        assert_eq!(executor.provider.inner.calls().len(), 1);
    }
    assert!(source.registry.stop_and_seal().await);
}
