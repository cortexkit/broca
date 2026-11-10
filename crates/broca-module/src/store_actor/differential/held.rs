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
    provider: Gated,
    wal: Arc<FileWalStore>,
    tools: Arc<HoldingTools>,
    slots: Arc<HeldSlots>,
}

#[async_trait]
impl RunExecutor for HeldExecutor {
    fn provider_held_lifecycle(
        &self,
    ) -> Option<Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>> {
        Some(self.slots.clone())
    }
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        job.control.admitted_context_limit =
            job.control.admitted_context_limit.or(job.req.context_limit);
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            StartMode::FreshOrResume,
            &self.provider,
            self.tools.as_ref(),
            &AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
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
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
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
            let now = || broca_core::run::epoch_millis().unwrap_or(0);
            let seams = broca_session_loop::run::Seams {
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
                auth_selection: None,
                step_progress: control.step_progress,
                model_call_ready: None,
            };
            match (admission, mode) {
                (Some(admission), _) => {
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                (_, StoreRunMode::Seal(cause)) => {
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
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(HeldExecutor {
            provider: Gated {
                inner: broca_core::provider::StubProvider::new(steps),
                gate,
                render_config: None,
                notes_sends: false,
            },
            wal: wal.clone(),
            tools,
            slots: HeldSlots::new(),
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
            wal.clone(),
            executor.clone(),
        )
        .with_idle_ttl(IDLE)
        .with_store_runtime(Arc::new(runtime));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
        let registry = if store {
            registry.with_store_new_sessions()
        } else {
            registry
        };
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
        for folder in ["wal", broca_wal::SESSION_STORAGE_DIR, "setup"] {
            copy(&self.dir.join(folder), &dir.join(folder));
        }
        Self::at(
            dir,
            self.key.clone(),
            self.store,
            self.cipher,
            vec![super::super::tests::text_step("m2", "done")],
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
        if self.store {
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
        } else {
            assert!(self
                .executor
                .wal
                .read_records(&self.key)
                .unwrap()
                .iter()
                .any(|frame| matches!(
                    &frame.record,
                    broca_wal::WalRecord::RunPaused {
                        reason: broca_protocol::PauseReason::ToolHeld,
                        ..
                    }
                )));
            self.executor
                .wal
                .open(&self.key)
                .unwrap()
                .append(
                    1,
                    &broca_wal::WalRecord::Queued {
                        submission_id: broca_protocol::SubmissionId("sub-i1".into()),
                        request: Box::new(request),
                        written_at_ms: at.written_at_ms,
                        ts_ms: at.ts_ms,
                        delivery: at.delivery,
                        request_identity_digest: None,
                        principal: None,
                        sent_by: None,
                        retention: None,
                        keep_warm: None,
                        conversation_key: None,
                        send_id: None,
                        title: None,
                        hooks: None,
                        message_marks: None,
                    },
                    true,
                )
                .unwrap();
        }
    }

    async fn pending(&self) -> Vec<String> {
        if self.store {
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
        } else {
            let records = self
                .executor
                .wal
                .read_records(&self.key)
                .unwrap()
                .into_iter()
                .map(|frame| frame.record)
                .collect::<Vec<_>>();
            broca_core::queue::pending_submissions(&records)
                .into_iter()
                .map(|pending| pending.submission_id.0)
                .collect()
        }
    }
}

fn comparable(mut value: Value) -> Value {
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
    for store in [false, true] {
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
    assert_eq!(statuses[1], statuses[0]);
    assert_eq!(results[1], results[0]);
    assert_eq!(queues[1], queues[0]);
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
    for store in [false, true] {
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
    let before = [initial[0].status().await, initial[1].status().await];
    assert_eq!(comparable(before[1].clone()), comparable(before[0].clone()));
    assert_eq!(before[1]["state"], "active");
    assert_eq!(before[1]["held_calls"].as_array().unwrap().len(), 1);
    assert_eq!(
        comparable(initial[1].result().await),
        comparable(initial[0].result().await)
    );
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
            } else {
                let records = side.executor.wal.read_records(&side.key).unwrap();
                assert!(records.iter().any(|frame| matches!(
                    frame.record,
                    broca_wal::WalRecord::ToolCallProviderHeld { .. }
                )));
                assert!(
                    !records.iter().any(|frame| matches!(
                        frame.record,
                        broca_wal::WalRecord::RunFinished { .. }
                    )),
                    "no terminal before same-run resume"
                );
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
    assert_eq!(
        comparable(restarted[1].status().await),
        comparable(restarted[0].status().await)
    );
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
    assert_eq!(restarted[1].status().await, restarted[0].status().await);
    let mut results = [restarted[0].result().await, restarted[1].result().await];
    for result in &mut results {
        result["final_message"]
            .as_object_mut()
            .unwrap()
            .remove("mid");
    }
    assert_eq!(results[1], results[0]);
    assert_eq!(results[1]["state"], "completed");
    for side in initial.iter().chain(&restarted) {
        assert!(side.registry.stop_and_seal().await);
    }
}

async fn boot_sweep_seals_store_orphans_without_a_run_index(cipher: Cipher) {
    let mut restarted = Vec::new();
    let mut originals = Vec::new();
    for store in [false, true] {
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
            // Neither side has a run index. Discovery must come from the new storage's heads.
            let sweep = side.registry.recover_store_crash_orphans().await.unwrap();
            assert_eq!(sweep.inspected, 1, "{sweep:?}");
            assert_eq!(sweep.recovered, 1, "{sweep:?}");
            assert!(sweep.failed.is_empty(), "{sweep:?}");
            let again = side.registry.recover_store_crash_orphans().await.unwrap();
            assert_eq!(
                again.recovered, 0,
                "finished runs must stay closed: {again:?}"
            );
        } else {
            let sweep = side.registry.mark_crash_orphans(
                side.executor.wal.as_ref(),
                vec![(RunId("r1".into()), side.key.clone())],
                std::time::SystemTime::now(),
            );
            let sweep = side.registry.seal_marked_crash_orphans(sweep).await;
            assert_eq!(sweep.sealed, 1, "{sweep:?}");
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
    assert_eq!(restarted[1].status().await, restarted[0].status().await);
    assert_eq!(restarted[1].result().await, restarted[0].result().await);
    assert_eq!(restarted[1].status().await["state"], "interrupted");
    for side in originals.iter().chain(&restarted) {
        assert!(side.registry.stop_and_seal().await);
    }
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
            },
            wal: source.executor.wal.clone(),
            tools: source.executor.tools.clone(),
            slots: HeldSlots::new(),
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
