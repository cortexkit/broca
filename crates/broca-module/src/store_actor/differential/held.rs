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
struct HoldingTools(AtomicUsize);

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
                stop: Default::default(),
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
    boot_sweep_seals_store_orphans_without_a_run_index
);

async fn held_status_and_late_continuation_match_after_restart(cipher: Cipher) {
    let mut initial = Vec::new();
    for store in [false, true] {
        let side = Side::new(store, cipher, vec![super::super::tests::read_step("m1")]).await;
        side.send().await;
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
