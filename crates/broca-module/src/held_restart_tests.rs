//! Held results must survive a restart after SQLite has checkpointed the run's earlier turns.

use super::tests::text_req;
use super::*;
use crate::held_slots::{HeldSlots, SlotDelivery};
use crate::late_results_puller::HeldDelivery;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use async_trait::async_trait;
use broca_core::tool_plane::{DispatchKey, LateResultRegistration, ProviderHeldReply};
use broca_core::Persister;
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore, StubProvider, ToolManifest,
    ToolPlane, ToolPlaneError,
};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall};
use broca_protocol::{BindIdentity, RunFinishReason};
use broca_protocol::{
    FinishReason, MessageId, NormalizedEvent, ProviderControl, ToolCallId, ToolOutput, ToolResult,
};
use cortexkit_role_tool_provider::late_results::LateEntry;
use std::sync::atomic::AtomicUsize;

struct HoldingPlane {
    dispatches: AtomicUsize,
}

#[async_trait]
impl ToolPlane for HoldingPlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    fn dispatch_target(&self, _: &BatchCtx, _: &ToolCallId) -> Option<String> {
        Some("tools".into())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
        panic!("held calls require a durable key")
    }
    async fn dispatch_keyed(
        &self,
        _: &BatchCtx,
        call: &ToolCall,
        key: DispatchKey<'_>,
    ) -> DispatchResult {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        if call.tool_call_id.0 == "inline" {
            return DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "inline".into(),
                },
                is_error: false,
            });
        }
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

struct Executor {
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    slots: Arc<HeldSlots>,
    plane: Arc<HoldingPlane>,
    provider: FrozenProvider,
}

struct FrozenProvider {
    inner: StubProvider,
    incompatible: bool,
}

#[async_trait]
impl broca_core::Provider for FrozenProvider {
    fn render_config_blob(&self) -> serde_json::Value {
        if self.incompatible {
            serde_json::json!({"renderer":"incompatible"})
        } else {
            self.inner.render_config_blob()
        }
    }
    async fn step(
        &self,
        step: broca_protocol::StepId,
        options: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.inner.step(step, options).await
    }
}

#[async_trait]
impl RunExecutor for Executor {
    fn provider_held_lifecycle(
        &self,
    ) -> Option<Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>> {
        Some(self.slots.clone())
    }
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
            RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &self.provider,
            self.plane.as_ref(),
            &AllowAll,
            self.wal.as_ref(),
            Some(self.store.clone()),
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

fn step(block: ContentBlock, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![block],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Default::default(),
            finish_reason,
            retries_used: 0,
        }),
    ]
}

async fn restarted_delivery(restarts: usize, refused: bool, queued: bool) {
    let root = broca_test_support::scratch_root().join(format!(
        "held-materialized-restarts-{restarts}-{refused}-{queued}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "broca", "held");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap());
    let plane = Arc::new(HoldingPlane {
        dispatches: AtomicUsize::new(0),
    });
    let mut registration = None;
    for generation in 0..=restarts {
        let slots = HeldSlots::new();
        let scripts = if generation == 0 {
            vec![
                step(
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("inline".into()),
                        tool_name: "ask".into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    }),
                    FinishReason::ToolCalls,
                ),
                step(
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("a".into()),
                        tool_name: "ask".into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    }),
                    FinishReason::ToolCalls,
                ),
            ]
        } else {
            let mut scripts = vec![step(
                ContentBlock::Text {
                    text: "done".into(),
                },
                FinishReason::Stop,
            )];
            if queued {
                scripts.push(step(
                    ContentBlock::Text {
                        text: "queued done".into(),
                    },
                    FinishReason::Stop,
                ));
            }
            scripts
        };
        let executor = Arc::new(Executor {
            wal: wal.clone(),
            store: store.clone(),
            slots: slots.clone(),
            plane: plane.clone(),
            provider: FrozenProvider {
                inner: StubProvider::new(scripts),
                incompatible: refused && generation == restarts,
            },
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            executor,
        )
        .with_materialized_store(store.clone());
        let registry = LineageRegistry::new(Arc::new(opener));
        if generation == 0 {
            let mut req = text_req(&key, "held");
            req.policy_allowed = ["ask".to_owned()].into_iter().collect();
            req.tools = vec![broca_protocol::request::Tool {
                name: "ask".into(),
                description: "ask".into(),
                input_schema: serde_json::json!({"type":"object"}),
                module: Some("tools".into()),
                execution_mode: Default::default(),
                reply: None,
            }];
            assert!(matches!(
                registry
                    .route_send(&key, req, RunKind::FreshOrResume)
                    .await
                    .unwrap(),
                SendOutcome::Active { .. }
            ));
            broca_test_support::wait_until("durable held pause", || async {
                wal.read_records(&key)
                    .unwrap()
                    .iter()
                    .any(|f| {
                        matches!(
                            f.record,
                            WalRecord::RunPaused {
                                reason: broca_protocol::PauseReason::ToolHeld,
                                ..
                            }
                        )
                    })
                    .then_some(())
                    .ok_or_else(|| {
                        format!(
                            "dispatches={} WAL={:?}",
                            plane.dispatches.load(Ordering::SeqCst),
                            wal.read_records(&key).unwrap()
                        )
                    })
            })
            .await;
            let frames = wal.read_records(&key).unwrap();
            let call_key = frames
                .iter()
                .find_map(|f| match &f.record {
                    WalRecord::ToolCallProviderHeld { call_key, .. } => Some(call_key.clone()),
                    _ => None,
                })
                .unwrap();
            registration = Some(LateResultRegistration {
                lineage: wal
                    .read_records_range(&key, 0, None, None)
                    .unwrap()
                    .lineage_id
                    .unwrap()
                    .to_string(),
                call_key,
                module: "tools".into(),
                bind: key.clone(),
                scope: None,
            });
            if queued {
                assert!(matches!(
                    registry
                        .route_send(&key, text_req(&key, "queued"), RunKind::FreshOrResume)
                        .await
                        .unwrap(),
                    SendOutcome::Pending { .. }
                ));
            }
        } else {
            registry.route_status(&key).await.unwrap();
        }
        if generation == restarts {
            let delivery = SlotDelivery::new(
                slots,
                Arc::new(registry.clone()),
                Arc::new(|id, _| {
                    Some(ToolResult {
                        tool_call_id: id.clone(),
                        output: ToolOutput::Text {
                            text: "remote_unavailable".into(),
                        },
                        is_error: true,
                    })
                }),
            )
            .with_timeout(Duration::from_millis(250));
            let entry = LateEntry::new(
                "result",
                "reserved:broca",
                "ref",
                1,
                "reserved:broca",
                &registration.as_ref().unwrap().call_key,
                "late",
                1,
            );
            delivery
                .deliver(
                    registration.as_ref().unwrap(),
                    &ToolCallId("a".into()),
                    &entry,
                )
                .await
                .expect("the run must sync the held terminal after restart");
            broca_test_support::wait_until("finished resumed run", || async {
                wal.read_records(&key)
                    .unwrap()
                    .iter()
                    .filter(|f| matches!(f.record, WalRecord::RunFinished { .. }))
                    .count()
                    .eq(&if queued { 2 } else { 1 })
                    .then_some(())
                    .ok_or_else(|| "not finished".to_owned())
            })
            .await;
            let frames = wal.read_records(&key).unwrap();
            let terminals: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.record {
                    WalRecord::RunFinished { reason, error, .. } => Some((reason, error)),
                    _ => None,
                })
                .collect();
            assert_eq!(terminals.len(), if queued { 2 } else { 1 });
            if queued {
                let end = frames
                    .iter()
                    .find(|f| matches!(&f.record, WalRecord::RunFinished { .. }))
                    .unwrap()
                    .seq;
                let next = frames.iter().find(|f| matches!(&f.record, WalRecord::RunStarted { run_id, .. } if run_id.0 != "held")).expect("queued submission runs").seq;
                assert!(
                    end < next,
                    "the held run ends before the queued send starts"
                );
            }
            if refused {
                assert_eq!(*terminals[0].0, RunFinishReason::Interrupted);
                assert_eq!(
                    terminals[0].1.as_ref().unwrap().provider_code.as_deref(),
                    Some("held_resume_refused")
                );
                assert!(frames.iter().any(|f| matches!(&f.record, WalRecord::ToolResult { result, outcome: broca_wal::DispatchOutcome::Completed, .. } if result.tool_call_id.0 == "a" && result.is_error && result.output == ToolOutput::Text { text: "remote_unavailable".into() })));
            }
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.record, WalRecord::ToolResult { .. }))
                    .count(),
                2
            );
            assert_eq!(
                plane.dispatches.load(Ordering::SeqCst),
                2,
                "never re-dispatch a held call"
            );
        }
        assert!(registry.stop_and_seal().await);
        let range = wal.read_records_range(&key, 0, None, None).unwrap();
        store.project_range(&key, &range).unwrap();
        if generation < restarts {
            use broca_core::MaterializedStore;
            let snapshot = store.read_admission_snapshot(&key).unwrap().unwrap();
            assert!(
                snapshot.watermark_seq > 1,
                "the checkpoint covers RunStarted and the inline turn"
            );
            assert!(
                snapshot.watermark_seq < wal.read_records(&key).unwrap().last().unwrap().seq,
                "the open held turn stays in the suffix"
            );
            let head = wal.read_records(&key).unwrap().last().unwrap().seq;
            let prior =
                broca_core::load_prior_wal_snapshot(wal.as_ref(), &key, head, Some(store.as_ref()))
                    .unwrap();
            assert!(
                prior.base_watermark > 0,
                "the restart must use the checkpoint-plus-tail loader"
            );
            assert!(
                !prior
                    .tail
                    .iter()
                    .any(|f| matches!(f.record, WalRecord::RunStarted { .. })),
                "the restart suffix must lack the episode identity"
            );
        }
    }
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn held_result_after_materialized_restart() {
    restarted_delivery(1, false, false).await;
}

#[tokio::test]
async fn held_result_after_two_materialized_restarts() {
    restarted_delivery(2, false, false).await;
}

#[tokio::test]
async fn held_result_after_restart_with_frozen_config_refusal() {
    restarted_delivery(1, true, false).await;
}

#[tokio::test]
async fn held_result_precedes_queued_send_after_restart() {
    restarted_delivery(1, false, true).await;
}

struct CountingWal {
    inner: Arc<FileWalStore>,
    full: AtomicUsize,
    ranges: AtomicUsize,
}

impl broca_core::WalStore for CountingWal {
    fn open(
        &self,
        key: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(key)
    }
    fn read_records(
        &self,
        key: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.full.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(key)
    }
    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records_range(key, after, through, anchor)
    }
}

#[tokio::test]
async fn finished_materialized_open_keeps_baseline_read_counts() {
    let root = broca_test_support::scratch_root()
        .join(format!("finished-materialized-open-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "broca", "finished");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap());
    let executor = Arc::new(Executor {
        wal: wal.clone(),
        store: store.clone(),
        slots: HeldSlots::new(),
        plane: Arc::new(HoldingPlane {
            dispatches: AtomicUsize::new(0),
        }),
        provider: FrozenProvider {
            inner: StubProvider::new(vec![step(
                ContentBlock::Text {
                    text: "done".into(),
                },
                FinishReason::Stop,
            )]),
            incompatible: false,
        },
    });
    let opener = || {
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            executor.clone(),
        )
        .with_materialized_store(store.clone())
    };
    let registry = LineageRegistry::new(Arc::new(opener()));
    registry
        .route_send(&key, text_req(&key, "finished"), RunKind::FreshOrResume)
        .await
        .unwrap();
    broca_test_support::wait_until("finished initial run", || async {
        wal.read_records(&key)
            .unwrap()
            .iter()
            .any(|f| matches!(f.record, WalRecord::RunFinished { .. }))
            .then_some(())
            .ok_or_else(|| "not finished".into())
    })
    .await;
    assert!(registry.stop_and_seal().await);
    store
        .project_range(&key, &wal.read_records_range(&key, 0, None, None).unwrap())
        .unwrap();
    let counted = Arc::new(CountingWal {
        inner: wal.clone(),
        full: AtomicUsize::new(0),
        ranges: AtomicUsize::new(0),
    });
    let registry = LineageRegistry::new(Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            counted.clone(),
            executor,
        )
        .with_materialized_store(store),
    ));
    registry.route_status(&key).await.unwrap();
    assert_eq!(
        counted.full.load(Ordering::SeqCst),
        0,
        "finished opens need no full read"
    );
    assert_eq!(
        counted.ranges.load(Ordering::SeqCst),
        1,
        "preserve the baseline range read"
    );
    assert!(registry.stop_and_seal().await);
    let _ = std::fs::remove_dir_all(root);
}
