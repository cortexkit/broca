//! Held results must survive a restart after either admission store has checkpointed earlier turns.

use super::tests::text_req;
use super::*;
use crate::held_slots::{HeldSlots, SlotDelivery};
use crate::late_results_puller::HeldDelivery;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use async_trait::async_trait;
use broca_core::tool_plane::{DispatchKey, LateResultRegistration, ProviderHeldReply};
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore, StubProvider, ToolManifest,
    ToolPlane, ToolPlaneError,
};
use broca_core::{LeaseStore, Persister};
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
    store: Option<Arc<broca_store::SqliteStore>>,
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
            self.store
                .clone()
                .map(|store| store as Arc<dyn broca_core::SessionStore>),
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

struct ObservedAdmissionReader {
    inner: Arc<dyn broca_core::context::AdmissionReader>,
    bounded_pauses: Arc<AtomicUsize>,
}

impl broca_core::context::AdmissionReader for ObservedAdmissionReader {
    fn prior_snapshot(
        &self,
        session: &BindIdentity,
        safe_head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        let snapshot = self.inner.prior_snapshot(session, safe_head)?;
        if snapshot
            .checkpoint_at_head
            .last_episode
            .as_ref()
            .is_some_and(|episode| {
                episode.disposition == EpisodeDisposition::Paused
                    && episode.pause_reason == Some(broca_protocol::PauseReason::ToolHeld)
            })
        {
            assert!(
                snapshot.base_watermark > 0,
                "held recovery must exercise a stored seed"
            );
            assert!(
                !snapshot
                    .tail
                    .iter()
                    .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. })),
                "the held episode starts below the admission watermark"
            );
            self.bounded_pauses.fetch_add(1, Ordering::SeqCst);
        }
        Ok(snapshot)
    }

    fn paused_episode(
        &self,
        session: &BindIdentity,
        safe_head: u64,
    ) -> Result<Option<broca_core::admission::LastEpisodeV1>, String> {
        self.inner.paused_episode(session, safe_head)
    }
}

struct DatabaseAdmissionOpener {
    lease_store: Arc<FileLeaseStore>,
    wal: Arc<CountingWal>,
    db: Arc<broca_store::db::OverhaulDb>,
    executor: Arc<dyn RunExecutor>,
    bounded_pauses: Arc<AtomicUsize>,
}

#[async_trait]
impl crate::registry::LineageOpener for DatabaseAdmissionOpener {
    async fn open(
        &self,
        key: &crate::registry::LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, crate::registry::OpenError> {
        use crate::registry::OpenError;
        let lease = self
            .lease_store
            .acquire(&broca_core::lease_key_for(key))
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let (fanout, observer) = crate::fanout::Fanout::new(self.wal.clone(), key.clone());
        let (publications, published) = tokio::sync::watch::channel(None);
        let (writer, lease) = WalHandle::open_and_spawn_observed(
            self.wal.clone(),
            key.clone(),
            lease,
            Some(observer.with_publications(publications)),
        )
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?;
        let writer = Arc::new(writer);
        let db = self.db.clone();
        let reader = self.db.admission_reader(move |session| {
            let publication = published
                .borrow()
                .clone()
                .ok_or("writer has no synced publication")?;
            db.publish(
                session,
                "held-seeded",
                broca_store::db::LogLocation::Live,
                &publication,
            )
            .map_err(|e| e.to_string())?;
            Ok(publication)
        });
        Ok(LineageActor::spawn_with_admission_reader(
            key.clone(),
            gen,
            lease,
            writer,
            self.executor.clone(),
            self.wal.clone(),
            registry,
            Duration::from_secs(60),
            fanout,
            None,
            Arc::new(ObservedAdmissionReader {
                inner: reader,
                bounded_pauses: self.bounded_pauses.clone(),
            }),
        ))
    }
}

async fn restarted_delivery(restarts: usize, refused: bool, queued: bool, seeded: bool) {
    let root = broca_test_support::scratch_root().join(format!(
        "held-admission-restarts-{restarts}-{refused}-{queued}-{seeded}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("project")).unwrap();
    let key = BindIdentity::new(root.join("project"), "broca", "held");
    let wal = Arc::new(FileWalStore::with_config(
        root.join("wal"),
        if seeded {
            broca_wal::WalConfig::OVERHAUL
        } else {
            broca_wal::WalConfig::LEGACY
        },
    ));
    let store =
        (!seeded).then(|| Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap()));
    let db = seeded.then(|| {
        std::fs::write(root.join("state-format"), "broca-state/2").unwrap();
        Arc::new(broca_store::db::OverhaulDb::open(&root).unwrap())
    });
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
        let counted = Arc::new(CountingWal {
            inner: wal.clone(),
            full: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            publication_floor: seeded.then(|| root.join("wal-floor")),
        });
        let bounded_pauses = Arc::new(AtomicUsize::new(0));
        let opener: Arc<dyn crate::registry::LineageOpener> = if let Some(db) = &db {
            Arc::new(DatabaseAdmissionOpener {
                lease_store: Arc::new(FileLeaseStore::new(root.join("leases"))),
                wal: counted.clone(),
                db: db.clone(),
                executor,
                bounded_pauses: bounded_pauses.clone(),
            })
        } else {
            Arc::new(
                DefaultOpener::new(
                    Arc::new(FileLeaseStore::new(root.join("leases"))),
                    wal.clone(),
                    executor,
                )
                .with_materialized_store(store.as_ref().unwrap().clone()),
            )
        };
        let registry = LineageRegistry::new(opener);
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
            if seeded {
                let sweep = registry.mark_crash_orphans(
                    wal.as_ref(),
                    vec![(RunId("held".into()), key.clone())],
                    std::time::SystemTime::now() + Duration::from_secs(1),
                );
                assert_eq!(
                    sweep.marked, 1,
                    "exercise the crash-orphan recovery selector too"
                );
                let sweep = registry.seal_marked_crash_orphans(sweep).await;
                assert_eq!(sweep.open_failed, 0, "{sweep:?}");
                assert_eq!(sweep.sealed, 0, "a held episode is not a crash orphan");
            }
            registry.route_status(&key).await.unwrap();
            if seeded {
                assert!(
                    bounded_pauses.load(Ordering::SeqCst) > 0,
                    "recovery must consume a bounded admission snapshot"
                );
                assert!(
                    counted.full.load(Ordering::SeqCst) > 0,
                    "unfinished checkpointed recovery must read the whole log"
                );
            }
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
            if seeded {
                assert!(frames.iter().any(|f| matches!(&f.record, WalRecord::ToolResult { result, outcome: broca_wal::DispatchOutcome::Completed, .. } if result.tool_call_id.0 == "a" && result.is_error && result.output == ToolOutput::Text { text: "remote_unavailable".into() })),
                    "the resumed held batch must durably receive its delivered result");
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
        if let Some(store) = &store {
            let range = wal.read_records_range(&key, 0, None, None).unwrap();
            store.project_range(&key, &range).unwrap();
        }
        if generation < restarts && !seeded {
            let store = store.as_ref().unwrap();
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
    if seeded {
        assert!(
            !root.join("store.db").exists(),
            "database admission needs no legacy checkpoint store"
        );
    }
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn held_result_after_materialized_restart() {
    restarted_delivery(1, false, false, false).await;
}

#[tokio::test]
async fn held_result_after_two_materialized_restarts() {
    restarted_delivery(2, false, false, false).await;
}

#[tokio::test]
async fn held_result_after_restart_with_frozen_config_refusal() {
    restarted_delivery(1, true, false, false).await;
}

#[tokio::test]
async fn held_result_precedes_queued_send_after_restart() {
    restarted_delivery(1, false, true, false).await;
}

#[tokio::test]
async fn held_result_below_database_admission_watermark_survives_restart() {
    restarted_delivery(1, false, false, true).await;
}

#[tokio::test]
async fn held_result_below_database_admission_watermark_survives_two_restarts() {
    restarted_delivery(2, false, false, true).await;
}

struct CountingWal {
    inner: Arc<FileWalStore>,
    full: AtomicUsize,
    ranges: AtomicUsize,
    publication_floor: Option<std::path::PathBuf>,
}

impl broca_core::WalStore for CountingWal {
    fn writer_marker_root(&self) -> Option<std::path::PathBuf> {
        self.inner.writer_marker_root()
    }

    fn wal_config(&self) -> broca_wal::WalConfig {
        self.inner.wal_config()
    }

    fn open(
        &self,
        key: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        if let Some(floor) = &self.publication_floor {
            // Expose FileWal::durable_publication, the byte end proven by this descriptor's
            // sync, without changing the file store's read-side path or frame format.
            Ok(Box::new(
                broca_wal::FileWal::open(self.inner.wal_path(key))?
                    .with_wal_config(self.inner.wal_config())?
                    .with_feature_floor(floor),
            ))
        } else {
            self.inner.open(key)
        }
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
        store: Some(store.clone()),
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
        publication_floor: None,
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
