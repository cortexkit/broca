use super::*;
use crate::{DefaultOpener, LineageRegistry, RunExecutor, RunJob, RunKind};
use broca_core::{
    AllowAll, Clock, FileLeaseStore, FileWalStore, Provider, RunSummary, StartMode, StubToolPlane,
};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    FinishReason, MessageId, StepId, Usage,
};
use broca_store::SqliteStore;
use sha2::Digest;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

struct LimitedProvider {
    clock: Clock,
    reject: AtomicBool,
    sent: Mutex<Vec<Vec<u8>>>,
    hold_success: AtomicBool,
    release_success: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Provider for LimitedProvider {
    async fn step(&self, step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.step_with_runtime(
            step,
            Arc::new(opts.clone()),
            broca_core::provider::StepRuntime {
                cancel: Default::default(),
                deadline: None,
                seal: None,
                progress: None,
                first_attempt: 0,
                attempt_hook: None,
            },
        )
        .await
    }

    async fn step_with_runtime(
        &self,
        _step: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(&opts);
        self.sent.lock().unwrap().push(bytes.clone());
        let rejected = self.reject.swap(false, Ordering::SeqCst);
        let observed = self.clock.now_ms();
        let mut attempt: broca_protocol::ModelAttempt = serde_json::from_value(serde_json::json!({
            "attempt":runtime.first_attempt, "request_body_len":bytes.len(),
            "request_body_sha256":format!("{:x}", sha2::Sha256::digest(&bytes)),
            "response_body_len":0, "response_body_sha256":"0".repeat(64),
            "outcome":if rejected {"http_error"} else {"complete"}, "ts_ms":observed
        }))
        .unwrap();
        if rejected {
            attempt.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: observed,
                resets_at_ms: Some(61_000),
                resets_at_source: Some("body".into()),
                limit_window: None,
            });
        }
        let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt,
        })];
        if rejected {
            events.push(NormalizedEvent::Control(ProviderControl::Error {
                error: serde_json::from_value(serde_json::json!({
                    "class":"transient", "message":"account window exhausted", "status":429,
                    "provider_code":"account_limit_exhausted", "resets_at_ms":61_000
                }))
                .unwrap(),
                retries_used: 0,
            }));
        } else {
            events.extend([
                NormalizedEvent::Control(ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: MessageId("answer".into()),
                        content: vec![ContentBlock::Text {
                            text: "done".into(),
                        }],
                    },
                }),
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage: Usage::default(),
                    finish_reason: FinishReason::Stop,
                    retries_used: 0,
                }),
            ]);
        }
        let (tx, rx) = mpsc::channel(32);
        let held = (!rejected && self.hold_success.load(Ordering::SeqCst))
            .then(|| self.release_success.clone());
        tokio::spawn(async move {
            if let Some(held) = held {
                held.notified().await;
            }
            for event in events {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
    fn reports_attempt_evidence(&self) -> bool {
        true
    }
    fn last_accepted_at_ms(&self, account: &str) -> Option<u64> {
        // The timer fixture starts with a cached account from an earlier session run.
        (account == "acct-a").then_some(1000)
    }
}

struct Executor {
    provider: Arc<LimitedProvider>,
    wal: Arc<FileWalStore>,
}

#[async_trait::async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        job.control.account_limit_pause = true;
        job.control.clock = Some(self.provider.clock.clone());
        job.control.auth_selection = Some(
            serde_json::from_value(
                serde_json::json!({"credential_id":"oauth:stub:work","account_ref":"acct-a"}),
            )
            .unwrap(),
        );
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            self.provider.as_ref(),
            &StubToolPlane::new(),
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
}

struct Fixture {
    root: std::path::PathBuf,
    key: broca_protocol::BindIdentity,
    registry: LineageRegistry,
    wal: Arc<FileWalStore>,
    provider: Arc<LimitedProvider>,
    clock: Clock,
}

impl Fixture {
    async fn over(root: std::path::PathBuf, reject: bool) -> Self {
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = broca_protocol::BindIdentity::new(root.join("project"), "runner", "timed");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
        let store = Arc::new(SqliteStore::open(root.join("index.db")).unwrap());
        let service = Arc::new(
            crate::retention::RetentionService::new(
                wal.clone(),
                store.clone(),
                leases.clone(),
                store,
                Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
            )
            .unwrap(),
        );
        let clock = service.clock.clone();
        let provider = Arc::new(LimitedProvider {
            clock: clock.clone(),
            reject: AtomicBool::new(reject),
            sent: Mutex::new(Vec::new()),
            hold_success: AtomicBool::new(false),
            release_success: Arc::new(tokio::sync::Notify::new()),
        });
        let registry = LineageRegistry::new_with_reader(
            Arc::new(DefaultOpener::new(
                leases,
                wal.clone(),
                Arc::new(Executor {
                    provider: provider.clone(),
                    wal: wal.clone(),
                }),
            )),
            wal.clone(),
            None,
        );
        registry.set_retention_service(service);
        registry
            .resume_restart_pauses(wal.clone(), Vec::new())
            .await;
        Self {
            root,
            key,
            registry,
            wal,
            provider,
            clock,
        }
    }
    async fn new(name: &str) -> Self {
        let root = broca_test_support::scratch_root().join(name);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
        Self::over(root, true).await
    }
    fn set_time(&self, now: u64) {
        std::fs::write(
            self.root.join("test-clock"),
            serde_json::to_vec(&serde_json::json!({"now_ms":now})).unwrap(),
        )
        .unwrap();
        self.clock.refresh().unwrap();
    }
    async fn state(&self, expected: broca_core::RunIndexState) -> broca_core::ReconciledRunState {
        broca_test_support::wait_until("timed account pause state", || async {
            let state = self
                .registry
                .route_run_state(&self.key, RunId("timed-run".into()))
                .await
                .unwrap()
                .unwrap();
            if state.state == expected {
                Ok(state)
            } else {
                Err(format!("state={:?}, expected={expected:?}", state.state))
            }
        })
        .await
    }
    async fn start(&self) {
        self.registry
            .route_send(
                &self.key,
                super::tests::text_req(&self.key, "timed-run"),
                RunKind::FreshOrResume,
            )
            .await
            .unwrap();
        self.state(broca_core::RunIndexState::Paused).await;
    }
    async fn stop(&self) {
        assert!(
            self.registry
                .stop_and_seal_with_budget(Duration::ZERO, broca_test_support::EVENT_HANG_GUARD)
                .await
        );
    }
    fn records(&self) -> Vec<broca_wal::FramedRecord> {
        broca_core::WalStore::read_records(self.wal.as_ref(), &self.key).unwrap()
    }
}

#[tokio::test]
async fn restart_while_account_paused_rebuilds_timer_and_resumes_same_request() {
    let first = Fixture::new("timed-account-restart").await;
    first.start().await;
    assert_eq!(first.clock.now_ms(), 1000);
    let failed = first.provider.sent.lock().unwrap()[0].clone();
    first.stop().await;
    assert!(matches!(
        broca_core::replay(
            &first
                .records()
                .into_iter()
                .map(|f| f.record)
                .collect::<Vec<_>>()
        )
        .unwrap()
        .resolution,
        broca_core::ReplayResolution::AuthRequired {
            reason: broca_protocol::PauseReason::AccountLimit,
            ..
        }
    ));
    let next = Fixture::over(first.root.clone(), false).await;
    next.registry
        .resume_restart_pauses(
            next.wal.clone(),
            vec![("timed-run".into(), next.key.clone())],
        )
        .await;
    next.set_time(60_999);
    let paused = next.state(broca_core::RunIndexState::Paused).await;
    assert_eq!(
        paused.account_limit_pause.unwrap().resume_not_before_ms,
        61_000
    );
    assert!(next.provider.sent.lock().unwrap().is_empty());
    next.set_time(500);
    next.state(broca_core::RunIndexState::Paused).await;
    assert!(next.provider.sent.lock().unwrap().is_empty());
    next.set_time(61_000);
    next.state(broca_core::RunIndexState::Completed).await;
    assert_eq!(*next.provider.sent.lock().unwrap(), vec![failed]);
    let frames = next.records();
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.record, WalRecord::RunStarted { .. }))
            .count(),
        1
    );
    let attempts: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.record {
            WalRecord::ModelAttemptFinished { attempt, .. } => Some(*attempt),
            _ => None,
        })
        .collect();
    assert_eq!(attempts, [0, 1]);
    next.stop().await;
}

#[tokio::test]
async fn cancel_of_an_account_pause_invalidates_the_timer() {
    let fixture = Fixture::new("timed-account-cancel").await;
    fixture.start().await;
    fixture
        .registry
        .route_cancel(&fixture.key, RunId("timed-run".into()))
        .await
        .unwrap();
    fixture.state(broca_core::RunIndexState::Cancelled).await;
    fixture.set_time(61_000);
    fixture.state(broca_core::RunIndexState::Cancelled).await;
    assert_eq!(fixture.provider.sent.lock().unwrap().len(), 1);
    fixture.stop().await;
}

#[tokio::test]
async fn expired_account_pause_on_boot_resumes_once_even_with_duplicate_recovery() {
    let first = Fixture::new("timed-account-expired-boot").await;
    first.start().await;
    first.stop().await;
    first.set_time(62_000);
    let next = Fixture::over(first.root.clone(), false).await;
    let candidates = vec![("timed-run".into(), next.key.clone())];
    next.registry
        .resume_restart_pauses(next.wal.clone(), candidates.clone())
        .await;
    next.state(broca_core::RunIndexState::Completed).await;
    next.registry
        .resume_restart_pauses(next.wal.clone(), candidates)
        .await;
    assert_eq!(next.provider.sent.lock().unwrap().len(), 1);
    assert_eq!(
        next.records()
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        1
    );
    next.stop().await;
}

#[tokio::test]
async fn account_pause_replays_its_handle_and_refuses_unrelated_sends() {
    let fixture = Fixture::new("timed-account-send-ownership").await;
    fixture.start().await;
    let own = fixture
        .registry
        .route_send(
            &fixture.key,
            super::tests::text_req(&fixture.key, "timed-run"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(
        own,
        SendOutcome::Paused {
            reason: broca_protocol::PauseReason::AccountLimit,
            ..
        }
    ));
    let other = fixture
        .registry
        .route_send(
            &fixture.key,
            super::tests::text_req(&fixture.key, "unrelated"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(
        other,
        SendOutcome::RunPaused {
            reason: broca_protocol::PauseReason::AccountLimit,
            ..
        }
    ));
    assert_eq!(fixture.provider.sent.lock().unwrap().len(), 1);
    assert!(!fixture
        .records()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::Queued { .. })));
    fixture.stop().await;
}

fn sample(name: &str, value: &impl serde::Serialize) {
    if let Ok(directory) = std::env::var("BROCA_ACCOUNT_LIMIT_SAMPLE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut bytes = serde_json::to_vec(value).unwrap();
        bytes.push(b'\n');
        std::fs::write(directory.join(name), bytes).unwrap();
    }
}

async fn subscribed_control(
    rx: &mut mpsc::Receiver<crate::fanout::Delivery>,
    paused: bool,
) -> broca_wire::SubscribeEvent {
    broca_test_support::wait_for(
        "account pause subscription continuation",
        async {
            loop {
                let delivery = rx
                    .recv()
                    .await
                    .expect("subscription stays attached across the pause");
                if let crate::fanout::Delivery::Control(event) = delivery {
                    let matches = if paused {
                        matches!(
                            event.1,
                            broca_protocol::ControlUnit::Paused {
                                reason: broca_protocol::PauseReason::AccountLimit,
                                ..
                            }
                        )
                    } else {
                        matches!(event.1, broca_protocol::ControlUnit::StepStarted { .. })
                    };
                    if matches {
                        return broca_wire::SubscribeEvent::Control {
                            wal_seq: event.0.wal_seq,
                            sub_index: event.0.sub_index,
                            unit: event.1.clone(),
                        };
                    }
                }
            }
        },
        || "no matching durable control on the original subscription".into(),
    )
    .await
}

#[tokio::test]
async fn account_pause_serializer_samples_and_live_subscription_continue_together() {
    use crate::serve::{ServeHandler, SubscribeFrom};
    let fixture = Fixture::new("timed-account-serializer-samples").await;
    let (tx, mut rx) = mpsc::channel(64);
    ServeHandler::subscribe(&fixture.registry, &fixture.key, SubscribeFrom::Live, tx)
        .await
        .unwrap();
    fixture.start().await;
    let paused = subscribed_control(&mut rx, true).await;
    let status =
        ServeHandler::run_status(&fixture.registry, &fixture.key, RunId("timed-run".into()))
            .await
            .unwrap();
    let head = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap();
    let status_json = serde_json::to_value(&status).unwrap();
    assert_eq!(status_json["state"], "paused");
    assert_eq!(status_json["pause_reason"], "account_limit");
    assert_eq!(status_json["resume_not_before_ms"], 61_000);
    assert_eq!(
        head.last_run_state.as_ref().unwrap().reason.as_deref(),
        Some("account_limit")
    );
    assert_eq!(head.last_ordinal, Some(0));
    assert!(serde_json::to_value(&head).unwrap()["last_run_state"]
        .get("resume_not_before_ms")
        .is_none());
    sample("run.status.paused.json", &status);
    sample("session.head.paused.json", &head);
    sample("subscribe.paused.json", &paused);
    let frames = fixture.records();
    let record = frames
        .iter()
        .find(|frame| {
            matches!(
                frame.record,
                WalRecord::RunPaused {
                    reason: broca_protocol::PauseReason::AccountLimit,
                    ..
                }
            )
        })
        .unwrap();
    sample("run_paused.account_limit.json", &record.record);
    fixture.provider.hold_success.store(true, Ordering::SeqCst);
    fixture.set_time(61_000);
    let resumed = subscribed_control(&mut rx, false).await;
    fixture.state(broca_core::RunIndexState::Active).await;
    let status =
        ServeHandler::run_status(&fixture.registry, &fixture.key, RunId("timed-run".into()))
            .await
            .unwrap();
    let head = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(&status).unwrap()["state"], "active");
    assert!(serde_json::to_value(&status)
        .unwrap()
        .get("resume_not_before_ms")
        .is_none());
    assert_eq!(head.last_run_state.as_ref().unwrap().run_id, "timed-run");
    assert_eq!(head.last_run_state.as_ref().unwrap().state, "active");
    assert_eq!(head.last_ordinal, Some(0));
    sample("run.status.resumed.json", &status);
    sample("session.head.resumed.json", &head);
    sample("subscribe.resumed.json", &resumed);
    fixture.provider.release_success.notify_one();
    fixture.state(broca_core::RunIndexState::Completed).await;
    fixture.stop().await;
}
