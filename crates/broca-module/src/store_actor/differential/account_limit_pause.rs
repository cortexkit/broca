//! Timed account pauses through both lease-owning actors, with a shared controllable clock.
use super::*;
use crate::serve::ServeHandler;
use broca_core::{Clock, Provider, StubToolPlane};
use broca_protocol::{provider::ProviderControl, StepId};
use broca_session_loop::writer::Writer;
use broca_session_store::keys::{Cipher, DeploymentKey};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const RESET: u64 = 61_000;

#[tokio::test]
async fn run_awaits_account_pause_keeps_the_future_floor_on_all_surfaces() {
    for store in [true] {
        let side = Side::new(store).await;
        let now = crate::store_awaits::now_ms();
        side.set_time(now);
        side.executor
            .provider
            .reset_at
            .store(now + 60_000, Ordering::SeqCst);
        side.executor
            .provider
            .accepted_at
            .store(now, Ordering::SeqCst);
        assert!(matches!(
            side.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Active { .. }
        ));
        side.executor.provider.release_limit.notify_one();
        side.idle().await;
        let body = crate::store_actor::awaits_tests::prove(
            &side.registry,
            &side.key,
            if store {
                "account-limit.after"
            } else {
                "account-limit.before"
            },
            store.then_some(("paused", "runner", Some("account_limit"))),
        )
        .await;
        if store {
            assert_eq!(body["awaits"]["next_ms"], body["resume_not_before_ms"]);
            assert!(body["awaits"]["next_ms"].as_u64().unwrap() > now);
        }
        side.stop().await;
    }
}

struct Limited {
    reset_at: std::sync::atomic::AtomicU64,
    accepted_at: std::sync::atomic::AtomicU64,
    require_bind: AtomicBool,
    bound: AtomicBool,
    clock: Clock,
    calls: AtomicUsize,
    release_limit: Arc<Notify>,
    release_success: Arc<Notify>,
    sent: std::sync::Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl Provider for Limited {
    fn bind_run_transport(&self, _: &BindIdentity) {
        self.bound.store(true, Ordering::SeqCst);
    }

    async fn step(
        &self,
        step: StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
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
        _: StepId,
        opts: Arc<broca_protocol::request::CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(&opts);
        self.sent.lock().unwrap().push(bytes.clone());
        let limited = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let mut attempt: broca_protocol::ModelAttempt = serde_json::from_value(json!({
            "attempt": runtime.first_attempt, "request_body_len": bytes.len(),
            "request_body_sha256": broca_core::render::request_digest_hex(&bytes),
            "response_body_len": 0, "response_body_sha256": "0".repeat(64),
            "outcome": if limited {"http_error"} else {"complete"}, "ts_ms": self.clock.now_ms()
        }))
        .unwrap();
        if limited {
            attempt.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: self.clock.now_ms(),
                resets_at_ms: Some(self.reset_at.load(Ordering::SeqCst)),
                resets_at_source: Some("body".into()),
                limit_window: None,
            });
        }
        let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt,
        })];
        if limited {
            events.push(NormalizedEvent::Control(ProviderControl::Error {
                error: serde_json::from_value(
                    json!({"class":"transient", "message":"account window exhausted",
                    "status":429, "provider_code":"account_limit_exhausted", "resets_at_ms":self.reset_at.load(Ordering::SeqCst)}),
                )
                .unwrap(),
                retries_used: 0,
            }));
        } else {
            events.extend(super::super::tests::text_step("answer", "done"));
        }
        let gate = if limited {
            self.release_limit.clone()
        } else {
            self.release_success.clone()
        };
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            gate.notified().await;
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
        // An earlier successful request on this account left a cache that survives the reset.
        (account == "acct-a"
            && (!self.require_bind.load(Ordering::SeqCst) || self.bound.load(Ordering::SeqCst)))
        .then(|| self.accepted_at.load(Ordering::SeqCst))
    }
}

fn selection() -> broca_wal::AuthSelection {
    serde_json::from_value(json!({"credential_id":"oauth:stub:work", "account_ref":"acct-a"}))
        .unwrap()
}

type FrozenRequest = (
    RunId,
    Option<broca_protocol::request::KeepWarm>,
    Option<broca_protocol::request::OnRestart>,
);

struct Executor {
    provider: Limited,
    refuse: AtomicBool,
    signed_out: AtomicBool,
    requests: std::sync::Mutex<Vec<FrozenRequest>>,
}

#[async_trait]
impl RunExecutor for Executor {
    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        use crate::executor::{StoreRunEnd, StoreRunError, StoreRunMode};
        let crate::executor::StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        self.requests.lock().unwrap().push((
            req.run_id.clone(),
            req.keep_warm.clone(),
            req.on_restart,
        ));
        let result = async {
            if self.refuse.swap(false, Ordering::SeqCst) {
                return Err(StoreRunError::Admission(RunError::Config(
                    "provider unavailable for resume".into(),
                )));
            }
            control.account_limit_pause = true;
            control.auth_selection = Some(selection());
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
            let now = || self.provider.clock.now_ms();
            let seams = broca_session_loop::run::Seams {
                person_steers: None,
                provider: &self.provider,
                tools: &StubToolPlane::new(),
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.unwrap_or_default(),
                stop: control.stop_requested.unwrap_or_default(),
                held: None,
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
                auth_selection: Some(selection()),
                step_progress: control.step_progress,
                model_call_ready: None,
                pause_for_auth_at_start: self.sign_in_error(),
                account_continue: None,
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
    executor: Arc<Executor>,
    store: bool,
}

impl Side {
    async fn open(
        dir: std::path::PathBuf,
        key: BindIdentity,
        store: bool,
        first_call: usize,
    ) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let leases = Arc::new(FileLeaseStore::new(dir.join("leases")));
        let service =
            Arc::new(crate::retention::RetentionService::new(&dir, leases.clone()).unwrap());
        let executor = Arc::new(Executor {
            provider: Limited {
                reset_at: std::sync::atomic::AtomicU64::new(RESET),
                accepted_at: std::sync::atomic::AtomicU64::new(1000),
                require_bind: AtomicBool::new(false),
                bound: AtomicBool::new(false),
                clock: service.clock.clone(),
                calls: AtomicUsize::new(first_call),
                release_limit: Arc::default(),
                release_success: Arc::default(),
                sent: Default::default(),
            },
            refuse: AtomicBool::new(false),
            signed_out: AtomicBool::new(false),
            requests: Default::default(),
        });
        let runtime = crate::store_open::StoreRuntime::open(
            &dir,
            DeploymentKey::new(1, &[7; 32]).creating(Cipher::None),
            executor.clone(),
            IDLE,
        )
        .await
        .unwrap();
        let opener = crate::opener::DefaultOpener::new(leases, executor.clone())
            .with_idle_ttl(IDLE)
            .with_store_runtime(Arc::new(runtime));
        let registry = LineageRegistry::new(Arc::new(opener));
        registry.set_retention_service(service);
        registry.enable_restart_resumes();
        registry.recover_store_crash_orphans().await.unwrap();
        Self {
            dir,
            key,
            registry,
            executor,
            store,
        }
    }
    async fn new(store: bool) -> Self {
        let dir = scratch_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
        let key = BindIdentity::new(dir.join("project"), "account-comparison", "s1");
        Self::open(dir, key, store, 0).await
    }
    fn set_time(&self, now: u64) {
        std::fs::write(
            self.dir.join("test-clock"),
            serde_json::to_vec(&json!({"now_ms":now})).unwrap(),
        )
        .unwrap();
        self.executor.provider.clock.refresh().unwrap();
    }
    async fn send(&self, run: &str, delivery: broca_protocol::Delivery) -> SendOutcome {
        let mut request = request("hello");
        request.tools = None;
        request.policy_allowed.clear();
        request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run.into()),
                request,
                RunKind::FreshOrResume,
                SendKey::SendId(run.into()),
                false,
                delivery,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap()
    }
    async fn idle(&self) {
        broca_test_support::wait_until("account actor parks", || async {
            match self.registry.route_status(&self.key).await.unwrap() {
                LineageStatus::Idle => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }
    async fn state(&self, run: &str) -> Value {
        serde_json::to_value(
            self.registry
                .run_status(&self.key, RunId(run.into()))
                .await
                .unwrap(),
        )
        .unwrap()
    }
    async fn wait_state(&self, run: &str, state: &str) -> Value {
        broca_test_support::wait_until("account run state", || async {
            let value = self.state(run).await;
            if value["state"] == state {
                Ok(value)
            } else {
                Err(format!(
                    "store={}, run={run}, expected={state}: {value}",
                    self.store
                ))
            }
        })
        .await
    }
    async fn paused(&self) {
        assert!(matches!(
            self.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Active { .. }
        ));
        self.executor.provider.release_limit.notify_one();
        let state = self.wait_state("r1", "paused").await;
        assert_eq!(state["reason"], "account_limit");
        assert_eq!(state["resume_not_before_ms"], RESET);
        self.idle().await;
        // The head shows the same resume time while the run is paused, in the session store,
        // where heads live. Today's storage leaves the optional member out of its head.
        let head = self
            .registry
            .head_for(&self.key, None, &broca_protocol::SendScope::default())
            .await
            .unwrap();
        let last = head.last_run_state.expect("the paused run is the latest");
        assert_eq!(last.state, "paused");
        assert_eq!(last.reason.as_deref(), Some("account_limit"));
        assert_eq!(
            last.resume_not_before_ms,
            self.store.then_some(RESET),
            "store={}",
            self.store
        );
    }
    async fn calls(&self, expected: usize) {
        broca_test_support::wait_until("account provider entered", || async {
            let calls = self.executor.provider.calls.load(Ordering::SeqCst);
            if calls == expected {
                Ok(())
            } else {
                Err(format!("calls={calls}, expected={expected}"))
            }
        })
        .await;
    }
    async fn stop(&self) {
        assert!(
            self.registry
                .stop_and_seal_with_budget(Duration::ZERO, broca_test_support::EVENT_HANG_GUARD)
                .await
        );
    }

    async fn records(&self) -> Vec<Value> {
        let mut result = Vec::new();
        if self.store {
            use broca_session_store::{
                backend::{LocalBackend, SessionAddr},
                record::Record,
            };
            let runtime = self.registry.store_runtime().unwrap();
            let class = runtime.class_of(&self.key).unwrap().unwrap();
            let session = Session::load(
                LocalBackend::new(class.store_root(&self.dir)),
                SessionAddr(broca_core::session_addr(&self.key)),
                &crate::store_open::triple(&self.key),
                runtime.deployment(),
            )
            .await
            .unwrap();
            for (_, record) in session.records_from(0).await.unwrap() {
                match record {
                    Record::RunStart(start) => result.push(json!([
                        "start",
                        start.run_id,
                        start.keep_warm,
                        start.on_restart
                    ])),
                    Record::Pause(pause) => result.push(json!([
                        "pause",
                        pause.run_id,
                        pause.reason,
                        pause.error,
                        normalized_pause((pause.account_limit).map(|json| json.parse()))
                    ])),
                    Record::Resumed(resumed) => result.push(json!(["resume", resumed.run_id])),
                    Record::RunEnd(end) => result.push(json!([
                        "end",
                        end.run_id,
                        end.reason,
                        end.error
                            .as_ref()
                            .map(|error| error.parse()["provider_code"].clone())
                    ])),
                    _ => {}
                }
            }
        }
        result
    }
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn account_limit_armed_store_actor_does_not_idle_close() {
    let side = Side::new(true).await;
    side.paused().await;
    assert!(side
        .registry
        .idle_holds(&side.key)
        .contains(&"account_limit_pause"));
    tokio::time::pause();
    tokio::time::advance(IDLE * 2).await;
    tokio::task::yield_now().await;
    assert_eq!(side.registry.slot_state(&side.key), Some("open"));
    assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 1);
    tokio::time::resume();
    side.stop().await;
}

#[tokio::test]
async fn account_limit_timer_requires_the_durable_source_evidence() {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        record::Record,
    };
    let side = Side::new(true).await;
    side.paused().await;
    side.stop().await;
    let runtime = side.registry.store_runtime().unwrap();
    let class = runtime.class_of(&side.key).unwrap().unwrap();
    let session = Session::load(
        LocalBackend::new(class.store_root(&side.dir)),
        SessionAddr(broca_core::session_addr(&side.key)),
        &crate::store_open::triple(&side.key),
        runtime.deployment(),
    )
    .await
    .unwrap();
    let records = session.records_from(0).await.unwrap();
    let pause = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Pause(pause) => Some(pause.clone()),
            _ => None,
        })
        .unwrap();
    let attempt = records
        .into_iter()
        .find_map(|(_, record)| match record {
            Record::Attempt(attempt) => Some(attempt),
            _ => None,
        })
        .unwrap();
    let mut actor = super::super::StoreActor::new(
        side.key.clone(),
        session,
        side.executor.clone(),
        Box::new(runtime.parts_for(class, Cipher::None).clone()),
    )
    .with_retention_clock(side.executor.provider.clock.clone());
    let original = actor.current_account_limit().await.unwrap().unwrap();
    side.set_time(RESET - 1);
    actor.wake_account_limit(original.clone()).await.unwrap();
    assert!(actor.run.is_none());
    assert!(actor.account_limit_timer.is_some());
    let not_ready = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(side.dir.join("leases"))),
        side.executor.clone(),
    )));
    assert!(!not_ready.handle().restart_resume_ready());
    actor.registry = Some(not_ready.handle());
    side.set_time(RESET);
    actor.wake_account_limit(original.clone()).await.unwrap();
    assert!(actor.run.is_none());
    assert!(actor.account_limit_timer.is_some());
    actor.registry = None;
    // Each changed field denies authorization independently, even though the run stays paused.
    for field in [
        "credential_id",
        "failed_attempt_seq",
        "failed_step",
        "attempt",
        "request_body_len",
        "request_body_sha256",
        "attempt_id",
    ] {
        let mut changed = pause.clone();
        let mut evidence = changed.account_limit.as_ref().unwrap().parse();
        evidence[field] = match field {
            "credential_id" => json!("other"),
            "request_body_sha256" => json!("1".repeat(64)),
            "attempt_id" => json!(broca_core::attempt_id::mint_attempt_id().unwrap().as_str()),
            _ => json!(999999),
        };
        changed.account_limit = Some(evidence.into());
        actor.append_own(Record::Pause(changed)).await.unwrap();
        assert!(actor.current_account_limit().await.is_err(), "{field}");
    }
    let mut missing = pause.clone();
    missing.account_limit = None;
    actor.append_own(Record::Pause(missing)).await.unwrap();
    assert!(actor
        .current_account_limit()
        .await
        .unwrap_err()
        .contains("no recovery evidence"));
    let mut permanent = pause.clone();
    let mut error = permanent.error.as_ref().unwrap().parse();
    error["class"] = json!("permanent");
    permanent.error = Some(error.into());
    actor.append_own(Record::Pause(permanent)).await.unwrap();
    assert!(actor
        .current_account_limit()
        .await
        .unwrap_err()
        .contains("transient source error"));
    let mut unclassified = attempt;
    unclassified.billing = None;
    let seq = actor.session.next_seq();
    actor
        .append_own(Record::Attempt(unclassified))
        .await
        .unwrap();
    let mut unnamed = pause.clone();
    let mut limit = unnamed.account_limit.as_ref().unwrap().parse();
    limit["failed_attempt_seq"] = json!(seq);
    unnamed.account_limit = Some(limit.into());
    actor.append_own(Record::Pause(unnamed)).await.unwrap();
    assert!(actor
        .current_account_limit()
        .await
        .unwrap_err()
        .contains("source-classified failed attempt"));
    actor.append_own(Record::Pause(pause)).await.unwrap();
    side.set_time(RESET);
    // A stale hint cannot authorize a different pause sequence, even after its reset.
    actor.wake_account_limit(original).await.unwrap();
    assert!(actor.run.is_none());
    assert!(actor.account_limit_timer.is_some());
    actor
        .append_own(Record::CancelRequest(
            broca_session_store::record::CancelRequest {
                run_id: "r1".into(),
                at: broca_session_store::record::CancelPoint::Now,
            },
        ))
        .await
        .unwrap();
    assert!(actor.current_account_limit().await.unwrap().is_none());
}

fn normalized_pause(pause: Option<Value>) -> Option<Value> {
    pause.map(|mut pause| {
        // Sequence numbers belong to each storage's record stream; attempts get fresh identities.
        let object = pause.as_object_mut().unwrap();
        object.remove("failed_attempt_seq");
        object.remove("attempt_id");
        pause
    })
}

#[tokio::test]
async fn account_limit_timer_waits_then_resumes_the_same_run_alike() {
    let mut histories = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        side.paused().await;
        side.set_time(RESET - 1);
        side.idle().await;
        assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 1);
        assert!(side
            .registry
            .idle_holds(&side.key)
            .contains(&"account_limit_pause"));
        assert!(matches!(
            side.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Paused { .. }
        ));
        assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 1);
        side.set_time(RESET);
        side.calls(2).await;
        assert_eq!(side.state("r1").await["state"], "active");
        let requests = side.executor.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        broca_test_support::pin!(
            "the timer restores the run id and recorded policies",
            requests[1]
        );
        assert_eq!(
            requests[1], requests[0],
            "the timer restores the run id and recorded policies"
        );
        let sent = side.executor.provider.sent.lock().unwrap().clone();
        broca_test_support::pin!("the frozen request is sent again", sent[1]);
        assert_eq!(sent[1], sent[0], "the frozen request is sent again");
        side.executor.provider.release_success.notify_one();
        side.wait_state("r1", "completed").await;
        side.idle().await;
        side.stop().await;
        histories.push(side.records().await);
    }
    broca_test_support::pin!("alike", histories[0]);
}

#[tokio::test]
async fn account_limit_timer_rebuilds_when_the_actor_reopens_alike() {
    let mut histories = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        side.paused().await;
        side.stop().await;
        let reopened = Side::open(side.dir.clone(), side.key.clone(), store, 1).await;
        reopened.registry.actor_for(&reopened.key).await.unwrap();
        reopened.idle().await;
        reopened.set_time(RESET - 1);
        reopened.idle().await;
        assert_eq!(reopened.executor.provider.calls.load(Ordering::SeqCst), 1);
        reopened.set_time(RESET);
        reopened.calls(2).await;
        reopened.executor.provider.release_success.notify_one();
        reopened.wait_state("r1", "completed").await;
        reopened.idle().await;
        reopened.stop().await;
        histories.push(reopened.records().await);
    }
    broca_test_support::pin!("alike", histories[0]);
}

#[tokio::test]
async fn account_limit_cancel_disarms_the_timer_alike() {
    let mut histories = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        side.paused().await;
        assert_eq!(
            side.registry
                .route_cancel(&side.key, RunId("r1".into()))
                .await
                .unwrap(),
            crate::actor::CancelOutcome::Ack
        );
        side.wait_state("r1", "cancelled").await;
        side.idle().await;
        side.set_time(RESET);
        side.idle().await;
        assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 1);
        assert!(!side
            .registry
            .idle_holds(&side.key)
            .contains(&"account_limit_pause"));
        side.stop().await;
        histories.push(side.records().await);
    }
    broca_test_support::pin!("alike", histories[0]);
}

#[tokio::test]
async fn account_limit_refused_resume_seals_and_drains_alike() {
    let mut histories = Vec::new();
    let mut reads = Vec::new();
    let mut terminals = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        assert!(matches!(
            side.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Active { .. }
        ));
        side.calls(1).await;
        let queued = side.send("r2", broca_protocol::Delivery::Queue).await;
        let SendOutcome::Pending { submission_id } = queued else {
            panic!("{queued:?}");
        };
        side.executor.provider.release_limit.notify_one();
        side.wait_state("r1", "paused").await;
        side.idle().await;
        let (sink, mut deliveries) = mpsc::channel(4096);
        assert_eq!(
            side.registry
                .route_subscribe(&side.key, crate::fanout::FromCursor::Start, sink)
                .await
                .unwrap(),
            crate::actor::SubscribeOutcome::Attached
        );
        side.executor.refuse.store(true, Ordering::SeqCst);
        side.set_time(RESET);
        let state = side.wait_state("r1", "interrupted").await;
        assert_eq!(state["error"]["class"], "permanent");
        assert_eq!(
            state["error"]["provider_code"],
            "account_limit_resume_refused"
        );
        assert_eq!(state["reason"], "runner_crash");
        let result = serde_json::to_value(
            side.registry
                .run_result(&side.key, RunId("r1".into()))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["reason"], "runner_crash");
        reads.push((without_error_message(state), without_error_message(result)));
        let terminal = broca_test_support::wait_until("refused resume control terminal", || {
            let mut found = None;
            while let Ok(delivery) = deliveries.try_recv() {
                if let crate::fanout::Delivery::Control(event) = delivery {
                    if matches!(&event.1, broca_protocol::caller::ControlUnit::RunFinished { run_id: Some(id), .. } if id.0 == "r1") {
                        found = Some(without_error_message(serde_json::to_value(&event.1).unwrap()));
                        break;
                    }
                }
            }
            async move { found.ok_or_else(|| "no terminal delivered".to_owned()) }
        }).await;
        terminals.push(terminal);
        side.calls(2).await;
        let next = crate::actor::run_id_of_queued(&submission_id);
        assert_eq!(side.state(&next.0).await["state"], "active");
        side.executor.provider.release_success.notify_one();
        side.wait_state(&next.0, "completed").await;
        side.idle().await;
        side.stop().await;
        let records = side.records().await;
        // The old seal also writes a resume record before the interrupted end. The session store
        // writes only the end because the refused resume ran no step. Compare the end and caller
        // reads instead of requiring a resume record for work that never resumed.
        histories.push(
            records
                .into_iter()
                .filter(|record| record[0] != "resume")
                .map(|mut record| {
                    if record[1] == next.0 {
                        record[1] = json!("queued");
                    }
                    record
                })
                .collect::<Vec<_>>(),
        );
    }
    broca_test_support::pin!("alike", histories[0]);
    broca_test_support::pin!("alike", reads[0]);
    broca_test_support::pin!("alike", terminals[0]);
}

#[tokio::test]
async fn account_limit_interrupt_disarms_the_store_timer_and_drains() {
    let side = Side::new(true).await;
    side.paused().await;
    let answer = side.send("r2", broca_protocol::Delivery::Interrupt).await;
    let SendOutcome::Pending { submission_id } = answer else {
        panic!("{answer:?}");
    };
    side.wait_state("r1", "cancelled").await;
    side.calls(2).await;
    let next = crate::actor::run_id_of_queued(&submission_id);
    assert_eq!(side.state(&next.0).await["state"], "active");
    assert!(!side
        .registry
        .idle_holds(&side.key)
        .contains(&"account_limit_pause"));
    side.set_time(RESET);
    side.executor.provider.release_success.notify_one();
    side.wait_state(&next.0, "completed").await;
    side.idle().await;
    assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 2);
    side.stop().await;
}

fn without_error_message(mut value: Value) -> Value {
    fn normalize(value: &mut Value) {
        match value {
            Value::Object(object) => {
                object.remove("awaits");
                // Each actor describes its own failed admission, but retains the same typed error.
                if object.contains_key("class") {
                    object.remove("message");
                }
                for value in object.values_mut() {
                    normalize(value);
                }
            }
            Value::Array(values) => values.iter_mut().for_each(normalize),
            _ => {}
        }
    }
    normalize(&mut value);
    value
}

impl Executor {
    fn sign_in_error(&self) -> Option<broca_protocol::ProviderError> {
        self.signed_out.load(Ordering::SeqCst).then(|| {
            serde_json::from_value(
                json!({"class":"auth", "message":"Sign in again", "provider_code":"needs_reauth"}),
            )
            .unwrap()
        })
    }
}

#[tokio::test]
async fn account_limit_wake_pauses_for_sign_in_alike() {
    let mut histories = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        side.paused().await;
        side.executor.signed_out.store(true, Ordering::SeqCst);
        side.set_time(RESET);
        broca_test_support::wait_until("account wake needs sign-in", || async {
            let status = side.state("r1").await;
            if status["reason"] == "auth_required" {
                Ok(())
            } else {
                Err(format!("{status}"))
            }
        })
        .await;
        side.idle().await;
        assert_eq!(side.executor.provider.calls.load(Ordering::SeqCst), 1);
        assert!(!side
            .registry
            .idle_holds(&side.key)
            .contains(&"account_limit_pause"));
        side.executor.signed_out.store(false, Ordering::SeqCst);
        assert!(matches!(
            side.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Active { .. }
        ));
        side.calls(2).await;
        let sent = side.executor.provider.sent.lock().unwrap().clone();
        broca_test_support::pin!("alike", sent[1]);
        assert_eq!(sent[1], sent[0]);
        side.executor.provider.release_success.notify_one();
        side.wait_state("r1", "completed").await;
        side.idle().await;
        side.stop().await;
        histories.push(side.records().await);
    }
    broca_test_support::pin!("alike", histories[0]);
}

#[tokio::test]
async fn account_limit_pause_uses_bound_session_evidence_on_both_storages() {
    let mut pauses = Vec::new();
    for store in [true] {
        let side = Side::new(store).await;
        side.executor
            .provider
            .require_bind
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            side.send("r1", broca_protocol::Delivery::Queue).await,
            SendOutcome::Active { .. }
        ));
        side.executor.provider.release_limit.notify_one();
        let state = broca_test_support::wait_until("account limit outcome", || async {
            let state = side.state("r1").await;
            if state["state"] == "active" {
                Err("run is still active".to_owned())
            } else {
                Ok(state)
            }
        })
        .await;
        assert_eq!(state["state"], "paused", "store={store}: {state}");
        assert_eq!(state["reason"], "account_limit");
        assert_eq!(state["resume_not_before_ms"], RESET);
        pauses.push((
            state["reason"].clone(),
            state["resume_not_before_ms"].clone(),
        ));
        side.stop().await;
    }
    broca_test_support::pin!("alike", pauses[0]);
}
