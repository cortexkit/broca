use super::tests::{
    auth_failure, bind, deployment, idle, request, text_step, triple, Commands, ADDR,
};
use super::*;
use crate::session_cmd::SendKey;
use broca_core::provider::Provider;
use broca_core::{OutputEvent, RunError, RunSummary, StubProvider, StubToolPlane};
use broca_session_loop::account_move::{AccountContinue, Answer, Move, Wall};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

enum RoutingReply {
    Hold(Option<u64>),
    Refuse,
    Move,
}

struct ScriptedExecutor {
    clock: broca_core::clock::Clock,
    replies: std::sync::Mutex<VecDeque<RoutingReply>>,
    asks: AtomicUsize,
    admissions: AtomicUsize,
    fail_admission: AtomicBool,
    fail_after_move: AtomicBool,
    reached_model: Arc<Notify>,
    release_failure: Arc<Notify>,
}

struct Continuation<'a> {
    executor: &'a ScriptedExecutor,
    probe: bool,
    recheck: bool,
}

fn selected(account: &str) -> broca_wal::AuthSelection {
    serde_json::from_value(serde_json::json!({"credential_id":format!("credential-{account}"),"account_ref":account,"auth_method":"oauth",
        "account_decision":{"path":"routed","admission_id":"r1","decision_id":format!("decision-{account}"),"conversation_key":"conversation"}})).unwrap()
}

fn sign_in_error() -> broca_protocol::ProviderError {
    serde_json::from_value(serde_json::json!({"class":"auth_required","provider_code":"needs_reauth","message":"vault needs sign-in"})).unwrap()
}

struct PendingModel(Arc<Notify>);
#[async_trait::async_trait]
impl Provider for PendingModel {
    async fn step(
        &self,
        _: broca_protocol::StepId,
        _: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.0.notify_one();
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl AccountContinue<Box<dyn Provider>> for Continuation<'_> {
    async fn ask(&self, _: &Wall<'_>) -> Answer<Box<dyn Provider>> {
        self.executor.asks.fetch_add(1, Ordering::SeqCst);
        match self
            .executor
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted continuation")
        {
            RoutingReply::Hold(until_ms) => {
                Answer::Hold(broca_protocol::error::AccountContinueAnswer {
                    routing_code: "pool_wait".into(),
                    until_ms,
                })
            }
            RoutingReply::Refuse => {
                Answer::Stay(Some(broca_protocol::error::AccountContinueAnswer {
                    routing_code: "pin_unavailable".into(),
                    until_ms: None,
                }))
            }
            RoutingReply::Move => Answer::Move(Box::new(Move {
                selection: selected("B"),
                account: "B".into(),
                valid_until_ms: self.executor.clock.now_ms() + 600_000,
                render: None,
                provider: if self.executor.fail_after_move.load(Ordering::SeqCst) {
                    Box::new(PendingModel(self.executor.reached_model.clone()))
                } else {
                    Box::new(StubProvider::new(vec![text_step("answer", "done")]))
                },
            })),
        }
    }
    fn adopted(&self, _: &broca_wal::AuthSelection) {}
    fn recheck_sign_in(&self) -> bool {
        self.recheck
    }
    fn probe_only(&self) -> bool {
        self.probe
    }
}

#[async_trait::async_trait]
impl RunExecutor for ScriptedExecutor {
    async fn execute(
        &self,
        _: crate::executor::RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("the fixture uses the real new-store loop")
    }
    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.admissions.fetch_add(1, Ordering::SeqCst);
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        if mode != StoreRunMode::Fresh && self.fail_admission.load(Ordering::SeqCst) {
            return StoreRunEnd {
                parts,
                result: Err(StoreRunError::Admission(RunError::CredentialsUnavailable)),
            };
        }
        let first_run = req.run_id.0 == "r1";
        if mode == StoreRunMode::Fresh && first_run {
            control.auth_selection = Some(selected("A"));
        }
        let provider: Box<dyn Provider> = Box::new(StubProvider::new(if first_run {
            vec![auth_failure()]
        } else {
            vec![text_step("queued-answer", "done")]
        }));
        let tools = StubToolPlane::new().with_canned_text("read", "one item");
        let now = || self.clock.now_ms();
        let continuation = Continuation {
            executor: self,
            probe: mode == StoreRunMode::ProbeSignIn,
            recheck: matches!(
                mode,
                StoreRunMode::ProbeSignIn | StoreRunMode::RecheckSignIn
            ),
        };
        let result = async {
            let admission = if mode == StoreRunMode::Fresh {
                Some(broca_session_loop::admit::admission(&mut req, &control, &provider, writer.operations(), Some(now())).map_err(StoreRunError::Admission)?)
            } else { None };
            let seams = broca_session_loop::Seams {
                provider:&provider, tools:&tools, bind:req.session.clone(), out:Some(&out), cancel:control.cancel.clone().unwrap_or_default(), stop:Default::default(),
                held:None, approval:&broca_core::AllowAll, hook_plane:None, now_ms:&now, session_address:None, compaction_plane:None,
                compaction_session:None, compaction_changes:false, keep_warm:None, account_reports:None, max_same_account_wait:Default::default(),
                auth_selection:None, step_progress:None, model_call_ready:None,
                pause_for_auth_at_start:first_run.then(sign_in_error), account_continue:first_run.then_some(&continuation),
            };
            if let Some(admission) = admission {
                broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams).await.map_err(StoreRunError::Loop)
            } else {
                let resumed = broca_session_loop::resume(&mut writer, parts.as_ref(), &seams);
                tokio::pin!(resumed);
                tokio::select! {
                    result = &mut resumed => result.map_err(StoreRunError::Loop),
                    () = self.release_failure.notified(), if self.fail_after_move.load(Ordering::SeqCst) =>
                        Err(StoreRunError::Loop(broca_session_loop::LoopError::Parts("injected read failure after durable pause lift".into()))),
                }
            }
        }.await;
        StoreRunEnd { parts, result }
    }
}

struct Fixture {
    actor: StoreActor<&'static MemoryBackend>,
    executor: Arc<ScriptedExecutor>,
    root: std::path::PathBuf,
    backend: &'static MemoryBackend,
    parts: SharedParts,
}

impl Fixture {
    async fn new(label: &str, replies: Vec<RoutingReply>) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("sign-in-clock-{label}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
        let clock = broca_core::clock::Clock::for_state_root(&root).unwrap();
        let executor = Arc::new(ScriptedExecutor {
            clock: clock.clone(),
            replies: std::sync::Mutex::new(replies.into()),
            asks: AtomicUsize::new(0),
            admissions: AtomicUsize::new(0),
            fail_admission: AtomicBool::new(false),
            fail_after_move: AtomicBool::new(false),
            reached_model: Arc::default(),
            release_failure: Arc::default(),
        });
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let session = Session::create(
            backend,
            SessionAddr(ADDR.into()),
            triple(),
            &deployment(),
            false,
        )
        .await
        .unwrap();
        let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()))
            .with_retention_clock(clock);
        Self {
            actor,
            executor,
            root,
            backend,
            parts,
        }
    }
    fn time(&self, now: u64) {
        std::fs::write(
            self.root.join("test-clock"),
            serde_json::to_vec(&serde_json::json!({"now_ms":now})).unwrap(),
        )
        .unwrap();
        self.executor.clock.refresh().unwrap();
    }
    async fn check(&mut self) {
        let (reply, answer) = oneshot::channel();
        self.actor
            .handle(LineageCmd::Send {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                conversation_key: None,
                title: None,
                run_id: RunId("r1".into()),
                request: Box::new(request("first")),
                kind: RunKind::FreshOrResume,
                key: SendKey::RunId,
                skip_materialization: false,
                delivery: broca_protocol::Delivery::Queue,
                plan: None,
                order: None,
                reply,
            })
            .await;
        self.actor.run_current_to_end().await;
        answer.await.unwrap();
    }
    async fn wake(&mut self) {
        let timer = self
            .actor
            .account_limit_timer
            .clone()
            .expect("timed hold is armed");
        self.actor.wake_account_limit(timer).await.unwrap();
        self.actor.run_current_to_end().await;
    }
    fn next(&self) -> u64 {
        self.actor
            .account_limit_timer
            .as_ref()
            .unwrap()
            .resume_not_before_ms
    }
}

#[tokio::test]
async fn timed_holds_use_the_later_of_the_router_time_and_thirty_second_floor() {
    for (until, expected) in [(2_000, 31_000), (90_000, 90_000)] {
        let mut fixture = Fixture::new(
            &format!("timed-floor-{until}"),
            vec![RoutingReply::Hold(Some(until)), RoutingReply::Move],
        )
        .await;
        fixture.check().await;
        assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.next(),
            expected,
            "the answer was received at 1000 ms"
        );
        fixture.time(expected - 1);
        fixture.wake().await;
        assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 1);
        fixture.time(expected);
        fixture.wake().await;
        assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 2);
        assert_eq!(
            fixture
                .actor
                .session
                .operations()
                .run
                .as_ref()
                .unwrap()
                .finished,
            Some(broca_protocol::RunFinishReason::Completed)
        );
    }
}

#[tokio::test]
async fn an_untimed_hold_has_a_floor_but_never_schedules_a_timer() {
    let mut fixture = Fixture::new(
        "untimed-floor",
        vec![RoutingReply::Hold(None), RoutingReply::Move],
    )
    .await;
    fixture.check().await;
    assert!(fixture.actor.account_limit_timer.is_none());
    fixture.check().await;
    assert_eq!(
        fixture.executor.asks.load(Ordering::SeqCst),
        1,
        "an immediate keyed retry must not ask again"
    );
    fixture.time(30_999);
    fixture.check().await;
    assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 1);
    assert!(fixture.actor.account_limit_timer.is_none());
    fixture.time(31_000);
    assert!(
        fixture.actor.account_limit_timer.is_none(),
        "passing the floor does not make an untimed hold automatic"
    );
    fixture.check().await;
    assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .actor
            .session
            .operations()
            .run
            .as_ref()
            .unwrap()
            .finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

#[tokio::test]
async fn failed_timed_hold_admissions_double_the_delay_up_to_ten_minutes() {
    let mut fixture = Fixture::new(
        "failed-admission-backoff",
        vec![RoutingReply::Hold(Some(2_000)), RoutingReply::Move],
    )
    .await;
    fixture.check().await;
    fixture
        .executor
        .fail_admission
        .store(true, Ordering::SeqCst);
    let schedule = [
        (31_000, 61_000),
        (61_000, 121_000),
        (121_000, 241_000),
        (241_000, 481_000),
        (481_000, 961_000),
        (961_000, 1_561_000),
        (1_561_000, 2_161_000),
    ];
    for (index, (now, expected)) in schedule.into_iter().enumerate() {
        fixture.time(now);
        fixture.wake().await;
        assert_eq!(
            fixture.executor.admissions.load(Ordering::SeqCst),
            index + 2
        );
        assert_eq!(
            fixture.executor.asks.load(Ordering::SeqCst),
            1,
            "failed admission is not an exchange"
        );
        assert_eq!(
            fixture.next(),
            expected,
            "backoff after failure {}",
            index + 1
        );
        assert_eq!(
            fixture
                .actor
                .session
                .operations()
                .run
                .as_ref()
                .unwrap()
                .paused,
            Some(broca_protocol::PauseReason::AuthRequired)
        );
    }
    fixture
        .executor
        .fail_admission
        .store(false, Ordering::SeqCst);
    fixture.time(2_161_000);
    fixture.wake().await;
    assert_eq!(fixture.executor.asks.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .actor
            .session
            .operations()
            .run
            .as_ref()
            .unwrap()
            .finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

#[tokio::test]
async fn a_probe_loop_error_after_pause_lift_ends_the_orphan_and_drains_the_queue() {
    let mut fixture = Fixture::new(
        "post-lift-cleanup",
        vec![RoutingReply::Refuse, RoutingReply::Move],
    )
    .await;
    fixture.check().await;
    let Fixture {
        actor,
        executor,
        backend,
        parts,
        ..
    } = fixture;
    drop(actor);
    executor.fail_after_move.store(true, Ordering::SeqCst);
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts))
        .with_retention_clock(executor.clock.clone());
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(actor.serve(rx));
    executor.reached_model.notified().await;
    let mut queued_request = request("queued");
    queued_request.tools = None;
    let queued = tests::send_with(&commands, "r2", queued_request, SendKey::Minted).await;
    assert!(matches!(queued, crate::actor::SendOutcome::Pending { .. }));
    executor.release_failure.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(
        run.run_id, "r2",
        "the failed unpaused r1 must not block queued work"
    );
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    let records = session.open_log_records().await.unwrap();
    let resumed = records
        .iter()
        .position(|(_, record)| matches!(record, Record::Resumed(_)))
        .unwrap();
    let ended = records.iter().position(|(_, record)| matches!(record, Record::RunEnd(end) if end.run_id == "r1" && end.reason == broca_protocol::RunFinishReason::Error)).unwrap();
    assert!(resumed < ended);
}
