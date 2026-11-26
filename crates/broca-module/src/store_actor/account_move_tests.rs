//! A run that moved to another account at a quota wall and was cut off by a crash after the
//! move was durable, before anything was sent on the new account. The session's next open
//! decides what becomes of it through the store actor's own recovery decision
//! (`recovery_mode`), as any run cut off inside a step: it is sealed as a crash's orphan, ending
//! `Interrupted` with the crash named, and never sent to the new account.

use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::Provider;
use broca_core::OutputEvent;
use broca_protocol::provider::{
    AccountLimitDisposition, AttemptAccountLimit, AttemptAccountObservation, ProviderControl,
};
use broca_protocol::request::CallOptions;
use broca_protocol::{AttemptOutcome, ModelAttempt, NormalizedEvent, StepId};
use broca_session_loop::account_move::{AccountContinue, Answer, Move, Wall};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::record::Record;
use serde_json::json;
use tokio::sync::{mpsc, Notify};

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;

use super::tests::{bind, deployment, idle, send, triple, Commands, ADDR};
use super::StoreActor;
use crate::actor::SendOutcome;
use crate::executor::{RunExecutor, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode};

/// The selection routing gave the run for `account`.
fn routed(account: &str) -> broca_records::record::AuthSelection {
    serde_json::from_value(json!({
        "credential_id": format!("cred-{account}"),
        "account_ref": account,
        "resolved_credential_id": format!("cred-{account}"),
        "auth_method": "oauth",
        "account_decision": {
            "path": "routed",
            "admission_id": "r1",
            "decision_id": format!("decision-{account}"),
            "conversation_key": "broca-session:conversation",
        },
    }))
    .unwrap()
}

fn now_ms() -> u64 {
    broca_core::clock::epoch_millis().unwrap_or(0)
}

/// Account A: its first request hits the seven-day wall, resetting in three days.
struct Walls;

#[async_trait]
impl Provider for Walls {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let now = now_ms();
        let attempt = ModelAttempt {
            attempt: 0,
            request_body_len: 10,
            request_body_sha256: "0".repeat(64),
            http_status: Some(429),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: "0".repeat(64),
            response_body_cap: None,
            outcome: AttemptOutcome::HttpError,
            ts_ms: now,
            content_progress: Default::default(),
            sent_at_ms: Some(now),
            transport: None,
            websocket_request: None,
            cache_evidence: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            credential_refresh: None,
            served_account_ref: Some("A".into()),
            account_observation: Box::new(AttemptAccountObservation {
                failure_received_at_ms: None,
                ratelimit_headers: Default::default(),
                limit: Some(AttemptAccountLimit {
                    disposition: AccountLimitDisposition::ACCOUNT,
                    observed_at_ms: now,
                    resets_at_ms: Some(now + 3 * 86_400_000),
                    resets_at_source: None,
                    limit_window: Some("seven_day".into()),
                }),
            }),
        };
        let error = serde_json::from_value(json!({
            "class": "transient",
            "message": "the account's usage window is spent",
            "status": 429,
            "provider_code": "account_limit_exhausted",
            "resets_at_ms": now + 3 * 86_400_000,
            "limit_window": "seven_day",
        }))
        .unwrap();
        let (tx, rx) = mpsc::channel(4);
        tx.send(NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt,
        }))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Error {
            error,
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }

    fn reports_attempt_evidence(&self) -> bool {
        true
    }
}

/// Account B: the process dies as its request is about to go out. The call says it was reached
/// and never returns, so nothing about it is ever recorded.
struct DiesBeforeSending(Arc<Notify>);

#[async_trait]
impl Provider for DiesBeforeSending {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.0.notify_one();
        std::future::pending().await
    }
}

/// Routing, offering B at the wall.
struct OffersB(Arc<Notify>);

#[async_trait]
impl AccountContinue<Box<dyn Provider>> for OffersB {
    async fn ask(&self, _wall: &Wall<'_>) -> Answer<Box<dyn Provider>> {
        Answer::Move(Box::new(Move {
            selection: routed("B"),
            account: "B".into(),
            valid_until_ms: now_ms() + 300_000,
            render: None,
            provider: Box::new(DiesBeforeSending(Arc::clone(&self.0))),
        }))
    }

    fn adopted(&self, _selection: &broca_records::record::AuthSelection) {}
}

/// Runs a fresh store run on A, with routing that moves it to B; seals a run the actor's
/// recovery decides to seal.
struct MovingExecutor {
    reached_b: Arc<Notify>,
}

#[async_trait]
impl RunExecutor for MovingExecutor {
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
        control.auth_selection = Some(routed("A"));
        let provider: Box<dyn Provider> = Box::new(Walls);
        let routing = OffersB(Arc::clone(&self.reached_b));
        let result = async {
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "contents");
            let now = now_ms;
            let seams = broca_session_loop::Seams {
                steer_yields: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &broca_core::AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: None,
                compaction_session: None,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: Some(routed("A")),
                step_progress: control.step_progress.clone(),
                accepting_drain: None,
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: Some(&routing),
            };
            match mode {
                StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &provider,
                        broca_session_loop::writer::Writer::operations(&writer),
                        Some(now_ms()),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                        .map_err(StoreRunError::Loop)
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause)
                        .await
                        .map_err(StoreRunError::Loop)
                }
                StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn => {
                    panic!("a run cut off mid-step is sealed, not resumed")
                }
            }
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// A crash after the move's records are durable and before the request on B: the session's
/// next open seals the run as a crash's orphan, through the actor's own recovery decision. The
/// run ends `Interrupted` naming the crash, its records name B as the account it moved to, and
/// no attempt on B exists.
#[tokio::test]
async fn a_run_cut_off_after_its_move_is_sealed_on_open_and_never_sent_to_the_new_account() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let reached_b = Arc::new(Notify::new());
    let executor = Arc::new(MovingExecutor {
        reached_b: Arc::clone(&reached_b),
    });
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::clone(&executor) as Arc<dyn RunExecutor>,
        Box::new(parts.clone()),
    )
    .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let commands = Commands::over(commands, backend);
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    // The run reached B's request: the move is durable, nothing is sent yet. The process dies.
    tokio::time::timeout(std::time::Duration::from_secs(30), reached_b.notified())
        .await
        .expect("the run moved to B");
    served.abort();
    let _ = served.await;

    // What the next open decides, from the run's records alone.
    let loaded = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let run = loaded.operations.run.clone().expect("the run");
    assert_eq!(
        super::recovery_mode(&run),
        Some(StoreRunMode::Seal(broca_core::seal::SealCause::CrashOrphan))
    );
    assert_eq!(
        run.refrozen_auth,
        Some(serde_json::to_value(routed("B")).unwrap())
    );
    let actor = StoreActor::new(
        bind(),
        loaded,
        Arc::clone(&executor) as Arc<dyn RunExecutor>,
        Box::new(parts.clone()),
    )
    .with_readers(Arc::new(move || backend));
    let (reopened, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    idle(&reopened).await;
    drop(reopened);
    let session = served.await.unwrap();
    let records = session.open_log_records().await.unwrap();
    let moved = records
        .iter()
        .position(|(_, record)| matches!(record, Record::AuthRefrozen(_)))
        .expect("the move is durable");
    let ends: Vec<_> = records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 1, "{ends:?}");
    assert_eq!(ends[0].reason, broca_protocol::RunFinishReason::Interrupted);
    assert_eq!(ends[0].interrupt_reason.as_deref(), Some("runner_crash"));
    let served_by: Vec<_> = records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::Attempt(attempt) => Some(attempt.served_account_ref.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        served_by,
        vec![Some("A".to_owned())],
        "nothing was sent on B"
    );
    assert!(
        records[..moved]
            .iter()
            .any(|(_, record)| matches!(record, Record::Attempt(_))),
        "the refused attempt on A precedes the move"
    );
}
