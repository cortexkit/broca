//! The same sends through today's session actor and the store actor, compared reply by reply.
//!
//! Today's side is the module's real path: a registry over a file lease store and a file WAL,
//! today's actor, and today's loop ([`broca_core::run_loop_on_writer_admitted_with_prior_snapshot`])
//! over a scripted provider. The store side is [`super::StoreActor`] over the new loop and the
//! same script. Where the store actor serves a send, both replies must be equal. Where it doesn't
//! yet, the scenario says so, and the store actor must refuse it by name while today's actor
//! serves it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore, FileWalStore,
    OutputEvent, RunError, RunSummary, StartMode, StubToolPlane, WalStore,
};
use broca_protocol::content::ContentBlock;
use broca_protocol::{BindIdentity, NormalizedEvent, RunId, SubmissionRequest};
use broca_session_store::backend::MemoryBackend;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{actor, auth_failure, executor, request, send_as, texts, Gated};
use crate::actor::{LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, RunKind};
use crate::registry::LineageRegistry;

/// Today's executor in this harness: today's loop over the scripted provider, on the writer the
/// actor hands it.
struct TodayLoop {
    provider: Gated,
    wal: Arc<FileWalStore>,
}

#[async_trait]
impl RunExecutor for TodayLoop {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        let tools = StubToolPlane::new().with_canned_text("read", "file contents");
        Box::pin(run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &self.provider,
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
}

/// Today's module over a scratch folder, which it removes when dropped.
struct Today {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    wal: Arc<FileWalStore>,
    key: BindIdentity,
}

impl Drop for Today {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn today(steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> Today {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-actor-differential-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor = Arc::new(TodayLoop {
        provider: Gated {
            inner: broca_core::provider::StubProvider::new(steps),
            gate,
        },
        wal: Arc::clone(&wal),
    });
    // A reader over the WAL alone: `session.read` serves from the log when no store is given.
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(Duration::from_secs(60)),
        ),
        wal.clone(),
        None,
    );
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Today {
        dir,
        registry,
        wal,
        key,
    }
}

impl Today {
    async fn send(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        key: SendKey,
        delivery: broca_protocol::Delivery,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                key,
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
            .expect("today's actor opens")
    }

    async fn idle(&self) {
        for _ in 0..500 {
            if self.registry.route_status(&self.key).await.unwrap() == LineageStatus::Idle {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("today's session never went idle");
    }

    /// What each run's start recorded about its send, in the shape [`store_starts`] reads from
    /// the store session.
    fn starts(&self) -> Vec<serde_json::Value> {
        self.wal
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .filter_map(|frame| match frame.record {
                broca_wal::WalRecord::RunStarted {
                    run_id,
                    submission_id,
                    send_id,
                    request_identity_digest,
                    title,
                    sent_by,
                    principal,
                    config,
                    ..
                } => Some(serde_json::json!({
                    "run_id": run_id.0,
                    "submission_id": submission_id.map(|id| id.0),
                    "send_id": send_id,
                    "request_identity_digest": request_identity_digest,
                    "title": title,
                    "sent_by": sent_by,
                    "principal": principal,
                    "model": config.model,
                })),
                _ => None,
            })
            .collect()
    }

    /// The text of every message today's served history holds, in order, as [`texts`] reads
    /// the store session's conversation. Read through `session.read`'s own path, so a steer or a
    /// queued send lands where today's actor places it.
    async fn texts(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut from = None;
        loop {
            let page = self
                .registry
                .route_read(&self.key, from, None)
                .await
                .expect("today's session reads");
            for message in &page.messages {
                out.extend(
                    message
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text { text } => Some(text.clone()),
                            _ => None,
                        }),
                );
            }
            match page.next_from_ordinal {
                Some(next) if !page.messages.is_empty() => from = Some(next),
                _ => return out,
            }
        }
    }
}

/// How a step's two replies must relate.
#[derive(Clone, Copy)]
enum Expect {
    /// Both actors give this same answer.
    Same,
    /// Today's actor serves it; the store actor refuses it by name, naming this.
    StoreRefuses(&'static str),
}

/// One step of a scenario, done on both sides.
enum Step {
    Send {
        run_id: &'static str,
        text: &'static str,
        key: Option<&'static str>,
        route_decision: Option<&'static str>,
        /// Changes the request before it is sent, the same on both sides.
        tweak: Option<fn(&mut SubmissionRequest)>,
        delivery: broca_protocol::Delivery,
        expect: Expect,
    },
    /// Retracts the queued send `submission_id` on both sides; the replies must be equal.
    Retract(&'static str),
    /// Lets the gated first model step through on both sides.
    Release,
    /// Waits until both sessions are idle.
    Idle,
}

fn send(run_id: &'static str, text: &'static str) -> Step {
    Step::Send {
        run_id,
        text,
        key: None,
        route_decision: None,
        tweak: None,
        delivery: broca_protocol::Delivery::Queue,
        expect: Expect::Same,
    }
}

/// A send keyed by the caller's `send_id` `key`, whose run id is `r-<key>` as the serve layer
/// derives it.
fn keyed(key: &'static str, text: &'static str) -> Step {
    Step::Send {
        run_id: Box::leak(format!("r-{key}").into_boxed_str()),
        text,
        key: Some(key),
        route_decision: None,
        tweak: None,
        delivery: broca_protocol::Delivery::Queue,
        expect: Expect::Same,
    }
}

fn expecting(mut step: Step, expected: Expect) -> Step {
    if let Step::Send { expect, .. } = &mut step {
        *expect = expected;
    }
    step
}

fn routed(mut step: Step, decision: &'static str) -> Step {
    if let Step::Send { route_decision, .. } = &mut step {
        *route_decision = Some(decision);
    }
    step
}

fn steered(mut step: Step) -> Step {
    if let Step::Send { delivery, .. } = &mut step {
        *delivery = broca_protocol::Delivery::Steer;
    }
    step
}

fn tweaked(mut step: Step, change: fn(&mut SubmissionRequest)) -> Step {
    if let Step::Send { tweak, .. } = &mut step {
        *tweak = Some(change);
    }
    step
}

/// Runs `steps` on both sides over the same provider script, the first model step gated when
/// `gated`. Every reply is compared as its step says; when every step is served alike, the two
/// sessions' conversations must also be the same.
async fn compare(script: Vec<Vec<NormalizedEvent>>, gated: bool, steps: Vec<Step>) {
    let today_gate = gated.then(|| Arc::new(Notify::new()));
    let store_gate = gated.then(|| Arc::new(Notify::new()));
    let today = today(script.clone(), today_gate.clone());
    let (commands, served, _) = actor(executor(script, store_gate.clone())).await;
    let mut all_same = true;
    for (index, step) in steps.into_iter().enumerate() {
        match step {
            Step::Send {
                run_id,
                text,
                key,
                route_decision,
                tweak,
                delivery,
                expect,
            } => {
                let mut sent = request(text);
                sent.route_decision_id = route_decision.map(str::to_owned);
                if let Some(tweak) = tweak {
                    tweak(&mut sent);
                }
                let send_key = || match key {
                    Some(key) => SendKey::SendId(key.into()),
                    None => SendKey::Minted,
                };
                let old = today.send(run_id, sent.clone(), send_key(), delivery).await;
                let new = send_as(&commands, run_id, sent, send_key(), delivery).await;
                match expect {
                    Expect::Same => assert_eq!(new, old, "step {index}: replies differ"),
                    Expect::StoreRefuses(what) => {
                        all_same = false;
                        assert!(
                            !matches!(old, SendOutcome::Error(_)),
                            "step {index}: today's actor must serve it, got {old:?}"
                        );
                        assert_eq!(
                            new,
                            SendOutcome::Error(super::unserved(what)),
                            "step {index}: the store actor must refuse it by name"
                        );
                    }
                }
            }
            Step::Retract(submission_id) => {
                let old = today
                    .registry
                    .route_retract(
                        &today.key,
                        broca_protocol::SubmissionId(submission_id.into()),
                    )
                    .await
                    .expect("today's actor opens");
                let new = super::tests::retract(&commands, submission_id).await;
                assert_eq!(new, old, "step {index}: retraction replies differ");
            }
            Step::Release => {
                for gate in today_gate.iter().chain(store_gate.iter()) {
                    gate.notify_one();
                }
            }
            Step::Idle => {
                today.idle().await;
                super::tests::idle(&commands).await;
            }
        }
    }
    drop(commands);
    let session: Session<&'static MemoryBackend> = served.await.unwrap();
    if all_same {
        assert_eq!(
            texts(&session),
            today.texts().await,
            "the conversations differ"
        );
        assert_eq!(
            store_starts(&session).await,
            today.starts(),
            "the run starts record the sends differently"
        );
    }
    assert!(today.registry.stop_and_seal().await);
}

/// What each run's start in `session` recorded about its send, in the shape [`Today::starts`]
/// reads from today's log.
async fn store_starts(session: &Session<&'static MemoryBackend>) -> Vec<serde_json::Value> {
    use broca_session_store::record::Record;
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) => Some(serde_json::json!({
                "run_id": start.run_id,
                "submission_id": start.submission_id,
                "send_id": start.send_id,
                "request_identity_digest": start.request_identity_digest,
                "title": start.title,
                "sent_by": start
                    .run_choices
                    .as_ref()
                    .and_then(|choices| choices.pointer("/record/sent_by"))
                    .cloned(),
                "principal": start.principal,
                "model": start.model,
            })),
            _ => None,
        })
        .collect()
}

fn hello() -> Vec<Vec<NormalizedEvent>> {
    vec![
        super::tests::text_step("a1", "hello"),
        super::tests::text_step("a2", "again"),
    ]
}

#[tokio::test]
async fn two_plain_sends_run_alike() {
    compare(
        hello(),
        false,
        vec![
            send("r1", "first"),
            Step::Idle,
            send("r2", "second"),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_finished_keyed_send_answers_its_retry_alike() {
    compare(
        hello(),
        false,
        vec![keyed("k1", "first"), Step::Idle, keyed("k1", "first")],
    )
    .await;
}

#[tokio::test]
async fn a_retry_with_another_payload_is_refused_alike() {
    compare(
        hello(),
        false,
        vec![keyed("k1", "first"), Step::Idle, keyed("k1", "changed")],
    )
    .await;
}

#[tokio::test]
async fn a_retry_of_the_running_run_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            keyed("k1", "first"),
            keyed("k1", "first"),
            Step::Release,
            Step::Idle,
            keyed("k1", "first"),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_retry_of_an_earlier_run_is_answered_alike() {
    compare(
        hello(),
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            send("r2", "second"),
            Step::Idle,
            keyed("k1", "first"),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_running_runs_routing_decision_is_fixed_alike() {
    compare(
        hello(),
        true,
        vec![
            routed(keyed("k1", "first"), "decision-1"),
            routed(keyed("k1", "first"), "decision-2"),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A run paused for sign-in owns the session: a new send is refused naming it, alike. Today's
/// actor drives the paused run again on its own retry, which the store actor doesn't serve yet.
#[tokio::test]
async fn a_send_behind_a_sign_in_pause_is_refused_alike() {
    compare(
        vec![auth_failure()],
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            send("r2", "second"),
            expecting(
                keyed("k1", "first"),
                Expect::StoreRefuses("driving an unfinished run again by its retry"),
            ),
        ],
    )
    .await;
}

/// A send while a run runs is queued, answered `pending`, and runs after that run, alike.
#[tokio::test]
async fn a_send_while_a_run_runs_is_queued_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            send("r2", "second"),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A queued send's retry is `pending`, and one with another payload is refused naming what
/// differs, alike; once it ran, its retry is answered from its run.
#[tokio::test]
async fn a_queued_sends_retry_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            keyed("k1", "first"),
            keyed("k2", "second"),
            keyed("k2", "second"),
            keyed("k2", "changed"),
            Step::Release,
            Step::Release,
            Step::Idle,
            keyed("k2", "second"),
        ],
    )
    .await;
}

fn leaves_its_model(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("stub", "another");
    request.inherit_frozen = [broca_protocol::request::InheritedParam::Model]
        .into_iter()
        .collect();
}

/// A plain send that leaves its model to the session is refused, before and after the session's
/// first run, alike: only a plan's model selection lets a send leave it out, and these sessions
/// have no plan.
#[tokio::test]
async fn a_send_leaving_its_model_to_the_session_runs_alike() {
    compare(
        vec![
            super::tests::text_step("a1", "hello"),
            super::tests::text_step("a2", "again"),
            super::tests::text_step("a3", "once more"),
        ],
        false,
        vec![
            tweaked(send("r0", "too early"), leaves_its_model),
            send("r1", "first"),
            Step::Idle,
            tweaked(send("r2", "second"), leaves_its_model),
            Step::Idle,
        ],
    )
    .await;
}

fn refused_at_admission(request: &mut SubmissionRequest) {
    request.stop_when = vec![broca_protocol::request::StopCondition::StepCount { n: 0 }];
}

/// A queued send whose run admission refuses ends, and the send behind it still runs, alike.
#[tokio::test]
async fn a_queued_send_refused_at_its_drain_ends_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            tweaked(send("r2", "refused"), refused_at_admission),
            send("r3", "third"),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

fn from_core(request: &mut SubmissionRequest) {
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    });
}

/// A send's principal is recorded as its run's sender and principal, for a run started directly
/// and one drained from the queue, alike.
#[tokio::test]
async fn a_sends_principal_is_recorded_alike() {
    compare(
        hello(),
        true,
        vec![
            tweaked(keyed("k1", "first"), from_core),
            tweaked(keyed("k2", "second"), from_core),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A steer while a run runs joins it, answered `active` naming that run, and the run answers it
/// at its next step, alike.
#[tokio::test]
async fn a_steer_joins_the_running_run_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            steered(send("s1", "steered")),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

fn another_model(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("stub", "another");
}

/// A steer naming a model the running run didn't freeze is refused naming it, alike.
#[tokio::test]
async fn a_steer_with_another_model_is_refused_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            tweaked(steered(send("s1", "steered")), another_model),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A steer on an idle session starts a run of its own, and a keyed one records its key, alike.
#[tokio::test]
async fn a_steer_on_an_idle_session_starts_a_run_alike() {
    compare(
        hello(),
        false,
        vec![
            steered(send("s1", "steered")),
            Step::Idle,
            steered(keyed("k1", "again")),
            Step::Idle,
        ],
    )
    .await;
}

/// A steer behind a run paused for sign-in is refused naming that run, alike.
#[tokio::test]
async fn a_steer_behind_a_sign_in_pause_is_refused_alike() {
    compare(
        vec![auth_failure()],
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            steered(send("s1", "steered")),
        ],
    )
    .await;
}

/// A queued send is retracted before it runs, a second retraction finds nothing pending, and
/// retracting a send whose run started is too late, alike.
#[tokio::test]
async fn a_queued_send_is_retracted_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            send("r2", "withdrawn"),
            send("r3", "third"),
            Step::Retract("sub-r2"),
            Step::Retract("sub-r2"),
            Step::Release,
            Step::Release,
            Step::Idle,
            Step::Retract("sub-r3"),
            Step::Retract("sub-r1"),
        ],
    )
    .await;
}
