//! Sends into a session whose latest run is paused for a restart, and keyed retries of sends
//! that never started a run of their own, through the store actor, each reply pinned and,
//! through [`super::compare`], what the session then holds: a steer into a run paused for a
//! restart, kept for its resume; and the retries of a steer, of a retracted send, and of a send
//! refused when it drained.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::OutputEvent;
use broca_protocol::{Delivery, PauseReason, RunId, SubmissionRequest};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::tests::{self as store, request};
use super::{compare, hello, keyed, refused_at_admission, send, steered, tweaked, Step};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, StoreRunEnd, StoreRunJob};

/// A steer joins the running run; its retry under the same key is answered by that run, active
/// while it runs and finished with the steer's delivery once it ended, and a retry with another
/// payload is refused naming what differs, alike.
#[tokio::test]
async fn a_steers_retry_is_answered_by_the_run_it_joined_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            steered(keyed("k1", "also this")),
            steered(keyed("k1", "also this")),
            steered(keyed("k1", "something else")),
            keyed("k1", "also this"),
            Step::Release,
            Step::Release,
            Step::Idle,
            steered(keyed("k1", "also this")),
        ],
    )
    .await;
}

/// A retracted send's retry is answered `retracted`, whatever its payload, and starts nothing,
/// alike.
#[tokio::test]
async fn a_retracted_sends_retry_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            keyed("k1", "later"),
            Step::Retract("sub-r-k1"),
            keyed("k1", "later"),
            keyed("k1", "changed"),
            Step::Release,
            Step::Idle,
            keyed("k1", "later"),
        ],
    )
    .await;
}

/// A queued send whose run admission refused when it drained: its retry is answered from that
/// refusal, alike.
#[tokio::test]
async fn a_refused_drains_retry_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            tweaked(keyed("k1", "refused"), refused_at_admission),
            Step::Release,
            Step::Idle,
            tweaked(keyed("k1", "refused"), refused_at_admission),
        ],
    )
    .await;
}

/// Asks the first run it starts to stop for a restart, as a module's graceful stop asks a running
/// run: that run pauses at its first boundary, under a restart policy that resumes it. Later runs
/// run on.
struct StopsFirstRun<E> {
    inner: Arc<E>,
    stopped: AtomicBool,
}

impl<E> StopsFirstRun<E> {
    fn new(inner: Arc<E>) -> Self {
        Self {
            inner,
            stopped: AtomicBool::new(false),
        }
    }

    /// A stop already asked for, the first time only.
    fn stop(&self) -> Option<CancellationToken> {
        (!self.stopped.swap(true, Ordering::SeqCst)).then(|| {
            let stop = CancellationToken::new();
            stop.cancel();
            stop
        })
    }
}

#[async_trait]
impl<E: RunExecutor> RunExecutor for StopsFirstRun<E> {
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.inner.hook_plane()
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.inner.compaction_plane()
    }

    async fn execute_on_store(
        &self,
        mut job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        if let Some(stop) = self.stop() {
            job.control.stop_requested = Some(stop);
        }
        self.inner.execute_on_store(job, out).await
    }
}

/// A send under a restart policy that resumes the run after a restart.
fn resumable(text: &str) -> SubmissionRequest {
    let mut sent = request(text);
    sent.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    sent
}

/// A store actor over a new session in memory whose first run pauses for a restart, under the
/// retention clock `clock` when there is one.
async fn restart_pausing(
    clock: Option<broca_core::clock::Clock>,
) -> (
    store::Commands,
    tokio::task::JoinHandle<
        broca_session_store::session::Session<&'static broca_session_store::backend::MemoryBackend>,
    >,
) {
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let backend: &'static broca_session_store::backend::MemoryBackend = Box::leak(Box::default());
    let session = broca_session_store::session::Session::create(
        backend,
        broca_session_store::backend::SessionAddr(store::ADDR.into()),
        store::triple(),
        &store::deployment(),
        false,
    )
    .await
    .unwrap();
    let mut actor = super::super::StoreActor::new(
        store::bind(),
        session,
        Arc::new(StopsFirstRun::new(Arc::new(store::executor(hello(), None)))),
        Box::new(parts),
    )
    .with_readers(Arc::new(move || backend));
    if let Some(clock) = clock {
        actor = actor.with_retention_clock(clock);
    }
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        store::Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

/// The owner's send that first freezes a storage policy, arriving as a steer into a run paused for
/// a restart, writes that policy with its steer, as a steer into a running run does.
#[tokio::test]
async fn a_steer_into_a_run_paused_for_a_restart_freezes_its_storage_policy() {
    use cortexkit_role_llm_runner::session_retention::{Keep, RetentionPolicy};
    let (commands, served) = restart_pausing(None).await;
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "starter".into(),
    };
    let caller = store::Caller::default();
    let mut first = resumable("first");
    first.principal = Some(owner.clone());
    let first = caller.shape("r1", &SendKey::Minted, first);
    let started = store::send_exactly(
        &commands,
        "r1",
        first,
        SendKey::Minted,
        Delivery::Queue,
        None,
    )
    .await;
    assert!(matches!(started, SendOutcome::Active { .. }), "{started:?}");
    store::idle(&commands).await;
    let policy = RetentionPolicy::new(Keep::days(7), 0, false, false);
    let mut steer = resumable("also this");
    steer.principal = Some(owner);
    steer.retention_policy = Some(policy.clone());
    let key = || SendKey::SendId("k1".into());
    let steer = caller.shape("r-k1", &key(), steer);
    let steered = store::send_exactly(&commands, "r-k1", steer, key(), Delivery::Steer, None).await;
    assert!(matches!(steered, SendOutcome::Paused { .. }), "{steered:?}");
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        session
            .operations
            .run
            .as_ref()
            .unwrap()
            .pending_steers
            .len(),
        1
    );
    assert_eq!(
        session.operations.retention_policy,
        Some(serde_json::to_value(&policy).unwrap()),
        "the steer's policy was frozen"
    );
}

/// A steer into a run paused for a restart, on a session kept by the older retention seconds, is
/// accepted at the retention clock's time, as a steer into a running run is: that time is the
/// session's activity its deadline counts from.
#[tokio::test]
async fn a_steer_into_a_run_paused_for_a_restart_is_stamped_by_the_retention_clock() {
    const NOW: u64 = 1_700_000_000_000;
    let dir = super::scratch_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("test-clock"),
        serde_json::json!({"now_ms": NOW}).to_string(),
    )
    .unwrap();
    let clock = broca_core::clock::Clock::for_state_root(&dir).unwrap();
    let (commands, served) = restart_pausing(Some(clock)).await;
    let caller = store::Caller::default();
    let first = caller.shape("r1", &SendKey::Minted, resumable("first"));
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Send {
            session_class: None,
            retention: Some(3_600),
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId("r1".into()),
            request: Box::new(first),
            kind: crate::executor::RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    let started = answer.await.unwrap();
    assert!(matches!(started, SendOutcome::Active { .. }), "{started:?}");
    store::idle(&commands).await;
    let key = || SendKey::SendId("k1".into());
    let steer = caller.shape("r-k1", &key(), resumable("also this"));
    let steered = store::send_exactly(&commands, "r-k1", steer, key(), Delivery::Steer, None).await;
    assert!(matches!(steered, SendOutcome::Paused { .. }), "{steered:?}");
    drop(commands);
    let session = served.await.unwrap();
    let steer = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, record)| match record {
            broca_session_store::record::Record::Steer(steer) => Some(steer),
            _ => None,
        })
        .expect("the steer was recorded");
    assert_eq!(steer.written_at_ms, Some(NOW));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A run paused for a restart takes a steer: the steer is recorded for the run's resume and
/// answered naming the paused run; a retry of it is answered by that run, and one with another
/// payload is refused naming what differs. A plain send waits behind the paused run.
#[tokio::test]
async fn a_steer_into_a_run_paused_for_a_restart_waits_for_its_resume_alike() {
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let (commands, served, _) = {
        let backend: &'static broca_session_store::backend::MemoryBackend =
            Box::leak(Box::default());
        let session = broca_session_store::session::Session::create(
            backend,
            broca_session_store::backend::SessionAddr(store::ADDR.into()),
            store::triple(),
            &store::deployment(),
            false,
        )
        .await
        .unwrap();
        let actor = super::super::StoreActor::new(
            store::bind(),
            session,
            Arc::new(StopsFirstRun::new(Arc::new(store::executor(hello(), None)))),
            Box::new(parts.clone()),
        )
        .with_readers(Arc::new(move || backend));
        let (commands, receiver) = mpsc::unbounded_channel();
        (
            store::Commands::over(commands, backend),
            tokio::spawn(actor.serve(receiver)),
            backend,
        )
    };
    let caller = store::Caller::default();
    let mut replies = Vec::new();
    for (run_id, text, key, delivery) in [
        ("r1", "first", None, Delivery::Queue),
        ("r-k1", "also this", Some("k1"), Delivery::Steer),
        ("r-k1", "also this", Some("k1"), Delivery::Steer),
        ("r-k1", "something else", Some("k1"), Delivery::Steer),
        ("r2", "after that", None, Delivery::Queue),
    ] {
        let send_key = || match key {
            Some(key) => SendKey::SendId(key.into()),
            None => SendKey::Minted,
        };
        let sent = caller.shape(run_id, &send_key(), resumable(text));
        let new = store::send_exactly(&commands, run_id, sent, send_key(), delivery, None).await;
        broca_test_support::pin!(format!("the replies to {run_id} differ"), new);
        if run_id == "r1" {
            // The run pauses at its first boundary before the next send.
            store::idle(&commands).await;
        }
        replies.push(new);
    }
    assert_eq!(
        replies[1],
        SendOutcome::Paused {
            run_id: RunId("r1".into()),
            reason: PauseReason::Restart,
            delivered: None,
        }
    );
    assert!(
        matches!(replies[3], SendOutcome::SendIdReuse { .. }),
        "{:?}",
        replies[3]
    );
    drop(commands);
    let session = served.await.unwrap();
    // The steer waits for the run's resume, unplaced.
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(run.paused, Some(PauseReason::Restart));
    assert_eq!(run.pending_steers.len(), 1);
    assert_eq!(
        session
            .operations
            .queue
            .iter()
            .map(|pending| pending.submission_id.as_str())
            .collect::<Vec<_>>(),
        ["sub-r2"]
    );
}
