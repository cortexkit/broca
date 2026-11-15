//! Sends into a session whose latest run is paused for a restart, and keyed retries of sends
//! that never started a run of their own, through the store actor, each reply pinned and,
//! through [`super::compare`], what the session then holds: a steer into a run paused for a
//! restart, kept for its resume; and the retries of a steer, of a retracted send, and of a send
//! refused when it drained.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_protocol::{Delivery, PauseReason, RunId, SubmissionRequest};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::tests::{self as store, request};
use super::{compare, hello, keyed, refused_at_admission, send, steered, tweaked, Step};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, StoreRunEnd, StoreRunJob};

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

    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        if let Some(stop) = self.stop() {
            job.control.stop_requested = Some(stop);
        }
        self.inner.execute(job, out).await
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
