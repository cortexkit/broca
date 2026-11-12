//! The store actor's own tests for what has no comparison with today's actor: an interrupt that
//! a process queued behind a paused run before it could end that run, a send arriving while
//! sends wait in an idle session's queue, and the retry of a run whose executor failed.

use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_protocol::{Delivery, PauseReason, RunId, SubmissionId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::queue;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{
    actor_with, auth_failure, bind, deployment, executor, idle, reopen, request, send_with,
    text_step, triple, Commands, LoopExecutor, ADDR,
};
use super::StoreActor;
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, StoreRunEnd, StoreRunError, StoreRunJob};

/// Runs each store job through `inner`. Notifying `stop` drops the loop and returns an
/// executor error without writing its end, so the actor must record the failure.
struct Cutting {
    inner: LoopExecutor,
    stop: Arc<Notify>,
}

/// A store run's end that wrote nothing more.
fn cut_off(why: &str) -> StoreRunEnd {
    StoreRunEnd {
        parts: Box::new(MemoryParts::default()),
        result: Err(StoreRunError::Admission(RunError::Config(why.into()))),
    }
}

#[async_trait]
impl RunExecutor for Cutting {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        tokio::select! {
            end = self.inner.execute_on_store(job, out) => end,
            () = self.stop.notified() => cut_off("the run's task was dropped"),
        }
    }
}

/// A store actor over a new session in memory whose runs go through `executor`, with the
/// setup parts and backend a reopened actor shares.
async fn actor_over(
    executor: Cutting,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    &'static MemoryBackend,
    SharedParts,
) {
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
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts.clone()))
        .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
        backend,
        parts,
    )
}

/// Each run's start and end in the session's open log, in order.
async fn run_story(session: &Session<&'static MemoryBackend>) -> Vec<String> {
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) => Some(format!("start {}", start.run_id)),
            Record::RunEnd(end) => Some(format!("end {} {:?}", end.run_id, end.reason)),
            _ => None,
        })
        .collect()
}

/// An interrupt queued behind a run paused for sign-in, by a process that stopped before the
/// paused run ended: the session's next open ends the paused run `cancelled` and runs the
/// interrupt, as today's drain does.
#[tokio::test]
async fn an_interrupt_left_behind_a_paused_run_ends_it_on_the_next_open() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served, backend) =
        actor_with(executor(vec![auth_failure()], None), parts.clone()).await;
    let sent = send_with(&commands, "r1", request("first"), SendKey::Minted).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    idle(&commands).await;
    drop(commands);
    let mut session = served.await.unwrap();
    // Capture the crash after the interrupt's durable queue append, before any executor
    // can finalize the paused run. A returned executor error is not a process crash.
    let queued = queue::queued_send(
        "sub-i1",
        None,
        &request("stop and do this"),
        &queue::Receipt::default(),
        queue::Queued {
            delivery: Some(Delivery::Interrupt),
            ..Default::default()
        },
        None,
        &mut parts.clone(),
    )
    .unwrap();
    Writer::append(&mut session, &[Record::QueuedSend(queued)], true)
        .await
        .unwrap();
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(run.paused, Some(PauseReason::AuthRequired), "still paused");
    assert_eq!(session.operations.queue.len(), 1, "the interrupt waits");
    drop(session);

    let (commands, served) = reopen(
        backend,
        Arc::new(executor(vec![text_step("a1", "done")], None)),
        &parts,
    )
    .await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert!(session.operations.queue.is_empty());
    assert_eq!(
        run_story(&session).await,
        [
            "start r1",
            "end r1 Cancelled",
            "start i1",
            "end i1 Completed"
        ]
    );
}

/// A send arriving while a send that starts a run still waits in an idle session's queue is
/// queued behind it and answered `pending`, as today's actor answers it. Here the waiting send
/// can't be read back to start its run, so both stay queued.
#[tokio::test]
async fn a_send_behind_sends_queued_on_an_idle_session_is_queued() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served, backend) = actor_with(
        executor(vec![text_step("a1", "hello")], None),
        parts.clone(),
    )
    .await;
    let sent = send_with(&commands, "r1", request("first"), SendKey::Minted).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    idle(&commands).await;
    drop(commands);
    let mut session = served.await.unwrap();
    // A queued send whose recorded transforms no build can read, so no drain starts it.
    let mut stuck = request("stuck");
    stuck.tools = None;
    let mut part_store = parts.clone();
    let mut send = queue::queued_send(
        "sub-r2",
        None,
        &stuck,
        &queue::Receipt::default(),
        queue::Queued::default(),
        None,
        &mut part_store,
    )
    .unwrap();
    let mut receipt = send.receipt.take().unwrap_or_else(|| serde_json::json!({}));
    receipt["hooks"] = serde_json::json!("not transforms");
    send.receipt = Some(receipt);
    Writer::append(&mut session, &[Record::QueuedSend(send)], true)
        .await
        .unwrap();
    drop(session);

    let (commands, served) = reopen(backend, Arc::new(executor(Vec::new(), None)), &parts).await;
    let sent = send_with(&commands, "r3", request("third"), SendKey::Minted).await;
    assert_eq!(
        sent,
        SendOutcome::Pending {
            submission_id: SubmissionId("sub-r3".into()),
        }
    );
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        session
            .operations
            .queue
            .iter()
            .map(|pending| pending.submission_id.as_str())
            .collect::<Vec<_>>(),
        ["sub-r2", "sub-r3"]
    );
}

/// An executor error after the start is admitted records a terminal instead of leaving an
/// apparently active run that nothing serves. Retrying its send reads that recorded error.
#[tokio::test]
async fn a_retry_of_a_run_whose_executor_failed_reads_its_recorded_end() {
    let stop = Arc::new(Notify::new());
    let (commands, served, _, _) = actor_over(Cutting {
        inner: executor(
            vec![text_step("a1", "hello")],
            Some(Arc::new(Notify::new())),
        ),
        stop: Arc::clone(&stop),
    })
    .await;
    let key = || SendKey::SendId("k1".into());
    let sent = send_with(&commands, "r-k1", request("first"), key()).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    stop.notify_one();
    idle(&commands).await;
    let retry = send_with(&commands, "r-k1", request("first"), key()).await;
    assert_eq!(
        retry,
        SendOutcome::Finished {
            run_id: RunId("r-k1".into()),
            reason: broca_protocol::RunFinishReason::Error,
            delivered: None,
        }
    );
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(run.finished, Some(broca_protocol::RunFinishReason::Error));
    assert!(run.paused.is_none());
    assert_eq!(run_story(&session).await, ["start r-k1", "end r-k1 Error"]);
}
