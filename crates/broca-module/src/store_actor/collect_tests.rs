//! A store actor deletes what its session's seals replaced once the grace period is over: when
//! it opens the session, and after each seal its runs write, at its next idle moment.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::writer::Writer as _;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::codec::Id128;
use broca_session_store::session::{Carry, Session, GRACE_MS};
use tokio::sync::mpsc;

use super::tests::{
    bind, deployment, executor, idle, send, status, text_step, triple, Commands, LoopExecutor, ADDR,
};
use super::StoreActor;
use crate::executor::{RunExecutor, RunJob, StoreRunEnd, StoreRunJob};

/// When the session's first seal committed.
const SEALED_AT: u64 = 1_000;

type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// A session sealed once, at `SEALED_AT`, then closed, as a module leaves a session it served:
/// the log the seal replaced waits in the head for the grace period. Returns the backend holding
/// it and that log.
async fn sealed_session() -> (&'static MemoryBackend, Id128) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let mut session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let (replaced, _) = session.open_log();
    session.seal(None, SEALED_AT).await.unwrap();
    session.close().await.unwrap();
    (backend, replaced)
}

/// A store actor over the session `backend` holds, loaded as a module opening it loads it, which
/// reads the time from `now`.
async fn open(
    backend: &'static MemoryBackend,
    executor: Arc<dyn RunExecutor>,
    now: Clock,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts))
        .with_readers(Arc::new(move || backend))
        .with_clock(now);
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

fn at(now_ms: u64) -> Clock {
    Arc::new(move || now_ms)
}

/// Waits until the log `replaced` is gone from `backend`, while the actor still serves.
async fn deleted(backend: &'static MemoryBackend, replaced: Id128) {
    broca_test_support::wait_until("the actor deletes the replaced log", || {
        let present = backend
            .log_names(&SessionAddr(ADDR.into()))
            .contains(&replaced);
        async move {
            if present {
                Err("the replaced log is still there".to_owned())
            } else {
                Ok(())
            }
        }
    })
    .await;
}

/// Opened inside the grace period, the actor keeps the log the seal replaced, through its idle
/// moments and its close. Opened once the grace period is over, it deletes it at its first idle
/// moment, while it still serves the session.
#[tokio::test]
async fn an_actor_opening_a_session_past_the_grace_deletes_what_its_seal_replaced() {
    let (backend, replaced) = sealed_session().await;
    let addr = SessionAddr(ADDR.into());

    let (commands, served) = open(
        backend,
        Arc::new(executor(Vec::new(), None)),
        at(SEALED_AT + GRACE_MS - 1),
    )
    .await;
    status(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert!(backend.log_names(&addr).contains(&replaced));
    assert_eq!(session.head().pending.len(), 1);
    drop(session);

    let (commands, served) = open(
        backend,
        Arc::new(executor(Vec::new(), None)),
        at(SEALED_AT + GRACE_MS),
    )
    .await;
    status(&commands).await;
    deleted(backend, replaced).await;
    drop(commands);
    let session = served.await.unwrap();
    assert!(session.head().pending.is_empty());
}

/// Runs each store job through the test loop, after sealing the session's ledger through the
/// run's writer, as a run whose compaction cuts the session seals it, at the time `now` holds.
pub(super) struct SealingExecutor {
    pub(super) inner: LoopExecutor,
    pub(super) now: Arc<AtomicU64>,
}

#[async_trait]
impl RunExecutor for SealingExecutor {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    async fn execute_on_store(
        &self,
        mut job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        job.writer
            .seal_ledger(&Carry::default(), self.now.load(Ordering::SeqCst))
            .await
            .unwrap();
        self.inner.execute_on_store(job, out).await
    }
}

/// The actor finds nothing due when it opens the session. A grace period after the session's
/// first seal, a run seals the session again; once the run ends, the actor deletes the log the
/// first seal replaced, while it still serves the session. Only the run's own seal still waits.
#[tokio::test]
async fn an_actor_deletes_what_came_due_after_a_seal_its_run_wrote() {
    let (backend, replaced) = sealed_session().await;
    let now = Arc::new(AtomicU64::new(SEALED_AT + GRACE_MS - 1));
    let reads = Arc::new(AtomicUsize::new(0));
    let clock: Clock = {
        let now = Arc::clone(&now);
        let reads = Arc::clone(&reads);
        Arc::new(move || {
            reads.fetch_add(1, Ordering::SeqCst);
            now.load(Ordering::SeqCst)
        })
    };
    let executor = SealingExecutor {
        inner: executor(vec![text_step("a1", "hello")], None),
        now: Arc::clone(&now),
    };
    let (commands, served) = open(backend, Arc::new(executor), clock).await;
    broca_test_support::wait_until("the actor collects on opening", || {
        let read = reads.load(Ordering::SeqCst);
        async move {
            if read > 0 {
                Ok(())
            } else {
                Err("the actor hasn't read the clock".to_owned())
            }
        }
    })
    .await;
    assert!(backend
        .log_names(&SessionAddr(ADDR.into()))
        .contains(&replaced));

    now.store(SEALED_AT + GRACE_MS, Ordering::SeqCst);
    send(&commands, "r1", "hi").await;
    idle(&commands).await;
    deleted(backend, replaced).await;
    drop(commands);
    let session = served.await.unwrap();
    let pending = &session.head().pending;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].unreferenced_at_ms, SEALED_AT + GRACE_MS);
}
