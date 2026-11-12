//! Task cancellation during teardown leaves durable work for the next opener. A panic is a
//! runner failure instead, so it must still release the queue in the current process.

use std::sync::Arc;

use broca_protocol::RunFinishReason;
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{self, executor, text_step, LoopExecutor};
use super::StoreActor;
use crate::actor::{LineageCmd, SendOutcome};
use crate::executor::StoreRunMode;

struct Fixture {
    actor: StoreActor<&'static MemoryBackend>,
    backend: &'static MemoryBackend,
    parts: SharedParts,
    commands: tests::Commands,
    receiver: mpsc::UnboundedReceiver<LineageCmd>,
}

impl Fixture {
    async fn new(executor: LoopExecutor) -> Self {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let session = Session::create(
            backend,
            SessionAddr(tests::ADDR.into()),
            tests::triple(),
            &tests::deployment(),
            false,
        )
        .await
        .unwrap();
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let actor = StoreActor::new(
            tests::bind(),
            session,
            Arc::new(executor),
            Box::new(parts.clone()),
        );
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            actor,
            backend,
            parts,
            commands: tests::Commands::over(sender, backend),
            receiver,
        }
    }

    async fn send(&mut self, run_id: &str) -> SendOutcome {
        let actor = &mut self.actor;
        let receiver = &mut self.receiver;
        tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
            let (answer, ()) = tokio::join!(tests::send(&self.commands, run_id, run_id), async {
                assert!(!actor.handle(receiver.recv().await.unwrap()).await);
                while actor.run.as_ref().is_some_and(|run| !run.started) {
                    let request = actor.run.as_mut().unwrap().link.next().await.unwrap();
                    actor.serve_request(request).await;
                }
            });
            answer
        })
        .await
        .expect("the actor admits the send")
    }

    async fn abort_run(&mut self) {
        let active = self.actor.run.as_mut().unwrap();
        active.done.abort();
        let end = (&mut active.done).await;
        assert!(matches!(&end, Err(error) if error.is_cancelled()));
        self.actor.finish_run(end).await;
    }

    async fn recover(self, interrupted: bool, queued: &[&str]) {
        let Self {
            actor,
            backend,
            parts,
            ..
        } = self;
        drop(actor);
        let executor = Arc::new(executor(
            queued.iter().map(|id| text_step(id, "answer")).collect(),
            None,
        ));
        let (commands, served) = tests::reopen(backend, Arc::clone(&executor), &parts).await;
        tests::idle(&commands).await;
        drop(commands);
        let session = served.await.unwrap();
        let ends: Vec<_> = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, record)| match record {
                Record::RunEnd(end) => Some(end),
                _ => None,
            })
            .collect();
        assert_eq!(ends.len(), queued.len() + 1, "{ends:?}");
        assert_eq!(ends[0].run_id, "r1");
        if interrupted {
            assert_eq!(ends[0].reason, RunFinishReason::Interrupted);
            assert_eq!(ends[0].interrupt_reason.as_deref(), Some("runner_crash"));
        } else {
            assert_eq!(ends[0].reason, RunFinishReason::Completed);
        }
        for (end, run_id) in ends[1..].iter().zip(queued) {
            assert_eq!(&end.run_id, run_id);
            assert_eq!(end.reason, RunFinishReason::Completed);
        }
        assert!(session.operations().queue.is_empty());
        assert_eq!(executor.provider.inner.calls().len(), queued.len());
    }
}

#[tokio::test]
async fn a_cancelled_run_task_leaves_its_end_and_queue_for_crash_recovery() {
    let mut fixture = Fixture::new(executor(Vec::new(), Some(Arc::new(Notify::new())))).await;
    assert!(matches!(
        fixture.send("r1").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        fixture.send("r2").await,
        SendOutcome::Pending { .. }
    ));
    let before = fixture.actor.session.next_seq();
    fixture.abort_run().await;
    assert_eq!(
        fixture.actor.session.next_seq(),
        before,
        "no terminal or queue refusal"
    );
    assert!(
        fixture.actor.run.is_none(),
        "the cancelled handle is no longer polled"
    );
    assert!(fixture
        .actor
        .session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .finished
        .is_none());
    assert_eq!(fixture.actor.session.operations().queue.len(), 1);
    fixture.actor.drain().await;
    assert_eq!(fixture.actor.session.next_seq(), before);
    fixture.recover(true, &["r2"]).await;
}

#[tokio::test]
async fn a_cancelled_drained_task_before_its_start_does_not_refuse_queued_sends() {
    let gate = Arc::new(Notify::new());
    let mut fixture =
        Fixture::new(executor(vec![text_step("a1", "first")], Some(gate.clone()))).await;
    assert!(matches!(
        fixture.send("r1").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        fixture.send("r2").await,
        SendOutcome::Pending { .. }
    ));
    assert!(matches!(
        fixture.send("r3").await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    fixture.actor.run_current_to_end().await;
    let active = fixture.actor.run.as_ref().unwrap();
    assert_eq!(active.run_id.0, "r2");
    assert!(!active.started);
    let before = fixture.actor.session.next_seq();
    fixture.abort_run().await;
    assert_eq!(
        fixture.actor.session.next_seq(),
        before,
        "no open_failed refusal"
    );
    assert_eq!(fixture.actor.session.operations().queue.len(), 2);
    fixture.actor.drain().await;
    assert!(
        fixture.actor.run.is_none(),
        "nothing else drains during teardown"
    );
    fixture.recover(false, &["r2", "r3"]).await;
}

#[tokio::test]
async fn a_cancelled_seal_task_leaves_the_run_for_crash_recovery() {
    let mut fixture = Fixture::new(executor(Vec::new(), Some(Arc::new(Notify::new())))).await;
    assert!(matches!(
        fixture.send("r1").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        fixture.send("r2").await,
        SendOutcome::Pending { .. }
    ));
    let active = fixture.actor.run.take().unwrap();
    active.done.abort();
    let _ = active.done.await;
    let start = fixture
        .actor
        .recovery_start(fixture.actor.session.operations().run.as_ref().unwrap())
        .unwrap();
    fixture.actor.stopping = true;
    fixture
        .actor
        .start_run(
            start,
            None,
            None,
            StoreRunMode::Seal(broca_core::seal::SealCause::Shutdown),
        )
        .await;
    let before = fixture.actor.session.next_seq();
    fixture.actor.run.as_ref().unwrap().done.abort();
    fixture.actor.run_to_end().await;
    assert_eq!(fixture.actor.session.next_seq(), before);
    assert_eq!(fixture.actor.session.operations().queue.len(), 1);
    fixture.recover(true, &["r2"]).await;
}

#[tokio::test]
async fn a_panicked_run_task_still_ends_runner_error_and_drains_the_queue() {
    let mut fixture = Fixture::new(executor(Vec::new(), Some(Arc::new(Notify::new())))).await;
    assert!(matches!(
        fixture.send("r1").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        fixture.send("r2").await,
        SendOutcome::Pending { .. }
    ));
    let active = fixture.actor.run.as_mut().unwrap();
    let original = std::mem::replace(
        &mut active.done,
        tokio::spawn(async { panic!("runner panic") }),
    );
    original.abort();
    let _ = original.await;
    let end = (&mut active.done).await;
    assert!(matches!(&end, Err(error) if error.is_panic()));
    fixture.actor.executor = Arc::new(executor(vec![text_step("a2", "answer")], None));
    fixture.actor.finish_run(end).await;
    fixture.actor.run_to_end().await;
    let ends: Vec<_> = fixture
        .actor
        .session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 2);
    assert_eq!(ends[0].run_id, "r1");
    assert_eq!(ends[0].reason, RunFinishReason::Error);
    let error = ends[0].error.as_ref().unwrap();
    assert_eq!(error["class"], "permanent");
    assert_eq!(error["provider_code"], "runner_error");
    assert_eq!(ends[1].run_id, "r2");
    assert_eq!(ends[1].reason, RunFinishReason::Completed);
    assert!(fixture.actor.session.operations().queue.is_empty());
}

#[tokio::test]
async fn a_cancelled_media_build_does_not_deliver_a_failure_to_the_run() {
    let mut executor = executor(Vec::new(), None);
    executor.waits_for_media = true;
    let mut fixture = Fixture::new(executor).await;
    // The session's image index is being built once from its records, as an open that finds
    // no saved index starts it, so the run waits for it.
    fixture.actor.media.build_from_records(0);
    assert_eq!(fixture.actor.media.start_build(), Some(0));
    // A queued blocking job can be cancelled before its worker starts. An async pending
    // task supplies that same JoinError without depending on blocking-pool saturation.
    fixture.actor.media_build = Some(tokio::spawn(std::future::pending()));
    assert!(matches!(
        fixture.send("r1").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        fixture.send("r2").await,
        SendOutcome::Pending { .. }
    ));
    let task = fixture.actor.media_build.take().unwrap();
    task.abort();
    let built = task.await;
    assert!(matches!(&built, Err(error) if error.is_cancelled()));
    let before = fixture.actor.session.next_seq();
    fixture.actor.media_built(built).await;
    assert!(
        fixture.actor.media_waiting.is_some(),
        "the run still waits for media"
    );
    assert!(
        !fixture.actor.media.ready(),
        "no failed index stands in for the build"
    );
    assert_eq!(fixture.actor.session.next_seq(), before);
    fixture.abort_run().await;
    fixture.recover(true, &["r2"]).await;
}

#[tokio::test]
async fn a_cancelled_task_before_admission_answers_transiently_and_writes_nothing() {
    let mut fixture = Fixture::new(executor(Vec::new(), None)).await;
    let commands = fixture.commands.clone();
    let send = tokio::spawn(async move { tests::send(&commands, "r1", "first").await });
    assert!(
        !fixture
            .actor
            .handle(fixture.receiver.recv().await.unwrap())
            .await
    );
    assert!(!fixture.actor.run.as_ref().unwrap().started);
    let before = fixture.actor.session.next_seq();
    fixture.abort_run().await;
    assert!(matches!(send.await.unwrap(), SendOutcome::Transient(_)));
    assert_eq!(fixture.actor.session.next_seq(), before);
    assert!(fixture.actor.session.operations().run.is_none());
}
