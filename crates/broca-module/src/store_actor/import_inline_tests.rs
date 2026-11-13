//! Inline import read failures must not stop another caller's active run.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use broca_core::{ImportError, ImportOrigin, ImportRequest};
use broca_protocol::{ContentBlock, Message, Role, RunFinishReason, RunId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{
    BackendError, MemoryBackend, SessionAddr, SessionBackend, Version,
};
use broca_session_store::codec::Id128;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, oneshot, Notify};

use super::super::tests::{
    bind, deployment, executor, status, text_step, triple, Commands, Hold, LoopExecutor, ADDR,
};
use super::super::StoreActor;
use crate::actor::{ImportOutcome, LineageCmd, LineageStatus, SendOutcome};

/// Fails one object read without poisoning the writes of the run sharing the actor.
#[derive(Default)]
struct ReadFault {
    inner: MemoryBackend,
    fail_next: AtomicBool,
    failures: AtomicUsize,
}

impl SessionBackend for ReadFault {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        self.inner.read_head(s).await
    }

    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        self.inner.write_head(s, head, expected, next).await
    }

    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.create_log(s, log, at).await
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        self.inner.read_log(s, log, range).await
    }

    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        self.inner.log_end(s, log).await
    }

    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        barrier: bool,
    ) -> Result<u64, BackendError> {
        self.inner
            .append_log(s, log, bytes, expected_end, at, barrier)
            .await
    }

    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.truncate_log(s, log, to, expected_end, at).await
    }

    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_log(s, log, at).await
    }

    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        self.inner.delete_session(s, at).await
    }

    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.put_object(s, name, bytes, at).await
    }

    async fn sync_objects(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.sync_objects(s, change, at).await
    }

    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(BackendError::Io(std::io::Error::other(
                "import lookup failed once",
            )));
        }
        self.inner.get_object(s, name).await
    }

    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_object(s, name, at).await
    }

    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.inner.delete_change(s, change, at).await
    }

    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        self.inner.put_setup(name, bytes).await
    }

    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.inner.get_setup(name).await
    }

    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        self.inner.list_setup().await
    }
}

fn serve(
    session: Session<&'static ReadFault>,
    backend: &'static ReadFault,
    executor: LoopExecutor,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static ReadFault>>,
) {
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    );
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

async fn import(commands: &Commands, run_id: &str) -> ImportOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Import {
            request: Box::new(ImportRequest {
                project_id: None,
                principal: None,
                run_id: RunId(run_id.into()),
                session: bind(),
                messages: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "seeded history".into(),
                    }],
                )],
                origin: ImportOrigin {
                    source_ref: "fixture:transcript".into(),
                },
            }),
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

async fn seeded() -> (Session<&'static ReadFault>, &'static ReadFault) {
    let backend: &'static ReadFault = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let (commands, served) = serve(session, backend, executor(Vec::new(), None));
    assert!(matches!(
        import(&commands, "seed").await,
        ImportOutcome::Imported(_)
    ));
    drop(commands);
    let mut session = served.await.unwrap();
    // The seed import's run-start record is held in memory until sealing; a retry then reads
    // the stored id index.
    session.seal(None, 1).await.unwrap();
    (session, backend)
}

#[tokio::test]
async fn an_inline_import_read_error_leaves_an_active_run_serving_and_completing() {
    let (session, backend) = seeded().await;
    let gate = Arc::new(Notify::new());
    let hold = Arc::new(Hold::default());
    let mut executor = executor(
        vec![text_step("answer", "completed normally")],
        Some(gate.clone()),
    );
    executor.provider.hold = Some(hold.clone());
    let (commands, served) = serve(session, backend, executor);
    // The seed has no tools; a later send inherits that admitted tool set.
    let mut request = super::super::tests::request("continue");
    request.tools = None;
    assert!(matches!(
        super::super::tests::send_with(&commands, "active", request, crate::actor::SendKey::Minted)
            .await,
        SendOutcome::Active { .. }
    ));
    broca_test_support::wait_for(
        "provider reaches the held step",
        hold.reached.notified(),
        || format!("actor finished: {}", served.is_finished()),
    )
    .await;

    assert!(matches!(
        import(&commands, "seed").await,
        ImportOutcome::Imported(outcome) if outcome.already_imported
    ));
    assert_eq!(import(&commands, "other").await, ImportOutcome::Busy);
    backend.fail_next.store(true, Ordering::SeqCst);
    let outcome = import(&commands, "seed").await;
    assert!(
        matches!(
            outcome,
            ImportOutcome::Error(ImportError::Wal(ref error)) if error.contains("import lookup failed once")
        ),
        "{outcome:?}"
    );
    assert_eq!(backend.failures.load(Ordering::SeqCst), 1);
    assert!(!backend.fail_next.load(Ordering::SeqCst));

    let (reply, answer) = oneshot::channel();
    let live = if commands.send(LineageCmd::Status { reply }).is_ok() {
        answer.await.ok()
    } else {
        None
    };
    gate.notify_one();
    if live.is_some() {
        broca_test_support::wait_until("active run completes", || async {
            let current = status(&commands).await;
            if current == LineageStatus::Idle {
                Ok(())
            } else {
                Err(format!("{current:?}"))
            }
        })
        .await;
    }
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert!(
        matches!(
            live,
            Some(LineageStatus::Active { ref run_id, .. }) if run_id.0 == "active"
        ),
        "import read failure retired the active actor: status={live:?}, finished={:?}",
        run.finished
    );
    assert_eq!(run.start.as_ref().unwrap().run_id, "active");
    assert_eq!(run.finished, Some(RunFinishReason::Completed));
    let records = session.records_from(0).await.unwrap();
    assert!(!records.iter().any(|(_, record)| matches!(
        record,
        Record::RunEnd(end) if end.reason == RunFinishReason::Interrupted
    )));
}

#[tokio::test]
async fn an_inline_import_read_error_retires_an_idle_actor() {
    let (session, backend) = seeded().await;
    let (commands, served) = serve(session, backend, executor(Vec::new(), None));
    assert_eq!(status(&commands).await, LineageStatus::Idle);
    backend.fail_next.store(true, Ordering::SeqCst);
    let outcome = import(&commands, "seed").await;
    assert!(
        matches!(
            outcome,
            ImportOutcome::Error(ImportError::Wal(ref error)) if error.contains("import lookup failed once")
        ),
        "{outcome:?}"
    );
    assert_eq!(backend.failures.load(Ordering::SeqCst), 1);
    assert!(!backend.fail_next.load(Ordering::SeqCst));
    broca_test_support::wait_for(
        "idle actor retires after the failed import",
        commands.closed(),
        || format!("actor finished: {}", served.is_finished()),
    )
    .await;
    let session = served.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(RunFinishReason::Completed)
    );
}
