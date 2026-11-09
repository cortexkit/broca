//! Billing for runs on the new storage: a run's end makes its spend facts owed, the actor
//! inserts them into the usage export through today's export path, and only then records the
//! run billed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use broca_core::{ChargeBasis, ExportError, ExportIndex, ExportPage};
use broca_protocol::RunId;
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::{Billed, Record, RunEnd};
use broca_session_store::session::Session;
use broca_store::SqliteStore;
use tokio::sync::mpsc;

use super::tests::{
    actor_with, bind, deployment, executor, idle, send, text_step, triple, Commands, LoopExecutor,
    ADDR,
};
use super::StoreActor;
use crate::actor::SendOutcome;
use crate::store_billing::StoreBilling;

/// What an insert into [`Export`] does.
#[derive(Clone, Copy)]
enum Insert {
    /// It commits.
    Commit,
    /// It fails with nothing written, as a full disk fails it.
    Fail,
    /// The process dies before it commits: nothing is written, and every later write to the
    /// session fails.
    CrashBefore,
    /// It commits, then the process dies: every later write to the session fails.
    CrashAfter,
}

/// Today's SQLite usage export, whose inserts a test scripts: each insert takes the next
/// scripted step, and commits once the script is used up.
struct Export {
    store: SqliteStore,
    script: Mutex<VecDeque<Insert>>,
    /// The backend holding the session, which a scripted crash stops.
    backend: &'static MemoryBackend,
}

impl Export {
    fn new(backend: &'static MemoryBackend, script: &[Insert]) -> Arc<Self> {
        Arc::new(Self {
            store: SqliteStore::open_in_memory().unwrap(),
            script: Mutex::new(script.iter().copied().collect()),
            backend,
        })
    }

    /// Every stored fact, as its run id and its whole stored form.
    fn facts(&self) -> Vec<(String, String)> {
        self.store
            .read_export_page(None, 1_000)
            .unwrap()
            .facts
            .iter()
            .map(|fact| {
                let segment = fact.segment.as_session().expect("a session's fact");
                (
                    segment.run_id.0.clone(),
                    serde_json::to_string(segment).unwrap(),
                )
            })
            .collect()
    }
}

impl ExportIndex for Export {
    fn missing_terminal_runs(&self) -> Result<Vec<broca_core::MissingExportRun>, ExportError> {
        ExportIndex::missing_terminal_runs(&self.store)
    }

    fn emit_run(
        &self,
        records: &[broca_wal::FramedRecord],
        run_id: &RunId,
        resolver: &dyn broca_core::ChargeBasisResolver,
    ) -> Result<(), ExportError> {
        let step = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Insert::Commit);
        match step {
            Insert::Commit => self.store.emit_export_run(records, run_id, resolver),
            Insert::Fail => Err(ExportError::Storage("disk full".into())),
            Insert::CrashBefore => {
                self.backend.crash_after(Some(0));
                Err(ExportError::Storage("the process died".into()))
            }
            Insert::CrashAfter => {
                let inserted = self.store.emit_export_run(records, run_id, resolver);
                self.backend.crash_after(Some(0));
                inserted
            }
        }
    }

    fn read_page(
        &self,
        since: Option<broca_core::ExportCursor>,
        limit: u64,
    ) -> Result<ExportPage, ExportError> {
        self.store.read_export_page(since, limit)
    }

    fn high_water(&self) -> u64 {
        self.store.high_water()
    }

    fn source_epoch(&self) -> u64 {
        self.store.source_epoch()
    }

    fn source_instance_id(&self) -> &str {
        self.store.source_instance_id()
    }
}

fn billing(export: &Arc<Export>) -> StoreBilling {
    StoreBilling {
        index: Arc::clone(export) as Arc<dyn ExportIndex>,
        resolver: Arc::new(|_: &str, _: &str| (ChargeBasis::MeteredApi, 4)),
    }
}

type Served = tokio::task::JoinHandle<Session<&'static MemoryBackend>>;

/// A billing store actor over the session `backend` holds, created when `create`.
async fn open_billed(
    backend: &'static MemoryBackend,
    executor: LoopExecutor,
    parts: &SharedParts,
    export: &Arc<Export>,
    create: bool,
) -> (Commands, Served) {
    let addr = SessionAddr(ADDR.into());
    let session = if create {
        Session::create(backend, addr, triple(), &deployment(), false).await
    } else {
        Session::load(backend, addr, &triple(), &deployment()).await
    }
    .unwrap();
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts.clone()))
        .with_billing(billing(export));
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

fn parts() -> SharedParts {
    SharedParts::new(Box::new(MemoryParts::default()))
}

fn leaked() -> &'static MemoryBackend {
    Box::leak(Box::default())
}

/// The session `backend` holds, loaded as a module opening it would load it.
async fn reload(backend: &'static MemoryBackend) -> Session<&'static MemoryBackend> {
    Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap()
}

/// The run ids the session's open log records billed, in order.
async fn billed(session: &Session<&'static MemoryBackend>) -> Vec<String> {
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::Billed(billed) => Some(billed.run_id),
            _ => None,
        })
        .collect()
}

async fn ran(commands: &Commands, run_id: &str, text: &str) {
    assert!(matches!(
        send(commands, run_id, text).await,
        SendOutcome::Active { .. }
    ));
    idle(commands).await;
}

fn run_ids(facts: &[(String, String)]) -> Vec<&str> {
    facts.iter().map(|(run, _)| run.as_str()).collect()
}

/// Each run's spend facts are in the usage export when its end has been written, and its
/// `Billed` record follows its end, so the session owes nothing.
#[tokio::test]
async fn a_runs_facts_are_written_at_its_end_and_it_is_billed() {
    let backend = leaked();
    let export = Export::new(backend, &[]);
    let steps = vec![text_step("a1", "hello"), text_step("a2", "again")];
    let (commands, served) =
        open_billed(backend, executor(steps, None), &parts(), &export, true).await;
    ran(&commands, "r1", "first").await;
    assert_eq!(run_ids(&export.facts()), ["r1"], "billed at r1's end");
    ran(&commands, "r2", "second").await;
    drop(commands);
    let session = served.await.unwrap();
    let facts = export.facts();
    assert_eq!(run_ids(&facts), ["r1", "r2"], "each run billed once");
    let fact: serde_json::Value = serde_json::from_str(&facts[0].1).unwrap();
    assert_eq!(fact["provider"], "stub");
    assert_eq!(fact["usage"]["input_tokens"], 100);
    assert_eq!(fact["usage"]["output_tokens"], 20);
    assert_eq!(fact["terminal_reason"], "completed");
    assert_eq!(fact["charge_basis"], "metered_api");
    assert!(session.operations.billing_owed.is_empty());
    assert_eq!(billed(&session).await, ["r1", "r2"]);
    // Each run's `Billed` record comes after its end.
    let records = session.open_log_records().await.unwrap();
    let at = |find: &dyn Fn(&Record) -> bool| records.iter().position(|(_, r)| find(r)).unwrap();
    for run in ["r1", "r2"] {
        let end = at(&|r| matches!(r, Record::RunEnd(end) if end.run_id == run));
        let bill = at(&|r| matches!(r, Record::Billed(billed) if billed.run_id == run));
        assert!(end < bill, "{run}: billed after its end");
    }
}

/// A process that dies after the insert commits and before `Billed` is written leaves the run
/// owed. The next open bills it again before taking a command, which inserts nothing new: the
/// stored fact is kept unchanged, and there is still one row.
#[tokio::test]
async fn a_crash_before_billed_bills_again_on_open_with_no_duplicate_row() {
    let backend = leaked();
    let export = Export::new(backend, &[Insert::CrashAfter]);
    let parts = parts();
    let (commands, served) = open_billed(
        backend,
        executor(vec![text_step("a1", "hello")], None),
        &parts,
        &export,
        true,
    )
    .await;
    ran(&commands, "r1", "first").await;
    drop(commands);
    let _ = served.await;
    backend.crash_after(None);
    let facts = export.facts();
    assert_eq!(run_ids(&facts), ["r1"], "the insert committed");
    let session = reload(backend).await;
    assert_eq!(session.operations.billing_owed, ["r1"], "still owed");
    assert!(billed(&session).await.is_empty());
    drop(session);

    let (commands, served) =
        open_billed(backend, executor(Vec::new(), None), &parts, &export, false).await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert!(session.operations.billing_owed.is_empty());
    assert_eq!(billed(&session).await, ["r1"]);
    assert_eq!(
        export.facts(),
        facts,
        "the stored fact is kept unchanged, once"
    );
}

/// `Billed` is written only once the insert has committed: a process that dies during the
/// insert leaves the run owed with no `Billed` record, and the run itself ended as it would
/// have. The next open bills it.
#[tokio::test]
async fn billed_is_never_written_before_the_insert_commits() {
    let backend = leaked();
    let export = Export::new(backend, &[Insert::CrashBefore]);
    let parts = parts();
    let (commands, served) = open_billed(
        backend,
        executor(vec![text_step("a1", "hello")], None),
        &parts,
        &export,
        true,
    )
    .await;
    ran(&commands, "r1", "first").await;
    drop(commands);
    let _ = served.await;
    backend.crash_after(None);
    assert!(export.facts().is_empty(), "the insert never committed");
    let session = reload(backend).await;
    assert!(
        billed(&session).await.is_empty(),
        "billed before its facts were stored"
    );
    assert_eq!(session.operations.billing_owed, ["r1"]);
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    drop(session);

    let (commands, served) =
        open_billed(backend, executor(Vec::new(), None), &parts, &export, false).await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(run_ids(&export.facts()), ["r1"]);
    assert_eq!(billed(&session).await, ["r1"]);
}

/// A failed insert never fails the run: the sender's run completes, the run stays owed, and the
/// next run's end bills both.
#[tokio::test]
async fn a_failed_insert_leaves_the_run_owed_until_the_next_runs_end() {
    let backend = leaked();
    let export = Export::new(backend, &[Insert::Fail]);
    let steps = vec![text_step("a1", "hello"), text_step("a2", "again")];
    let (commands, served) =
        open_billed(backend, executor(steps, None), &parts(), &export, true).await;
    ran(&commands, "r1", "first").await;
    assert!(export.facts().is_empty());
    ran(&commands, "r2", "second").await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(run_ids(&export.facts()), ["r1", "r2"]);
    assert!(session.operations.billing_owed.is_empty());
    assert_eq!(billed(&session).await, ["r1", "r2"]);
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

/// A converted session holds a `Billed` record after each run's end, since today's storage
/// billed those runs: it opens owing nothing and inserts no fact. A run on the new storage after
/// them is billed once.
#[tokio::test]
async fn a_converted_session_owes_nothing_and_a_later_run_is_billed_once() {
    // A session whose run ended and was recorded billed, as the converter writes it.
    let parts = parts();
    let (commands, served, backend) = actor_with(
        executor(vec![text_step("a0", "before")], None),
        parts.clone(),
    )
    .await;
    ran(&commands, "r0", "converted").await;
    drop(commands);
    let mut session = served.await.unwrap();
    assert_eq!(session.operations.billing_owed, ["r0"]);
    session
        .append(
            &[Record::Billed(Billed {
                run_id: "r0".into(),
            })],
            true,
        )
        .await
        .unwrap();
    session.close().await.unwrap();

    let export = Export::new(backend, &[]);
    let (commands, served) = open_billed(
        backend,
        executor(vec![text_step("a1", "hello")], None),
        &parts,
        &export,
        false,
    )
    .await;
    idle(&commands).await;
    assert!(export.facts().is_empty(), "open billed a converted run");
    ran(&commands, "r1", "first").await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(run_ids(&export.facts()), ["r1"]);
    assert_eq!(billed(&session).await, ["r0", "r1"]);
}

/// A run seeded from an import never called a provider: its end owes nothing, and opening its
/// session inserts no fact. A run on the new storage after it is billed once.
#[tokio::test]
async fn an_imported_session_owes_nothing_and_a_later_run_is_billed_once() {
    // A real run's start, from a scratch session sharing the parts store, to copy as the start
    // an import writes.
    let parts = parts();
    let (commands, served, _) = actor_with(
        executor(vec![text_step("a0", "scratch")], None),
        parts.clone(),
    )
    .await;
    ran(&commands, "r0", "scratch").await;
    drop(commands);
    let scratch = served.await.unwrap();
    let mut start = scratch
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.clone())
        .unwrap();
    start.run_id = "imported".into();
    start.seeded = Some(true);
    let backend = leaked();
    let mut session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    session
        .append(
            &[
                Record::RunStart(Box::new(start)),
                Record::RunEnd(RunEnd {
                    run_id: "imported".into(),
                    reason: broca_protocol::RunFinishReason::Completed,
                    usage: None,
                    ts_ms: Some(1),
                    retries_used: 0,
                    interrupt_reason: None,
                    error: None,
                }),
            ],
            true,
        )
        .await
        .unwrap();
    assert!(session.operations.billing_owed.is_empty());
    session.close().await.unwrap();

    let export = Export::new(backend, &[]);
    let (commands, served) = open_billed(
        backend,
        executor(vec![text_step("a1", "hello")], None),
        &parts,
        &export,
        false,
    )
    .await;
    // The session's creating send was the import's, not this caller's.
    commands.caller().joins_existing();
    idle(&commands).await;
    assert!(export.facts().is_empty(), "open billed an imported run");
    ran(&commands, "r1", "first").await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(run_ids(&export.facts()), ["r1"]);
    assert_eq!(billed(&session).await, ["r1"]);
}
