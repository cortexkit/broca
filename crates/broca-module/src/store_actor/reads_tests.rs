//! Reads of a session kept in the new storage, served without reloading the session: by its
//! store actor while the session is open, and from kept views once it is closed. Each read is
//! compared with the same read from a fresh `Session::view` at the same moment.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{BindIdentity, NormalizedEvent, RunId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{
    BackendError, LocalBackend, MemoryBackend, SessionAddr, SessionBackend, Version,
};
use broca_session_store::codec::Id128;
use broca_session_store::head::SessionTriple;
use broca_session_store::record::{Marker, Placement, Record};
use broca_session_store::session::Session;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Notify};

use super::tests::{bind, deployment, executor, idle, read_step, request, text_step, ADDR};
use super::StoreActor;
use crate::actor::{LineageCmd, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::LineageRegistry;
use crate::store_reads::{
    execute, PageRequest, ReadContext, StoreReadOutcome, StoreReadReply, StoreReadRequest,
};
use crate::store_status::SettledRuns;

/// A memory backend that records how far the session's log is written and how far a barrier
/// made it durable, and can fail the next barrier.
#[derive(Default)]
struct Recording {
    inner: MemoryBackend,
    /// Per log: its length, and its length at its last barrier.
    logs: Mutex<HashMap<Id128, (u64, u64)>>,
    last_log: Mutex<Option<Id128>>,
    barriers: AtomicUsize,
    fail_barrier: AtomicBool,
}

impl Recording {
    /// The newest log's length, its durable length, and how many barriers were appended.
    fn log_state(&self) -> (u64, u64, usize) {
        let log = self.last_log.lock().unwrap().expect("a log was written");
        let (len, durable) = self.logs.lock().unwrap()[&log];
        (len, durable, self.barriers.load(Ordering::SeqCst))
    }
}

impl SessionBackend for Recording {
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
        if barrier && self.fail_barrier.swap(false, Ordering::SeqCst) {
            return Err(BackendError::Io(std::io::Error::other("the sync failed")));
        }
        let end = self
            .inner
            .append_log(s, log, bytes, expected_end, at, barrier)
            .await?;
        let mut logs = self.logs.lock().unwrap();
        let state = logs.entry(*log).or_default();
        state.0 = end;
        if barrier {
            state.1 = end;
            self.barriers.fetch_add(1, Ordering::SeqCst);
        }
        *self.last_log.lock().unwrap() = Some(*log);
        Ok(end)
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

/// A store actor over a new session on a [`Recording`] backend whose run stops at its first tool
/// call's approval: after the step that made the call was appended without a barrier, and before
/// the batch's intents make it durable. Returns the actor's commands, the backend, and the
/// approval's `reached` and `release`.
async fn stopped_in_the_gap() -> (
    super::tests::Commands,
    tokio::task::JoinHandle<Session<&'static Recording>>,
    &'static Recording,
    Arc<Notify>,
) {
    let backend: &'static Recording = Box::leak(Box::default());
    let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let mut executor = executor(vec![read_step("a1"), text_step("a2", "done")], None);
    executor.approval_gate = Some((Arc::clone(&reached), Arc::clone(&release)));
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        super::tests::triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts));
    let (commands, receiver) = mpsc::unbounded_channel();
    let commands = super::tests::Commands::over(commands, backend);
    let served = tokio::spawn(actor.serve(receiver));
    let mut first = request("first");
    first.principal = Some(super::differential::core());
    assert!(matches!(
        super::tests::send_with(&commands, "r1", first, crate::actor::SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    reached.notified().await;
    (commands, served, backend, release)
}

/// Asks the actor behind `commands` for `request`.
async fn ask(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    request: StoreReadRequest,
) -> StoreReadOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::StoreRead {
            request: Box::new(request),
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

fn entries() -> StoreReadRequest {
    StoreReadRequest::Entries {
        from: 1,
        limit: 100,
    }
}

/// The entries the actor answered with from memory.
fn served_entries(outcome: StoreReadOutcome) -> Vec<broca_session_store::session::StoredEntry> {
    let StoreReadOutcome::Served(reply) = outcome else {
        panic!("the actor answers the read from memory");
    };
    let StoreReadReply::Entries(Ok(entries)) = *reply else {
        panic!("the actor answers with the entries");
    };
    entries
}

/// A read that reaches the actor after a step that called a tool was appended without a barrier,
/// and before the batch's intents make it durable, sees the step only once the actor made it
/// durable: one claim frame with a barrier, before the answer. A second read in the same gap
/// finds nothing left to make durable and writes nothing.
#[tokio::test]
async fn a_read_in_the_gap_before_a_barrier_makes_the_step_durable_first() {
    let (commands, served, backend, release) = stopped_in_the_gap().await;
    let (len, durable, barriers) = backend.log_state();
    assert!(
        durable < len,
        "the step's records wait for the batch's barrier"
    );
    let read = served_entries(ask(&commands, entries()).await);
    let held = read.last().expect("the run's messages");
    assert!(
        held.open && held.message.role == broca_protocol::content::Role::Assistant,
        "the read serves the step's assistant message, held for its call's result"
    );
    let (len_after, durable_after, barriers_after) = backend.log_state();
    assert_eq!(
        durable_after, len_after,
        "the log was durable before the answer"
    );
    assert!(len_after > len, "a claim frame was appended");
    assert_eq!(barriers_after, barriers + 1, "with one barrier");

    let again = served_entries(ask(&commands, entries()).await);
    assert_eq!(again, read);
    assert_eq!(
        backend.log_state(),
        (len_after, durable_after, barriers_after),
        "nothing was left to make durable"
    );
    release.notify_one();
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
}

/// A read whose sync fails is refused, transient, `durability_flush_failed`, and nothing is served
/// from the records that couldn't be made durable.
#[tokio::test]
async fn a_read_whose_sync_fails_is_refused_with_no_answer() {
    let (commands, served, backend, _release) = stopped_in_the_gap().await;
    let (len, durable, _) = backend.log_state();
    backend.fail_barrier.store(true, Ordering::SeqCst);
    match ask(&commands, entries()).await {
        StoreReadOutcome::Refused(crate::serve::ServeError::Open(message, class)) => {
            assert!(message.starts_with("durability_flush_failed"), "{message}");
            assert_eq!(class, Some(broca_protocol::ErrorClass::Transient));
        }
        _ => panic!("the read is refused"),
    }
    assert_eq!(backend.log_state().1, durable, "nothing more is durable");
    assert!(durable < len);
    drop(commands);
    served.await.unwrap();
}

async fn authorization_owner(commands: &super::tests::Commands) -> broca_core::SessionOwnershipV1 {
    match ask(commands, StoreReadRequest::AuthorizationOwnership).await {
        StoreReadOutcome::Served(answer) => match *answer {
            StoreReadReply::AuthorizationOwnership(Ok(owner)) => owner,
            _ => panic!("authorization ownership is answered"),
        },
        _ => panic!("authorization ownership is in memory"),
    }
}

#[tokio::test]
async fn refused_flow_authorization_keeps_pending_log_byte_identical() {
    let (commands, served, backend, release) = stopped_in_the_gap().await;
    let before = backend.log_state();
    assert!(before.1 < before.0);
    let log = backend.last_log.lock().unwrap().unwrap();
    let addr = SessionAddr(ADDR.into());
    let bytes = backend.read_log(&addr, &log, 0..before.0).await.unwrap();
    let owner = authorization_owner(&commands).await;
    let mut scope = super::differential::head_scope(super::differential::core());
    let broca_protocol::SendScope::Stamped(bind) = &mut scope else {
        unreachable!()
    };
    bind.scope.flow_id = Some("flow-1".into());
    assert!(matches!(
        broca_core::scope_admission::decide_owner_access(
            &owner,
            Some(&super::differential::core()),
            &scope,
        ),
        Err(broca_core::ScopeRefusal::OwnerMismatch(_))
    ));
    assert_eq!(backend.log_state(), before, "no append or barrier");
    assert_eq!(
        backend.read_log(&addr, &log, 0..before.0).await.unwrap(),
        bytes
    );
    release.notify_one();
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
}

#[tokio::test]
async fn admitted_owner_operation_is_durable_after_pending_records() {
    let (commands, served, backend, release) = stopped_in_the_gap().await;
    let before = backend.log_state();
    assert!(before.1 < before.0);
    let owner = authorization_owner(&commands).await;
    broca_core::scope_admission::decide_owner_access(
        &owner,
        Some(&super::differential::core()),
        &broca_protocol::SendScope::Unstamped,
    )
    .unwrap();
    assert_eq!(backend.log_state(), before, "authorization did not flush");
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_wal::record::RunCancelAt::Now,
            reply,
        })
        .unwrap();
    assert!(matches!(
        answer.await.unwrap(),
        crate::actor::CancelOutcome::Ack
    ));
    let after = backend.log_state();
    assert!(after.0 > before.0);
    assert_eq!(after.1, after.0);
    assert_eq!(after.2, before.2 + 1, "only the operation's barrier");
    release.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let records = session.records_from(0).await.unwrap();
    let step = records
        .iter()
        .find(|(_, record)| matches!(record, Record::Assistant(_)))
        .unwrap()
        .0;
    let cancel = records
        .iter()
        .find(|(_, record)| matches!(record, Record::CancelRequest(_)))
        .unwrap()
        .0;
    assert!(step < cancel, "the cancel follows the pending step records");
}

/// The idle time of the module below: long enough that the session closes only when a test closes
/// it, so each read happens with the actor open or closed as the test means.
const OPEN: Duration = Duration::from_secs(600);

#[test]
fn submission_admission_future_stays_small() {
    fn size<'a, T: 'a, Fut>(_: impl FnOnce(&'a T) -> Fut) -> usize {
        std::mem::size_of::<Fut>()
    }
    let bytes = size(|m: &Module| m.send("r", "text"));
    assert!(
        bytes <= 4096,
        "registry send carries {bytes} bytes; keep ownership replay on the heap"
    );
}

/// A module whose new sessions start in the new storage, over a scratch folder, whose scripted
/// provider answers `steps`, each one once the test lets it through `gate`.
struct Module {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    gate: Arc<Notify>,
    /// The caller whose sends reach the module's session.
    caller: super::tests::Caller,
}

impl Drop for Module {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn module(steps: Vec<Vec<NormalizedEvent>>) -> Module {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-reads-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let gate = Arc::new(Notify::new());
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(steps, Some(Arc::clone(&gate))));
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor),
        OPEN,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(OPEN)
            .with_store_runtime(Arc::new(runtime)),
        ),
        wal,
        None,
    )
    .with_store_new_sessions();
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Module {
        dir,
        registry,
        key,
        gate,
        caller: Default::default(),
    }
}

impl Module {
    fn runtime(&self) -> Arc<crate::store_open::StoreRuntime> {
        self.registry.store_runtime().expect("the new storage")
    }

    fn backend(&self) -> LocalBackend {
        LocalBackend::new(self.dir.join(broca_wal::SESSION_STORAGE_DIR))
    }

    fn addr(&self) -> SessionAddr {
        SessionAddr(broca_core::session_addr(&self.key))
    }

    fn triple(&self) -> SessionTriple {
        SessionTriple {
            project_root: self.key.project_root.display().to_string(),
            harness: self.key.harness.clone(),
            session: self.key.session.clone(),
        }
    }

    /// Sends `text` as the module's caller sends it.
    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &SendKey::Minted, request(text)),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    /// The read as the module serves it: by the actor while one is open, else from kept views.
    async fn served(&self, request: &StoreReadRequest) -> StoreReadReply {
        self.runtime()
            .read(&self.registry, &self.key, request.clone())
            .await
            .expect("the read is served")
            .expect("the session holds records")
    }

    /// The same read from a fresh `Session::view` of the session as stored now, with the same
    /// view of the registry's slot, and a cache of finished runs of its own, so finished runs are
    /// walked from their records.
    async fn fresh(&self, request: &StoreReadRequest) -> StoreReadReply {
        let runtime = self.runtime();
        let view = Session::view(
            self.backend(),
            self.addr(),
            &self.triple(),
            &deployment_key(),
        )
        .await
        .expect("the session loads");
        let slot = self.registry.slot_state(&self.key);
        let settled = SettledRuns::default();
        let addr = self.addr().0;
        let context = ReadContext {
            token_counts: None,
            parts: runtime.parts_for(
                broca_wal::StorageClass::Ordinary,
                broca_session_store::keys::Cipher::XChaCha20Poly1305,
            ),
            held: slot.is_some(),
            open: slot == Some("open"),
            settled: Some((&settled, &addr)),
            session_name: &self.key.session,
            live_step: None,
            stream_head: None,
            entry_speaks_for: crate::store_entries::EntryAttribution::Known(false),
        };
        execute(&view, &context, request).await
    }

    /// Each read in `requests`, served and fresh, compared.
    /// Each read in `requests`, served and fresh, compared; and the reads that don't depend on the
    /// runs: `session.history`'s head, and the compaction route's `session.entries`.
    async fn compare(&self, requests: &[StoreReadRequest], when: &str) {
        let route = StoreReadRequest::Compaction {
            key: self.key.clone(),
            caller: Some("compactor".into()),
            method: "session.entries".into(),
            params: serde_json::value::RawValue::from_string(
                r#"{"select": {"by": "after"}, "detail": "full"}"#.into(),
            )
            .unwrap(),
        };
        let session_wide = [StoreReadRequest::HistoryHead { mid: None }, route];
        for request in requests.iter().chain(&session_wide) {
            let served = shape(self.served(request).await);
            let fresh = shape(self.fresh(request).await);
            assert_eq!(served, fresh, "{when}: {request:?}");
        }
    }

    /// Asks the session's open actor for `request` directly, to see where the read was answered.
    async fn ask_actor(&self, request: StoreReadRequest) -> StoreReadOutcome {
        let handle = self.registry.open_handle(&self.key).expect("an open actor");
        let (reply, answer) = oneshot::channel();
        let sent = handle
            .send(LineageCmd::StoreRead {
                request: Box::new(request),
                reply,
            })
            .await;
        assert!(sent.is_ok(), "the actor takes the read");
        answer.await.unwrap()
    }

    /// The open actor answers each page with every opt-in from memory whenever it answers the
    /// same page without them from memory: what the opt-ins add is in the session it holds.
    async fn opt_ins_stay_in_memory(&self, when: &str) {
        for page in pages() {
            let plain = matches!(
                self.ask_actor(StoreReadRequest::Page(page.clone())).await,
                StoreReadOutcome::Served(_)
            );
            let all = matches!(
                self.ask_actor(StoreReadRequest::Page(opted(page.clone())))
                    .await,
                StoreReadOutcome::Served(_)
            );
            assert_eq!(
                all, plain,
                "{when}: {page:?} with every opt-in is answered where it is without them"
            );
        }
    }

    /// The served `run.status` of `run`, as JSON.
    async fn status(&self, run: &str) -> Value {
        match self
            .served(&StoreReadRequest::RunStatusReply(RunId(run.into())))
            .await
        {
            StoreReadReply::RunStatusReply(Ok(reply)) => serde_json::to_value(reply).unwrap(),
            StoreReadReply::RunStatusReply(Err(error)) => panic!("run.status {run}: {error:?}"),
            _ => panic!("another read's reply"),
        }
    }

    /// Waits until the provider is answering step `step` of the active run `run`, as the actor
    /// serves the run's in-flight step.
    async fn in_step(&self, run: &str, step: u64) {
        broca_test_support::wait_until("the step is in flight", || async {
            let status = self.status(run).await;
            match status["step"]["step_id"].as_u64() {
                Some(at) if at == step => Ok(()),
                _ => Err(status.to_string()),
            }
        })
        .await;
    }

    /// Waits until `run` ended as `state`.
    async fn ended(&self, run: &str, state: &str) {
        broca_test_support::wait_until("the run ends", || async {
            let status = self.status(run).await;
            match status["state"].as_str() {
                Some(at) if at == state => Ok(()),
                _ => Err(status.to_string()),
            }
        })
        .await;
    }

    /// Closes the session's actor, as its idle deadline would.
    async fn close(&self) {
        if let Some(handle) = self.registry.open_handle(&self.key) {
            let _ = handle.send(LineageCmd::IdleTick).await;
        }
        broca_test_support::wait_until("the session closes", || async {
            match self.registry.slot_state(&self.key) {
                None => Ok(()),
                Some(state) => Err(state.to_owned()),
            }
        })
        .await;
    }

    /// The session's writer, loaded as no actor holds it.
    async fn writer(&self) -> Session<LocalBackend> {
        Session::load(
            self.backend(),
            self.addr(),
            &self.triple(),
            &deployment_key(),
        )
        .await
        .expect("the session opens for writing")
    }
}

fn deployment_key() -> broca_session_store::keys::DeploymentKey {
    broca_session_store::keys::DeploymentKey::new(1, &[7; 32])
}

/// A reply as text both paths can be compared by. An active run's `step` is left out: it is the
/// actor's live hint, which a fresh view doesn't have (see the test of the hint itself).
fn shape(reply: StoreReadReply) -> String {
    fn without_step(mut value: Value) -> Value {
        if let Value::Object(object) = &mut value {
            object.remove("step");
            for member in object.values_mut() {
                *member = without_step(member.take());
            }
        }
        value
    }
    match reply {
        StoreReadReply::RunStatus(reply) => match reply {
            Ok(reply) => without_step(serde_json::to_value(reply).unwrap()).to_string(),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::RunStatusReply(reply) => match reply {
            Ok(reply) => without_step(serde_json::to_value(reply).unwrap()).to_string(),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::Head(head) => match head {
            Ok(head) => serde_json::to_value(&head.reply).unwrap().to_string(),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::Page(page) => match page {
            Ok(page) => format!(
                "{} {:?}",
                serde_json::to_value(&page.response).unwrap(),
                page.from_ordinal,
            ),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::Compaction(answer) => match answer {
            Ok(answer) => answer.get().to_owned(),
            Err(refusal) => format!("{refusal:?}"),
        },
        StoreReadReply::HistoryHead(head) => format!("{head:?}"),
        StoreReadReply::Media(media) => format!("{media:?}"),
        StoreReadReply::TagLocation(location) => format!("{location:?}"),
        StoreReadReply::WritingView(view) => format!("{view:?}"),
        StoreReadReply::OutputRefNamespace(namespace) => namespace,
        StoreReadReply::PlanChange(read) => format!("{:?}", read.map(|read| read.0)),
        StoreReadReply::PlanIdentities(identities) => format!("{identities:?}"),
        StoreReadReply::Baseline(facts) => format!("{facts:?}"),
        StoreReadReply::Entries(entries) => format!("{entries:?}"),
        StoreReadReply::AuthorizationOwnership(ownership) => format!("{ownership:?}"),
        StoreReadReply::AuthorizationSubmissionSender(sender) => format!("{sender:?}"),
        StoreReadReply::RunResult(reply) => match reply {
            Ok(reply) => serde_json::to_value(reply).unwrap().to_string(),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::RunToolCalls(reply) => match reply {
            Ok(reply) => serde_json::to_value(reply).unwrap().to_string(),
            Err(error) => format!("{error:?}"),
        },
        StoreReadReply::SendStatus(reply) => match reply {
            Ok(reply) => serde_json::to_value(reply).unwrap().to_string(),
            Err(error) => format!("{error:?}"),
        },
    }
}

/// Requires a send to have started its run.
fn active(outcome: SendOutcome) {
    assert!(
        matches!(outcome, SendOutcome::Active { .. }),
        "the send starts its run: {outcome:?}"
    );
}

/// The reads each comparison makes: every run's `run.status` and `run.result`, the head, the
/// newest page and a page from the first message, each also with every opt-in, the session's
/// entries, and its ownership.
fn reads(runs: &[&str]) -> Vec<StoreReadRequest> {
    let mut reads: Vec<StoreReadRequest> = runs
        .iter()
        .flat_map(|run| {
            [
                StoreReadRequest::RunStatusReply(RunId((*run).into())),
                StoreReadRequest::RunResult(RunId((*run).into())),
            ]
        })
        .collect();
    reads.extend([
        StoreReadRequest::Head,
        StoreReadRequest::Page(PageRequest {
            limit: Some(2),
            ..PageRequest::default()
        }),
        StoreReadRequest::Page(PageRequest {
            from_ordinal: Some(1),
            limit: Some(3),
            ..PageRequest::default()
        }),
    ]);
    reads.extend(pages().map(|page| StoreReadRequest::Page(opted(page))));
    reads.extend([
        StoreReadRequest::Entries {
            from: 1,
            limit: 200,
        },
        StoreReadRequest::AuthorizationOwnership,
    ]);
    reads
}

/// The pages the comparisons read: the newest two messages, and three from the first.
fn pages() -> [PageRequest; 2] {
    [
        PageRequest {
            limit: Some(2),
            ..PageRequest::default()
        },
        PageRequest {
            from_ordinal: Some(1),
            limit: Some(3),
            ..PageRequest::default()
        },
    ]
}

/// `page` with every opt-in: display facts, tools and Rich UI stamps.
fn opted(page: PageRequest) -> PageRequest {
    PageRequest {
        include_display_facts: true,
        include_tools: true,
        include_rich_ui: true,
        ..page
    }
}

/// The ordinal and id of the window message whose text is `text`.
async fn entry_of(session: &Session<LocalBackend>, text: &str) -> (u64, String) {
    let window = session.conversation.window_start();
    session
        .entries(window, 200)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| {
            entry.message.content.iter().any(|block| {
                matches!(block, broca_protocol::content::ContentBlock::Text { text: at } if at == text)
            })
        })
        .and_then(|entry| Some((entry.ordinal, entry.state.facts?.mid)))
        .expect("the message has an id")
}

/// The seq of run `run`'s own input, where a cut can fall.
async fn input_of(session: &Session<LocalBackend>, run: &str) -> u64 {
    session
        .records_from(1)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(seq, record)| match record {
            Record::Input(input)
                if input.run_id == run && input.placement == Placement::RunInput =>
            {
                Some(seq)
            }
            _ => None,
        })
        .expect("the run's input")
}

/// At every step of a session's life, open and closed, each read served by the actor or from a
/// kept view equals the same read from a fresh view: while a run's model step is in flight, after
/// a step that called a tool, once the run ended with the actor still open, and once closed. Then
/// after a seal at a compaction cut, with an actor open over the sealed history and closed again,
/// and after a revert, closed and open.
#[tokio::test]
async fn every_read_equals_a_fresh_views_at_every_step() {
    let m = module(vec![
        read_step("a1"),
        text_step("a2", "one"),
        text_step("b1", "two"),
        text_step("c1", "three"),
        text_step("d1", "four"),
    ])
    .await;

    // Run r1: a step that calls a tool, then one that answers.
    active(m.send("r1", "first").await);
    m.in_step("r1", 1).await;
    m.compare(&reads(&["r1"]), "r1's first step in flight")
        .await;
    m.gate.notify_one();
    m.in_step("r1", 2).await;
    m.compare(
        &reads(&["r1"]),
        "r1's second step in flight, after its tool call",
    )
    .await;
    // Nothing is sealed: the actor answers every read from what it holds.
    for request in reads(&["r1"]) {
        assert!(
            matches!(
                m.ask_actor(request.clone()).await,
                StoreReadOutcome::Served(_)
            ),
            "{request:?} is answered by the actor"
        );
    }
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    m.compare(&reads(&["r1"]), "r1 ended, the actor open").await;
    m.opt_ins_stay_in_memory("r1 ended, the actor open").await;

    // Run r2, then the session closes.
    active(m.send("r2", "second").await);
    m.in_step("r2", 1).await;
    m.compare(&reads(&["r1", "r2"]), "r2 in flight").await;
    m.opt_ins_stay_in_memory("r2 in flight").await;
    m.gate.notify_one();
    m.ended("r2", "completed").await;
    m.close().await;
    m.compare(&reads(&["r1", "r2"]), "closed").await;
    assert_eq!(
        m.runtime().views().usage().0,
        1,
        "the closed session's view is kept"
    );
    m.compare(
        &reads(&["r1", "r2"]),
        "closed, read again from the kept view",
    )
    .await;

    // A compaction cut before r2's input, as the compaction provider's cut records it, and the
    // seal there: r1 is sealed, r2 stays in the window.
    let mut writer = m.writer().await;
    let cut = input_of(&writer, "r2").await;
    let (ordinal, mid) = entry_of(&writer, "second").await;
    writer
        .append(
            &[Record::Marker(Marker {
                view_version: 0,
                first_kept_mid: mid,
                first_kept_ordinal: ordinal,
                checkpoint: 0,
                harness: Some(m.key.harness.clone()),
                session: Some(m.key.session.clone()),
            })],
            true,
        )
        .await
        .unwrap();
    writer.seal(Some(cut), 1).await.unwrap();
    writer.close().await.unwrap();
    m.compare(&reads(&["r1", "r2"]), "closed, after a seal")
        .await;

    // Run r3 on an actor over the sealed history: the first page and r1 are sealed.
    active(m.send("r3", "third").await);
    m.in_step("r3", 1).await;
    m.compare(
        &reads(&["r1", "r2", "r3"]),
        "r3 in flight over sealed history",
    )
    .await;
    m.opt_ins_stay_in_memory("r3 in flight over sealed history")
        .await;
    // The running run and the newest page are in memory; the first page is sealed, and is read
    // from storage at the position the actor names.
    assert!(matches!(
        m.ask_actor(StoreReadRequest::RunStatusReply(RunId("r3".into())))
            .await,
        StoreReadOutcome::Served(_)
    ));
    assert!(matches!(
        m.ask_actor(StoreReadRequest::Head).await,
        StoreReadOutcome::Served(_)
    ));
    assert!(matches!(
        m.ask_actor(StoreReadRequest::Page(PageRequest {
            from_ordinal: Some(1),
            limit: Some(3),
            ..PageRequest::default()
        }))
        .await,
        StoreReadOutcome::Stored { .. }
    ));
    m.gate.notify_one();
    m.ended("r3", "completed").await;
    m.compare(&reads(&["r1", "r2", "r3"]), "r3 ended over sealed history")
        .await;
    m.close().await;
    m.compare(&reads(&["r1", "r2", "r3"]), "closed over sealed history")
        .await;

    // A revert to r3's input: r9 takes r3's place, and is left unfinished, as a crash leaves a
    // run before its end is recorded.
    let mut writer = m.writer().await;
    let (_, target) = entry_of(&writer, "third").await;
    let mut start = writer
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.clone())
        .expect("r3's start");
    start.run_id = "r9".into();
    start.send_id = Some("s9".into());
    let input = writer
        .records_from(cut)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, record)| match record {
            Record::Input(mut input) if input.run_id == "r3" => {
                input.run_id = "r9".into();
                Some(input)
            }
            _ => None,
        })
        .expect("r3's input");
    let facts = writer
        .records_from(cut)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, record)| match record {
            Record::EntryFacts(mut facts) => {
                facts.run_id = Some("r9".into());
                Some(facts)
            }
            _ => None,
        })
        .expect("entry facts");
    writer
        .revert(
            &target,
            vec![
                Record::RunStart(Box::new(start)),
                Record::Input(input),
                Record::EntryFacts(facts),
            ],
            None,
            2,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();
    m.compare(&reads(&["r1", "r2", "r3", "r9"]), "closed, after a revert")
        .await;

    // Run r4 on an actor opened after the revert, which first seals r9 as a crash's orphan.
    active(m.send("r4", "fourth").await);
    m.in_step("r4", 1).await;
    m.compare(
        &reads(&["r1", "r2", "r9", "r4"]),
        "r4 in flight after a revert",
    )
    .await;
    m.gate.notify_one();
    m.ended("r4", "completed").await;
    m.close().await;
    m.compare(&reads(&["r1", "r2", "r9", "r4"]), "closed after r4")
        .await;
}

/// A session deleted and created again with the same history reads as the new session: its kept
/// view is never served for it, though the new session's head versions and log length can equal
/// the old one's, since the new session's log is another log.
#[tokio::test]
async fn a_session_created_again_is_read_fresh() {
    let m = module(vec![text_step("a1", "one"), text_step("a1", "one")]).await;
    m.gate.notify_one();
    m.send("r1", "first").await;
    m.ended("r1", "completed").await;
    m.close().await;
    let before = shape(m.served(&StoreReadRequest::Head).await);
    let first = Session::stored_position(&m.backend(), &m.addr(), &deployment_key())
        .await
        .unwrap()
        .unwrap();
    for folder in ["heads", "logs", "objects"] {
        let _ = std::fs::remove_dir_all(
            m.dir
                .join(broca_wal::SESSION_STORAGE_DIR)
                .join(folder)
                .join(&m.addr().0),
        );
    }
    m.gate.notify_one();
    m.send("r1", "first").await;
    m.ended("r1", "completed").await;
    m.close().await;
    let second = Session::stored_position(&m.backend(), &m.addr(), &deployment_key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (second.version, second.len),
        (first.version, first.len),
        "the new session stands where the old one stood, but for its log"
    );
    let after = m.served(&StoreReadRequest::Head).await;
    assert_ne!(shape(after), before, "a new incarnation");
    m.compare(&reads(&["r1"]), "the session created again")
        .await;
}
