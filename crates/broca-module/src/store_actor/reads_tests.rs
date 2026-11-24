//! Reads of a session kept in the session store, served without reloading the session: by its
//! store actor while the session is open, and from kept views once it is closed. Each read is
//! compared with the same read from a fresh `Session::view` at the same moment.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::FileLeaseStore;
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

mod capability_tests;

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
            at: broca_records::record::RunCancelAt::Now,
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

/// A module whose new sessions start in the session store, over a scratch folder, whose scripted
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
    module_with(|gate| Arc::new(executor(steps, Some(gate)))).await
}

/// [`module`] with retention's service installed, as production runs it: every operation's
/// expiry check then reads the retention clock rather than taking the clockless early return.
#[cfg(feature = "test-clock")]
async fn module_with_retention(steps: Vec<Vec<NormalizedEvent>>) -> Module {
    let m = module(steps).await;
    let leases = Arc::new(FileLeaseStore::new(m.dir.join("leases")));
    m.registry.set_retention_service(Arc::new(
        crate::retention::RetentionService::new(&m.dir, leases).unwrap(),
    ));
    m
}

async fn module_with(make_executor: impl FnOnce(Arc<Notify>) -> Arc<dyn RunExecutor>) -> Module {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-reads-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let gate = Arc::new(Notify::new());
    let executor = make_executor(Arc::clone(&gate));
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor),
        OPEN,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            std::sync::Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
        )
        .with_idle_ttl(OPEN),
    ));
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
        LocalBackend::new(self.dir.join(broca_records::SESSION_STORAGE_DIR))
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

    /// Sends `request`, already shaped as the module's caller sends it. Kept apart from
    /// [`Self::send`], whose future `submission_admission_future_stays_small` measures. Only the
    /// retention-clock tests send a shaped request.
    #[cfg(feature = "test-clock")]
    async fn submit(
        &self,
        run_id: &str,
        request: broca_protocol::SubmissionRequest,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
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

    /// Sends `text` as the module's caller sends it, under `key`.
    async fn send_keyed(&self, run_id: &str, text: &str, key: SendKey) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &key, request(text)),
                RunKind::FreshOrResume,
                key,
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
            labels: Default::default(),
            token_counts: None,
            parts: runtime.parts_for(
                broca_records::session_storage::StorageClass::Ordinary,
                broca_session_store::keys::Cipher::XChaCha20Poly1305,
            ),
            held: slot.is_some(),
            open: slot == Some("open"),
            settled: Some((&settled, &addr)),
            session_name: &self.key.session,
            live_step: None,
            stream_head: None,
            entry_speaks_for: crate::store_entries::EntryAttribution::Known(false),
            // The root the actor's session is in, so a head names the policy it names.
            class: Some(broca_records::session_storage::StorageClass::Ordinary),
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

#[tokio::test]
async fn real_actor_work_publishes_nonzero_phase_timings() {
    if !crate::phase_metrics_tests::isolated(
        "store_actor::reads_tests::real_actor_work_publishes_nonzero_phase_timings",
    ) {
        return;
    }
    use broca_session_store::metrics::snapshot;
    use std::sync::atomic::AtomicU64;

    let before = snapshot();
    let m = module_with(|gate| {
        Arc::new(super::collect_tests::SealingExecutor {
            inner: executor(
                vec![text_step("a1", "one"), text_step("b1", "two")],
                Some(gate),
            ),
            now: Arc::new(AtomicU64::new(1_000)),
        })
    })
    .await;
    for run in ["r1", "r2"] {
        active(m.send(run, "hello").await);
        m.in_step(run, 1).await;
        crate::store_read::serve_read(
            &m.registry,
            &m.runtime(),
            crate::serve::SessionReadRequest::new(m.key.clone(), Some(1), Some(20)),
        )
        .await
        .expect("the open actor serves a real read");
        m.gate.notify_one();
        m.ended(run, "completed").await;
        m.close().await;
    }

    let session = m.writer().await;
    let request = serde_json::from_value(serde_json::json!({
        "select": {"by": "after"}, "detail": "full"
    }))
    .unwrap();
    let page = crate::store_entries::entries_page(&session, &request)
        .await
        .unwrap();
    assert!(!page.entries.is_empty());
    session.close().await.unwrap();

    let after = snapshot();
    for phase in [
        "open",
        "append",
        "sync",
        "seal",
        "media_wait",
        "request_prepare",
        "media_fold",
        "read",
        "entries_page",
        "collection",
    ] {
        let old = &before[phase];
        let new = &after[phase];
        assert!(new.count > old.count, "{phase}: no completed work");
        assert!(new.total_us > old.total_us, "{phase}: no measured time");
        assert!(
            new.last_us > 0 && new.max_us >= new.last_us,
            "{phase}: invalid timings"
        );
    }
    assert_eq!(
        after["open"].count - before["open"].count,
        2,
        "create and reload"
    );
    assert_eq!(
        after["seal"].count - before["seal"].count,
        2,
        "both seals run through the actor"
    );
    let sealed = after["seal"].sealed.as_ref().unwrap();
    assert!(sealed.ledger_records > 0);
    assert!(sealed.ledger_bytes > 0);
    assert!(sealed.index_entries > 0);
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
        StoreReadReply::RunAdmission(reply) => match reply {
            Ok(reply) => serde_json::to_value(reply).unwrap().to_string(),
            Err(error) => format!("{error:?}"),
        },
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
        StoreReadReply::PlanView(reply) => format!("{reply:?}"),
        StoreReadReply::HeldCalls(held) => format!("{held:?}"),
        StoreReadReply::Entries(entries) => format!("{entries:?}"),
        StoreReadReply::AuthorizationOwnership(ownership) => format!("{ownership:?}"),
        StoreReadReply::AuthorizationRead(admitted) => format!("{admitted:?}"),
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
        .and_then(|entry| Some((entry.ordinal, entry.state.facts?.mid.clone())))
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
                std::sync::Arc::make_mut(&mut facts).run_id = Some("r9".into());
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

#[tokio::test]
async fn store_read_future_keeps_recovery_off_the_callers_stack() {
    let m = module(Vec::new()).await;
    let runtime = m.runtime();
    let read = runtime.read(&m.registry, &m.key, StoreReadRequest::Head);
    assert!(
        std::mem::size_of_val(&read) < 4096,
        "composed request futures must not inline the actor-opening recovery future: {} bytes",
        std::mem::size_of_val(&read)
    );
    drop(read);
    assert!(m.registry.stop_and_seal().await);
}

#[tokio::test]
async fn paused_session_reads_stay_in_the_view_without_opening_an_actor() {
    for reason in [
        broca_protocol::PauseReason::AuthRequired,
        broca_protocol::PauseReason::AccountLimit,
    ] {
        let m = module(vec![text_step("a1", "seed")]).await;
        active(m.send("r1", "seed").await);
        m.gate.notify_one();
        m.ended("r1", "completed").await;
        m.close().await;
        let mut writer = m.writer().await;
        let mut start = writer
            .operations
            .run
            .as_ref()
            .unwrap()
            .start
            .clone()
            .unwrap();
        start.run_id = "paused".into();
        start.submission_id = None;
        start.send_id = None;
        writer
            .append(
                &[
                    Record::RunStart(Box::new(start)),
                    Record::Pause(broca_session_store::record::Pause {
                        run_id: "paused".into(),
                        reason,
                        ts_ms: None,
                        error: None,
                        account_limit: None,
                    }),
                ],
                true,
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        let before = m.backend().read_stored_head(&m.addr()).unwrap();
        for request in [
            StoreReadRequest::Head,
            StoreReadRequest::RunStatusReply(RunId("paused".into())),
        ] {
            let expected = shape(m.fresh(&request).await);
            let actual = shape(m.served(&request).await);
            assert_eq!(actual, expected, "{reason:?}: {request:?}");
            assert!(
                m.registry.slot_state(&m.key).is_none(),
                "{reason:?} reads must not open an actor"
            );
        }
        assert_eq!(m.backend().read_stored_head(&m.addr()).unwrap(), before);
        assert!(m.registry.stop_and_seal().await);
    }
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
                .join(broca_records::SESSION_STORAGE_DIR)
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

/// `session.head` carries the same control cursor (`head`) as a `session.read` page of the same
/// session, the cursor a subscribe resumes from, both while the session's actor is open and
/// from the stored view once it closed.
#[tokio::test]
async fn session_head_serves_the_cursor_a_read_page_serves() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let m = module(vec![text_step("a1", "one")]).await;
    active(m.send("r1", "first").await);
    m.gate.notify_one();
    m.ended("r1", "completed").await;

    let cursors = || async {
        let head = m.registry.head(&m.key).await.unwrap().head;
        let read = m
            .registry
            .read(SessionReadRequest::new(m.key.clone(), Some(1), None))
            .await
            .unwrap()
            .head;
        (
            serde_json::to_value(head).unwrap(),
            serde_json::to_value(read).unwrap(),
        )
    };
    let (head, read) = cursors().await;
    assert!(
        !read.is_null(),
        "a read page of a session that ran carries a cursor"
    );
    assert_eq!(head, read, "the open actor's head serves the read's cursor");
    m.close().await;
    let (closed_head, closed_read) = cursors().await;
    assert_eq!(
        closed_head, closed_read,
        "the stored view's head serves the read's cursor"
    );
    assert_eq!(closed_head, head, "closing moved nothing");
}
/// A run's part-number rules (whether its requests showed the model numbered parts, and whether
/// its replies are cleaned of those numbers) are decoded from its render config once: every later
/// read of the same run's rules, as each page showing the run's messages asks for them, decodes
/// nothing.
#[tokio::test]
async fn a_runs_part_number_rules_are_decoded_once() {
    let m = module(vec![text_step("a1", "one")]).await;
    active(m.send("r1", "first").await);
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    m.close().await;
    let view = Session::view(m.backend(), m.addr(), &m.triple(), &deployment_key())
        .await
        .expect("the session loads");
    let runtime = m.runtime();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let rules = || crate::store_output_refs::run_rules(&view, &*parts, "r1");
    let before = broca_session_loop::parts::decodes_on_this_thread();
    let first = rules().await.unwrap();
    assert!(
        broca_session_loop::parts::decodes_on_this_thread() > before,
        "the first read decodes the run's render config"
    );
    let after_first = broca_session_loop::parts::decodes_on_this_thread();
    assert_eq!(rules().await.unwrap(), first);
    assert_eq!(
        broca_session_loop::parts::decodes_on_this_thread(),
        after_first,
        "a later read of the same run's rules decodes nothing"
    );
}

/// Every llm-runner surface serves a store session's history version one above the version its
/// head stores (`store_exposure::EXPOSURE_EPOCH`), from the session's first read on, and takes a
/// caller's version back the same way: a page asked for at the served version is served, and one
/// asked for at the stored version, which an earlier build served, is refused as after any
/// rewrite. A revert raises both by one.
#[tokio::test]
async fn the_served_history_version_is_the_stored_one_past_the_exposure_epoch() {
    use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
    use crate::store_exposure::{served_history_version, EXPOSURE_EPOCH};
    assert_eq!(EXPOSURE_EPOCH, 1);
    let m = module(vec![text_step("a1", "one"), text_step("b1", "two")]).await;
    active(m.send("r1", "first").await);
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    active(m.send("r2", "second").await);
    m.gate.notify_one();
    m.ended("r2", "completed").await;

    let read = m
        .registry
        .read(SessionReadRequest::new(m.key.clone(), Some(1), None))
        .await
        .unwrap();
    assert_eq!(read.history_version, 1, "a new session serves version 1");
    let head = m.registry.head(&m.key).await.unwrap();
    assert_eq!(head.history_version, 1);
    let history = crate::history::read(
        &m.registry,
        &m.key,
        &serde_json::from_value(serde_json::json!({"from_ordinal": 1})).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(history["history_version"], 1);

    // `after_mid` at the served version pages on; at the stored one it is refused.
    let after = |version| {
        let mut request = SessionReadRequest::new(m.key.clone(), None, None);
        request.after_mid = Some(read.messages[0].mid.clone());
        request.history_version = Some(version);
        request
    };
    let next = m.registry.read(after(1)).await.unwrap();
    assert_eq!(next.messages[0].ordinal, 2);
    assert!(matches!(
        m.registry.read(after(0)).await,
        Err(ServeError::HistoryChanged)
    ));

    // A revert to r2's input raises the stored version, and the served one with it.
    m.close().await;
    let mut writer = m.writer().await;
    assert_eq!(writer.head().history_version, 0);
    let (ordinal, target) = entry_of(&writer, "second").await;
    let records = writer.records_from(1).await.unwrap();
    let mut start = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) if start.run_id == "r2" => Some(start.clone()),
            _ => None,
        })
        .expect("r2's start");
    start.run_id = "r9".into();
    start.send_id = Some("s9".into());
    let input = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Input(input) if input.run_id == "r2" => {
                let mut input = input.clone();
                input.run_id = "r9".into();
                Some(input)
            }
            _ => None,
        })
        .expect("r2's input");
    let facts = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::EntryFacts(facts) if facts.ordinal == ordinal => {
                let mut facts = (**facts).clone();
                facts.run_id = Some("r9".into());
                Some(facts)
            }
            _ => None,
        })
        .expect("the input's entry facts");
    writer
        .revert(
            &target,
            vec![
                Record::RunStart(start),
                Record::Input(input),
                Record::EntryFacts(std::sync::Arc::new(facts)),
            ],
            None,
            2,
        )
        .await
        .unwrap();
    let stored = writer.head().history_version;
    assert_eq!(stored, 1, "the revert raised the stored version");
    writer.close().await.unwrap();
    let view = Session::view(m.backend(), m.addr(), &m.triple(), &deployment_key())
        .await
        .unwrap();
    let runtime = m.runtime();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let page = |version: Option<u64>| {
        let mut request = SessionReadRequest::new(m.key.clone(), Some(1), None);
        request.history_version = version;
        request
    };
    let reverted = crate::store_read::serve_view(&*view, parts, &page(None))
        .await
        .unwrap();
    assert_eq!(reverted.history_version, served_history_version(stored));
    assert_eq!(
        reverted.history_version, 2,
        "the served version rose by one"
    );
    assert!(crate::store_read::serve_view(&*view, parts, &page(Some(2)))
        .await
        .is_ok());
    for stale in [0, 1] {
        assert!(matches!(
            crate::store_read::serve_view(&*view, parts, &page(Some(stale))).await,
            Err(ServeError::HistoryChanged)
        ));
    }
    let head = crate::store_entries::history_head_of(&*view, None)
        .await
        .unwrap();
    assert_eq!(head.history_version, 2);
}

/// A send queued while a run runs has its records in that run. Its open actor answers a poll of
/// its status from memory, without reading storage, during the run the send started and after a
/// later run started; and the latest run's status poll the same. Each answer is a fresh view's.
#[tokio::test]
async fn a_queued_sends_status_is_answered_from_memory() {
    let m = module(vec![
        text_step("a1", "one"),
        text_step("b1", "two"),
        text_step("c1", "three"),
    ])
    .await;
    let keyed = |key: &str| SendKey::SendId(key.into());
    active(m.send_keyed("r1", "first", keyed("k1")).await);
    m.in_step("r1", 1).await;
    assert!(matches!(
        m.send_keyed("r2", "second", keyed("k2")).await,
        SendOutcome::Pending { .. }
    ));
    m.gate.notify_one();
    m.in_step("r2", 1).await;
    let poll = StoreReadRequest::SendStatus {
        own_run: RunId("r2".into()),
        send_id: "k2".into(),
        principal: None,
    };
    let answered = |when: &'static str, requests: Vec<StoreReadRequest>| {
        let m = &m;
        async move {
            for request in requests {
                assert!(
                    matches!(
                        m.ask_actor(request.clone()).await,
                        StoreReadOutcome::Served(_)
                    ),
                    "{when}: {request:?} is answered from the actor's memory"
                );
                assert_eq!(
                    shape(m.served(&request).await),
                    shape(m.fresh(&request).await),
                    "{when}: {request:?}"
                );
            }
        }
    };
    answered(
        "during the run the send started",
        vec![
            poll.clone(),
            StoreReadRequest::RunStatusReply(RunId("r2".into())),
        ],
    )
    .await;
    m.gate.notify_one();
    m.ended("r2", "completed").await;
    active(m.send_keyed("r3", "third", keyed("k3")).await);
    m.in_step("r3", 1).await;
    answered(
        "after a later run started",
        vec![poll, StoreReadRequest::RunStatusReply(RunId("r3".into()))],
    )
    .await;
    m.gate.notify_one();
    m.ended("r3", "completed").await;
}

/// Every read the actor answers from memory only, on a session of three runs: the first called a
/// tool and queued the sends the second and third started from, and a plan refresh came while
/// the second ran; the third is in flight. A read the actor can't answer from memory comes back
/// as the session's position (`Stored`). The reads that come back so must be exactly those that
/// did so before the open id index kept records' places instead of the records
/// (`BEFORE_STORED`), and every read answers as a fresh view does.
#[tokio::test]
async fn in_memory_reads_refuse_only_where_they_did_before() {
    let m = module(vec![
        read_step("a1"),
        text_step("a2", "one"),
        text_step("b1", "two"),
        text_step("c1", "three"),
    ])
    .await;
    let keyed = |key: &str| SendKey::SendId(key.into());
    active(m.send_keyed("r1", "first", keyed("k1")).await);
    m.in_step("r1", 1).await;
    for (run, text, key) in [("r2", "second", "k2"), ("r3", "third", "k3")] {
        assert!(matches!(
            m.send_keyed(run, text, keyed(key)).await,
            SendOutcome::Pending { .. }
        ));
    }
    m.gate.notify_one();
    m.in_step("r1", 2).await;
    m.gate.notify_one();
    m.in_step("r2", 1).await;
    let manifest: broca_records::record::PlanManifest = serde_json::from_value(serde_json::json!({
        "plan": "{}", "plan_identity": "refreshed-plan", "composition_digest": "composition",
        "tools": [], "system_text": "", "system_digest": "system",
        "join_version": 1, "role_versions": {}, "declarations": [], "fetches": []
    }))
    .unwrap();
    let refreshed = m
        .registry
        .route_session_refresh(
            &m.key,
            1,
            "on_prefix_rebuild".into(),
            Box::new(manifest),
            Vec::new(),
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            refreshed,
            crate::actor::SessionRefreshOutcome::Accepted { .. }
        ),
        "{refreshed:?}"
    );
    m.gate.notify_one();
    m.in_step("r3", 1).await;

    let run = |id: &str| RunId(id.into());
    let mut requests = Vec::new();
    for id in ["r1", "r2", "r3", "nope"] {
        requests.extend([
            StoreReadRequest::RunAdmission(run(id)),
            StoreReadRequest::RunStatus(run(id)),
            StoreReadRequest::RunStatusReply(run(id)),
            StoreReadRequest::RunResult(run(id)),
            StoreReadRequest::RunToolCalls(
                cortexkit_role_llm_runner::tool_calls::ToolCallsRequest::new(id),
            ),
        ]);
    }
    for (id, key) in [("r1", "k1"), ("r2", "k2"), ("r3", "k3"), ("r9", "k9")] {
        requests.push(StoreReadRequest::SendStatus {
            own_run: run(id),
            send_id: key.into(),
            principal: None,
        });
    }
    requests.extend(reads(&[]));
    requests.extend([
        StoreReadRequest::AuthorizationSubmissionSender("sub-r2".into()),
        StoreReadRequest::HistoryHead { mid: None },
        StoreReadRequest::Media {
            sha256: "0".repeat(64),
        },
        StoreReadRequest::TagLocation(1),
        StoreReadRequest::WritingView { ordinal: 1 },
        StoreReadRequest::OutputRefNamespace,
        StoreReadRequest::PlanChange {
            generation: Some(1),
            send_id: Some("policy-1".into()),
        },
        StoreReadRequest::PlanChange {
            generation: None,
            send_id: None,
        },
        StoreReadRequest::PlanIdentities,
        StoreReadRequest::Baseline,
        StoreReadRequest::Compaction {
            key: m.key.clone(),
            caller: Some("compactor".into()),
            method: "session.entries".into(),
            params: serde_json::value::RawValue::from_string(
                r#"{"select": {"by": "after"}, "detail": "full"}"#.into(),
            )
            .unwrap(),
        },
    ]);
    let mut stored = Vec::new();
    for request in &requests {
        match m.ask_actor(request.clone()).await {
            StoreReadOutcome::Served(_) => {}
            StoreReadOutcome::Stored { .. } => stored.push(format!("{request:?}")),
            StoreReadOutcome::Refused(error) => panic!("{request:?}: refused {error:?}"),
            StoreReadOutcome::Reopen => panic!("{request:?}: the actor closed"),
        }
        assert_eq!(
            shape(m.served(request).await),
            shape(m.fresh(request).await),
            "{request:?}"
        );
    }
    // The reads that came back as the session's position before the index kept positions, on
    // the same session: those whose history isn't in memory however it's indexed.
    let before: Vec<String> = BEFORE_STORED
        .iter()
        .map(|read| (*read).to_owned())
        .collect();
    assert_eq!(stored, before, "the reads the actor answered from storage");
    m.gate.notify_one();
    m.ended("r3", "completed").await;
}

/// The reads `in_memory_reads_refuse_only_where_they_did_before` saw come back as the session's
/// position on the code before the open id index kept positions, in its order: the status and
/// tool calls of the runs before the latest, whose records the actor keeps only for its latest
/// run, and a medium no record names.
const BEFORE_STORED: &[&str] = &[
    "RunStatus(RunId(\"r1\"))",
    "RunToolCalls(ToolCallsRequest { run_id: \"r1\", after: None, limit: None })",
    "RunStatus(RunId(\"r2\"))",
    "RunToolCalls(ToolCallsRequest { run_id: \"r2\", after: None, limit: None })",
    "Media { sha256: \"0000000000000000000000000000000000000000000000000000000000000000\" }",
];

/// What a busy session's operations cost: with its actor open and its log growing, its sends,
/// the status polls waiting on its runs, `session.head`, `run.status`, a subscribe's authorization and a `session.read` page each
/// read none of the session's log. The fixture runs retention's clock, as production does:
/// without it every operation's expiry check skipped loading the session, which hid that check's
/// full read of a running session's log from every other test.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn operations_on_a_busy_session_read_none_of_its_log() {
    busy_session_operations_read_none_of_its_log(false).await;
}

/// The same for a session whose first send carried a storage policy, which keeps it in the root
/// of sessions that expire: while its actor is open, that actor decides its expiry, so no
/// operation's expiry check loads the session either.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn operations_on_a_busy_expiring_session_read_none_of_its_log() {
    busy_session_operations_read_none_of_its_log(true).await;
}

/// The first `session.head` after an idle close reads none of the session's log: the closing
/// actor kept the head it built from memory at the position it left the session. Each later
/// write moves the session, so before, nearly every head read of a closed session loaded its
/// whole view. The kept head is the head a read of the closed session builds. It counts log
/// reads, which the store runtime keeps only under `test-clock`.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn the_first_head_after_an_idle_close_reads_no_log() {
    use crate::serve::ServeHandler;
    let m = module(vec![text_step("a1", "one"), text_step("a2", "two")]).await;
    let runtime = m.runtime();
    let scope = broca_protocol::SendScope::default();
    for run in ["r1", "r2"] {
        active(m.send(run, "hello").await);
        m.gate.notify_one();
        m.ended(run, "completed").await;
        m.close().await;
        runtime.log_reads.store(0, Ordering::Relaxed);
        let served = m.registry.head_for(&m.key, None, &scope).await.unwrap();
        assert_eq!(
            runtime.log_reads.load(Ordering::Relaxed),
            0,
            "{run}: the head after the close reads no log"
        );
        assert_eq!(served.last_run_state.as_ref().unwrap().run_id, run);
        let StoreReadReply::Head(Ok(fresh)) = m.fresh(&StoreReadRequest::Head).await else {
            panic!("{run}: a fresh head builds");
        };
        // What `session.head` adds for its caller (who may send, the input profile and the
        // prompt-cache clock) isn't part of the kept head.
        let unadorned = |mut reply: broca_wire::HeadResponse| {
            reply.sender = Default::default();
            reply.input = None;
            reply.cache = None;
            reply
        };
        assert_eq!(unadorned(served), unadorned(fresh.reply), "{run}");
    }
}

#[cfg(feature = "test-clock")]
async fn busy_session_operations_read_none_of_its_log(policy: bool) {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let m = module_with_retention(vec![
        text_step("a1", "one"),
        text_step("a2", "two"),
        text_step("a3", "three"),
    ])
    .await;
    let runtime = m.runtime();
    let reads = || runtime.log_reads.load(Ordering::Relaxed);
    for (n, run) in ["r1", "r2", "r3"].into_iter().enumerate() {
        // Counted from before the run, so the status polls that wait for its end count too:
        // each lands on a position the log has just moved past.
        runtime.log_reads.store(0, Ordering::Relaxed);
        let text = format!("message {n}");
        if policy && n == 0 {
            let mut request = m.caller.shape(run, &SendKey::Minted, request(&text));
            request.retention_policy = Some(
                cortexkit_role_llm_runner::session_retention::RetentionPolicy::new(
                    cortexkit_role_llm_runner::session_retention::Keep::days(30),
                    0,
                    false,
                    false,
                ),
            );
            active(m.submit(run, request).await);
            assert_eq!(
                runtime.class_of(&m.key).unwrap(),
                Some(broca_records::session_storage::StorageClass::Ephemeral),
                "a session with a storage policy is kept in the root of expiring sessions"
            );
        } else {
            active(m.send(run, &text).await);
        }
        m.gate.notify_one();
        m.ended(run, "completed").await;
        assert!(
            m.registry.open_handle(&m.key).is_some(),
            "the actor stays open"
        );
        let running = reads();
        runtime.log_reads.store(0, Ordering::Relaxed);
        m.registry.head(&m.key).await.unwrap();
        let head = reads();
        m.status(run).await;
        let status = reads() - head;
        ServeHandler::authorize_session_read(
            &m.registry,
            &m.key,
            &crate::reader_admission::ReadAccess::default(),
        )
        .await
        .unwrap();
        let authorize = reads() - head - status;
        m.registry
            .read(SessionReadRequest::new(m.key.clone(), Some(1), None))
            .await
            .unwrap();
        let page = reads() - head - status - authorize;
        assert_eq!(
            (running, head, status, authorize, page),
            (0, 0, 0, 0, 0),
            "round {n}: log reads of the run's sends and polls, head, status, authorization \
             and a page"
        );
    }
}
