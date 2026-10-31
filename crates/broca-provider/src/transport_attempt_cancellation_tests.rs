use super::*;
use broca_core::{FileWalStore, Provider, WalHandle, WalStore};
use broca_wal::{FramedRecord, Wal, WalConfig, WalError, WalRecord};
use std::sync::atomic::AtomicBool;

#[derive(Default)]
struct BarrierGate {
    entered: AtomicBool,
    fail_sync: AtomicBool,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}

impl BarrierGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct ReleaseOnDrop(Arc<BarrierGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

// Hold only the reservation's sync, not admission or writer startup. Every other
// operation delegates to the real file WAL so the test checks the durable terminal.
struct ReservationBarrierWal {
    inner: Box<dyn Wal + Send>,
    gate: Arc<BarrierGate>,
    reserved: bool,
}

impl Wal for ReservationBarrierWal {
    fn durable_publication(&self) -> Option<broca_wal::file_wal::DurablePublication> {
        self.inner.durable_publication()
    }
    fn log_path(&self) -> Option<&std::path::Path> {
        self.inner.log_path()
    }
    fn wal_config(&self) -> WalConfig {
        self.inner.wal_config()
    }
    fn writer_id(&self) -> Option<[u8; 16]> {
        self.inner.writer_id()
    }
    fn writer_close_reserve(&self) -> u64 {
        self.inner.writer_close_reserve()
    }
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.inner.append_budget()
    }
    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }
    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.inner.append(fence, record, sync)
    }
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.inner.append_preencoded(fence, record, sync, frame)
    }
    fn append_v4(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        prepared: broca_wal::PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<u64, WalError> {
        self.reserved |= matches!(record, WalRecord::AttemptReserved { .. });
        self.inner
            .append_v4(fence, record, sync, prepared, appended_ms)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self.reserved {
            let mut released = self.gate.released.lock().unwrap();
            self.gate.entered.store(true, Ordering::SeqCst);
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
            if self.gate.fail_sync.load(Ordering::SeqCst) {
                return Err(WalError::Io(std::io::Error::other(
                    "injected reservation sync failure",
                )));
            }
        }
        self.inner.sync()
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(&self, seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.tail_after_seq(seq)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
}

struct ObserveStep<P> {
    inner: P,
    cancel: Mutex<Option<CancellationToken>>,
    journaled: AtomicBool,
}

#[async_trait::async_trait]
impl<P: Provider> Provider for ObserveStep<P> {
    async fn step(&self, _: StepId, _: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        unreachable!("the run loop supplies runtime state")
    }
    async fn step_with_runtime(
        &self,
        step: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        *self.cancel.lock().unwrap() = Some(runtime.cancel.clone());
        self.journaled
            .store(runtime.attempt_hook.is_some(), Ordering::SeqCst);
        self.inner.step_with_runtime(step, opts, runtime).await
    }
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.inner.render_request(opts)
    }
    fn reports_attempt_evidence(&self) -> bool {
        self.inner.reports_attempt_evidence()
    }
}

#[derive(Clone, Copy)]
enum BarrierStop {
    Caller,
    Watchdog,
    SyncFailure,
    Seal,
}

async fn stop_at_reservation_barrier(stop: BarrierStop) {
    let watchdog = matches!(stop, BarrierStop::Watchdog);
    let server = ScriptedServer::start(vec![ScriptResponse::Hold]).await;
    let provider = ObserveStep {
        inner: retry_test_provider(&server, 0),
        cancel: Mutex::new(None),
        journaled: AtomicBool::new(false),
    };
    let root = attempt_scratch("cancel-reservation-barrier");
    let session = attempt_session();
    let store = FileWalStore::with_config(root.join("wal"), WalConfig::OVERHAUL);
    let gate = Arc::new(BarrierGate::default());
    let writer = WalHandle::spawn_in_store_with_fs_for_test(
        Box::new(ReservationBarrierWal {
            inner: store.open(&session).unwrap(),
            gate: gate.clone(),
            reserved: false,
        }),
        &store,
        &session,
        1,
        None,
        Arc::new(broca_wal::fs_ops::RealFs),
    )
    .unwrap();
    let release = ReleaseOnDrop(gate.clone());
    let cancel = CancellationToken::new();
    let seal = broca_core::ShutdownSeal::new();
    let control = broca_core::RunControl {
        cancel: Some(cancel.clone()),
        provider_call_timeout: watchdog.then_some(Duration::from_millis(100)),
        shutdown_seal: matches!(stop, BarrierStop::Seal).then(|| seal.clone()),
        ..Default::default()
    };
    let (out, _rx) = mpsc::channel(128);
    let stop_at_gate = async {
        // A runnable readiness waiter prevents Tokio from auto-advancing the paused
        // watchdog while the dedicated WAL thread is still reaching the explicit gate.
        while !gate.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(server.request_bodies().is_empty());
        match stop {
            BarrierStop::Watchdog => tokio::time::advance(Duration::from_millis(100)).await,
            BarrierStop::Caller => cancel.cancel(),
            BarrierStop::SyncFailure => gate.fail_sync.store(true, Ordering::SeqCst),
            BarrierStop::Seal => {
                seal.seal_and_collect(Duration::ZERO).await;
            }
        }
        if matches!(stop, BarrierStop::Watchdog | BarrierStop::Caller) {
            let step_cancel = provider.cancel.lock().unwrap().clone().unwrap();
            step_cancel.cancelled().await;
        }
        tokio::time::resume();
        gate.release();
    };
    tokio::time::pause();
    let tools = broca_core::StubToolPlane::new();
    let (result, ()) = tokio::join!(
        broca_core::run::run_loop_on_writer(
            attempt_run_request(session.clone()),
            &writer,
            broca_core::run::StartMode::FreshOrResume,
            &provider,
            &tools,
            &broca_core::AllowAll,
            &store,
            None,
            control,
            &out,
        ),
        stop_at_gate
    );
    // Join the writer even when the run returns an error, so a failing regression leaves no thread.
    drop(release);
    let shutdown = writer.shutdown().await;
    if matches!(stop, BarrierStop::SyncFailure | BarrierStop::Seal) {
        let error = result.unwrap_err().to_string();
        // Sealing can close the transport's receipt before the journal observes the
        // seal itself. Both refusals must remain WAL errors, not ordinary run terminals.
        let expected = if matches!(stop, BarrierStop::SyncFailure) {
            "injected reservation sync failure"
        } else {
            "attempt_durability_failed:"
        };
        assert!(error.contains(expected), "{error}");
        assert!(server.request_bodies().is_empty());
        let frames = store.read_records(&session).unwrap();
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. })));
        std::fs::remove_dir_all(root).unwrap();
        return;
    }
    shutdown.unwrap();
    result.unwrap();
    assert!(
        server.request_bodies().is_empty(),
        "no send permission was granted"
    );
    let frames = store.read_records(&session).unwrap();
    assert_eq!(assert_attempts_settled_or_superseded(&frames), 1);
    let releases: Vec<_> = frames.iter().filter(|frame| matches!(&frame.record,
        WalRecord::AttemptReleased { envelope, .. } if envelope.sent == broca_wal::record::AttemptSent::No
    )).collect();
    assert_eq!(releases.len(), 1);
    assert!(!frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::AttemptFinished { .. }
            | WalRecord::ModelAttemptFinished {
                attempt_id: Some(_),
                ..
            }
    )));
    let terminals: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunFinished { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(terminals.len(), 1);
    assert_eq!(
        *terminals[0],
        if watchdog {
            broca_protocol::RunFinishReason::Interrupted
        } else {
            broca_protocol::RunFinishReason::Cancelled
        }
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn caller_cancel_at_reservation_barrier_releases_unsent() {
    stop_at_reservation_barrier(BarrierStop::Caller).await;
}

#[tokio::test]
async fn watchdog_at_reservation_barrier_releases_unsent() {
    stop_at_reservation_barrier(BarrierStop::Watchdog).await;
}

#[tokio::test]
async fn reservation_sync_failure_remains_a_wal_error() {
    stop_at_reservation_barrier(BarrierStop::SyncFailure).await;
}

#[tokio::test]
async fn reservation_seal_refusal_remains_a_wal_error() {
    stop_at_reservation_barrier(BarrierStop::Seal).await;
}

#[tokio::test]
async fn legacy_run_installs_no_reservation_hook_or_billing_fields() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200,
        headers: vec![],
        body: b"done",
    }])
    .await;
    let provider = ObserveStep {
        inner: retry_test_provider(&server, 0),
        cancel: Mutex::new(None),
        journaled: AtomicBool::new(false),
    };
    let root = attempt_scratch("legacy-no-attempt-hook");
    let session = attempt_session();
    let store = FileWalStore::new(root.join("wal"));
    let writer = WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
    assert!(!writer.is_overhaul());
    let (out, _rx) = mpsc::channel(128);
    broca_core::run::run_loop_on_writer(
        attempt_run_request(session.clone()),
        &writer,
        broca_core::run::StartMode::FreshOrResume,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        &store,
        None,
        broca_core::RunControl::default(),
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    assert!(!provider.journaled.load(Ordering::SeqCst));
    assert_eq!(server.request_bodies().len(), 1);
    let frames = store.read_records(&session).unwrap();
    let mut attempts = 0;
    for frame in &frames {
        assert!(!matches!(
            frame.record,
            WalRecord::AttemptReserved { .. }
                | WalRecord::AttemptReleased { .. }
                | WalRecord::AttemptFinished { .. }
        ));
        if let WalRecord::ModelAttemptFinished {
            attempt_id,
            reservation,
            envelope,
            ..
        } = &frame.record
        {
            attempts += 1;
            assert!(attempt_id.is_none() && reservation.is_none() && envelope.is_none());
            let bytes = serde_json::to_string(&frame.record).unwrap();
            assert!(
                !bytes.contains("\"attempt_id\"")
                    && !bytes.contains("\"reservation\"")
                    && !bytes.contains("\"envelope\"")
            );
        }
    }
    assert_eq!(attempts, 1);
    std::fs::remove_dir_all(root).unwrap();
}
