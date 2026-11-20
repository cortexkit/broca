use crate::input_transformations::tests::transformation_attempt;
use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use tokio::sync::Notify;

use super::*;

#[test]
fn full_replay_fallback_stays_at_the_selected_admission_head() {
    struct LaterWal(Vec<broca_wal::FramedRecord>);
    impl WalStore for LaterWal {
        fn open(
            &self,
            _: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            panic!("the replay probe never opens a writer")
        }
        fn read_records(
            &self,
            _: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(self.0.clone())
        }
    }
    let session = BindIdentity::new("/fixture", "test", "replay-cut");
    let started = serde_json::from_value(serde_json::json!({
        "type":"run_started","run_id":"run","session":session,
        "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],
            "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
            "context_epoch":0,"renderer_version":1,"frozen_render_config":null},
        "input":[{"role":"user","content":[{"type":"text","text":"selected"}]}],
        "origin":{"kind":"fresh"}
    }))
    .unwrap();
    let frames = vec![
            broca_wal::FramedRecord { seq: 1, fence: 1, record: started },
            broca_wal::FramedRecord { seq: 2, fence: 1, record: serde_json::from_value(serde_json::json!({
                "type":"steered","input":[{"role":"user","content":[{"type":"text","text":"later live input"}]}]
            })).unwrap() },
        ];
    let snapshot = PriorWalSnapshot {
        lineage_id: None,
        anchor: None,
        base_watermark: 1,
        head_seq: 1,
        tail: Arc::new(vec![]),
        checkpoint_at_base: None,
        checkpoint_at_head: crate::AdmissionCheckpointV1::from_frames(&frames[..1]).unwrap(),
    };
    let replayed = replay_complete_snapshot_or_wal(&snapshot, &LaterWal(frames), &session).unwrap();
    assert!(replayed.pending_steers.is_empty());
    assert_eq!(replayed.prompt.len(), 1);
    assert!(serde_json::to_string(&replayed.prompt)
        .unwrap()
        .contains("selected"));
    assert!(!serde_json::to_string(&replayed.prompt)
        .unwrap()
        .contains("later live input"));
}

fn attempt_template() -> broca_wal::record::AttemptReservation {
    serde_json::from_value(serde_json::json!({
        "run_id":"billing-run", "step_id":1, "attempt":0, "send_index":1,
        "warm":false, "reserved_at_ms":1, "provider":"test", "endpoint_host":"example.com",
        "model_sent":"model", "credential":{},
        "session":{"project_root":"/project","harness":"runner","session":"s","session_ref":"ref"},
        "run":{}, "request_body_bytes":10, "compressed":false
    }))
    .unwrap()
}

fn physical_send() -> PhysicalSend {
    PhysicalSend {
        attempt: 0,
        send_index: 1,
        endpoint_host: "example.com".into(),
        model_sent: "model".into(),
        request_body_bytes: 10,
        compressed: false,
        account_ref: None,
        speed_requested: None,
        warm: None,
        auth_refreshed: false,
    }
}

struct BusyAttemptWal {
    inner: RecordingWal,
    hold: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    entered: Arc<Notify>,
    synced_heads: Arc<std::sync::Mutex<Vec<usize>>>,
}

struct UnblockBarrierOnDrop(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Drop for UnblockBarrierOnDrop {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = false;
        self.0 .1.notify_all();
    }
}

#[derive(Default)]
struct ReservationWaitProvider {
    granted: Arc<std::sync::atomic::AtomicUsize>,
    refusal: Arc<std::sync::Mutex<Option<ProviderError>>>,
    refused: Arc<Notify>,
}

#[async_trait::async_trait]
impl Provider for ReservationWaitProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        unreachable!("reservation test must pass runtime state")
    }
    async fn step_with_runtime(
        &self,
        _step_id: StepId,
        _opts: Arc<CallOptions>,
        runtime: crate::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(4);
        let granted = self.granted.clone();
        let refusal = self.refusal.clone();
        let refused = self.refused.clone();
        tokio::spawn(async move {
            let result = runtime
                .attempt_hook
                .unwrap()
                .before_send(physical_send(), runtime.cancel)
                .await;
            match result {
                Ok(_) => {
                    granted.fetch_add(1, Ordering::SeqCst);
                }
                Err(error) => {
                    *refusal.lock().unwrap() = Some(error.clone());
                    refused.notify_one();
                    let _ = tx
                        .send(NormalizedEvent::Control(ProviderControl::Error {
                            error,
                            retries_used: 0,
                        }))
                        .await;
                }
            }
        });
        rx
    }
    fn render_request(&self, _opts: &CallOptions) -> Vec<u8> {
        b"{}".to_vec()
    }
}

async fn check_stuck_reservation_barrier(watchdog: bool) {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hold = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let entered = Arc::new(Notify::new());
    let writer = WalHandle::spawn(
        Box::new(BusyAttemptWal {
            inner: RecordingWal {
                records: records.clone(),
            },
            hold: hold.clone(),
            entered: entered.clone(),
            synced_heads: Default::default(),
        }),
        1,
    )
    .unwrap();
    *hold.0.lock().unwrap() = true;
    // Release only during teardown, after checking that the step stopped while the
    // writer remained blocked. A panic also unblocks the writer before its join.
    let unblock = UnblockBarrierOnDrop(hold.clone());
    let cancel = CancellationToken::new();
    let provider = ReservationWaitProvider::default();
    let control = RunControl {
        cancel: Some(cancel.clone()),
        provider_call_timeout: watchdog.then_some(Duration::from_millis(50)),
        ..Default::default()
    };
    let seal = crate::ShutdownSeal::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let hook = Arc::new(ChannelAttemptHook(tx));
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: Some(&seal),
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let opts = Arc::new(CallOptions::new(Vec::new()));
    let (out, _rx) = mpsc::channel(4);
    let step = run_step_watched(
        &provider,
        StepId(1),
        &opts,
        0,
        &control,
        display_text::DisplayOutput::new(&out, false),
        AttemptCallbacks {
            record: &|_| Box::pin(async { Ok(()) }),
            before_send: Some(hook),
        },
    );
    let cancel_at_barrier = async {
        let entered = entered.notified();
        tokio::pin!(entered);
        // Keep the paused watchdog from auto-advancing during writer-thread I/O.
        while futures::poll!(&mut entered).is_pending() {
            tokio::task::yield_now().await;
        }
        if watchdog {
            tokio::time::advance(Duration::from_millis(50)).await;
        } else {
            cancel.cancel();
        }
    };
    let (result, ()) = tokio::join!(
        tokio::time::timeout(
            Duration::from_millis(500),
            journal_model_step(step, &mut rx, &mut journal)
        ),
        cancel_at_barrier
    );
    assert!(
        *hold.0.lock().unwrap(),
        "the barrier must still be held when the step stops"
    );
    assert_eq!(
        result
            .expect("stuck reservation must not starve cancellation or watchdog")
            .unwrap()
            .unwrap()
            .err()
            .expect("a stopped step must not produce a model outcome"),
        if watchdog {
            RunFinishReason::Interrupted
        } else {
            RunFinishReason::Cancelled
        }
    );
    tokio::time::timeout(Duration::from_millis(100), provider.refused.notified())
        .await
        .unwrap();
    assert_eq!(
        provider
            .refusal
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .provider_code
            .as_deref(),
        Some("attempt_durability_failed")
    );
    assert_eq!(provider.granted.load(Ordering::SeqCst), 0);
    assert!(journal.pending.values().all(|pending| !pending.started));
    drop(unblock);
    writer.sync().await.unwrap();
    seal.seal_and_collect(Duration::ZERO).await;
    seal.record_hook_fallbacks(&writer).await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(records.iter().filter(|record| matches!(record, WalRecord::AttemptReleased { envelope, .. } if envelope.sent == broca_wal::record::AttemptSent::No)).count(), 1);
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::AttemptFinished { .. })));
}

#[tokio::test(start_paused = true)]
async fn attempt_hook_stuck_barrier_caller_cancel_is_bounded() {
    check_stuck_reservation_barrier(false).await;
}

#[tokio::test(start_paused = true)]
async fn attempt_hook_stuck_barrier_provider_watchdog_is_pollable() {
    check_stuck_reservation_barrier(true).await;
}

impl broca_wal::Wal for BusyAttemptWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.inner.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        let (lock, changed) = &*self.hold;
        let mut hold = lock.lock().unwrap();
        if *hold {
            self.entered.notify_one();
        }
        while *hold {
            hold = changed.wait(hold).unwrap();
        }
        self.synced_heads
            .lock()
            .unwrap()
            .push(self.inner.records.lock().unwrap().len());
        self.inner.sync()
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(
        &self,
        seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.tail_after_seq(seq)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
}

#[tokio::test]
async fn attempt_hook_writer_busy_and_pause_releases_unsent() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hold = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let entered = Arc::new(Notify::new());
    let writer = WalHandle::spawn(
        Box::new(BusyAttemptWal {
            inner: RecordingWal {
                records: records.clone(),
            },
            hold: hold.clone(),
            entered: entered.clone(),
            synced_heads: Default::default(),
        }),
        1,
    )
    .unwrap();
    *hold.0.lock().unwrap() = true;
    let cancel = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let recorded = Default::default();
    let seal = crate::ShutdownSeal::new();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: Some(&seal),
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let step = async {
        let error = hook
            .before_send(physical_send(), cancel.clone())
            .await
            .unwrap_err();
        assert_eq!(
            error.provider_code.as_deref(),
            Some("attempt_durability_failed")
        );
    };
    let unblock = async {
        entered.notified().await;
        // A reservation barrier must not retain the writer's enqueue lock. Enqueueing
        // another record while fsync is blocked would deadlock if the hook held it.
        writer
            .record(WalRecord::TurnFinished {
                step_id: StepId(99),
            })
            .into_result()
            .unwrap();
        cancel.cancel();
        *hold.0.lock().unwrap() = false;
        hold.1.notify_all();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(journal_model_step(step, &mut rx, &mut journal), unblock)
    })
    .await
    .expect("reservation holds no writer lock across await");
    result.unwrap();
    // The release is queued, not yet durable. A later successful sync persists it,
    // and sealing must not duplicate the settlement already owned by the journal.
    seal.seal_and_collect(Duration::ZERO).await;
    seal.record_hook_fallbacks(&writer).await.unwrap();
    let (out, _rx) = mpsc::channel(16);
    pause_run_impl(
        &writer,
        &out,
        &RunControl::default(),
        RunId("billing-run".into()),
        broca_protocol::PauseReason::AuthRequired,
        None,
        None,
        0,
        Usage::default(),
        0,
        None,
    )
    .await
    .unwrap();
    let records = records.lock().unwrap();
    let release = records
        .iter()
        .position(|record| matches!(record, WalRecord::AttemptReleased { .. }))
        .unwrap();
    let pause = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunPaused { .. }))
        .unwrap();
    assert!(release < pause);
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::AttemptReserved { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record,
                WalRecord::AttemptFinished { .. } | WalRecord::ModelAttemptFinished { .. }
            ))
            .count(),
        0
    );
}

#[tokio::test(start_paused = true)]
async fn attempt_hook_missing_receipt_has_permission_deadline() {
    let (tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let error = tokio::time::timeout(
        RESERVATION_PERMISSION_TIMEOUT + Duration::from_secs(1),
        hook.before_send(physical_send(), CancellationToken::new()),
    )
    .await
    .expect("permission wait must be bounded even without a provider watchdog")
    .unwrap_err();
    assert_eq!(
        error.provider_code.as_deref(),
        Some("attempt_durability_failed")
    );
    assert!(error.message.contains("permission deadline"));
    let AttemptRequest::Reserve { reply, .. } = requests.try_recv().unwrap() else {
        panic!("one reservation request")
    };
    assert!(
        reply.is_closed(),
        "a late handler must not grant send permission"
    );
}

#[tokio::test]
async fn completed_step_refuses_late_reservation_without_send_permission() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    let (tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: None,
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let reservation = hook.before_send(physical_send(), CancellationToken::new());
    tokio::pin!(reservation);
    let step = async {
        // Enqueue from the step's final poll, after the outer select has already
        // polled the empty request queue. Keep the receipt alive after completion.
        assert!(futures::poll!(&mut reservation).is_pending());
        17
    };
    assert_eq!(
        journal_model_step(step, &mut requests, &mut journal)
            .await
            .unwrap(),
        17
    );
    let permission = reservation.await;
    assert!(
        permission.is_err(),
        "a completed step must not grant a late send permission"
    );
    assert_eq!(
        permission.unwrap_err().provider_code.as_deref(),
        Some("attempt_durability_failed")
    );
    writer.sync().await.unwrap();
    assert!(
        records.lock().unwrap().is_empty(),
        "a late reservation must not create a new send intent"
    );
}

#[tokio::test]
async fn completed_step_still_settles_late_finish() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    let (tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: None,
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let reservation = hook.before_send(physical_send(), CancellationToken::new());
    tokio::pin!(reservation);
    assert!(futures::poll!(&mut reservation).is_pending());
    journal
        .handle(requests.recv().await.unwrap(), &CancellationToken::new())
        .await
        .unwrap();
    let id = reservation.await.unwrap();
    let mut attempt = transformation_attempt(None, true);
    attempt.sent_at_ms = Some(1);
    let response = broca_wal::record::AttemptEnvelope {
        vendor_request_id: Some("reported-request".into()),
        usage: Some(Usage {
            input_tokens: Some(0),
            output_tokens: Some(7),
            ..Default::default()
        }),
        usage_raw: Some(serde_json::json!({"input_tokens":0,"output_tokens":7,"future_key":false})),
        ..Default::default()
    };
    let finish = hook.finished(
        id.clone(),
        attempt,
        broca_wal::record::BillingAttemptOutcome::Complete,
        Some(response.clone()),
    );
    tokio::pin!(finish);
    let step = async {
        assert!(futures::poll!(&mut finish).is_pending());
        23
    };
    assert_eq!(
        journal_model_step(step, &mut requests, &mut journal)
            .await
            .unwrap(),
        23
    );
    finish.await.unwrap();
    writer.sync().await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(records.iter().filter(|record| matches!(record, WalRecord::ModelAttemptFinished { attempt_id: Some(terminal_id), .. } if terminal_id == &id)).count(), 1);
    let envelope = records
        .iter()
        .find_map(|record| match record {
            WalRecord::ModelAttemptFinished { envelope, .. } => envelope.as_ref(),
            _ => None,
        })
        .unwrap();
    assert_eq!(envelope.usage, response.usage);
    assert_eq!(envelope.usage_raw, response.usage_raw);
    assert_eq!(envelope.vendor_request_id, response.vendor_request_id);
    assert_eq!(envelope.sent_at_ms, Some(1));
    assert_eq!(
        envelope.outcome,
        broca_wal::record::BillingAttemptOutcome::Complete
    );
    assert!(!records.iter().any(|record| matches!(
        record,
        WalRecord::AttemptFinished { .. } | WalRecord::AttemptReleased { .. }
    )));
}

#[tokio::test]
async fn attempt_hook_compression_refusal_and_resend_share_one_sync() {
    use broca_wal::record::BillingAttemptOutcome;
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let synced_heads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(BusyAttemptWal {
            inner: RecordingWal {
                records: records.clone(),
            },
            hold: Default::default(),
            entered: Default::default(),
            synced_heads: synced_heads.clone(),
        }),
        1,
    )
    .unwrap();
    synced_heads.lock().unwrap().clear();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: None,
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let step = async {
        let mut send = physical_send();
        send.compressed = true;
        let first = hook
            .before_send(send.clone(), CancellationToken::new())
            .await
            .unwrap();
        let mut refusal = transformation_attempt(None, true);
        refusal.sent_at_ms = Some(1);
        refusal.http_status = Some(415);
        refusal.outcome = broca_protocol::AttemptOutcome::HttpError;
        hook.finished(
            first.clone(),
            refusal,
            BillingAttemptOutcome::CompressionRefused,
            None,
        )
        .await
        .unwrap();
        assert!(recorded.lock().unwrap().is_empty());
        send.send_index = 2;
        send.compressed = false;
        let second = hook
            .before_send(send, CancellationToken::new())
            .await
            .unwrap();
        assert_ne!(first, second);
        hook.finished(
            second,
            transformation_attempt(None, true),
            BillingAttemptOutcome::Complete,
            None,
        )
        .await
        .unwrap();
    };
    journal_model_step(step, &mut rx, &mut journal)
        .await
        .unwrap();
    writer.sync().await.unwrap();
    assert_eq!(
        *synced_heads.lock().unwrap(),
        [1, 3, 4],
        "refusal is not synced separately from resend reservation"
    );
    let records = records.lock().unwrap();
    assert!(
        matches!(&records[1], WalRecord::ModelAttemptFinished { envelope: Some(envelope), .. } if envelope.outcome == BillingAttemptOutcome::CompressionRefused)
    );
    assert!(
        matches!(&records[2], WalRecord::AttemptReserved { reservation, .. } if reservation.send_index == 2 && !reservation.compressed)
    );
}

#[tokio::test]
#[ignore = "explicit disk-fsync latency measurement on each target"]
async fn attempt_hook_render_to_send_latency_and_fsync_cost() {
    use crate::durability_counters::{counts_for_fence, FsyncCause};
    let root = broca_test_support::scratch_root().join(format!(
        "attempt-latency-{}",
        crate::attempt_id::mint_attempt_id().unwrap().as_str()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let template = attempt_template();
    let session = template.session.bind.clone();
    let store = crate::FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
    let fence = 0x4154544500000000 + u64::from(std::process::id());
    let writer =
        WalHandle::spawn_in_store(store.open(&session).unwrap(), &store, &session, fence, None)
            .unwrap();
    assert!(writer.is_overhaul());
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: None,
        template,
        pending: Default::default(),
        recorded: &recorded,
        before_start: None,
    };
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let mut first = Vec::new();
    let mut retry = Vec::new();
    let step = async {
        for _ in 0..256 {
            // Admission must be durable before acknowledging active; tool results must
            // be durable before rendering the next step. Rendering after either sync
            // leaves the new reservation needing its own durability barrier.
            writer
                .record(WalRecord::TurnFinished {
                    step_id: StepId(99),
                })
                .into_result()
                .unwrap();
            writer.sync().await.unwrap();
            for ordinal in 0..2 {
                let body =
                    serde_json::to_vec(&serde_json::json!({"model":"m", "input":"x".repeat(8192)}))
                        .unwrap();
                let before = counts_for_fence(fence);
                let rendered = std::time::Instant::now();
                let mut send = physical_send();
                send.attempt = ordinal;
                send.request_body_bytes = body.len() as u64;
                let id = hook
                    .before_send(send, CancellationToken::new())
                    .await
                    .unwrap();
                let latency = rendered.elapsed().as_nanos();
                if ordinal == 0 {
                    first.push(latency);
                } else {
                    retry.push(latency);
                }
                let after = counts_for_fence(fence);
                assert_eq!(
                    after.get(FsyncCause::Barrier) - before.get(FsyncCause::Barrier),
                    1
                );
                assert_eq!(after.total() - before.total(), 1);
                let mut terminal = transformation_attempt(None, true);
                terminal.attempt = ordinal;
                hook.finished(
                    id,
                    terminal,
                    broca_wal::record::BillingAttemptOutcome::Complete,
                    None,
                )
                .await
                .unwrap();
            }
        }
    };
    journal_model_step(step, &mut rx, &mut journal)
        .await
        .unwrap();
    fn percentiles(samples: &mut [u128]) -> (u128, u128) {
        samples.sort_unstable();
        (
            samples[samples.len() / 2],
            samples[(samples.len() * 99).div_ceil(100) - 1],
        )
    }
    let (first_p50, first_p99) = percentiles(&mut first);
    let (retry_p50, retry_p99) = percentiles(&mut retry);
    println!("attempt reservation latency: os={} samples_per_kind=256 first_p50_ns={first_p50} first_p99_ns={first_p99} retry_p50_ns={retry_p50} retry_p99_ns={retry_p99}; added_fsyncs_per_attempt barrier=1 deadline=0 shutdown=0 disconnect=0", std::env::consts::OS);
    println!("computed production-count day: first=110956 retry=221 extra_barrier=111177 extra_deadline=0 extra_shutdown=0 extra_disconnect=0 (computed, not observed)");
    drop(journal);
    writer.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn attempt_hook_shutdown_cut_terminals_precede_interrupted_exactly_once() {
    use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome};
    for point in [
        "enqueued",
        "synced",
        "send_started",
        "response_begun",
        "settled",
    ] {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = WalHandle::spawn(
            Box::new(RecordingWal {
                records: records.clone(),
            }),
            1,
        )
        .unwrap();
        let seal = crate::ShutdownSeal::new();
        let id = crate::attempt_id::mint_attempt_id().unwrap();
        let mut pending = PendingAttempt {
            reservation: attempt_template(),
            started: false,
            barrier_acknowledged: point != "enqueued",
        };
        assert!(seal
            .reserve_billing_attempt(
                &writer,
                id.clone(),
                WalRecord::AttemptReserved {
                    attempt_id: id.clone(),
                    reservation: Box::new(pending.reservation.clone()),
                },
                pending.fallback(id.clone())
            )
            .unwrap());
        if point != "enqueued" {
            writer.sync().await.unwrap();
        }
        let started = matches!(point, "send_started" | "response_begun" | "settled");
        if started {
            pending.started = true;
            assert!(seal.start_billing_attempt(&id, pending.fallback(id.clone())));
        }
        if point == "settled" {
            let mut terminal =
                WalRecord::model_attempt_finished(StepId(1), transformation_attempt(None, true));
            if let WalRecord::ModelAttemptFinished {
                attempt_id,
                reservation,
                envelope,
                ..
            } = &mut terminal
            {
                *attempt_id = Some(id.clone());
                *reservation = Some(Box::new(pending.reservation.clone()));
                *envelope = Some(Box::new(AttemptEnvelope {
                    sent: AttemptSent::Yes,
                    outcome: BillingAttemptOutcome::Complete,
                    ..Default::default()
                }));
            }
            seal.settle_billing_attempt(&writer, &id, terminal).unwrap();
        }
        seal.seal_and_collect(Duration::ZERO).await;
        seal.record_hook_fallbacks(&writer).await.unwrap();
        // A late completion cannot settle again after the seal has taken its slot.
        seal.settle_billing_attempt(&writer, &id, pending.fallback(id.clone()))
            .unwrap();
        writer
            .record_terminal(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Interrupted,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            })
            .unwrap();
        writer.sync().await.unwrap();
        let records = records.lock().unwrap();
        let terminals: Vec<_> = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(
                    record,
                    WalRecord::AttemptReleased { .. }
                        | WalRecord::AttemptFinished { .. }
                        | WalRecord::ModelAttemptFinished { .. }
                )
            })
            .collect();
        assert_eq!(terminals.len(), 1, "{point}");
        assert!(
            terminals[0].0
                < records
                    .iter()
                    .position(|record| matches!(record, WalRecord::RunFinished { .. }))
                    .unwrap(),
            "{point}"
        );
        assert!(
            match terminals[0].1 {
                WalRecord::AttemptReleased { envelope, .. } =>
                    !started && envelope.sent == AttemptSent::No,
                WalRecord::AttemptFinished { envelope, .. } =>
                    started && point != "settled" && envelope.sent == AttemptSent::Unknown,
                WalRecord::ModelAttemptFinished { .. } => point == "settled",
                _ => false,
            },
            "wrong terminal at {point}"
        );
    }
}

#[tokio::test]
async fn attempt_hook_cancel_after_barrier_never_grants_permission() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let reached = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: None,
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: Some((reached.clone(), proceed.clone())),
    };
    let (reply, receipt) = tokio::sync::oneshot::channel();
    let step_stopped = CancellationToken::new();
    let stop = async {
        reached.notified().await;
        cancel.cancel();
        proceed.notify_one();
    };
    let request = AttemptRequest::Reserve {
        send: physical_send(),
        cancel: cancel.clone(),
        reply,
    };
    let (result, ()) = tokio::join!(journal.handle(request, &step_stopped), stop);
    result.unwrap();
    assert!(
        receipt.await.unwrap().is_err(),
        "even an unpolled receipt must never receive permission after cancellation"
    );
    journal.close().unwrap();
    writer.sync().await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::AttemptReserved { .. }))
            .count(),
        1
    );
    assert_eq!(records.iter().filter(|record| matches!(record, WalRecord::AttemptReleased { envelope, .. } if envelope.sent == broca_wal::record::AttemptSent::No)).count(), 1);
    assert!(!records.iter().any(|record| matches!(
        record,
        WalRecord::AttemptFinished { .. } | WalRecord::ModelAttemptFinished { .. }
    )));
}

#[tokio::test]
async fn attempt_hook_seal_refused_start_releases_through_real_channel() {
    use broca_wal::record::AttemptSent;
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    let seal = crate::ShutdownSeal::new();
    let cancel = CancellationToken::new();
    let reached = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let hook = ChannelAttemptHook(tx);
    let recorded = Default::default();
    let mut journal = AttemptJournal {
        evidence: None,
        account_limit_pause: false,
        writer: &writer,
        seal: Some(&seal),
        template: attempt_template(),
        pending: Default::default(),
        recorded: &recorded,
        before_start: Some((reached.clone(), proceed.clone())),
    };
    let step = async {
        let result = hook.before_send(physical_send(), cancel.clone()).await;
        assert!(
            result.is_err(),
            "a sealed reservation must never grant send permission"
        );
    };
    let cancel_and_seal = async {
        reached.notified().await;
        // Seal after the barrier but before permission, without caller cancellation,
        // so the seal itself must refuse to start the durable reservation.
        seal.seal_and_collect(Duration::ZERO).await;
        proceed.notify_one();
    };
    let (result, ()) = tokio::join!(
        journal_model_step(step, &mut rx, &mut journal),
        cancel_and_seal
    );
    result.unwrap();
    seal.record_hook_fallbacks(&writer).await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::AttemptReserved { .. }))
            .count(),
        1
    );
    assert_eq!(records.iter().filter(|record| matches!(record, WalRecord::AttemptReleased { envelope, .. } if envelope.sent == AttemptSent::No)).count(), 1);
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, WalRecord::AttemptFinished { .. })),
        "refused start must remain provably unsent"
    );
}

#[test]
fn unchanged_effort_checkpoint_keeps_opted_in_store_prior_on_fast_path() {
    let frozen = serde_json::json!({
        "family_id": "openai_responses_chatgpt", "wire_model_id": "gpt-6.1-sol",
        "chatgpt_effort_updates": true, "reasoning": { "effort": "xhigh" }
    });
    let mut prior = crate::replay::replay(&[]).unwrap();
    prior.render_episodes = None;
    prior.run_config = Some(RunConfig {
        global_system: None,
        account_route: None,
        route_decision_id: None,
        served_provider: None,
        account_limit_pause: false,
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: broca_protocol::request::ModelHandle::new("chatgpt", "gpt-6.1-sol"),
        tools: vec![].into(),
        tool_choice: Default::default(),
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: frozen.clone(),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
    });
    assert!(
        store_prior_needs_episode_spans(&prior),
        "unknown history cannot use the store seed"
    );
    let mut checkpoint = crate::AdmissionCheckpointV1::empty();
    checkpoint.effort_history.as_mut().unwrap().first_config = frozen;
    checkpoint.effort_history.as_mut().unwrap().has_first = true;
    checkpoint.restore_unchanged_effort(&mut prior);
    assert!(
        !store_prior_needs_episode_spans(&prior),
        "unchanged effort must not force full replay"
    );
    checkpoint.effort_history.as_mut().unwrap().changed = true;
    prior.render_episodes = None;
    checkpoint.restore_unchanged_effort(&mut prior);
    assert!(
        store_prior_needs_episode_spans(&prior),
        "any historical change still needs full replay"
    );
}

fn failed_step() -> Result<StepOutcome, RunFinishReason> {
    Ok(StepOutcome::Failed {
        error: ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class: broca_protocol::ErrorClass::ProviderUnavailable,
            message:
                "provider stream ended without an assembled assistant message and Finished control"
                    .into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            account_continue: None,
        },
        retries_used: 0,
    })
}

/// The run-loop half of the cancel race: a step that failed while the run's cancel was
/// tripped ends the run as Cancelled. The e2e test in broca-provider exercises the race
/// itself but loses it only occasionally, so the rule is pinned here deterministically.
/// Both control arms matter: a failure with no cancel stays a failure, and a completed
/// step keeps its result even when cancel is set.
#[test]
fn a_step_that_failed_during_a_cancel_is_a_cancellation() {
    assert!(matches!(
        attribute_step_failure_to_cancel(failed_step(), true),
        Err(RunFinishReason::Cancelled)
    ));
    assert!(matches!(
        attribute_step_failure_to_cancel(failed_step(), false),
        Ok(StepOutcome::Failed { .. })
    ));
    let completed = Ok(StepOutcome::Completed {
        raw_finish_reason: None,
        tool_args_cuts: Vec::new(),
        message: AssistantMessage {
            message_id: broca_protocol::ids::MessageId("m".into()),
            content: Vec::new(),
        },
        usage: Usage::default(),
        retries_used: 0,
        finish_reason: None,
        service_tier_used: None,
        speed_used: None,
        served_model: None,
        usage_iterations: None,
    });
    assert!(matches!(
        attribute_step_failure_to_cancel(completed, true),
        Ok(StepOutcome::Completed { .. })
    ));
    assert!(matches!(
        attribute_step_failure_to_cancel(Err(RunFinishReason::Interrupted), true),
        Err(RunFinishReason::Interrupted)
    ));
}

#[test]
fn resume_work_class_uses_frozen_value_over_caller_value() {
    assert_eq!(
        admitted_work_class(Some("athena"), true, false, Some("mason")),
        Some("mason".into())
    );
    assert_eq!(
        admitted_work_class(Some("athena"), false, true, Some("mason")),
        Some("athena".into())
    );
}

#[test]
fn fresh_tool_filter_matches_clone_filter_and_preserves_request_order() {
    let make_tool = |name: &str, index: usize| Tool {
        name: name.into(),
        description: format!("description-{index}"),
        input_schema: serde_json::json!({"index": index}),
        module: Some(format!("module-{index}")),
        execution_mode: if index.is_multiple_of(2) {
            ExecutionMode::Parallel
        } else {
            ExecutionMode::Sequential
        },
        reply: None,
    };
    let mut req = RunRequest {
        account_route: None,
        route_decision_id: None,
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("filter-order".into()),
        session: BindIdentity {
            project_root: "/tmp/filter-order".into(),
            harness: "test".into(),
            session: "filter-order".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: Vec::new(),
        tools: vec![
            make_tool("denied-first", 0),
            make_tool("allowed-first", 1),
            make_tool("denied-middle", 2),
            make_tool("allowed-second", 3),
            make_tool("denied-last", 4),
        ],
        policy_allowed: ["allowed-first".into(), "allowed-second".into()]
            .into_iter()
            .collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_management: None,
        inherit_frozen_cache: false,
        structured_output: None,
        head_turn: false,
    };
    // This is the cloning filter that retain replaces; comparing complete Tool values pins
    // both policy membership and the provider-visible request order.
    let clone_filter_output = req
        .tools
        .iter()
        .filter(|tool| req.policy_allowed.contains(&tool.name))
        .cloned()
        .collect::<Vec<_>>();
    let prior = replay(&[]).expect("empty replay state");

    let provider = crate::provider::StubProvider::new(Vec::new());
    let cfg = request_config(&mut req, false, &prior, &provider).expect("fresh config resolves");

    assert_eq!(cfg.tools.as_ref(), &clone_filter_output);
    assert!(
        req.tools.is_empty(),
        "fresh config takes ownership of the request tool buffer"
    );
}

#[tokio::test]
async fn retention_call_options_flag_and_projection_follow_first_wal_run() {
    for (name, retention) in [("retained", Some(3600)), ("forever", None)] {
        let root = broca_test_support::scratch_root()
            .join(format!("core-retention-call-{}-{name}", std::process::id()));
        let session = BindIdentity::new("/retention-test", "runner", name);
        let wal_store = crate::FileWalStore::new(root.join("wal"));
        let path = if retention.is_some() {
            wal_store.ephemeral_wal_path(&session)
        } else {
            wal_store.wal_path(&session)
        };
        crate::wal_writer::retention_tests::write_decoded_fixture(
            &path,
            &crate::retention::tests::frames(retention),
        );
        let submission = serde_json::from_value(serde_json::json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [{"role": "user", "content": [{"type": "text", "text": "next"}]}],
            "stop_when": [{"type": "step_count", "n": 1}]
        }))
        .unwrap();
        let req = RunRequest::from_submission(
            submission,
            RunId("r2".into()),
            session,
            broca_protocol::SubmissionId("sub2".into()),
        );
        let provider = crate::StubProvider::new(vec![Vec::new()]);
        let store = Arc::new(CapturingProjectionStore::default());
        let (out, _events) = mpsc::channel(64);
        run_loop_append_episode(
            req,
            &provider,
            &crate::StubToolPlane::new(),
            &crate::FileLeaseStore::new(root.join("leases")),
            &crate::AllowAll,
            &wal_store,
            Some(store.clone()),
            &out,
        )
        .await
        .unwrap();
        let calls = provider.calls();
        assert_eq!(
            calls.len(),
            1,
            "the provider must observe the actual execution path"
        );
        assert_eq!(calls[0].ephemeral_session, retention.is_some());
        if retention.is_some() {
            assert_eq!(
                store
                    .projection_calls
                    .load(std::sync::atomic::Ordering::SeqCst),
                0,
                "no content crosses the Persister seam, even on a terminal"
            );
        } else {
            assert!(
                store
                    .projection_calls
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0,
                "forever projection positive control"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn retention_projection_skips_per_turn_and_terminal_persister_calls() {
    let root = broca_test_support::scratch_root()
        .join(format!("core-retention-persister-{}", std::process::id()));
    let session = BindIdentity::new("/retention-test", "runner", "persister");
    let wal_store = crate::FileWalStore::new(root.join("wal"));
    crate::wal_writer::retention_tests::write_decoded_fixture(
        &wal_store.ephemeral_wal_path(&session),
        &crate::retention::tests::frames(Some(3600)),
    );
    let store = Arc::new(CapturingProjectionStore::default());
    for safe_h in [4, 5] {
        project_committed(&wal_store, store.clone(), &session, safe_h, false).await;
    }
    assert_eq!(
        store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(store.projected_seqs.lock().unwrap().is_empty());
    assert!(store.persisted_anchor.lock().unwrap().is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn retained_approval_debt_uses_only_the_content_free_persister_seam() {
    struct WalView {
        records: Vec<broca_wal::FramedRecord>,
        lineage: broca_wal::WalLineageId,
    }
    impl WalStore for WalView {
        fn open(
            &self,
            _: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unreachable!("read-only fixture")
        }
        fn read_records(
            &self,
            _: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(self.records.clone())
        }
        fn read_records_range(
            &self,
            _: &BindIdentity,
            after: u64,
            through: Option<u64>,
            _: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            let mut range = broca_wal::WalRangeRead::without_anchors(
                self.records
                    .iter()
                    .filter(|frame| {
                        frame.seq > after && through.is_none_or(|head| frame.seq <= head)
                    })
                    .cloned()
                    .collect(),
            );
            range.lineage_id = Some(self.lineage);
            Ok(range)
        }
    }
    let lineage = crate::projection_plan_tests::incoming_lineage();
    let records = crate::retention::tests::deferred_records(Some(3600), &lineage.to_string());
    let safe_h = records.last().unwrap().seq;
    let wal = WalView { records, lineage };
    let store = Arc::new(CapturingProjectionStore::default());
    project_committed(
        &wal,
        store.clone(),
        &BindIdentity::new("/retention-test", "runner", "approval"),
        safe_h,
        false,
    )
    .await;
    let metadata = store.retention_metadata.lock().unwrap();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].withdrawals_owed_runs, ["r1"]);
    assert!(!format!("{metadata:?}").contains(crate::retention::tests::MARKER));
    assert_eq!(
        store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(store.projected_seqs.lock().unwrap().is_empty());
    assert!(store.persisted_anchor.lock().unwrap().is_none());
}

#[test]
fn transformed_render_reuses_payloads_and_append_keeps_the_snapshot() {
    let mut request = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "large snapshot sentinel".into(),
        }],
    )]);
    request.tools = Arc::new(vec![Tool {
        name: "nested-tool".into(),
        description: "shared transform payload".into(),
        input_schema: serde_json::json!({"properties": {"nested": {"type": "object"}}}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    }]);
    request.provider_options = Arc::new(serde_json::json!({"large": "x".repeat(4096)}));
    let opts = Arc::new(request);
    assert_eq!(Arc::strong_count(&opts.tools), 1);
    assert_eq!(Arc::strong_count(&opts.tool_choice), 1);
    assert_eq!(Arc::strong_count(&opts.response_format), 1);
    assert_eq!(Arc::strong_count(&opts.generation), 1);
    assert_eq!(Arc::strong_count(&opts.provider_options), 1);
    let transformed = opts.with_prompt(vec![Message::new(Role::User, Vec::new())]);
    // `with_prompt` must clone these Arc handles rather than their payloads, increasing each
    // strong count from one to two; copying a payload into a new Arc would leave it at one.
    assert_eq!(Arc::strong_count(&opts.tools), 2);
    assert_eq!(Arc::strong_count(&opts.tool_choice), 2);
    assert_eq!(Arc::strong_count(&opts.response_format), 2);
    assert_eq!(Arc::strong_count(&opts.generation), 2);
    assert_eq!(Arc::strong_count(&opts.provider_options), 2);
    assert!(Arc::ptr_eq(&opts.tools, &transformed.tools));
    assert!(Arc::ptr_eq(&opts.tool_choice, &transformed.tool_choice));
    assert!(Arc::ptr_eq(
        &opts.response_format,
        &transformed.response_format
    ));
    assert!(Arc::ptr_eq(&opts.generation, &transformed.generation));
    assert!(Arc::ptr_eq(
        &opts.provider_options,
        &transformed.provider_options
    ));
    drop(transformed);

    let allocation = Arc::as_ptr(&opts);
    let mut opts = opts;
    assert_eq!(
        Arc::strong_count(&opts),
        1,
        "append requires an exclusive snapshot"
    );
    mutable_call_options(&mut opts)
        .prompt
        .push(Message::new(Role::User, Vec::new()));
    assert_eq!(
        Arc::as_ptr(&opts),
        allocation,
        "append must not allocate a cloned prompt"
    );
}

struct CountingWal {
    last_seq: u64,
    sync_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl broca_wal::Wal for CountingWal {
    fn append(
        &mut self,
        _fence: u64,
        _record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.last_seq += 1;
        if sync {
            self.sync()?;
        }
        Ok(self.last_seq)
    }

    fn sync(&self) -> Result<(), broca_wal::WalError> {
        self.sync_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(
        &self,
        _after_seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

struct RecordingWal {
    records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
}

struct HeldProbePlane {
    durable_head: Arc<std::sync::atomic::AtomicU64>,
    groups: std::sync::Mutex<Vec<Vec<crate::tool_plane::DispatchRegistration>>>,
}

#[async_trait::async_trait]
impl ToolPlane for HeldProbePlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
        panic!("durable call key was dropped")
    }
    async fn register_dispatch_group(
        &self,
        _: &BatchCtx,
        calls: &[crate::tool_plane::DispatchRegistration],
    ) -> Result<(), ToolPlaneError> {
        self.groups.lock().unwrap().push(calls.to_vec());
        Ok(())
    }
    async fn dispatch_keyed(
        &self,
        _: &BatchCtx,
        call: &ToolCall,
        key: crate::tool_plane::DispatchKey<'_>,
    ) -> DispatchResult {
        assert_eq!(self.groups.lock().unwrap().len(), 1);
        key.held_recorder
            .expect("batch supplies its writer")
            .sync_held(&crate::tool_plane::ProviderHeldReply {
                tool_call_id: call.tool_call_id.clone(),
                call_key: key.call_key.into(),
                module: "provider".into(),
                reason: "running".into(),
                detail_digest: None,
            })
            .await
            .unwrap();
        assert_eq!(
            self.durable_head.load(std::sync::atomic::Ordering::Acquire),
            1,
            "held record must be synced before acting"
        );
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "late terminal".into(),
            },
            is_error: false,
        })
    }
}

#[tokio::test]
async fn provider_held_dispatch_syncs_its_record_before_acting() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
    let ctx = BatchCtx {
        media_capture: None,
        batch_id: BatchId("batch".into()),
        bind: BindIdentity::new("/project", "broca", "session"),
        run_id: RunId("run".into()),
        scope: None,
    };
    let inner = HeldProbePlane {
        durable_head: writer.durable_head(),
        groups: Default::default(),
    };
    let plane = ProviderHeldPlane {
        inner: &inner,
        writer: &writer,
        ctx: &ctx,
        sequential: true,
        held: ProviderHeldBatch::default(),
        lifecycle: None,
        clock: None,
        control: None,
    };
    let mut dispatch = PlannedDispatch::from(ToolCall {
        tool_call_id: ToolCallId("call".into()),
        tool_name: "tool".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
        provider_state: None,
    });
    dispatch.call_key = Some("lineage:19".into());
    let plan = CallPlan::Dispatch(dispatch);
    register_dispatch_plans(&plane, &ctx, std::iter::once(&plan))
        .await
        .unwrap();
    let result = resolve(plan, &ctx, &plane, None, None).await;
    assert_eq!(
        result.result.output,
        ToolOutput::Text {
            text: "late terminal".into()
        }
    );
    let records = records.lock().unwrap().clone();
    assert_eq!(records.len(), 1);
    assert!(
        matches!(&records[0], WalRecord::ToolCallProviderHeld { batch_id, tool_call_id, call_key, module, reason, sequential: true, .. }
            if batch_id == &ctx.batch_id && tool_call_id.0 == "call" && call_key == "lineage:19" && module == "provider" && reason == "running")
    );
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_provider_held_reply_is_recorded_as_refused_without_pausing() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
    let call = ToolCall {
        tool_call_id: ToolCallId("call".into()),
        tool_name: "tool".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
        provider_state: None,
    };
    let manifest = ToolManifest::from_tools(vec![serde_json::from_value(serde_json::json!({"name":"tool","description":"","input_schema":{},"execution_mode":"parallel"})).unwrap()]);
    let plane = crate::StubToolPlane::new().with_tool("tool", |_| {
        crate::StubReply::Output(ToolOutput::error(
            "malformed provider-held reply: prerequisites not met",
        ))
    });
    let (out, _events) = mpsc::channel(64);
    let outcome = dispatch_batch(
        &RunId("run".into()),
        StepId(1),
        &BindIdentity::new("/project", "broca", "session"),
        None,
        vec![call],
        &manifest,
        &plane,
        &crate::AllowAll,
        &writer,
        None,
        &[],
        false,
        None,
        None,
        None,
        &out,
        None,
    )
    .await
    .unwrap();
    assert!(outcome.interruption.is_none());
    assert!(outcome.open.is_empty());
    let records = records.lock().unwrap().clone();
    assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { reason: Some(reason), .. } if reason == "malformed_provider_held_reply")));
    assert!(!records.iter().any(|record| matches!(
        record,
        WalRecord::RunPaused { .. } | WalRecord::ToolCallProviderHeld { .. }
    )));
    writer.shutdown().await.unwrap();
}

struct LineageRecordingWal(RecordingWal);
impl broca_wal::Wal for LineageRecordingWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.0.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        self.0.sync()
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.0.replay()
    }
    fn tail_after_seq(
        &self,
        seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.0.tail_after_seq(seq)
    }
    fn last_seq(&self) -> u64 {
        self.0.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        use sha2::Digest;
        let mut frame = [0x5a; 69];
        frame[..4].copy_from_slice(&16u32.to_le_bytes());
        frame[4] = 2;
        frame[5..21].fill(0);
        let digest = sha2::Sha256::digest([&frame[4..21], &frame[53..69]].concat());
        frame[21..53].copy_from_slice(&digest);
        let broca_wal::framing::DecodeStep::Lineage { lineage_id, .. } =
            broca_wal::framing::decode_next(&frame)
        else {
            panic!("test lineage frame")
        };
        Some(lineage_id)
    }
}

struct ParkedPlane {
    records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
    head: Arc<std::sync::atomic::AtomicU64>,
    sent: std::sync::Mutex<Vec<ToolCallId>>,
    gate: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl ToolPlane for ParkedPlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
        panic!("keyed dispatch required")
    }
    async fn dispatch_keyed(
        &self,
        _: &BatchCtx,
        call: &ToolCall,
        key: crate::tool_plane::DispatchKey<'_>,
    ) -> DispatchResult {
        self.sent.lock().unwrap().push(call.tool_call_id.clone());
        if call.tool_call_id.0 == "a" {
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            key.held_recorder
                .unwrap()
                .sync_held(&crate::tool_plane::ProviderHeldReply {
                    tool_call_id: call.tool_call_id.clone(),
                    call_key: key.call_key.into(),
                    module: "tools".into(),
                    reason: "running".into(),
                    detail_digest: None,
                })
                .await
                .unwrap();
            return std::future::pending().await;
        }
        let records = self.records.lock().unwrap();
        let terminal = records.iter().position(|record| matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Completed, .. } if result.tool_call_id.0 == "a")).expect("held terminal before successor send");
        assert!(
            self.head.load(Ordering::Acquire) > terminal as u64,
            "held terminal must be synced before successor send"
        );
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            is_error: false,
            output: ToolOutput::Text {
                text: "successor".into(),
            },
        })
    }
}

/// A lifecycle whose held call is answered at once, recording each `terminal_synced` it is
/// told about and whether that call's `ToolResult` was durable when it was told.
struct DeliveredHeld {
    records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
    head: Arc<std::sync::atomic::AtomicU64>,
    synced: std::sync::Mutex<Vec<(ToolCallId, bool)>>,
}

struct ClosingHeld {
    records: Arc<std::sync::Mutex<Vec<WalRecord>>>,
    head: Arc<std::sync::atomic::AtomicU64>,
    terminals: mpsc::UnboundedSender<ToolCallId>,
    withdrawals: mpsc::UnboundedSender<String>,
}

#[async_trait::async_trait]
impl crate::tool_plane::ProviderHeldLifecycle for ClosingHeld {
    async fn wait(
        &self,
        _: &BatchCtx,
        _: &ToolCall,
        _: &crate::tool_plane::ProviderHeldReply,
    ) -> DispatchResult {
        std::future::pending().await
    }
    async fn terminal_synced(&self, _: &BatchCtx, id: &ToolCallId) {
        let durable = self.head.load(Ordering::Acquire) as usize;
        assert!(self.records.lock().unwrap()[..durable].iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id == *id)));
        self.terminals.send(id.clone()).unwrap();
    }
    async fn withdraw_held(
        &self,
        call: &crate::tool_plane::ProviderHeldReply,
    ) -> Result<(), String> {
        let durable = self.head.load(Ordering::Acquire) as usize;
        assert!(
            self.records.lock().unwrap()[..durable]
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. })),
            "withdrawal must follow the synced seal"
        );
        self.withdrawals.send(call.call_key.clone()).unwrap();
        std::future::pending().await
    }
}

struct LiveClosingPlane {
    entered: mpsc::UnboundedSender<ToolCallId>,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ToolPlane for LiveClosingPlane {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
        panic!("key required")
    }
    async fn dispatch_keyed(
        &self,
        _: &BatchCtx,
        call: &ToolCall,
        key: crate::tool_plane::DispatchKey<'_>,
    ) -> DispatchResult {
        if call.tool_call_id.0 != "b" {
            key.held_recorder
                .unwrap()
                .sync_held(&crate::tool_plane::ProviderHeldReply {
                    tool_call_id: call.tool_call_id.clone(),
                    call_key: key.call_key.into(),
                    module: "tools".into(),
                    reason: "running".into(),
                    detail_digest: None,
                })
                .await
                .unwrap();
            self.entered.send(call.tool_call_id.clone()).unwrap();
            std::future::pending().await
        } else {
            self.entered.send(call.tool_call_id.clone()).unwrap();
            self.release.notified().await;
            DispatchResult::Completed(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                is_error: false,
                output: ToolOutput::Text {
                    text: "sibling finished".into(),
                },
            })
        }
    }
}

#[tokio::test]
async fn provider_held_live_cancel_closes_before_sibling_and_withdraws_after_seal() {
    tokio::time::timeout(Duration::from_secs(5), async {
            {
                let records = Arc::new(std::sync::Mutex::new(crate::replay::deferred_tests::batch(&["a", "b", "c"])));
                let writer = WalHandle::spawn(Box::new(LineageRecordingWal(RecordingWal { records: records.clone() })), 1).unwrap();
                writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
                let (entered_tx, mut entered) = mpsc::unbounded_channel();
                let (terminal_tx, mut terminals) = mpsc::unbounded_channel();
                let (withdrawal_tx, mut withdrawals) = mpsc::unbounded_channel();
                let lifecycle = Arc::new(ClosingHeld { records: records.clone(), head: writer.durable_head(), terminals: terminal_tx, withdrawals: withdrawal_tx });
                let token = CancellationToken::new();
                let control = RunControl { cancel: Some(token.clone()), provider_held_lifecycle: Some(lifecycle), ..Default::default() };
                let plane = LiveClosingPlane { entered: entered_tx, release: tokio::sync::Notify::new() };
                let calls = match &records.lock().unwrap()[1] { WalRecord::ModelStepFinished { assistant_message, .. } => assistant_message.tool_calls().cloned().collect(), _ => unreachable!() };
                let manifest = ToolManifest::from_tools(vec![Tool { name: "edit".into(), description: "edit".into(), input_schema: serde_json::json!({"type":"object"}), module: Some("tools".into()), execution_mode: ExecutionMode::Parallel, reply: None }]);
                let (out, _events) = mpsc::channel(32);
                let bind = BindIdentity::new("/project", "broca", "session");
                let run = RunId("r".into());
                let mut batch = Box::pin(dispatch_batch(&run, StepId(1), &bind, None, calls, &manifest, &plane, &crate::AllowAll, &writer,
                    Some(ResumeCtx { batch_already_started: true, ..Default::default() }), &[], false, control.stop_requested.as_ref(), None, None, &out, Some(&control)));
                for _ in 0..3 {
                    tokio::select! { _ = entered.recv() => {}, _ = &mut batch => panic!("batch ended before sibling") }
                }
                writer.record(WalRecord::RunCancelRequested { run_id: run.clone(), at: broca_wal::record::RunCancelAt::Now });
                writer.sync().await.unwrap();
                token.cancel();
                for _ in 0..2 {
                    tokio::select! { id = terminals.recv() => assert_ne!(id.unwrap().0, "b"), _ = &mut batch => panic!("seal must wait for sibling") }
                }
                assert!(!records.lock().unwrap().iter().any(|record| matches!(record, WalRecord::RunFinished { .. })));
                assert!(withdrawals.try_recv().is_err());
                plane.release.notify_one();
                let batch = batch.await.unwrap();
                assert_eq!(batch.interruption, Some(BatchInterruption::CallerCancel));
                finish_run(&writer, &control, &out, &run, RunFinishReason::Cancelled, 1, Usage::default(), 0, None, None).await.unwrap();
                let first = withdrawals.recv().await.unwrap();
                let second = withdrawals.recv().await.unwrap();
                assert_ne!(first, second);
                assert!(withdrawals.try_recv().is_err(), "one withdrawal per closed held call");
                for id in ["a", "b", "c"] {
                    let raw = records.lock().unwrap();
                    let results: Vec<_> = raw.iter().filter_map(|record| match record { WalRecord::ToolResult { result, outcome, reason, .. } if result.tool_call_id.0 == id => Some((outcome, reason)), _ => None }).collect();
                    assert_eq!(results.len(), 1);
                    assert_eq!(*results[0].0, if id == "b" { DispatchOutcome::Completed } else { DispatchOutcome::Interrupted });
                }
                writer.shutdown().await.unwrap();
            }
        }).await.expect("held close-out never waits for hanging withdrawals");
}
#[async_trait::async_trait]
impl crate::tool_plane::ProviderHeldLifecycle for DeliveredHeld {
    async fn wait(
        &self,
        _: &BatchCtx,
        call: &ToolCall,
        _: &crate::tool_plane::ProviderHeldReply,
    ) -> DispatchResult {
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            is_error: false,
            output: ToolOutput::Text {
                text: "answer".into(),
            },
        })
    }

    async fn terminal_synced(&self, _: &BatchCtx, tool_call_id: &ToolCallId) {
        let durable = self.head.load(Ordering::Acquire) as usize;
        let records = self.records.lock().unwrap();
        let terminal_durable = records[..durable.min(records.len())]
                .iter()
                .any(|record| matches!(record, WalRecord::ToolResult { result, .. } if &result.tool_call_id == tool_call_id));
        self.synced
            .lock()
            .unwrap()
            .push((tool_call_id.clone(), terminal_durable));
    }
}

#[tokio::test]
async fn provider_held_sequential_pause_resume_syncs_before_successor() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut prefix = crate::replay::deferred_tests::batch(&["a", "b"]);
        if let WalRecord::ToolBatchStarted { batch_id, .. } = &mut prefix[2] {
            *batch_id = BatchId("r-b1".into());
        }
        let records = Arc::new(std::sync::Mutex::new(prefix));
        let writer = WalHandle::spawn(
            Box::new(LineageRecordingWal(RecordingWal {
                records: records.clone(),
            })),
            1,
        )
        .unwrap();
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let plane = ParkedPlane {
            records: records.clone(),
            head: writer.durable_head(),
            sent: Default::default(),
            gate: None,
        };
        let calls: Vec<_> = match &records.lock().unwrap()[1] {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => assistant_message.tool_calls().cloned().collect(),
            _ => unreachable!(),
        };
        let manifest = ToolManifest::from_tools(vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: serde_json::json!({"type":"object"}),
            module: Some("tools".into()),
            execution_mode: ExecutionMode::Sequential,
            reply: None,
        }]);
        let (out, _rx) = mpsc::channel(32);
        let control = RunControl::default();
        let batch = dispatch_batch(
            &RunId("r".into()),
            StepId(1),
            &BindIdentity::new("/project", "broca", "session"),
            None,
            calls.clone(),
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            Some(ResumeCtx {
                batch_already_started: true,
                ..Default::default()
            }),
            &[],
            false,
            None,
            None,
            None,
            &out,
            Some(&control),
        )
        .await
        .unwrap();
        assert_eq!(
            batch.interruption,
            Some(BatchInterruption::ProviderHeldPause)
        );
        assert_eq!(*plane.sent.lock().unwrap(), [ToolCallId("a".into())]);
        let summary = pause_approval(
            &writer,
            &out,
            &control,
            &BindIdentity::new("/project", "broca", "session"),
            &RunId("r".into()),
            1,
            Usage::default(),
            0,
            &[],
            true,
        )
        .await
        .unwrap();
        assert_eq!(summary.paused, Some(PauseReason::ToolHeld));
        let state = crate::replay(&records.lock().unwrap()).unwrap();
        let dispatch_times: Vec<_> = records
            .lock()
            .unwrap()
            .iter()
            .filter_map(|record| match record {
                WalRecord::ToolDispatchIntent {
                    tool_call_id,
                    dispatched_at_ms,
                    ..
                } => Some((tool_call_id.clone(), *dispatched_at_ms)),
                _ => None,
            })
            .collect();
        assert_eq!(dispatch_times.len(), 2);
        assert!(
            dispatch_times.iter().all(|(_, time)| time.is_some()),
            "the writer stamps both barrier-1 observations"
        );
        let ReplayResolution::ProviderHeld { pending, .. } = state.resolution else {
            panic!("not provider-held")
        };
        assert_eq!(pending.held_successors, [ToolCallId("b".into())]);
        let frames: Vec<_> = records
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, record)| broca_wal::FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect();
        let intents = held_resume_intents(&pending, &frames, &writer).unwrap();
        let resumed = ResumeCtx {
            batch_already_started: true,
            intents,
            provider_held: pending.provider_held,
            ..Default::default()
        };
        let lifecycle = Arc::new(DeliveredHeld {
            records: records.clone(),
            head: writer.durable_head(),
            synced: Default::default(),
        });
        let control = RunControl {
            provider_held_lifecycle: Some(lifecycle.clone()),
            ..Default::default()
        };
        let outcome = dispatch_batch(
            &RunId("r".into()),
            StepId(1),
            &BindIdentity::new("/project", "broca", "session"),
            None,
            calls,
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            Some(resumed),
            &[],
            false,
            None,
            None,
            None,
            &out,
            Some(&control),
        )
        .await
        .unwrap();
        assert_eq!(outcome.interruption, None);
        assert_eq!(outcome.results.len(), 2);
        assert_eq!(
            *plane.sent.lock().unwrap(),
            [ToolCallId("a".into()), ToolCallId("b".into())]
        );
        // The late result counts as delivered only once its terminal is durable, and only
        // the held call is reported: the sequential successor was an ordinary dispatch.
        assert_eq!(
            *lifecycle.synced.lock().unwrap(),
            [(ToolCallId("a".into()), true)],
            "the held terminal must be synced before its delivery is answered"
        );
        assert_eq!(
            records
                .lock()
                .unwrap()
                .iter()
                .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
                .count(),
            2
        );
        assert_eq!(
            records
                .lock()
                .unwrap()
                .iter()
                .filter(|record| matches!(record, WalRecord::ToolResult { .. }))
                .count(),
            2
        );
        assert_eq!(
            records
                .lock()
                .unwrap()
                .iter()
                .filter_map(|record| match record {
                    WalRecord::ToolDispatchIntent {
                        tool_call_id,
                        dispatched_at_ms,
                        ..
                    } => Some((tool_call_id.clone(), *dispatched_at_ms)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            dispatch_times,
            "resuming does not re-observe either original dispatch intent",
        );
        assert!(
            records
                .lock()
                .unwrap()
                .iter()
                .filter_map(|record| match record {
                    WalRecord::ToolResult { finished_at_ms, .. } => Some(finished_at_ms),
                    _ => None,
                })
                .all(Option::is_some),
            "held delivery and successor completion use the writer's tool-time observations"
        );
        writer.shutdown().await.unwrap();
    })
    .await
    .expect("held batch must pause or deliver without sending through its hold");
}

#[tokio::test]
async fn provider_held_cancel_racing_pause_closes_the_full_sequential_batch() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut prefix = crate::replay::deferred_tests::batch(&["a", "b"]);
        if let WalRecord::ToolBatchStarted { batch_id, .. } = &mut prefix[2] {
            *batch_id = BatchId("r-b1".into());
        }
        let records = Arc::new(std::sync::Mutex::new(prefix));
        let writer = WalHandle::spawn(
            Box::new(LineageRecordingWal(RecordingWal {
                records: records.clone(),
            })),
            1,
        )
        .unwrap();
        writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
        let plane = ParkedPlane {
            records: records.clone(),
            head: writer.durable_head(),
            sent: Default::default(),
            gate: None,
        };
        let calls = match &records.lock().unwrap()[1] {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => assistant_message.tool_calls().cloned().collect(),
            _ => unreachable!(),
        };
        let manifest = ToolManifest::from_tools(vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: serde_json::json!({"type":"object"}),
            module: Some("tools".into()),
            execution_mode: ExecutionMode::Sequential,
            reply: None,
        }]);
        let (out, _events) = mpsc::channel(32);
        let cancel = CancellationToken::new();
        let control = RunControl {
            cancel: Some(cancel.clone()),
            ..Default::default()
        };
        let bind = BindIdentity::new("/project", "broca", "session");
        let run = RunId("r".into());
        let batch = dispatch_batch(
            &run,
            StepId(1),
            &bind,
            None,
            calls,
            &manifest,
            &plane,
            &crate::AllowAll,
            &writer,
            Some(ResumeCtx {
                batch_already_started: true,
                ..Default::default()
            }),
            &[],
            false,
            None,
            None,
            None,
            &out,
            Some(&control),
        )
        .await
        .unwrap();
        assert_eq!(
            batch.interruption,
            Some(BatchInterruption::ProviderHeldPause)
        );
        cancel.cancel();
        let summary = pause_approval(
            &writer,
            &out,
            &control,
            &bind,
            &run,
            1,
            Usage::default(),
            0,
            &[],
            true,
        )
        .await
        .unwrap();
        assert_eq!(summary.reason, RunFinishReason::Cancelled);
        assert_eq!(*plane.sent.lock().unwrap(), [ToolCallId("a".into())]);
        let raw = records.lock().unwrap().clone();
        assert!(!raw
            .iter()
            .any(|record| matches!(record, WalRecord::RunPaused { .. })));
        for (id, expected) in [
            (
                "a",
                "provider_held_cancelled: the run was cancelled while the provider held the call",
            ),
            ("b", crate::resume::NOT_DISPATCHED_TEXT),
        ] {
            let terminals: Vec<_> = raw
                .iter()
                .filter_map(|record| match record {
                    WalRecord::ToolResult {
                        result, outcome, ..
                    } if result.tool_call_id.0 == id => Some((result, outcome)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                terminals.len(),
                1,
                "close-out must include the unsent successor"
            );
            assert_eq!(*terminals[0].1, DispatchOutcome::Interrupted);
            assert_eq!(terminals[0].0.output, ToolOutput::error(expected));
        }
        let prior = crate::replay(&raw).unwrap();
        assert!(prior.open_tool_batch.is_none());
        assert!(matches!(
            prior.resolution,
            ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Cancelled
            }
        ));
        writer.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn provider_held_live_shutdown_preserves_obligation_until_restart_resume() {
    tokio::time::timeout(Duration::from_secs(5), async {
            let mut prefix = crate::replay::deferred_tests::batch(&["a", "b"]);
            if let WalRecord::ToolBatchStarted { batch_id, .. } = &mut prefix[2] { *batch_id = BatchId("r-b1".into()); }
            let records = Arc::new(std::sync::Mutex::new(prefix));
            let writer = WalHandle::spawn(Box::new(LineageRecordingWal(RecordingWal { records: records.clone() })), 1).unwrap();
            writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
            let calls: Vec<_> = match &records.lock().unwrap()[1] { WalRecord::ModelStepFinished { assistant_message, .. } => assistant_message.tool_calls().cloned().collect(), _ => unreachable!() };
            let (entered_tx, mut entered) = mpsc::unbounded_channel();
            let plane = LiveClosingPlane { entered: entered_tx, release: tokio::sync::Notify::new() };
            let manifest = ToolManifest::from_tools(vec![Tool { name: "edit".into(), description: "edit".into(), input_schema: serde_json::json!({"type":"object"}), module: Some("tools".into()), execution_mode: ExecutionMode::Parallel, reply: None }]);
            let stop = CancellationToken::new();
            let control = RunControl { stop_requested: Some(stop.clone()), ..Default::default() };
            let (out, _events) = mpsc::channel(32);
            let run = RunId("r".into());
            let bind = BindIdentity::new("/project", "broca", "session");
            let mut batch = Box::pin(dispatch_batch(&run, StepId(1), &bind, None, calls.clone(), &manifest, &plane, &crate::AllowAll, &writer,
                Some(ResumeCtx { batch_already_started: true, ..Default::default() }), &[], false, Some(&stop), None, None, &out, Some(&control)));
            for _ in 0..2 {
                tokio::select! { id = entered.recv() => { id.unwrap(); }, _ = &mut batch => panic!("sibling still running") }
            }
            stop.cancel();
            plane.release.notify_one();
            let batch = batch.await.unwrap();
            assert_eq!(batch.interruption, Some(BatchInterruption::ProviderHeldPause), "shutdown preserves a provider's open obligation");
            let summary = pause_approval(&writer, &out, &control, &bind, &run, 1, Usage::default(), 0, &[], true).await.unwrap();
            assert_eq!(summary.paused, Some(PauseReason::ToolHeld));
            writer.shutdown().await.unwrap();
            let raw = records.lock().unwrap().clone();
            assert!(!raw.iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "a")), "shutdown must not close a held call");
            assert!(!raw.iter().any(|record| matches!(record, WalRecord::RunFinished { .. })));
            let ReplayResolution::ProviderHeld { pending, .. } = crate::replay(&raw).unwrap().resolution else { panic!("hold must survive restart"); };
            let writer = WalHandle::spawn(Box::new(LineageRecordingWal(RecordingWal { records: records.clone() })), 1).unwrap();
            writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
            let frames: Vec<_> = raw.into_iter().enumerate().map(|(index, record)| broca_wal::FramedRecord { seq: index as u64 + 1, fence: 1, record }).collect();
            let intents = held_resume_intents(&pending, &frames, &writer).unwrap();
            let resumed = ResumeCtx {
                batch_already_started: true, intents, provider_held: pending.provider_held,
                existing: pending.existing.into_iter().map(|(id, result)| (id, std::collections::VecDeque::from([result]))).collect(),
                ..Default::default()
            };
            let lifecycle = Arc::new(DeliveredHeld { records: records.clone(), head: writer.durable_head(), synced: Default::default() });
            let control = RunControl { provider_held_lifecycle: Some(lifecycle), ..Default::default() };
            let batch = dispatch_batch(&run, StepId(1), &bind, None, calls, &manifest, &plane, &crate::AllowAll, &writer,
                Some(resumed), &[], false, None, None, None, &out, Some(&control)).await.unwrap();
            assert_eq!(batch.interruption, None);
            assert!(entered.try_recv().is_err(), "restart must not re-send either call");
            let raw = records.lock().unwrap().clone();
            assert_eq!(raw.iter().filter(|record| matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Completed, .. } if result.tool_call_id.0 == "a")).count(), 1);
            assert!(crate::replay(&raw).unwrap().open_tool_batch.is_none());
            writer.shutdown().await.unwrap();
        }).await.expect("shutdown must pause a held call without waiting for its answer");
}

struct WarmProbe {
    active: Arc<AtomicBool>,
    started: tokio::sync::Notify,
    calls: AtomicU64,
}
#[async_trait::async_trait]
impl crate::keep_warm_wait::KeepWarmDriver for WarmProbe {
    fn timing(&self, _: &RunConfig) -> Result<crate::keep_warm_wait::WarmTiming, String> {
        Ok(crate::keep_warm_wait::WarmTiming {
            ttl_ms: 1000,
            lead_ms: 1,
            after_send_ms: Some(1),
        })
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        Some(0)
    }
    fn costs(&self, _: &RunConfig) -> Result<crate::keep_warm::WarmCosts, String> {
        Ok(crate::keep_warm::WarmCosts {
            warm: 1.0,
            bust: 100.0,
        })
    }
    async fn warm(&self, _: &RunConfig, _: &RunId) -> crate::keep_warm_wait::WarmTick {
        struct ActiveWarm(Arc<AtomicBool>);
        impl Drop for ActiveWarm {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        self.active.store(true, Ordering::Release);
        let _active = ActiveWarm(self.active.clone());
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn provider_held_pause_stops_keep_warm_for_a_day() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(LineageRecordingWal(RecordingWal {
            records: records.clone(),
        })),
        1,
    )
    .unwrap();
    writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
    let gate = Arc::new(tokio::sync::Notify::new());
    let plane = ParkedPlane {
        records,
        head: writer.durable_head(),
        sent: Default::default(),
        gate: Some(gate.clone()),
    };
    let probe = Arc::new(WarmProbe {
        active: Arc::new(AtomicBool::new(false)),
        started: tokio::sync::Notify::new(),
        calls: AtomicU64::new(0),
    });
    let driver: Arc<dyn crate::keep_warm_wait::KeepWarmDriver> = probe.clone();
    let config = match crate::replay::deferred_tests::batch(&["a"]).remove(0) {
        WalRecord::RunStarted { config, .. } => config,
        _ => unreachable!(),
    };
    let run = RunId("r".into());
    let unsupported = AtomicBool::new(false);
    let missed = AtomicBool::new(false);
    let warm = crate::keep_warm_wait::WarmWait {
        driver: &driver,
        config: &config,
        run_id: &run,
        cancel: None,
        unsupported_logged: &unsupported,
        warm_missed: &missed,
        keep_warm: &broca_protocol::request::KeepWarm::Auto,
        warms_before: 0,
    };
    let manifest = ToolManifest::from_tools(vec![Tool {
        name: "edit".into(),
        description: "edit".into(),
        input_schema: serde_json::json!({"type":"object"}),
        module: Some("tools".into()),
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }]);
    let (out, _rx) = mpsc::channel(16);
    let bind = BindIdentity::new("/project", "broca", "warm-held");
    let call = ToolCall {
        tool_call_id: ToolCallId("a".into()),
        tool_name: "edit".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
        provider_state: None,
    };
    let batch = dispatch_batch(
        &run,
        StepId(1),
        &bind,
        None,
        vec![call],
        &manifest,
        &plane,
        &crate::AllowAll,
        &writer,
        None,
        &[],
        false,
        None,
        None,
        Some(warm),
        &out,
        None,
    );
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(batch, async {
            probe.started.notified().await;
            gate.notify_one();
        })
    })
    .await
    .expect("provider-held batch must pause rather than warm indefinitely");
    assert_eq!(
        result.unwrap().interruption,
        Some(BatchInterruption::ProviderHeldPause)
    );
    assert!(
        !probe.active.load(Ordering::Acquire),
        "pausing drops in-flight keep-warm IO"
    );
    let count = probe.calls.load(Ordering::Acquire);
    assert_eq!(count, 1, "live tool waits still warm");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
    assert_eq!(
        probe.calls.load(Ordering::Acquire),
        count,
        "paused runs never warm"
    );
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_core_writer_freezes_agent_run_scope() {
    let scope: broca_protocol::SessionScope = serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#).unwrap();
    let submission = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "written"}]}],
        "stop_when": [{"type": "step_count", "n": 1}]
    }))
    .unwrap();
    let mut req = RunRequest::from_submission(
        submission,
        RunId("next".into()),
        BindIdentity::new("/agent-run-writer", "runner", "basal:codemode-parent-run:0"),
        broca_protocol::SubmissionId("next".into()),
    );
    req.scope = Some(scope.clone());
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .unwrap();
    let (out, _events) = mpsc::channel(64);
    let (ack, admitted) = tokio::sync::oneshot::channel();
    let result = run_loop_on_writer_admitted_with_prior_snapshot(
        req,
        &writer,
        StartMode::FreshOrResume,
        &crate::StubProvider::new(vec![Vec::new()]),
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        &ReadableWalStore,
        None,
        RunControl::default(),
        &out,
        Some(ack),
        None,
    )
    .await;
    assert!(result.is_ok(), "{result:?}");
    assert!(admitted.await.unwrap().is_ok());
    writer.sync().await.unwrap();
    assert!(records.lock().unwrap().iter().any(|record| matches!(
        record, WalRecord::RunStarted { scope: Some(frozen), .. } if frozen == &scope
    )));
    let _ = writer.shutdown().await;
}

impl broca_wal::Wal for RecordingWal {
    fn append(
        &mut self,
        _fence: u64,
        record: &WalRecord,
        _sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        let mut records = self.records.lock().unwrap();
        records.push(record.clone());
        Ok(records.len() as u64)
    }

    fn sync(&self) -> Result<(), broca_wal::WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(
        &self,
        _after_seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.records.lock().unwrap().len() as u64
    }
}

struct ReadableWalStore;

impl WalStore for ReadableWalStore {
    fn open(
        &self,
        _session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        unreachable!("projection only reads the WAL")
    }

    fn read_records(
        &self,
        _session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(Vec::new())
    }
}

struct BoundedReadableWalStore;

impl WalStore for BoundedReadableWalStore {
    fn open(
        &self,
        _session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        unreachable!("projection only reads the WAL")
    }

    fn read_records(
        &self,
        _session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok((1..=64)
            .map(|seq| broca_wal::FramedRecord {
                seq,
                fence: 1,
                record: WalRecord::TurnFinished {
                    step_id: StepId(seq),
                },
            })
            .collect())
    }
}

struct AnchoredReadableWalStore {
    observed_anchor: std::sync::Mutex<Option<broca_wal::WalByteAnchor>>,
}

impl WalStore for AnchoredReadableWalStore {
    fn open(
        &self,
        _session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        unreachable!("projection only reads the WAL")
    }

    fn read_records(
        &self,
        _session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        unreachable!("the test exercises the range implementation")
    }

    fn read_records_range(
        &self,
        _session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        assert_eq!(after_seq, 1);
        assert_eq!(through_seq, Some(3));
        *self.observed_anchor.lock().unwrap() = anchor.copied();
        let records = (2..=3)
            .map(|seq| broca_wal::FramedRecord {
                seq,
                fence: 1,
                record: WalRecord::TurnFinished {
                    step_id: StepId(seq),
                },
            })
            .collect();
        let anchors = (2..=3)
            .map(|seq| broca_wal::WalByteAnchor {
                file_id: 11,
                seq,
                byte_end: seq * 100,
            })
            .collect();
        Ok(broca_wal::WalRangeRead::with_anchors(records, anchors))
    }
}

#[derive(Default)]
struct CapturingProjectionStore {
    seed: Option<crate::ProjectionSeed>,
    seed_watermark: u64,
    seed_anchor: Option<broca_wal::WalByteAnchor>,
    projected_watermark: Option<u64>,
    projected_seqs: std::sync::Mutex<Vec<u64>>,
    persisted_anchor: std::sync::Mutex<Option<broca_wal::WalByteAnchor>>,
    projection_calls: std::sync::atomic::AtomicUsize,
    retention_metadata: std::sync::Mutex<Vec<crate::projection_plan::RetainedWithdrawalProjection>>,
}

impl crate::MaterializedStore for CapturingProjectionStore {
    fn read_prefix_with_watermark(
        &self,
        _session: &BindIdentity,
    ) -> Result<crate::StorePrefix, String> {
        Ok(crate::StorePrefix::empty())
    }
}

impl crate::Persister for CapturingProjectionStore {
    fn project_retention_metadata(
        &self,
        _session: &BindIdentity,
        metadata: &crate::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), String> {
        self.retention_metadata
            .lock()
            .unwrap()
            .push(metadata.clone());
        Ok(())
    }
    fn projection_seed(
        &self,
        _session: &BindIdentity,
    ) -> Result<Option<crate::ProjectionSeed>, String> {
        Ok(self.seed.clone().or_else(|| {
            Some(crate::ProjectionSeed {
                watermark_seq: self.seed_watermark,
                anchor: self.seed_anchor,
                ..crate::ProjectionSeed::empty()
            })
        }))
    }

    fn project(
        &self,
        _session: &BindIdentity,
        records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        self.projection_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.projected_seqs.lock().unwrap() = records.iter().map(|record| record.seq).collect();
        Ok(self
            .projected_watermark
            .unwrap_or_else(|| records.last().map(|record| record.seq).unwrap_or(0)))
    }

    fn persist_wal_anchor(
        &self,
        _session: &BindIdentity,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<(), String> {
        *self.persisted_anchor.lock().unwrap() = anchor.copied();
        Ok(())
    }
}

#[derive(Default)]
struct BlockingProjectionState {
    started: bool,
    released: bool,
    calls: usize,
    projected_seqs: Vec<u64>,
}

#[derive(Default)]
struct BlockingProjectionStore {
    state: std::sync::Mutex<BlockingProjectionState>,
    changed: std::sync::Condvar,
}

impl BlockingProjectionStore {
    fn wait_until_started(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, std::time::Duration::from_secs(2), |state| {
                !state.started
            })
            .unwrap();
        assert!(!timeout.timed_out(), "projection write did not start");
        assert!(state.started);
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

impl crate::MaterializedStore for BlockingProjectionStore {
    fn read_prefix_with_watermark(
        &self,
        _session: &BindIdentity,
    ) -> Result<crate::StorePrefix, String> {
        Ok(crate::StorePrefix::empty())
    }
}

impl crate::Persister for BlockingProjectionStore {
    fn project(
        &self,
        _session: &BindIdentity,
        records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
        state.calls += 1;
        state.projected_seqs = records.iter().map(|record| record.seq).collect();
        Ok(records.last().map_or(0, |record| record.seq))
    }
}

/// Serializes the tests that read the process-wide projection warning counter. libtest runs
/// tests on parallel threads in one process, so one test's failing projection moves the count
/// another test is asserting on: an exact count then fails, or a reset lands between another
/// test's before and after reads. Any test that makes a projection fail, or reads the
/// counter, holds this for its whole body.
static PROJECTION_COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct FailingProjectionStore;

impl crate::MaterializedStore for FailingProjectionStore {
    fn read_prefix_with_watermark(
        &self,
        _session: &BindIdentity,
    ) -> Result<crate::StorePrefix, String> {
        Ok(crate::StorePrefix::empty())
    }
}

impl crate::Persister for FailingProjectionStore {
    fn project(
        &self,
        _session: &BindIdentity,
        _records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        Err("injected projection failure".into())
    }
}

#[tokio::test]
async fn tripped_sequential_resume_preserves_already_durable_terminal() {
    let batch_id = BatchId("resume-trip-b1".into());
    let prior_result = ToolResult {
        tool_call_id: ToolCallId("call_b".into()),
        output: ToolOutput::Json {
            value: serde_json::json!({
                "status": "committed before crash",
                "nested": {"attempt": 7}
            })
            .into(),
        },
        is_error: false,
    };
    let prior_record = WalRecord::ToolResult {
        finished_at_ms: None,
        tags: None,
        hooks: None,
        batch_id: batch_id.clone(),
        result: prior_result.clone().into(),
        outcome: DispatchOutcome::Completed,
        reason: None,
        withdraw_elicitation: None,
        provider_held: None,
    };
    let prior_bytes = serde_json::to_vec(&prior_record).expect("serialize prior terminal");
    let records = Arc::new(std::sync::Mutex::new(vec![prior_record]));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .expect("spawn writer");
    // This test calls `dispatch_batch` directly, skipping `reconstruct_prior`, the admission
    // step that seeds the writer's tag state from the WAL. Seed it as that step would: the
    // seeded history's only record is an untagged ToolResult, which is a lineage admitted
    // before tags existed, so its state is `Off` and the new records stay untagged.
    writer.seed_lineage_tags(Some(crate::tags::LineageTags::Off));
    let calls = vec![
        ToolCall {
            thought_signature: None,
            provider_state: None,
            tool_call_id: ToolCallId("call_a".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({"path": "a"}).into(),
        },
        ToolCall {
            thought_signature: None,
            provider_state: None,
            tool_call_id: ToolCallId("call_b".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({"path": "b"}).into(),
        },
    ];
    let manifest = ToolManifest::from_tools(vec![Tool {
        name: "edit".into(),
        description: "edit a file".into(),
        input_schema: serde_json::json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }]);
    let plane = crate::StubToolPlane::new().with_tool("edit", |call| {
        if call.tool_call_id.0 == "call_a" {
            crate::StubReply::OutcomeUnknown("reset after send".into())
        } else {
            crate::StubReply::Output(ToolOutput::Text {
                text: "SHOULD-NOT-RUN".into(),
            })
        }
    });
    let resume = ResumeCtx {
        batch_already_started: true,
        prepared: Vec::new(),
        provider_held: Vec::new(),
        intents: std::collections::HashMap::new(),
        existing: [(
            ToolCallId("call_b".into()),
            std::collections::VecDeque::from([prior_result.clone()]),
        )]
        .into_iter()
        .collect(),
    };
    let (out, _events) = mpsc::channel(8);

    let outcome = dispatch_batch(
        &RunId("resume-trip".into()),
        StepId(1),
        &BindIdentity::new("/tmp/project", "test", "resume-trip"),
        None,
        calls,
        &manifest,
        &plane,
        &crate::AllowAll,
        &writer,
        Some(resume),
        &[],
        false,
        None,
        None,
        None,
        &out,
        None,
    )
    .await
    .expect("resume batch resolves");

    assert_eq!(outcome.interruption, Some(BatchInterruption::Ambiguous));
    assert_eq!(outcome.results.len(), 2);
    assert_eq!(
        serde_json::to_vec(&outcome.results[1]).expect("serialize resumed result"),
        serde_json::to_vec(&prior_result).expect("serialize prior result"),
        "the resumed result remains byte-identical"
    );
    assert_eq!(plane.exec_count(&ToolCallId("call_b".into())), 0);

    {
        let records = records.lock().unwrap();
        let b_terminals: Vec<&WalRecord> = records
            .iter()
            .filter(|record| match record {
                WalRecord::ToolNoDispatch { tool_call_id, .. } => tool_call_id.0 == "call_b",
                WalRecord::ToolResult { result, .. } => result.tool_call_id.0 == "call_b",
                _ => false,
            })
            .collect();
        assert_eq!(
            b_terminals.len(),
            1,
            "an existing durable terminal must not be rewritten"
        );
        assert_eq!(
            serde_json::to_vec(b_terminals[0]).expect("serialize retained terminal"),
            prior_bytes,
            "the durable terminal record remains byte-identical"
        );
    }
    let _ = writer.shutdown().await;
}

#[tokio::test]
async fn cancellation_wins_before_pause_becomes_durable() {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = WalHandle::spawn(
        Box::new(RecordingWal {
            records: records.clone(),
        }),
        1,
    )
    .expect("spawn writer");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let control = RunControl {
        cancel: Some(cancel),
        ..Default::default()
    };
    let (out, mut events) = mpsc::channel(8);

    let summary = pause_run(
        &writer,
        &out,
        &control,
        RunId("paused-race".into()),
        broca_protocol::PauseReason::AuthRequired,
        None,
        2,
        Usage::default(),
        1,
    )
    .await
    .unwrap();

    assert_eq!(summary.reason, RunFinishReason::Cancelled);
    assert_eq!(summary.paused, None);
    assert!(matches!(
        events.recv().await,
        Some(OutputEvent::Control(ControlUnit::RunFinished { .. }))
    ));
    assert!(matches!(
        records.lock().unwrap().as_slice(),
        [WalRecord::RunFinished {
            reason: RunFinishReason::Cancelled,
            ..
        }]
    ));
    let _ = writer.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_projection_uses_exact_fsync_head_despite_later_append() {
    let sync_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let writer = Arc::new(
        WalHandle::spawn(
            Box::new(CountingWal {
                last_seq: 0,
                sync_calls: sync_calls.clone(),
            }),
            1,
        )
        .expect("spawn writer"),
    );
    let (projector, pending) = projection_channel();
    let (out, mut events) = mpsc::channel(1);
    out.send(OutputEvent::Control(ControlUnit::RunFinished {
        awaits: None,
        run_id: Some(RunId("run-probe".into())),
        reason: RunFinishReason::Completed,
        usage: None,
        final_step_finish_reason: None,
        indeterminate_tool_calls: None,
        retries_used: 0,
    }))
    .await
    .unwrap();

    let task_writer = writer.clone();
    let task = tokio::spawn(async move {
        let result = finish_run(
            task_writer.as_ref(),
            &RunControl::default(),
            &out,
            &RunId("run-probe".into()),
            RunFinishReason::Completed,
            0,
            Usage::default(),
            0,
            None,
            None,
        )
        .await;
        (result, projector)
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while sync_calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal barrier fsyncs");

    let later_seq = writer
        .record(WalRecord::ApprovalRequested {
            tool_call_id: ToolCallId("racing-actor-record".into()),
        })
        .into_result()
        .unwrap();
    assert_eq!(later_seq, 2);
    events.recv().await.expect("remove prefilled event");
    events.recv().await.expect("receive terminal event");
    let (result, projector) = task.await.expect("finish task joins");
    let (_summary, last_fsynced_seq) = result.expect("finish succeeds");
    projector.enqueue_terminal(last_fsynced_seq);

    let request = *pending.borrow();
    assert_eq!(
        request.safe_h, 1,
        "projection must stop at the fsynced head"
    );
    assert_eq!(writer.next_record_seq(), 3);
    projector.close();
    drop(projector);
    drop(pending);
    let _ = Arc::try_unwrap(writer)
        .ok()
        .expect("finish task released writer")
        .shutdown()
        .await;
}

#[tokio::test]
async fn budgeted_terminal_projection_uses_a_valid_tail_seed() {
    struct BudgetTailWalStore {
        observed_after: std::sync::Mutex<Vec<u64>>,
    }
    impl WalStore for BudgetTailWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unreachable!("projection only reads the WAL")
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            unreachable!("valid budget seed must avoid full reads")
        }

        fn read_records_range(
            &self,
            _session: &BindIdentity,
            after_seq: u64,
            through_seq: Option<u64>,
            _anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            assert_eq!(through_seq, Some(10));
            self.observed_after.lock().unwrap().push(after_seq);
            Ok(broca_wal::WalRangeRead::with_anchors(
                (after_seq + 1..=10)
                    .map(|seq| broca_wal::FramedRecord {
                        seq,
                        fence: 1,
                        record: WalRecord::TurnFinished {
                            step_id: StepId(seq),
                        },
                    })
                    .collect(),
                Vec::new(),
            ))
        }
    }

    let config = RunConfig {
        global_system: None,
        account_route: None,
        route_decision_id: None,
        served_provider: None,
        account_limit_pause: false,
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: ModelHandle::new("stub", "m"),
        tools: Vec::new().into(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: Some(BudgetConfig {
            max_input_tokens: None,
            nudges: Vec::new(),
            finalize_at_tool_calls: 99,
            finalize_text: "finalize".into(),
        }),
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    };
    let seed = crate::ProjectionSeed {
        refusal: Default::default(),
        open_episode_span: Some(1),
        open_episode: None,
        earlier_episodes: Vec::new(),
        projection_episodes: None,
        deferred_calls: None,
        watermark_seq: 8,
        anchor: None,
        prefix_len: 2,
        completed_steps: 1,
        usage: Usage::default(),
        retries_used: 0,
        last_step_usage: None,
        last_step_finish_reason: None,
        trailing_origin: None,
        last_run_id: Some(RunId("budget".into())),
        admission_checkpoint: crate::AdmissionCheckpointV1 {
            recovery_cuts: Vec::new(),
            has_model_attempt: None,
            tool_set_history: Default::default(),
            media: None,
            head: None,
            has_deferred_calls: None,
            tags: Some(crate::tags::LineageTags::Off),
            pending_order: None,
            output_reservation: Some(Default::default()),
            input_budget: Some(crate::input_budget::InputTokenBudget::default()),
            session_class: None,
            // A valid tail seed includes the first run's WAL-derived policy.
            lineage_retention: Some(crate::LineageRetention::Forever),
            compaction: None,
            effort_history: None,
            ownership: Some(crate::SessionOwnershipV1::default()),
            at_seq: 8,
            last_episode: Some(crate::LastEpisodeV1 {
                account_limit_pause: None,
                cancel_at: None,
                on_restart: None,
                pause_reason: None,
                keep_warm: None,
                marker_order_version: None,
                request_identity_digest: None,
                run_id: RunId("budget".into()),
                config,
                input: Vec::new(),
                provider_call_started: true,
                config_refrozen: false,
                disposition: crate::EpisodeDisposition::Running,
                usage: None,
                retries_used: 0,
                budget_replay_state: Some(crate::BudgetReplayStateV1 {
                    version: crate::BUDGET_REPLAY_STATE_VERSION,
                    tool_call_count: 0,
                    fired_nudge_indices: Vec::new(),
                    finalize_fired: false,
                    finalize_pending: false,
                    budget_last_threshold: None,
                }),
            }),
            pending_fifo: Arc::new(Vec::new()),
        },
        tag_seed: Default::default(),
    };
    let wal_store = BudgetTailWalStore {
        observed_after: std::sync::Mutex::new(Vec::new()),
    };
    let store = Arc::new(CapturingProjectionStore {
        seed: Some(seed),
        ..Default::default()
    });
    project_committed(
        &wal_store,
        store.clone() as Arc<dyn SessionStore>,
        &BindIdentity::new("/tmp/project", "runner", "budget-tail"),
        10,
        true,
    )
    .await;
    assert_eq!(*wal_store.observed_after.lock().unwrap(), vec![8]);
    assert_eq!(*store.projected_seqs.lock().unwrap(), vec![9, 10]);
}

#[tokio::test]
async fn projection_read_is_bounded_by_the_fsynced_safe_head() {
    let session = BindIdentity::new("/tmp/project", "runner", "session");
    let store = Arc::new(CapturingProjectionStore::default());
    let (projector, pending) = projection_channel();
    projector.enqueue(2);
    let task_store = store.clone();
    let task = tokio::spawn(async move {
        projector_task(
            &BoundedReadableWalStore,
            task_store as Arc<dyn SessionStore>,
            &session,
            pending,
        )
        .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("projection starts");
    projector.close();
    task.await.expect("projector task joins");

    assert_eq!(*store.projected_seqs.lock().unwrap(), vec![1, 2]);
}

#[tokio::test]
async fn projector_coalesces_rapid_barriers_to_the_highest_safe_head() {
    const BARRIERS: u64 = 64;
    let session = BindIdentity::new("/tmp/project", "runner", "coalesced");
    let store = Arc::new(CapturingProjectionStore::default());
    let (projector, pending) = projection_channel();

    for safe_h in 1..=BARRIERS {
        projector.enqueue(safe_h);
    }
    let task_store = store.clone();
    let task = tokio::spawn(async move {
        projector_task(
            &BoundedReadableWalStore,
            task_store as Arc<dyn SessionStore>,
            &session,
            pending,
        )
        .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("projection starts");
    projector.close();
    task.await.expect("projector task joins");

    assert_eq!(
        store.projected_seqs.lock().unwrap().last().copied(),
        Some(BARRIERS)
    );
    assert!(
        store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            < BARRIERS as usize,
        "coalescing must perform fewer iterations than published barriers"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projector_shutdown_waits_for_current_write_and_skips_queued_work() {
    let session = BindIdentity::new("/tmp/project", "runner", "blocked-drain");
    let store = Arc::new(BlockingProjectionStore::default());
    let (projector, pending) = projection_channel();
    projector.enqueue(1);
    let task_store = store.clone();
    let mut task = tokio::spawn(async move {
        projector_task(
            &BoundedReadableWalStore,
            task_store as Arc<dyn SessionStore>,
            &session,
            pending,
        )
        .await;
    });

    let wait_store = store.clone();
    tokio::task::spawn_blocking(move || wait_store.wait_until_started())
        .await
        .expect("start waiter joins");
    projector.enqueue(2);
    projector.close();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut task)
            .await
            .is_err(),
        "shutdown must not abandon the in-flight blocking write"
    );

    store.release();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("drain completes after write release")
        .expect("projector task joins");
    let state = store.state.lock().unwrap();
    assert_eq!(state.calls, 1, "queued-only projection must be skipped");
    assert_eq!(state.projected_seqs, vec![1]);
}

#[tokio::test]
async fn projection_consumes_and_persists_exact_watermark_anchor() {
    let session = BindIdentity::new("/tmp/project", "runner", "session");
    let seed_anchor = broca_wal::WalByteAnchor {
        file_id: 11,
        seq: 1,
        byte_end: 100,
    };
    let wal_store = AnchoredReadableWalStore {
        observed_anchor: std::sync::Mutex::new(None),
    };
    let store = Arc::new(CapturingProjectionStore {
        seed_watermark: 1,
        seed_anchor: Some(seed_anchor),
        projected_watermark: Some(2),
        ..CapturingProjectionStore::default()
    });

    project_committed(
        &wal_store,
        store.clone() as Arc<dyn SessionStore>,
        &session,
        3,
        false,
    )
    .await;

    assert_eq!(
        *wal_store.observed_anchor.lock().unwrap(),
        Some(seed_anchor)
    );
    assert_eq!(*store.projected_seqs.lock().unwrap(), vec![2, 3]);
    assert_eq!(
        *store.persisted_anchor.lock().unwrap(),
        Some(broca_wal::WalByteAnchor {
            file_id: 11,
            seq: 2,
            byte_end: 200,
        }),
        "the projection watermark can precede the range's last record"
    );
}

/// Projection reads the WAL through the same decoder, so a WAL a newer build appended to
/// projects nothing: the store's watermark stays put rather than advancing over a record
/// this build cannot interpret, and the failure is counted as a projection warning.
#[tokio::test]
async fn projection_projects_nothing_from_a_newer_writer_wal() {
    let _counter = PROJECTION_COUNTER_LOCK.lock().await;
    let root = broca_test_support::scratch_root()
        .join(format!("broca-projection-newer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let wal_store = crate::FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/tmp/project", "runner", "newer-writer");
    let record = |n| WalRecord::StepStarted { step_id: StepId(n) };
    {
        let mut wal = wal_store.open(&session).unwrap();
        wal.append(1, &record(1), true).unwrap();
        wal.append(1, &record(2), true).unwrap();
    }
    let path = wal_store.wal_path(&session);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(
        broca_wal::framing::encode_with_requires(3, 1, &["from-the-future/v1"], &record(3))
            .unwrap(),
    );
    std::fs::write(&path, bytes).unwrap();
    assert!(matches!(
        wal_store.read_records_range(&session, 0, Some(3), None),
        Err(broca_wal::WalError::NewerWriter { ver: 3, .. })
    ));

    let store = Arc::new(CapturingProjectionStore::default());
    let warnings_before = projection_failures();
    project_committed(
        &wal_store,
        store.clone() as Arc<dyn SessionStore>,
        &session,
        3,
        false,
    )
    .await;
    assert_eq!(
        store
            .projection_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(store.projected_seqs.lock().unwrap().is_empty());
    assert!(projection_failures() > warnings_before);
    assert!(crate::projection_health::is_behind(&session));
    project_committed(
        &ReadableWalStore,
        Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
        &session,
        3,
        false,
    )
    .await;
    assert!(!crate::projection_health::is_behind(&session));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn projection_failure_then_success_clears_behind_but_keeps_lifetime_count() {
    let _counter = PROJECTION_COUNTER_LOCK.lock().await;
    let failures_before = projection_failures();
    let session = BindIdentity::new("/tmp/project", "runner", "projection-recovery");

    let (projector, pending) = projection_channel();
    projector.enqueue(1);
    let task_session = session.clone();
    let task_session_probe = session.clone();
    let task = tokio::spawn(async move {
        projector_task(
            &ReadableWalStore,
            Arc::new(FailingProjectionStore) as Arc<dyn SessionStore>,
            &task_session,
            pending,
        )
        .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !crate::projection_health::is_behind(&task_session_probe) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("projection failure is reported");
    projector.close();
    task.await.expect("projector task joins");

    // This retry succeeds: health no longer reports the session behind, while the lifetime
    // projection_failures metric still records the original failed attempt.
    // Other tests project in parallel, so the process-wide counters only ever prove a lower
    // bound; this session's own membership is exact.
    let failures_after_failure = super::projection_failures();
    assert!(
        failures_after_failure > failures_before,
        "a failing projection must be visible to operators"
    );
    assert!(
        crate::projection_health::is_behind(&session),
        "the failed session is still behind"
    );

    project_committed(
        &ReadableWalStore,
        Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
        &session,
        1,
        false,
    )
    .await;
    assert!(
        projection_failures() >= failures_after_failure,
        "recovery preserves the process-lifetime failure count"
    );
    assert!(
        !crate::projection_health::is_behind(&session),
        "successful projection clears the session's recovery debt"
    );
}

#[tokio::test]
async fn two_projection_failures_with_one_recovery_leave_one_session_behind() {
    let _counter = PROJECTION_COUNTER_LOCK.lock().await;
    let failures_before = projection_failures();
    let first = BindIdentity::new("/tmp/project", "runner", "health-first");
    let second = BindIdentity::new("/tmp/project", "runner", "health-second");

    for session in [&first, &second] {
        project_committed(
            &ReadableWalStore,
            Arc::new(FailingProjectionStore) as Arc<dyn SessionStore>,
            session,
            1,
            false,
        )
        .await;
    }
    let failures_after = projection_failures();
    assert!(failures_after >= failures_before + 2);
    assert!(crate::projection_health::is_behind(&first));
    assert!(crate::projection_health::is_behind(&second));

    project_committed(
        &ReadableWalStore,
        Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
        &first,
        1,
        false,
    )
    .await;
    assert!(projection_failures() >= failures_after);
    assert!(!crate::projection_health::is_behind(&first));
    assert!(
        crate::projection_health::is_behind(&second),
        "recovering one session leaves the other session's debt intact"
    );

    // Do not leave synthetic debt for later tests in this shared process.
    project_committed(
        &ReadableWalStore,
        Arc::new(CapturingProjectionStore::default()) as Arc<dyn SessionStore>,
        &second,
        1,
        false,
    )
    .await;
    assert!(!crate::projection_health::is_behind(&second));
}

struct CancellationProbeProvider {
    started: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
}

impl CancellationProbeProvider {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn wait_cancelled(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !self.cancelled.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("step token was not cancelled");
    }
}

#[async_trait::async_trait]
impl Provider for CancellationProbeProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        panic!("cancellation tests must pass the step token to the provider")
    }

    async fn step_with_cancel(
        &self,
        _step_id: StepId,
        _opts: &CallOptions,
        cancel: CancellationToken,
        _deadline: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(1);
        let started = Arc::clone(&self.started);
        let cancelled = Arc::clone(&self.cancelled);
        tokio::spawn(async move {
            started.notify_one();
            cancel.cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            drop(tx);
        });
        rx
    }
}

/// If each step clones the run cancellation token, the watchdog cancellation propagates to the
/// run itself, causing the final assertion to fail.
#[tokio::test]
async fn watchdog_step_cancel_is_isolated_from_run_cancel() {
    let provider = CancellationProbeProvider::new();
    let run_cancel = CancellationToken::new();
    let control = RunControl {
        cancel: Some(run_cancel.clone()),
        provider_call_timeout: Some(std::time::Duration::from_millis(10)),
        ..Default::default()
    };
    let (out, _output_rx) = mpsc::channel(1);

    let outcome = run_step_watched(
        &provider,
        StepId(1),
        &Arc::new(CallOptions::new(Vec::new())),
        0,
        &control,
        display_text::DisplayOutput::new(&out, false),
        AttemptCallbacks {
            record: &|_| Box::pin(async { Ok(()) }),
            before_send: None,
        },
    )
    .await;

    assert!(matches!(outcome, Ok(Err(RunFinishReason::Interrupted))));
    provider.wait_cancelled().await;
    assert!(
        !run_cancel.is_cancelled(),
        "a watchdog must cancel only the timed-out step"
    );
}

#[tokio::test]
async fn no_timeout_parent_cancel_propagates_to_step() {
    let provider = CancellationProbeProvider::new();
    let run_cancel = CancellationToken::new();
    let control = RunControl {
        cancel: Some(run_cancel.clone()),
        provider_call_timeout: None,
        ..Default::default()
    };
    let (out, _output_rx) = mpsc::channel(1);
    let opts = Arc::new(CallOptions::new(Vec::new()));
    let run = run_step_watched(
        &provider,
        StepId(1),
        &opts,
        0,
        &control,
        display_text::DisplayOutput::new(&out, false),
        AttemptCallbacks {
            record: &|_| Box::pin(async { Ok(()) }),
            before_send: None,
        },
    );
    let cancel_parent = async {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            provider.started.notified(),
        )
        .await
        .expect("provider step did not start");
        run_cancel.cancel();
    };

    let (outcome, ()) = tokio::join!(run, cancel_parent);

    assert!(matches!(outcome, Ok(Err(RunFinishReason::Cancelled))));
    provider.wait_cancelled().await;
}

struct MessageWithoutFinishedProvider;

#[async_trait::async_trait]
impl Provider for MessageWithoutFinishedProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(1);
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId("unterminated".into()),
                    content: vec![ContentBlock::Text {
                        text: "not authoritative".into(),
                    }],
                },
            },
        ))
        .await
        .unwrap();
        rx
    }
}

#[tokio::test]
async fn message_without_finished_control_is_not_authoritative() {
    let (out, _output_rx) = mpsc::channel(2);
    let outcome = run_step(
        &MessageWithoutFinishedProvider,
        StepId(1),
        Arc::new(CallOptions::new(Vec::new())),
        0,
        &CancellationToken::new(),
        None,
        &mut display_text::DisplayOutput::new(&out, false),
        &mut None,
        &|_| Box::pin(async { Ok(()) }),
        None,
        None,
        None,
    )
    .await;

    match outcome.unwrap() {
        StepOutcome::Failed { error, .. } => {
            assert_eq!(error.class, ErrorClass::ProviderUnavailable);
            assert!(error.message.contains("Finished control"));
        }
        StepOutcome::Completed { .. } => {
            panic!("a message without Finished must not complete a model step")
        }
    }
}
