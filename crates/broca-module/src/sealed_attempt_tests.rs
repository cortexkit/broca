//! The shutdown seal records the provider attempt it finds in flight.
//!
//! These drive the real run loop over the real HTTP transport against a local server, and stop
//! it through the registry's real seal path. The expected byte counts and digests come from the
//! server's own bytes, never from anything broca reports.

use super::tests::text_req;
use super::wal_test_support::NoFsyncWalStore;
use super::*;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore, FileWalStore,
    Provider, ShutdownSeal, StartMode, StubToolPlane,
};
use broca_protocol::provider::NormalizedEvent;
use broca_protocol::{AttemptOutcome, BindIdentity, CallOptions, RunFinishReason, StepId};
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::AuthHandle,
    LiveProvider,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

/// The registry budget the tests seal under. Zero grace skips the wait for in-flight runs to
/// finish on their own (a stalled run never would), so the seal starts at once; the total is
/// the production figure.
const GRACE: Duration = Duration::ZERO;
const TOTAL: Duration = Duration::from_secs(20);

/// An Anthropic stream cut short: the message has started and one text delta has arrived, and
/// the delta is the LAST event, so the run seeing that delta's display event proves the client
/// has read every byte of this prefix.
fn anthropic_prefix() -> Vec<u8> {
    let events = [
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"output_tokens":0}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}"#,
        ),
    ];
    let mut body = String::new();
    for (event, data) in events {
        body.push_str(&format!("event: {event}\ndata: {data}\n\n"));
    }
    body.into_bytes()
}

/// What the server does with each connection, in order.
enum Reply {
    /// A 200 promising more than `prefix`, writing `prefix`, then holding the connection open
    /// until the client drops it.
    Stall {
        prefix: Vec<u8>,
    },
    /// A 503 asking for a long retry delay, so the transport sits between attempts.
    RetryLater,
    Complete {
        body: Vec<u8>,
    },
}

struct Server {
    url: String,
    request_bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let request_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies = Arc::clone(&request_bodies);
        let task = tokio::spawn(async move {
            for reply in script {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body = read_request_body(&mut socket).await;
                bodies.lock().unwrap().push(body);
                // Each connection is served on its own task so a stalled one does not keep
                // the next from being accepted.
                tokio::spawn(async move { write_reply(&mut socket, reply).await });
            }
        });
        Self {
            url,
            request_bodies,
            task,
        }
    }

    fn request_bodies(&self) -> Vec<Vec<u8>> {
        self.request_bodies.lock().unwrap().clone()
    }
}

async fn read_request_body(socket: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected before sending the request");
        request.extend_from_slice(&chunk[..read]);
        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
    let content_length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(|value| value.trim().parse::<usize>().unwrap())
        .unwrap_or(0);
    while request.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(
            read, 0,
            "client disconnected before the request body completed"
        );
        request.extend_from_slice(&chunk[..read]);
    }
    request[header_end..header_end + content_length].to_vec()
}

async fn write_reply(socket: &mut TcpStream, reply: Reply) {
    match reply {
        Reply::Complete { body } => {
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n", body.len());
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.flush().await.unwrap();
            return;
        }
        Reply::Stall { prefix } => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
                prefix.len() + 4096
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&prefix).await.unwrap();
            socket.flush().await.unwrap();
        }
        Reply::RetryLater => {
            let body = b"overloaded";
            let head = format!(
                "HTTP/1.1 503 Busy\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nRetry-After: 60\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
            socket.flush().await.unwrap();
        }
    }
    // Hold the connection until the client goes away.
    let mut byte = [0u8; 1];
    while matches!(socket.read(&mut byte).await, Ok(n) if n > 0) {}
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Drives the real run loop with `provider`, and signals `displayed` on the first display
/// event the step produces.
struct LoopExecutor {
    provider: Box<dyn Provider>,
    wal_store: Arc<NoFsyncWalStore>,
    displayed: Arc<Notify>,
}

#[async_trait::async_trait]
impl RunExecutor for LoopExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let (tap_tx, mut tap_rx) = mpsc::channel::<OutputEvent>(256);
        let displayed = Arc::clone(&self.displayed);
        tokio::spawn(async move {
            while let Some(event) = tap_rx.recv().await {
                if matches!(event, OutputEvent::Display(_)) {
                    displayed.notify_one();
                }
                if out.send(event).await.is_err() {
                    break;
                }
            }
        });
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            StartMode::FreshOrResume,
            &self.provider,
            &StubToolPlane::new(),
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            job.control,
            &tap_tx,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

struct Fixture {
    registry: LineageRegistry,
    key: BindIdentity,
    root: std::path::PathBuf,
    displayed: Arc<Notify>,
    wal_store: Arc<NoFsyncWalStore>,
}

impl Fixture {
    fn new(provider: impl FnOnce() -> Box<dyn Provider>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-sealed-attempt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        // These tests inspect transport handover and record ordering, not crash durability.
        let wal_store = Arc::new(NoFsyncWalStore::new(FileWalStore::new(root.join("wal"))));
        let displayed = Arc::new(Notify::new());
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal_store.clone(),
            Arc::new(LoopExecutor {
                provider: provider(),
                wal_store: wal_store.clone(),
                displayed: Arc::clone(&displayed),
            }),
        );
        Self {
            registry: LineageRegistry::new(Arc::new(opener)),
            key,
            root,
            displayed,
            wal_store,
        }
    }

    fn live(server: &Server) -> Self {
        let url = server.url.clone();
        Self::new(move || {
            let mut frozen = anthropic_frozen_config("m");
            frozen.base_url = Some(url);
            Box::new(LiveProvider::new(
                AnthropicMessages,
                frozen,
                AuthHandle::Bearer("test".into()),
            ))
        })
    }

    async fn start(&self) {
        assert!(matches!(
            self.registry
                .route_send(
                    &self.key,
                    text_req(&self.key, "sealed-run"),
                    RunKind::FreshOrResume
                )
                .await
                .unwrap(),
            SendOutcome::Active { .. }
        ));
    }

    /// Seal through the registry, returning how long the whole seal took.
    async fn seal(&self) -> Duration {
        self.wal_store.refuse_physical_sync();
        let started = tokio::time::Instant::now();
        assert!(
            self.registry.stop_and_seal_with_budget(GRACE, TOTAL).await,
            "the seal must complete: {:?}",
            self.registry.shutdown_report()
        );
        let elapsed = started.elapsed();
        assert_eq!(self.registry.slot_counts().open, 0);
        elapsed
    }

    /// Every record, read back from the file after the actor and its writer are gone.
    fn records(&self) -> Vec<WalRecord> {
        FileWalStore::new(self.root.join("wal"))
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn terminals(records: &[WalRecord]) -> Vec<RunFinishReason> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect()
}

fn attempt_outcomes(records: &[WalRecord]) -> Vec<(u32, AttemptOutcome)> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ModelAttemptFinished {
                attempt, outcome, ..
            } => Some((*attempt, outcome.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_seal_records_the_attempt_in_flight_before_the_interrupted_terminal() {
    let prefix = anthropic_prefix();
    let server = Server::start(vec![Reply::Stall {
        prefix: prefix.clone(),
    }])
    .await;
    let f = Fixture::live(&server);
    f.start().await;
    // The text delta ends the prefix, so its display event means every prefix byte arrived.
    broca_test_support::wait_for(
        "stalled stream prefix reaches the run",
        f.displayed.notified(),
        || {
            format!(
                "slots={:?}, records={}",
                f.registry.slot_counts(),
                broca_test_support::record_kinds(f.records())
            )
        },
    )
    .await;

    let elapsed = f.seal().await;
    eprintln!("seal with a stalled attempt in flight took {elapsed:?}");

    let records = f.records();
    let attempts: Vec<(usize, &WalRecord)> = records
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r, WalRecord::ModelAttemptFinished { .. }))
        .collect();
    assert_eq!(
        attempts.len(),
        1,
        "exactly one attempt record: {attempts:#?}"
    );
    let (attempt_at, record) = attempts[0];
    let WalRecord::ModelAttemptFinished {
        step_id,
        attempt,
        request_body_len,
        request_body_sha256,
        http_status,
        content_type,
        response_body_len,
        response_body_sha256,
        response_body_cap,
        outcome,
        ..
    } = record
    else {
        unreachable!()
    };
    assert_eq!(outcome, &AttemptOutcome::SealedInFlight);
    assert_eq!(*step_id, StepId(1));
    assert_eq!(*attempt, 0);
    assert_eq!(*http_status, Some(200));
    assert_eq!(content_type.as_deref(), Some("text/event-stream"));
    assert_eq!(*response_body_len, prefix.len() as u64);
    assert_eq!(response_body_sha256, &sha256_hex(&prefix));
    assert_eq!(*response_body_cap, None);
    let sent = server.request_bodies();
    assert_eq!(sent.len(), 1, "the server saw one attempt");
    assert_eq!(*request_body_len, sent[0].len() as u64);
    assert_eq!(request_body_sha256, &sha256_hex(&sent[0]));

    let terminal_at = records
        .iter()
        .position(|r| matches!(r, WalRecord::RunFinished { .. }))
        .expect("the seal wrote a terminal");
    assert!(
        attempt_at < terminal_at,
        "the attempt record must precede the run's terminal"
    );
    assert_eq!(terminals(&records), vec![RunFinishReason::Interrupted]);
    assert!(
        !terminals(&records).contains(&RunFinishReason::Cancelled)
            && !attempt_outcomes(&records)
                .iter()
                .any(|(_, outcome)| outcome == &AttemptOutcome::Cancelled),
        "a seal must never read as a caller's cancel: {records:#?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_refusal_resend_continues_in_a_new_episode_without_reusing_attempt_identity() {
    let complete = |reason: &str| {
        let mut bytes = anthropic_prefix();
        bytes.extend_from_slice(format!("event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\nevent: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{reason}\"}},\"usage\":{{\"output_tokens\":1}}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n").as_bytes());
        bytes
    };
    let server = Server::start(vec![
        Reply::Complete {
            body: complete("refusal"),
        },
        Reply::Stall {
            prefix: anthropic_prefix(),
        },
        Reply::Complete {
            body: complete("end_turn"),
        },
    ])
    .await;
    let mut frozen = anthropic_frozen_config("claude-opus-5-5");
    frozen.base_url = Some(server.url.clone());
    let mut fallback = anthropic_frozen_config("claude-opus-4-8");
    fallback.base_url = Some(server.url.clone());
    frozen.refusal_fallback = Some(Box::new(broca_provider::wire::RefusalFallbackConfig {
        model: broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8"),
        context_limit: None,
        frozen: fallback,
    }));
    let initial = frozen.clone();
    let f = Fixture::new(move || {
        Box::new(LiveProvider::new(
            AnthropicMessages,
            initial,
            AuthHandle::Bearer("test".into()),
        ))
    });
    let mut request = text_req(&f.key, "sealed-run");
    request.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-5-5");
    assert!(matches!(
        f.registry
            .route_send(&f.key, request.clone(), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    broca_test_support::wait_for(
        "refusal resend reaches the fallback HTTP request",
        async {
            while server.request_bodies().len() < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        },
        || format!("records={:?}", f.records()),
    )
    .await;
    f.seal().await;
    let sealed = f.records();
    let store = FileWalStore::new(f.root.join("wal"));
    let lineage = {
        use broca_wal::Wal;
        broca_wal::FileWal::open(store.wal_path(&f.key))
            .unwrap()
            .lineage_id()
            .unwrap()
    };
    let sealed_ids: Vec<_> = store
        .read_records(&f.key)
        .unwrap()
        .into_iter()
        .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
        .map(|frame| format!("{lineage}:{}", frame.seq))
        .collect();
    assert_eq!(
        attempt_outcomes(&sealed),
        [
            (0, AttemptOutcome::Complete),
            (1, AttemptOutcome::SealedInFlight)
        ]
    );
    let before = broca_core::replay(&sealed)
        .unwrap()
        .refusal_fallback
        .unwrap();
    assert_eq!(
        before.phase,
        broca_wal::RefusalFallbackPhase::Fallback { remaining: 10 }
    );
    let provider = LiveProvider::new(AnthropicMessages, frozen, AuthHandle::Bearer("test".into()));
    let (out, mut events) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let result = broca_core::run_loop(
        request.clone(),
        &provider,
        &StubToolPlane::new(),
        &FileLeaseStore::new(f.root.join("leases")),
        &AllowAll,
        &FileWalStore::new(f.root.join("wal")),
        &out,
    )
    .await
    .unwrap();
    assert_eq!(
        result.reason,
        RunFinishReason::Interrupted,
        "a sealed run is terminal, so an idempotent resend does not reopen it"
    );
    assert_eq!(server.request_bodies().len(), 2);
    request.run_id = broca_protocol::RunId("after-seal".into());
    let result = broca_core::run_loop_append_episode(
        request,
        &provider,
        &StubToolPlane::new(),
        &FileLeaseStore::new(f.root.join("leases")),
        &AllowAll,
        &FileWalStore::new(f.root.join("wal")),
        None,
        &out,
    )
    .await
    .unwrap();
    drop(out);
    collector.await.unwrap();
    assert_eq!(result.reason, RunFinishReason::Completed);
    let records = f.records();
    assert_eq!(
        attempt_outcomes(&records),
        [
            (0, AttemptOutcome::Complete),
            (1, AttemptOutcome::SealedInFlight),
            (0, AttemptOutcome::Complete)
        ]
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepRefused { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count(),
        2
    );
    let mut run = String::new();
    let mut identities = Vec::new();
    for record in &records {
        match record {
            WalRecord::RunStarted { run_id, .. } => run.clone_from(&run_id.0),
            WalRecord::ModelAttemptFinished {
                step_id, attempt, ..
            } => identities.push((run.clone(), *step_id, *attempt)),
            _ => {}
        }
    }
    assert_eq!(
        identities,
        [
            ("sealed-run".into(), StepId(1), 0),
            ("sealed-run".into(), StepId(1), 1),
            ("after-seal".into(), StepId(1), 0)
        ]
    );
    let frames = store.read_records(&f.key).unwrap();
    let resumed_lineage = {
        use broca_wal::Wal;
        broca_wal::FileWal::open(store.wal_path(&f.key))
            .unwrap()
            .lineage_id()
            .unwrap()
    };
    assert_eq!(resumed_lineage, lineage);
    let ids: Vec<_> = frames
        .iter()
        .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
        .map(|frame| format!("{resumed_lineage}:{}", frame.seq))
        .collect();
    assert_eq!(&ids[..2], sealed_ids.as_slice());
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        3
    );
    assert!(frames.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
    let after = broca_core::replay(&records)
        .unwrap()
        .refusal_fallback
        .unwrap();
    assert_eq!(
        after.phase,
        broca_wal::RefusalFallbackPhase::Fallback { remaining: 9 }
    );
    assert_eq!(after.re_refusals, before.re_refusals);
    let sent = server.request_bodies();
    assert_eq!(sent.len(), 3);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&sent[0]).unwrap()["model"],
        "claude-opus-5-5"
    );
    for bytes in &sent[1..] {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["model"],
            "claude-opus-4-8"
        );
    }
}

/// With no attempt in flight (the transport is waiting out a retry delay after a recorded
/// attempt), the seal adds no record, does not duplicate the recorded one, and no further
/// attempt starts.
#[tokio::test(flavor = "multi_thread")]
async fn the_seal_between_attempts_adds_no_attempt_record() {
    let server = Server::start(vec![
        Reply::RetryLater,
        Reply::Stall {
            prefix: anthropic_prefix(),
        },
    ])
    .await;
    let f = Fixture::live(&server);
    f.start().await;
    // Wait for the failed attempt's record to reach the WAL, which puts the transport in its
    // 60-second retry delay.
    broca_test_support::wait_until("first attempt is recorded", || async {
        let records = f.records();
        if !attempt_outcomes(&records).is_empty() {
            Ok(())
        } else {
            Err(broca_test_support::record_kinds(records))
        }
    })
    .await;

    f.seal().await;

    let records = f.records();
    assert_eq!(
        attempt_outcomes(&records),
        vec![(0, AttemptOutcome::HttpError)],
        "the seal must neither add nor repeat an attempt record"
    );
    assert_eq!(terminals(&records), vec![RunFinishReason::Interrupted]);
    assert_eq!(
        server.request_bodies().len(),
        1,
        "no attempt may start once the seal has tripped"
    );
}

/// A provider that registers an attempt with the seal and then never hands its record over.
struct IgnoresTheSeal {
    started: Arc<Notify>,
}

#[async_trait::async_trait]
impl Provider for IgnoresTheSeal {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        panic!("the loop must hand this provider the shutdown seal")
    }

    async fn step_with_shutdown_seal(
        &self,
        _step_id: StepId,
        _opts: Arc<CallOptions>,
        _cancel: CancellationToken,
        _deadline: Option<tokio::time::Instant>,
        seal: ShutdownSeal,
    ) -> mpsc::Receiver<NormalizedEvent> {
        assert!(seal.begin_attempt());
        let (tx, rx) = mpsc::channel(1);
        // Keep the stream open and silent for as long as the run reads it.
        tokio::spawn(async move { tx.closed().await });
        self.started.notify_one();
        rx
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_seal_stays_bounded_when_the_transport_never_hands_over_its_record() {
    let started = Arc::new(Notify::new());
    let provider_started = Arc::clone(&started);
    let f = Fixture::new(move || {
        Box::new(IgnoresTheSeal {
            started: provider_started,
        })
    });
    f.start().await;
    broca_test_support::wait_for("provider step starts", started.notified(), || {
        format!("slots={:?}", f.registry.slot_counts())
    })
    .await;

    let elapsed = f.seal().await;

    // Waiting can only lengthen `elapsed`, so this lower bound holds on any runner: the seal
    // waited out the handover bound instead of giving up on the record early.
    assert!(
        elapsed >= SEALED_ATTEMPT_HANDOVER_BOUND,
        "the seal gave up on the record before its bound: {elapsed:?}; {:?}",
        f.registry.shutdown_report()
    );
    // The upper bound is the actor's own: its seal deadline covers the handover wait and the
    // terminal write, and the registry records a failure when the deadline is missed. Reading
    // that verdict, rather than timing the whole stop from here, keeps a slow runner from
    // failing a seal that met its bound.
    let failures = f.registry.seal_failures();
    assert!(
        failures.is_empty(),
        "the seal must finish within its per-actor bound: {failures:?}; {:?}",
        f.registry.shutdown_report()
    );
    let records = f.records();
    assert!(attempt_outcomes(&records).is_empty());
    assert_eq!(terminals(&records), vec![RunFinishReason::Interrupted]);
}
